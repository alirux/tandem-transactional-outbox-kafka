package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.spi.ToolProvider;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Guards what lets an application running on virtual threads use Tandem without configuring anything:
 * <b>nothing on the write path holds a monitor</b>, so the outbox insert can never pin a carrier
 * (virtual-threads-decision.md §6). A monitor is only safe where Tandem owns the thread, which is the
 * relay, and the relay creates platform threads itself.
 *
 * <p>Reading the compiled classes rather than the sources is the point, twice over: a
 * {@code synchronized} <i>method</i> leaves no bytecode at all, only an access flag, and a
 * {@code synchronized} block inside a lambda or a nested class is easy to miss by eye. Both are
 * visible here. The property is also the kind that breaks in silence: adding a lock to the write path
 * breaks no test and contradicts no compiler, it only makes a published claim false.
 *
 * <p>The scan covers {@code tandem-core} as well as this module, since the write path spans both and
 * {@code tandem-core} has no test of its own that could see this.
 */
class VirtualThreadFootprintTest {

    /**
     * The only classes allowed to hold a monitor, all of them relay-side and all of them running on
     * threads {@code WorkerPool} creates itself. Nested classes count as their own entry: a monitor
     * hidden in one is exactly what this scan exists to surface.
     */
    private static final Set<String> RELAY_OWNED_THREADS = Set.of(
            "com.codingful.tandem.jdbc.WorkerPool",
            "com.codingful.tandem.jdbc.WorkerWakeups$Slot",
            "com.codingful.tandem.jdbc.PgNotifyWakeup");

    @Test
    void GIVEN_an_application_running_on_virtual_threads_WHEN_it_writes_to_the_outbox_THEN_nothing_it_executes_can_pin_a_carrier() {
        List<String> holdingAMonitor = classesHoldingAMonitor(compiledClasses());

        assertThat(holdingAMonitor)
                .as("only the relay's own threads may hold a monitor; everything else runs on the caller's thread")
                .isEmpty();
    }

    /** Without this, an empty result above could mean the scan sees nothing at all. */
    @Test
    void GIVEN_a_class_that_does_hold_a_monitor_WHEN_the_same_scan_reads_it_THEN_it_is_reported() {
        CompiledClass fixture = new CompiledClass(
                codeSourceOf(VirtualThreadFootprintTest.class).toString(), SynchronizedFixture.class.getName());

        List<String> found = classesHoldingAMonitor(List.of(fixture));

        assertThat(found).containsExactly(SynchronizedFixture.class.getName());
    }

    @Test
    void GIVEN_the_write_path_spans_two_modules_WHEN_the_guard_checks_it_THEN_both_of_them_are_covered() {
        List<String> scanned = compiledClasses().stream().map(CompiledClass::className).toList();

        assertThat(scanned)
                .contains(JdbcOutboxRepository.class.getName())
                .contains(OutboxMessage.class.getName());
    }

    /**
     * Names every scanned class that holds a monitor and is not allowed to, whether through a
     * {@code synchronized} method (an access flag, no bytecode) or a {@code monitorenter} (a
     * {@code synchronized} block).
     */
    private static List<String> classesHoldingAMonitor(List<CompiledClass> classes) {
        List<String> offenders = new ArrayList<>();
        for (CompiledClass compiled : classes) {
            if (RELAY_OWNED_THREADS.contains(compiled.className())) {
                continue;
            }
            String disassembly = disassemble(compiled);
            boolean synchronizedMethod = disassembly.lines().anyMatch(line -> line.contains(" synchronized "));
            boolean synchronizedBlock = disassembly.contains("monitorenter");
            if (synchronizedMethod || synchronizedBlock) {
                offenders.add(compiled.className());
            }
        }
        return offenders;
    }

    /**
     * {@code javap -p -c} on one class, through {@link ToolProvider} so no external process and no
     * bytecode library is needed. The class is named on a classpath rather than passed as a file, which
     * is what lets the same code read a module that arrives as a jar. Absence of the tool fails the test
     * rather than skipping it: a footprint guard that quietly stops checking is worse than none.
     */
    private static String disassemble(CompiledClass compiled) {
        ToolProvider javap = ToolProvider.findFirst("javap").orElseThrow(() -> new IllegalStateException(
                "javap is not available on this JDK, so the monitor scan cannot run"));
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        int status = javap.run(new PrintWriter(out), new PrintWriter(err),
                "-p", "-c", "-cp", compiled.classpathRoot(), compiled.className());
        if (status != 0) {
            throw new IllegalStateException("javap failed on " + compiled.className() + ": " + err);
        }
        return out.toString();
    }

    /** One compiled class, and the classpath entry it can be read from. */
    private record CompiledClass(String classpathRoot, String className) {
    }

    /** Every compiled class of the two modules the write path spans. */
    private static List<CompiledClass> compiledClasses() {
        List<CompiledClass> classes = new ArrayList<>();
        classes.addAll(classesIn(codeSourceOf(JdbcOutboxRepository.class)));
        classes.addAll(classesIn(codeSourceOf(OutboxMessage.class)));
        return classes;
    }

    /**
     * Where a module's classes actually are, read off the class itself rather than built from a
     * relative path: a sibling module arrives on this module's test classpath as a jar, and a
     * hand-written {@code ../tandem-core/build/classes/...} would break the first time anything moves.
     */
    private static Path codeSourceOf(Class<?> type) {
        try {
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("could not locate the compiled classes of " + type, e);
        }
    }

    /** Every class in one classpath entry, be it a directory of classes or a jar. */
    private static List<CompiledClass> classesIn(Path root) {
        String classpathRoot = root.toString();
        try (FileSystem jar = Files.isDirectory(root) ? null : FileSystems.newFileSystem(root, Map.of())) {
            Path tree = jar == null ? root : jar.getPath("/");
            try (Stream<Path> files = Files.walk(tree)) {
                return files.filter(path -> path.toString().endsWith(".class"))
                        .map(path -> binaryName(tree, path))
                        .filter(name -> !name.equals("module-info") && !name.endsWith(".package-info"))
                        .map(name -> new CompiledClass(classpathRoot, name))
                        .toList();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String binaryName(Path root, Path classFile) {
        String relative = root.relativize(classFile).toString();
        return relative.substring(0, relative.length() - ".class".length())
                .replace(classFile.getFileSystem().getSeparator(), ".");
    }

    /** Holds a monitor both ways, so the scan is proven to see both. */
    @SuppressWarnings("unused")
    static final class SynchronizedFixture {

        private int counter;

        synchronized void countWithASynchronizedMethod() {
            counter++;
        }

        void countInsideASynchronizedBlock() {
            synchronized (this) {
                counter++;
            }
        }
    }
}
