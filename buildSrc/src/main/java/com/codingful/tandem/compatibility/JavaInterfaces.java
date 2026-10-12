package com.codingful.tandem.compatibility;

import com.sun.source.doctree.DocCommentTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.Tree;
import com.sun.source.util.DocSourcePositions;
import com.sun.source.util.DocTrees;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreePath;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.gradle.api.GradleException;

/**
 * Finds the public interfaces of a module's API in its Java sources, and the kind each one's javadoc
 * states (LLD-api-stability §4.3). An interface is API when it is public, top-level or nested in a type
 * that is itself API, and no segment of its package is {@code internal} (HLD-api-stability §3);
 * annotation types are not interfaces for this rule.
 *
 * <p>The sources are parsed with the JDK's own compiler, without compiling them: the working tree's
 * {@code src/main/java} and a baseline's sources jar go through the same parser.
 */
public final class JavaInterfaces {

    private JavaInterfaces() {
    }

    /** Reads Java source files, such as those of a source set's {@code java} directory set. */
    public static List<PublicInterface> inFiles(Collection<File> files) {
        Map<String, String> sources = new LinkedHashMap<>();
        files.forEach(file -> sources.put(file.getPath(), read(file)));
        return parse(sources);
    }

    public static List<PublicInterface> inSourcesJar(File jar) {
        Map<String, String> sources = new LinkedHashMap<>();
        try (ZipFile zip = new ZipFile(jar)) {
            for (ZipEntry entry : zip.stream().filter(entry -> entry.getName().endsWith(".java")).toList()) {
                try (InputStream content = zip.getInputStream(entry)) {
                    sources.put(jar.getName() + "!/" + entry.getName(),
                            new String(content.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + jar, e);
        }
        return parse(sources);
    }

    /**
     * Parses Java sources, keyed by the name a parse error reports them under.
     *
     * @throws GradleException when a source does not parse: an interface it declares would otherwise go
     *                         unchecked
     */
    static List<PublicInterface> parse(Map<String, String> sources) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new GradleException("The JVM running Gradle has no Java compiler to read javadoc with; run the "
                    + "build on a JDK");
        }
        List<JavaFileObject> files = new ArrayList<>();
        sources.forEach((name, content) -> files.add(new InMemorySource(files.size(), name, content)));
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null,
                StandardCharsets.UTF_8)) {
            JavacTask task = (JavacTask) compiler.getTask(null, fileManager, diagnostics, List.of("-proc:none"), null,
                    files);
            DocTrees docTrees = DocTrees.instance(task);
            Iterable<? extends CompilationUnitTree> units = task.parse();
            List<String> errors = diagnostics.getDiagnostics().stream()
                    .filter(diagnostic -> diagnostic.getKind() == Diagnostic.Kind.ERROR)
                    .map(diagnostic -> diagnostic.getSource().getName() + ":" + diagnostic.getLineNumber() + ": "
                            + diagnostic.getMessage(null))
                    .toList();
            if (!errors.isEmpty()) {
                throw new GradleException("Cannot parse the sources whose interfaces are checked:\n"
                        + String.join("\n", errors));
            }
            List<PublicInterface> interfaces = new ArrayList<>();
            for (CompilationUnitTree unit : units) {
                new UnitScan(unit, docTrees, interfaces).scan();
            }
            interfaces.sort(Comparator.comparing(PublicInterface::binaryName));
            return interfaces;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(File file) {
        try {
            return Files.readString(file.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }

    /** One compilation unit's API interfaces, found by walking its type declarations and their members. */
    private record UnitScan(CompilationUnitTree unit, DocTrees docTrees, List<PublicInterface> found) {

        void scan() {
            String packageName = unit.getPackageName() == null ? "" : unit.getPackageName().toString();
            if (Arrays.asList(packageName.split("\\.")).contains("internal")) {
                return;
            }
            TreePath unitPath = new TreePath(unit);
            for (Tree declaration : unit.getTypeDecls()) {
                if (declaration instanceof ClassTree type && type.getModifiers().getFlags().contains(Modifier.PUBLIC)) {
                    String binaryName = (packageName.isEmpty() ? "" : packageName + ".") + type.getSimpleName();
                    visit(new TreePath(unitPath, type), binaryName);
                }
            }
        }

        /** Records {@code path}'s type when it is an interface, then its members that are API too. */
        private void visit(TreePath path, String binaryName) {
            ClassTree type = (ClassTree) path.getLeaf();
            if (type.getKind() == Tree.Kind.INTERFACE) {
                found.add(new PublicInterface(binaryName, StatedKind.of(javadoc(path))));
            }
            // A member of an interface or an annotation type is public whether or not it says so.
            boolean membersPublic = type.getKind() == Tree.Kind.INTERFACE
                    || type.getKind() == Tree.Kind.ANNOTATION_TYPE;
            for (Tree member : type.getMembers()) {
                if (member instanceof ClassTree nested
                        && (membersPublic || nested.getModifiers().getFlags().contains(Modifier.PUBLIC))) {
                    visit(new TreePath(path, nested), binaryName + "$" + nested.getSimpleName());
                }
            }
        }

        /** The declaration's own javadoc comment, as written; {@code null} when it has none. */
        private String javadoc(TreePath path) {
            DocCommentTree comment = docTrees.getDocCommentTree(path);
            if (comment == null) {
                return null;
            }
            DocSourcePositions positions = docTrees.getSourcePositions();
            int start = (int) positions.getStartPosition(unit, comment, comment);
            int end = (int) positions.getEndPosition(unit, comment, comment);
            if (start < 0) {
                // An empty comment, /** */, has nothing to locate.
                return "";
            }
            try {
                return unit.getSourceFile().getCharContent(true).subSequence(start, end).toString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * A source held in memory, so a file on disk and an entry of a jar are read alike. Its URI is unique and
     * ends in the source's own file name, which javac checks a declaration against; diagnostics name the
     * source by {@link #getName()}.
     */
    private static final class InMemorySource extends SimpleJavaFileObject {

        private final String name;
        private final String content;

        InMemorySource(int index, String name, String content) {
            super(uri(index, name), Kind.SOURCE);
            this.name = name;
            this.content = content;
        }

        private static URI uri(int index, String name) {
            try {
                String fileName = name.substring(Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\')) + 1);
                return new URI("memory", null, "/" + index + "/" + fileName, null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(name, e);
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return content;
        }
    }
}
