package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JavaInterfacesTest {

    private static final String SPI = "/** A port. <p><b>SPI:</b> the application implements it. */\n";
    private static final String PROVIDED = "/** A port. <p><b>Provided:</b> Tandem implements it. */\n";

    private static List<PublicInterface> read(String source) {
        return JavaInterfaces.parse(Map.of("Source.java", source));
    }

    @Test
    void GIVEN_public_interfaces_stating_each_kind_WHEN_their_sources_are_read_THEN_each_is_found_with_its_kind() {
        List<PublicInterface> found = JavaInterfaces.parse(Map.of(
                "Router.java", "package com.acme;\n" + SPI + "public interface Router {}",
                "Store.java", "package com.acme;\n" + PROVIDED + "public interface Store {}"));

        assertThat(found).containsExactly(
                new PublicInterface("com.acme.Router", StatedKind.SPI),
                new PublicInterface("com.acme.Store", StatedKind.PROVIDED));
    }

    @Test
    void GIVEN_an_interface_nested_in_an_interface_WHEN_read_THEN_it_is_api_with_its_binary_name_and_its_own_kind() {
        List<PublicInterface> found = read("package com.acme;\n" + SPI
                + "public interface Source {\n" + PROVIDED + "    interface Listener {}\n}");

        assertThat(found).containsExactly(
                new PublicInterface("com.acme.Source", StatedKind.SPI),
                new PublicInterface("com.acme.Source$Listener", StatedKind.PROVIDED));
    }

    @Test
    void GIVEN_the_outer_type_states_a_kind_WHEN_a_nested_interface_states_none_THEN_the_nested_one_states_none() {
        List<PublicInterface> found = read("package com.acme;\n" + SPI
                + "public interface Recorder {\n    interface Span {}\n}");

        assertThat(found).contains(new PublicInterface("com.acme.Recorder$Span", StatedKind.NONE));
    }

    @Test
    void GIVEN_interfaces_nested_in_a_public_class_WHEN_read_THEN_only_the_public_ones_are_api() {
        List<PublicInterface> found = read("package com.acme;\npublic final class Holder {\n"
                + SPI + "    public interface Exposed {}\n"
                + SPI + "    interface Hidden {}\n"
                + "    public static final class Inner {\n" + SPI + "        public interface Deep {}\n    }\n}");

        assertThat(found).extracting(PublicInterface::binaryName)
                .containsExactly("com.acme.Holder$Exposed", "com.acme.Holder$Inner$Deep");
    }

    @Test
    void GIVEN_interfaces_reachable_only_through_a_type_that_is_not_api_WHEN_read_THEN_none_is_found() {
        List<PublicInterface> found = JavaInterfaces.parse(Map.of(
                "Jdbc.java", "package com.acme;\nfinal class Jdbc {\n" + SPI + "    public interface SqlAction {}\n}",
                "Classifier.java", "package com.acme;\n" + SPI + "interface Classifier {}",
                "Holder.java", "package com.acme;\npublic class Holder {\n    static class Inner {\n"
                        + SPI + "        public interface Deep {}\n    }\n}"));

        assertThat(found).isEmpty();
    }

    @Test
    void GIVEN_an_interface_in_an_internal_package_WHEN_read_THEN_it_is_not_api() {
        List<PublicInterface> found = JavaInterfaces.parse(Map.of(
                "Hidden.java", "package com.acme.internal.outbox;\npublic interface Hidden {}",
                "Kept.java", "package com.acme.internals;\n" + SPI + "public interface Kept {}"));

        assertThat(found).extracting(PublicInterface::binaryName).containsExactly("com.acme.internals.Kept");
    }

    @Test
    void GIVEN_an_interface_in_the_default_package_WHEN_read_THEN_its_binary_name_is_its_own_name() {
        List<PublicInterface> found = read(SPI + "public interface Source {\n" + PROVIDED + "    interface Listener {}\n}");

        assertThat(found).extracting(PublicInterface::binaryName).containsExactly("Source", "Source$Listener");
    }

    @Test
    void GIVEN_an_annotation_type_WHEN_read_THEN_it_is_not_an_interface_for_the_rule() {
        List<PublicInterface> found = read("package com.acme;\n/** Marks a method. */\npublic @interface Outbox {\n"
                + "    String aggregateType() default \"\";\n}");

        assertThat(found).isEmpty();
    }

    @Test
    void GIVEN_interfaces_whose_javadoc_states_no_single_kind_WHEN_read_THEN_each_states_none_or_conflicting() {
        List<PublicInterface> found = JavaInterfaces.parse(Map.of(
                "Bare.java", "package com.acme;\npublic interface Bare {}",
                "Empty.java", "package com.acme;\n/** */\npublic interface Empty {}",
                "Plain.java", "package com.acme;\n/** A port with no kind. */\npublic interface Plain {}",
                "Both.java", "package com.acme;\n/** <b>SPI:</b> one. <b>Provided:</b> other. */\npublic interface Both {}",
                "Twice.java", "package com.acme;\n/** <b>SPI:</b> one. <b>SPI:</b> again. */\npublic interface Twice {}"));

        assertThat(found).containsExactly(
                new PublicInterface("com.acme.Bare", StatedKind.NONE),
                new PublicInterface("com.acme.Both", StatedKind.CONFLICTING),
                new PublicInterface("com.acme.Empty", StatedKind.NONE),
                new PublicInterface("com.acme.Plain", StatedKind.NONE),
                new PublicInterface("com.acme.Twice", StatedKind.CONFLICTING));
    }

    @Test
    void GIVEN_the_paragraph_sits_in_a_comment_that_is_not_javadoc_WHEN_read_THEN_it_states_no_kind() {
        List<PublicInterface> found = read("package com.acme;\n/* <b>SPI:</b> a block comment. */\n"
                + "// <b>Provided:</b> a line comment.\npublic interface Port {}");

        assertThat(found).containsExactly(new PublicInterface("com.acme.Port", StatedKind.NONE));
    }

    @Test
    void GIVEN_annotations_between_the_javadoc_and_the_declaration_WHEN_read_THEN_the_javadoc_still_states_the_kind() {
        List<PublicInterface> found = read("package com.acme;\n" + SPI
                + "@FunctionalInterface\n@SuppressWarnings({\"a\", \"b\"})\npublic interface Mapper { void map(); }");

        assertThat(found).containsExactly(new PublicInterface("com.acme.Mapper", StatedKind.SPI));
    }

    @Test
    void GIVEN_the_same_sources_in_a_jar_and_on_disk_WHEN_read_THEN_both_give_the_same_interfaces(@TempDir Path dir)
            throws IOException {
        String source = "package com.acme;\n" + SPI + "public interface Source {\n" + PROVIDED + "    interface Listener {}\n}";
        Path file = Files.createDirectories(dir.resolve("src/com/acme")).resolve("Source.java");
        Files.writeString(file, source);
        File jar = dir.resolve("module-sources.jar").toFile();
        try (OutputStream out = Files.newOutputStream(jar.toPath()); ZipOutputStream zip = new ZipOutputStream(out)) {
            zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            zip.write("Manifest-Version: 1.0\n".getBytes(StandardCharsets.UTF_8));
            zip.putNextEntry(new ZipEntry("com/acme/Source.java"));
            zip.write(source.getBytes(StandardCharsets.UTF_8));
        }

        List<PublicInterface> fromJar = JavaInterfaces.inSourcesJar(jar);

        assertThat(fromJar).hasSize(2).isEqualTo(JavaInterfaces.inFiles(List.of(file.toFile())));
    }

    @Test
    void GIVEN_a_source_that_does_not_parse_WHEN_read_THEN_the_check_fails_naming_it() {
        assertThatThrownBy(() -> JavaInterfaces.parse(Map.of("Broken.java", "package com.acme;\npublic interface {")))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining("Broken.java");
    }
}
