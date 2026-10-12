package com.codingful.tandem.compatibility;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.gradle.api.GradleException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * The changelog the baseline release shipped, compared with the working tree's (LLD-api-stability §5.1). An
 * operator's {@code DATABASECHANGELOG} holds the checksum of every changeset it applied, so a shipped
 * changeset never changes and no declaration accepts a difference: every changeset file of the baseline jar
 * is still there with the same bytes, and the baseline master's includes are, in order, the first ones of
 * the current master.
 */
public final class ShippedChangesets {

    /** Where {@code tandem-jdbc}'s jar carries the changelog: a published path (LLD-jdbc §6). */
    public static final String JAR_DIRECTORY = "tandem/schema/postgres/changelog/";

    public static final String MASTER = "db.changelog-master.xml";

    /** A schema version's changeset file, {@code v<n>-<name>.sql}. */
    private static final Pattern CHANGESET = Pattern.compile("v\\d+-[^/]+\\.sql");

    /**
     * How the two changelogs compared.
     *
     * @param shipped  the changeset files the baseline jar carries, sorted
     * @param problems one line per difference; empty when the check passes
     */
    public record Comparison(List<String> shipped, List<String> problems) {
    }

    private ShippedChangesets() {
    }

    /**
     * @param baselineJar      {@code tandem-jdbc}'s jar at the baseline release
     * @param currentChangelog the working tree's {@code schema/postgres/changelog} directory
     */
    public static Comparison compare(File baselineJar, Path currentChangelog) {
        Map<String, byte[]> shipped = new TreeMap<>();
        byte[] baselineMaster = null;
        try (ZipFile jar = new ZipFile(baselineJar)) {
            for (Enumeration<? extends ZipEntry> entries = jar.entries(); entries.hasMoreElements(); ) {
                ZipEntry entry = entries.nextElement();
                if (!entry.getName().startsWith(JAR_DIRECTORY)) {
                    continue;
                }
                String name = entry.getName().substring(JAR_DIRECTORY.length());
                if (CHANGESET.matcher(name).matches()) {
                    shipped.put(name, read(jar, entry));
                } else if (name.equals(MASTER)) {
                    baselineMaster = read(jar, entry);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the baseline jar " + baselineJar, e);
        }
        List<String> problems = new ArrayList<>();
        shipped.forEach((name, bytes) -> {
            Path current = currentChangelog.resolve(name);
            if (!Files.isRegularFile(current)) {
                problems.add(name + " was shipped by the baseline and is missing");
            } else if (!Arrays.equals(bytes, readAllBytes(current))) {
                problems.add(name + " differs from the file the baseline shipped");
            }
        });
        Path currentMaster = currentChangelog.resolve(MASTER);
        if (baselineMaster == null) {
            problems.add("the baseline jar carries no " + JAR_DIRECTORY + MASTER + ", the changelog's published path");
        } else if (!Files.isRegularFile(currentMaster)) {
            problems.add(MASTER + " is missing");
        } else {
            includesProblem(includes(baselineMaster), includes(readAllBytes(currentMaster))).ifPresent(problems::add);
        }
        return new Comparison(List.copyOf(shipped.keySet()), problems);
    }

    /** The first baseline include the current master does not repeat at the same position, if any. */
    private static Optional<String> includesProblem(List<String> baseline, List<String> current) {
        for (int i = 0; i < baseline.size(); i++) {
            if (i >= current.size() || !baseline.get(i).equals(current.get(i))) {
                return Optional.of(MASTER + " no longer starts with the baseline's includes, in order: entry "
                        + (i + 1) + " was <" + baseline.get(i) + ">, is now "
                        + (i < current.size() ? "<" + current.get(i) + ">" : "absent"));
            }
        }
        return Optional.empty();
    }

    /**
     * The master's {@code include} and {@code includeAll} elements in document order, each rendered with
     * all its attributes sorted by name: an attribute such as {@code relativeToChangelogFile} decides the path
     * Liquibase records for the included changesets, so it is part of the entry.
     */
    static List<String> includes(byte[] master) {
        Element root;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            root = factory.newDocumentBuilder().parse(new ByteArrayInputStream(master)).getDocumentElement();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            throw new GradleException("Cannot read the changelog master " + MASTER, e);
        }
        List<String> includes = new ArrayList<>();
        for (Node child = root.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element element
                    && (element.getLocalName().equals("include") || element.getLocalName().equals("includeAll"))) {
                Map<String, String> attributes = new TreeMap<>();
                NamedNodeMap all = element.getAttributes();
                for (int a = 0; a < all.getLength(); a++) {
                    attributes.put(all.item(a).getNodeName(), all.item(a).getNodeValue());
                }
                StringBuilder rendered = new StringBuilder(element.getLocalName());
                attributes.forEach((name, value) -> rendered.append(' ').append(name).append("=\"").append(value)
                        .append('"'));
                includes.add(rendered.toString());
            }
        }
        return includes;
    }

    private static byte[] read(ZipFile jar, ZipEntry entry) throws IOException {
        try (InputStream in = jar.getInputStream(entry)) {
            return in.readAllBytes();
        }
    }

    private static byte[] readAllBytes(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
