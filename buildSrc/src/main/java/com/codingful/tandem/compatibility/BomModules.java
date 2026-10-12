package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.gradle.api.GradleException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * The modules a {@code tandem-bom} POM manages (LLD-api-stability §2.4): the baseline's BOM says which
 * modules the baseline release contains, and every one of them must stay managed by the current BOM.
 */
public final class BomModules {

    /** The gate kind of the declared breaks {@code checkBomModules} reads. */
    public static final String GATE = "bom";

    private BomModules() {
    }

    /** The artifact ids of the Tandem modules in the POM's {@code dependencyManagement}. */
    public static SortedSet<String> managed(File pom) {
        Element project;
        try {
            project = parser().parse(pom).getDocumentElement();
        } catch (IOException | SAXException e) {
            throw new GradleException("Cannot read the BOM " + pom, e);
        }
        SortedSet<String> modules = new TreeSet<>();
        for (Element management : children(project, "dependencyManagement")) {
            for (Element dependencies : children(management, "dependencies")) {
                for (Element dependency : children(dependencies, "dependency")) {
                    if (CompatibilityBaseline.GROUP.equals(text(dependency, "groupId"))) {
                        modules.add(text(dependency, "artifactId"));
                    }
                }
            }
        }
        return modules;
    }

    /**
     * What the BOMs' comparison makes fail: a module the baseline BOM manages and the current one no
     * longer does, unless declared, and a declaration no such module used.
     *
     * @param module       the BOM's own module name, as declarations name it
     * @param baseline     the modules the baseline BOM manages
     * @param current      the modules the current BOM manages
     * @param declarations the BOM's {@code bom} entries of the declared breaks
     * @return one line per problem; empty when the check passes
     */
    public static List<String> problems(String module, Set<String> baseline, Set<String> current,
            List<DeclaredBreaks.Entry> declarations) {
        Collection<String> gone = new TreeSet<>(baseline);
        gone.removeAll(current);
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(gone, declarations);
        List<String> problems = new ArrayList<>();
        for (String removed : matching.undeclared()) {
            problems.add(removed + " is managed by the baseline BOM and no longer by this one, which breaks every "
                    + "application declaring it without a version; if intended, declare it as '" + GATE + " | "
                    + module + " | " + removed + " | <release-notes line>'");
        }
        for (DeclaredBreaks.Entry stale : matching.stale()) {
            problems.add("Declared break matches no module the BOM stopped managing, remove it: " + stale);
        }
        return problems;
    }

    private static DocumentBuilder parser() {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            return factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Element> children(Element parent, String name) {
        List<Element> children = new ArrayList<>();
        for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element element && element.getTagName().equals(name)) {
                children.add(element);
            }
        }
        return children;
    }

    private static String text(Element parent, String name) {
        List<Element> elements = children(parent, name);
        return elements.isEmpty() ? "" : elements.get(0).getTextContent().strip();
    }
}
