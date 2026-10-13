package com.codingful.tandem.compatibility;

import japicmp.model.AccessModifier;
import japicmp.model.JApiBehavior;
import japicmp.model.JApiChangeStatus;
import japicmp.model.JApiClass;
import japicmp.model.JApiCompatibility;
import japicmp.model.JApiCompatibilityChange;
import japicmp.model.JApiCompatibilityChangeType;
import japicmp.model.JApiConstructor;
import japicmp.model.JApiField;
import japicmp.model.JApiImplementedInterface;
import japicmp.model.JApiMethod;
import japicmp.model.JApiParameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javassist.CtBehavior;

/**
 * Judges the incompatible changes japicmp found in one module (LLD-api-stability §4.2): every change
 * japicmp marks binary or source incompatible is a break, except a method added to a provided interface,
 * and a break is accepted only when the declared breaks name it.
 *
 * <p>A break is named by its item, the string a {@code japicmp} declaration carries: a type by its binary
 * name ({@code com.acme.Store$Listener}), a method or constructor by the name the report prints for it
 * ({@code com.acme.Store.flush(java.lang.String)}, {@code com.acme.Store(int)}), a field by its type and
 * name ({@code com.acme.Headers.TRACE}), an implemented interface as
 * {@code <type> implements <interface>}. A type that is removed, or less accessible than protected, is one
 * break: its members go with it and are not judged apart. A type japicmp marks incompatible only through
 * its members has no item of its own, and is accepted when each of those members is.
 *
 * <p>The judge sees elements in the order the rich report visits them, a type before its members, and
 * remembers every item it reported, so that {@link #outcome()} can tell the declarations no break used.
 */
final class JavaApiBreaks {

    /** The gate kind of the declared breaks {@code checkJavaApi} reads. */
    static final String GATE = "japicmp";

    /** How one element of the comparison was judged. */
    enum Verdict {
        /** Compatible, or a member of a type that is gone: nothing to report. */
        COMPATIBLE,
        /** Its only incompatible change is a method added to a provided interface (HLD-api-stability §4.1). */
        PROVIDED,
        /** A break the declared breaks name. */
        DECLARED,
        /** A break the declared breaks do not name. */
        UNDECLARED,
        /** A type incompatible only through members, every one of them accepted. */
        MEMBERS_ACCEPTED,
        /** A type incompatible only through members, one or more of them not accepted. */
        MEMBERS_UNDECLARED
    }

    /**
     * @param verdict the verdict
     * @param item    the item a declaration names the break by; empty when the element has no break of its own
     * @param changes the incompatible changes behind the verdict, by japicmp's names
     * @param entry   the declaration that accepted the break; {@code null} unless {@link Verdict#DECLARED}
     */
    record Judgement(Verdict verdict, String item, List<JApiCompatibilityChangeType> changes,
            DeclaredBreaks.Entry entry) {

        static final Judgement COMPATIBLE = new Judgement(Verdict.COMPATIBLE, "", List.of(), null);
    }

    /**
     * What a comparison concluded, as {@code checkJavaApi} reports it.
     *
     * @param accepted the declarations that accepted a break, each with its release-notes line
     * @param problems what fails the gate, one line each: undeclared breaks and stale declarations
     */
    record Outcome(List<String> accepted, List<String> problems) {

        private static final String ACCEPTED = "accepted ";
        private static final String PROBLEM = "problem ";

        void write(Path file) {
            String text = Stream.concat(accepted.stream().map(line -> ACCEPTED + line),
                            problems.stream().map(line -> PROBLEM + line))
                    .map(line -> line + "\n")
                    .collect(Collectors.joining());
            try {
                Files.createDirectories(file.getParent());
                Files.writeString(file, text, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        static Outcome read(Path file) {
            List<String> lines;
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            List<String> accepted = new ArrayList<>();
            List<String> problems = new ArrayList<>();
            for (String line : lines) {
                if (line.startsWith(ACCEPTED)) {
                    accepted.add(line.substring(ACCEPTED.length()));
                } else {
                    problems.add(line.substring(PROBLEM.length()));
                }
            }
            return new Outcome(accepted, problems);
        }
    }

    private final String module;
    private final Set<String> providedInterfaces;
    private final List<DeclaredBreaks.Entry> declarations;
    private final Map<String, DeclaredBreaks.Entry> declared = new LinkedHashMap<>();
    private final Map<JApiCompatibility, Judgement> judged = new IdentityHashMap<>();
    private final Map<String, List<JApiCompatibilityChangeType>> reported = new LinkedHashMap<>();

    /**
     * @param module             the module compared, as declarations name it
     * @param providedInterfaces the binary names of the module's interfaces whose kind for the gate is
     *                           provided (§4.3)
     * @param declarations       the module's {@code japicmp} entries of the declared breaks
     */
    JavaApiBreaks(String module, Set<String> providedInterfaces, List<DeclaredBreaks.Entry> declarations) {
        this.module = module;
        this.providedInterfaces = Set.copyOf(providedInterfaces);
        this.declarations = List.copyOf(declarations);
        declarations.forEach(entry -> declared.put(entry.item(), entry));
    }

    /**
     * Judges a type and, with it, every member it lists, which {@link #judgement} then returns.
     */
    Judgement type(JApiClass type) {
        List<JApiCompatibility> members = new ArrayList<>();
        members.addAll(type.getFields());
        members.addAll(type.getMethods());
        members.addAll(type.getConstructors());
        members.addAll(type.getInterfaces());
        Judgement verdict;
        if (gone(type)) {
            // Removed, or out of the API: the type's own change is the break, and its members go with it.
            verdict = judge(type.getFullyQualifiedName(), incompatible(type), List.of());
            members.forEach(member -> judged.put(member, Judgement.COMPATIBLE));
        } else {
            List<Judgement> memberVerdicts = new ArrayList<>();
            for (JApiCompatibility member : members) {
                Judgement memberVerdict = judge(item(type, member), incompatible(member),
                        member instanceof JApiMethod && providedInterfaces.contains(type.getFullyQualifiedName())
                                ? List.of(JApiCompatibilityChangeType.METHOD_ADDED_TO_INTERFACE) : List.of());
                judged.put(member, memberVerdict);
                memberVerdicts.add(memberVerdict);
            }
            List<JApiCompatibilityChangeType> own = incompatible(type);
            if (!own.isEmpty()) {
                verdict = judge(type.getFullyQualifiedName(), own, List.of());
            } else if (memberVerdicts.stream().anyMatch(member -> member.verdict() == Verdict.UNDECLARED)) {
                verdict = new Judgement(Verdict.MEMBERS_UNDECLARED, "", List.of(), null);
            } else if (memberVerdicts.stream().anyMatch(member -> member.verdict() != Verdict.COMPATIBLE)) {
                verdict = new Judgement(Verdict.MEMBERS_ACCEPTED, "", List.of(), null);
            } else {
                verdict = Judgement.COMPATIBLE;
            }
        }
        judged.put(type, verdict);
        return verdict;
    }

    /**
     * The judgement of an element: a type's, or a member's once its type has been judged.
     *
     * @throws NullPointerException for a member whose type was not judged first
     */
    Judgement judgement(JApiCompatibility element) {
        if (element instanceof JApiClass type) {
            return type(type);
        }
        return Objects.requireNonNull(judged.get(element), () -> "A member was judged before its type: " + element);
    }

    /**
     * What the comparison concluded: the breaks no declaration names, and the declarations no break used
     * (LLD-api-stability §3, rule 2).
     */
    Outcome outcome() {
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(reported.keySet(), declarations);
        List<String> problems = new ArrayList<>();
        for (String item : matching.undeclared()) {
            problems.add(item + ": " + names(reported.get(item)) + "; if intended, declare it as '" + declaration(item)
                    + "'");
        }
        for (DeclaredBreaks.Entry stale : matching.stale()) {
            problems.add("Declared break matches no incompatible change, remove it: " + stale);
        }
        List<String> accepted = matching.accepted().values().stream()
                .map(entry -> entry + ", release notes: " + entry.releaseNote())
                .toList();
        return new Outcome(accepted, problems);
    }

    /** The declaration that would accept a break of {@code item}, with a placeholder for its release-notes line. */
    String declaration(String item) {
        return DeclaredBreaks.declaration(GATE, module, item);
    }

    static String names(Collection<JApiCompatibilityChangeType> changes) {
        return changes.stream().map(Enum::name).collect(Collectors.joining(", "));
    }

    private Judgement judge(String item, List<JApiCompatibilityChangeType> changes,
            List<JApiCompatibilityChangeType> allowed) {
        List<JApiCompatibilityChangeType> breaking = changes.stream()
                .filter(change -> !allowed.contains(change))
                .toList();
        if (breaking.isEmpty()) {
            return changes.isEmpty() ? Judgement.COMPATIBLE : new Judgement(Verdict.PROVIDED, "", changes, null);
        }
        reported.putIfAbsent(item, breaking);
        DeclaredBreaks.Entry entry = declared.get(item);
        return entry == null ? new Judgement(Verdict.UNDECLARED, item, breaking, null)
                : new Judgement(Verdict.DECLARED, item, breaking, entry);
    }

    /** Removed, or now less accessible than protected: no longer API, whatever happened to its members. */
    private static boolean gone(JApiClass type) {
        return type.getChangeStatus() == JApiChangeStatus.REMOVED || type.getAccessModifier().getNewModifier()
                .map(access -> access.getLevel() < AccessModifier.PROTECTED.getLevel())
                .orElse(false);
    }

    /**
     * The element's own incompatible changes, including those japicmp records on the parts the rich report
     * does not visit apart: a type's superclass, a method's return type and parameters.
     */
    private static List<JApiCompatibilityChangeType> incompatible(JApiCompatibility element) {
        List<JApiCompatibilityChange> changes = new ArrayList<>(element.getCompatibilityChanges());
        if (element instanceof JApiClass type) {
            changes.addAll(type.getSuperclass().getCompatibilityChanges());
        }
        if (element instanceof JApiMethod method) {
            changes.addAll(method.getReturnType().getCompatibilityChanges());
        }
        if (element instanceof JApiBehavior behavior) {
            for (JApiParameter parameter : behavior.getParameters()) {
                changes.addAll(parameter.getCompatibilityChanges());
            }
        }
        return changes.stream()
                .filter(change -> !change.isBinaryCompatible() || !change.isSourceCompatible())
                .map(JApiCompatibilityChange::getType)
                .distinct()
                .toList();
    }

    /** The item a declaration names a member's break by, the name the report prints for it. */
    static String item(JApiClass type, JApiCompatibility member) {
        if (member instanceof JApiMethod method) {
            return method.getNewMethod().or(method::getOldMethod).map(CtBehavior::getLongName).orElseThrow();
        }
        if (member instanceof JApiConstructor constructor) {
            return constructor.getNewConstructor().or(constructor::getOldConstructor).map(CtBehavior::getLongName)
                    .orElseThrow();
        }
        if (member instanceof JApiField field) {
            return type.getFullyQualifiedName() + "." + field.getName();
        }
        JApiImplementedInterface implemented = (JApiImplementedInterface) member;
        return type.getFullyQualifiedName() + " implements " + implemented.getFullyQualifiedName();
    }
}
