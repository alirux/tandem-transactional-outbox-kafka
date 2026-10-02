package com.codingful.tandem.test.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks that the conditions on an autoconfiguration's bean methods can be read in an application that
 * does not have the optional libraries those conditions are about (LLD-spring-config §1.1).
 *
 * <p>A condition written with a class literal, {@code @ConditionalOnClass(Tracer.class)}, is evaluated
 * correctly without the library: Spring reads it from the class file. But Spring also reads the
 * annotations of every method of every bean reflectively, for unrelated reasons, and a class literal
 * naming an absent type cannot be read that way. The context still starts, and every start logs a
 * {@code WARN} per such annotation. Naming the type as a string ({@code name = "..."},
 * {@code type = "..."}) leaves nothing to load.
 *
 * <p>No test on the module's own classpath can see this, since every optional library is there. This
 * helper re-defines the configuration class in a loader that refuses the given packages, which is what
 * an application without those libraries looks like to that one class, and reads every annotation
 * attribute of every method. A test fixture of this repository, never published.
 */
public final class OptionalLibraryConditions {

    private OptionalLibraryConditions() {
    }

    /**
     * @param configuration  the autoconfiguration whose bean methods are inspected
     * @param absentPackages package prefixes of the libraries the application does not have, each ending
     *                       with a dot ({@code "io.micrometer.tracing."})
     * @return one line per annotation attribute that cannot be read without those libraries; empty when
     *         every condition is readable
     */
    public static List<String> unreadableWithout(Class<?> configuration, String... absentPackages) {
        Class<?> withoutLibraries;
        try {
            withoutLibraries = new HidingLoader(configuration, absentPackages).loadClass(configuration.getName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        List<String> unreadable = new ArrayList<>();
        for (Method method : withoutLibraries.getDeclaredMethods()) {
            for (Annotation annotation : method.getDeclaredAnnotations()) {
                for (Method attribute : annotation.annotationType().getDeclaredMethods()) {
                    try {
                        attribute.invoke(annotation);
                    } catch (InvocationTargetException e) {
                        unreadable.add(method.getName() + " @" + annotation.annotationType().getSimpleName() + "."
                                + attribute.getName() + ": " + e.getCause());
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
        return unreadable;
    }

    /** Defines the one configuration class itself, so the types its annotations name resolve through it. */
    private static final class HidingLoader extends ClassLoader {

        private final Class<?> configuration;
        private final String[] absentPackages;

        HidingLoader(Class<?> configuration, String[] absentPackages) {
            super(configuration.getClassLoader());
            this.configuration = configuration;
            this.absentPackages = absentPackages;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            for (String absent : absentPackages) {
                if (name.startsWith(absent)) {
                    throw new ClassNotFoundException(name);
                }
            }
            if (name.equals(configuration.getName())) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> loaded = findLoadedClass(name);
                    return loaded != null ? loaded : define(name);
                }
            }
            return super.loadClass(name, resolve);
        }

        private Class<?> define(String name) {
            String resource = name.replace('.', '/') + ".class";
            try (InputStream in = configuration.getClassLoader().getResourceAsStream(resource)) {
                byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
