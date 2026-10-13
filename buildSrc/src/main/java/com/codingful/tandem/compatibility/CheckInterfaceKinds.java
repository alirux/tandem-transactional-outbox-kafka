package com.codingful.tandem.compatibility;

import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkInterfaceKinds} (LLD-api-stability §4.3): every public interface of the module states in its
 * javadoc whether it is SPI or provided. Writes the binary names of the provided ones, which the Java API gate
 * reads. It needs no baseline: the kind is the one the current sources state.
 */
@CacheableTask
public abstract class CheckInterfaceKinds extends DefaultTask {

    @Input
    public abstract Property<String> getModuleName();

    /** The module's Java sources. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSources();

    /** One binary name per line, sorted, of each interface that states it is provided. */
    @OutputFile
    public abstract RegularFileProperty getProvidedInterfaces();

    /** Registers the check in a published library module, as part of its {@code check}. */
    public static TaskProvider<CheckInterfaceKinds> register(Project project) {
        TaskProvider<CheckInterfaceKinds> check = project.getTasks().register("checkInterfaceKinds",
                CheckInterfaceKinds.class, task -> {
            task.setGroup("verification");
            task.setDescription("Checks that every public interface states its kind, SPI or provided.");
            task.getModuleName().set(project.getName());
            task.getSources().from(project.getExtensions().getByType(SourceSetContainer.class).getByName("main")
                    .getJava());
            task.getProvidedInterfaces().set(project.getLayout().getBuildDirectory()
                    .file("compatibility/provided-interfaces.txt"));
        });
        project.getTasks().named("check").configure(all -> all.dependsOn(check));
        return check;
    }

    @TaskAction
    public void check() {
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(JavaInterfaces.inFiles(getSources().getFiles()));
        if (!verdict.problems().isEmpty()) {
            throw new GradleException("Interface kinds of " + getModuleName().get() + " (LLD-api-stability §4.3):\n  "
                    + String.join("\n  ", verdict.problems()));
        }
        BaselineGate.write(getProvidedInterfaces().get().getAsFile().toPath(),
                verdict.provided().stream().map(name -> name + "\n").collect(Collectors.joining()));
    }
}
