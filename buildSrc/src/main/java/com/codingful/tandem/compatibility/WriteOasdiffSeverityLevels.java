package com.codingful.tandem.compatibility;

import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.DisableCachingByDefault;

/**
 * {@code writeOasdiffSeverityLevels} (LLD-api-stability §6.2): writes the policy file the first time, every
 * rule the pinned image classes as {@code info} at that level, for the policy to be applied by editing it.
 * It refuses to overwrite the file: once it exists, {@code checkOpenApi} names each rule it lacks.
 */
@DisableCachingByDefault(because = "It writes a file of the source tree once")
public abstract class WriteOasdiffSeverityLevels extends DefaultTask {

    /** The oasdiff image, by tag and digest. */
    @Input
    public abstract Property<String> getImage();

    /** The repository the container mounts. */
    @Internal
    public abstract DirectoryProperty getRepository();

    /** The committed policy, written only when it does not exist. */
    @Internal
    public abstract RegularFileProperty getPolicy();

    /** Where the catalog run's output is kept. */
    @Internal
    public abstract DirectoryProperty getReports();

    @TaskAction
    public void write() {
        Path policy = getPolicy().get().getAsFile().toPath();
        if (Files.exists(policy)) {
            throw new GradleException(policy + " exists, and is edited by hand: checkOpenApi names each info rule "
                    + "of the pinned image it lacks (LLD-api-stability §6.2).");
        }
        Path reports = getReports().get().getAsFile().toPath();
        Oasdiff oasdiff = new Oasdiff(getImage().get(), getRepository().get().getAsFile().toPath(), reports);
        Oasdiff.write(policy, SeverityPolicy.initial(SeverityPolicy.catalog(
                oasdiff.run("catalog", reports, "checks", "changelog", "--format", "json"))));
        getLogger().lifecycle("Severity policy written from the oasdiff catalog, to classify by hand file:{}", policy);
    }
}
