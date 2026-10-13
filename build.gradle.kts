import com.codingful.tandem.compatibility.CheckInterfaceKinds
import com.codingful.tandem.compatibility.CheckJavaApi
import com.codingful.tandem.compatibility.CheckSpringMetadata
import com.codingful.tandem.compatibility.CompatibilityBaseline

plugins {
    // Applied per-subproject below; declared here so it is on the build classpath once.
    id("com.vanniktech.maven.publish") version "0.37.0" apply false
}

allprojects {
    group = "com.codingful"
    // The release workflow injects the tag version via the VERSION env var; default to a SNAPSHOT locally.
    version = System.getenv("VERSION") ?: "0.1.0-SNAPSHOT"
}

// The generated `libs` accessor only resolves in the root script scope, so capture the test
// dependencies here and reference them inside the subprojects block below.
val junitBom = libs.junit.bom
val junitJupiter = libs.junit.jupiter
val assertjCore = libs.assertj.core
val junitLauncher = libs.junit.platform.launcher

// Unpublished modules that configure their own plugins/toolchain/tests directly rather than the shared
// java-library/publishing convention below: tandem-sample is a Java 17 tutorial app; tandem-benchmark
// needs a newer JDK for virtual threads (LLD-benchmark §2); tandem-coverage is a build-only module that
// only produces the aggregated JaCoCo report (no Java sources, no artifact to publish); tandem-relay is
// the standalone relay application, distributed as a container image and an executable jar, never as a
// Maven artifact, and it runs on its own JDK and Spring Boot line (LLD-relay §3).
val unpublishedModules = setOf("tandem-sample", "tandem-sample-spring", "tandem-benchmark", "tandem-coverage",
        "tandem-relay")

// Modules that ARE libraries in every other respect — java-library convention, tests, coverage
// aggregation — but must not be published yet: an independently versioned module belongs here until its
// own release workflow exists (IMPLEMENTATION-PLAN-rabbitmq.md §1), and leaves it in the change that adds
// that workflow and excludes it from release.yml. The set gates only the two publishing blocks below,
// never the java-library convention.
//
// Why a set here rather than an exclusion in the release workflow: `release.yml` publishes every
// module that applies the publishing plugin, and a Maven Central version can never be deleted or
// overwritten. A workflow flag can be forgotten at tag time; a module that has no publishing task
// at all cannot be published by accident.
val notYetPublishedModules = setOf<String>()

// Published modules on a version of their own (today tandem-rabbitmq, LLD-rabbitmq §9): each keeps its own
// contract, so the compatibility gates, which compare with a release of the library, leave them out
// (LLD-api-stability §1).
val independentlyVersionedModules = setOf("tandem-rabbitmq")

// The published library modules of HLD-api-stability §3: the modules the compatibility gates check. The BOM
// carries no code; its one gate, checkBomModules, is registered in its own build script.
val libraryModules = subprojects.map { it.name }.toSet() - unpublishedModules - notYetPublishedModules -
        independentlyVersionedModules - "tandem-bom"

// The library modules whose jar carries Spring configuration metadata: each runs checkSpringMetadata
// (LLD-api-stability §7).
val springMetadataModules = setOf("tandem-spring-producer", "tandem-spring-relay", "tandem-admin")

// Independently versioned modules (today: tandem-rabbitmq) depend on their siblings by published
// coordinate with an explicit floor, because a project dependency would stamp their own version into
// the POM as the sibling's version (IMPLEMENTATION-PLAN-rabbitmq.md §8). During development those
// coordinates resolve back to the working tree, so the repository stays one buildable unit; published
// metadata keeps the declared floor. Applied in every project, not only in the connector, because a
// module depending on the connector (tandem-benchmark) inherits the coordinates transitively. The
// exempt configurations are the ones whose whole point is to resolve from Maven Central: the connector's
// floorTest classpath, the classpath of the relay application's pinned jar (LLD-relay §7.3), and the
// configurations that resolve the compatibility baseline (LLD-api-stability §2.2). A configuration missing
// from this set is substituted like any other, and the gate built on it then passes while testing the
// working tree.
val centralClasspaths = setOf("floorTestRuntimeClasspath", "pinnedRuntimeClasspath") +
        CompatibilityBaseline.CONFIGURATIONS
val siblingPaths = subprojects.filter { it.name !in unpublishedModules }.associate { it.name to it.path }

// Test tasks that start containers (PostgreSQL, Kafka, RabbitMQ) through Testcontainers. The build runs
// projects in parallel, and a change deep in the dependency graph invalidates every module's integration
// tests at once: left unlimited, that is one broker per module starting together, more than a developer
// machine's Docker reliably starts, and the run fails on a container that never came up rather than on a
// test. The shared service below is a semaphore: Gradle queues the tasks that declare it beyond the
// limit. Unit tests are untouched and stay fully parallel.
//
// A Docker-bound test task under a new name must be added here; nothing fails if it is not, it simply
// starts its containers outside the limit.
val dockerBoundTestTasks = setOf("integrationTest", "floorTest", "pinnedTest", "noKafkaTest", "schemaCompatTest")

// Reference files (LLD-api-stability §8.1): `./gradlew test -PupdateReferenceFiles` runs the reference tests in
// write mode, each rewriting its file under src/test/resources/reference instead of comparing with it. CI always
// compares, so the property is refused there. Any value but `false` turns write mode on, the bare flag included.
val updateReferenceFiles = providers.gradleProperty("updateReferenceFiles").map { it != "false" }.getOrElse(false)
if (updateReferenceFiles && providers.environmentVariable("CI").isPresent) {
    throw GradleException("-PupdateReferenceFiles is refused when the CI environment variable is set: a CI run " +
        "compares the reference files and never rewrites them (LLD-api-stability §8.1).")
}

abstract class DockerSlots : BuildService<BuildServiceParameters.None>

val dockerSlots = gradle.sharedServices.registerIfAbsent("dockerSlots", DockerSlots::class) {
    maxParallelUsages.set(3)
}

// Runs every gate that reads the compatibility baseline (LLD-api-stability §9.2), for the dry run of §10 and a
// quick run after the baseline moves: `./gradlew checkCompatibility -PtandemBaseline=<release> --continue`.
// Each gate adds itself here, and to its module's `check`, as it is registered
// (CompatibilityBaseline.registerGate), so this task keeps no list of them.
tasks.register(CompatibilityBaseline.CHECK_COMPATIBILITY) {
    group = "verification"
    description = "Runs every compatibility gate that compares the build with the tandemBaseline release."
}

subprojects {
    tasks.withType<Test>().matching { it.name in dockerBoundTestTasks }.configureEach {
        usesService(dockerSlots)
    }

    // Every Test task, so a reference test is in the same mode whichever task runs it. The mode is an input, so
    // neither an up-to-date check nor the build cache hands one mode the result of the other; in write mode the
    // task's state is not tracked at all, so it always runs and always rewrites what differs.
    tasks.withType<Test>().configureEach {
        systemProperty("tandem.updateReferenceFiles", updateReferenceFiles)
        inputs.property("updateReferenceFiles", updateReferenceFiles)
        if (updateReferenceFiles) {
            doNotTrackState("-PupdateReferenceFiles rewrites the reference files in the source tree")
        }
    }

    configurations.matching { it.name !in centralClasspaths }.configureEach {
        resolutionStrategy.dependencySubstitution {
            siblingPaths.forEach { (name, path) ->
                if (name == "tandem-bom") {
                    // A BOM is requested as a platform and must be answered as one: a plain project
                    // substitution asks tandem-bom for a library variant, which it does not have.
                    substitute(platform(module("com.codingful:$name"))).using(platform(project(path)))
                } else {
                    substitute(module("com.codingful:$name")).using(project(path))
                }
            }
        }
    }

    if (name !in unpublishedModules && name !in notYetPublishedModules) {
        apply(plugin = "com.vanniktech.maven.publish")

        // Build the javadoc jar the release publishes as part of `check`, so a javadoc error (a dangling
        // {@link}, say) fails the PR instead of the tag. Only errors fail it; doclint warnings stay
        // warnings (AGENTS.md, Javadoc rule 6). Matched by name because the publishing plugin registers
        // the task later in configuration, and tandem-bom has none.
        tasks.matching { it.name == "check" }.configureEach {
            dependsOn(tasks.matching { it.name == "plainJavadocJar" })
        }
    }

    // Every module except the BOM is a Java library on a Java 17 toolchain with JUnit 6 + AssertJ.
    if (name != "tandem-bom" && name !in unpublishedModules) {
        apply(plugin = "java-library")
        apply(plugin = "jacoco")

        configure<JavaPluginExtension> {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(17))
            }
        }

        tasks.withType<JavaCompile>().configureEach {
            options.compilerArgs.add("-Xlint:all")
        }

        // A stable module name for an application on the module path, named after the module's root
        // package (tandem-spring-relay: com.codingful.tandem.spring.relay). Part of the frozen names of
        // HLD-api-stability §2; without it the name would derive from the jar's file name.
        val automaticModuleName = "com.codingful." + name.replace('-', '.')
        val rootPackageDir = layout.projectDirectory.dir("src/main/java/" + automaticModuleName.replace('.', '/'))
        tasks.named<Jar>("jar") {
            manifest {
                attributes("Automatic-Module-Name" to automaticModuleName)
            }
            doFirst {
                check(rootPackageDir.asFile.isDirectory) {
                    "Automatic-Module-Name $automaticModuleName is not a package of this module: a module " +
                        "whose root package departs from its name needs that name set explicitly"
                }
            }
        }

        dependencies {
            "testImplementation"(platform(junitBom))
            "testImplementation"(junitJupiter)
            "testImplementation"(assertjCore)
            "testRuntimeOnly"(junitLauncher)
        }

        // The PostgreSQL image the Testcontainers tests start (TandemTestContainer.postgresImage()).
        // Forwarded to the test JVM so the documented system property reaches it, and declared as a
        // task input so it reaches the cache key: the build cache is on, Test tasks are cacheable, and
        // without this a run against one major would be handed the cached result of a run against
        // another. The postgres-majors matrix would then go green having started no container at all.
        val postgresImage = providers.systemProperty("tandem.test.postgres.image")
                .orElse(providers.environmentVariable("TANDEM_TEST_POSTGRES_IMAGE"))

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            inputs.property("tandemPostgresImage", postgresImage.orElse("default"))
            postgresImage.orNull?.let { systemProperty("tandem.test.postgres.image", it) }
        }

        // `test` runs the Docker-free unit tests; `integrationTest` runs the @Tag("integration")
        // Testcontainers tests. `check` runs both; skip integration locally with
        // `./gradlew check -x integrationTest` (or just run `./gradlew test`).
        val mainSourceSet = the<SourceSetContainer>()["main"]
        val testSourceSet = the<SourceSetContainer>()["test"]
        val integrationTest = tasks.register<Test>("integrationTest") {
            description = "Runs @Tag(\"integration\") tests (require Docker)."
            group = "verification"
            testClassesDirs = testSourceSet.output.classesDirs
            classpath = testSourceSet.runtimeClasspath
            useJUnitPlatform { includeTags("integration") }
            shouldRunAfter(tasks.named("test"))
        }

        // Three JaCoCo reports per module:
        //   jacocoTestReport            — unit tests only       (default, always runs after `test`)
        //   jacocoIntegrationTestReport — integration tests only (runs after `integrationTest`)
        //   jacocoMergedReport          — unit + integration     (runs after `check`; upload this to Codecov)
        //
        // fileTree collects whichever .exec files exist, so that a skipped phase (e.g. -x integrationTest)
        // does not cause the report to fail. dependsOn declares the task-output relationship required by
        // Gradle's incremental-build validation; it also means running a report task directly will
        // run the corresponding test phase first.
        val jacocoExecDir = layout.buildDirectory.dir("jacoco")

        val jacocoIntegrationTestReport = tasks.register<JacocoReport>("jacocoIntegrationTestReport") {
            description = "JaCoCo report for @Tag(\"integration\") tests."
            group = "verification"
            dependsOn(integrationTest)
            executionData(fileTree(jacocoExecDir) { include("integrationTest.exec") })
            sourceSets(mainSourceSet)
        }

        val jacocoMergedReport = tasks.register<JacocoReport>("jacocoMergedReport") {
            description = "Merged JaCoCo report (every test phase) — upload this to Codecov."
            group = "verification"
            // All Test tasks, not a hand-listed set: executionData globs *.exec below, so a module that
            // adds a phase (e.g. the Spring modules' bootFourTest) is covered without touching this.
            dependsOn(tasks.withType<Test>())
            executionData(fileTree(jacocoExecDir) { include("*.exec") })
            sourceSets(mainSourceSet)
        }

        tasks.named<Test>("test") {
            useJUnitPlatform { excludeTags("integration") }
            finalizedBy(tasks.named("jacocoTestReport"))
        }

        tasks.named("integrationTest") {
            finalizedBy(jacocoIntegrationTestReport)
        }

        tasks.named("check") {
            dependsOn(integrationTest)
            finalizedBy(jacocoMergedReport)
        }

        // All JacocoReport tasks emit XML + HTML; Codecov consumes jacocoMergedReport.xml.
        tasks.withType<JacocoReport>().configureEach {
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }
    }

    // The baseline configurations, and the gates of HLD-api-stability §5 that every library module runs
    // (LLD-api-stability §2.2, §4). Each gate joins `check` and checkCompatibility as it is registered.
    if (name in libraryModules) {
        CompatibilityBaseline.registerLibrary(project)
        val interfaceKinds = CheckInterfaceKinds.register(project)
        CheckJavaApi.register(project, libraryModules, interfaceKinds)
        if (name in springMetadataModules) {
            CheckSpringMetadata.register(project, springMetadataModules)
            // The configuration processor writes a property's description only when it compiles the class
            // declaring it, so an incremental compilation leaves the others without one. A full compilation
            // keeps the metadata of every jar complete (LLD-api-stability §7). The flag is not part of the
            // build cache key, so the input below keys these compilations apart from any an incremental
            // compilation stored, which the cache would otherwise hand back for the same sources.
            tasks.named<JavaCompile>("compileJava") {
                options.isIncremental = false
                inputs.property("fullCompilation", true)
            }
        }
    }

    // Common Maven Central / POM metadata. Per-module name+description come from the subproject.
    if (name !in unpublishedModules && name !in notYetPublishedModules)
        configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        publishToMavenCentral()
        signAllPublications()
        pom {
            name.set(project.name)
            description.set(project.description ?: project.name)
            url.set("https://github.com/alirux/tandem-transactional-outbox-kafka")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    id.set("codingful")
                    name.set("Codingful")
                }
            }
            scm {
                url.set("https://github.com/alirux/tandem-transactional-outbox-kafka")
                connection.set("scm:git:https://github.com/alirux/tandem-transactional-outbox-kafka.git")
                developerConnection.set("scm:git:ssh://git@github.com/alirux/tandem-transactional-outbox-kafka.git")
            }
        }
    }
}
