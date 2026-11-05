// Mutation testing with PIT, kept out of the build on purpose: it is an occasional audit of the pure-logic
// modules, not a gate. Nothing in the build applies it; this init script does, only when passed:
//
//   ./gradlew --init-script tools/pitest.init.gradle.kts :tandem-core:pitest
//
// Reports land in <module>/build/reports/pitest/ (HTML + XML). A surviving mutant is a question, not a
// verdict: read the code and the tests covering it before writing a test, since many are equivalent.
// PIT runs only the module's own unit tests, so code exercised by integration tests or by other modules
// shows as NO_COVERAGE.

import info.solidsoft.gradle.pitest.PitestPlugin
import info.solidsoft.gradle.pitest.PitestPluginExtension

initscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath("info.solidsoft.gradle.pitest:gradle-pitest-plugin:1.19.0")
    }
}

// Per module: the classes to mutate, and what to leave out of the run.
data class PitTarget(
    val targetClasses: Set<String>,
    val excludedClasses: Set<String> = emptySet(),
    val excludedTestClasses: Set<String> = emptySet(),
)

val targets = mapOf(
    "tandem-core" to PitTarget(setOf("com.codingful.tandem.*")),
    "tandem-jdbc" to PitTarget(
        targetClasses = setOf("com.codingful.tandem.jdbc.*"),
        // WorkerPool's concurrent lifecycle: its mutants hang the tests until the timeout, for no signal.
        excludedClasses = setOf("com.codingful.tandem.jdbc.WorkerPool", "com.codingful.tandem.jdbc.WorkerPool$*"),
        excludedTestClasses = setOf(
            "com.codingful.tandem.jdbc.WorkerPoolTest",
            "com.codingful.tandem.jdbc.VirtualThreadFootprintTest",
        ),
    ),
    "tandem-test" to PitTarget(
        targetClasses = setOf("com.codingful.tandem.test.*"),
        // Testcontainers plumbing: no logic of its own to mutate.
        excludedClasses = setOf(
            "com.codingful.tandem.test.TandemTestContainer*",
            "com.codingful.tandem.test.DriverManagerDataSource*",
        ),
    ),
)

allprojects {
    val target = targets[name] ?: return@allprojects
    pluginManager.withPlugin("java") {
        apply<PitestPlugin>()
        configure<PitestPluginExtension> {
            pitestVersion.set("1.30.1")
            junit5PluginVersion.set("1.2.3")
            targetClasses.set(target.targetClasses)
            excludedClasses.set(target.excludedClasses)
            excludedTestClasses.set(target.excludedTestClasses)
            // The tag the Testcontainers suites carry (some inherit it from a base class).
            excludedGroups.set(setOf("integration"))
            mutators.set(setOf("STRONGER"))
            threads.set(Runtime.getRuntime().availableProcessors())
            timeoutConstInMillis.set(2000)
            timestampedReports.set(false)
            outputFormats.set(setOf("HTML", "XML"))
        }
    }
}
