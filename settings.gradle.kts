plugins {
    // Lets the Java toolchain auto-provision JDK 17 when it is not already installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "tandem"

// Modules live in a directory per role, but keep flat project paths (`:tandem-core`, not
// `:libs:tandem-core`): the project name is the artifactId, and task paths stay independent of the
// directory layout.
val modulesByDirectory = mapOf(
    "libs" to listOf(
        "tandem-bom",
        "tandem-core",
        "tandem-jdbc",
        "tandem-cloudevents",
        "tandem-kafka",
        "tandem-rabbitmq",
        "tandem-test",
        "tandem-spring-producer",
        "tandem-spring-relay",
        "tandem-micrometer",
        "tandem-tracing-otel",
        "tandem-admin",
    ),
    "apps" to listOf("tandem-relay"),
    "examples" to listOf("tandem-sample", "tandem-sample-spring"),
    "tools" to listOf("tandem-benchmark", "tandem-coverage"),
)

modulesByDirectory.forEach { (directory, modules) ->
    modules.forEach { module ->
        include(module)
        project(":$module").projectDir = file("$directory/$module")
    }
}
