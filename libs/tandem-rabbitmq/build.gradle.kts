description = "Tandem RabbitMQ connector — OutboxDispatcher over AMQP 0.9.1 (CloudEvents binary binding)"

// This module carries its own version and its own release workflow (rabbitmq-release.yml, tags
// rabbitmq-v*), independent of the library's VERSION (LLD-rabbitmq §9). Unset outside a release, and
// then the publish guard below refuses to upload, so a manual run cannot push a snapshot to Central.
val rabbitmqVersion: String? = System.getenv("RABBITMQ_VERSION")
version = rabbitmqVersion ?: "0.1.0-SNAPSHOT"

// The oldest library release this connector is verified against: the first carrying
// CloudEventsHeaders.AMQP_BINARY_PREFIX. Declared as published coordinates so the POM states it; the
// root build substitutes the working tree back during development, and floorTest below checks the
// claim against the artifacts actually on Maven Central.
val tandemFloor = "0.10.0"

dependencies {
    api("com.codingful:tandem-core:$tandemFloor")
    // The envelope itself; this module adds only the AMQP binding on top of it.
    api("com.codingful:tandem-cloudevents:$tandemFloor")
    // Relay-side only — never on the client write-side (§1.3).
    api(libs.rabbitmq.amqp.client)
    // slf4j-api is already a runtime-scope transitive of amqp-client; declaring it explicitly makes it
    // usable at compile time and pins it to a current version (HLD-logging.md §2.2), exactly as
    // tandem-kafka does for the same reason.
    api(libs.slf4j.api)

    // Unit tests drive a hand-written in-memory Channel double, not a mock framework.
    testImplementation("com.codingful:tandem-test:$tandemFloor")
    // The worked-example encoder the custom-envelope case publishes through; test-only, so this
    // module's own versioning is unaffected. Never published, so it always comes from the working tree.
    testImplementation(testFixtures(project(":tandem-test")))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.rabbitmq)
}

// The same tests, run on a classpath where every Tandem coordinate resolves from Maven Central at the
// floor instead of from the working tree. Ordinary CI tests the working tree, so without this "works
// with tandem-core x.y.z" would be an unverified claim; same role the dual-generation gate plays for the
// Spring modules. The test fixtures are never published, so their jar joins as a plain file: it depends
// on tandem-core alone, which the floor provides.
val floorTestRuntimeClasspath by configurations.creating {
    isCanBeConsumed = false
    extendsFrom(configurations["api"], configurations["implementation"], configurations["runtimeOnly"],
            configurations["testRuntimeOnly"])
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

// The fixtures jar alone, without the working-tree tandem-core it would otherwise bring along.
val floorTestFixtures by configurations.creating {
    isCanBeConsumed = false
    isTransitive = false
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

dependencies {
    floorTestFixtures(testFixtures(project(":tandem-test")))
    // testImplementation minus the project fixtures, which floorTestRuntimeClasspath cannot resolve
    // from Central; restated rather than extended for that reason.
    floorTestRuntimeClasspath("com.codingful:tandem-test:$tandemFloor")
    floorTestRuntimeClasspath(platform(libs.testcontainers.bom))
    floorTestRuntimeClasspath(libs.testcontainers.junit)
    floorTestRuntimeClasspath(libs.testcontainers.rabbitmq)
    floorTestRuntimeClasspath(platform(libs.junit.bom))
    floorTestRuntimeClasspath(libs.junit.jupiter)
    floorTestRuntimeClasspath(libs.assertj.core)
}

val testSourceSet = sourceSets["test"]
val floorTest = tasks.register<Test>("floorTest") {
    description = "Runs every test against the Tandem floor ($tandemFloor) resolved from Maven Central."
    group = "verification"
    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.output + sourceSets["main"].output + floorTestRuntimeClasspath + floorTestFixtures
    shouldRunAfter(tasks.named("test"), tasks.named("integrationTest"))
}
tasks.named("check") { dependsOn(floorTest) }

// A publish without RABBITMQ_VERSION would upload 0.1.0-SNAPSHOT, and a coordinate on Central is
// permanent. Checked on the task graph, so the build stops before any publishing task (or its
// credential lookup) runs.
gradle.taskGraph.whenReady {
    if (rabbitmqVersion == null && allTasks.any { it.project == project && it.name.contains("MavenCentral") }) {
        throw GradleException("RABBITMQ_VERSION is unset: tandem-rabbitmq publishes only from rabbitmq-release.yml")
    }
}
