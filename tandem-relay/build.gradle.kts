import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    java
    alias(libs.plugins.spring.boot)
}

description = "Tandem relay: the standalone relay application, shipped as a container image and an executable jar (not published to Maven Central)"

// The application's own version, injected by relay-release.yml from the tag (relay-v*), independent of
// the library's VERSION (LLD-relay §7.3). Unset outside a release, and then the build is a snapshot.
val relayVersion: String? = System.getenv("RELAY_VERSION")
version = relayVersion ?: "0.1.0-SNAPSHOT"

// The one library release this application contains. Declared as published coordinates through the BOM,
// the way an adopter consumes Tandem; the root build substitutes the working tree back during
// development, and pinnedRuntimeClasspath below is the one classpath exempt from that, so the jar that
// ships is built from the artifacts on Maven Central (LLD-relay §7.3).
val tandemPin = "0.11.1"

val mainClassName = "com.codingful.tandem.relay.TandemRelayApplication"

// Java 25, not the project-wide 17 (LLD-relay §3.1): an application constrains no consumer's classpath,
// so it takes the current LTS. Safe because nothing here is published as a library.
configure<JavaPluginExtension> {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:all")
}

springBoot {
    mainClass.set(mainClassName)
    // META-INF/build-info.properties, which the info endpoint and the startup line read: an image's
    // version does not imply the library release inside it, so both are stated (LLD-relay §7.3). The
    // build time is left out so the jar stays reproducible and the task cacheable.
    buildInfo {
        excludes.set(setOf("time"))
        properties {
            additional.set(mapOf("tandem.library" to tandemPin))
        }
    }
}

dependencies {
    // The Spring Boot runtime the image ships on, at the application's own catalog version.
    implementation(platform(libs.spring.boot.dependencies.relay.app))
    implementation(platform("com.codingful:tandem-bom:$tandemPin"))

    // The relay, the one transport the image carries (tandem-spring-relay redistributes none), the
    // metrics adapter and the optional second role (LLD-relay §3.2, §4).
    implementation("com.codingful:tandem-spring-relay")
    implementation("com.codingful:tandem-kafka")
    implementation("com.codingful:tandem-micrometer")
    implementation("com.codingful:tandem-admin")

    // The web container the Admin API and Actuator need, the DataSource and its pool, and the health
    // and metrics endpoints.
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    // The driver: here Tandem is the application, so it supplies one (LLD-relay §9).
    runtimeOnly("org.postgresql:postgresql")

    // Unit tests: the verdict and the startup checks, with no Spring context and no Docker.
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    // The driver's own name for the socket timeout, so the shipped default is asserted against it.
    testImplementation("org.postgresql:postgresql")
}

// ---------------------------------------------------------------------------------------------------
// Two executable jars from the same application classes, differing only in where Tandem comes from
// (LLD-relay §7.1): bootJar takes it from the working tree, pinnedBootJar from the pinned release on
// Maven Central. The pinned one is what ships, so it carries the plain name.
// ---------------------------------------------------------------------------------------------------
val pinnedRuntimeClasspath by configurations.creating {
    isCanBeConsumed = false
    extendsFrom(configurations["implementation"], configurations["runtimeOnly"])
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
}

val mainSourceSet = sourceSets["main"]

val worktreeBootJar = tasks.named<BootJar>("bootJar") {
    archiveClassifier.set("worktree")
}

val pinnedBootJar = tasks.register<BootJar>("pinnedBootJar") {
    description = "The executable jar built on the pinned Tandem release ($tandemPin) from Maven Central."
    group = "build"
    mainClass.set(mainClassName)
    targetJavaVersion.set(JavaVersion.VERSION_25)
    classpath(mainSourceSet.output, pinnedRuntimeClasspath)
}

tasks.named("assemble") {
    dependsOn(pinnedBootJar)
}

// ---------------------------------------------------------------------------------------------------
// The integration test starts the jar as a process and talks to it from outside (LLD-relay §8.2), so
// it needs nothing of the application on its own classpath. It lives in a source set of its own for
// that reason: the main and test classpaths carry the Spring Boot 4 BOM, which manages a Testcontainers
// generation and a Jackson generation that tandem-test's container helper is not built against.
// ---------------------------------------------------------------------------------------------------
val integrationTestSourceSet = sourceSets.create("integrationTest")

dependencies {
    "integrationTestImplementation"(platform("com.codingful:tandem-bom:$tandemPin"))
    "integrationTestImplementation"("com.codingful:tandem-test")
    "integrationTestImplementation"(platform(libs.junit.bom))
    "integrationTestImplementation"(libs.junit.jupiter)
    "integrationTestImplementation"(libs.assertj.core)
    "integrationTestRuntimeOnly"(libs.junit.platform.launcher)
    // Reads the JSON the running application answers with; the test's own tool, unrelated to the
    // binding inside the application under test.
    "integrationTestImplementation"(platform(libs.jackson.bom))
    "integrationTestImplementation"(libs.jackson.databind)
}

val applicationLauncher = javaToolchains.launcherFor(java.toolchain)

// The PostgreSQL image the container helper starts (TandemTestContainer.postgresImage()), declared as
// a task input for the reason the root convention gives: with the build cache on, a run against one
// major would otherwise be handed the cached result of a run against another.
val postgresImage = providers.systemProperty("tandem.test.postgres.image")
        .orElse(providers.environmentVariable("TANDEM_TEST_POSTGRES_IMAGE"))

fun Test.launching(jar: TaskProvider<BootJar>) {
    group = "verification"
    inputs.property("tandemPostgresImage", postgresImage.orElse("default"))
    postgresImage.orNull?.let { systemProperty("tandem.test.postgres.image", it) }
    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath
    useJUnitPlatform()
    val jarFile = jar.flatMap { it.archiveFile }
    inputs.file(jarFile).withPropertyName("applicationJar")
    // The jar under test and the JVM to start it with: the module's own toolchain, not whichever JDK
    // runs Gradle.
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
                "-Dtandem.relay.jar=${jarFile.get().asFile.absolutePath}",
                "-Dtandem.relay.pin=$tandemPin",
                "-Dtandem.relay.java=${applicationLauncher.get().executablePath.asFile.absolutePath}")
    })
    shouldRunAfter(tasks.named("test"))
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs the application built on the working tree as a process (requires Docker)."
    launching(worktreeBootJar)
}

val pinnedTest = tasks.register<Test>("pinnedTest") {
    description = "Runs the application built on the pinned Tandem release ($tandemPin) as a process (requires Docker)."
    launching(pinnedBootJar)
    shouldRunAfter(integrationTest)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named("check") {
    dependsOn(integrationTest, pinnedTest)
}
