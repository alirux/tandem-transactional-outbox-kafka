import java.util.zip.ZipFile
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory
import org.springframework.boot.gradle.tasks.bundling.BootJar
import org.w3c.dom.Element

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
    // The licence and the third-party notices travel with the redistribution (LLD-relay §7.2). The
    // image reads both from here, so the jar and the image carry the same two files.
    metaInf {
        from(rootProject.file("LICENSE"), rootProject.file("THIRD-PARTY-NOTICES.md"))
    }
}

tasks.named("assemble") {
    dependsOn(pinnedBootJar)
}

// The image states its own version and the library release inside it in two labels (LLD-relay §7.2),
// and a Dockerfile can only take a label's value from a build argument. This task is where every image
// build, local, CI or release, gets those arguments: the same two values the jar's build-info carries,
// so there is one source for both, and the Dockerfile refuses a jar whose build-info disagrees.
//   docker build $(./gradlew -q :tandem-relay:imageBuildArgs) --tag tandem-relay:local tandem-relay
tasks.register("imageBuildArgs") {
    group = "build"
    description = "Prints the docker build arguments naming the image's version and the Tandem release inside it."
    val arguments = mapOf("RELAY_VERSION" to version.toString(), "TANDEM_VERSION" to tandemPin)
    doLast {
        arguments.forEach { (name, value) -> println("--build-arg=$name=$value") }
    }
}

// ---------------------------------------------------------------------------------------------------
// Third-party notices (LLD-relay §10). The jar and the image redistribute every library in the pinned
// jar's BOOT-INF/lib, so the tandem-relay list in THIRD-PARTY-NOTICES.md is generated from that
// directory, never kept by hand: updateThirdPartyNotices rewrites it, and checkThirdPartyNotices, part
// of check, fails when the committed list no longer matches the jar. Each library's licence is read from
// its POM, or from the nearest parent POM that declares one, which is how Maven inherits it.
// ---------------------------------------------------------------------------------------------------
val thirdPartyNotices = rootProject.file("THIRD-PARTY-NOTICES.md")
val noticesBegin = "<!-- BEGIN tandem-relay libraries: generated by ./gradlew :tandem-relay:updateThirdPartyNotices, do not edit by hand -->"
val noticesEnd = "<!-- END tandem-relay libraries -->"

// The one jar in BOOT-INF/lib that no configuration resolves: the Boot plugin adds it by itself, and the
// Dockerfile's layer extraction runs on it.
val bootVersion = libs.versions.spring.boot.relay.app.get()
val jarmodeToolsJar = "spring-boot-jarmode-tools-$bootVersion.jar"
val jarmodeTools = "org.springframework.boot:spring-boot-jarmode-tools:$bootVersion"

// Licence names as the POMs spell them, mapped to SPDX identifiers. A name missing here fails the
// generator, so a licence new to the image enters it only once a person has read it, and added its
// section to THIRD-PARTY-NOTICES.md.
val spdxByPomLicence = mapOf(
        "Apache-2.0" to "Apache-2.0",
        "Apache License, Version 2.0" to "Apache-2.0",
        "The Apache License, Version 2.0" to "Apache-2.0",
        "The Apache Software License, Version 2.0" to "Apache-2.0",
        "BSD-2-Clause" to "BSD-2-Clause",
        "BSD 2-Clause License" to "BSD-2-Clause",
        "MIT" to "MIT",
        "The MIT License" to "MIT",
        "EPL-2.0" to "EPL-2.0",
        "EPL 2.0" to "EPL-2.0",
        "LGPL-2.1-only" to "LGPL-2.1-only",
        "GPL2 w/ CPE" to "GPL-2.0-only WITH Classpath-exception-2.0",
        "Public Domain, per Creative Commons CC0" to "CC0-1.0",
).mapKeys { it.key.lowercase() }

val pinnedLibraryCoordinates = pinnedRuntimeClasspath.incoming.artifacts.resolvedArtifacts.map { artifacts ->
    artifacts.mapNotNull { artifact ->
        (artifact.id.componentIdentifier as? ModuleComponentIdentifier)?.let { artifact.file.name to it.displayName }
    }.toMap()
}

fun childElements(parent: Element, name: String): List<Element> {
    val nodes = parent.childNodes
    return (0 until nodes.length).map { nodes.item(it) }.filterIsInstance<Element>().filter { it.tagName == name }
}

fun childText(parent: Element, name: String): String =
        childElements(parent, name).firstOrNull()?.textContent?.replace(Regex("\\s+"), " ")?.trim().orEmpty()

/** The POM of each coordinate, resolved from the repositories like any other artifact. */
fun pomFiles(coordinates: Set<String>): Map<String, File> {
    val poms = configurations.detachedConfiguration(*coordinates.map { dependencies.create("$it@pom") }.toTypedArray())
    poms.isTransitive = false
    return poms.incoming.artifacts.resolvedArtifacts.get().associate { pom ->
        (pom.id.componentIdentifier as ModuleComponentIdentifier).displayName to pom.file
    }
}

/** The licence names each library declares, following parent POMs until one declares any. */
fun pomLicences(libraries: Collection<String>): Map<String, List<String>> {
    val xml = DocumentBuilderFactory.newInstance().apply {
        setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
        setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    }.newDocumentBuilder()
    val licences = mutableMapOf<String, List<String>>()
    var nextPom = libraries.associateWith { it }
    while (nextPom.isNotEmpty()) {
        val poms = pomFiles(nextPom.values.toSet())
        val parents = mutableMapOf<String, String>()
        nextPom.forEach { (library, coordinate) ->
            val project = xml.parse(poms.getValue(coordinate)).documentElement
            val names = childElements(project, "licenses").flatMap { childElements(it, "license") }
                    .map { licence -> childText(licence, "name").ifEmpty { childText(licence, "url") } }
            val parent = childElements(project, "parent").firstOrNull()
            when {
                names.isNotEmpty() -> licences[library] = names
                parent != null -> parents[library] =
                        listOf("groupId", "artifactId", "version").joinToString(":") { childText(parent, it) }
                else -> throw GradleException("No licence in the POM of $library, nor in any of its parents")
            }
        }
        nextPom = parents
    }
    return licences
}

/** The generated part of the tandem-relay section, from the jar itself. */
fun relayNotices(jar: File, coordinatesByFile: Map<String, String>): String {
    val bundled = ZipFile(jar).use { zip ->
        zip.entries().asSequence().map { it.name }
                .filter { it.startsWith("BOOT-INF/lib/") && it.endsWith(".jar") }
                .map { it.removePrefix("BOOT-INF/lib/") }.toList()
    }
    val unattributed = bundled.filter { it !in coordinatesByFile && it != jarmodeToolsJar }
    if (unattributed.isNotEmpty()) {
        throw GradleException("No Maven coordinates for these jars of ${jar.name}: $unattributed")
    }
    // Tandem's own modules are the library release the image is built on, under Tandem's own licence.
    val libraries = bundled.map { coordinatesByFile[it] ?: jarmodeTools }
            .filterNot { it.startsWith("com.codingful:") }.sortedBy { it.substringBeforeLast(":") }
    val licences = pomLicences(libraries)
    val unknown = licences.flatMap { (library, names) ->
        names.filter { it.lowercase() !in spdxByPomLicence }.map { "$library: \"$it\"" }
    }
    if (unknown.isNotEmpty()) {
        throw GradleException("Licences with no SPDX identifier in tandem-relay/build.gradle.kts, "
                + "read them and add them there and to THIRD-PARTY-NOTICES.md:\n" + unknown.joinToString("\n"))
    }
    val rows = libraries.map { library ->
        val (group, artifact, libraryVersion) = library.split(":")
        // Several licences in one POM are alternatives, by Maven's own definition of the element.
        val licence = licences.getValue(library).map { spdxByPomLicence.getValue(it.lowercase()) }.distinct()
                .joinToString(" OR ")
        listOf("$group:$artifact", libraryVersion, licence)
    }
    val header = listOf("Library", "Version", "License")
    val widths = header.indices.map { column -> (rows + listOf(header)).maxOf { it[column].length } }
    fun line(cells: List<String>) = cells.mapIndexed { i, cell -> cell.padEnd(widths[i]) }
            .joinToString(" | ", "| ", " |")
    return buildString {
        appendLine()
        appendLine("Built on Tandem $tandemPin. ${rows.size} third-party libraries:")
        appendLine()
        appendLine(line(header))
        appendLine(widths.joinToString("-|-", "|-", "-|") { "-".repeat(it) })
        rows.forEach { appendLine(line(it)) }
        appendLine()
    }
}

fun withRelayNotices(notices: String, generated: String): String {
    val begin = notices.indexOf(noticesBegin)
    val end = notices.indexOf(noticesEnd)
    if (begin < 0 || end < begin) {
        throw GradleException("${thirdPartyNotices.name} has lost the two lines that delimit the generated "
                + "tandem-relay list:\n$noticesBegin\n$noticesEnd")
    }
    return notices.substring(0, begin + noticesBegin.length) + "\n" + generated + notices.substring(end)
}

tasks.register("updateThirdPartyNotices") {
    group = "documentation"
    description = "Regenerates the tandem-relay list in THIRD-PARTY-NOTICES.md from the pinned jar's BOOT-INF/lib."
    val jar = pinnedBootJar.flatMap { it.archiveFile }
    inputs.file(jar)
    doLast {
        val generated = relayNotices(jar.get().asFile, pinnedLibraryCoordinates.get())
        thirdPartyNotices.writeText(withRelayNotices(thirdPartyNotices.readText(), generated))
    }
}

val checkThirdPartyNotices = tasks.register("checkThirdPartyNotices") {
    group = "verification"
    description = "Fails when the tandem-relay list in THIRD-PARTY-NOTICES.md no longer matches the pinned jar."
    val jar = pinnedBootJar.flatMap { it.archiveFile }
    val verified = layout.buildDirectory.file("notices/verified")
    inputs.file(jar)
    inputs.file(thirdPartyNotices)
    outputs.file(verified)
    doLast {
        val committed = thirdPartyNotices.readText()
        if (withRelayNotices(committed, relayNotices(jar.get().asFile, pinnedLibraryCoordinates.get())) != committed) {
            throw GradleException("The tandem-relay list in ${thirdPartyNotices.name} does not match the libraries "
                    + "in ${jar.get().asFile.name}. Run ./gradlew :tandem-relay:updateThirdPartyNotices and commit "
                    + "the result.")
        }
        verified.get().asFile.writeText("${thirdPartyNotices.name} matches ${jar.get().asFile.name}\n")
    }
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
    dependsOn(integrationTest, pinnedTest, checkThirdPartyNotices)
}
