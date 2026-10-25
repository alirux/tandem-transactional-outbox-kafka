plugins {
    java
    application
}

application {
    mainClass.set("com.codingful.tandem.sample.jpa.JpaSampleApplication")
}

description = "Tandem JPA sample: concurrent JPA writers on Spring Boot 4 (not published)"

configure<JavaPluginExtension> {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Xlint:all")
}

dependencies {
    // The Spring Boot 4 runtime this leaf app runs on. A stock Boot 4 classpath: Jackson 3 only, no
    // Jackson 2 databind, which is the cell tandem-admin's jacksonThreeTest guards (LLD-spring-config §1.3).
    implementation(platform(libs.spring.boot.dependencies.v4))
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")  // Hibernate + JpaTransactionManager

    // The write-side tiers and the relay autoconfiguration under demonstration. The transport is a port,
    // so the application names the adapter it publishes to.
    implementation(project(":tandem-spring-producer"))
    implementation(project(":tandem-spring-relay"))
    implementation(project(":tandem-kafka"))
    // [DEMO-ONLY] The Admin API against the outbox just written; the web starter keeps the app running
    // as a server after SampleRunner finishes.
    implementation(project(":tandem-admin"))
    implementation("org.springframework.boot:spring-boot-starter-webmvc")

    // [DEMO-ONLY] TandemTestContainer starts a real PostgreSQL and Kafka so the sample is
    // self-contained; a real app supplies its own DataSource.
    implementation(project(":tandem-test"))

    // Unpublished, so the root convention configures neither JUnit/AssertJ nor an integrationTest phase
    // here. spring-boot-starter-test is avoided: it would bring its own JUnit line against JUnit 6.
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.spring.test)
}

// Every test boots the application against real containers, so all of them are tagged `integration`:
// `./gradlew test` stays Docker-free, `check` runs them.
val sourceSets = the<SourceSetContainer>()
val testSourceSet = sourceSets["test"]

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs the concurrent JPA writer scenarios against PostgreSQL and Kafka (requires Docker)."
    group = "verification"
    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.named("check") {
    dependsOn(integrationTest)
}
