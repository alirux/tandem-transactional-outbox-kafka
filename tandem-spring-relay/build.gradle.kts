description = "Tandem Spring Boot autoconfiguration — relay engine, wiring whichever publish adapter is on the classpath"

dependencies {
    // The only redistributed dependency: the JDBC relay engine, which every relay needs whatever it
    // publishes to.
    api(project(":tandem-jdbc"))

    // Optional, like Spring and Micrometer below: the transport is a port, so this module wires
    // whichever adapter is on the classpath instead of carrying one (HLD §1.3). An application
    // relaying to RabbitMQ must not inherit the Kafka client. TandemKafkaAutoConfiguration is gated
    // on these classes and is never loaded without them; the noKafkaTest source set below proves the
    // rest of the module loads on a classpath that genuinely lacks them.
    compileOnly(project(":tandem-kafka"))

    // Spring is compile-only so no Spring version is propagated to the consumer — the application
    // brings its own Boot 3.x or 4.x and the JVM binds at runtime (LLD-spring-config §1.1).
    compileOnly(platform(libs.spring.boot.dependencies))
    compileOnly(libs.spring.boot.autoconfigure)
    compileOnly(libs.slf4j.api)

    // Optional, like Spring itself: an application that never adds tandem-micrometer must not
    // inherit it. The first optional dependency between two Tandem modules (LLD-micrometer §5, Q31).
    compileOnly(project(":tandem-micrometer"))
    compileOnly(libs.micrometer.core)
    // Optional too: instrumented mode's span emitter is wired only when the application already runs
    // Micrometer Tracing (HLD-tracing.md §6); never forced, hence compile-only.
    compileOnly(libs.micrometer.tracing)

    // Generates META-INF/spring-configuration-metadata.json for IDE completion (LLD-spring-config §2.4).
    annotationProcessor(platform(libs.spring.boot.dependencies))
    annotationProcessor(libs.spring.boot.configuration.processor)

    // Tests run against a real Spring context on the baseline (3.x) line; the end-to-end relay test
    // uses TandemTestContainer's real PostgreSQL + Kafka.
    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.autoconfigure)
    testImplementation(libs.spring.boot.test)
    testImplementation(libs.slf4j.api)
    // AbstractDataSource is the real base the wiring tests' stub DataSource extends (test-only).
    testImplementation(libs.spring.jdbc)
    testImplementation(project(":tandem-test"))
    // tandem-test brings tandem-kafka transitively; declared explicitly because this module's tests
    // assert the Kafka wiring directly and should not depend on another module's dependency graph.
    testImplementation(project(":tandem-kafka"))
    // Real Micrometer classes on the test classpath, so the wiring tests can prove the conditional
    // fires with a real MeterRegistry bean present and backs off to NOOP without one.
    testImplementation(project(":tandem-micrometer"))
    testImplementation(libs.micrometer.core)
    // SimpleMeterRegistry (used everywhere else in this module's tests) never materializes real
    // histogram buckets regardless of Timer config, so verifying tandem.metrics.max-publish-latency
    // actually reaches MicrometerTandemMetrics needs a registry that renders one (mirrors the same
    // trade-off in tandem-micrometer's own test suite).
    testImplementation(libs.micrometer.registry.prometheus)
    // A real tracer, propagator and in-memory span exporter, so the publish span is asserted as a real
    // exported span with a real parent rather than through a hand-written stand-in.
    testImplementation(libs.micrometer.tracing)
    testImplementation(libs.micrometer.tracing.bridge.otel)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    // The reference-configuration check shared with tandem-spring-producer (LLD-spring-config §2.4) —
    // internal-only, not published (see tandem-test/build.gradle.kts).
    testImplementation(testFixtures(project(":tandem-test")))
}

// The configuration processor reads META-INF/additional-spring-configuration-metadata.json (the hand-written
// entries of LLD-spring-config §2.4) off the *processed* resources, so the resources must be there before
// compileJava runs — without this the file is silently ignored and the keys it documents vanish from the
// metadata the IDE reads.
tasks.named("compileJava") {
    inputs.files(tasks.named("processResources"))
}

// ---------------------------------------------------------------------------------------------------
// Three-line compatibility matrix (LLD-spring-config §1.2)
//
// The module's main sources are compiled ONCE against the Boot 3.x baseline (compileOnly, above). These
// tasks re-run the very same compiled test AND main classes with Spring swapped to another line on the
// test runtime classpath — which is exactly the binary compatibility the single-artifact strategy bets
// on (§1.1). bootLatestThreeTest covers the latest Boot 3.x patch (Framework 6.2.x), otherwise never
// exercised between the 6.1.x baseline and the 7.x line; bootFourTest covers the 4.x line. Only the
// lightweight context-runner tests run here; the Docker-bound integration test stays on the baseline,
// where its far slower containers buy no extra compatibility signal.
// ---------------------------------------------------------------------------------------------------
val bootLatestThreeTestRuntimeClasspath: Configuration by configurations.creating
val bootFourTestRuntimeClasspath: Configuration by configurations.creating

dependencies {
    bootLatestThreeTestRuntimeClasspath(platform(libs.spring.boot.dependencies.v3.latest))
    bootLatestThreeTestRuntimeClasspath(libs.spring.boot.autoconfigure)
    bootLatestThreeTestRuntimeClasspath(libs.spring.boot.test)
    bootLatestThreeTestRuntimeClasspath(libs.spring.jdbc)
    bootLatestThreeTestRuntimeClasspath(libs.slf4j.api)
    bootLatestThreeTestRuntimeClasspath(project(":tandem-test"))
    bootLatestThreeTestRuntimeClasspath(testFixtures(project(":tandem-test")))
    bootLatestThreeTestRuntimeClasspath(project(":tandem-micrometer"))
    bootLatestThreeTestRuntimeClasspath(libs.micrometer.core)
    bootLatestThreeTestRuntimeClasspath(libs.micrometer.registry.prometheus)
    bootLatestThreeTestRuntimeClasspath(libs.micrometer.tracing)
    bootLatestThreeTestRuntimeClasspath(libs.micrometer.tracing.bridge.otel)
    bootLatestThreeTestRuntimeClasspath(libs.opentelemetry.sdk)
    bootLatestThreeTestRuntimeClasspath(libs.opentelemetry.sdk.testing)
    bootLatestThreeTestRuntimeClasspath(platform(libs.junit.bom))
    bootLatestThreeTestRuntimeClasspath(libs.junit.jupiter)
    bootLatestThreeTestRuntimeClasspath(libs.junit.platform.launcher)
    bootLatestThreeTestRuntimeClasspath(libs.assertj.core)

    bootFourTestRuntimeClasspath(platform(libs.spring.boot.dependencies.v4))
    bootFourTestRuntimeClasspath(libs.spring.boot.autoconfigure)
    bootFourTestRuntimeClasspath(libs.spring.boot.test)
    bootFourTestRuntimeClasspath(libs.spring.jdbc)
    bootFourTestRuntimeClasspath(libs.slf4j.api)
    bootFourTestRuntimeClasspath(project(":tandem-test"))
    bootFourTestRuntimeClasspath(testFixtures(project(":tandem-test")))
    bootFourTestRuntimeClasspath(project(":tandem-micrometer"))
    bootFourTestRuntimeClasspath(libs.micrometer.core)
    bootFourTestRuntimeClasspath(libs.micrometer.registry.prometheus)
    bootFourTestRuntimeClasspath(libs.micrometer.tracing)
    bootFourTestRuntimeClasspath(libs.micrometer.tracing.bridge.otel)
    bootFourTestRuntimeClasspath(libs.opentelemetry.sdk)
    bootFourTestRuntimeClasspath(libs.opentelemetry.sdk.testing)
    bootFourTestRuntimeClasspath(platform(libs.junit.bom))
    bootFourTestRuntimeClasspath(libs.junit.jupiter)
    bootFourTestRuntimeClasspath(libs.junit.platform.launcher)
    bootFourTestRuntimeClasspath(libs.assertj.core)
}

val sourceSets = the<SourceSetContainer>()
val mainOutput = sourceSets["main"].output
val testOutput = sourceSets["test"].output

val bootLatestThreeTest = tasks.register<Test>("bootLatestThreeTest") {
    description = "Re-runs the unit tests against the latest Spring Boot 3.x line (three-line matrix)."
    group = "verification"
    testClassesDirs = testOutput.classesDirs
    classpath = files(testOutput, mainOutput, bootLatestThreeTestRuntimeClasspath)
    useJUnitPlatform { excludeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

val bootFourTest = tasks.register<Test>("bootFourTest") {
    description = "Re-runs the unit tests against the Spring Boot 4.x line (three-line matrix)."
    group = "verification"
    testClassesDirs = testOutput.classesDirs
    classpath = files(testOutput, mainOutput, bootFourTestRuntimeClasspath)
    useJUnitPlatform { excludeTags("integration") }
    shouldRunAfter(tasks.named("test"))
}

// ---------------------------------------------------------------------------------------------------
// The no-Kafka gate (IMPLEMENTATION-PLAN-rabbitmq.md §6)
//
// tandem-kafka is compileOnly here, so the module must load and wire a relay on a classpath that does
// not contain it. A FilteredClassLoader context-runner test cannot prove that: it leaves the
// configuration class loaded by the parent loader, so a Kafka type in a @Bean signature still
// resolves and the test stays green (verified once already, for the Micrometer Tracing bridge). Only a
// genuinely Kafka-free classpath does, which is what this source set is. Same role jacksonThreeTest
// plays in tandem-admin.
// ---------------------------------------------------------------------------------------------------
val noKafka = sourceSets.create("noKafkaTest") {
    compileClasspath += mainOutput
    runtimeClasspath += mainOutput
}
listOf("noKafkaTestCompileClasspath", "noKafkaTestRuntimeClasspath").forEach { name ->
    configurations[name].exclude(group = "com.codingful", module = "tandem-kafka")
    configurations[name].exclude(group = "org.apache.kafka", module = "kafka-clients")
}

dependencies {
    "noKafkaTestImplementation"(platform(libs.spring.boot.dependencies))
    "noKafkaTestImplementation"(libs.spring.boot.autoconfigure)
    "noKafkaTestImplementation"(libs.spring.boot.test)
    "noKafkaTestImplementation"(libs.spring.jdbc)
    "noKafkaTestImplementation"(libs.slf4j.api)
    "noKafkaTestImplementation"(project(":tandem-jdbc"))
    "noKafkaTestImplementation"(platform(libs.junit.bom))
    "noKafkaTestImplementation"(libs.junit.jupiter)
    "noKafkaTestRuntimeOnly"(libs.junit.platform.launcher)
    "noKafkaTestImplementation"(libs.assertj.core)
}

val noKafkaTest = tasks.register<Test>("noKafkaTest") {
    description = "Runs the relay autoconfiguration on a classpath with no Kafka adapter at all."
    group = "verification"
    testClassesDirs = noKafka.output.classesDirs
    classpath = noKafka.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.named("test"))
}

tasks.named("check") {
    dependsOn(bootLatestThreeTest, bootFourTest, noKafkaTest)
}
