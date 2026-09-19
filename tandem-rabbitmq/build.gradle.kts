description = "Tandem RabbitMQ connector — OutboxDispatcher over AMQP 0.9.1 (CloudEvents binary binding)"

dependencies {
    // Project dependencies for now. This module carries its own version and its own release workflow,
    // so before its first publication these become published coordinates with an explicit floor, kept
    // substitutable for local development (IMPLEMENTATION-PLAN-rabbitmq.md §8). A project dependency
    // would otherwise stamp THIS module's version into the POM as the core's version.
    api(project(":tandem-core"))
    // The envelope itself; this module adds only the AMQP binding on top of it.
    api(project(":tandem-cloudevents"))
    // Relay-side only — never on the client write-side (§1.3).
    api(libs.rabbitmq.amqp.client)
    // slf4j-api is already a runtime-scope transitive of amqp-client; declaring it explicitly makes it
    // usable at compile time and pins it to a current version (HLD-logging.md §2.2), exactly as
    // tandem-kafka does for the same reason.
    api(libs.slf4j.api)

    // Unit tests drive a hand-written in-memory Channel double, not a mock framework.
    testImplementation(project(":tandem-test"))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.rabbitmq)
}
