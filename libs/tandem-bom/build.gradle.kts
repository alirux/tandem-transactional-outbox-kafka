plugins {
    `java-platform`
}

description = "Tandem BOM — aligns versions of all Tandem modules"

dependencies {
    // Every published module, so a consumer that imports this BOM can declare any of them without a
    // version. A module missing here is unusable that way — add new published modules in the same change.
    constraints {
        api(project(":tandem-core"))
        api(project(":tandem-jdbc"))
        api(project(":tandem-cloudevents"))
        api(project(":tandem-kafka"))
        api(project(":tandem-test"))
        api(project(":tandem-spring-producer"))
        api(project(":tandem-spring-relay"))
        api(project(":tandem-micrometer"))
        api(project(":tandem-tracing-otel"))
        api(project(":tandem-admin"))
        // tandem-rabbitmq is deliberately absent: it carries its own version and its own release
        // cadence (LLD-rabbitmq §9), and this BOM's promise is one aligned version for everything it
        // lists. Pinning it here would force a BOM release on every connector release. A consumer
        // declares it with its own version.
    }
}
