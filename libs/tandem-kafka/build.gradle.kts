description = "Tandem Kafka adapter — OutboxDispatcher over the Kafka producer (CloudEvents binary binding)"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only — never on the client write-side (§1.3).
    api(libs.kafka.clients)
    // The envelope itself; this module adds only the Kafka binding on top of it.
    api(project(":tandem-cloudevents"))
    api(libs.cloudevents.kafka)   // brings cloudevents-core transitively
    // slf4j-api is already a runtime-scope transitive of kafka-clients; declaring it explicitly
    // makes it usable at compile time and pins it to a current version (HLD-logging.md §2.2).
    api(libs.slf4j.api)

    // kafka-clients 3.9.2 resolves lz4-java 1.10.1, which carries advisories fixed up to 1.11.4:
    // native XXHash bindings that can crash the JVM (GHSA-xx22-p4ch-683r), unvalidated length
    // headers that allocate up to 1 GiB (GHSA-4v53-57pg-c464, GHSA-6cx8-rjf8-pr8g), stream decoders
    // open to CPU and stack exhaustion (GHSA-gm45-99xc-r7wv, GHSA-343h-94h5-c4wr), and native library
    // extraction to a shared temporary directory (GHSA-mcr4-qmvw-px4g). tandem-kafka is published, so
    // consumers inherit the flagged coordinate on their runtime classpath and see it in their own
    // scans. Raise the floor rather than make every consumer do it. This is a constraint, not a
    // dependency: it sets a minimum if lz4-java is present, and pulls in nothing on its own. Drop it
    // once kafka-clients ships >= 1.11.4.
    constraints {
        api(libs.lz4.java) {
            because("CVE remediation: lz4-java <= 1.11.3 carries advisories fixed in 1.11.4")
        }
    }

    // Unit tests use Kafka's own in-memory MockProducer (a real test double, not a mock framework).
    testImplementation(project(":tandem-test"))
    // The reference-file helper (LLD-api-stability §8.1), internal-only and not published (see
    // libs/tandem-test/build.gradle.kts).
    testImplementation(testFixtures(project(":tandem-test")))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.kafka)
}
