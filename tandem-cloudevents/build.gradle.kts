description = "Tandem CloudEvents envelope: OutboxRecord to CloudEvent, independent of the transport that carries it"

dependencies {
    api(project(":tandem-core"))
    // Relay-side only, never on the client write-side (§1.3). The CloudEvents SDK's transport
    // bindings are NOT here: each of them belongs to its own transport adapter (cloudevents-kafka in
    // tandem-kafka), so this module stays usable by any of them.
    api(libs.cloudevents.core)
}
