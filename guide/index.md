# Tandem User Guide

## Overview

Tandem is a Java library that implements the **Transactional Outbox Pattern**. You insert an event
into an outbox table **inside the same transaction** that mutates your domain, so the write is
atomic by your database's own ACID guarantees, with no dual write and no distributed transaction.
A separate **relay** then polls the outbox and publishes to Kafka, at-least-once, preserving
per-aggregate ordering.

It runs on the database and the broker you already have: no change data capture, no Kafka Connect,
no two-phase commit, and no process to operate beyond your own application.

These pages cover what you have to **do**. For what the library *is*, with the code shape of each
API tier, start from the
[README](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/README.md); for
*why* it is built this way, read the
[design documents](https://github.com/alirux/tandem-transactional-outbox-kafka/tree/main/docs).
The project site, [tandem.codingful.com](https://tandem.codingful.com), has an interactive
walkthrough of the message flow.

## Chapters

| Chapter | Topics |
|---|---|
| [Getting Started](getting-started.md) | First use, step by step with Spring Boot and with plain Java: dependencies, schema, writing an event in your transaction, starting the relay, the settings you will touch, checking the result, idempotent consumers. |
| [Consuming Events](consuming-events.md) | What a consumer receives (key, value, every header), what it can and cannot rely on, reading an event in Java, Spring Kafka and Python, idempotency, ordering, failed processing, continuing the trace. |
| [Adoption Guide](adoption.md) | Ordering precondition, write-side tiers, `seq` modes and the write lock, applying the schema to a database with data in it, strangler cutover, aggregate-less events, rollout order. |
| [Publishing Your Own Message Format](message-format.md) | The format and transport seams, writing a portable encoder, wiring it in plain Java and in Spring, what the ordering key means per broker, and the compatibility rules a published envelope carries. |
| [Configuration Reference](configuration.md) | Every `tandem.*` property with its default, its meaning, which side reads it and its plain Java equivalent, and the settings that must agree. |
| [Troubleshooting](troubleshooting.md) | By symptom: events not reaching Kafka, failed rows, startup errors, writing errors, what consumers see, Admin API and CLI problems, missing metrics and traces. |
| [Database Compatibility](compatibility.md) | What Tandem requires of an engine, PostgreSQL versions, managed and PostgreSQL-derived engines, connection poolers, what is out of scope and why, checking your own engine. |
| [Reliability and Topology](reliability.md) | What Tandem does under failure (retries, permanent errors, a Kafka or database outage, a relay crash), embedded and split deployments, `SINGLE` and `LEASE` coordination, the bucket count and its guard, the tables Tandem keeps and how long. |
| [Advanced Integration](advanced-integration.md) | The Kafka producer and the settings Tandem protects, topic routing and partitions, batch inserts, `@TransactionalOutbox` and Spring events, object payload serializers, replay and query from your own code, replacing a default. |
| [Testing and Upgrades](testing-and-upgrades.md) | Testing with the in-memory outbox, the recording dispatcher and real containers, the tests only you can write, why mixed versions are safe, and a step-by-step upgrade. |
| [Observability](observability.md) | Logs (where they go, how a line is built, the lines worth knowing), metrics (turning them on, every meter, the alerts to build and their limits) and distributed traces (propagation, the relay publish span, the correlation id, reading a trace), then how the three join in an incident. |
| [Admin API](admin-api.md) | Turning it on and securing it, the life of an outbox row, then every endpoint by function (health, finding messages, replay and discard, relay control, buckets and workers) with curl, Python, JavaScript and Java examples. |
| [Command Line](cli.md) | Installing and connecting `tandem-cli`, output and exit codes, then every command by function with real output, plus scripting and a worked recovery. |

Every chapter stands on its own and numbers its own sections, so it can be read, bookmarked and
cited without the rest of the guide.

## License

Tandem is released under the [Apache License 2.0](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/LICENSE).

This documentation is licensed separately, under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): you may share and adapt it,
including commercially, as long as you give attribution.
