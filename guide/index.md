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
| [Adoption Guide](adoption.md) | Ordering precondition, write-side tiers, `seq` modes and the write lock, applying the schema to a database with data in it, strangler cutover, aggregate-less events, rollout order. |
| [Database Compatibility](compatibility.md) | What Tandem requires of an engine, PostgreSQL versions, managed and PostgreSQL-derived engines, connection poolers, what is out of scope and why, checking your own engine. |

Every chapter stands on its own and numbers its own sections, so it can be read, bookmarked and
cited without the rest of the guide.

## License

Tandem is released under the [Apache License 2.0](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/LICENSE).

This documentation is licensed separately, under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): you may share and adapt it,
including commercially, as long as you give attribution.
