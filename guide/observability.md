# Observability

---

## 0. Who this page is for

Your events are flowing (see [Getting Started](getting-started.md)) and you want to know, without
guessing, whether delivery is healthy, and what happened when it is not. Tandem gives you three
signals for that: **logs**, **metrics** and **distributed traces**. This page shows how to get each
one, how to read it, and which question it answers.

!!! question "The question this page answers"
    **How do I see what Tandem is doing, get alerted when delivery goes wrong, and follow one
    business operation from the database write to the consumer?**

Two things hold for all three. Metrics and traces are **off until you ask for them**, so an
application that wants neither pays nothing. And none of them ever contains your event payloads,
credentials or header values: only identifiers, counts and timings.

---

## 1. Three signals, three questions

| Signal | Answers | Where to look first |
|---|---|---|
| **Logs** | What happened to this relay, or this row? Why did it fail? | The relay's log, filtered by level and by the row or aggregate id. |
| **Metrics** | Is delivery healthy right now, and is it getting worse? | A dashboard, and the alerts you build on it. |
| **Traces** | What happened to this one business operation, across services? | A tracing backend (Tempo, Jaeger, Zipkin and the like). |

They are meant to be joined, and the thing that joins them is the **correlation id**: an opaque
identifier for one unit of business work, such as a request or a saga. Once you enable it (§4.4) it
appears in the Kafka message headers, on the relay's publish span, and in the outbox row itself,
where the [Admin API](admin-api.md) and the [CLI](cli.md) can search it. A ticket, a log line or a
span all lead to the same set of events.

The Admin API and the CLI are a fourth, on-demand view: they read the outbox itself, so they keep
working when the relay, and therefore its metrics, are down.

---

## 2. Logs

### 2.1 Where the logs go

Tandem ships **no logging configuration**: routing and formatting are your application's job. What
you need to do depends on the module.

| Module | Logs through | What you do |
|---|---|---|
| `tandem-jdbc` (relay lifecycle, claim and reclaim, dispatch outcomes) and `tandem-core` | The JDK's `System.Logger`, so no logging library lands on your write side | Spring Boot: nothing, the lines already appear in your log. Elsewhere: one bridge dependency, below. |
| `tandem-kafka` (encode and send failures) | SLF4J | Nothing: it uses the binding your Kafka client already uses. |
| `tandem-admin` (audit of every write operation) | SLF4J | Nothing. |

Under Spring Boot with its default logging, `System.Logger` output is already routed to your
backend, and Tandem's lines look like any other:

```
2026-09-20T14:01:22.441+02:00  INFO 16809 --- [tandem-sample-spring] [16809-90b7-jobs] c.c.tandem.jdbc.BucketLeaseManager : Relay claimed buckets owner:tandem-host-16809-90b7, count:256
```

Outside Spring Boot, bridge `System.Logger` to your backend with one runtime dependency and no code:

```kotlin
runtimeOnly("org.slf4j:slf4j-jdk-platform-logging:2.0.16")
```

Keep exactly one such bridge on the classpath. If two are present (this one and Log4j2's, for
example), which of them wins is undefined.

### 2.2 How a line is built

Every Tandem log line is a **fixed message, then a flat tail of `name:value` pairs** separated by
commas, always in the same order:

```
Outbox row dispatch failed, retrying rowId:1042, aggregateType:Order, aggregateId:order-77, attempts:2, delayMs:800
```

The message never changes between occurrences, so you can search or count by it, and everything that
varies is in the tail. Every `WARN` and `ERROR` carries the identifiers you need to find the affected
record (row id, aggregate id, topic, worker index, bucket) so you do not have to reproduce the
failure to investigate it. The exception, with its stack trace, is attached to the line.

### 2.3 Levels

Levels follow how often a line can occur, not how important it is.

| Level | What it holds | When you want it |
|---|---|---|
| `ERROR` | A failure the current attempt cannot recover from: a row that failed for good, a worker that died, a startup check that failed. | Always. Alert on it. |
| `WARN` | Abnormal but handled automatically: a publish attempt that will be retried, a lost race for a bucket. | Always. Worth a look if it recurs. |
| `INFO` | Once per lifecycle or coordination event: relay started and stopped, buckets claimed or released, periodic progress. | Normal operation. |
| `DEBUG` | Once per poll or batch cycle: what each claim, reclaim or flush did. | While troubleshooting a stalled relay. |
| `TRACE` | Once per row. | Rarely, on a narrow scope, because a busy outbox floods it. |

Set the level by logger name. The relay's engine logs under `com.codingful.tandem.jdbc` and the
Kafka publisher under `com.codingful.tandem.kafka`:

```yaml
logging:
  level:
    com.codingful.tandem: INFO            # the default for everything Tandem
    com.codingful.tandem.jdbc: DEBUG      # while investigating a stalled relay
```

### 2.4 The lines worth knowing

The message text is stable, so these are safe to search, count and alert on. The tail carries the
identifiers noted for each.

| Message | Level | Meaning and what to do |
|---|---|---|
| `Starting relay` / `Relay stopped` | `INFO` | The relay's lifecycle, with the instance id and coordination mode. A relay that keeps restarting shows here first. |
| `Relay worker progress` | `INFO` | A periodic heartbeat of counts (`processed`, `ok`, `ko`), every `tandem.relay.log-every-rows` outcomes. |
| `Relay claimed buckets`, `Relay released ...` | `INFO` | Bucket ownership changing under `LEASE`. Useful to date a rebalance. |
| `Outbox row dispatch failed, retrying` | `WARN` | One publish attempt failed and will be retried with backoff. Carries `rowId`, `aggregateId`, `attempts`. Occasional is normal; a run of them for the same row is not. |
| `Outbox row dispatch failed permanently` | `ERROR` | The row is now `FAILED` and its aggregate is blocked. Read the attached exception, fix the cause, then replay (see [Admin API](admin-api.md#33-recovering-messages)). Carries `rowId`, `aggregateId`. |
| `Publishing outbox row failed`, `Sending outbox row failed synchronously`, `Encoding outbox row failed` | `ERROR` | The Kafka side of a failure, with `rowId` and `topic`. The exception says whether the broker refused it, the topic is missing, or the row could not be encoded. |
| `Relay worker died; restarting`, `Relay worker iteration failed` | `ERROR` | A worker thread hit an unexpected error and is supervised back to life. Carries `workerIndex`. If it repeats, the exception tells you why. |
| `Lease reclaim failed`, `Cleanup failed`, `Bucket heartbeat failed`, `Relay control refresh failed`, `Reading relay metrics failed` | `ERROR` | A housekeeping task could not reach the database. Usually a database availability problem, and it clears when the database is back. |
| `Published an aggregate's events out of insert order ...` | `ERROR` | Events of one aggregate were published out of order, which means concurrent writers to it are not serialized. A write-side bug that Tandem cannot repair. Carries `rowId`, `aggregateId` and `workerId`. |
| A bucket count mismatch, or the row lease being shorter than the producer's delivery timeout | `ERROR` | A startup check failed and the relay refuses to run. The message names the values. |
| `Outbox message replayed`, `... discarded`, `Relay pause applied`, `... resume applied`, `Relay bucket released` | `INFO` | The audit trail of the [Admin API](admin-api.md), with the acting user when your application authenticates one. |

### 2.5 What is never logged

Payload bodies, header values, credentials, JDBC URLs with passwords and bound SQL parameters never
reach a log line, and the string form of Tandem's own types follows the same rule: an outbox message
prints its payload size and header names, not their contents. What logs do carry (row ids, aggregate
ids and types, buckets, topics, counts, timings, and the correlation id) is deliberately the set that
is safe to ship to a log platform.

### 2.6 From a log line to the row

A log line names a `rowId`. Look the row up with the Admin API or the CLI to see its status, its
attempts and its last error:

```bash
tandem-cli outbox get 1042
```

The reverse also works: from a ticket that only has a correlation id, find the rows first
(`tandem-cli outbox search --correlation-id <id>`) and then their log lines.

---

## 3. Metrics

Metrics are published through a small interface with no library behind it by default. The optional
**`tandem-micrometer`** module binds it to a Micrometer `MeterRegistry`, so whatever backend your
application already exports to (Prometheus, OTLP, Datadog and so on) receives Tandem's meters. They
are relay-side: enable them **where the relay runs**, and the write side never inherits Micrometer.

### 3.1 Turning them on

=== "Spring Boot"

    Add the module next to your usual Micrometer setup. Tandem's meters are registered on the
    `MeterRegistry` bean your application already has, with no property to set.

    ```kotlin
    dependencies {
        implementation("com.codingful:tandem-micrometer")
        implementation("org.springframework.boot:spring-boot-starter-actuator")
        implementation("io.micrometer:micrometer-registry-prometheus")   // or your registry
    }
    ```

    ```yaml
    management:
      endpoints:
        web:
          exposure:
            include: prometheus
    ```

    If there is no `MeterRegistry` bean, or `tandem-micrometer` is not on the classpath, Tandem
    reports nothing and costs nothing. A `TandemMetrics` bean of your own takes precedence over the
    one Tandem would create.

=== "Plain Java"

    Pass the adapter to the relay. It takes the seven-argument form of the constructor, where
    everything but the metrics is what the shorter one already uses:

    ```java
    WorkerPool relay = new WorkerPool(store, kafka, relayConfig,
        new MicrometerTandemMetrics(meterRegistry),
        Clock.systemUTC(), BackoffStrategy.fullJitter(),
        BucketSource.embedded(relayConfig.bucketCount()));
    relay.start();
    ```

    `MicrometerTandemMetrics` is in `com.codingful.tandem.micrometer`.

The relay reads the database-derived gauges periodically (every 10 seconds by default,
`tandem.relay.metrics-interval`), so a reading is a trend, not a live count.

### 3.2 What is published

Names are Micrometer's, with dots. Prometheus renders them with underscores and adds its own
suffixes, shown in the second column.

| Meter (Prometheus name) | Type | What it says |
|---|---|---|
| `tandem.outbox.lag.count` (`tandem_outbox_lag_count`) | Gauge | Rows waiting to be published. |
| `tandem.outbox.lag.age_seconds` (`tandem_outbox_lag_age_seconds`) | Gauge | Age of the oldest waiting row. The most useful single number. |
| `tandem.outbox.published` (`tandem_outbox_published_total`) | Counter | Rows published and acknowledged. Take a rate over it in your backend. |
| `tandem.outbox.publish.latency` (`tandem_outbox_publish_latency_seconds_*`) | Histogram | Time from a row's creation to its Kafka acknowledgement. A histogram, so your backend computes correct percentiles across relay instances. |
| `tandem.outbox.failed.count` (`tandem_outbox_failed_count`) | Gauge | Rows in `FAILED` right now. |
| `tandem.outbox.blocked.count` (`tandem_outbox_blocked_count`) | Gauge | Waiting rows stuck behind a `FAILED` row of the same aggregate. Included in `lag.count` too. |
| `tandem.outbox.retry.count` (`tandem_outbox_retry_count_total`) | Counter | Retry attempts. |
| `tandem.outbox.order_violation.count` (`tandem_outbox_order_violation_count_total`) | Counter | Events published behind a later one of the same aggregate. |
| `tandem.outbox.lease_expired.count` (`tandem_outbox_lease_expired_count_total`) | Counter | Rows reclaimed from an expired lease, a proxy for a worker that crashed mid-send. |
| `tandem.outbox.workers.active` (`tandem_outbox_workers_active`) | Gauge | Live relay workers. Alive is not the same as progressing. |
| `tandem.outbox.workers.cycle_age_seconds` (`tandem_outbox_workers_cycle_age_seconds`) | Gauge | Time since the slowest live worker last finished a cycle. Rises when a worker is alive but stuck. |
| `tandem.outbox.bucket.uncovered` (`tandem_outbox_bucket_uncovered`) | Gauge | Buckets with waiting rows and no owner. Reported under `LEASE` coordination. |
| `tandem.relay.config.invalid` (`tandem_relay_config_invalid`) | Gauge | Set to `1`, tagged with the failed check, just before the relay aborts on a bad configuration. |

The publish latency histogram has a ceiling of 5 minutes by default; a longer wait is still counted
but cannot be placed precisely. Change it with `tandem.metrics.max-publish-latency` if you need to.

### 3.3 Alerts worth having

Read the metrics together, because a single value is ambiguous. In particular, a `FAILED` row blocks
its own aggregate for good, so one poison message keeps `lag.count` above zero and `lag.age_seconds`
climbing forever while everything else is delivered. An age alert that ignores that latches on after
the first failure and never clears.

| Alert on | Means | Do |
|---|---|---|
| `lag.age_seconds` high **and `blocked.count` = 0** | The relay is stalled, or delivery is not keeping up with writes. | Check the relay's log, the broker and the database. |
| `blocked.count` > 0 | Not a relay fault: an aggregate is stuck behind a failed event. | Find it with `failed.count`, then replay or discard. |
| `failed.count` > 0 | A row exhausted its attempts and needs an operator. | The `ERROR` log names the row; see [Admin API](admin-api.md#5-a-worked-case-recovering-a-failed-event). |
| `increase(order_violation.count)` > 0 | A write-side ordering bug. It never clears on its own. | Read the `ERROR` log for the aggregate, then fix the writers. |
| `workers.cycle_age_seconds` high | A worker is alive but not progressing. | Look for a database call or a broker handshake that never returns. |
| `lease_expired.count` growing | Workers are dying mid-send. | Check the JVM and the relay's `ERROR` logs. |
| The lag series **absent or frozen** | The relay itself is down. | See below. |

As PromQL, for Prometheus:

```
# stalled, not merely blocked
(tandem_outbox_lag_age_seconds > 60) and on() (tandem_outbox_blocked_count == 0)

# an aggregate is stuck behind a failure
tandem_outbox_blocked_count > 0

# writers to one aggregate are not serialized
increase(tandem_outbox_order_violation_count_total[15m]) > 0

# the relay is not reporting at all
absent(tandem_outbox_lag_age_seconds)
```

Thresholds and label matching are yours to adapt to your deployment.

### 3.4 What the metrics cannot tell you

- **A dead relay reports nothing.** The lag gauges are read by the relay, so when it is down the
  backlog grows unseen and the series freeze or vanish. That is why the alert above is on the
  *absence* of a fresh reading, and why a reading from outside the relay is the natural complement:
  the [Admin API's summary](admin-api.md#31-health-of-the-outbox), which queries the same table.
- **A short burst is invisible.** Readings are periodic, so a backlog that builds and drains between
  two of them never appears. The gauges are for trends and alerts, not for diagnosis.
- **A zero order-violation count proves nothing.** It only counts what can be detected in process at
  publish time. A non-zero value is always real; a zero one is not evidence that writers are
  serialized. That has to be established by design, as
  [Adoption Guide §1](adoption.md#1-the-one-precondition-that-can-actually-block-you) describes.
- **`config.invalid` may never be scraped.** It is set once, just before the process exits, so a
  scraper can miss it. The relay's `ERROR` log line at the same moment is the dependable channel.

---

## 4. Distributed traces

### 4.1 The problem, and what you get

A trace follows one operation across services. The outbox breaks the usual chain: the event is
produced inside your transaction, where the trace context is live, but sent to Kafka later by the
relay, on another thread and perhaps another process, where that context is gone. Left alone, the
consumer's trace would start from nothing and look unrelated to the request that caused it.

Tandem fixes this in two steps. At write time it **captures** the current trace context and stores
it with the event. At publish time the relay puts it on the Kafka message, so a consumer continues
the same trace. Optionally, the relay also emits one span of its own.

### 4.2 Two levels, both off by default

| Level | What it does | Where it runs |
|---|---|---|
| **Propagation** | Captures the trace context and the correlation id when the event is written, and carries them to Kafka. Consumers continue the trace. | The write side. |
| **Relay publish span** | Adds one `tandem.relay.publish` span per published event, marked with the real send. This makes the wait in the outbox and any retries visible on the trace. | The relay. |

Propagation is the base; the publish span builds on it. Without a captured context there is nothing
to attach the span to, and Tandem will not invent a new, disconnected trace for it: **an event with
no captured trace context gets no publish span.** The two are switched on separately because in a
split deployment they run in different processes and cost different things: a header on the row,
against export volume in your tracing backend.

### 4.3 Turning them on

=== "Spring Boot"

    Tracing rides on your application's own Micrometer Tracing setup: a bridge to OpenTelemetry or
    Brave and an exporter, as you would configure it for any service. Tandem adds two switches.

    On the **write side**, in the application that inserts events:

    ```yaml
    tandem:
      tracing:
        enabled: true
        # correlation-id-mdc-key: correlationId   # the default
    ```

    On the **relay**, for the publish span:

    ```yaml
    tandem:
      tracing:
        publish-span: true
    ```

    Both keys live under `tandem.tracing`, so an application that writes and relays sets both there.
    Neither is switched on just because a tracing library is present.

    With Micrometer Tracing on the classpath, `enabled: true` captures the distributed trace context
    in whichever propagation format you configured, together with the correlation id. Without a
    tracing library it captures the correlation id alone.

=== "Plain Java (OpenTelemetry)"

    Add the optional module, which redistributes only the OpenTelemetry API; the SDK stays your own.

    ```kotlin
    implementation("com.codingful:tandem-tracing-otel")
    ```

    On the **write side**, hand the repository a propagator built from your `OpenTelemetry`:

    ```java
    TracePropagator propagator = TracePropagator.composite(
        new OtelTracePropagator(openTelemetry),
        TracePropagator.fromTandemContext());          // the correlation id, see §4.4

    OutboxRepository outbox = new JdbcOutboxRepository(dataSource, bucketCount, propagator);
    ```

    On the **relay**, give the Kafka publisher a span recorder for the publish span:

    ```java
    OutboxDispatcher kafka = new KafkaRelay(
        Map.of("bootstrap.servers", "localhost:9092"),
        TopicRouter.kebabWithSuffix("-topic"),
        KafkaRelayConfig.of("/orders/service"),
        new OtelTandemSpanRecorder(openTelemetry));
    ```

    The classes are in `com.codingful.tandem.tracing.otel`. `TracePropagator` is in
    `com.codingful.tandem.core.port`.

!!! warning "Only sampled traces are exported"
    Whether a trace is sampled is decided when the event is written and travels inside the captured
    context, so it is frozen for the life of the row: published seconds later, retried an hour
    later, or replayed next year, it stays sampled or not as it was. Frameworks often sample only a
    fraction of traces by default (Spring Boot does), so a missing trace for one event is often
    just sampling. The correlation id, unlike the trace, is written for every row.

### 4.4 The correlation id, with or without a tracing library

The correlation id is an application-level identifier: it groups everything one request or saga
produced, even across several traces. It needs no tracing library.

=== "Spring Boot"

    Put your correlation id in the logging MDC, as most services already do for their logs, under the
    key `tandem.tracing.correlation-id-mdc-key` names (`correlationId` by default). Tandem reads it
    when the event is written. To set it explicitly instead, use the static `TandemContext` API below.

=== "Plain Java"

    ```java
    TandemContext.setCorrelationId(requestId);
    try {
        // ... your transaction and outbox insert
    } finally {
        TandemContext.clear();
    }
    ```

    `TandemContext` is in `com.codingful.tandem.core`. It is thread-local, so clear it when the work
    ends, or a stale id leaks into whatever that thread handles next.

Once captured, the id is stored on the row (in its own indexed column), sent to Kafka as the
`correlation-id` header, and set on the relay's publish span. It is searchable through the Admin API
and the CLI:

```bash
tandem-cli outbox search --correlation-id 9f3c1e2a --status FAILED
```

One correlation id normally matches many rows across several aggregates, so combine it with a status
to narrow the page.

### 4.5 What the consumer receives

The relay copies the stored trace headers onto the Kafka record unchanged: **`traceparent`**,
optionally **`tracestate`**, and **`correlation-id`**. `traceparent` is the standard W3C header, so
any consumer instrumented with OpenTelemetry (or Micrometer Tracing) continues the trace with no
Tandem dependency and nothing to configure beyond its own Kafka instrumentation. A consumer that is
not instrumented can read the headers itself and put the correlation id into its own logs.

### 4.6 Reading a trace

A trace that crosses the outbox has three parts:

```
write (your transaction)   [outbox wait: no span]   tandem.relay.publish   consumer
```

- **Your write span** ends at commit.
- **The outbox wait** is not a span. Tandem deliberately emits nothing for it; you read it off the
  waterfall as the gap between the write span ending and `tandem.relay.publish` starting. It is how
  long a committed event sat before the relay sent it.
- **`tandem.relay.publish`** covers the real send to Kafka, one span per event and parented to the
  context captured on that event. It ends when Kafka acknowledges, or with an error if it does not.
- **The consumer span** continues from the propagated `traceparent`.

The publish span carries identifiers only, never payloads or header values:

| Attribute | Value |
|---|---|
| `tandem.outbox.row_id` | The outbox row id, the same one the logs and the Admin API use. |
| `tandem.aggregate.type`, `tandem.aggregate.id` | The aggregate. |
| `tandem.attempts` | The delivery attempt. Above one means it is a retry. |
| `tandem.topic` | The destination topic. |
| `tandem.correlation_id` | The correlation id, when there is one. |

The row id and the correlation id are the join keys: from a slow or failed publish span you go to
the row with `tandem-cli outbox get <row id>` or to its siblings with `--correlation-id`, and from a
log line or a ticket you go the other way.

### 4.7 Limits to know

- **The publish span needs a format it can read.** It finds its parent through your propagator, so
  an application configured for B3 rather than W3C gets propagation but no publish span.
- **A batch has no span of its own.** A batch of rows claimed together comes from unrelated
  transactions, each with its own trace, so Tandem emits one span per event and never one per batch
  or per poll. Spans that a host framework's automatic instrumentation wraps around the relay's own
  loop are yours to exclude.
- **The tracing backend complements the Admin API; it does not replace it.** A trace exists only for
  the sampled fraction, while the outbox row and its correlation id exist for every event.

---

## 5. Putting them together: an incident

An alert fires: `blocked.count` is above zero.

1. **Metrics** say it is not the relay: it is delivering, but an aggregate is stuck.
2. **Logs** name the culprit. Search the relay's log for `Outbox row dispatch failed permanently`:
   the line carries `rowId` and `aggregateId`, and the attached exception says why (a missing topic,
   a rejected message).
3. **The row** confirms it: `tandem-cli outbox get <rowId>` shows `FAILED` and the last error.
4. **The trace**, if you need the business context, is one search away: the row's correlation id
   finds the request that produced it, and the publish span shows the attempts.
5. **Recover**: fix the cause, then replay. `tandem-cli outbox replay <rowId>`.

---

## 6. Checklist

- [ ] The relay's logs reach your log platform, at `INFO` or above, and `ERROR` is alerted on.
- [ ] `tandem-micrometer` is on the relay, and the meters reach your backend.
- [ ] Alerts exist for stalled delivery (with the blocked qualifier), blocked, failed, order
  violations, and the relay not reporting.
- [ ] A correlation id is set where events are written, if you want to search by it.
- [ ] Trace propagation is enabled on the write side, and the publish span on the relay if you want
  the outbox wait visible.
- [ ] Your tracing backend's sampling is what you expect for the traces you need.
