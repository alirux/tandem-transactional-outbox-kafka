# The Relay Image

---

## 0. Who this page is for

You want the relay to run **as its own process**, next to your application rather than inside it, and
you would rather deploy a prebuilt container than write and maintain a Spring Boot application whose
only job is to host the relay. `tandem-relay` is that application, packaged as a container image and an
executable jar. This page covers running it: what to configure, which ports and probes it offers, how
to stop it without a kill, how to run several replicas, and how its versions work.

!!! question "The question this page answers"
    **How do I deploy and operate the prebuilt relay, and what do I have to configure myself?**

It assumes the split deployment described in
[Reliability and Topology](reliability.md#32-split): your application writes events, and the relay
reads them from the same database and publishes them to Kafka.

---

## 1. What the image is

The image runs the same relay you would embed with `tandem-spring-relay`, inside an application that
binds nothing of its own: every relay setting is the library's, configured as
[Configuration Reference](configuration.md) describes, plus a few deployment defaults (ports, probes,
a database socket timeout). It contains:

- the relay, publishing to **Kafka** with the default CloudEvents envelope;
- the **Admin API**, off unless you turn it on (§9);
- **Prometheus metrics** for every Tandem meter (see [Observability](observability.md#3-metrics));
- the **PostgreSQL** driver.

| Use the image when | Build your own relay application when |
|---|---|
| You want the split deployment and its defaults suit you | You publish to RabbitMQ: the connector needs a bean of yours ([Publishing to RabbitMQ](rabbitmq.md)) |
| Configuration through properties is all you need | You replace a default with your own bean: a topic router, an envelope, a metrics registry other than Prometheus |
| | You want the relay inside your application anyway (the default, [Getting Started](getting-started.md#6-step-4-start-the-relay)) |

The image never touches your schema: you apply it yourself, once, before the first start (§8.1).

---

## 2. Getting it

### 2.1 Released images

Released images are published to the GitHub Container Registry, for `linux/amd64` and `linux/arm64`:

```bash
docker pull ghcr.io/alirux/tandem-relay:<version>
```

Each release also attaches the executable jar it was built from to its
[GitHub Release](https://github.com/alirux/tandem-transactional-outbox-kafka/releases), as
`tandem-relay-<version>.jar`. It runs on Java 25 with `java -jar` and takes exactly the configuration
the image takes: the image is that jar on Eclipse Temurin's Java runtime.

### 2.2 Building it from the repository

From a clone of the repository, with Docker running:

```bash
./gradlew :tandem-relay:pinnedBootJar
docker build $(./gradlew -q :tandem-relay:imageBuildArgs) --tag tandem-relay:local apps/tandem-relay
```

The first command builds the jar on the library release the image is pinned to (§11.1), taken from
Maven Central. The second packages it for your machine's architecture. `imageBuildArgs` prints the two
values the image is labelled with, its own version and the library release inside it, and the build
refuses a jar that says otherwise.

### 2.3 Trying it

The repository has a Compose file that runs the image the way this page recommends: a PostgreSQL with
the schema applied, a single-node Kafka, and two relay replicas sharing the work under `LEASE`, with the
wakeup on.

```bash
docker compose -f apps/tandem-relay/docker-compose.example.yml up --wait
curl http://127.0.0.1:8081/actuator/health/readiness      # relay-1
curl http://127.0.0.1:8082/actuator/health/readiness      # relay-2
docker compose -f apps/tandem-relay/docker-compose.example.yml down -v
```

`--wait` returns once both replicas report ready. It uses the image you built in §2.2; to run a
released one instead, set `TANDEM_RELAY_IMAGE=ghcr.io/alirux/tandem-relay:<version>` first. The example
publishes neither PostgreSQL nor Kafka on your machine. To watch events go through, publish the
`postgres` service's port, point an application's write side at that database, and read the topic from
inside the broker's container:

```bash
docker compose -f apps/tandem-relay/docker-compose.example.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-topic --from-beginning
```

---

## 3. Roles and ports

### 3.1 One image, three roles

The same image runs as a relay, as an Admin API, or as both, chosen with the library's own switches:

| Role | `TANDEM_RELAY_ENABLED` | `TANDEM_ADMIN_ENABLED` | Use it for |
|---|---|---|---|
| **Relay** (default) | `true` (default) | `false` (default) | The split relay. |
| **Admin API** | `false` | `true` | A standalone Admin API over the outbox database. Needs no Kafka setting. |
| **Both** | `true` | `true` | Small deployments: one process, one connection pool. |

A process with neither role refuses to start, with a message naming both settings: it would otherwise
start, report ready and do nothing.

### 3.2 Two ports

| Port | Serves |
|---|---|
| **8080** | The Admin API, under `/tandem/admin/v1`, when it is enabled. In the relay role nothing is mapped on it and every path answers 404. |
| **8081** | Health and probes (`/actuator/health/...`), build information (`/actuator/info`) and the Prometheus scrape (`/actuator/prometheus`). Nothing else is exposed. |

They are separate so that each can be exposed and firewalled on its own: the scrape and the probes to
your monitoring, the Admin API to operators only. `SERVER_PORT` and `MANAGEMENT_SERVER_PORT` move them.

---

## 4. Probes

### 4.1 Readiness

`GET /actuator/health/readiness` on port 8081 answers 200 while the relay delivers and 503 when it
does not. Its `tandemRelay` component reports the relay's own state, read from memory, so a probe never
queries the database:

| What the relay is doing | Status | Probe |
|---|---|---|
| Running, every worker completing cycles | `UP` | 200 |
| Running with fewer workers than configured (a died worker is restarted by itself) | `UP`, the deficit in the details | 200 |
| Paused through the Admin API | `UP`, `paused: true` in the details | 200 |
| A worker has not completed a cycle for 60 seconds | `DOWN` | 503 |
| No worker alive, or stopped | `DOWN` | 503 |
| Draining at shutdown | `OUT_OF_SERVICE` | 503 |
| No relay in this process (the Admin API role) | `UNKNOWN`, which does not lower the overall status | 200 |

A worker stops completing cycles whatever the cause: the database is unreachable, the broker's metadata
is unavailable, or a call never returns. `DOWN` therefore means "this relay is not delivering", and the
logs say why. The details always name the instance (`instanceId`), its coordination mode, the workers
configured and alive, and the age of the oldest worker's last cycle (`cycleAgeSeconds`).

What readiness is good for depends on the role:

- **Relay role.** Nothing sends traffic to a relay, so readiness does not take it "out of service". It
  **gates a rollout**, since a new version that cannot deliver never replaces the old one, and it is
  the signal to **alert** on, together with the metrics in
  [Observability](observability.md#33-alerts-worth-having).
- **Both roles.** A relay that is not delivering makes the pod unready, which also removes the Admin API
  from its Service at the moment you need it. If you rely on the Admin API in production, run the two
  roles as two deployments of the same image.

The 60 seconds are `management.health.tandem-relay.stalled-after`
(`MANAGEMENT_HEALTH_TANDEM_RELAY_STALLED_AFTER`). An idle worker completes a cycle only once per
`tandem.relay.poll-interval`, so the relay refuses to start unless the threshold is more than 2.4 times
the poll interval. Raise it if you raise the poll interval a lot, as you might with the wakeup (§7).

### 4.2 Liveness

`GET /actuator/health/liveness` is Spring Boot's own: the process is up and answering. The relay's
state is deliberately not part of it. A liveness failure restarts the container, and a restart cures
none of the usual reasons a relay stops delivering: with a broker missing it would restart a healthy
process in a loop. The one stall a restart would cure, a database call that never returns, is cut
instead by a socket timeout on the connection (§8.2).

**Use the two group paths, never `/actuator/health` itself**: that one also runs Spring Boot's database
check on every call, which is useful to a person and wasteful at probe frequency.

### 4.3 On Kubernetes

```yaml
containers:
  - name: tandem-relay
    image: ghcr.io/alirux/tandem-relay:<version>
    ports:
      - name: management
        containerPort: 8081
    readinessProbe:
      httpGet: { path: /actuator/health/readiness, port: management }
      periodSeconds: 10
    livenessProbe:
      httpGet: { path: /actuator/health/liveness, port: management }
      periodSeconds: 10
    startupProbe:
      httpGet: { path: /actuator/health/liveness, port: management }
      periodSeconds: 2
      failureThreshold: 60
```

The image has no `HEALTHCHECK` of its own, and no `curl` or `wget` either: an orchestrator probes it
from outside. The Compose example shows a healthcheck that needs nothing but the `bash` the image
already has.

---

## 5. Configuring it

### 5.1 From environment variables

Every setting the image needs can be given as an environment variable, which is how a container is
usually configured. The name is the property's, in capitals, with dots and dashes turned into
underscores: `tandem.relay.max-attempts` is `TANDEM_RELAY_MAX_ATTEMPTS`.

Required in the relay role:

| Variable | What it is |
|---|---|
| `SPRING_DATASOURCE_URL` | The outbox database, `jdbc:postgresql://host:5432/db` |
| `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD` | Its credentials |
| `TANDEM_KAFKA_SOURCE` | The CloudEvents `source` of every event, a URI naming the producer |
| `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | The Kafka brokers |

Without them the relay does not start. The Admin API role needs the three `SPRING_DATASOURCE_*`
values only.

The ones you will usually set as well:

| Variable | Default | See |
|---|---|---|
| `TANDEM_OUTBOX_BUCKET_COUNT` | `256` | Must be the write side's value ([Reliability](reliability.md#4-the-bucket-count)) |
| `TANDEM_RELAY_COORDINATION` | `SINGLE` | §6 |
| `TANDEM_RELAY_INSTANCE_ID` | derived | §6.2 |
| `TANDEM_OUTBOX_WAKEUP` | `none` | §7 |
| `TANDEM_ADMIN_ENABLED` | `false` | §9 |

**Kafka producer settings** are reachable the same way. `tandem.kafka.producer` is passed to the
producer as it is, and the words after `TANDEM_KAFKA_PRODUCER_` become Kafka's own dotted name:

```bash
TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS=broker-1:9093,broker-2:9093   # bootstrap.servers
TANDEM_KAFKA_PRODUCER_SECURITY_PROTOCOL=SASL_SSL                      # security.protocol
TANDEM_KAFKA_PRODUCER_SASL_MECHANISM=SCRAM-SHA-512                    # sasl.mechanism
TANDEM_KAFKA_PRODUCER_COMPRESSION_TYPE=lz4                            # compression.type
```

The settings Tandem protects (`enable.idempotence`, `acks`, `max.in.flight.requests.per.connection`)
are checked at startup as usual ([Advanced Integration](advanced-integration.md)).

A setting that cannot be spelled as a variable name can still be given in `SPRING_APPLICATION_JSON`,
or in a YAML file mounted into the container and named in `SPRING_CONFIG_ADDITIONAL_LOCATION`.

### 5.2 Secrets as files

Passwords and the SASL JAAS configuration are better mounted as files than set as variables. Spring
Boot reads a directory of files as properties, one file per property, named after it:

```
/run/secrets/tandem/
├── spring.datasource.password
└── tandem.kafka.producer.sasl.jaas.config
```

```bash
SPRING_CONFIG_IMPORT=configtree:/run/secrets/tandem/
```

On Kubernetes, mount a Secret whose keys are those names at that path. Only `health`, `info` and
`prometheus` are exposed on the management port, so no endpoint can print the configuration back.

### 5.3 Memory

`JAVA_TOOL_OPTIONS` is `-XX:MaxRAMPercentage=75`, so the heap may grow to three quarters of the
container's memory limit. Give the container a limit, and set the variable yourself to change or add JVM flags;
keep the percentage when you do:

```bash
JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
```

The JVM prints the value it picked up as the first line of the output; that line is the JVM's, not a
warning.

---

## 6. Stopping it, and running several

### 6.1 The grace period

On `SIGTERM` the relay stops in two steps:

1. It stops taking work, waits for each worker to finish its current cycle (up to 10 seconds per worker
   still busy), records what Kafka has acknowledged and, under `LEASE`, releases its buckets so another
   replica takes them at once.
2. The Kafka producer closes, which waits for the sends still in flight, up to the producer's
   `delivery.timeout.ms` (30 seconds by default). Those rows are delivered again later, by whichever
   replica claims them after their lease: a duplicate, never a loss.

The container runtime must allow for both, or it ends the stop with a kill. **Size the grace period as
the workers' wait plus `delivery.timeout.ms`: 60 seconds with the defaults.** The runtimes' own defaults
are shorter.

| Runtime | Default | Set |
|---|---|---|
| Kubernetes | 30 s | `terminationGracePeriodSeconds: 60` in the pod spec |
| Docker Compose | 10 s | `stop_grace_period: 60s` on the service |
| `docker run` | 10 s | `--stop-timeout 60` |

If you raise `delivery.timeout.ms`, raise the grace period with it. A kill before the end costs more duplicates and, under `LEASE`,
leaves the replica's buckets unowned until their leases expire (30 seconds by default). A clean stop
ends with exit code 143, the JVM's code for `SIGTERM`; 137 means the runtime had to kill it.

### 6.2 More than one replica: `LEASE` and the instance id

The image keeps the library's default coordination, `SINGLE`, which assumes **exactly one** relay. Two
`SINGLE` relays on one database corrupt nothing, but they do the same work twice and blind the
out-of-order detector ([Reliability](reliability.md#33-single-and-lease)).

- **One replica**: keep `SINGLE`, and on Kubernetes deploy it with `strategy: Recreate`. A rolling update
  starts the new pod before stopping the old one, which is two relays for a moment.
- **More than one**: set `TANDEM_RELAY_COORDINATION=LEASE` on every replica. Replicas then share the
  buckets through the database, and scaling up or down needs no other change.

Under `LEASE`, **give every replica its instance id**. The derived one keeps the first 30 characters of
the host name, and a pod's name differs from its siblings only at the end; the process id is the same
in every container. Two replicas with the same id would both believe they own the same buckets. On
Kubernetes, take it from the pod name:

```yaml
env:
  - name: TANDEM_RELAY_COORDINATION
    value: LEASE
  - name: TANDEM_RELAY_INSTANCE_ID
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

On Compose, give each service its own value, as the example does. The id is at most 64 characters, and
it is what the logs, the lease table and the Admin API show, so a readable one pays off.

---

## 7. The wakeup

With `TANDEM_OUTBOX_WAKEUP=pg-notify` a row written into a quiet bucket is claimed at once instead of at
the next poll. It is a setting of the pair, not of the image: **the write side must set the same value**
(`tandem.outbox.wakeup: pg-notify` in your application), or nothing is ever signalled. A mismatch costs
latency only, since the relay keeps polling.

The relay then holds one extra database connection, listening. A connection pooler in **transaction
pooling** mode (PgBouncer's default, RDS Proxy, Supavisor's pooled port) silently breaks it: give the
relay a direct connection, or one through session pooling. See
[Database Compatibility](compatibility.md#5-the-operational-caveats-which-matter-more-than-the-sql).

---

## 8. The database

### 8.1 The schema is yours to apply

The image **does not create, migrate or check** the `tandem_*` tables. Whoever owns the database
applies the schema, before the first start, with the same files an embedded relay uses
([Getting Started](getting-started.md#4-step-2-create-the-schema)):

- a **new database**: `schema/postgres/tandem-baseline.sql`, the whole schema in one script, or the
  Liquibase changelog `schema/postgres/changelog/db.changelog-master.xml`;
- an **existing database**, before an image with a newer library release: the changesets you do not
  have yet, as [Testing and Upgrades](testing-and-upgrades.md#22-upgrading-step-by-step) describes.
  The release notes of the image say when the library release it moves to brings one.

Keeping DDL out of the relay means its database user needs no right to change the schema. It needs
read and write access to the `tandem_*` rows, plus `INSERT` on `tandem_meta`: on the very first start
against a database, the relay records the bucket count there.

If the tables are missing, the relay fails to start under `LEASE`, naming them. Under `SINGLE` it
starts, logs an `ERROR` each cycle and turns not ready after the stall threshold (§4.1).

### 8.2 A socket timeout on every connection

The image sets the PostgreSQL driver's `socketTimeout` to **30 seconds**. Without one, a connection that
dies without closing (a failover, a dropped NAT entry) leaves a worker waiting forever, still holding its
buckets. With it the call fails, the pool replaces the connection, and the worker carries on, well before
readiness reports a stall. Nothing the relay or the Admin API runs comes close to 30 seconds.

To change it, put it in the URL, which takes precedence over the image's default:

```bash
SPRING_DATASOURCE_URL='jdbc:postgresql://db:5432/orders?socketTimeout=60'
```

---

## 9. The Admin API in the image

It is **off by default**. `TANDEM_ADMIN_ENABLED=true` turns it on, on port 8080, and the image logs a
`WARN` at every start while it is on, because it is then an **unauthenticated** management surface:
replay, discard and pause change delivery, and nothing checks who asks
([Admin API](admin-api.md#13-security-is-yours)). The image ships no default credential. Keep port 8080
on an internal network, or behind a gateway that authenticates, and never publish it.

For production, run it as its own deployment of the same image, with `TANDEM_RELAY_ENABLED=false` and
`TANDEM_ADMIN_ENABLED=true`: it then needs no Kafka setting, its readiness does not depend on a relay,
and [`tandem-cli`](cli.md) can point at it.

---

## 10. What is inside

| | |
|---|---|
| Base | Eclipse Temurin's Java 25 runtime (`eclipse-temurin:25-jre`) |
| Application | Spring Boot 4.1, the Kafka client Spring Boot manages (4.2), the PostgreSQL driver |
| Runs as | User `10001`, group `10001`, not root |
| Entrypoint | `java -jar application.jar`, in `/app` |
| Licences | `/licenses/LICENSE` and `/licenses/THIRD-PARTY-NOTICES.md` |

The Kafka 4.x client talks to brokers from Kafka 2.1 on. Of this table, only the user id is a promise
the image's version keeps (§11.2); the rest moves with patch releases.

---

## 11. Versions

### 11.1 The image's own version

The image is versioned **independently of the library**, on tags `relay-v<version>`, because it is more
than the library: a Java runtime, a Spring Boot, a base image, each patched on its own schedule. A fix in
any of them is a new image and nothing else.

Each image contains **exactly one library release**, built from Maven Central, never from unreleased
code. Find out which:

```bash
docker image inspect --format '{{ index .Config.Labels "com.codingful.tandem.library.version" }}' ghcr.io/alirux/tandem-relay:<version>
curl http://127.0.0.1:8081/actuator/info      # "build": { "version": ..., "tandem": { "library": ... } }
```

The startup log states both too, in its line `Tandem relay application starting`.

| Image | Tandem library | Status |
|---|---|---|
| `0.1.0` | `0.11.1` | Released 2026-10-03 |

A new library release reaches the image only once it is on Maven Central, so an image release always
follows a library release, sometimes after a while. Running the write side, the relay image and the
Admin API at different library versions against one database is safe, as for any split deployment
([Testing and Upgrades](testing-and-upgrades.md#2-upgrades)).

### 11.2 What a version number promises

The image's contract is what your manifests depend on:

| Covered | Not covered |
|---|---|
| The image name | The Spring Boot, Java and base image versions |
| The two ports, and which serves what | The exact set of health details |
| The probe paths | The layout inside the container |
| The default role | The format of log lines |
| The user id it runs as | |
| Being configured by the library's `tandem.*` settings and Spring Boot's own | |

| Change | Version |
|---|---|
| A rebuild on a patched base image, Spring Boot or library dependency; a fix in the image | Patch |
| A move to a library patch release | Patch |
| A move to a library minor release, or a new capability of the image | Minor |
| A covered item changes, or the library release inside asks something of you (a schema migration to apply first, a change in the published events) | Breaking: a major version, or a minor before 1.0 |

`latest` follows the newest release without a pre-release suffix, so a release candidate is never
what `latest` pulls. **Pin a version in production**: a version tag always names the same image, and a
rebuild is published as a new patch rather than over an existing tag. Each release's notes state the
library release it contains, on the
[Releases](https://github.com/alirux/tandem-transactional-outbox-kafka/releases) page under
`relay-v<version>`.
