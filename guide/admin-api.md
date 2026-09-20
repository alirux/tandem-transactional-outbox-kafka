# Admin API

---

## 0. Who this page is for

Your events are flowing (see [Getting Started](getting-started.md)) and now you want to **see the
outbox and act on it**: is anything stuck, why, can I publish that event again, can I pause the
relay while I fix something. The Admin API answers those questions over HTTP.

!!! question "The question this page answers"
    **How do I turn the Admin API on, and how do I call each endpoint from a script or a program?**

The same operations are available from a terminal through [`tandem-cli`](cli.md), which is a thin
frontend over this API. Read this page first: the concepts apply to both.

---

## 1. Concepts

### 1.1 What it is

The Admin API is an **optional REST module**, `tandem-admin`. It is **off by default**: until you
enable it there is no endpoint and no cost. It talks to the outbox database and nothing else, so it
does not need Kafka and it does not call your application.

It can run in two places, with no difference in what it offers:

- **Embedded**: inside the Spring Boot application that already writes events or runs the relay.
- **Standalone**: as its own small Spring Boot service pointed at the outbox database. It has its
  own lifecycle and its own network boundary, which is often what you want for something that can
  change delivery state.

### 1.2 Turning it on

Add the module, and make sure the application is a web application with a `DataSource`.

=== "Gradle"

    ```kotlin
    dependencies {
        implementation(platform("com.codingful:tandem-bom:x.y.z"))
        implementation("com.codingful:tandem-admin")
        implementation("org.springframework.boot:spring-boot-starter-web")
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
      <dependency>
        <groupId>com.codingful</groupId>
        <artifactId>tandem-admin</artifactId>
      </dependency>
      <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
      </dependency>
    </dependencies>
    ```

    Import `tandem-bom` in `dependencyManagement` as shown in
    [Getting Started](getting-started.md#3-step-1-add-the-dependencies).

Then enable it:

```yaml
tandem:
  admin:
    enabled: true
    # base-path: /tandem/admin     # the default
```

`tandem-admin` works on Spring Boot 3.x and 4.x and brings no JSON library of its own: it renders
through the one your application already runs.

### 1.3 Security is yours

!!! danger "When enabled, the endpoints are as open as you leave them"
    Tandem ships the endpoints, **not the authentication**. Replay, discard and pause change
    delivery, so secure the API with whatever your application already uses (Spring Security, a
    gateway, an internal-only network) before you enable it anywhere reachable. The contract
    declares a bearer token and an `X-API-Key` header as the expected shapes, but nothing checks
    them until your application does.

Every write operation is logged at `INFO` once it succeeds, with the identifiers it touched and the
`actor` your authentication established, if there is one. Reads are not logged.

### 1.4 The URL

Every endpoint lives under one prefix:

```
{your host}{base-path}/v1
```

With the default base path that is `/tandem/admin/v1`, for example
`http://localhost:8080/tandem/admin/v1/outbox/summary`. The `/v1` is the version of the contract. A
change that could break a caller would ship as `/v2` next to it, never inside `/v1`.

### 1.5 The life of an outbox row

Every row has a status, and most of what this API does is move a row between them.

| Status | Meaning |
|---|---|
| `PENDING` | Written, waiting for the relay. |
| `IN_FLIGHT` | Claimed by the relay, being published right now. |
| `DONE` | Published and acknowledged by Kafka. |
| `FAILED` | Every delivery attempt failed. The row waits for an operator. |
| `DISCARDED` | Abandoned by an operator. Final. |

A `FAILED` row blocks **its own aggregate** and nothing else, because later events of that aggregate
must not overtake it. Every other aggregate keeps flowing. You resolve a `FAILED` row in one of two
ways: **replay** it (back to `PENDING`, the relay tries again) or **discard** it (give up on that
event, which unblocks the aggregate at the price of skipping it).

### 1.6 Two coordination modes

The relay runs under one of two coordination modes, set in its own configuration
(`tandem.relay.coordination`). `SINGLE` is the default: one relay, in charge of everything.
`LEASE` lets several relay instances share the work.

Most of the API behaves the same under both. The endpoints that talk about **buckets and workers**
need `LEASE`, because under `SINGLE` there is no per-bucket ownership to report. Called there, they
answer `409` (problem type `relay-coordination-unsupported`) instead of a misleading empty answer.
Section 3 marks them, and a `409` here means "not applicable to this deployment", never "try again".

### 1.7 Requests and responses

- Bodies are **JSON**. Timestamps are ISO 8601 in UTC.
- Errors are **`application/problem+json`** (RFC 9457), always with a `type` such as
  `https://tandem.codingful.com/problems/not-found`. The last segment of that URL is the **stable
  identifier** to match on; the `detail` text is for people.
- Lists are paged by **cursor**: a response carries a `nextCursor`, and you pass it back to get the
  following page. There are no page numbers.
- Read responses **tolerantly**: ignore fields and enum values you do not know. New optional fields
  are added within `/v1`, and a client that rejects them breaks for no reason.

---

## 2. Calling it

The examples in section 3 use a small helper in each language, defined once here. It sets the base
URL, adds credentials if you have any, and turns an error response into an exception carrying the
problem type. Adapt the authentication to what your application enforces.

=== "curl"

    ```bash
    export BASE=http://localhost:8080/tandem/admin/v1
    # only if your application requires a bearer token:
    export AUTH='Authorization: Bearer <token>'
    ```

    If your application requires a credential, add `-H "$AUTH"` to each call. The examples leave it
    out to stay short.

    Add `-i` to any call to see the status code, and `| jq` to format the answer.

=== "Python"

    ```python
    import os
    import requests

    BASE = os.environ.get("TANDEM_ADMIN_URL", "http://localhost:8080/tandem/admin/v1")
    session = requests.Session()
    if token := os.environ.get("TANDEM_ADMIN_TOKEN"):
        session.headers["Authorization"] = f"Bearer {token}"


    def call(method, path, **kwargs):
        response = session.request(method, BASE + path, timeout=30, **kwargs)
        if response.status_code >= 400:
            problem = response.json()
            slug = problem["type"].rsplit("/", 1)[-1]
            raise RuntimeError(f"{response.status_code} {slug}: {problem.get('detail', problem['title'])}")
        return response.json()
    ```

    Needs `pip install requests`. `params=` sends a query string and `json=` sends a JSON body.

=== "JavaScript"

    ```javascript
    // Node 18 or later, as an ES module (fetch is built in).
    const BASE = process.env.TANDEM_ADMIN_URL ?? "http://localhost:8080/tandem/admin/v1";
    const headers = process.env.TANDEM_ADMIN_TOKEN
      ? { Authorization: `Bearer ${process.env.TANDEM_ADMIN_TOKEN}` }
      : {};

    async function call(method, path, { query, body } = {}) {
      const url = new URL(BASE + path);
      for (const [name, value] of Object.entries(query ?? {})) url.searchParams.set(name, value);
      const response = await fetch(url, {
        method,
        headers: body ? { ...headers, "Content-Type": "application/json" } : headers,
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(`${response.status} ${data.type.split("/").pop()}: ${data.detail ?? data.title}`);
      }
      return data;
    }
    ```

=== "Java"

    ```java
    // Java 11 or later, no dependency: java.net.http is part of the JDK.
    static final String BASE = System.getenv().getOrDefault("TANDEM_ADMIN_URL", "http://localhost:8080/tandem/admin/v1");
    static final String TOKEN = System.getenv("TANDEM_ADMIN_TOKEN");
    static final HttpClient HTTP = HttpClient.newHttpClient();

    /** Returns the response body as JSON text; parse it with the JSON library you already use. */
    static String call(String method, String path, String jsonBody) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(BASE + path))
                .timeout(Duration.ofSeconds(30));
        if (TOKEN != null) {
            request.header("Authorization", "Bearer " + TOKEN);
        }
        if (jsonBody == null) {
            request.method(method, BodyPublishers.noBody());
        } else {
            request.header("Content-Type", "application/json")
                    .method(method, BodyPublishers.ofString(jsonBody));
        }
        HttpResponse<String> response = HTTP.send(request.build(), BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException(response.statusCode() + " " + response.body());
        }
        return response.body();
    }
    ```

    The imports are `java.net.URI`, `java.time.Duration` and the `java.net.http` classes
    (`HttpClient`, `HttpRequest`, `HttpRequest.BodyPublishers`, `HttpResponse`,
    `HttpResponse.BodyHandlers`). The Java examples below pass the request body as a JSON string.

Every language can call the API this way; these four are the ones shown. Anything that speaks HTTP
and JSON works.

---

## 3. The endpoints, by function

| Function | Endpoint | Needs `LEASE` |
|---|---|---|
| [Health](#31-health-of-the-outbox) | `GET /outbox/summary` | |
| [Finding messages](#32-finding-messages) | `GET /outbox/messages` | |
| | `GET /outbox/messages/{id}` | |
| [Recovering messages](#33-recovering-messages) | `POST /outbox/messages/{id}/replay` | |
| | `POST /outbox/replay` | |
| | `POST /outbox/messages/{id}/discard` | |
| [Relay state and control](#34-relay-state-and-control) | `GET /relay/status` | |
| | `POST /relay/pause` | Only with a bucket |
| | `POST /relay/resume` | Only with a bucket |
| [Buckets and workers](#35-buckets-and-workers-lease-only) | `GET /relay/buckets` | Yes |
| | `GET /relay/buckets/{bucket}` | Yes |
| | `POST /relay/buckets/{bucket}/release` | Yes |
| | `GET /relay/workers` | Yes |

All paths are relative to the prefix in §1.4. The examples use real answers from a running
application; yours will have your own ids and timestamps.

### 3.1 Health of the outbox

#### `GET /outbox/summary`

How many rows are in each status, and how far behind delivery is. This is the call to poll, and the
first to make when something feels wrong: a growing `FAILED` or a `lagAgeSeconds` that keeps
climbing is the signal.

=== "curl"

    ```bash
    curl -s "$BASE/outbox/summary"
    ```

=== "Python"

    ```python
    summary = call("GET", "/outbox/summary")
    print(summary["counts"]["FAILED"], summary["lagAgeSeconds"])
    ```

=== "JavaScript"

    ```javascript
    const summary = await call("GET", "/outbox/summary");
    console.log(summary.counts.FAILED, summary.lagAgeSeconds);
    ```

=== "Java"

    ```java
    String summary = call("GET", "/outbox/summary", null);
    ```

```json
{"counts":{"PENDING":0,"IN_FLIGHT":0,"DONE":2,"FAILED":0,"DISCARDED":0},"lagCount":0,"lagAgeSeconds":0.0}
```

| Field | Meaning |
|---|---|
| `counts` | Number of rows per status. |
| `lagCount` | Number of `PENDING` rows. |
| `lagAgeSeconds` | Age of the oldest `PENDING` row, in seconds. `0` when nothing waits. |

### 3.2 Finding messages

#### `GET /outbox/messages`

Searches the outbox. Every filter is optional and they combine. The list leaves out each row's
payload and headers, to keep pages small; fetch one message to read them.

| Query parameter | Meaning |
|---|---|
| `status` | One of `PENDING`, `IN_FLIGHT`, `DONE`, `FAILED`, `DISCARDED`. |
| `aggregateId` | Exact aggregate id. |
| `aggregateType` | Exact aggregate type, for example `Order`. |
| `type` | The event type, for example `com.acme.order.placed`. |
| `createdFrom`, `createdTo` | Bounds on the creation time, ISO 8601. |
| `correlationId` | Exact match on the correlation id. One id usually matches many rows, so combine it with `status`. |
| `limit` | Page size, 1 to 500. Default 50. |
| `cursor` | The `nextCursor` of the previous page. |

=== "curl"

    ```bash
    curl -s "$BASE/outbox/messages?status=FAILED&aggregateType=Order&limit=20"
    ```

=== "Python"

    ```python
    page = call("GET", "/outbox/messages", params={"status": "FAILED", "aggregateType": "Order", "limit": 20})
    for message in page["items"]:
        print(message["id"], message["aggregateId"], message["lastError"])
    ```

=== "JavaScript"

    ```javascript
    const page = await call("GET", "/outbox/messages", {
      query: { status: "FAILED", aggregateType: "Order", limit: 20 },
    });
    for (const message of page.items) console.log(message.id, message.aggregateId, message.lastError);
    ```

=== "Java"

    ```java
    String page = call("GET", "/outbox/messages?status=FAILED&aggregateType=Order&limit=20", null);
    ```

```json
{"items":[{"id":1,"aggregateId":"order-lease-1","aggregateType":"Order","type":"OrderPlaced","seq":1,"status":"DONE","attempts":0,"replays":0,"createdAt":"2026-09-20T12:01:22.449393Z"}],"nextCursor":"1"}
```

`seq` is absent for a message written with no sequence number, and it is not zero: do not fill one in.
`nextCursor` is `null` on the last page.

**Reading every page.** Pass the cursor back until it is `null`:

=== "curl"

    ```bash
    curl -s "$BASE/outbox/messages?status=FAILED&limit=100&cursor=<nextCursor>"
    ```

=== "Python"

    ```python
    cursor = None
    while True:
        params = {"status": "FAILED", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        page = call("GET", "/outbox/messages", params=params)
        for message in page["items"]:
            print(message["id"])
        cursor = page["nextCursor"]
        if not cursor:
            break
    ```

=== "JavaScript"

    ```javascript
    let cursor = null;
    do {
      const page = await call("GET", "/outbox/messages", {
        query: { status: "FAILED", limit: 100, ...(cursor && { cursor }) },
      });
      for (const message of page.items) console.log(message.id);
      cursor = page.nextCursor;
    } while (cursor);
    ```

#### `GET /outbox/messages/{id}`

One message in full: the same fields as the list plus `headers` and `payload`, and, for a row that
failed, `lastError`. Use it to find out why a row is `FAILED`.

=== "curl"

    ```bash
    curl -s "$BASE/outbox/messages/1"
    ```

=== "Python"

    ```python
    message = call("GET", "/outbox/messages/1")
    print(message["status"], message.get("lastError"), message["payload"])
    ```

=== "JavaScript"

    ```javascript
    const message = await call("GET", "/outbox/messages/1");
    console.log(message.status, message.lastError, message.payload);
    ```

=== "Java"

    ```java
    String message = call("GET", "/outbox/messages/1", null);
    ```

```json
{"id":1,"aggregateId":"order-lease-1","aggregateType":"Order","type":"OrderPlaced","seq":1,"status":"DONE","attempts":0,"replays":0,"createdAt":"2026-09-20T12:01:22.449393Z","headers":{"content-type":"application/json"},"payload":{"event": "OrderPlaced", "order": "order-lease-1"}}
```

An unknown id answers `404`. `replays` counts how many times an operator replayed this row, over its
whole life; `attempts` counts the delivery attempts of the current round and starts again after a
replay.

### 3.3 Recovering messages

#### `POST /outbox/messages/{id}/replay`

Resets one `DONE` or `FAILED` message to `PENDING`, so the relay publishes it again. Use it after you
have fixed whatever made a row fail, or to deliver an event to consumers a second time on purpose.
The answer is the updated message.

Replaying a `DONE` row publishes a **duplicate**: consumers must be idempotent, as
[Getting Started](getting-started.md#8-step-6-write-your-consumer-to-tolerate-repeats) recommends.

=== "curl"

    ```bash
    curl -s -X POST "$BASE/outbox/messages/1/replay"
    ```

=== "Python"

    ```python
    message = call("POST", "/outbox/messages/1/replay")
    print(message["status"], message["replays"])
    ```

=== "JavaScript"

    ```javascript
    const message = await call("POST", "/outbox/messages/1/replay");
    console.log(message.status, message.replays);
    ```

=== "Java"

    ```java
    String message = call("POST", "/outbox/messages/1/replay", null);
    ```

A row in any other status answers `409` (`message-not-replayable`).

#### `POST /outbox/replay`

Replays every `DONE` or `FAILED` message that matches a selector, in one call. **At least one
selector is required**: a request that would replay the whole outbox is refused with `400`
(`replay-no-selector`).

| Body field | Meaning |
|---|---|
| `aggregateId` | Replay this aggregate's rows. |
| `aggregateType` | Replay every row of this type. |
| `fromId`, `toId` | Replay an id range. |
| `statuses` | Restrict to `DONE`, `FAILED`, or both. When left out, both are eligible. |
| `dryRun` | `true` returns how many rows match without changing anything. |

**Run it with `dryRun` first.** A bulk replay can touch a very large number of rows.

=== "curl"

    ```bash
    # how many would this replay?
    curl -s -X POST "$BASE/outbox/replay" \
         -H 'Content-Type: application/json' \
         -d '{"aggregateType": "Order", "statuses": ["FAILED"], "dryRun": true}'

    # do it
    curl -s -X POST "$BASE/outbox/replay" \
         -H 'Content-Type: application/json' \
         -d '{"aggregateType": "Order", "statuses": ["FAILED"]}'
    ```

=== "Python"

    ```python
    selector = {"aggregateType": "Order", "statuses": ["FAILED"]}

    preview = call("POST", "/outbox/replay", json={**selector, "dryRun": True})
    print(preview["matched"], "rows would be replayed")

    result = call("POST", "/outbox/replay", json=selector)
    print(result["replayed"], "rows replayed")
    ```

=== "JavaScript"

    ```javascript
    const selector = { aggregateType: "Order", statuses: ["FAILED"] };

    const preview = await call("POST", "/outbox/replay", { body: { ...selector, dryRun: true } });
    console.log(preview.matched, "rows would be replayed");

    const result = await call("POST", "/outbox/replay", { body: selector });
    console.log(result.replayed, "rows replayed");
    ```

=== "Java"

    ```java
    String preview = call("POST", "/outbox/replay",
            "{\"aggregateType\":\"Order\",\"statuses\":[\"FAILED\"],\"dryRun\":true}");
    String result = call("POST", "/outbox/replay",
            "{\"aggregateType\":\"Order\",\"statuses\":[\"FAILED\"]}");
    ```

```json
{"matched":1,"replayed":0,"dryRun":true}
```

`matched` is how many rows fit the selector; `replayed` is how many were reset (`0` on a dry run).

#### `POST /outbox/messages/{id}/discard`

Abandons a `FAILED` message for good, so the relay stops holding its aggregate back.

!!! warning "Discarding skips an event"
    The aggregate's consumers never receive it, so per-aggregate ordering is broken for that
    aggregate, permanently. It cannot be undone. The request must say you know:
    `acknowledgeOrderingBreak` has to be `true`, or the call is refused with `400`
    (`ordering-break-not-acknowledged`). Prefer replay whenever the cause can be fixed.

| Body field | Meaning |
|---|---|
| `acknowledgeOrderingBreak` | Required, must be `true`. |
| `reason` | Free text kept for the audit trail. Optional in the API, but always give one. |

=== "curl"

    ```bash
    curl -s -X POST "$BASE/outbox/messages/7/discard" \
         -H 'Content-Type: application/json' \
         -d '{"acknowledgeOrderingBreak": true, "reason": "payload rejected by the broker, event obsolete"}'
    ```

=== "Python"

    ```python
    message = call("POST", "/outbox/messages/7/discard", json={
        "acknowledgeOrderingBreak": True,
        "reason": "payload rejected by the broker, event obsolete",
    })
    print(message["status"])   # DISCARDED
    ```

=== "JavaScript"

    ```javascript
    const message = await call("POST", "/outbox/messages/7/discard", {
      body: {
        acknowledgeOrderingBreak: true,
        reason: "payload rejected by the broker, event obsolete",
      },
    });
    console.log(message.status);   // DISCARDED
    ```

=== "Java"

    ```java
    String message = call("POST", "/outbox/messages/7/discard",
            "{\"acknowledgeOrderingBreak\":true,\"reason\":\"payload rejected by the broker, event obsolete\"}");
    ```

Only a `FAILED` row can be discarded; any other answers `409` (`message-not-discardable`). The answer
is the updated message, with the `reason` in `discardReason`.

### 3.4 Relay state and control

#### `GET /relay/status`

Whether the relay is running, and whether the buckets are covered. It works under both coordination
modes.

=== "curl"

    ```bash
    curl -s "$BASE/relay/status"
    ```

=== "Python"

    ```python
    status = call("GET", "/relay/status")
    print(status["state"], status["workers"])
    ```

=== "JavaScript"

    ```javascript
    const status = await call("GET", "/relay/status");
    console.log(status.state, status.workers);
    ```

=== "Java"

    ```java
    String status = call("GET", "/relay/status", null);
    ```

```json
{"state":"RUNNING","bucketCount":256,"uncoveredBuckets":0,"workers":1}
```

| Field | Meaning |
|---|---|
| `state` | `RUNNING`, `PAUSED`, or `DOWN`. `DOWN` means no relay instance has reported in for a few seconds, and it wins over `PAUSED`: a paused relay that then stops reads as `DOWN`. |
| `bucketCount` | The total number of buckets, the value you configured. |
| `uncoveredBuckets` | Buckets holding pending rows that no relay owns. Always `0` under `SINGLE`. |
| `workers` | Live relay instances. Always `0` under `SINGLE`, which does **not** mean the relay is down: read `state`. |

#### `POST /relay/pause` and `POST /relay/resume`

Pause stops the relay from publishing; resume lets it continue. Rows keep being written and simply
wait as `PENDING`. A worker in the middle of a send finishes what it already claimed, and the change
reaches every relay instance within a few seconds rather than instantly. The answer is the relay's
status.

With no body they act on the **whole relay**, under either coordination mode. Under `LEASE` you can
name a single bucket in the body, `{"bucket": 12}`, to pause or resume just that one; under `SINGLE`
that answers `409`.

=== "curl"

    ```bash
    curl -s -X POST "$BASE/relay/pause"
    curl -s -X POST "$BASE/relay/resume"

    # one bucket, LEASE only
    curl -s -X POST "$BASE/relay/pause" -H 'Content-Type: application/json' -d '{"bucket": 12}'
    ```

=== "Python"

    ```python
    call("POST", "/relay/pause")
    call("POST", "/relay/resume")

    # one bucket, LEASE only
    call("POST", "/relay/pause", json={"bucket": 12})
    ```

=== "JavaScript"

    ```javascript
    await call("POST", "/relay/pause");
    await call("POST", "/relay/resume");

    // one bucket, LEASE only
    await call("POST", "/relay/pause", { body: { bucket: 12 } });
    ```

=== "Java"

    ```java
    call("POST", "/relay/pause", null);
    call("POST", "/relay/resume", null);

    // one bucket, LEASE only
    call("POST", "/relay/pause", "{\"bucket\":12}");
    ```

### 3.5 Buckets and workers (LEASE only)

Work is divided into virtual buckets, and under `LEASE` each bucket has one owner at a time. These
endpoints show who owns what. Under `SINGLE` all four answer `409`
(`relay-coordination-unsupported`).

#### `GET /relay/buckets` and `GET /relay/buckets/{bucket}`

Owner, lease expiry and backlog for every bucket, or for one. Add `uncoveredOnly=true` to the list to
see only the buckets that have pending rows and no live owner: those are the stalled ones.

=== "curl"

    ```bash
    curl -s "$BASE/relay/buckets?uncoveredOnly=true"
    curl -s "$BASE/relay/buckets/0"
    ```

=== "Python"

    ```python
    stalled = call("GET", "/relay/buckets", params={"uncoveredOnly": "true"})
    bucket = call("GET", "/relay/buckets/0")
    ```

=== "JavaScript"

    ```javascript
    const stalled = await call("GET", "/relay/buckets", { query: { uncoveredOnly: true } });
    const bucket = await call("GET", "/relay/buckets/0");
    ```

=== "Java"

    ```java
    String stalled = call("GET", "/relay/buckets?uncoveredOnly=true", null);
    String bucket = call("GET", "/relay/buckets/0", null);
    ```

```json
{"bucket":0,"owner":"tandem-host-16809-90b7","leaseUntil":"2026-09-20T12:02:00.499772Z","covered":true,"paused":false,"pendingCount":0}
```

| Field | Meaning |
|---|---|
| `owner` | The worker holding the bucket, absent when nobody does. |
| `leaseUntil` | When that claim expires unless renewed. |
| `covered` | Whether the bucket has a live owner. |
| `paused` | Whether the bucket was paused on purpose. A paused bucket keeps its owner, so `covered` can be `true` while it is idle; this field tells a deliberate pause from a stall. |
| `pendingCount`, `lagAgeSeconds` | Rows waiting in the bucket and the age of the oldest. |

A bucket number outside `0` to `bucketCount - 1` answers `404`.

#### `POST /relay/buckets/{bucket}/release`

Clears a bucket's ownership so another worker can claim it. Use it when the owner is a zombie: its
heartbeat is stale, the bucket has pending rows, and nothing is moving. The answer is the bucket's
new state.

=== "curl"

    ```bash
    curl -s -X POST "$BASE/relay/buckets/0/release"
    ```

=== "Python"

    ```python
    bucket = call("POST", "/relay/buckets/0/release")
    ```

=== "JavaScript"

    ```javascript
    const bucket = await call("POST", "/relay/buckets/0/release");
    ```

=== "Java"

    ```java
    String bucket = call("POST", "/relay/buckets/0/release", null);
    ```

#### `GET /relay/workers`

The live relay instances, with their last heartbeat and how many buckets each owns.

=== "curl"

    ```bash
    curl -s "$BASE/relay/workers"
    ```

=== "Python"

    ```python
    for worker in call("GET", "/relay/workers"):
        print(worker["workerId"], worker["bucketCount"], worker["lastHeartbeat"])
    ```

=== "JavaScript"

    ```javascript
    for (const worker of await call("GET", "/relay/workers")) {
      console.log(worker.workerId, worker.bucketCount, worker.lastHeartbeat);
    }
    ```

=== "Java"

    ```java
    String workers = call("GET", "/relay/workers", null);
    ```

```json
[{"workerId":"tandem-host-16809-90b7","lastHeartbeat":"2026-09-20T12:01:30.498316Z","bucketCount":256}]
```

---

## 4. Errors

Every error is a problem document. This is the answer to a `discard` without the acknowledgement:

```json
{
  "type": "https://tandem.codingful.com/problems/ordering-break-not-acknowledged",
  "title": "Ordering-break acknowledgement is required to discard",
  "status": 400,
  "detail": "Discarding a message breaks per-aggregate ordering; acknowledgeOrderingBreak must be true",
  "instance": "/tandem/admin/v1/outbox/messages/2/discard"
}
```

Match on the last segment of `type` (the slug), which never changes once published. Each slug
resolves to a page at `https://tandem.codingful.com/problems/{slug}`.

| Slug | Status | When | What to do |
|---|---|---|---|
| `invalid-parameter` | 400 | A query or path value could not be read, such as an unknown `status`. | Fix the request. |
| `replay-no-selector` | 400 | Bulk replay with no selector. | Add at least one selector. |
| `ordering-break-not-acknowledged` | 400 | Discard without `acknowledgeOrderingBreak: true`. | Confirm it, or reconsider. |
| `unauthorized` | 401 | Credentials missing or wrong, as decided by your application's security. | Check the credentials. |
| `not-found` | 404 | No such message or bucket. | Check the id or bucket number. |
| `message-not-replayable` | 409 | Replay of a row that is not `DONE` or `FAILED`. | Check its status first. |
| `message-not-discardable` | 409 | Discard of a row that is not `FAILED`. | Check its status first. |
| `relay-coordination-unsupported` | 409 | A bucket or worker operation under `SINGLE`. | Not applicable here; do not retry. |
| `internal-error` | 500 | Unexpected failure on the server. | Read the application log. |

A `401` or `403` produced by a security layer or gateway in front of the API is not necessarily a
problem document, so check that a response body is JSON before you parse it.

---

## 5. A worked case: recovering a failed event

An alert says `FAILED` is above zero. Here is the whole path with the Python helper from §2.

```python
# 1. How bad is it?
summary = call("GET", "/outbox/summary")
print(summary["counts"]["FAILED"], "failed rows")

# 2. Which rows, and why?
failed = call("GET", "/outbox/messages", params={"status": "FAILED", "limit": 50})["items"]
for row in failed:
    detail = call("GET", f"/outbox/messages/{row['id']}")
    print(detail["id"], detail["aggregateId"], detail["lastError"])

# 3. Fix the cause outside Tandem (create the missing topic, correct the broker setting), then
#    preview and replay everything that failed for the same reason.
selector = {"aggregateType": "Order", "statuses": ["FAILED"]}
print(call("POST", "/outbox/replay", json={**selector, "dryRun": True})["matched"], "would be replayed")
print(call("POST", "/outbox/replay", json=selector)["replayed"], "replayed")

# 4. Confirm the relay drained them.
print(call("GET", "/outbox/summary")["counts"])
```

Only if a specific event can never be delivered and consumers can live without it, discard that one
row, with a reason, as in §3.3.

---

## 6. Compatibility

The contract is committed in the repository as
[`admin-api.openapi.yaml`](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/admin-api.openapi.yaml)
and is the source of truth for everything above. Within `/v1` it only grows: new optional fields, new
endpoints, new optional parameters. A field is never removed, renamed or retyped, and a problem slug
never changes. In return, a client has to read tolerantly (§1.7). If you generate a client from the
contract, keep its response validation permissive.

Its response types belong to the API and are deliberately not the library's internal ones: do not
assume the two change together.
