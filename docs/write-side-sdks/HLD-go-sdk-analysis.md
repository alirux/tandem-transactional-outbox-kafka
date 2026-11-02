# Go write-side SDK: analysis

**Status:** Working analysis, input for the Go design document (backlog item 59). Not a spec.
**Builds on:** [HLD-write-side-sdks.md](HLD-write-side-sdks.md) (Accepted), which already decides the
contract, the conformance approach, Go as the first language, the layout (`sdks/go/`), the module path
(reopened by D14) and the tag form. This note covers what that HLD leaves to "a short design of its
own" per language (§7), plus the gaps found by reading the Java writer against it.

---

## 1. What is already decided (not reopened here)

- Contract: eight columns, FNV-1a 64 with signed floor modulus, two `seq` modes, both binding the
  column (a value or `NULL`), `lockedWrite` via `pg_advisory_xact_lock(hashtext($1))`, optional wakeup, bucket count
  reconciled at construction (HLD §4).
- Join the caller's transaction, never own one; Go takes `*sql.Tx` (HLD §5.1).
- Footprint: `database/sql`, `encoding/json`, `hash/fnv`, nothing else; `pgx` native is a later
  optional package (HLD §5.2).
- No logging, errors only (HLD §5.4).
- Vectors in `schema/write-side/`, verified by `tandem-core` and `tandem-jdbc`; three test layers per
  SDK (HLD §6).
- Module path `github.com/alirux/tandem-transactional-outbox-kafka/sdks/go`, tags `sdks/go/vX.Y.Z`,
  independent semver, schema floor `v6-drop-managed-seq` (HLD §8).

---

## 2. Gaps between the HLD and the Java writer

Reading `OutboxMessage`, `AggregateId` and `JdbcOutboxRepository` against the HLD turns up points the
contract does not state yet. Each one is a place where a Go port written from the HLD alone would
diverge from Java. They belong in the HLD (an amendment) or in the vectors before the Go code exists.

| # | Java does | HLD says | Consequence for Go |
|---|---|---|---|
| G1 | Refuses a message whose `dataschema` header is not a `java.net.URI` (`requirePublishable`), because the relay would fail the row for good and block its aggregate | Not in the failure table (§4.9) | Go's `net/url.Parse` accepts far more than `java.net.URI` (RFC 2396). A Go writer that does not check, or checks with `net/url`, can store a row the relay can never publish. The contract must define the accepted URI syntax |
| G2 | Counts every length it bounds in code points: `aggregate_id`, `aggregate_type`, `type`, and the `correlation_id` copy, cut on a character boundary (`3152f38`) | §4.1 says the same | None: a Go writer counting with `utf8.RuneCountInString` agrees with Java on every case of the POC (§7.1) |
| G3 | "Blank" is `String.isBlank()`: every code point is `Character.isWhitespace` | "Non-blank", undefined | Go's `strings.TrimSpace` uses `unicode.IsSpace`, a different set (U+00A0 and U+0085 are space in Go, not whitespace in Java). The contract must name the set, and a rejection vector must pin one character on which they differ |
| G4 | No typed `dataschema` setter: only `contentType(String)`; `dataschema` is a plain header | §5.3: "typed setters for `content-type` and `dataschema`" | Either Java gains the setter or the HLD drops it; the two SDKs must expose the same concepts (§5.3) |
| G5 | Driver message is withheld from the cause (`withheld`): PostgreSQL quotes bound values, so the payload would reach logs | §4.9 says errors never carry payload or header values, not how the driver's error is handled | In Go the idiom is wrapping with `%w`. The POC shows both drivers keep bound values out of `Error()` and in the `Detail` and `Where` fields, reachable through `errors.As` (§7.2). See §4.5 |
| G6 | Wakeup is a writer-level option (`Builder.wakeup`), not per call | §4.8: "when the caller opts in" | Matches if Go makes it a constructor option; worth stating |
| G7 | Header values must be non-null; nothing else is checked on names | Not stated | Go `map[string]string` cannot hold a null; nothing to do, except stating that an empty name is allowed (it is, in Java) or refused |
| G8 | Layer 3 of §6 compares the CloudEvent with "the one the same message produces when written by Java" | No artifact holds that expected envelope | Go's CI has no JVM. The expected envelope (minus `id` and `time`) has to be a vector too, produced and verified by `tandem-kafka`'s tests |

G1 and G3 are the ones that change stored data or block an aggregate. G2 is closed and stays in the
table as the row the vectors must pin.

---

## 3. Go-specific findings

### 3.1 The wakeup array needs no driver type

`database/sql` has no array type: `pq.Array` is `lib/pq`'s, and `pgx` has its own. Binding the bucket
list as the text literal `{3,17,42}` and casting in SQL works on every driver with the standard
abstraction:

```sql
SELECT pg_notify('tandem_wakeup', bucket::text) FROM unnest($1::int[]) AS bucket
```

The channel name can be a literal, as here, since it is part of the contract.

### 3.2 SQLSTATE without importing a driver

The duplicate-seq error (`23505`) needs the SQLSTATE, which `database/sql` does not expose. Both
mainstream drivers implement a method with the same signature: `pgconn.PgError.SQLState() string`
(`pgx`) and `(*pq.Error).SQLState() string` (`lib/pq` from v1.10.6; absent up to v1.10.5, checked
in §7). An unexported interface assertion reads it with no import:

```go
var withState interface{ SQLState() string }
if errors.As(err, &withState) && withState.SQLState() == "23505" { ... }
```

A driver that offers neither still works; its duplicates are reported as a generic insert error.
The driver matrix tested in CI decides which drivers are promised (§5, D6).

### 3.3 Invalid UTF-8

A Go `string` can hold invalid UTF-8; a Java `String` cannot (it would be encoded with replacement
characters before hashing). PostgreSQL refuses invalid UTF-8 (SQLSTATE `22021`) and a NUL byte in
`text`. The Go SDK should refuse both in `aggregate_id`, `aggregate_type`, `type` and header values
as a validation error, before hashing, so the failure is a clear one and not a database error naming
nothing. Rejection vectors pin it (as raw bytes, since JSON cannot hold invalid UTF-8: hex encoded).

### 3.4 Batch insert

`database/sql` has no batch. Three options for `InsertAll`:

| Option | Round trips | Order | Error attribution |
|---|---|---|---|
| One prepared statement, one `Exec` per row | N | Exactly the caller's | Exact: the failing message is known |
| Multi-row `VALUES` per run of one shape, chunked under 65535 parameters | 1 per run | The order of the `VALUES` list; PostgreSQL assigns `id` and `nextval` per row in that order in practice, not by documented guarantee | The whole run |
| `pgx` `SendBatch` | 1 | Exact | Exact, but driver-specific: the optional `pgx` package only |

Per-row `Exec` matches Java's semantics, attributes errors better than Java does (§4.9's batch
ambiguity disappears), and costs latency only on large batches. Multi-row is an optimisation that can
come later without a contract change, once a test pins its ordering.

### 3.5 Transaction handles across the Go ecosystem

`*sql.Tx` (HLD §5.1) reaches most of the ecosystem:

| Library | How the caller gets a `*sql.Tx` |
|---|---|
| plain `database/sql`, `sqlc` | directly (`sqlc`'s `WithTx` takes one) |
| `sqlx` | `sqlx.Tx` embeds `*sql.Tx` (`tx.Tx`) |
| `bun` | `bun.Tx` has a `Tx *sql.Tx` field |
| GORM | `tx.Statement.ConnPool.(*sql.Tx)` inside `db.Transaction` |
| `ent` | no public access to the underlying `*sql.Tx` |
| `pgx` native (`pgx.Tx`) | none: the optional `pgx` package (HLD §5.2) |

An interface (`ExecContext` plus `QueryContext`, like `sqlc`'s `DBTX`) would also accept `ent` via a
wrapper and `*sql.DB`, but loses the compile-time guarantee that a transaction is held: a `*sql.DB`
would autocommit each statement, splitting the event from the business change. The HLD's choice holds;
`ent` users are the cost, to be stated in the README.

### 3.6 Test dependencies and the footprint

A `require` in `sdks/go/go.mod` is visible to every consumer's tooling (module graph, `go.sum`,
vulnerability scanners), even when only tests use it. Conformance layers 2 and 3 need a PostgreSQL
driver and a container library (`testcontainers-go` or `docker compose`), and a Kafka client to read
the envelope back. To keep the SDK's own `go.mod` with **zero `require` lines**, those tests live in a
nested module, for example `sdks/go/conformance/` with its own `go.mod` and a `replace` to `../`.
Layer 1 (bucket and rejection vectors) needs only the standard library and stays in the SDK module.

### 3.7 Go version floor

`go` in `go.mod` is the minimum toolchain a consumer needs. Upstream supports the two latest releases
(1.26 and 1.27 at the time of writing). A library usually declares an older floor than an application:
the CLI is on 1.25. Nothing in the SDK needs a recent feature (`hash/fnv`, `encoding/json`,
`unicode/utf8` are all ancient), so the floor is a policy choice (D5).

### 3.8 Package name

The module path ends in `/go`, which is not a valid identifier to import by, so the package declares
its own name (`package tandem`) and every import reads `tandem.Message`. That is legal and works with
`goimports`, but linters flag a package name that differs from the last path element. Alternative:
the importable package one level down (`sdks/go/tandem`, path `.../sdks/go/tandem`), which keeps the
module at `sdks/go` for the tag form and makes path and name agree (D3).

---

## 4. API sketch

A sketch to make the decisions below concrete; nothing here is decided.

### 4.1 Writer

```go
w, err := tandem.NewWriter(ctx, db,                 // *sql.DB: bucket count reconciliation (HLD §4.7)
    tandem.WithBucketCount(256),                    // optional, default 256
    tandem.WithWakeup(),                            // optional, off by default
)

err = w.Insert(ctx, tx, msg)                        // tx *sql.Tx, never owned
err = w.InsertAll(ctx, tx, msgs)                    // order preserved, locks first (HLD §4.6)
```

`context.Context` on every call is the Go idiom and lets the caller's deadline cancel a statement
waiting on an advisory lock.

### 4.2 Message: struct literal, seq mode as a value with no zero

```go
msg := tandem.Message{
    AggregateID:   order.ID,
    AggregateType: "Order",
    Type:          "OrderPlaced",               // optional
    Seq:           tandem.Unsequenced(),         // or tandem.SeqOf(n): the number the aggregate gave the event
    Payload:       payload,                      // []byte, already JSON
    ContentType:   "application/json",           // optional, overrides headers["content-type"]
    Headers:       map[string]string{"correlation-id": cid},
    LockedWrite:   false,
}
```

A struct literal is the Go idiom for a value with optional fields; a builder would be a Java accent.
`Seq` is an opaque type whose **zero value means "no mode stated"**, which `Insert` refuses with the
same message Java gives (both modes named, `Unsequenced()` as the usual answer). This keeps HLD
§4.3's "no default" without a builder: forgetting the field is caught, and stating two modes is
impossible by construction, so Java's "mutually exclusive" error has no Go counterpart.

Alternative: a builder (`tandem.NewMessage(...).Unsequenced().Build()`), closer to Java's names (§5.3)
but foreign to Go readers.

### 4.3 Payload helper

`Payload []byte` keeps "the SDK never serializes" (core rule). A one-line helper,
`tandem.JSON(v any) ([]byte, error)` over `encoding/json`, is optional sugar; it adds no dependency.
Whether it exists at all is D8.

### 4.4 Validation

Before any statement, in this order, all messages of a batch first (as Java): required fields, blank
rule (G3), code point lengths (§4.1), valid UTF-8 and no NUL (§3.3), `dataschema` syntax
(G1), seq mode stated.

### 4.5 Errors

```go
var (
    ErrInvalidMessage = errors.New("tandem: invalid message")      // validation, before any statement
    ErrDuplicateSeq   = errors.New("tandem: duplicate (aggregate_id, seq)")
    ErrInsert         = errors.New("tandem: outbox insert failed")
    ErrBucketCount    = errors.New("tandem: bucket count conflict") // construction only
)
```

Concrete error types carry `AggregateType`, `AggregateID`, the seq value or mode name, and the
SQLSTATE, and match the sentinels through `errors.Is`. For G5: the type's `Error()` never includes the
driver's text; `Unwrap()` returns the driver error so `errors.As(err, &pgErr)` still works for a
caller who wants it and knowingly takes on what it contains. That keeps "logging `err` never prints a
payload" while staying idiomatic.

---

## 5. Decisions

All taken on 2026-10-27, together with D15 to D19 (§7.4): the recommendation in each row is the
decision, D14 included.

| # | Decision | Options | Decision and reason |
|---|---|---|---|
| D1 | Name and kind of the Go design document | `docs/write-side-sdks/LLD-go-sdk.md` (backlog says "LLD Go"; HLD-write-side-sdks is the HLD) · `docs/write-side-sdks/HLD-go-sdk.md` | **LLD**: the architecture is decided; what is left is module layout, API, tests, CI and release, which is what an LLD holds here (LLD-cli for the Go CLI) |
| D2 | How to close the contract gaps G1 to G8 | Amend HLD-write-side-sdks (v0.2) in the same branch, before the LLD · state them in the LLD only | **Amend the HLD**: they are contract, shared by every future SDK, not Go detail |
| D3 | Package path | `package tandem` at the module root (`.../sdks/go`) · `.../sdks/go/tandem` | **`sdks/go/tandem`** for path/name agreement; open to the shorter path if the import length bothers |
| D4 | Message shape | struct literal with a no-zero `Seq` type · builder mirroring Java | **struct literal** |
| D5 | Go floor | oldest upstream-supported release at first tag (1.26) · match the CLI (1.25) · older (1.22) | **1.25**, aligned with the CLI and one release below upstream support, re-evaluated per release |
| D6 | Drivers promised and tested | `pgx/v5/stdlib` and `lib/pq` · `pgx` only | **both**: `lib/pq` is in maintenance mode but still widespread |
| D7 | Batch strategy for the first release | per-row `Exec` · multi-row `VALUES` | **per-row `Exec`** (§3.4) |
| D8 | `tandem.JSON` helper | include · leave serialization to the caller | **leave it out**: one line for the caller, one less API to keep |
| D9 | `dataschema` check (G1) | a URI grammar the contract defines, stricter than or equal to `java.net.URI`, pinned by vectors · no check in SDKs (relay fails the row) · Java relaxes to RFC 3986 and both follow it | **contract-defined grammar, never looser than Java's**: a row Go accepts must be publishable |
| D10 | Blank definition (G3) | Java's `Character.isWhitespace` set, written out in the contract · Unicode `White_Space` and Java changes | **Java's set**: changing Java narrows what it accepts today |
| D11 | Who produces vector expectations | Java generates them through the reference-file mechanism (`-PupdateReferenceFiles`, LLD-api-stability §8), diff reviewed · hand-written, Java only verifies | **generated by Java**, bucket vectors frozen: a diff there is a breaking change by definition |
| D12 | Conformance tests location (§3.6) | nested module `sdks/go/conformance` · same module | **nested module**, SDK `go.mod` with no `require` |
| D13 | Commit scope | `go-sdk` · `sdk-go` · `sdks` | **`go-sdk`** (reads like the other component scopes; added to `commitlint.config.mjs` with the first SDK commit) |
| D14 | Module path (reopens HLD §8.2) | vanity path on `tandem.codingful.com` · keep the GitHub path | **vanity path `tandem.codingful.com/sdks/go`**: the import path is the module's identity, and the GitHub path ties it to a repository name that ends in `-kafka` while the library also targets RabbitMQ; renaming the repository would then be a breaking change of the SDK. The site already has to stay reachable for the problem type pages; it serves the `go-import` meta tag pointing at the repository, and the path keeps mirroring the directory, so the tags stay `sdks/go/vX.Y.Z`. The LLD defines the pages and a CI check that `go get` resolves through them |

---

## 6. Order of work implied

1. HLD-write-side-sdks v0.2 for G1, G3, G4, G5, G7 and G8, the lock order and the module path (D2,
   D9, D10, D14, D16 to D19).
2. Vectors in `schema/write-side/` plus their Java verification (bucket in `tandem-core`, rows in
   `tandem-jdbc`, envelope in `tandem-kafka`).
3. LLD for the Go SDK (D1), with the decisions of §5.
4. Implementation, CI job, release workflow, registration (AGENTS.md role `sdks/`, CONTRIBUTING,
   THIRD-PARTY-NOTICES, README, commitlint scope), the `go-import` pages on the site (D14).

Steps 1 and 2 change the Java side and can ship in a library release independently of Go; step 2 is
what makes the contract verifiable for any language, Go included.

---

## 7. Proof of concept: results

`poc/go-sdk/` holds a minimal Go writer (`tandem/`), a Java harness (`java/JavaFacts.java`) that runs
the real `BucketHash`, `OutboxMessage` and `JdbcOutboxRepository` on shared inputs (`inputs.json`)
and records what they do (`java-facts.json`), and Go tests that compare the two against
PostgreSQL 16 with the schema of `schema/postgres/tandem-baseline.sql`, through both `pgx/v5/stdlib`
(v5.11.0) and `lib/pq` (v1.12.3). Not a test suite to keep: it answers the questions below.

The proof of concept ran against schema v5, when a message had a third mode in which Tandem drew the
number (`managedSeq()`) and the row recorded its source in `seq_source`. Schema v6 removed both
(HLD-seq §4.5), so the PoC no longer runs against the current schema, and the rows below that
concern the third mode or `seq_source` describe that schema, not today's contract. Every other result
stands.

To rerun: start `postgres:16-alpine` on port 55432 (user, password and database `poc`) with the baseline
schema as an init script, build `tandem-core` and `tandem-jdbc` jars, run the harness from `poc/go-sdk`
(`java -cp <core>:<jdbc>:<pgjdbc> java/JavaFacts.java java-facts.json <jdbc url>`), then
`go test ./tandem -v`.

### 7.1 Confirmed as the HLD and this analysis expect

| Point | Result |
|---|---|
| Bucket (HLD §4.2) | 91 cases (13 ids including empty-adjacent, NUL, emoji, 255 code points; counts 1, 7, 100, 256, 1000, 32767, 2^31-1) identical to `BucketHash`. `order-7`: unsigned gives 59 at both 256 and 100, signed floor modulus gives 43 at 100: the trap is real |
| Lengths (HLD §4.1) | `aggregate_id`, `aggregate_type` and `type` of 255 `a`, 255 `é`, 255 emoji (510 UTF-16 units) and 128 emoji are accepted by Java and stored by PostgreSQL; 256 code points are refused by both. Java and Go agree on every case, on both drivers |
| `correlation_id` copy (HLD §4.4) | 300 ASCII characters, 254 `c` plus an emoji at position 255, and 200 emoji: Java and Go store the same value (first 255 code points, the emoji whole), on both drivers |
| Rows (HLD §4.1, §4.4) | For APPLICATION, MANAGED, NONE, a non-power-of-two count, typed `content-type` overriding the header, `type`, HTML characters in headers, non-ASCII payload with key reordering and `1.0e2`: every stored column identical to Java's, on both drivers |
| `seq` modes (§4.3) | APPLICATION with `seq` omitted is accepted and draws a managed number (the trap exists); MANAGED bound to `NULL` is refused by `tandem_outbox_seq_source_agrees` (23514) |
| Wakeup as text array (§3.1) | `unnest(CAST($1 AS int[]))` with `'{246,67}'` delivers one notification per distinct bucket, in order, on both drivers, with no driver array type |
| SQLSTATE (§3.2) | `errors.As` to `interface{ SQLState() string }` recognises 23505 on both drivers with no driver import |
| Bucket count reconciliation (§4.7) | Seeds a fresh database, refuses 256 against a stored 100; 30 rounds of two writers seeding 100 and 256 concurrently: exactly one fails every time |
| `lockedWrite` (§4.6) | A second transaction on the same aggregate blocks until the first ends, on both drivers |
| Invalid UTF-8 and NUL (§3.3) | Both drivers pass them through and PostgreSQL refuses both with 22021 naming nothing: SDK-side validation is what gives a useful error |
| Test dependencies (§3.6) | A module whose only `lib/pq` import is in a `_test.go` file still puts `lib/pq` in its `go.mod`, and a consumer importing it gets `lib/pq` in its `go.sum` and in `go list -m all`. The nested conformance module is needed |

### 7.2 Corrections to this analysis

- **Binding the payload as `[]byte` works** on both drivers (`CAST($n AS jsonb)` receives it as
  text). The POC binds a `string` anyway; either is fine.
- **The driver's `Error()` does not leak (G5 is narrower than stated).** Both drivers render only the
  primary message and the SQLSTATE. The bound values are in the error's fields: for invalid JSON,
  `Where` quotes the payload (`JSON data, line 1: {"card":"S3CRET-PAYLOAD" oops...`); for a duplicate,
  `Detail` quotes the key (`Key (aggregate_id, seq)=(poc-dup, 1) already exists.`). So logging the SDK
  error, or even the unwrapped driver error, prints no payload; a caller that reaches the driver type
  with `errors.As` and logs `Detail` or `Where` does. The design of §4.5 stands, and the LLD should
  say which fields are unsafe.
- **Multi-row `VALUES` kept list order** in 20 rounds of 200 rows, for both `id` and the managed
  `seq`. Observed, not guaranteed; it does not change D7.

### 7.3 Gaps confirmed, with the exact data

- **G1, `dataschema`.** Of 50 candidates, `net/url.Parse` accepts 16 that `java.net.URI` refuses,
  among them `http://example.com/a b`, `a#b#c`, `http://example.com/{v}`, `C:\schemas\o.json`,
  `schema v1`, `mailto:`. A candidate rule (ASCII RFC 3986 characters, valid `%XX`, at most one `#`,
  well-formed scheme with something after it, numeric port, no empty authority with nothing after it)
  accepts nothing Java refuses on that set, and refuses 3 that Java accepts (`http://example.com/ü`,
  `http://host:abc/`, `http://ex%41mple.com/`). Stricter than Java is the safe direction (D9). A
  sample is not a proof: the vectors must grow a URI section, and the rule should be stated in the
  contract as a grammar, not as "what `java.net.URI` accepts".
- **G3, blank.** `unicode.IsSpace` but not Java whitespace: U+0085, U+00A0, U+2007, U+202F. Java
  whitespace but not `unicode.IsSpace`: U+001C to U+001F. A Go function reproducing
  `Character.isWhitespace` exactly (checked over all code points) is 10 lines; the contract can list
  the set.
- **Lock order (§4.6).** Java's `TreeSet` order and Go's byte order differ as soon as a character
  above U+FFFF meets one in U+E000 to U+FFFF (`😀` sorts before `\uE000`, `Ａ` and `\uFFFD` in Java,
  after them in bytes). A comparison by UTF-16 code units in Go (`utf16.Encode` plus `slices.Compare`)
  matches Java exactly. Cheap enough to require, rather than accept the deadlock risk the HLD
  tolerates.

### 7.4 New decisions raised

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D15 | `lib/pq` floor | ≥ v1.10.6 (has `SQLState()`) · any version, duplicates degrade to a generic error below it | **v1.10.6**, stated in the README; below it everything works except the duplicate-seq classification |
| D16 | Lock order in SDKs | UTF-16 code units, as Java · byte order, accepting a rare deadlock | **UTF-16**, stated in the contract and pinned by a vector |
| D17 | Typed `dataschema` setter (G4) | drop it from HLD §5.3 · add it to Java | **drop it**: the release candidate window takes fixes only; Java can gain it in a 1.x minor, and the SDKs follow then |
| D18 | Empty header name (G7) | allowed, as Java does, stated in the contract and pinned by a vector · refused | **allowed**: refusing it would add a rejection Java does not have |
| D19 | Expected CloudEvent envelope (G8) | a vector produced and verified by `tandem-kafka`'s tests, `id` and `time` left out · no artifact | **a vector**: Go's CI has no JVM, so layer 3 of HLD §6 needs the envelope as data |
