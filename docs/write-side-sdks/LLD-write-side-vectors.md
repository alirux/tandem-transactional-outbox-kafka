# Tandem: Write-side Test Vectors (LLD)

**Version:** 0.1  
**Status:** Draft. Designed, not implemented.  
**Companion to:** [HLD-write-side-sdks.md](HLD-write-side-sdks.md) §4, §6;
[LLD-api-stability.md](../LLD-api-stability.md) §8

The test vectors are the write contract of HLD-write-side-sdks §4 written as data: inputs and the
results every writer must reproduce, in files any language can read. This document fixes where they
live, their format, what each file holds, how the Java build generates and verifies them, and how an
SDK reads them.

---

## 1. Summary

- The vectors are JSON files in `schema/write-side/`, outside every Gradle module and every published
  artifact.
- **Java generates them.** Each file belongs to one Java test, which holds the inputs in code, runs
  them through the Java writer, and renders the whole file, inputs and results. The test compares the
  rendering with the committed file; with `-PupdateReferenceFiles` it rewrites the file instead, as
  for the reference files of LLD-api-stability §8.1. Review judges the diff.
- **Reviewed against the specification, not against Java.** Every change to a case goes through a
  review that derives the expected results from the HLD and the standards it cites without reading the
  Java writer (§6), so a Java bug cannot become the contract unnoticed.
- **Every SDK verifies them.** It reads the files, runs each case through its own code, and compares.
  It never writes them.
- Two inputs no Java string can hold, or Java does not compute, are written by hand inside the Java
  test: byte sequences that are not UTF-8 (§4.4) and the verdicts of the `dataschema` grammar
  (§4.5). Java checks what it can about them.

---

## 2. Location

`schema/write-side/`, beside `schema/postgres/`, because the write contract is part of the schema
contract and its expected values do not depend on the engine (HLD-write-side-sdks §6). Only
`schema/postgres/changelog/` is packaged into `tandem-jdbc`, so no vector reaches a published
artifact. An SDK in this repository reads them by a path relative to the repository root.

| File | Rendered by | Test task |
|---|---|---|
| `bucket.json` | `BucketVectorsTest`, `tandem-jdbc` | `test` |
| `lock-order.json` | `LockOrderVectorsTest`, `tandem-jdbc` | `test` |
| `not-unicode.json` | `NotUnicodeVectorsTest`, `tandem-jdbc` | `test` |
| `dataschema.json` | `DataSchemaVectorsTest`, `tandem-jdbc` | `test` |
| `rejections.json` | `RejectionVectorsIT`, `tandem-jdbc` | `integrationTest` |
| `rows.json` | `RowVectorsIT`, `tandem-jdbc` | `integrationTest` |
| `database-rejections.json` | `DatabaseRejectionVectorsIT`, `tandem-jdbc` | `integrationTest` |
| `envelope.json` | `EnvelopeVectorsIT`, `tandem-kafka` | `integrationTest` |

Every file but the envelope belongs to `tandem-jdbc`: it holds the writer (`JdbcOutboxRepository`),
the lock order and the `dataschema` check, and its tests reach `BucketHash` through `tandem-core`.
Rendering the bucket vectors in `tandem-core` would give that module's tests a dependency on
`tandem-test`, which brings the JDBC and Kafka adapters with it, for one file.

---

## 3. Format

### 3.1 The file

A file is one JSON object, UTF-8, LF line endings, a final newline:

```json
{"comment": "Rendered by BucketVectorsTest; regenerate with ./gradlew :tandem-jdbc:test -PupdateReferenceFiles",
 "cases": [
  {"name": "ascii-default-count", "aggregate_id": "order-7", "bucket_count": 256, "hash": "dee47a9f34bede3b", "bucket": 59},
  {"name": "ascii-top-bit-hundred", "aggregate_id": "order-7", "bucket_count": 100, "hash": "dee47a9f34bede3b", "bucket": 43}
 ]}
```

- **`comment`** names the rendering test and the regeneration command, the role the first comment
  line plays in a reference file, since JSON has no comments.
- **One case per line**, so a diff shows each case that changed and nothing else.
- **`name`** identifies a case: kebab-case, unique in its file, stable. An SDK's test reports a
  failure by it, and a case keeps its name for as long as it exists.
- **Keys** are snake_case and follow the contract's column names where a column exists. Their order is
  fixed by the renderer, for stable diffs; a reader does not depend on it.
- **Escapes.** A string writes `"`, `\` and the code points below U+0020 escaped, as JSON requires, and
  also every code point of either whitespace set of HLD-write-side-sdks §4.1 (U+0085, U+00A0, U+1680,
  U+2000 to U+200A, U+2028, U+2029, U+202F, U+205F, U+3000), so that a whitespace case is visible in a
  diff. Everything else is written as UTF-8, characters above U+FFFF included.
- Additional top-level keys hold what all cases of a file share (the relay settings of
  `envelope.json`, the base message of `not-unicode.json`).

### 3.2 The message

The files that take a message as input write it the same way:

```json
{"aggregate_id": "order-7", "aggregate_type": "Order", "type": "OrderPlaced",
 "seq": {"mode": "application", "value": 3}, "locked_write": false,
 "content_type": "application/json", "headers": {"x-tenant": "acme"}, "payload": "{\"total\":10}"}
```

| Key | Presence | Meaning |
|---|---|---|
| `aggregate_id`, `aggregate_type` | always | as in HLD-write-side-sdks §4.1 |
| `type` | optional | absent means no event type |
| `seq` | always | `{"mode": "application", "value": n}`, `{"mode": "managed"}` or `{"mode": "none"}`. Only a rejection case writes `null` (no mode stated) or an array of modes (more than one stated) |
| `locked_write` | always | `true` when the message asks for the advisory lock (HLD-write-side-sdks §4.6) |
| `content_type` | optional | the typed setter; it overrides a `content-type` entry of `headers` |
| `headers` | always | an object of string to string, `{}` when empty; names in insertion order |
| `payload` | always | the serialized event, as a JSON string holding its UTF-8 text |

### 3.3 Reading rules for an SDK

- **Ignore unknown keys** at the top level and in a case. A file grows additively (§7): a key added
  for a new optional feature must not fail an SDK that does not know the feature.
- **Skip a case whose `message` holds a key the SDK does not know**, and report it as skipped. Such a
  key is an input the SDK cannot send, so running the case without it would compare a different
  message with the expected result.
- Compare `headers` and payloads **as JSON values**, never as text: PostgreSQL's `jsonb` reorders keys
  and normalizes spacing.
- Run every case, and report each failure by file and `name`.

---

## 4. The files

### 4.1 `bucket.json`

Case: `aggregate_id`, `bucket_count`, `hash` (FNV-1a 64 of the UTF-8 bytes, 16 lowercase hexadecimal
digits, unsigned), `bucket`.

The hash is there so that a port that gets the bucket wrong can tell whether its hash or its reduction
is wrong (HLD-write-side-sdks §4.2). The cases cross identifiers (ASCII, two-, three- and four-byte
UTF-8 characters, an identifier of 255 code points, one whose hash has the top bit set and one whose
hash has it clear) with bucket counts (1, 7, 100, 256, 1000, 32767). Java computes `bucket` with
`BucketHash.bucketFor` and `hash` with the same FNV-1a loop the test writes out, checked against a
published FNV-1a 64 test value.

**A case of this file never changes once committed.** The bucket is stored for the life of the data,
so a changed line is a broken contract, whatever its review says. The test enforces it beyond the
diff: it holds the committed cases' buckets as literals too, so a change to `BucketHash` fails even in
update mode.

### 4.2 `lock-order.json`

Case: `messages` (each an `aggregate_id` and its `locked_write`), `lock_order` (the identifiers whose
lock the writer takes, in the order it takes them).

It pins three rules of HLD-write-side-sdks §4.6 together: only `locked_write` messages lock, each
identifier is locked once, and the order compares UTF-16 code units. One case puts `"😀"` against
`""` and `"Ａ"`, where UTF-16 and byte order disagree. Java computes `lock_order` with the
writer's own ordering, which this design extracts from `JdbcOutboxRepository` into a package-private
static method so the test calls the code the writer runs.

### 4.3 `rejections.json`

Case: `message`, `field` (the field that makes it invalid: `aggregate_id`, `aggregate_type`, `type`,
`seq`, `payload`, `dataschema` or `headers`), `java` (where the Java writer refuses it: `build`, when
the message is built, or `insert`, when the insert runs).

Every case is refused by an SDK **before any statement**. `java` records where Java refuses it, which
is not always that early: Java refuses U+0000 only when the insert runs, in the driver or the database,
and an SDK refuses it at validation. The test asserts that Java refuses every case somewhere, so no case can claim a
rejection the reference implementation does not make. It runs against PostgreSQL for that reason.

The cases cover the blank rule on each code point where the two whitespace sets differ (a value made
only of U+00A0 is accepted, so it is a row vector, §4.6, not a rejection), the length bound at 256
code points for each bounded field, U+0000 in each text field, a missing payload, no `seq` mode and two
modes, and an invalid `dataschema`.

### 4.4 `not-unicode.json`

Top level: `base_message`, a valid message. Case: `field` (`aggregate_id`, `aggregate_type`, `type`, a
header name or a header value) and `bytes`, hexadecimal, that replace that field's value.

These inputs cannot be strings: JSON and Java can hold only valid Unicode. The cases are written by
hand in the test, and Java checks the one thing it can, that each `bytes` value fails to decode as
UTF-8 with a reporting decoder. The cases: a lone continuation byte, a truncated multi-byte sequence,
an overlong encoding, an encoded surrogate (`ed a0 80`), and a value above U+10FFFF.

### 4.5 `dataschema.json`

Case: `value`, `grammar` (`accept` or `refuse`: the verdict of the grammar of HLD-write-side-sdks
§4.4), `java` (`accept` or `refuse`: the verdict of `java.net.URI`, which the Java writer applies).

`grammar` is written by hand in the test: Java does not implement the grammar, and implementing it in
test code would make the test check itself. `java` is computed through the writer's own check. The test
fails on any case with `grammar` `accept` and `java` `refuse`, the one combination the contract forbids
(a row an SDK writes that the relay cannot publish). The cases include each example of the HLD, the 16
values of the proof of concept that `net/url` accepts and Java refuses, and values Java accepts that
the grammar refuses (a non-ASCII path, a non-numeric port).

An SDK checks `grammar` only; `java` is there for the reader.

### 4.6 `rows.json`

Case: `bucket_count`, `message`, `row`, the row read back: `bucket`, `seq` (a number, `null`, or
`"assigned"` for a MANAGED row, whose value the sequence decides), `seq_source`, `type`, `headers`,
`correlation_id`, `payload` (the `jsonb` value as PostgreSQL returns it, compared as JSON).

Java inserts each message with `JdbcOutboxRepository` into the schema of
`schema/postgres/tandem-baseline.sql`, in a transaction it rolls back, and reads the row by named
columns. The cases: each `seq` mode, with and without `type`, `content_type` overriding a header, an
empty header map, an empty header name (HLD-write-side-sdks §4.4), a value made only of U+00A0, an
`aggregate_id` of 255 code points longer than 255 UTF-16 code units, and the `correlation_id` copy
untruncated, truncated, and truncated where the 255th code point lies above U+FFFF.

### 4.7 `database-rejections.json`

Case: `messages`, inserted in order in one transaction, `failing` (the index of the message the
database refuses), `sqlstate`.

The rows the database refuses even from a correct writer: a duplicate `(aggregate_id, seq)` (`23505`)
and a payload that is not JSON (`22P02`). Java reads the SQLSTATE from the exception the writer throws,
which keeps it although it withholds the driver's text (HLD-write-side-sdks §4.9).

### 4.8 `envelope.json`

Top level: `relay`, the settings the published event depends on: `source`, the default content type,
the default data schema (absent), the topic router (`kebab-with-suffix`, suffix `-topic`). Case:
`message`, `published`: `topic`, `key`, `headers` (an object, without `ce_id` and `ce_time`, which
change on every publication), `body` (compared as JSON).

Java inserts the message with `JdbcOutboxRepository`, reads the row back as the relay reads it, and
encodes it with `KafkaMessageEncoder.cloudEvents` under the `relay` settings. The broker adds nothing
to a record, which `KafkaRelayIT` already pins, so the test stops at the encoded record. The row goes
through PostgreSQL because the body the relay publishes is the payload as `jsonb` returns it.

An SDK's end-to-end test (HLD-write-side-sdks §6, layer 3) starts the relay image with the `relay`
settings, writes each case's message, consumes the event, and compares it with `published`.

---

## 5. Generation and verification in the build

- **Renderer.** A test fixture of `tandem-test`, `VectorFile`, beside `ReferenceFile`, renders a file
  from a list of cases (each a list of key and value pairs) under the rules of §3.1, and compares or
  writes it through `ReferenceFile`.
- **`ReferenceFile` outside a module.** A second factory resolves a path against the repository root
  (the first directory upward holding `settings.gradle.kts`) instead of the module's
  `src/test/resources/reference/`, and takes the test task's name, so that the regeneration command of
  an integration test names `integrationTest`. The comparison, the update mode and the failure message
  stay as they are.
- **Inputs of the test tasks.** Every `Test` task of `tandem-jdbc` and `tandem-kafka` declares
  `schema/write-side/` as an input. Without it, a file edited by hand would leave the task up to date,
  and the build cache would replay a pass.
- **Regeneration.** `./gradlew :tandem-jdbc:test :tandem-jdbc:integrationTest
  :tandem-kafka:integrationTest -PupdateReferenceFiles`, refused in CI like every update
  (LLD-api-stability §8.1).

---

## 6. Independent review against the specification

Generated by Java, the vectors record what Java does, which is the contract only where Java is right.
A Java bug would enter the files as an expected value, pass every Java build, and bind every SDK to it.
Reviewing the diff of a regeneration does not catch it either: a reviewer reading results next to the
code that produced them tends to confirm them. The vectors therefore go through a review that ignores
where they came from.

**When.** On the change that first adds the files, and on every later change that adds or alters a
case. A change limited to `comment` lines needs none.

**How.** The reviewer works from HLD-write-side-sdks and the standards it cites (FNV-1a, RFC 3986,
Unicode, the CloudEvents Kafka binding) and the changed files, and **does not read the Java writer or
the tests that render the files**. A person or an agent given only those inputs qualifies. For each
case the reviewer derives the expected result from the specification and compares it with the file,
and looks for what the cases fail to cover:

| File | What the reviewer checks independently |
|---|---|
| `bucket.json` | recomputes `hash` and `bucket` with an implementation written from §4.2 of the HLD alone, in a language other than Java, and checks the hash against published FNV-1a 64 values; checks that the cases include top bit set and clear and counts that are not powers of two |
| `lock-order.json` | derives each order from the UTF-16 rule and the `locked_write` flags; checks a case exists where UTF-16 and byte order disagree |
| `rejections.json`, `rows.json` | checks each accepted or refused value against the blank set, the code point lengths, the Unicode text rule and the seq modes of HLD §4.1 and §4.3; checks each expected row column by column against §4.1 and §4.4 (`content_type` overriding the header, the `correlation_id` truncation point) |
| `not-unicode.json` | checks each `bytes` value is ill-formed under the UTF-8 definition of the Unicode standard, and that each class of ill-formed sequence appears |
| `dataschema.json` | derives the `grammar` verdict of each value from the RFC 3986 `URI-reference` grammar and the three narrowing rules of HLD §4.4 |
| `database-rejections.json` | checks each SQLSTATE against PostgreSQL's documented error codes |
| `envelope.json` | derives each published header and the topic from the CloudEvents Kafka binding, HLD-cloudevents and the `relay` settings, and the body from the payload |

**Outcome.** The findings are recorded in the pull request. A disagreement is resolved by deciding
which side is wrong, never by editing a file by hand: when Java is wrong, Java is fixed and the files
regenerated; when the specification is silent or ambiguous, the HLD is amended first and the case
follows it. A case that only the review could have caught also becomes a plain unit test of the Java
writer, so the build guards it from then on.

---

## 7. Evolution

The vectors follow the reading of a reference file (LLD-api-stability §8.1): a case added is a minor
change of the contract, a case changed or removed is a breaking one, and `bucket.json` never changes
(§4.1). The release checklist therefore reads `schema/write-side/` alongside the reference files:
AGENTS.md (Releases, step 1) and LLD-api-stability §11 gain that path in the change that adds the
files.

A new optional field of the message or the row is a new key plus new cases that use it. An SDK that
does not know the feature keeps passing: it ignores the key in a result, and skips a case whose
message holds it (§3.3).

---

## 8. Rejected alternatives

- **Inputs written by hand in the JSON, results filled in by Java.** The Java tests would need a JSON
  parser, which neither `tandem-core` nor `tandem-jdbc` has on its test classpath, and the file would
  have two authors. With the inputs in Java, one test owns each file whole.
- **Vectors in each module's `src/test/resources/reference/`.** An SDK's tests would read files from
  inside Java modules, and the vectors would be split across directories by which Java module renders
  them, an accident of the Java build that means nothing to an SDK.
- **Line-oriented text, as the reference files.** Every language reads JSON with its standard library,
  and the message, with nested headers, has no natural one-line text form.
