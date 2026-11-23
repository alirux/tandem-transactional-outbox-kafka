# Personal Data and GDPR

---

## 0. Who this page is for

Your events carry personal data, or might, and you need to know where Tandem puts it, how long it stays
there, who can read it, and what to do when someone asks for their data to be erased.

!!! question "The question this page answers"
    **Where does personal data go once I write it to the outbox, and what is mine to control?**

This page describes what Tandem does with the data you give it. It is not legal advice: which data is
personal, which legal basis applies and how long you may keep it are decisions for you and whoever
handles data protection in your organization.

Tandem is a library and a relay that run in your own infrastructure. It sends nothing anywhere except to
your database and to your broker: no telemetry, no external service.

---

## 1. Where the data goes

An event you write passes through these places. Everything in this table is **in clear text**: Tandem
does not encrypt or pseudonymize anything.

| Place | What it holds | How long | Who can read it |
|---|---|---|---|
| The `tandem_outbox` table | The aggregate id, the payload, the headers, the correlation id | Until cleanup: 14 days by default for delivered rows; **forever** for `FAILED` rows (§3) | Anyone with access to the database |
| The broker | Key = aggregate id, value = payload, every header (`ce_subject` and `ce_partitionkey` repeat the aggregate id) | The topic's own retention, which Tandem does not set | Anyone your broker's ACLs let consume the topic |
| [Admin API](admin-api.md) and [CLI](cli.md) | A single message in full, payload and headers included; lists and searches show the aggregate id but not the payload | Live view of the table | Whoever you let reach the Admin API (§5) |
| Logs | Row ids, aggregate ids and types, buckets, topics, the correlation id. **Never** payloads or header values | Your log platform's retention | Whoever reads your logs |
| Traces, when the publish span is on | The aggregate id (`tandem.aggregate.id`) and the correlation id (`tandem.correlation_id`) as span attributes | Your tracing backend's retention | Whoever reads your traces |
| Error messages | The aggregate id, in the exceptions Tandem throws when an insert fails | Wherever your application logs them | Whoever reads your logs |

The pattern is simple: the **payload** and the **header values** stay in the table, on the broker and
in the Admin API detail view. The **aggregate id** and the **correlation id** go everywhere, logs and
traces included, because they are what you use to find a row during an incident.

---

## 2. Choosing what goes into an event

The cheapest personal data to protect is the data you never put in. Each part of an event has a
different exposure, so decide part by part.

### 2.1 The aggregate id is never secret

The aggregate id is the ordering key: Tandem hashes it to pick a bucket, locks on it, and uses it as the
Kafka key. It cannot be encrypted, and it appears in every place of §1, logs and traces included.

**Use a surrogate id**, a technical identifier with no meaning outside your system (`customer-8f3a…`,
a database key), never an email address, a tax code, a phone number or a name. A surrogate id is still
personal data if it can be linked back to a person, but it says nothing on its own, and the link lives
only in your database, where you can erase it.

If an existing key really is personal data and you cannot change it, you can pass a **keyed hash** of
it instead, for example an HMAC with a secret key your application holds. The same input always gives
the same hash, so ordering still works. Two things to know before you do it:

- a consumer that needs the real value cannot recover it from the hash: put it in the payload, where
  your other protections apply;
- the hash is only as safe as its key. Rotating the key changes every aggregate id, which breaks the
  ordering between the events written before and after the rotation.

### 2.2 Headers travel in clear and are copied as they are

Every header you attach to an event is stored in the row and copied unchanged onto the broker record.
Header values are never logged, but anyone who can read the table, the topic or the Admin API detail
view sees them. Put no personal data in a header unless a consumer must filter on it without reading the
payload.

The **correlation id** is a special case: Tandem also stores it in a column of its own and writes it to
logs and traces. It should be an opaque identifier (a request id, a saga id), never a user's email or
account name.

### 2.3 The payload

The payload is stored and published exactly as you wrote it. Put in it what consumers need and nothing
more: an event that carries a customer id lets a consumer look up the details it is entitled to, while
an event that carries the customer's full profile copies that profile into every consumer.

When a field must travel but only some consumers may read it, encrypt that field yourself before writing
the event. Tandem does not provide field encryption, but your own
[`PayloadSerializer`](advanced-integration.md#5-object-payloads-and-the-serializer) is the natural place
for it: the row, the broker and the Admin API then only ever see the encrypted value.

### 2.4 Never secrets

Passwords, tokens, API keys and similar secrets never belong in an event: not in the payload, not in a
header. Tandem treats both as data to keep and to deliver, not as something to protect. The credentials
Tandem itself needs, for the database and the broker, belong in your secret store; for the relay image,
see [secrets as files](relay-image.md#52-secrets-as-files).

---

## 3. How long the outbox keeps it

The relay deletes rows that are `DONE` or `DISCARDED` once they are older than `tandem.relay.retention`,
**14 days** by default, counted from when the row was created
([Reliability and Topology](reliability.md#53-cleanup-and-retention)).

- **Shorter retention means less exposure**, but a cleaned-up row can no longer be replayed. Choose the
  shortest window you are willing to replay from.
- **`FAILED` rows are never deleted.** They wait for an operator, with their payload, for as long as it
  takes. An [alert on failed rows](observability.md#33-alerts-worth-having) is therefore also a data
  protection measure: it keeps them from being forgotten.
- Two free-text columns can hold data you did not write as an event: `last_error`, the broker's error
  text (or your own encoder's, if you wrote one, so keep payload content out of its exception
  messages), and `discard_reason`, the reason an operator typed when discarding a row. Ask operators not
  to put personal data in a discard reason.

---

## 4. An erasure request

When a person asks for their data to be erased, check each place of §1 in turn.

### 4.1 In the outbox

Find the person's rows, by the aggregate id or by a field of the payload:

```sql
SELECT id, status FROM tandem_outbox WHERE aggregate_id = 'customer-8f3a';
SELECT id, status FROM tandem_outbox WHERE payload @> '{"customerId": "customer-8f3a"}';
```

Then act by status. Status codes: `0` `PENDING`, `1` `IN_FLIGHT`, `2` `DONE`, `3` `FAILED`,
`4` `DISCARDED`.

| Status | What to do |
|---|---|
| `DONE`, `DISCARDED` | Delete them. This is exactly what cleanup does, only earlier, and it does not affect ordering or the events still to be delivered. |
| `FAILED` | It will not be delivered by itself. Either replay it and let it be delivered, or [discard it](admin-api.md#33-recovering-messages) (which skips it, an ordering break you acknowledge). Then delete it as `DISCARDED` or `DONE`. |
| `PENDING`, `IN_FLIGHT` | Let it be delivered: it normally takes seconds. Then delete it as `DONE`. Deleting it before delivery skips an event the consumers have not received. |

```sql
DELETE FROM tandem_outbox
WHERE aggregate_id = 'customer-8f3a' AND status IN (2, 4);   -- DONE, DISCARDED
```

### 4.2 On the broker

A record on a topic cannot be edited or removed one by one. What removes it is the topic's own policy:

- on a topic with **time or size retention**, the record disappears when its segment expires;
- on a **compacted** topic, only the latest value of each key is kept. Publish a new event for the same
  aggregate that carries no personal data (an "erased" state, for example), and compaction removes the
  earlier records in time. The key itself stays, which is one more reason for a surrogate id (§2.1).

Tandem cannot publish a Kafka tombstone (a record with an empty value) today: every event has a payload.

### 4.3 In the consumers

Consumers often keep their own copy of what they read, in their own database. Neither Tandem nor the
broker can reach it. Publish an **erasure event** (`com.acme.customer.erased`, say) that every consumer
holding the person's data handles by deleting its copy. Since it is an event like any other, it is
delivered in order after the person's earlier events, so a consumer never receives an update after the
erasure for the same aggregate.

---

## 5. Who can read it

- **The database.** The `tandem_outbox` table holds every event in clear text until cleanup. Grant
  access to it as you would to the tables the events come from.
- **The broker.** Restrict who may consume each topic with your broker's ACLs.
- **The Admin API and the CLI.** The detail view of a message shows its payload and headers. Tandem does
  not authenticate the Admin API: [securing it is yours](admin-api.md#13-security-is-yours), and the
  people you let use it can read every event still in the table. Write operations are logged with the
  `actor` your authentication established, which is how you know who replayed or discarded what.
- **Logs and traces.** They carry aggregate ids and correlation ids (§1), never payloads. If your aggregate
  ids are surrogate ids, logs and traces hold nothing a reader can link to a person without your
  database.

---

## 6. Checklist

- [ ] Aggregate ids are surrogate ids, not emails, tax codes, phone numbers or names.
- [ ] The correlation id is an opaque request or saga id.
- [ ] Headers carry no personal data unless a consumer must filter on it.
- [ ] Payloads carry only what consumers need; fields only some consumers may read are encrypted by your serializer.
- [ ] No secret ever goes into an event.
- [ ] `tandem.relay.retention` is the shortest replay window you accept.
- [ ] An alert fires on `FAILED` rows, so none stays in the table forgotten.
- [ ] Operators know not to put personal data in a discard reason.
- [ ] Access to the outbox table, the topics and the Admin API is restricted.
- [ ] Topic retention or compaction is set to match how long consumers may keep the data.
- [ ] There is an erasure event, and every consumer that keeps a copy handles it.
