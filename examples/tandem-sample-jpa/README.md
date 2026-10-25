# JPA/Hibernate with Spring Boot 4

A runnable write-side example on Spring Boot 4, Hibernate, PostgreSQL and Kafka: how
concurrent JPA writers keep each aggregate's events in order.

With Docker running, use `./examples/tandem-sample-jpa/run.sh` (or `run.cmd` on
Windows), as for `tandem-sample-spring`. Alternatively, from the repository root:

```sh
./gradlew :tandem-sample-jpa:run
./gradlew :tandem-sample-jpa:check
```

Like `tandem-sample-spring`, the runner writes, inspects the outbox, then checks with a
real Kafka consumer what the autoconfigured relay delivered:

1. Two child notes in one transaction, each with its own domain event revision.
2. Two overlapping transactions on one aggregate. PostgreSQL confirms that the
   second is waiting for the first root lock before the runner releases the first.
3. Two overlapping commands mentioning two aggregates in opposite order, showing
   why the complete lock set is acquired in stable order.
4. Two overlapping transactions that only add children and lock no root row:
   Tandem's per-aggregate lock makes the second wait instead.
5. A separate stream using the JPA version after flush, followed by a no-op.
6. A rollback after outbox inserts, followed by a successful command reusing revision 1.
7. Outbox rows in insertion order, with their current delivery status.
8. All twelve committed events consumed from Kafka in their actual delivery order,
   checking each aggregate's expected revisions. Duplicate deliveries are removed by
   event ID, consistent with Tandem's at-least-once contract.
9. A clearly labelled simulated delivery failure and ready-to-run Admin API commands
   for inspecting messages, replaying or discarding the failed row and controlling the relay.

A lock timeout, missing delivery or wrong sequence fails the run. Afterwards the
application keeps running as a web server and prints Admin API commands on port 8080
(`SERVER_PORT=8081` runs it beside `tandem-sample-spring`); Ctrl+C stops it. The
`order:rollback` row it marks FAILED is a simulated delivery failure, unrelated to the
JPA rollback of step 6.

`check` runs the Docker-bound tests on real PostgreSQL and Kafka: `WriteOrderingIT`
holds the concurrency and rollback scenarios, unsafe counterexamples included, with
the relay disabled; `JpaSampleSmokeIT` boots the whole runner, then consumes Kafka
and calls the Admin API on its own. Lock inspection uses PostgreSQL's
`pg_blocking_pids`, so the example covers PostgreSQL only.

Only `OrderService`, the entities and `application.yml` are application code.
`DemoContainerInitializer` (registered in `META-INF/spring.factories`) starts the
containers before Boot configures the DataSource, and `SampleRunner` narrates; both
are demo-only. Hibernate's `create-drop` manages only the example's entity tables,
the Tandem schema comes from the container. A real application supplies its own
DataSource and applies reviewed migrations.

## What order is guaranteed?

For **one aggregate**, the root row lock determines the serialization order. The
first writer holding it inserts and commits before the next writer can allocate
its event sequence. This is not request-arrival order, thread-start order or wall
clock order. Lock acquisition is not a FIFO queue. A rejected transaction produces
no committed event.

For **several aggregates in one transaction**, acquire the entire set of root locks
in sorted, distinct ID order before changing anything. `append([B, A])` and
`append([A, B])` therefore contend on the same first lock, avoiding the classic
opposite-order deadlock. Preserve the command's original event order after taking
the locks; do not sort the business operations themselves.

For **independent aggregates**, transactions remain independent: B can commit while
A is still open. There is no total chronological order across aggregates, no global
meaning to comparing A's `seq` with B's, and no atomic visibility across Kafka
partitions. A multi-aggregate database commit does not create such a delivery
guarantee. See [causal ordering](../../docs/HLD-causal-ordering.md) for that separate
concern and its implementation status.

## The recommended path in this example

Read [OrderService.java](src/main/java/com/codingful/tandem/sample/jpa/OrderService.java)
starting at `append`:

1. Enter one Spring transaction through the service proxy. Boot supplies a
   `JpaTransactionManager`; JPA and Tandem JDBC use the **same DataSource and physical
   transaction**. Tandem's producer autoconfiguration supplies the transaction-aware
   repository. Do not construct a repository on an unrelated pool or use
   `DataSourceTransactionManager` for Hibernate work.
2. Find every root with `PESSIMISTIC_WRITE`, in sorted ID order, before reading the
   revision or modifying children. The roots must already exist: locking a missing
   row is not a portable creation lock. Creation races require a uniqueness policy
   and retry of the complete creation transaction.
3. Append a child note and advance the root's **domain event revision**
   (`eventSequence`) once for each emitted event. Hibernate's `@Version` is separate
   optimistic concurrency metadata. This also supports two events in one command
   and an event that changes no business field on the root. Advancing this revision
   intentionally dirties the root even for a child-only business operation.
4. Insert the outbox row with the revision just allocated. No `flush()` is needed:
   the lock is already held, the revision is read from memory, and domain and
   outbox rows become visible together at commit, whatever order Hibernate sends
   its SQL in. A flush per event would only repeat Hibernate's dirty checking over
   the whole persistence context and break JDBC batching. Flush explicitly only
   when the outbox row needs something that exists after a flush: a JPA `@Version`
   (next section), or a row lock that comes from the flushed `UPDATE` because no
   lock was taken first (LLD-spring-producer §7).
5. Commit root, children, counter and outbox together. An exception after even the
   last insert rolls all of them back. The next successful transaction can reuse a
   rolled-back domain revision. Database-generated outbox IDs may have gaps.

Use globally unambiguous aggregate IDs (for example `order:<uuid>`). Tandem's
ordering identity is the aggregate ID, not `(aggregateType, aggregateId)`.

Every writer, including batch jobs and other service instances, must follow the
same locking protocol. One call must know the complete lock set for its transaction:
calling `append([B])` then `append([A])` inside an outer transaction defeats the
sorted-lock rule. Enter with a fresh persistence context; do not load stale roots
before acquiring locks, merge detached snapshots, or mutate entities before the
service runs. Disable Open EntityManager in View as shown. Do not share managed
entities or an EntityManager across threads.

The sample inserts synchronously inside the transaction. `@Async`, after-commit
listeners, a different transaction manager, or `REQUIRES_NEW` around only the outbox
break the business/outbox atomicity. Proxy self-invocation also bypasses Spring's
transaction advice. For retryable deadlocks, lock timeouts or optimistic conflicts,
roll back and retry the **whole command in a fresh transaction**, with bounded
backoff. Retrying just the insert or continuing in an aborted transaction is wrong.
Request retries after an uncertain commit additionally need business idempotency.

## When seq really must be JPA's version

`confirmWithVersion` demonstrates the narrower alternative on **separate aggregate
IDs**: acquire the root lock, mutate persistent state, flush, then read `@Version`
and insert one event. A repeated confirmation changes no state, so it emits no
second event. The test verifies both the persisted version and absence of a
second outbox row.

Do not mix this method with `append` on the same aggregate stream: they assign
sequences from different sources. Choose one sequence contract for that stream.
Do not manually increment an `@Version` field. Do not capture its value in an event
before flushing and expect a later flush to update that event. Two events from one
flush still share one JPA version. If each event needs a sequence, use a dedicated
revision as above, or choose a different sequence mode explicitly.

## When writers never touch the root

Some writers only add children (an order line, a note) and never update or lock the
root. They have no shared row to serialize on, and flushing does not help. The test
counterexample below shows the result: a later event commits while an earlier one is
still invisible. `appendChildOnly` shows the fix that needs no domain row at all:

1. `lockedWrite()` makes Tandem take a transaction-scoped PostgreSQL advisory lock on
   the aggregate ID right before the outbox insert, held until commit. A concurrent
   writer to the same aggregate waits there, so commit order equals insert order.
2. `managedSeq()` lets the database number the event at that insert, after the lock.
   An application-assigned `seq` computed before the call would be read too early.
   Tandem's sequence is shared by every managed stream, so the numbers increase per
   aggregate but are not dense: consumers must not expect `1, 2, 3`.

It is opt-in per message, because it turns concurrent writers to one aggregate into
waiters. Every writer of that aggregate must use it: one path without it is not
serialized. Keep it on its own stream, as here: a stream takes one sequence contract.
`unsequenced()` also combines with `lockedWrite()` when consumers need no `ce_seq`.

## Warnings made observable

| Scenario in `WriteOrderingIT` | Observable result |
| --- | --- |
| Concurrent writers on one root | PostgreSQL reports the second session blocked; no event is visible before the first commits; insertion order is `[1, 2]`. |
| Commands mention two roots in opposite order | The second waits; both streams retain `[1, 2]`. |
| Disjoint roots | B commits while A's row is still invisible. |
| Repeated child notes, including two in one command | Every event gets a distinct revision: `[1, 2, 3]`. |
| Failure after outbox inserts | Children, root counter and outbox roll back together; retry starts at `1`. |
| State change followed by a no-op | One row carries the flushed JPA version. |
| **Unsafe:** two events read an unflushed version | `DuplicateSeqException`; business state and outbox roll back. |
| **Unsafe:** only children are flushed, no root lock | The second writer commits `seq=2` while `seq=1` is invisible. A polling relay cannot wait for an invisible predecessor. |
| Children only, with `lockedWrite()` and `managedSeq()` | PostgreSQL reports the second session blocked on the advisory lock; nothing is visible before the first commits; both numbers increase in insertion order. |

The negative examples live only in the test. The concurrency checks wait for
`pg_blocking_pids`, with bounded deadlines, instead of assuming a sleep proves a
writer was blocked. Queries sort by outbox **id**, never by seq, since sorting by
seq would conceal the behavior under test. No-op/version assertions and rollback
checks would fail if domain and outbox used separate transactions.

A unique `(aggregate_id, seq)` constraint catches duplicate numbers, not reordered
commits. An increasing outbox ID, timestamp, sequence generator, or one relay bucket
cannot repair concurrent writes that commit out of order. An optimistic `@Version`
conflict requires retry; it does not make failed commands succeed automatically.
Flushing only children locks their rows, not a shared aggregate root.

`managedSeq()` and `unsequenced()` change the number contract, not write
serialization. `lockedWrite()` serializes outbox writers, but it cannot refresh
business state read before its lock or fix numbers calculated before it. Do not mix
root-first and advisory-first lock acquisition across paths; for multi-aggregate work
acquire the complete lock set consistently. Root locks remain the default here;
`lockedWrite()` is for the writers that have no root to lock.

## Further reading

* [Sequence and concurrent write hazards](../../docs/HLD-managed-seq.md), especially §3 and §4.
* [Spring write tiers and JPA warnings](../../docs/LLD-spring-producer.md#7-the-seq--version-invariant-all-tiers).
* [Spring JPA transaction integration](https://docs.spring.io/spring-framework/reference/data-access/orm/jpa.html).
* [Hibernate flushing and locking](https://docs.hibernate.org/orm/7.4/userguide/html_single/).
