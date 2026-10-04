package com.codingful.tandem.test;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.MessageEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * The contract every {@link MessageEncoder} must keep, as a check an encoder author runs in their own
 * test suite (LLD-alternative-envelope §3, LLD-test §6). It holds an envelope to the invariants
 * Tandem's delivery promises rest on (HLD-alternative-envelope §3):
 *
 * <ol>
 *   <li>every row shape the write side accepts encodes, unsequenced and untyped rows included;</li>
 *   <li>two encodes of one row are equal (destination, key, body and headers);</li>
 *   <li>the event id is present, and stable across those two encodes;</li>
 *   <li>distinct rows have distinct event ids;</li>
 *   <li>the ordering key is the aggregate id, unless the encoder
 *       {@linkplain #declaresOwnOrderingKey(String) declares its own};</li>
 *   <li>every row header reaches the wire with its value as UTF-8 bytes, unless the encoder
 *       {@linkplain #consumesHeaders(String...) consumes it} as an attribute of its envelope.</li>
 * </ol>
 *
 * <pre>{@code
 * MessageEncoderContract.of(new MyEncoder(), MessageEncoderContract.header("my-id"))
 *         .consumesHeaders("content-type")
 *         .verify();
 * }</pre>
 *
 * <p><b>Framework-neutral</b>: {@link #verify()} throws an {@link AssertionError}, which every test
 * framework reports as a failure, so the kit brings no test framework onto the caller's classpath. It
 * collects every violation before throwing, and each one names the check, the fixture's row id and the
 * offending field names, never a payload or a header value.
 *
 * <p>A transport-bound encoder ({@code KafkaMessageEncoder}, {@code RabbitMessageEncoder}) is run by
 * adapting its output to an {@link EncodedMessage} in the calling test.
 *
 * <p>Not checked, because it cannot be observed from outside: purity beyond determinism, and the
 * permanence of an encoding failure, which is a dispatcher property.
 */
public final class MessageEncoderContract {

    private static final String AGGREGATE_ID = "order-1";
    private static final String AGGREGATE_TYPE = "Order";
    private static final Instant CREATED_AT = Instant.parse("2024-01-01T00:00:00Z");

    private final MessageEncoder encoder;
    private final Function<EncodedMessage, String> eventId;
    private final Set<String> consumedHeaders = new LinkedHashSet<>();
    private String ownOrderingKeyReason;
    private List<OutboxRecord> fixtures = defaultFixtures();

    private MessageEncoderContract(MessageEncoder encoder, Function<EncodedMessage, String> eventId) {
        this.encoder = Objects.requireNonNull(encoder, "encoder");
        this.eventId = Objects.requireNonNull(eventId, "eventId");
    }

    /**
     * Starts a contract run.
     *
     * @param encoder the encoder under test
     * @param eventId reads the event id off the encoder's output; there is no default, because only
     *                the envelope knows where it keeps its id (a header, a body field, a property)
     * @return a run to configure and {@link #verify()}
     * @throws NullPointerException if either argument is {@code null}
     */
    public static MessageEncoderContract of(MessageEncoder encoder, Function<EncodedMessage, String> eventId) {
        return new MessageEncoderContract(encoder, eventId);
    }

    /**
     * Reads the event id from a header, as UTF-8 text; {@code null} when the header is absent.
     *
     * @param name the header carrying the id, e.g. {@code RawHeaders.ID} or {@code CloudEventsHeaders.CE_ID}
     * @return an event id extractor for {@link #of}
     * @throws NullPointerException if {@code name} is {@code null}
     */
    public static Function<EncodedMessage, String> header(String name) {
        Objects.requireNonNull(name, "name");
        return message -> {
            byte[] value = message.headers().get(name);
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        };
    }

    /** Row headers the envelope consumes as attributes of its own; exempt from the passthrough check. */
    public MessageEncoderContract consumesHeaders(String... names) {
        for (String name : names) {
            consumedHeaders.add(Objects.requireNonNull(name, "name"));
        }
        return this;
    }

    /**
     * Declares that the ordering key is deliberately not the aggregate id, and skips the key check.
     * The reason is not checked; it exists so the deviation reads as a sentence in the test source.
     *
     * @throws IllegalArgumentException if {@code reason} is {@code null} or blank
     */
    public MessageEncoderContract declaresOwnOrderingKey(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("declaring an ordering key of one's own needs a reason");
        }
        this.ownOrderingKeyReason = reason;
        return this;
    }

    /**
     * Replaces the built-in fixture rows (a sequenced and typed row, an unsequenced one and an untyped
     * one, all carrying {@code content-type}, {@code dataschema}, trace, correlation and application
     * headers) with the caller's. The passthrough check then covers only the headers these rows carry.
     *
     * @throws IllegalArgumentException if the rows carry fewer than two distinct row ids, which the
     *                                  distinct-id check needs
     */
    public MessageEncoderContract records(List<OutboxRecord> fixtures) {
        List<OutboxRecord> copy = List.copyOf(fixtures);
        if (copy.stream().map(OutboxRecord::id).distinct().count() < 2) {
            throw new IllegalArgumentException("the contract needs at least two rows with distinct ids");
        }
        this.fixtures = copy;
        return this;
    }

    /**
     * Runs every check on every fixture row.
     *
     * @throws AssertionError listing every violation found, if there is any
     */
    public void verify() {
        List<String> violations = new ArrayList<>();
        List<Throwable> causes = new ArrayList<>();
        Map<String, Long> rowByEventId = new HashMap<>();

        for (OutboxRecord record : fixtures) {
            long rowId = record.id();
            EncodedMessage first;
            EncodedMessage second;
            try {
                first = encoder.encode(record);
                second = encoder.encode(record);
            } catch (RuntimeException e) {
                violations.add(violation(1, "the encoder accepts every row", rowId,
                        "encode threw " + e.getClass().getName()));
                causes.add(e);
                continue;
            }
            if (first == null || second == null) {
                violations.add(violation(1, "the encoder accepts every row", rowId, "encode returned null"));
                continue;
            }

            Set<String> differing = differences(first, second);
            if (!differing.isEmpty()) {
                violations.add(violation(2, "two encodes of one row are equal", rowId, "differing:" + differing));
            }

            String id = extractId(first, rowId, violations, causes);
            String again = extractId(second, rowId, violations, causes);
            if (id != null && again != null) {
                if (id.isBlank()) {
                    violations.add(violation(3, "the event id is present and stable", rowId, "event id is blank"));
                } else if (!id.equals(again)) {
                    violations.add(violation(3, "the event id is present and stable", rowId,
                            "event id differs between two encodes"));
                } else {
                    Long clash = rowByEventId.putIfAbsent(id, rowId);
                    if (clash != null && clash != rowId) {
                        violations.add(violation(4, "distinct rows have distinct event ids", rowId,
                                "same event id as rowId:" + clash));
                    }
                }
            }

            if (ownOrderingKeyReason == null && !record.aggregateId().value().equals(first.key())) {
                violations.add(violation(5, "the ordering key is the aggregate id", rowId, "field:key"));
            }

            Set<String> missing = new TreeSet<>();
            Set<String> altered = new TreeSet<>();
            record.headers().forEach((name, value) -> {
                if (consumedHeaders.contains(name)) {
                    return;
                }
                byte[] published = first.headers().get(name);
                if (published == null) {
                    missing.add(name);
                } else if (!Arrays.equals(published, value.getBytes(StandardCharsets.UTF_8))) {
                    altered.add(name);
                }
            });
            if (!missing.isEmpty() || !altered.isEmpty()) {
                violations.add(violation(6, "row headers reach the wire", rowId,
                        "missing:" + missing + ", altered:" + altered));
            }
        }

        if (!violations.isEmpty()) {
            AssertionError failure = new AssertionError("The encoder breaks the message encoder contract, violations:"
                    + violations.size() + System.lineSeparator() + String.join(System.lineSeparator(), violations));
            causes.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    private String extractId(EncodedMessage message, long rowId, List<String> violations, List<Throwable> causes) {
        String id;
        try {
            id = eventId.apply(message);
        } catch (RuntimeException e) {
            violations.add(violation(3, "the event id is present and stable", rowId,
                    "event id extractor threw " + e.getClass().getName()));
            causes.add(e);
            return null;
        }
        if (id == null) {
            violations.add(violation(3, "the event id is present and stable", rowId, "event id is missing"));
        }
        return id;
    }

    /** Names of the fields that differ between two encodes of one row; header values compared by content. */
    private static Set<String> differences(EncodedMessage first, EncodedMessage second) {
        Set<String> differing = new TreeSet<>();
        if (!first.destination().equals(second.destination())) {
            differing.add("destination");
        }
        if (!Objects.equals(first.key(), second.key())) {
            differing.add("key");
        }
        if (!Arrays.equals(first.body(), second.body())) {
            differing.add("body");
        }
        Set<String> names = new TreeSet<>(first.headers().keySet());
        names.addAll(second.headers().keySet());
        for (String name : names) {
            if (!Arrays.equals(first.headers().get(name), second.headers().get(name))) {
                differing.add("header:" + name);
            }
        }
        return differing;
    }

    private static String violation(int check, String name, long rowId, String detail) {
        return "Check " + check + " (" + name + ") failed, rowId:" + rowId + ", " + detail;
    }

    /**
     * The row shapes the write side accepts, built the way the store builds a row: the content type is
     * set both as the typed field and as the {@code content-type} header.
     */
    private static List<OutboxRecord> defaultFixtures() {
        return List.of(
                fixture(1, base().type("com.acme.order.placed").seq(1)),
                fixture(2, base().type("com.acme.order.placed").unsequenced()),
                fixture(3, base().seq(3)));
    }

    private static OutboxMessage.Builder base() {
        return OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID)
                .aggregateType(AGGREGATE_TYPE)
                .payload("{\"total\":10}".getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .header(TandemHeaders.CONTENT_TYPE, "application/json")
                .header(TandemHeaders.DATA_SCHEMA, "https://schemas.example.com/order")
                .header(TandemHeaders.TRACEPARENT, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .header(TandemHeaders.TRACESTATE, "vendor=value")
                .header(TandemHeaders.CORRELATION_ID, "corr-1")
                .header("x-tenant", "acme");
    }

    private static OutboxRecord fixture(long id, OutboxMessage.Builder message) {
        return OutboxRecord.builder().id(id).message(message.build()).createdAt(CREATED_AT).build();
    }
}
