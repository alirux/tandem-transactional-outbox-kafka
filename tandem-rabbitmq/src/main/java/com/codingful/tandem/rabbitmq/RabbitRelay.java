package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP publish adapter (LLD-rabbitmq §2): implements {@link OutboxDispatcher} by handing an
 * {@link OutboxRecord} to a {@link RabbitMessageEncoder} ({@link CloudEventAmqpEncoder} unless the
 * caller supplies another) and publishing the result on one channel in confirm mode. The returned
 * future completes on the broker's publisher confirm, or completes <b>exceptionally</b> with an
 * {@code OutboxDispatchException} carrying the retriable/permanent verdict (§4) — never blocking, so
 * the relay overlaps many records of distinct aggregates on a single channel.
 *
 * <p>Per-aggregate ordering is enforced upstream, by the relay keeping one head row per aggregate in
 * flight. What this adapter cannot give, and Kafka can, is <b>consumer-observed</b> order under
 * competing consumers: AMQP has no partitions. See §7 for the two supported arrangements.
 */
public final class RabbitRelay implements OutboxDispatcher, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RabbitRelay.class);

    /** AMQP delivery mode 2: the broker persists the message, so a confirm survives a broker restart (§1). */
    private static final int PERSISTENT = 2;

    private final PublishChannel channel;
    private final RabbitMessageEncoder encoder;
    private final RabbitErrorClassifier classifier;
    private final long confirmTimeoutMs;
    private final TandemSpanRecorder spanRecorder;
    private final ScheduledExecutorService timeouts;

    /** Guards the publish critical section only: a Channel is not thread-safe and the tag must match the publish (§2). */
    private final Object publishLock = new Object();

    private final ConcurrentNavigableMap<Long, Pending> pending = new ConcurrentSkipListMap<>();
    private final Map<String, Long> tagsByMessageId = new ConcurrentHashMap<>();

    /**
     * Opens a connection and a confirm-mode channel, verifies the exchange exists, and publishes
     * Tandem's default CloudEvents envelope routed by {@link RabbitRouter#kebabWithSuffix}. Delegates
     * to the 3-arg constructor with {@link TandemSpanRecorder#NOOP}, that is instrumented mode
     * disabled (HLD-tracing §7).
     *
     * @param factory the AMQP connection settings; automatic recovery is switched on
     * @param cfg     the exchange, the routing-key suffix, the confirm timeout and the CloudEvents settings
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if the broker is
     *         unreachable or the exchange does not exist
     */
    public RabbitRelay(ConnectionFactory factory, RabbitRelayConfig cfg) {
        this(factory, cfg, TandemSpanRecorder.NOOP);
    }

    /**
     * @param factory      the AMQP connection settings; automatic recovery is switched on
     * @param cfg          the exchange, the routing-key suffix, the confirm timeout and the CloudEvents settings
     * @param spanRecorder emits the {@code tandem.relay.publish} span per record when
     *                     {@link TandemSpanRecorder#isEnabled()} (HLD-tracing §6)
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if the broker is
     *         unreachable or the exchange does not exist
     * @throws NullPointerException if any argument is {@code null}
     */
    public RabbitRelay(ConnectionFactory factory, RabbitRelayConfig cfg, TandemSpanRecorder spanRecorder) {
        this(factory, cfg,
                new CloudEventAmqpEncoder(RabbitRouter.kebabWithSuffix(cfg.exchange(), cfg.routingKeySuffix()), cfg),
                spanRecorder);
    }

    /**
     * Publishes in a format of the caller's choosing rather than Tandem's default CloudEvents envelope
     * (§5): routing and the CloudEvents settings are the encoder's concern. A transport-neutral
     * {@link com.codingful.tandem.core.port.MessageEncoder} reaches here through
     * {@link RabbitMessageEncoder#from}.
     *
     * @param factory      the AMQP connection settings; automatic recovery is switched on
     * @param cfg          read here only for the exchange to verify and the confirm timeout to enforce
     * @param encoder      turns each record into the message to publish
     * @param spanRecorder emits the {@code tandem.relay.publish} span per record when enabled
     * @throws com.codingful.tandem.core.exception.TandemConfigurationException if the broker is
     *         unreachable or the exchange does not exist
     * @throws NullPointerException if any argument is {@code null}
     */
    public RabbitRelay(ConnectionFactory factory, RabbitRelayConfig cfg, RabbitMessageEncoder encoder,
            TandemSpanRecorder spanRecorder) {
        this(AmqpPublishChannel.open(Objects.requireNonNull(factory, "factory"),
                        Objects.requireNonNull(cfg, "cfg").exchange()),
                encoder, new DefaultRabbitErrorClassifier(), cfg.confirmTimeout().toMillis(), spanRecorder,
                defaultTimeoutScheduler());
    }

    /** For tests: inject the channel, the classifier and the timeout scheduler directly. */
    RabbitRelay(PublishChannel channel, RabbitMessageEncoder encoder, RabbitErrorClassifier classifier,
            long confirmTimeoutMs, TandemSpanRecorder spanRecorder, ScheduledExecutorService timeouts) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.encoder = Objects.requireNonNull(encoder, "encoder");
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        this.confirmTimeoutMs = confirmTimeoutMs;
        this.spanRecorder = Objects.requireNonNull(spanRecorder, "spanRecorder");
        this.timeouts = Objects.requireNonNull(timeouts, "timeouts");
        channel.onConfirm(this::onConfirm);
        channel.onReturn(this::onReturn);
        channel.onShutdown(this::onShutdown);
    }

    private static ScheduledExecutorService defaultTimeoutScheduler() {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tandem-rabbit-confirm-timeout");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public CompletableFuture<Void> dispatch(OutboxRecord record) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        AmqpMessage message;
        try {
            message = encoder.encode(record);
        } catch (RuntimeException encodeFailure) {
            // Permanent by construction, and deliberately NOT routed through the classifier: re-encoding
            // the same row always fails the same way, so a retry would only keep the aggregate's chain
            // blocked for the whole backoff ladder (§4).
            LOG.error("Encoding outbox row failed rowId:{}, aggregateType:{}, aggregateId:{}", record.id(),
                    record.aggregateType(), record.aggregateId().value(), encodeFailure);
            ack.completeExceptionally(permanent(encodeFailure));
            return ack;
        }

        AmqpRoute route = message.route();
        String messageId = String.valueOf(record.id());
        AMQP.BasicProperties properties = message.properties().builder()
                .messageId(messageId)          // reserved: correlates an unroutable return back to this row (§2)
                .deliveryMode(PERSISTENT)      // reserved: the persistence mark-DONE-after-ack rests on (§1)
                .build();

        TandemSpanRecorder.Span span = spanRecorder.isEnabled()
                ? spanRecorder.startPublishSpan(record.id(), record.aggregateType(), record.aggregateId().value(),
                        record.attempts(), route.routingKey(),
                        record.headers().get(TandemHeaders.TRACEPARENT), record.headers().get(TandemHeaders.TRACESTATE),
                        record.headers().get(TandemHeaders.CORRELATION_ID))
                : TandemSpanRecorder.Span.NOOP;
        Pending entry = new Pending(record, route, messageId, ack, span);

        synchronized (publishLock) {
            long tag = channel.nextPublishSeqNo();
            pending.put(tag, entry);
            tagsByMessageId.put(messageId, tag);
            entry.timeout = timeouts.schedule(() -> expire(tag), confirmTimeoutMs, TimeUnit.MILLISECONDS);
            try {
                channel.publish(route, properties, message.body());
            } catch (IOException | RuntimeException publishFailure) {
                // The client increments the delivery tag inside the publish, so a throw leaves the tag
                // unconsumed and the next publish would reuse it: this entry has to go.
                discard(tag);
                LOG.error("Publishing outbox row failed rowId:{}, exchange:{}, routingKey:{}", record.id(),
                        route.exchange(), route.routingKey(), publishFailure);
                span.end(publishFailure);
                ack.completeExceptionally(classifier.classify(publishFailure));
            }
        }
        return ack;
    }

    /**
     * A confirm may carry {@code multiple}, acknowledging every tag up to and including its own, so it
     * resolves a range of the pending map rather than one entry.
     */
    private void onConfirm(long deliveryTag, boolean multiple, boolean acknowledged) {
        if (!multiple) {
            settle(deliveryTag, acknowledged);
            return;
        }
        NavigableMap<Long, Pending> resolved = pending.headMap(deliveryTag, true);
        for (Long tag : List.copyOf(resolved.keySet())) {
            settle(tag, acknowledged);
        }
    }

    private void settle(long tag, boolean acknowledged) {
        Pending entry = discard(tag);
        if (entry == null) {
            return;   // already settled by a timeout, a shutdown, or an earlier multiple-ack
        }
        String returnReason = entry.returnReason;
        if (returnReason != null) {
            LOG.error("Publishing outbox row failed, no binding matched rowId:{}, exchange:{}, routingKey:{}, broker:{}",
                    entry.record.id(), entry.route.exchange(), entry.route.routingKey(), returnReason);
            fail(entry, new OutboxDispatchException("AMQP publish failed (permanent): the broker returned the"
                    + " message as unroutable - " + returnReason, false));
        } else if (acknowledged) {
            entry.span.end();
            entry.ack.complete(null);
        } else {
            LOG.error("Publishing outbox row failed, broker sent a nack rowId:{}, exchange:{}, routingKey:{}",
                    entry.record.id(), entry.route.exchange(), entry.route.routingKey());
            fail(entry, new OutboxDispatchException("AMQP publish failed (retriable): the broker nacked the"
                    + " message", true));
        }
    }

    /**
     * The return arrives before the confirm for the same message, so it only marks the entry; the
     * confirm that follows reads the mark and fails the row permanently (§2).
     */
    private void onReturn(String messageId, int replyCode, String replyText) {
        Long tag = messageId == null ? null : tagsByMessageId.get(messageId);
        Pending entry = tag == null ? null : pending.get(tag);
        if (entry == null) {
            LOG.warn("Received an AMQP return for an unknown message messageId:{}, replyCode:{}", messageId, replyCode);
            return;
        }
        entry.returnReason = replyCode + " " + replyText;
    }

    private void onShutdown(Throwable cause) {
        if (pending.isEmpty()) {
            return;
        }
        LOG.warn("AMQP channel shut down, failing pending confirms pendingCount:{}", pending.size(), cause);
        failAllPending(cause);
    }

    private void expire(long tag) {
        Pending entry = discard(tag);
        if (entry == null) {
            return;
        }
        LOG.error("Publisher confirm timed out rowId:{}, exchange:{}, routingKey:{}, confirmTimeoutMs:{}",
                entry.record.id(), entry.route.exchange(), entry.route.routingKey(), confirmTimeoutMs);
        fail(entry, new OutboxDispatchException("AMQP publish failed (retriable): no publisher confirm within "
                + confirmTimeoutMs + " ms", true));
    }

    private void failAllPending(Throwable cause) {
        for (Long tag : List.copyOf(pending.keySet())) {
            Pending entry = discard(tag);
            if (entry != null) {
                fail(entry, classifier.classify(cause));
            }
        }
    }

    /** Removes the entry from both indexes and cancels its timeout; {@code null} when already settled. */
    private Pending discard(long tag) {
        Pending entry = pending.remove(tag);
        if (entry == null) {
            return null;
        }
        tagsByMessageId.remove(entry.messageId, tag);
        if (entry.timeout != null) {
            entry.timeout.cancel(false);
        }
        return entry;
    }

    private static void fail(Pending entry, OutboxDispatchException failure) {
        entry.span.end(failure);
        entry.ack.completeExceptionally(failure);
    }

    private static OutboxDispatchException permanent(RuntimeException encodeFailure) {
        String detail = encodeFailure.getMessage() == null ? "" : " - " + encodeFailure.getMessage();
        return new OutboxDispatchException("Encoding the outbox row failed (permanent): "
                + encodeFailure.getClass().getSimpleName() + detail, false, encodeFailure);
    }

    /**
     * How many publishes this adapter is still waiting for the broker to settle.
     *
     * <p>Every one of them holds an entry in a map of this adapter's own until a confirm, a return, a
     * timeout or a channel shutdown resolves it, so the count is also where a resolution path that
     * forgot to release its entry would show: over a long run it would climb and never come back down.
     * An idle adapter reports zero.
     *
     * @return the number of unsettled publishes, never negative
     */
    public int inFlightConfirms() {
        return pending.size();
    }

    /** For tests: the second index, which every resolution path must release together with the first. */
    int trackedMessageIds() {
        return tagsByMessageId.size();
    }

    /**
     * Reports the confirm timeout to the relay so the {@code rowLease > deliveryTimeout} invariant is
     * validated against the deadline this adapter really enforces (§3, LLD-jdbc §3.5).
     */
    @Override
    public OptionalLong deliveryTimeoutMillis() {
        return OptionalLong.of(confirmTimeoutMs);
    }

    @Override
    public void close() {
        timeouts.shutdownNow();
        try {
            channel.close();
        } catch (IOException closeFailure) {
            LOG.warn("Closing the AMQP publish channel failed", closeFailure);
        }
        failAllPending(new IOException("The AMQP publish channel was closed"));
    }

    /** One in-flight publish, from the moment it is written until its confirm, timeout or shutdown. */
    private static final class Pending {

        private final OutboxRecord record;
        private final AmqpRoute route;
        private final String messageId;
        private final CompletableFuture<Void> ack;
        private final TandemSpanRecorder.Span span;

        /** Set by the return listener, read by the confirm that follows it; different threads. */
        private volatile String returnReason;
        private volatile ScheduledFuture<?> timeout;

        private Pending(OutboxRecord record, AmqpRoute route, String messageId, CompletableFuture<Void> ack,
                TandemSpanRecorder.Span span) {
            this.record = record;
            this.route = route;
            this.messageId = messageId;
            this.ack = ack;
            this.span = span;
        }
    }
}
