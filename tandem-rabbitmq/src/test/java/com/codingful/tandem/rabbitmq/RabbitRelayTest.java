package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RabbitRelayTest {

    private static final String EXCHANGE = "tandem-events";
    private static final long CONFIRM_TIMEOUT_MS = 30_000;

    private final RecordingPublishChannel channel = new RecordingPublishChannel();
    private final ScheduledExecutorService timeouts = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void stopScheduler() {
        timeouts.shutdownNow();
    }

    private static OutboxRecord record(long id, String aggregateId) {
        return OutboxRecord.builder()
                .id(id)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").seq(1).payload("{}".getBytes()).build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();
    }

    private RabbitRelay relay() {
        return relayWithConfirmTimeout(CONFIRM_TIMEOUT_MS);
    }

    private RabbitRelay relayWithConfirmTimeout(long confirmTimeoutMs) {
        RabbitRelayConfig cfg = RabbitRelayConfig.of("/tandem/orders", EXCHANGE);
        return new RabbitRelay(channel,
                new CloudEventAmqpEncoder(RabbitRouter.kebabWithSuffix(EXCHANGE, "-topic"), cfg),
                new DefaultRabbitErrorClassifier(), confirmTimeoutMs, TandemSpanRecorder.NOOP, timeouts);
    }

    private RabbitRelay relayWith(TandemSpanRecorder spanRecorder) {
        RabbitRelayConfig cfg = RabbitRelayConfig.of("/tandem/orders", EXCHANGE);
        return new RabbitRelay(channel,
                new CloudEventAmqpEncoder(RabbitRouter.kebabWithSuffix(EXCHANGE, "-topic"), cfg),
                new DefaultRabbitErrorClassifier(), CONFIRM_TIMEOUT_MS, spanRecorder, timeouts);
    }

    private static OutboxDispatchException failureOf(CompletableFuture<Void> ack) {
        assertThatThrownBy(() -> ack.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(OutboxDispatchException.class);
        return (OutboxDispatchException) ack.handle((ignored, error) -> error).join();
    }

    @Test
    void GIVEN_a_pending_row_WHEN_the_broker_confirms_it_THEN_the_dispatch_succeeds() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));
        assertThat(ack).isNotCompleted();

        channel.confirm(channel.last().deliveryTag());

        assertThat(ack).isCompletedWithValue(null);
        assertThat(channel.last().route()).isEqualTo(new AmqpRoute(EXCHANGE, "order-topic"));
    }

    @Test
    void GIVEN_a_pending_row_WHEN_the_broker_refuses_responsibility_for_it_THEN_the_row_is_retried() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));
        channel.nack(channel.last().deliveryTag());

        assertThat(failureOf(ack).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_row_no_binding_matches_WHEN_the_broker_sends_it_back_THEN_the_row_fails_permanently() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> ack = relay.dispatch(record(42, "order-1"));
        // The broker returns the message first and confirms it afterwards; only the confirm settles it.
        channel.returnUnroutable("42", 312, "NO_ROUTE");
        assertThat(ack).isNotCompleted();
        channel.confirm(channel.last().deliveryTag());

        OutboxDispatchException failure = failureOf(ack);
        assertThat(failure.isRetriable()).isFalse();
        assertThat(failure).hasMessageContaining("NO_ROUTE");
    }

    @Test
    void GIVEN_a_row_that_cannot_be_encoded_WHEN_it_is_dispatched_THEN_it_fails_permanently_without_reaching_the_broker() {
        RabbitMessageEncoder broken = record -> {
            throw new IllegalStateException("no encoding for this row");
        };
        RabbitRelay relay = new RabbitRelay(channel, broken, new DefaultRabbitErrorClassifier(),
                CONFIRM_TIMEOUT_MS, TandemSpanRecorder.NOOP, timeouts);

        OutboxDispatchException failure = failureOf(relay.dispatch(record(1, "order-1")));

        assertThat(failure.isRetriable()).isFalse();
        assertThat(channel.published()).isEmpty();
    }

    @Test
    void GIVEN_several_aggregates_in_flight_WHEN_one_confirm_covers_them_all_THEN_every_row_succeeds() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> first = relay.dispatch(record(1, "order-1"));
        CompletableFuture<Void> second = relay.dispatch(record(2, "order-2"));
        CompletableFuture<Void> third = relay.dispatch(record(3, "order-3"));

        channel.confirmUpTo(channel.last().deliveryTag());

        assertThat(first).isCompletedWithValue(null);
        assertThat(second).isCompletedWithValue(null);
        assertThat(third).isCompletedWithValue(null);
    }

    @Test
    void GIVEN_a_confirm_that_never_arrives_WHEN_the_deadline_passes_THEN_the_row_is_retried_rather_than_held_forever() {
        RabbitRelay relay = relayWithConfirmTimeout(50);

        OutboxDispatchException failure = failureOf(relay.dispatch(record(1, "order-1")));

        assertThat(failure.isRetriable()).isTrue();
        assertThat(failure).hasMessageContaining("50 ms");
    }

    @Test
    void GIVEN_rows_in_flight_WHEN_the_connection_drops_THEN_they_are_retried_rather_than_left_pending() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> first = relay.dispatch(record(1, "order-1"));
        CompletableFuture<Void> second = relay.dispatch(record(2, "order-2"));

        channel.shutdown(new IOException("connection reset"));

        assertThat(failureOf(first).isRetriable()).isTrue();
        assertThat(failureOf(second).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_broker_that_rejects_the_write_WHEN_a_row_is_dispatched_THEN_the_row_is_retried_and_its_slot_is_released() {
        RabbitRelay relay = relay();
        channel.failNextPublishWith(new IOException("channel is closed"));

        assertThat(failureOf(relay.dispatch(record(1, "order-1"))).isRetriable()).isTrue();

        // The failed publish consumed no delivery tag, so the next row takes the one it left behind and
        // its confirm still resolves the right record.
        CompletableFuture<Void> next = relay.dispatch(record(2, "order-2"));
        channel.confirm(channel.last().deliveryTag());
        assertThat(next).isCompletedWithValue(null);
    }

    @Test
    void GIVEN_any_row_WHEN_it_is_published_THEN_it_is_persistent_and_carries_its_outbox_id_for_return_correlation() {
        RabbitRelay relay = relay();

        relay.dispatch(record(77, "order-1"));

        assertThat(channel.last().properties().getDeliveryMode()).isEqualTo(2);
        assertThat(channel.last().properties().getMessageId()).isEqualTo("77");
    }

    @Test
    void GIVEN_a_relay_WHEN_the_engine_validates_the_row_lease_THEN_it_reads_the_deadline_this_adapter_enforces() {
        assertThat(relayWithConfirmTimeout(12_345).deliveryTimeoutMillis()).hasValue(12_345);
    }

    @Test
    void GIVEN_rows_in_flight_WHEN_the_relay_shuts_down_THEN_the_channel_closes_and_the_rows_are_retried() {
        RabbitRelay relay = relay();
        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));

        relay.close();

        assertThat(channel.isClosed()).isTrue();
        assertThat(failureOf(ack).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_instrumented_mode_WHEN_a_row_is_published_THEN_the_span_names_the_row_and_where_it_went() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        RabbitRelay relay = relayWith(spanRecorder);

        CompletableFuture<Void> ack = relay.dispatch(record(5, "order-1"));
        channel.confirm(channel.last().deliveryTag());

        assertThat(ack).isCompletedWithValue(null);
        assertThat(spanRecorder.started).singleElement().satisfies(started -> {
            assertThat(started.rowId()).isEqualTo(5);
            assertThat(started.destination()).isEqualTo(channel.last().route().routingKey());
        });
        assertThat(spanRecorder.spans).singleElement().satisfies(span -> {
            assertThat(span.succeeded).isTrue();
            assertThat(span.failure).isNull();
        });
    }

    @Test
    void GIVEN_instrumented_mode_WHEN_the_broker_rejects_a_row_THEN_the_span_carries_the_failure() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        RabbitRelay relay = relayWith(spanRecorder);

        relay.dispatch(record(6, "order-1"));
        channel.nack(channel.last().deliveryTag());

        assertThat(spanRecorder.spans).singleElement()
                .satisfies(span -> assertThat(span.failure).isInstanceOf(OutboxDispatchException.class));
    }

    @Test
    void GIVEN_a_row_already_failed_by_its_deadline_WHEN_the_broker_confirms_it_late_THEN_the_outcome_does_not_change() {
        RabbitRelay relay = relayWithConfirmTimeout(50);

        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));
        assertThat(failureOf(ack).isRetriable()).isTrue();

        // The confirm arrives after the row was already handed back for retry: it must not complete the
        // same future a second time, nor resolve a record that is no longer in flight.
        channel.confirm(channel.last().deliveryTag());

        assertThat(ack).isCompletedExceptionally();
        assertThat(failureOf(ack)).hasMessageContaining("no publisher confirm");
    }

    @Test
    void GIVEN_rows_waiting_on_the_broker_WHEN_the_relay_is_asked_what_is_unsettled_THEN_it_counts_them() {
        RabbitRelay relay = relay();

        relay.dispatch(record(1, "order-1"));
        relay.dispatch(record(2, "order-2"));
        assertThat(relay.inFlightConfirms()).isEqualTo(2);

        channel.confirm(channel.last().deliveryTag());
        assertThat(relay.inFlightConfirms()).isEqualTo(1);
    }

    @Test
    void GIVEN_rows_settled_by_every_route_there_is_WHEN_the_dust_settles_THEN_the_relay_is_tracking_nothing() {
        // The point is not that each row completed (the tests above pin that), but that nothing is
        // left behind it. Each way out holds an entry in two indexes, and a route that released only
        // one of them would look perfectly healthy for the length of any test but this one: the count
        // would climb for the life of the process, which on a long run is the whole failure.
        RabbitRelay relay = relay();

        CompletableFuture<Void> confirmed = relay.dispatch(record(1, "order-1"));
        channel.confirm(channel.last().deliveryTag());
        CompletableFuture<Void> nacked = relay.dispatch(record(2, "order-2"));
        channel.nack(channel.last().deliveryTag());
        CompletableFuture<Void> returned = relay.dispatch(record(3, "order-3"));
        channel.returnUnroutable(channel.last().properties().getMessageId(), 312, "NO_ROUTE");
        channel.confirm(channel.last().deliveryTag());
        CompletableFuture<Void> refused = relay.dispatch(record(4, "order-4"));
        channel.shutdown(new IOException("connection reset"));

        assertThat(confirmed).isCompletedWithValue(null);
        assertThat(failureOf(nacked).isRetriable()).isTrue();
        assertThat(failureOf(returned).isRetriable()).isFalse();
        assertThat(failureOf(refused).isRetriable()).isTrue();
        assertThat(relay.inFlightConfirms()).isZero();
        assertThat(relay.trackedMessageIds()).isZero();
    }

    @Test
    void GIVEN_a_row_the_broker_never_answered_for_WHEN_its_deadline_passes_THEN_nothing_is_left_tracking_it() {
        RabbitRelay relay = relayWithConfirmTimeout(50);

        failureOf(relay.dispatch(record(1, "order-1")));

        assertThat(relay.inFlightConfirms()).isZero();
        assertThat(relay.trackedMessageIds()).isZero();
    }

    @Test
    void GIVEN_a_return_for_a_row_that_is_no_longer_in_flight_WHEN_it_arrives_THEN_it_is_ignored() {
        RabbitRelay relay = relay();

        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));
        channel.confirm(channel.last().deliveryTag());

        channel.returnUnroutable("1", 312, "NO_ROUTE");
        channel.returnUnroutable(null, 312, "NO_ROUTE");

        assertThat(ack).isCompletedWithValue(null);
    }

    @Test
    void GIVEN_a_channel_that_fails_to_close_WHEN_the_relay_shuts_down_THEN_the_rows_in_flight_are_still_retried() {
        RabbitRelay relay = relay();
        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));
        channel.failCloseWith(new IOException("broker already gone"));

        relay.close();

        assertThat(failureOf(ack).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_closed_relay_WHEN_a_row_is_dispatched_THEN_it_fails_retriably_and_nothing_stays_in_flight() {
        RecordingSpanRecorder spanRecorder = new RecordingSpanRecorder();
        RabbitRelay relay = relayWith(spanRecorder);
        relay.close();

        CompletableFuture<Void> ack = relay.dispatch(record(1, "order-1"));

        assertThat(failureOf(ack).isRetriable()).isTrue();
        assertThat(channel.published()).isEmpty();
        assertThat(relay.inFlightConfirms()).isZero();
        assertThat(relay.trackedMessageIds()).isZero();
        assertThat(spanRecorder.spans).singleElement()
                .satisfies(span -> assertThat(span.failure).isInstanceOf(OutboxDispatchException.class));
    }

    /** A real, non-NOOP {@link TandemSpanRecorder} standing in for tandem-spring-relay/tandem-tracing-otel. */
    private static final class RecordingSpanRecorder implements TandemSpanRecorder {

        record StartedSpan(long rowId, String aggregateType, String aggregateId, int attempts,
                String destination, String traceparent, String tracestate, String correlationId) {
        }

        final List<StartedSpan> started = new ArrayList<>();
        final List<RecordingSpan> spans = new ArrayList<>();

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public Span startPublishSpan(long rowId, String aggregateType, String aggregateId, int attempts,
                String destination, String traceparent, String tracestate, String correlationId) {
            started.add(new StartedSpan(rowId, aggregateType, aggregateId, attempts, destination, traceparent,
                    tracestate, correlationId));
            RecordingSpan span = new RecordingSpan();
            spans.add(span);
            return span;
        }

        static final class RecordingSpan implements Span {
            boolean succeeded;
            Throwable failure;

            @Override
            public void end() {
                succeeded = true;
            }

            @Override
            public void end(Throwable thrown) {
                failure = thrown;
            }
        }
    }
}
