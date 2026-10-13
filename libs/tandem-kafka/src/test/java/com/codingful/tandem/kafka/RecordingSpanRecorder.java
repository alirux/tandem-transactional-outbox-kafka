package com.codingful.tandem.kafka;

import com.codingful.tandem.core.port.TandemSpanRecorder;
import java.util.ArrayList;
import java.util.List;

/** A real, non-NOOP {@link TandemSpanRecorder} standing in for tandem-spring-relay/tandem-tracing-otel. */
final class RecordingSpanRecorder implements TandemSpanRecorder {

    record StartedSpan(long rowId, String aggregateType, String aggregateId, int attempts,
            String topic, String traceparent, String tracestate, String correlationId) {
    }

    boolean enabled = true;
    final List<StartedSpan> started = new ArrayList<>();
    final List<RecordingSpan> spans = new ArrayList<>();

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Span startPublishSpan(long rowId, String aggregateType, String aggregateId, int attempts,
            String topic, String traceparent, String tracestate, String correlationId) {
        started.add(new StartedSpan(rowId, aggregateType, aggregateId, attempts, topic, traceparent, tracestate,
                correlationId));
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
        public void end(Throwable f) {
            failure = f;
        }
    }
}
