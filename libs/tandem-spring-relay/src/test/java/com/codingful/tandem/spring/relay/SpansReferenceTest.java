package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.test.reference.ReferenceFile;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Pins the spans the Micrometer Tracing recorder emits (LLD-api-stability §8.4): their name, kind and
 * attribute keys, which an operator's saved queries in the tracing backend are written against. The
 * recorder parents each span through the application's {@code Propagator}, so it runs on Micrometer
 * Tracing's OpenTelemetry bridge and exports into OpenTelemetry's in-memory exporter. One span carries
 * every optional attribute, the other none; both belong to a traced row, since a row without a trace
 * parent gets no span at all.
 */
class SpansReferenceTest {

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();

    @Test
    void GIVEN_a_traced_row_with_and_without_its_optional_fields_WHEN_published_THEN_the_spans_match_their_reference_file() {
        TandemSpanRecorder recorder = new MicrometerTandemSpanRecorder(new OtelPropagator(
                ContextPropagators.create(W3CTraceContextPropagator.getInstance()), tracerProvider.get("tandem-relay")));

        recorder.startPublishSpan(42L, "Order", "order-1", 2, "order-topic", TRACEPARENT, "rojo=00f067aa0ba902b7",
                "checkout-42").end();
        recorder.startPublishSpan(43L, "Order", "order-2", 0, "order-topic", TRACEPARENT, null, null).end();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        assertThat(spans).hasSize(2);
        ReferenceFile.of("spans.txt", SpansReferenceTest.class).assertMatches(List.of(
                spanLine("complete", spans.get(0)),
                spanLine("minimal", spans.get(1))));
    }

    private static String spanLine(String name, SpanData span) {
        String attributeKeys = span.getAttributes().asMap().keySet().stream()
                .map(AttributeKey::getKey)
                .sorted()
                .collect(Collectors.joining(", ", "[", "]"));
        return "[" + name + "] " + span.getName() + " " + span.getKind().name().toLowerCase(Locale.ROOT) + " "
                + attributeKeys;
    }
}
