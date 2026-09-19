package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.MessageEncoder;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.kafka.CloudEventEncoder;
import com.codingful.tandem.kafka.KafkaMessageEncoder;
import com.codingful.tandem.kafka.KafkaRelay;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Contributes the Kafka publish adapter to {@link TandemRelayAutoConfiguration}'s engine: the topic
 * router, the message encoder and the {@link OutboxDispatcher} itself, bound to the
 * {@code tandem.kafka.*} properties (LLD-spring-config §4.4).
 *
 * <p><b>A separate {@code @AutoConfiguration} class, gated at class level</b>, because
 * {@code tandem-kafka} is an optional dependency of this module rather than a redistributed one: an
 * application relaying to another broker must not inherit the Kafka client (HLD §1.3). The gate has to
 * be on the class and not on the {@code @Bean} methods, because the Kafka types appear in those
 * methods' signatures and Spring resolves a signature while introspecting the configuration class,
 * before any method-level condition is evaluated. A gated class is never loaded at all
 * (LLD-spring-config §1.1 rule 2). {@link TandemMicrometerAutoConfiguration} is wired the same way, for
 * the same reason.
 *
 * <p>Ordered {@code before} the relay autoconfiguration: Spring only guarantees
 * {@link ConditionalOnMissingBean} ordering across explicitly ordered autoconfiguration classes, so
 * this is what lets that class's own "nothing to publish with" diagnostic back off once a dispatcher
 * has been contributed here.
 *
 * <p>The whole class backs off when the application contributes an {@link OutboxDispatcher} of its own:
 * publishing through another transport adapter is then the application's choice, and building a Kafka
 * producer beside it would demand {@code tandem.kafka.source} from an application that publishes no
 * CloudEvents envelope over Kafka at all. The condition is on the class rather than on each bean
 * because Spring guarantees no ordering between the {@code @Bean} methods of one class, and the
 * dispatcher this class itself contributes would race the conditions of the other two.
 */
@AutoConfiguration(before = TandemRelayAutoConfiguration.class)
@ConditionalOnClass({KafkaProducer.class, KafkaRelay.class})
@ConditionalOnMissingBean(OutboxDispatcher.class)
// Gated on exactly what the relay it feeds is gated on: without a relay there is nothing to publish
// with, and building a Kafka producer anyway would fail an application that only loads the module.
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(prefix = "tandem.relay", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(TandemKafkaProperties.class)
public class TandemKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    TopicRouter tandemTopicRouter(TandemKafkaProperties kafka) {
        return TopicRouter.kebabWithSuffix(kafka.topicSuffix());
    }

    /**
     * The published wire format (LLD-kafka §3), Tandem's CloudEvents binary binding unless the
     * application contributes an encoder of its own, which it does in one of two ways.
     *
     * <p>A {@link MessageEncoder} bean is the portable one and the one to reach for: the application
     * writes its envelope once against the core port, names no broker type, and the same class serves
     * whatever transport adapter it is deployed against. It is lifted onto Kafka here.
     *
     * <p>A {@link KafkaMessageEncoder} bean is for a format that genuinely needs Kafka in its
     * signature, as Tandem's own CloudEvents binding does, and takes precedence over both.
     *
     */
    @Bean
    @ConditionalOnMissingBean
    KafkaMessageEncoder tandemMessageEncoder(TandemKafkaProperties kafka, TopicRouter topicRouter,
            ObjectProvider<MessageEncoder> portableEncoder) {
        MessageEncoder portable = portableEncoder.getIfAvailable();
        if (portable != null) {
            return KafkaMessageEncoder.from(portable);
        }
        // tandem.kafka.source is the one key with no default. Checked here so the failure names the
        // property the operator has to set, instead of surfacing as a NullPointerException from the
        // CloudEvents config, several frames away from the configuration that caused it. Reached only
        // on the CloudEvents default: an application publishing its own envelope carries no source
        // attribute, so it is not asked for one.
        if (kafka.source() == null) {
            throw new TandemConfigurationException("Missing required configuration: `tandem.kafka.source`"
                    + " — the CloudEvents source URI identifying this application (e.g. /orders/service)."
                    + " Set it in your application configuration (LLD-spring-config §2.3).");
        }
        return new CloudEventEncoder(topicRouter,
                new KafkaRelayConfig(kafka.source(), kafka.defaultContentType(), kafka.defaultDataSchema()));
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxDispatcher tandemOutboxDispatcher(TandemKafkaProperties kafka, KafkaMessageEncoder encoder,
            ObjectProvider<TandemSpanRecorder> spanRecorder) {
        Map<String, Object> producerConfig = new HashMap<>(kafka.producer());
        return new KafkaRelay(producerConfig, encoder,
                spanRecorder.getIfAvailable(() -> TandemSpanRecorder.NOOP));
    }
}
