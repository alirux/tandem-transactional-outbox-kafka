package com.codingful.tandem.relay;

import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WorkerPool;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports whether this process's relay is delivering (LLD-relay §5.1). It only fetches the reading:
 * the judgement is {@link RelayVerdict}'s. Both reads are in-memory by contract, {@link WorkerPool#status()}
 * and {@link RelayControlSource#wholeRelayPaused()}, so the indicator is safe at probe frequency.
 *
 * <p><b>No bean condition, on purpose.</b> The relay is looked up, not required: a
 * {@code @ConditionalOnBean(WorkerPool.class)} on a scanned class is evaluated before the
 * autoconfiguration that contributes the pool is processed, and would never match. And the readiness
 * group names this contributor, so it has to exist in every role; without a relay it answers
 * {@code UNKNOWN}, which does not lower a group's status.
 *
 * <p>The bean name is what gives the contributor its name, {@code tandemRelay}: the one the readiness
 * group includes and the response shows.
 */
@Component(RelayHealthIndicator.BEAN_NAME)
class RelayHealthIndicator implements HealthIndicator {

    static final String BEAN_NAME = "tandemRelayHealthIndicator";

    private final WorkerPool workerPool;   // null when this process runs no relay
    private final RelayControlSource controlSource;
    private final RelayHealthProperties properties;
    private final Clock clock;

    @Autowired
    RelayHealthIndicator(ObjectProvider<WorkerPool> workerPool, ObjectProvider<RelayControlSource> controlSource,
            ObjectProvider<RelayConfig> relayConfig, RelayHealthProperties properties) {
        this(workerPool, controlSource, relayConfig, properties, Clock.systemUTC());
    }

    RelayHealthIndicator(ObjectProvider<WorkerPool> workerPool, ObjectProvider<RelayControlSource> controlSource,
            ObjectProvider<RelayConfig> relayConfig, RelayHealthProperties properties, Clock clock) {
        relayConfig.ifAvailable(config -> properties.requireMarginOver(config.pollInterval()));
        this.workerPool = workerPool.getIfAvailable();
        this.controlSource = controlSource.getIfAvailable(() -> RelayControlSource.NOOP);
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public Health health() {
        if (workerPool == null) {
            return Health.unknown().build();
        }
        return RelayVerdict.of(workerPool.status(), controlSource.wholeRelayPaused(), clock.instant(),
                properties.stalledAfter());
    }
}
