package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.benchmark.scenario.S12BrokerOutage;
import com.codingful.tandem.benchmark.scenario.S1SustainedThroughput;
import com.codingful.tandem.benchmark.scenario.S5WorkerFailover;
import com.codingful.tandem.benchmark.scenario.S6PoisonMessage;
import com.codingful.tandem.benchmark.scenario.S8MultiInstanceLease;
import com.codingful.tandem.benchmark.scenario.Scenario;
import com.codingful.tandem.benchmark.scenario.ScenarioContext;
import com.codingful.tandem.benchmark.scenario.ScenarioResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * The same smoke check as {@link SmokeLoadTest}, against RabbitMQ (LLD-benchmark §3.1): tiny rate,
 * short duration, <b>correctness only</b>.
 *
 * <p>It is here because the AMQP connector's own suite drives {@code dispatch} directly, one row at a
 * time, waiting for each ack, so the relay loop around it (concurrent claims from several workers,
 * retry on a retriable failure, quarantine, {@code LEASE} coordination between two instances) never
 * reaches the connector there. These five scenarios are the ones that exercise a structurally
 * distinct part of that loop: the ramp, failover and its duplicates, the poison gate, two instances
 * on one outbox, and an interrupted broker.
 *
 * <p>A second Postgres and a RabbitMQ container, on top of {@link SmokeLoadTest}'s: the two suites
 * cannot share an environment, because which broker a run publishes to is fixed when the environment
 * is built.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RabbitSmokeLoadTest {

    private BenchmarkEnvironment env;
    private BenchmarkConfig smokeConfig;

    @BeforeAll
    void start() {
        smokeConfig = BenchmarkConfig.defaults().toSmoke().toBuilder().broker(Broker.RABBIT).build();
        env = new BenchmarkEnvironment(smokeConfig).start();
    }

    @AfterAll
    void stop() {
        if (env != null) {
            env.close();
        }
    }

    @Test
    void s1SustainedThroughputSmoke() throws Exception {
        assertPassed(new S1SustainedThroughput());
    }

    @Test
    void s5WorkerFailoverSmoke() throws Exception {
        assertPassed(new S5WorkerFailover());
    }

    @Test
    void s6PoisonMessageSmoke() throws Exception {
        assertPassed(new S6PoisonMessage());
    }

    @Test
    void s8MultiInstanceLeaseSmoke() throws Exception {
        assertPassed(new S8MultiInstanceLease());
    }

    @Test
    void s12BrokerOutageSmoke() throws Exception {
        assertPassed(new S12BrokerOutage());
    }

    private void assertPassed(Scenario scenario) throws Exception {
        ScenarioResult result = scenario.run(new ScenarioContext(env, smokeConfig));
        assertThat(result.passed())
                .as("%s: %s", result.scenarioId(), result.summary())
                .isTrue();
    }
}
