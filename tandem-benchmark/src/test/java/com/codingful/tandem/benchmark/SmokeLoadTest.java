package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.benchmark.scenario.S10ColdBurst;
import com.codingful.tandem.benchmark.scenario.S1SustainedThroughput;
import com.codingful.tandem.benchmark.scenario.S3HotPartition;
import com.codingful.tandem.benchmark.scenario.S5WorkerFailover;
import com.codingful.tandem.benchmark.scenario.S6PoisonMessage;
import com.codingful.tandem.benchmark.scenario.S8MultiInstanceLease;
import com.codingful.tandem.benchmark.scenario.S9Endurance;
import com.codingful.tandem.benchmark.scenario.Scenario;
import com.codingful.tandem.benchmark.scenario.ScenarioContext;
import com.codingful.tandem.benchmark.scenario.ScenarioResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * CI smoke check (HLD-load-testing.md §7, LLD-benchmark §9): tiny rate, short duration — keeps the
 * harness compiling and wired, asserts <b>correctness only</b>, never KPI numbers. Covers S1, S3, S5,
 * S6, S8 — the scenarios that each exercise a structurally distinct code path (ramp, skew, failover,
 * poison, multi-instance LEASE coordination); S2/S4 reuse S1's infrastructure and are exercised only
 * in full runs. S10 is included for the same reason the others are: nothing else exercises the
 * post-commit wakeup, and its two arms build their own relay instances with and without a listening
 * connection.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SmokeLoadTest {

    private BenchmarkEnvironment env;
    private BenchmarkConfig smokeConfig;

    @BeforeAll
    void start() {
        smokeConfig = BenchmarkConfig.defaults().toSmoke();
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
    void s3HotPartitionSmoke() throws Exception {
        assertPassed(new S3HotPartition());
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
    void s9EnduranceSmoke() throws Exception {
        assertPassed(new S9Endurance());
    }

    @Test
    void s10ColdBurstSmoke() throws Exception {
        assertPassed(new S10ColdBurst());
    }

    private void assertPassed(Scenario scenario) throws Exception {
        ScenarioResult result = scenario.run(new ScenarioContext(env, smokeConfig));
        assertThat(result.passed())
                .as("%s: %s", result.scenarioId(), result.summary())
                .isTrue();
    }
}
