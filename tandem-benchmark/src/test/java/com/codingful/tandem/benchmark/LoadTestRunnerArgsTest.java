package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins how the harness is sized from its command line (LLD-benchmark §9). Every assertion here exists
 * because the failure it guards is silent: a run given the wrong knobs still produces a clean-looking
 * PASS line and a plausible percentile, and the mistake is only visible once the number is published.
 */
class LoadTestRunnerArgsTest {

    private static final String WORKERS = "--workers=2";
    private static final String POLL_INTERVAL = "--poll-interval=20";

    @Test
    void GIVEN_no_sizing_arguments_WHEN_the_harness_is_configured_THEN_it_uses_the_full_run_defaults() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of());

        assertThat(config.workers()).isEqualTo(BenchmarkConfig.defaults().workers());
        assertThat(config.pollInterval()).isEqualTo(BenchmarkConfig.defaults().pollInterval());
    }

    @Test
    void GIVEN_a_worker_count_and_a_poll_interval_WHEN_the_harness_is_configured_THEN_both_are_honoured() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of(WORKERS, POLL_INTERVAL));

        assertThat(config.workers()).isEqualTo(2);
        assertThat(config.pollInterval()).isEqualTo(Duration.ofMillis(20));
    }

    @Test
    void GIVEN_a_preset_that_sizes_the_relay_itself_WHEN_an_explicit_worker_count_is_also_given_THEN_the_explicit_one_wins() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of("--smoke", "--workers=6"));

        // --smoke caps workers at 2 of its own accord, so this only passes if the flag is applied after
        // the preset rather than before it.
        assertThat(config.workers()).isEqualTo(6);
        assertThat(config.duration()).isEqualTo(BenchmarkConfig.defaults().toSmoke().duration());
    }

    @Test
    void GIVEN_a_sizing_argument_alongside_a_duration_WHEN_the_harness_is_configured_THEN_neither_override_drops_the_other() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of("--duration=240", POLL_INTERVAL));

        assertThat(config.duration()).isEqualTo(Duration.ofSeconds(240));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofMillis(20));
    }

    @Test
    void GIVEN_an_offered_rate_and_a_reporting_window_WHEN_the_harness_is_configured_THEN_both_are_honoured() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of("--rate=250", "--window=1200"));

        assertThat(config.offeredRate()).isEqualTo(250.0);
        assertThat(config.window()).isEqualTo(Duration.ofMinutes(20));
    }

    @Test
    void GIVEN_a_connection_pool_size_WHEN_the_harness_is_configured_THEN_it_overrides_the_default() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of("--connections=48"));

        assertThat(config.maxConnections()).isEqualTo(48);
    }

    @Test
    void GIVEN_no_offered_rate_WHEN_the_harness_is_configured_THEN_the_scenario_is_left_to_find_one() {
        assertThat(LoadTestRunner.configFrom(List.of()).offeredRate()).isZero();
    }

    @Test
    void GIVEN_sizing_arguments_and_a_scenario_list_WHEN_the_scenarios_are_selected_THEN_only_the_list_is_read_as_scenarios() {
        List<String> scenarios = LoadTestRunner.scenarioIdsFrom(List.of(WORKERS, "S1,S2", POLL_INTERVAL));

        assertThat(scenarios).containsExactly("S1", "S2");
    }

    @Test
    void GIVEN_only_sizing_arguments_WHEN_the_scenarios_are_selected_THEN_every_known_scenario_runs() {
        List<String> scenarios = LoadTestRunner.scenarioIdsFrom(List.of(WORKERS, POLL_INTERVAL));

        assertThat(scenarios).containsExactlyElementsOf(LoadTestRunner.knownScenarioIds());
    }

    @Test
    void GIVEN_a_misspelled_sizing_argument_WHEN_the_harness_is_configured_THEN_it_refuses_to_run() {
        assertThatThrownBy(() -> LoadTestRunner.main(new String[] {"--pollinterval=20"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--pollinterval=20");
    }

    @Test
    void GIVEN_a_sizing_argument_carrying_something_that_is_not_a_number_WHEN_the_harness_is_configured_THEN_the_error_names_the_flag() {
        assertThatThrownBy(() -> LoadTestRunner.configFrom(List.of("--poll-interval=20ms")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--poll-interval=")
                .hasMessageContaining("20ms");
    }

    @Test
    void GIVEN_a_poll_interval_of_zero_WHEN_the_harness_is_configured_THEN_it_refuses_to_run() {
        assertThatThrownBy(() -> LoadTestRunner.configFrom(List.of("--poll-interval=0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollInterval");
    }

    @Test
    void GIVEN_a_configured_poll_interval_WHEN_a_relay_is_built_from_it_THEN_the_relay_polls_at_that_interval() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of(WORKERS, POLL_INTERVAL));

        // The knob is worthless if it stops at the harness config, and the gap between the two is a
        // single line in the mapping that nothing else would notice losing.
        var relayConfig = BenchmarkEnvironment.relayConfigBuilder(config, config.deliveryTimeoutMs()).build();

        assertThat(relayConfig.pollInterval()).isEqualTo(config.pollInterval());
        assertThat(relayConfig.workersPerInstance()).isEqualTo(config.workers());
    }

    @Test
    void GIVEN_a_cleanup_policy_on_the_command_line_WHEN_a_relay_is_built_from_it_THEN_it_deletes_on_that_policy() {
        BenchmarkConfig config = LoadTestRunner.configFrom(
                List.of("--retention=600", "--cleanup-interval=30", "--cleanup-batch=20000"));

        var relayConfig = BenchmarkEnvironment.relayConfigBuilder(config, config.deliveryTimeoutMs()).build();

        // A run shorter than the 14-day default retention deletes nothing at all, so the outbox only
        // grows — which for a run measured in hours is both the wrong shape to measure and a way to
        // fill the host's disk. The knob only means anything if it survives the crossing into RelayConfig.
        assertThat(relayConfig.retention()).isEqualTo(Duration.ofMinutes(10));
        assertThat(relayConfig.cleanupInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(relayConfig.cleanupBatchSize()).isEqualTo(20_000);
    }

    @Test
    void GIVEN_no_cleanup_arguments_WHEN_a_relay_is_built_THEN_it_keeps_the_libraries_own_defaults() {
        BenchmarkConfig config = LoadTestRunner.configFrom(List.of());

        var relayConfig = BenchmarkEnvironment.relayConfigBuilder(config, config.deliveryTimeoutMs()).build();

        assertThat(relayConfig.retention()).isEqualTo(Duration.ofDays(14));
        assertThat(relayConfig.cleanupBatchSize()).isEqualTo(1000);
    }
}
