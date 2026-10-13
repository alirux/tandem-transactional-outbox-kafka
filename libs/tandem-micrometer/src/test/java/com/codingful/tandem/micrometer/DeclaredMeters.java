package com.codingful.tandem.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Records each meter as the code declares it, before the registry adds anything of its own: a registry
 * stamps its base time unit on every timer, which is the registry's choice and not the code's, and it
 * merges its own histogram defaults into the configuration a meter states. A real {@link MeterFilter},
 * added to the registry before the meters are registered.
 */
final class DeclaredMeters implements MeterFilter {

    private final List<Meter.Id> ids = new ArrayList<>();
    private final Map<String, DistributionStatisticConfig> distributions = new HashMap<>();

    @Override
    public Meter.Id map(Meter.Id id) {
        ids.add(id);
        return id;
    }

    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
        distributions.put(id.getName(), config);
        return config;
    }

    List<Meter.Id> ids() {
        return ids;
    }

    /** @return the distribution statistics the named meter states, or {@code null} for one that states none */
    DistributionStatisticConfig distribution(String name) {
        return distributions.get(name);
    }
}
