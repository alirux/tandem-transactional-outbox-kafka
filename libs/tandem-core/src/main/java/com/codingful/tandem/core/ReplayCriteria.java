package com.codingful.tandem.core;

import java.util.Set;

/**
 * Selection criteria for {@link com.codingful.tandem.core.port.ReplayService} (LLD-core §2.6, §8).
 * At least one selector ({@code aggregateId}, {@code aggregateType}, an id bound, or a status set)
 * must be present — a criteria-less replay would re-publish the entire outbox.
 *
 * @param dryRun when {@code true}, the service reports what would match without changing any row.
 */
public record ReplayCriteria(AggregateId aggregateId,
                             String aggregateType,
                             Long fromId,
                             Long toId,
                             Set<OutboxStatus> statuses,
                             boolean dryRun) {

    public ReplayCriteria {
        statuses = statuses == null ? Set.of() : Set.copyOf(statuses);
        boolean hasSelector = aggregateId != null
                || aggregateType != null
                || fromId != null
                || toId != null
                || !statuses.isEmpty();
        if (!hasSelector) {
            throw new IllegalArgumentException("ReplayCriteria requires at least one selector");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private AggregateId aggregateId;
        private String aggregateType;
        private Long fromId;
        private Long toId;
        private Set<OutboxStatus> statuses = Set.of();
        private boolean dryRun;

        private Builder() {
        }

        public Builder aggregateId(AggregateId aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public Builder aggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        public Builder fromId(Long fromId) {
            this.fromId = fromId;
            return this;
        }

        public Builder toId(Long toId) {
            this.toId = toId;
            return this;
        }

        public Builder statuses(Set<OutboxStatus> statuses) {
            this.statuses = statuses;
            return this;
        }

        public Builder dryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        /** @throws IllegalArgumentException if no selector is set */
        public ReplayCriteria build() {
            return new ReplayCriteria(aggregateId, aggregateType, fromId, toId, statuses, dryRun);
        }
    }
}
