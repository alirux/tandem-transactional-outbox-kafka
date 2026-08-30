package com.codingful.tandem.spring.producer;

import com.codingful.tandem.jdbc.Wakeup;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Binds {@code tandem.outbox.*} — the values the write-side and the relay must agree on
 * (LLD-spring-config §2.1). The bucket count is the one value both sides must share and it must never
 * change after first deployment, because it is baked into every stored row's {@code bucket}; the
 * cross-module guard (LLD-bucket-count-guard) makes a mismatch fail fast rather than stall delivery
 * silently.
 *
 * @param bucketCount number of virtual buckets; the single value the write-side and relay must share,
 *                    bound identically by both modules, and it must never change after first deploy
 * @param wakeup      whether an insert also signals the relay that the bucket it wrote has work
 *                    ({@code pg-notify}), or leaves discovery to the relay's poll loop ({@code none},
 *                    the default). Like the bucket count it belongs to both sides: the relay must
 *                    listen for what the write-side emits, or the signal is never consumed. Setting it
 *                    on one side alone is harmless — polling still bounds discovery
 */
@ConfigurationProperties("tandem.outbox")
public record TandemOutboxProperties(@DefaultValue("256") int bucketCount, @DefaultValue("none") Wakeup wakeup) {
}
