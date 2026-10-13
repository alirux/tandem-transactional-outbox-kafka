package com.codingful.tandem.core.port;

import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;

/**
 * Replay matching rows back to {@code PENDING} for re-delivery (LLD-core §2.6, §8). Honours {@code dryRun}.
 *
 * <p><b>Provided:</b> Tandem implements this interface and the application calls it. A minor version
 * may add abstract methods, so an application implementing it follows Tandem's minor versions
 * (HLD-api-stability §4.1).
 */
public interface ReplayService {

    /**
     * @param criteria which rows to match, and whether to actually reset them ({@code dryRun})
     * @return the match/replay counts (§8)
     */
    ReplayResult replay(ReplayCriteria criteria);
}
