#!/bin/bash
# Samples how often the relay's claim actually runs, from PostgreSQL's own counters.
#
# Each claim execution scans idx_tandem_outbox_dispatch exactly once, empty claims included, which
# is the number we want and the one the relay itself does not count today. The head-of-chain
# NOT EXISTS lands on idx_tandem_outbox_aggregate instead, so the two are reported separately
# rather than summed.
#
# Usage: sample-claims.sh <interval_seconds> > claims.tsv
set -u
INTERVAL="${1:-30}"
printf 'time\tdispatch_idx_scan\taggregate_idx_scan\txact_commit\n'
while true; do
    c=$(docker ps --filter ancestor=postgres:16-alpine --format '{{.Names}}' | head -1)
    if [ -n "$c" ]; then
        row=$(docker exec "$c" psql -U test -d test -tAF$'\t' -c "
            SELECT
              (SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname = 'idx_tandem_outbox_dispatch'),
              (SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname = 'idx_tandem_outbox_aggregate'),
              (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database())" 2>/dev/null | tr -d '\r')
        [ -n "$row" ] && printf '%s\t%s\n' "$(date +%H:%M:%S)" "$row"
    fi
    sleep "$INTERVAL"
done
