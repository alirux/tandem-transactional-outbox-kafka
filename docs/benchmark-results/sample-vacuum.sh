#!/bin/bash
# Samples autovacuum/analyze state for tandem_outbox, to correlate with the claim planner's
# oscillation between idx_tandem_outbox_dispatch and idx_tandem_outbox_aggregate.
# Catalog reads only. Usage: sample-vacuum.sh <interval_seconds> > vacuum.tsv
set -u
INTERVAL="${1:-60}"
printf 'time\tn_live\tn_dead\tvacuum_count\tautovacuum_count\tanalyze_count\tautoanalyze_count\tlast_autovacuum\tlast_autoanalyze\tdispatch_kb\taggregate_kb\n'
while true; do
    c=$(docker ps --filter ancestor=postgres:16-alpine --format '{{.Names}}' | head -1)
    if [ -n "$c" ]; then
        row=$(docker exec "$c" psql -U test -d test -tAF$'\t' -c "
            SELECT n_live_tup, n_dead_tup, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
                   coalesce(to_char(last_autovacuum,'HH24:MI:SS'),'-'), coalesce(to_char(last_autoanalyze,'HH24:MI:SS'),'-'),
                   pg_relation_size('idx_tandem_outbox_dispatch')/1024,
                   pg_relation_size('idx_tandem_outbox_aggregate')/1024
            FROM pg_stat_user_tables WHERE relname='tandem_outbox'" 2>/dev/null | tr -d '\r')
        [ -n "$row" ] && printf '%s\t%s\n' "$(date +%H:%M:%S)" "$row"
    fi
    sleep "$INTERVAL"
done
