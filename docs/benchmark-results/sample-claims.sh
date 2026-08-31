#!/bin/bash
# Samples how often the relay's claim actually runs, from PostgreSQL's own counters, because nothing
# in the product counts claims or wakeups today (dispatch-latency.md Q-H).
#
# ⚠️ READ THIS BEFORE CONVERTING THE COUNTERS INTO claims/s. There is no fixed divisor. The planner
# alternates between two plans for the claim query, binary, roughly every 5-10 minutes (backlog item
# 9b, measured over six hours in 2026-08-31-endurance-wakeup/):
#
#   dispatch plan   the outer scan is an Index Scan on idx_tandem_outbox_dispatch with
#                   bucket = ANY(array), a ScalarArrayOpExpr, which is understood to advance idx_scan
#                   once per array element: one claim would then count as (buckets owned by that
#                   worker) scans, 16 in the standard 256-bucket / 16-worker layout. ⚠️ Treat that as
#                   the working assumption it is, not as a verified constant. A synthetic check gave
#                   3.0 scans per query for a 3-element array but 2.0 for a 1-element one, which the
#                   rule does not predict. The RATIO between two arms sampled under the same plan is
#                   divisor-independent and is the number to quote; an absolute claims/s is not.
#   aggregate plan  the outer scan moves to idx_tandem_outbox_aggregate, the bucket predicate is
#                   demoted to a Filter, and one claim counts as 1 scan there and 0 on dispatch.
#
# So total index scans move by ~6x between regimes while the real work does not change at all
# (xact_commit stays flat across both). Derived claims/s are comparable ONLY at equal plan. The
# regime column below classifies each interval from the dispatch delta so the raw numbers cannot be
# read as one series; it is a classification, not a reading of the plan.
#
# ⚠️ An EXPLAIN from psql will NOT tell you which plan the relay is running: the relay's prepared
# statement passes the bucket array as a parameter and gets a generic plan, while a hand-typed
# EXPLAIN with a literal array gets a different one. This was measured (item 9b, capture #4). The
# only instruments that read the relay's own plan are auto_explain and EXPLAIN EXECUTE.
#
# Raw cumulative counters are always emitted, never only the deltas: a classification can be redone
# later, a lost counter cannot.
#
# Usage: sample-claims.sh <interval_seconds> > claims.tsv
set -u
INTERVAL="${1:-30}"
printf 'time\tdispatch_idx_scan\taggregate_idx_scan\txact_commit\tdispatch_per_s\taggregate_per_s\txact_per_s\tregime\n'
prev_t=""; prev_d=0; prev_a=0; prev_x=0
while true; do
    c=$(docker ps --filter ancestor=postgres:16-alpine --format '{{.Names}}' | head -1)
    if [ -n "$c" ]; then
        row=$(docker exec "$c" psql -U test -d test -tAF$'\t' -c "
            SELECT
              (SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname = 'idx_tandem_outbox_dispatch'),
              (SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname = 'idx_tandem_outbox_aggregate'),
              (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database())" 2>/dev/null | tr -d '\r')
        if [ -n "$row" ]; then
            now=$(date +%s)
            d=$(printf '%s' "$row" | cut -f1); a=$(printf '%s' "$row" | cut -f2); x=$(printf '%s' "$row" | cut -f3)
            if [ -n "$prev_t" ]; then
                el=$((now - prev_t)); [ "$el" -lt 1 ] && el=1
                dps=$(( (d - prev_d) / el )); aps=$(( (a - prev_a) / el )); xps=$(( (x - prev_x) / el ))
                # Thresholds bracket the measured bimodal split (~9700/s or ~0/s); anything between
                # them is an interval that spans a flip, and is labelled as such rather than guessed.
                if [ "$dps" -lt 500 ]; then regime=aggregate
                elif [ "$dps" -gt 5000 ]; then regime=dispatch
                else regime=mixed; fi
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date +%H:%M:%S)" "$d" "$a" "$x" "$dps" "$aps" "$xps" "$regime"
            else
                printf '%s\t%s\t%s\t%s\t\t\t\tfirst\n' "$(date +%H:%M:%S)" "$d" "$a" "$x"
            fi
            prev_t=$now; prev_d=$d; prev_a=$a; prev_x=$x
        fi
    fi
    sleep "$INTERVAL"
done
