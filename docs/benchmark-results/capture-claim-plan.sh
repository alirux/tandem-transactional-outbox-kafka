#!/bin/bash
# Captures the plan the RELAY actually executes for its claim query, using auto_explain.
#
# Why this exists: a hand-typed EXPLAIN does NOT answer the question. The relay's prepared statement
# passes the bucket array as a parameter and gets a *generic* plan; an EXPLAIN with a literal array
# gets a different one. Measured during the 2026-08-31 endurance run (backlog item 9b): while the
# relay was demonstrably scanning idx_tandem_outbox_dispatch at 1772 scans/s, a fresh psql plan for
# the identical query text still chose the aggregate index. auto_explain reads the executed plan, so
# it is the only instrument that settles this from outside the product.
#
# Three things about the mechanism, all of which shape what this script can and cannot do:
#
#  1. auto_explain is loaded per session via session_preload_libraries, set with ALTER DATABASE. That
#     needs NO server restart and NO change to the container, so nothing in tandem-test is touched.
#  2. Settings apply at CONNECTION START, so only sessions opened after this script arms them will
#     log anything. The relay's pool connections are long-lived: plans appear as the pool recycles
#     them (HikariCP maxLifetime), not immediately. On a six-hour run that is fine; on a two-minute
#     one you may capture nothing, and the script says so rather than leaving you guessing.
#  3. For the same reason a session that started logging CANNOT be told to stop until it is recycled.
#     That is why volume is bounded by auto_explain.sample_rate rather than by disarming: disarming
#     only stops *future* sessions. At ~3700 statements/s a sample rate of 0.001 is a few plans per
#     second, which is a readable log rather than gigabytes.
#
# The capture is labelled with the claim plan regime measured immediately before and after, from the
# dispatch index counter, because the planner alternates between two plans every 5-10 minutes and a
# plan with no regime attached is not attributable to anything (item 9b).
#
# Usage: capture-claim-plan.sh [--window=300] [--sample-rate=0.001] [--regime=any|dispatch|aggregate]
#                              [--timeout=1800] [--out=claim-plans.txt] [--max-plans=25]
set -u

WINDOW=300; SAMPLE_RATE=0.001; WANT_REGIME=any; TIMEOUT=1800; OUT=claim-plans.txt; MAX_PLANS=25
for arg in "$@"; do
    case "$arg" in
        --window=*)      WINDOW="${arg#*=}" ;;
        --sample-rate=*) SAMPLE_RATE="${arg#*=}" ;;
        --regime=*)      WANT_REGIME="${arg#*=}" ;;
        --timeout=*)     TIMEOUT="${arg#*=}" ;;
        --out=*)         OUT="${arg#*=}" ;;
        --max-plans=*)   MAX_PLANS="${arg#*=}" ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done

PG=$(docker ps --filter ancestor=postgres:16-alpine --format '{{.Names}}' | head -1)
if [ -z "$PG" ]; then echo "No postgres:16-alpine container is running." >&2; exit 1; fi
psql_() { docker exec "$PG" psql -U test -d test -tAc "$1" 2>/dev/null | tr -d '\r'; }

# Classifies the current regime from the dispatch index counter over a short probe.
regime_now() {
    local a b r
    a=$(psql_ "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname='idx_tandem_outbox_dispatch'")
    sleep 15
    b=$(psql_ "SELECT idx_scan FROM pg_stat_user_indexes WHERE indexrelname='idx_tandem_outbox_dispatch'")
    if [ -z "$a" ] || [ -z "$b" ]; then echo "unknown"; return; fi
    r=$(( (b - a) / 15 ))
    if   [ "$r" -lt 500 ];  then echo "aggregate $r"
    elif [ "$r" -gt 5000 ]; then echo "dispatch $r"
    else                         echo "mixed $r"; fi
}

if [ "$WANT_REGIME" != "any" ]; then
    echo "Waiting for the $WANT_REGIME regime (timeout ${TIMEOUT}s)..." >&2
    deadline=$(( $(date +%s) + TIMEOUT ))
    while :; do
        cur=$(regime_now)
        [ "${cur%% *}" = "$WANT_REGIME" ] && break
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "Timed out waiting for the $WANT_REGIME regime; last seen: $cur" >&2; exit 3
        fi
    done
fi

REGIME_BEFORE=$(regime_now)
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)   # the Z is load-bearing: docker logs --since
                                       # reads a naive timestamp as LOCAL time.

psql_ "ALTER DATABASE test SET session_preload_libraries = 'auto_explain'" >/dev/null
psql_ "ALTER DATABASE test SET auto_explain.log_min_duration = 0" >/dev/null
psql_ "ALTER DATABASE test SET auto_explain.log_analyze = on" >/dev/null
psql_ "ALTER DATABASE test SET auto_explain.log_parameter_max_length = 1024" >/dev/null
psql_ "ALTER DATABASE test SET auto_explain.sample_rate = $SAMPLE_RATE" >/dev/null
echo "Armed at $START (sample_rate=$SAMPLE_RATE); waiting ${WINDOW}s for pool connections to recycle..." >&2

sleep "$WINDOW"

# Disarming stops FUTURE sessions only; sessions already logging are bounded by sample_rate above.
psql_ "ALTER DATABASE test RESET session_preload_libraries" >/dev/null
psql_ "ALTER DATABASE test RESET auto_explain.log_min_duration" >/dev/null
psql_ "ALTER DATABASE test RESET auto_explain.log_analyze" >/dev/null
psql_ "ALTER DATABASE test RESET auto_explain.log_parameter_max_length" >/dev/null
psql_ "ALTER DATABASE test RESET auto_explain.sample_rate" >/dev/null

REGIME_AFTER=$(regime_now)

RAW=$(mktemp)
# One plan per record, bounded by the next "duration: ... plan:" line, and the block is flushed *at*
# that boundary rather than after resetting the buffer. The claim is recognised by matching the whole
# accumulated block, never a single line: auto_explain wraps the SQL after "Query Text:" and the
# statement's first words can land on the following line, so a line-anchored filter silently matches
# nothing. No interval quantifiers either, the awk shipped with macOS rejects them.
docker logs --since "$START" "$PG" 2>&1 \
    | awk 'function flush() { if (buf ~ /WITH claimed AS/) print buf "\n"; buf = "" }
           /LOG: +duration:.*plan:/ { flush(); buf = $0; next }
           { if (buf != "") buf = buf "\n" $0 }
           END { flush() }' > "$RAW"

N=$(grep -c "LOG: +duration" "$RAW")
[ "$N" -eq 0 ] && N=$(grep -c "duration:" "$RAW")

{
    echo "=== Relay claim plans captured by auto_explain ==="
    echo "container:      $PG"
    echo "window:         $START .. $(date -u +%Y-%m-%dT%H:%M:%SZ) (${WINDOW}s armed)"
    echo "sample_rate:    $SAMPLE_RATE"
    echo "regime before:  $REGIME_BEFORE   (label value = dispatch index scans/s)"
    echo "regime after:   $REGIME_AFTER"
    echo "plans captured: $N"
    echo
    echo "--- which index each plan drove the outer scan from (the item 9b question) ---"
    awk -v RS="" -v FS="\n" '
        {
          d = a = 0; generic = custom = 0
          for (i = 1; i <= NF; i++) {
            line = $i
            if (line ~ /idx_tandem_outbox_dispatch/)  d = 1
            if (line ~ /idx_tandem_outbox_aggregate/) a = 1
            # Only a plan node states the resolved condition. The query text names $1 whatever the
            # plan is, so classifying from it would call every plan generic.
            if (line ~ /(Index Cond|Recheck Cond|Filter):/) {
              if (line ~ /ANY \(\$/)  generic = 1
              if (line ~ /ANY \('"'"'/) custom  = 1
            }
          }
          g = (generic && !custom) ? "generic" : ((custom && !generic) ? "custom" : (generic ? "mixed" : "n/a"))
          k = (d && a) ? "both" : (d ? "dispatch" : (a ? "aggregate" : "neither"))
          tally[k "  (" g " plan)"]++
        }
        END { for (k in tally) printf "  %-30s %d\n", k, tally[k] }' "$RAW" | sort
    echo
    echo "--- up to $MAX_PLANS full plans ---"
    awk -v RS="" -v max="$MAX_PLANS" 'n < max { print; print ""; n++ }' "$RAW"
} > "$OUT"
rm -f "$RAW"
echo "Captured $N relay claim plans -> $OUT" >&2
if [ "$N" -eq 0 ]; then
    cat >&2 <<'MSG'
None captured. The most likely cause is trap 2 above: no pool connection was recycled inside the
window, so no relay session had auto_explain loaded. Re-run with a longer --window (the pool's
maxLifetime is the natural lower bound), or run it against a long-lived load rather than a short one.
MSG
fi
