#!/usr/bin/env bash
#
# Samples a macOS host alongside a long tandem-benchmark run (the endurance scenario, S9).
#
# The Linux counterpart is sample-resources.sh; this exists because the two hosts hide their
# throttling in different places. An Intel Mac reduces its clock under sustained load, and a run
# whose throughput decays because the laptop got hot reads exactly like a run whose throughput
# decays because the software leaked. `CPU_Speed_Limit` (pmset -g therm) is what tells the two
# apart, so it is the first column and the reason this script exists at all.
#
# Output is one TSV row per container per sample, so a row carries both the host state and that
# container's counters at the same instant:
#
#   time  cpu_speed_limit  sched_limit  load1  container  cpu_pct  mem_used  mem_pct  net_io  block_io
#
# Usage:  ./sample-host-mac.sh [interval_seconds] > host-samples.tsv
#         ./sample-host-mac.sh 60 > ~/endurance/host-samples.tsv &
#
# Write the container map alongside it, since `docker stats` reports names and a reader six months
# later cannot tell which random name was PostgreSQL:
#
#   docker ps --format '{{.Names}}\t{{.Image}}' > container-map.tsv

set -u
INTERVAL="${1:-60}"

printf 'time\tcpu_speed_limit\tsched_limit\tload1\tcontainer\tcpu_pct\tmem_used\tmem_pct\tnet_io\tblock_io\n'

while true; do
    stamp=$(date +%H:%M:%S)

    # pmset prints "CPU_Speed_Limit = 100" only while some limit is being applied on Intel Macs;
    # a missing key means "no limit", which is 100, not zero. Apple Silicon prints neither key —
    # the columns then stay 100 and the run's own numbers are all there is to go on.
    therm=$(pmset -g therm 2>/dev/null)
    speed=$(printf '%s\n' "$therm" | awk -F'= *' '/CPU_Speed_Limit/ {print $2; found=1} END {if (!found) print 100}')
    sched=$(printf '%s\n' "$therm" | awk -F'= *' '/CPU_Scheduler_Limit/ {print $2; found=1} END {if (!found) print 100}')
    load1=$(uptime | sed -E 's/.*load averages?: *([0-9.]+).*/\1/')

    # --no-stream takes one reading and exits; without it docker stats never returns. A failure
    # here must not end the sampling: a run that loses its instrument at hour three still wants
    # the hours it already has.
    stats=$(docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}' 2>/dev/null)

    if [ -z "$stats" ]; then
        # Docker said nothing (daemon gone, containers gone). The host columns are the ones that
        # answer "did the machine slow down?", so they are still written, with the container
        # fields empty — a gap in this series is exactly what must not happen silently.
        printf '%s\t%s\t%s\t%s\t\t\t\t\t\t\n' "$stamp" "$speed" "$sched" "$load1"
    else
        printf '%s\n' "$stats" | while IFS= read -r line; do
            printf '%s\t%s\t%s\t%s\t%s\n' "$stamp" "$speed" "$sched" "$load1" "$line"
        done
    fi

    sleep "$INTERVAL"
done
