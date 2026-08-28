#!/usr/bin/env bash
#
# Samples host and container resource use alongside a tandem-benchmark run.
#
# Two things here are deliberate, both fixes for defects in the ad-hoc sampler this replaces:
#
#   1. iostat is asked for TWO reports and only the second is kept. iostat's first report always
#      covers the time since the machine booted, so `iostat 1 1` yields a lifetime average that
#      barely moves regardless of load — the earlier runs' disk columns are unusable for exactly
#      this reason.
#   2. Disk columns are located BY HEADER NAME, not by fixed position. The column order of
#      `iostat -dx` changes between sysstat versions, so a hardcoded $6/$7/$NF silently reads the
#      wrong metric on a different host.
#
# mpstat needs no such care: its `Average:` line already covers the interval, which the earlier
# data confirms empirically (its CPU columns track the offered rate, while the disk ones did not).
#
# Usage: ./sample-resources.sh <out-dir>
#        ./sample-resources.sh --self-test     # prove each channel responds, then exit
set -uo pipefail

# --------------------------------------------------------------------------------------------- #
# --self-test: apply a known stimulus and require the sampler to see it.
#
# This is the check that would have caught the original defect before it cost a run. A sampler
# reporting since-boot averages looks perfectly healthy at rest — it emits plausible small numbers —
# and only reveals itself when something known-large happens and the reading does not move. So make
# something known-large happen.
# --------------------------------------------------------------------------------------------- #
self_test() {
  local fail=0

  local idle busy
  idle=$(read_disk | cut -d, -f3)
  dd if=/dev/zero of=/tmp/.sampler-selftest bs=1M count=512 oflag=direct 2>/dev/null &
  local dd_pid=$!
  busy=$(read_disk | cut -d, -f3)
  wait "$dd_pid" 2>/dev/null
  rm -f /tmp/.sampler-selftest
  echo "disk %util:  idle=${idle}  under load=${busy}"
  awk -v i="${idle:-0}" -v b="${busy:-0}" 'BEGIN { exit !(b > i + 20) }' || {
    echo "  FAIL: writing 512 MB moved disk utilisation by less than 20 points."
    echo "        The sampler is almost certainly reading since-boot averages instead of the interval."
    fail=1; }

  local cpu_idle cpu_busy
  cpu_idle=$(mpstat 1 1 | awk '/Average/ && $2 == "all" { print $3 }')
  timeout 3 bash -c 'while :; do :; done' &
  local spin=$!
  cpu_busy=$(mpstat 1 1 | awk '/Average/ && $2 == "all" { print $3 }')
  kill "$spin" 2>/dev/null; wait "$spin" 2>/dev/null
  echo "cpu %usr:    idle=${cpu_idle}  under load=${cpu_busy}"
  awk -v i="${cpu_idle:-0}" -v b="${cpu_busy:-0}" 'BEGIN { exit !(b > i + 10) }' || {
    echo "  FAIL: a busy loop moved user CPU by less than 10 points."; fail=1; }

  if [ "$fail" -eq 0 ]; then echo "self-test OK — every channel responded to its stimulus"; fi
  return "$fail"
}

if [ "${1:-}" = "--self-test" ]; then
  # read_disk is defined below; source order matters only for this early exit.
  read_disk() {
    iostat -dxk 1 2 | awk '
      /^Device/ { for (i = 1; i <= NF; i++) col[$i] = i; next }
      /nvme0n1|xvda/ { r = $col["rkB/s"]; w = $col["wkB/s"]; u = $col["%util"] }
      END { if (u == "") print "NA,NA,NA"; else printf "%s,%s,%s\n", r, w, u }'
  }
  self_test
  exit $?
fi

out="${1:?usage: sample-resources.sh <out-dir>}"
mkdir -p "$out"
host_csv="$out/resources.csv"
cont_csv="$out/containers.csv"

# %steal is carried because this host is a burstable instance: when two consecutive runs disagree
# on throughput, the first question is whether the hypervisor took cycles away, and without this
# column that question can only be inferred from "same CPU%, half the work" rather than answered.
echo "timestamp,cpu_usr,cpu_sys,cpu_iowait,cpu_steal,cpu_idle,mem_used_mb,mem_avail_mb,disk_read_kbps,disk_write_kbps,disk_util_pct" > "$host_csv"
echo "timestamp,container,cpu_pct,mem_used_mb" > "$cont_csv"

# Second report only, columns resolved from the header that precedes it.
read_disk() {
  iostat -dxk 1 2 | awk '
    /^Device/ { for (i = 1; i <= NF; i++) col[$i] = i; next }
    /nvme0n1|xvda/ { r = $col["rkB/s"]; w = $col["wkB/s"]; u = $col["%util"] }
    END { if (u == "") print "NA,NA,NA"; else printf "%s,%s,%s\n", r, w, u }'
}

while true; do
  ts=$(date +%H:%M:%S)

  # Columns are located by header name, for the same reason the disk ones are: mpstat's column
  # set differs between sysstat versions, and %steal in particular is absent on some platforms.
  cpu=$(mpstat 1 1 | awk '
    /%usr/ { for (i = 1; i <= NF; i++) col[$i] = i; next }
    /Average/ && $2 == "all" {
      printf "%s,%s,%s,%s,%s", $col["%usr"], $col["%sys"], $col["%iowait"],
             (("%steal" in col) ? $col["%steal"] : "NA"), $col["%idle"]
    }')
  mem=$(free -m | awk '/^Mem:/ { printf "%s,%s", $3, $7 }')
  disk=$(read_disk)
  echo "$ts,${cpu:-NA,NA,NA,NA,NA},${mem:-NA,NA},${disk:-NA,NA,NA}" >> "$host_csv"

  # MemUsage arrives as "123.4MiB / 7.6GiB"; keep the used side, normalised to MB.
  docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' 2>/dev/null \
    | awk -v ts="$ts" -F'\t' '
        {
          gsub(/%/, "", $2)
          split($3, m, " / "); v = m[1]
          unit = v; sub(/^[0-9.]+/, "", unit); sub(/[A-Za-z]+$/, "", v)
          if (unit ~ /^GiB/) v *= 1024; else if (unit ~ /^KiB/) v /= 1024
          printf "%s,%s,%s,%.1f\n", ts, $1, $2, v
        }' >> "$cont_csv"

  sleep 1
done
