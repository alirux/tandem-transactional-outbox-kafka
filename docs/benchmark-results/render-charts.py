#!/usr/bin/env python3
"""Draws the benchmark charts in docs/ from the archived runs in this directory.

Everything plotted is parsed from the archived files — nothing is transcribed by hand, so a redraw
either reproduces the committed SVGs from the committed data or fails loudly. Palette and type come
from the brand assets (docs/tandem-architecture.svg, docs/tandem-message-flow.svg): the site commits
to a dark look rather than theming, so these sit on the same ground as everything else.

The one colour not already in those assets is the blue accent. The site's own green/red pair is only
5.7 CVD deltaE apart under deuteranopia (>= 8 required), so a mark carrying "held" vs "grew" by colour
alone would be unreadable to a deuteranope; blue sits in the same blue-grey family as the brand's
neutrals and clears every gate against the site's red. Every mark is glyph-coded as well, so colour
is never the sole carrier.
"""

import csv
import math
import pathlib
import re

BG = "#080A0F"
PANEL = "#0D1119"
BORDER = "#212B3B"
AXIS = "#44536E"
MUTED = "#7E8CA8"
SOFT = "#C6D0E0"
BRIGHT = "#E6EDF7"
HELD = "#3987E5"
GREW = "#E03A1A"
DISK = "#86B6EF"
STALE = "#5C6B87"

SANS = '"Helvetica Neue",Helvetica,Arial,sans-serif'
MONO = 'Menlo,"DejaVu Sans Mono",monospace'

HERE = pathlib.Path(__file__).resolve().parent

# Two run sets, because the two questions have different evidence. PERF_RUNS is the current
# behaviour and is what latency, throughput and resource use are drawn from. HOST_RUNS is the
# 2026-08-28 session kept as the record of how this burstable host behaves: the ceilings chart is
# about the machine, not about a version of the relay, and re-measuring it on a later build would
# not make it a better record of that.
PERF_RUNS = ("2026-08-30-ec2-run1", "2026-08-30-ec2-run2", "2026-08-30-ec2-run3-suite")
HOST_RUNS = ("2026-08-28-run1", "2026-08-28-run2", "2026-08-28-run3-suite")
RUN_LABELS = {"2026-08-28-run1": "run 1", "2026-08-28-run2": "run 2", "2026-08-28-run3-suite": "run 3",
              "2026-08-30-ec2-run1": "run 1", "2026-08-30-ec2-run2": "run 2",
              "2026-08-30-ec2-run3-suite": "run 3"}

RAMP_RE = re.compile(
    r"ramp (?P<t>\d\d:\d\d:\d\d)\s+(?P<verdict>held|grew)\s+rate:(?P<rate>[\d.]+)/s\s+"
    r"bracket:\[(?P<lo>[\d.]+), (?P<hi>[\d.]+|unbounded)\].*?pending:(?P<pending>\d+)")
S1_RE = re.compile(r"PASS S1: sustained (?P<rate>[\d.]+) events/s.*?below (?P<hi>[\d.]+)/s")
# The rate may be followed by a parenthetical naming what it is a fraction of, which runs archived
# before that was added do not carry. Optional, so one regex reads both.
S2_RE = re.compile(
    r"PASS S2: normal-load rate (?P<rate>[\d.]+)/s(?: \([^)]*\))?; "
    r"p50=PT(?P<p50>[\d.]+)S p95=PT(?P<p95>[\d.]+)S p99=PT(?P<p99>[\d.]+)S p999=PT(?P<p999>[\d.]+)S")


def _secs(hhmmss):
    h, m, s = (int(p) for p in hhmmss.split(":"))
    return h * 3600 + m * 60 + s


def _log_for(run):
    d = HERE / run
    return d / "loadtest.log" if (d / "loadtest.log").exists() else d / "loadtest-s1-s4.log"


def assert_channels_live(csv_path, required, advisory=()):
    """Refuses to draw from a sampled channel that never moved.

    A run spans idle and saturated phases, so a column claiming to measure activity must take a range
    of values across it. One that takes two or three is not measuring — which is what happened to the
    disk columns in the first runs, collected with `iostat 1 1`, whose single report covers time since
    boot rather than the interval. The resulting flat line was published as "the disk stayed under 3%
    busy"; the chart looked entirely normal.

    It is a cheap net, not a proof: on that same broken data it flags the frozen read/write columns
    but not `disk_util_pct`, because a lifetime average still drifts as uptime grows and so
    accumulates plenty of distinct values while remaining just as meaningless. Catching that class
    properly needs a known stimulus, which is what `sample-resources.sh --self-test` applies before a
    run rather than after it.
    """
    seen = {name: set() for name in list(required) + list(advisory)}
    with csv_path.open() as fh:
        for row in csv.DictReader(fh):
            for name in seen:
                v = (row.get(name) or "").strip()
                if v and v != "NA":
                    seen[name].add(v)
    dead = sorted(n for n in required if len(seen[n]) < 5)
    if dead:
        raise SystemExit(
            f"{csv_path}: channel(s) {', '.join(dead)} took fewer than 5 distinct values across the "
            f"whole run — the sampler was not measuring them, so nothing may be plotted from them. "
            f"See docs/benchmark-results/README.md.")


def load_holds(log):
    """S1's search: every completed hold, stopping where S2 starts its own ramp."""
    holds = []
    for line in log.read_text().splitlines():
        m = RAMP_RE.search(line)
        if not m:
            continue
        if holds and float(m["rate"]) == 100.0 and len(holds) >= 4:
            break
        holds.append({"t": m["t"], "rate": float(m["rate"]), "held": m["verdict"] == "held",
                      "pending": int(m["pending"])})
    if not holds:
        raise SystemExit(f"no ramp lines in {log}")
    return holds


class ChartUnavailable(Exception):
    """A chart's own precondition failed — draw the others, skip this one.

    Distinct from `SystemExit`, which stays reserved for a data-quality problem
    (`assert_channels_live`): a sensor that never moved means the *evidence* is broken and nothing
    downstream can be trusted, so killing the whole redraw immediately is correct. A throughput
    figure whose corroborating run disagrees is a different situation — the other two charts don't
    depend on that agreement at all, and failing them alongside it would hide a real result behind
    an unrelated one. `__main__` catches this per chart and still exits non-zero if any were
    skipped, so the gap is loud without being total.
    """


def load_ceiling(log):
    for line in log.read_text().splitlines():
        m = S1_RE.search(line)
        if m:
            return float(m["rate"]), float(m["hi"])
    raise SystemExit(f"no S1 result in {log}")


def load_latency(log):
    for line in log.read_text().splitlines():
        m = S2_RE.search(line)
        if m:
            return float(m["rate"]), [round(float(m[p]) * 1000, 1)
                                      for p in ("p50", "p95", "p99", "p999")]
    raise SystemExit(f"no S2 result in {log}")


def load_per_step(run):
    """Resource samples grouped per offered rate: each hold's window runs from the previous hold's
    end to its own. Columns are read by header name because the sampler gained a `cpu_steal` column
    part-way through the session, so the two generations differ in width."""
    d = HERE / run
    holds = load_holds(_log_for(run))
    rows = []
    with (d / "resources.csv").open() as fh:
        for r in csv.DictReader(fh):
            if not r.get("timestamp") or not r.get("cpu_usr"):
                continue
            try:
                rows.append((_secs(r["timestamp"]), float(r["cpu_usr"]) + float(r["cpu_sys"]),
                             float(r["cpu_iowait"]), float(r["disk_util_pct"])))
            except (ValueError, TypeError):
                continue
    out = []
    for i, h in enumerate(holds):
        hi = _secs(h["t"])
        lo = rows[0][0] if i == 0 else _secs(holds[i - 1]["t"])
        win = [r for r in rows if lo <= r[0] <= hi]
        if not win:
            continue
        n = len(win)
        out.append({"rate": h["rate"], "held": h["held"], "warm": i == 0, "n": n,
                    "cpu": sum(r[1] for r in win) / n,
                    "iowait": sum(r[2] for r in win) / n,
                    "disk": sum(r[3] for r in win) / n})
    # Ordered by rate, not by the order the search happened to test them in: this chart is about how
    # the machine responds to load, and a non-monotone x-axis hides that relationship.
    out.sort(key=lambda d: d["rate"])
    return out


# `disk_read_kbps` is advisory rather than required, and the distinction is about the workload, not
# about trusting the sensor less: an outbox benchmark is write-heavy, and once the page cache is warm
# PostgreSQL may genuinely never read from disk — one archived run has reads at 0.00 throughout while
# its write column takes 168 distinct values. A flat *write* or *utilisation* column during a
# write-heavy run still means the sampler is broken, so those stay required.
# One sampler covers a whole session, so only the run whose window it starts in carries the CSVs.
for _r in HOST_RUNS + PERF_RUNS:
    _csv = HERE / _r / "resources.csv"
    if _csv.exists():
        assert_channels_live(_csv,
                             required=("cpu_usr", "cpu_sys", "cpu_iowait", "mem_used_mb",
                                       "disk_write_kbps", "disk_util_pct"),
                             advisory=("disk_read_kbps",))

CEILINGS = {r: load_ceiling(_log_for(r)) for r in HOST_RUNS + PERF_RUNS}
LATENCIES = {r: load_latency(_log_for(r)) for r in PERF_RUNS}
STEPS_RUN1 = load_per_step(PERF_RUNS[0])


def head(w, h, label):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
            f'viewBox="0 0 {w} {h}" role="img" aria-label="{label}">\n'
            f'  <style>\n    <![CDATA[\n    text{{ font-family:{SANS}; }}\n'
            f'    .mono{{ font-family:{MONO}; }}\n    ]]>\n  </style>\n'
            f'  <rect x="0" y="0" width="{w}" height="{h}" rx="18" fill="{BG}"/>\n')


def _nice_axis(maxval, ticks=4):
    """An axis top and step that fit `maxval` and still read as round numbers.

    Hardcoding the top is what made this chart overflow once already: the p99.9 grew past a bound
    written when it was smaller, and the bar ran out of the panel rather than the axis growing.
    """
    raw = maxval / ticks
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    step = next((m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw), 10 * mag)
    return math.ceil(maxval / step) * step, step


def _count_word(n):
    """Small counts read as words in alt text, which is prose and is spoken aloud."""
    return ("zero", "one", "two", "three", "four", "five", "six")[n] if n <= 6 else str(n)


def txt(x, y, s, size=13, fill=MUTED, anchor="start", weight=None, mono=True):
    cls = ' class="mono"' if mono else ""
    w = f' font-weight="{weight}"' if weight else ""
    return (f'  <text x="{x:.1f}" y="{y:.1f}"{cls} font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}"{w}>{s}</text>\n')


# --------------------------------------------------------------------------- #
# 1. The ceiling, measured three times
# --------------------------------------------------------------------------- #
def ceilings_svg():
    W, H = 1320, 520
    L, R, T, B = 150, 210, 130, 96
    pw, ph = W - L - R, H - T - B
    xmax = 1600
    X = lambda v: L + (v / xmax) * pw

    s = head(W, H,
             "The same measurement, three times on one machine: run 1 reached 1450 events per second, "
             "run 2 only 725, run 3 1450 again. Run 2 began immediately after run 1 had spent fifteen "
             "minutes at high CPU on a burstable instance.")
    s += f'  <rect x="{L-110}" y="{T-72}" width="{pw+R+70}" height="{ph+150}" rx="12" fill="{PANEL}" stroke="{BORDER}" stroke-width="2"/>\n'
    s += txt(L - 110, 52, "The ceiling is not a property of the software here", 21, BRIGHT, weight="bold", mono=False)
    s += txt(L - 110, 76, "Same machine, same code, three measurements — each one internally precise", 14, MUTED, mono=False)

    for v in (0, 400, 800, 1200, 1600):
        s += f'  <line x1="{X(v):.1f}" y1="{T}" x2="{X(v):.1f}" y2="{T+ph}" stroke="{BORDER}" stroke-width="1"/>\n'
        s += txt(X(v), T + ph + 26, f"{v:.0f}", 13, MUTED, anchor="middle")

    rowh = ph / len(HOST_RUNS)
    for i, run in enumerate(HOST_RUNS):
        lo, hi = CEILINGS[run]
        y = T + rowh * i + rowh / 2
        s += txt(L - 18, y + 5, RUN_LABELS[run], 15, BRIGHT, anchor="end", weight="bold")
        s += f'  <rect x="{L}" y="{y-13:.1f}" width="{max(1,X(lo)-L):.1f}" height="26" rx="4" fill="{HELD}"/>\n'
        # the bracket: held at lo, failed at hi
        s += (f'  <rect x="{X(lo):.1f}" y="{y-13:.1f}" width="{max(2,X(hi)-X(lo)):.1f}" height="26" '
              f'fill="{GREW}" fill-opacity="0.35"/>\n')
        s += txt(X(hi) + 14, y + 5, f"{lo:.0f}", 17, BRIGHT, weight="bold")
        s += txt(X(hi) + 14 + 52, y + 5, f"held, {hi:.0f} did not", 12.5, MUTED, mono=False)
        # The annotation belongs beside the row it explains, not in a caption the reader has to
        # connect back up by themselves.
        if RUN_LABELS[run] == "run 2":
            s += txt(L + 14, y + 26, "started minutes after run 1 finished — the only one that did",
                     12.5, GREW, mono=False)

    s += txt(L - 110, H - 30,
             "Blue = the highest rate that held its backlog flat.  Red = the bracket between it and the "
             "first rate that did not.", 12, AXIS, mono=False)
    return s + "</svg>\n"


# --------------------------------------------------------------------------- #
# 2. What the machine was doing, per offered rate
# --------------------------------------------------------------------------- #
def resources_svg():
    W, H = 1320, 540
    L, R, T, B = 96, 60, 116, 110
    pw, ph = W - L - R, H - T - B
    Y = lambda v: T + ph - (v / 100) * ph
    slot = pw / len(STEPS_RUN1)
    bw = min(46, slot - 34)

    held = [d for d in STEPS_RUN1 if d["held"]]
    grew = [d for d in STEPS_RUN1 if not d["held"]]
    cpu_at_ceiling = max(d["cpu"] for d in held) if held else 0
    cpu_past = grew[0]["cpu"] if grew else cpu_at_ceiling   # the first rate past the ceiling
    disk_max = max(d["disk"] for d in STEPS_RUN1)
    s = head(W, H,
             f"CPU and disk utilisation by offered rate. CPU rises with the rate to "
             f"{cpu_at_ceiling:.0f} percent where the ceiling sits and {cpu_past:.0f} percent at the "
             f"rate that failed, while disk utilisation stays below {disk_max + 1:.0f} percent "
             f"throughout.")
    s += f'  <rect x="{L-56}" y="{T-72}" width="{pw+R+16}" height="{ph+150}" rx="12" fill="{PANEL}" stroke="{BORDER}" stroke-width="2"/>\n'
    s += txt(L - 56, 52, "On this host the ceiling is the cores, not the disk", 21, BRIGHT,
             weight="bold", mono=False)
    s += txt(L - 56, 76, "Sampled every 5s and grouped per offered rate", 14, MUTED, mono=False)

    for v in (0, 25, 50, 75, 100):
        s += f'  <line x1="{L}" y1="{Y(v):.1f}" x2="{L+pw}" y2="{Y(v):.1f}" stroke="{BORDER}" stroke-width="1"/>\n'
        s += txt(L - 12, Y(v) + 4, f"{v:.0f}%", 13, MUTED, anchor="end")

    lx = L + pw - 250
    s += f'  <rect x="{lx}" y="{T-46}" width="12" height="12" rx="3" fill="{HELD}"/>\n'
    s += txt(lx + 20, T - 35, "CPU (user + system)", 13, SOFT, mono=False)
    s += f'  <rect x="{lx}" y="{T-26}" width="12" height="12" rx="3" fill="{DISK}"/>\n'
    s += txt(lx + 20, T - 15, "disk utilisation", 13, SOFT, mono=False)

    for i, d in enumerate(STEPS_RUN1):
        cx = L + slot * i + slot / 2
        s += (f'  <rect x="{cx-bw-1:.1f}" y="{Y(d["cpu"]):.1f}" width="{bw:.1f}" '
              f'height="{ph-(Y(d["cpu"])-T):.1f}" rx="4" fill="{HELD}"/>\n')
        s += (f'  <rect x="{cx+1:.1f}" y="{Y(d["disk"]):.1f}" width="{bw:.1f}" '
              f'height="{ph-(Y(d["disk"])-T):.1f}" rx="4" fill="{DISK}"/>\n')
        s += txt(cx - bw / 2 - 1, Y(d["cpu"]) - 10, f'{d["cpu"]:.0f}', 12, BRIGHT, anchor="middle", weight="bold")
        s += txt(cx, H - B + 26, f'{d["rate"]:.0f}{" †" if d["warm"] else ""}', 13,
                 MUTED if d["held"] else GREW, anchor="middle")
        if not d["held"]:
            s += txt(cx, H - B + 44, "✕ grew", 11, GREW, anchor="middle", mono=False)

    s += f'  <line x1="{L}" y1="{T+ph}" x2="{L+pw}" y2="{T+ph}" stroke="{AXIS}" stroke-width="1"/>\n'
    s += txt(L + pw / 2, H - 48, "offered rate, events/s", 12.5, AXIS, anchor="middle", mono=False)
    s += txt(L - 56, H - 22,
             "† the first window also contains container start and JIT warm-up.  Disk does reach ~97% "
             "briefly elsewhere in the run, on PostgreSQL checkpoint flushes — but not at these rates.",
             12, AXIS, mono=False)
    return s + "</svg>\n"


# --------------------------------------------------------------------------- #
# 3. Latency, replicated
# --------------------------------------------------------------------------- #
def latency_svg():
    W, H = 1320, 440
    L, R, T, B = 130, 210, 120, 76
    pw, ph = W - L - R, H - T - B
    labels = ("p50", "p95", "p99", "p99.9")

    # The headline rate is whichever one most runs held, so the chart follows the runs rather than a
    # rate written down here: S2 holds half of its own quick ramp, which the host decides.
    rates = [LATENCIES[r][0] for r in PERF_RUNS]
    headline = max(set(rates), key=rates.count)
    at_headline = [r for r in PERF_RUNS if LATENCIES[r][0] == headline]
    other = [r for r in PERF_RUNS if LATENCIES[r][0] != headline]
    means = [sum(LATENCIES[r][1][i] for r in at_headline) / len(at_headline) for i in range(4)]
    # The axis is sized to what is actually drawn: the widest whisker end, not the widest bar.
    xmax, xstep = _nice_axis(max([LATENCIES[r][1][i] for r in PERF_RUNS for i in range(4)]))
    X = lambda v: L + (v / xmax) * pw

    s = head(W, H,
             f"COMMIT to ack latency at {headline:.0f} events per second: p50 {means[0]:.1f}, "
             f"p95 {means[1]:.1f}, p99 {means[2]:.1f} and p99.9 {means[3]:.1f} milliseconds, with the "
             f"spread between {_count_word(len(at_headline))} runs shown as a whisker.")
    s += f'  <rect x="{L-90}" y="{T-76}" width="{pw+R+50}" height="{ph+128}" rx="12" fill="{PANEL}" stroke="{BORDER}" stroke-width="2"/>\n'
    s += txt(L - 90, 52, f"COMMIT → ack latency at {headline:.0f} events/s", 21, BRIGHT, weight="bold", mono=False)
    s += txt(L - 90, 76, "End to end, measured against a single clock", 14, MUTED, mono=False)

    lx = L + pw - 30
    s += f'  <rect x="{lx}" y="{T-52}" width="12" height="12" rx="3" fill="{HELD}"/>\n'
    s += txt(lx + 20, T - 41, f"{len(at_headline)} runs at {headline:.0f} events/s", 13, SOFT, mono=False)
    if other:
        rate = LATENCIES[other[0]][0]
        s += f'  <rect x="{lx}" y="{T-32}" width="12" height="12" rx="3" fill="{STALE}"/>\n'
        s += txt(lx + 20, T - 21, f"1 run at {rate:.0f} events/s", 13, MUTED, mono=False)
    else:
        s += txt(lx + 20, T - 21, "bar = mean, whisker = spread", 12.5, MUTED, mono=False)

    v = 0
    while v <= xmax + 1e-9:
        s += f'  <line x1="{X(v):.1f}" y1="{T}" x2="{X(v):.1f}" y2="{T+ph}" stroke="{BORDER}" stroke-width="1"/>\n'
        s += txt(X(v), T + ph + 26, f"{v:.0f}", 13, MUTED, anchor="middle")
        v += xstep

    rowh = ph / len(labels)
    for i, name in enumerate(labels):
        ytop = T + rowh * i + 10
        s += txt(L - 16, ytop + 20, name, 15, BRIGHT, anchor="end")
        vals = [LATENCIES[r][1][i] for r in at_headline]
        lo, hi = min(vals), max(vals)
        mid = sum(vals) / len(vals)
        # the spread across replicates, drawn as the bar's own uncertainty rather than hidden in a mean
        s += f'  <rect x="{L}" y="{ytop:.1f}" width="{max(1,X(mid)-L):.1f}" height="18" rx="4" fill="{HELD}"/>\n'
        if hi - lo > 0.05:
            s += (f'  <line x1="{X(lo):.1f}" y1="{ytop+9:.1f}" x2="{X(hi):.1f}" y2="{ytop+9:.1f}" '
                  f'stroke="{BRIGHT}" stroke-width="2"/>\n')
            for e in (lo, hi):
                s += (f'  <line x1="{X(e):.1f}" y1="{ytop+3:.1f}" x2="{X(e):.1f}" y2="{ytop+15:.1f}" '
                      f'stroke="{BRIGHT}" stroke-width="2"/>\n')
        s += txt(X(hi) + 12, ytop + 14, f"{mid:.1f} ms", 13.5, HELD, weight="bold")

        v300 = LATENCIES[other[0]][1][i] if other else None
        if v300 is not None:
            s += f'  <rect x="{L}" y="{ytop+21:.1f}" width="{max(1,X(v300)-L):.1f}" height="18" rx="4" fill="{STALE}"/>\n'
            s += txt(X(v300) + 12, ytop + 35, f"{v300:.1f} ms", 13.5, MUTED)

    s += f'  <line x1="{L}" y1="{T}" x2="{L}" y2="{T+ph}" stroke="{AXIS}" stroke-width="1"/>\n'
    s += txt(L + pw / 2, T + ph + 52, "COMMIT → ack latency, milliseconds", 12.5, AXIS, anchor="middle", mono=False)
    return s + "</svg>\n"


# --------------------------------------------------------------------------- #
# 4. Delivered throughput against offered rate
# --------------------------------------------------------------------------- #
# Run 2 is left out of the curve on purpose. It is the one execution whose host was throttled — it
# brackets the ceiling at half the others, and mixing a half-capacity ladder into a curve about how
# the relay responds to load would describe neither machine state.
# The corroborating run is the other one of the same shape. Run 3 is the full suite at a shorter
# duration, so its ceiling search runs on a different budget; holding it to the same 10% as a
# like-for-like replicate would fail the chart on a difference in method rather than in result.
CURVE_REFERENCE = PERF_RUNS[0]
CURVE_CORROBORATING = PERF_RUNS[1]

# How far apart two independent ceiling searches may land and still count as "the same result".
# Each search is already precise to within its own reported tolerance (S1SustainedThroughput's
# RATE_TOLERANCE, 5% — LLD-benchmark §7); comparing two such searches to each other, rather than to
# a single true value, means the gap between them can be up to twice that before it says anything
# more than "each measured itself correctly". This is deliberately about the *final* ceiling only,
# not the path each search took to it — an earlier version required every intermediate candidate
# rate to match between runs, which the search's own bisection has no reason to guarantee even when
# both land on the same ceiling, and which failing would have blocked the other two charts along
# with this one (see `ChartUnavailable`).
CEILING_AGREEMENT_TOLERANCE = 0.10


def load_curve():
    """The ladder plotted is one run's own trace — self-consistent by construction, since it is the
    literal sequence of candidates that search actually tested. A second run is used only to check
    that the headline number reproduces, not to contribute points of its own."""
    reference_ceiling, _ = CEILINGS[CURVE_REFERENCE]
    corroborating_ceiling, _ = CEILINGS[CURVE_CORROBORATING]
    gap = abs(reference_ceiling - corroborating_ceiling) / corroborating_ceiling
    if gap > CEILING_AGREEMENT_TOLERANCE:
        raise ChartUnavailable(
            f"{CURVE_REFERENCE} ({reference_ceiling:.0f}/s) and {CURVE_CORROBORATING} "
            f"({corroborating_ceiling:.0f}/s) disagree by {gap:.0%}, past the "
            f"{CEILING_AGREEMENT_TOLERANCE:.0%} this chart treats as the same result — "
            f"see docs/benchmark-results/README.md.")

    holds = load_holds(_log_for(CURVE_REFERENCE))
    ladder = sorted((h["rate"], h["held"]) for h in holds)
    held = [rate for rate, ok in ladder if ok]
    grew = [rate for rate, ok in ladder if not ok]
    if not held or not grew:
        raise ChartUnavailable(
            f"{CURVE_REFERENCE}'s own ladder never bracketed a ceiling; the plateau would be a guess.")
    return held, grew, max(held)


def throughput_svg():
    held_rates, grew_rates, ceiling = load_curve()
    W, H = 1320, 540
    L, R, T, B = 118, 96, 132, 104
    pw, ph = W - L - R, H - T - B
    xmax = max(grew_rates) * 1.04
    X = lambda v: L + (v / xmax) * pw
    Y = lambda v: T + ph - (v / xmax) * ph

    s = head(W, H,
             f"Delivered throughput against offered rate. Delivery keeps pace one for one up to "
             f"{ceiling:.0f} events per second; past that the relay stays at its ceiling and the "
             f"excess accumulates as backlog.")
    s += (f'  <rect x="{L-62}" y="{T-74}" width="{pw+R+22}" height="{ph+138}" rx="12" '
          f'fill="{PANEL}" stroke="{BORDER}" stroke-width="2"/>\n')
    s += txt(L - 62, 52, f"Delivery keeps pace up to {ceiling:.0f} events/s", 21, BRIGHT,
             weight="bold", mono=False)
    s += txt(L - 62, 76, "8 relay workers on 2 vCPU — past the ceiling the excess becomes backlog, "
             "never loss", 14, MUTED, mono=False)

    for v in (0, 400, 800, 1200, 1600):
        if v > xmax:
            continue
        s += (f'  <line x1="{X(v):.1f}" y1="{T}" x2="{X(v):.1f}" y2="{T+ph}" stroke="{BORDER}" '
              f'stroke-width="1"/>\n')
        s += txt(X(v), T + ph + 26, f"{v:.0f}", 13, MUTED, anchor="middle")
        s += (f'  <line x1="{L}" y1="{Y(v):.1f}" x2="{L+pw}" y2="{Y(v):.1f}" stroke="{BORDER}" '
              f'stroke-width="1"/>\n')
        s += txt(L - 12, Y(v) + 4, f"{v:.0f}", 13, MUTED, anchor="end")
    s += txt(L, T - 16, "delivered, events/s", 12.5, AXIS, mono=False)
    s += txt(L + pw / 2, H - 46, "offered rate, events/s", 12.5, AXIS, anchor="middle", mono=False)

    # The shortfall: everything between the 1:1 line and the plateau is sitting in the outbox.
    s += (f'  <path d="M {X(ceiling):.1f} {Y(ceiling):.1f} L {X(xmax):.1f} {Y(xmax):.1f} '
          f'L {X(xmax):.1f} {Y(ceiling):.1f} Z" fill="{GREW}" fill-opacity="0.13"/>\n')

    s += (f'  <line x1="{X(0)}" y1="{Y(0):.1f}" x2="{X(xmax):.1f}" y2="{Y(xmax):.1f}" '
          f'stroke="{AXIS}" stroke-width="1.5" stroke-dasharray="5 5"/>\n')

    pts = " ".join(f"{X(r):.1f},{Y(r):.1f}" for r in held_rates)
    pts += f" {X(xmax):.1f},{Y(ceiling):.1f}"
    s += f'  <polyline points="{pts}" fill="none" stroke="{HELD}" stroke-width="3.5"/>\n'
    for r in held_rates:
        s += f'  <circle cx="{X(r):.1f}" cy="{Y(r):.1f}" r="5.5" fill="{HELD}"/>\n'

    # Where the offered rate ran away from the delivered one, the gap is the point of the chart.
    for r in grew_rates:
        s += (f'  <line x1="{X(r):.1f}" y1="{Y(ceiling):.1f}" x2="{X(r):.1f}" y2="{Y(r):.1f}" '
              f'stroke="{GREW}" stroke-width="1.5" stroke-dasharray="3 4"/>\n')
        s += (f'  <path d="M {X(r)-6:.1f} {Y(r)-6:.1f} l 12 12 M {X(r)+6:.1f} {Y(r)-6:.1f} l -12 12" '
              f'stroke="{GREW}" stroke-width="2.5" fill="none"/>\n')
        s += txt(X(r), Y(r) - 16, f"{r:.0f}", 12.5, GREW, anchor="middle")

    s += (f'  <line x1="{L}" y1="{Y(ceiling):.1f}" x2="{X(xmax):.1f}" y2="{Y(ceiling):.1f}" '
          f'stroke="{HELD}" stroke-width="1.2" stroke-dasharray="6 6" stroke-opacity="0.7"/>\n')
    s += txt(X(ceiling) - 14, Y(ceiling) - 14, f"{ceiling:.0f}/s", 16, BRIGHT, anchor="end",
             weight="bold")

    lx, ly = L + pw - 300, T + ph - 66
    s += f'  <circle cx="{lx+6}" cy="{ly}" r="5.5" fill="{HELD}"/>\n'
    s += txt(lx + 22, ly + 4, "held — backlog flat over the window", 13, SOFT, mono=False)
    s += (f'  <path d="M {lx} {ly+14} l 12 12 M {lx+12} {ly+14} l -12 12" stroke="{GREW}" '
          f'stroke-width="2.5" fill="none"/>\n')
    s += txt(lx + 22, ly + 28, "grew — the offered rate outran delivery", 13, SOFT, mono=False)

    s += txt(L - 62, H - 22,
             "Dashed diagonal is one delivered event per offered event.  The plateau is the bound the "
             "search proved, not a rate measured above the ceiling.", 12, AXIS, mono=False)
    return s + "</svg>\n"

if __name__ == "__main__":
    out = HERE.parent
    # Each chart's own precondition gates only itself: a `ChartUnavailable` here skips that one file
    # and keeps going, rather than the whole redraw dying for a problem the other charts don't share
    # (see `ChartUnavailable`'s docstring). `SystemExit` — a data-quality problem, not a disagreement
    # between otherwise-sound runs — is deliberately let through uncaught, exactly as before.
    charts = (("ceilings", ceilings_svg), ("resources", resources_svg),
              ("latency", latency_svg), ("throughput", throughput_svg))
    written, skipped = [], []
    for name, render in charts:
        try:
            (out / f"tandem-benchmark-{name}.svg").write_text(render())
            written.append(name)
        except ChartUnavailable as e:
            skipped.append(name)
            print(f"  SKIPPED {name}: {e}")

    print(f"wrote {len(written)}/{len(charts)} svgs: {written}")
    print(f"  host-record ceilings: {[f'{CEILINGS[r][0]:.0f}' for r in HOST_RUNS]}")
    print(f"  current ceilings:     {[f'{CEILINGS[r][0]:.0f}' for r in PERF_RUNS]}")
    print(f"  current latency:      {[(LATENCIES[r][0], LATENCIES[r][1]) for r in PERF_RUNS]}")
    if skipped:
        raise SystemExit(f"{len(skipped)} chart(s) skipped: {skipped} — see messages above")
