#!/usr/bin/env python3
"""Visualize Pulse scheduler benchmark results.

Reads the CSV produced by ``pulse_bench`` and renders:

  * a self-contained HTML report with inline SVG bar charts (no dependencies,
    always produced), and
  * optional PNG charts via Matplotlib when it is installed.

Usage:
    python3 scripts/plot_results.py [results.csv] [--out report_dir]
                                    [--workers N] [--scaling WORKLOAD]
                                    [--matplotlib]

The default inputs/outputs:
    results.csv  = data/pulse_results.csv
    report_dir   = data/report
"""

from __future__ import annotations

import argparse
import csv
import html
import os
import statistics
import sys
from collections import defaultdict

POLICIES = ["fifo", "work-stealing", "critical-path", "locality-aware"]
POLICY_COLORS = {
    "fifo": "#4e79a7",
    "work-stealing": "#f28e2b",
    "critical-path": "#e15759",
    "locality-aware": "#59a14f",
}
WORKLOAD_ORDER = [
    "chain-1k",
    "forkjoin-d10",
    "layered-20x100",
    "random-5k-3",
    "dataflow-8k",
    "straggler",
    "contended",
]

NUMERIC = [
    "makespan_ns",
    "total_work_ns",
    "throughput",
    "speedup",
    "utilization",
    "load_imbalance",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "p99_wait_ns",
    "steals",
    "migrations",
]


def load_rows(path):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for raw in reader:
            row = {"workload": raw["workload"], "policy": raw["policy"]}
            row["workers"] = int(raw["workers"])
            for k in NUMERIC:
                row[k] = float(raw[k])
            rows.append(row)
    return rows


def aggregate(rows):
    """Mean of every numeric field per (workload, policy, workers)."""
    buckets = defaultdict(lambda: defaultdict(list))
    for r in rows:
        key = (r["workload"], r["policy"], r["workers"])
        for k in NUMERIC:
            buckets[key][k].append(r[k])
    agg = {}
    for key, fields in buckets.items():
        agg[key] = {k: statistics.fmean(v) for k, v in fields.items()}
    return agg


# --- tiny dependency-free SVG bar chart --------------------------------------

def _fmt(v, unit):
    if unit == "ms":
        return f"{v:.2f}"
    if unit == "s":
        return f"{v:.1f}"
    if unit == "":
        if v >= 1000:
            return f"{v/1000:.1f}k"
        return f"{v:.0f}"
    return f"{v:g}"


def bar_chart_svg(title, groups, series, values, unit="", note=""):
    """Grouped bar chart. values[(group, series)] -> float."""
    bar_w, series_gap, group_gap = 18, 2, 26
    left, top, right, bottom = 78, 42, 16, 92
    plot_h = 300
    n_series = len(series)
    group_w = n_series * bar_w + max(0, n_series - 1) * series_gap
    plot_w = max(240, len(groups) * (group_w + group_gap))
    width = left + plot_w + right
    height = top + plot_h + bottom

    vmax = 0.0
    for g in groups:
        for s in series:
            vmax = max(vmax, values.get((g, s), 0.0))
    vmax = vmax * 1.15 if vmax > 0 else 1.0

    def y(v):
        return top + plot_h - (v / vmax) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'font-family="ui-monospace,Menlo,Consolas,monospace" font-size="11">',
        f'<text x="{left}" y="20" font-size="14" font-weight="700">{html.escape(title)}</text>',
    ]
    if note:
        parts.append(f'<text x="{left}" y="35" fill="#666">{html.escape(note)}</text>')

    # gridlines + y labels
    for i in range(5):
        frac = i / 4.0
        val = vmax * frac
        yy = y(val)
        parts.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{left+plot_w}" y2="{yy:.1f}" '
                     f'stroke="#e5e5e5" stroke-width="1"/>')
        parts.append(f'<text x="{left-6}" y="{yy+4:.1f}" text-anchor="end" fill="#888">'
                     f'{_fmt(val, unit)}</text>')

    # bars
    for gi, g in enumerate(groups):
        gx = left + gi * (group_w + group_gap) + group_gap / 2
        for si, s in enumerate(series):
            v = values.get((g, s), 0.0)
            bx = gx + si * (bar_w + series_gap)
            by = y(v)
            bh = top + plot_h - by
            color = POLICY_COLORS.get(s, "#888")
            parts.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w}" height="{bh:.1f}" '
                         f'fill="{color}"><title>{html.escape(g)} / {html.escape(s)}: '
                         f'{_fmt(v, unit)}</title></rect>')
        # x label (rotated if many groups)
        cx = gx + group_w / 2
        rot = -35 if len(groups) > 4 else 0
        label = html.escape(g)
        if rot:
            parts.append(f'<text x="{cx:.1f}" y="{top+plot_h+14}" text-anchor="end" '
                         f'transform="rotate({rot} {cx:.1f} {top+plot_h+14})">{label}</text>')
        else:
            parts.append(f'<text x="{cx:.1f}" y="{top+plot_h+16}" text-anchor="middle">{label}</text>')

    # legend
    lx = left
    ly = height - 18
    for s in series:
        parts.append(f'<rect x="{lx}" y="{ly-9}" width="11" height="11" fill="{POLICY_COLORS.get(s,"#888")}"/>')
        parts.append(f'<text x="{lx+15}" y="{ly}" fill="#333">{html.escape(s)}</text>')
        lx += 18 + 7 * len(s) + 22

    parts.append("</svg>")
    return "\n".join(parts)


def ordered_workloads(rows):
    present = {r["workload"] for r in rows}
    ordered = [w for w in WORKLOAD_ORDER if w in present]
    ordered += sorted(present - set(ordered))
    return ordered


def build_report(rows, agg, out_dir, workers, scaling_workload):
    os.makedirs(out_dir, exist_ok=True)
    loads = ordered_workloads(rows)
    worker_counts = sorted({r["workers"] for r in rows})
    wcount = workers if workers in worker_counts else max(worker_counts)

    def chart(metric, title, scale, unit, note=""):
        values = {}
        for w in loads:
            for p in POLICIES:
                cell = agg.get((w, p, wcount))
                if cell is not None:
                    values[(w, p)] = cell[metric] * scale
        return bar_chart_svg(title, loads, POLICIES, values, unit, note)

    charts = [
        chart("makespan_ns", f"Makespan by workload ({wcount} workers)", 1e-6, "ms",
              "lower is better"),
        chart("throughput", "Throughput by workload", 1.0, "", "higher is better"),
        chart("p99_ns", "p99 task latency by workload", 1e-6, "ms",
              "lower is better (tail latency)"),
        chart("utilization", "CPU utilization by workload", 1.0, "",
              "higher is better; 1.0 = all cores busy"),
        chart("load_imbalance", "Load imbalance (std-dev / mean busy)", 1.0, "",
              "lower is better"),
        chart("steals", "Successful work-steals per run", 1.0, "",
              "locality-aware should steal far less"),
    ]

    # scaling chart: makespan vs worker count for one workload
    scaling_svg = ""
    if scaling_workload:
        values = {}
        for wc in worker_counts:
            for p in POLICIES:
                cell = agg.get((scaling_workload, p, wc))
                if cell is not None:
                    values[(str(wc), p)] = cell["makespan_ns"] * 1e-6
        if values:
            scaling_svg = bar_chart_svg(
                f"Scaling: {scaling_workload} makespan vs workers",
                [str(wc) for wc in worker_counts], POLICIES, values, "ms",
                "lower is better")

    # summary table
    table_rows = []
    for w in loads:
        for p in POLICIES:
            cell = agg.get((w, p, wcount))
            if cell is None:
                continue
            table_rows.append(
                f"<tr><td>{html.escape(w)}</td><td>{html.escape(p)}</td>"
                f"<td>{cell['makespan_ns']*1e-6:.3f}</td>"
                f"<td>{cell['throughput']:.0f}</td>"
                f"<td>{cell['utilization']:.3f}</td>"
                f"<td>{cell['p50_ns']*1e-6:.3f}</td>"
                f"<td>{cell['p95_ns']*1e-6:.3f}</td>"
                f"<td>{cell['p99_ns']*1e-6:.3f}</td>"
                f"<td>{cell['steals']:.0f}</td></tr>")

    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Pulse scheduler benchmark report</title>
<style>
 body {{ font-family: system-ui, sans-serif; margin: 0; background:#fafafa; color:#1a1a1a; }}
 header {{ padding: 24px 32px; background:#1a1a1a; color:#fff; }}
 header h1 {{ margin:0 0 4px; font-size:20px; }}
 header .sub {{ color:#bbb; font-size:13px; }}
 main {{ padding: 16px 32px 48px; }}
 section {{ background:#fff; border:1px solid #e5e5e5; border-radius:8px; padding:16px; margin:16px 0; }}
 h2 {{ font-size:15px; margin:0 0 12px; }}
 table {{ border-collapse: collapse; width:100%; font-size:12px; }}
 th,td {{ border-bottom:1px solid #eee; padding:5px 8px; text-align:right; }}
 th:first-child,td:first-child,th:nth-child(2),td:nth-child(2) {{ text-align:left; }}
 thead th {{ background:#f4f4f4; }}
 .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(430px,1fr)); gap:8px; }}
</style></head>
<body>
<header>
  <h1>Pulse &mdash; parallel scheduler benchmark report</h1>
  <div class="sub">workloads: {len(loads)} &nbsp;|&nbsp; policies: {len(POLICIES)}
   &nbsp;|&nbsp; workers shown: {wcount} &nbsp;|&nbsp; worker counts: {worker_counts}</div>
</header>
<main>
  <div class="grid">
    {''.join(f'<section>{c}</section>' for c in charts)}
    {f'<section>{scaling_svg}</section>' if scaling_svg else ''}
  </div>
  <section>
    <h2>Aggregate table ({wcount} workers)</h2>
    <table>
      <thead><tr><th>workload</th><th>policy</th><th>makespan&nbsp;(ms)</th>
      <th>tput&nbsp;(/s)</th><th>util</th><th>p50&nbsp;(ms)</th><th>p95&nbsp;(ms)</th>
      <th>p99&nbsp;(ms)</th><th>steals</th></tr></thead>
      <tbody>{''.join(table_rows)}</tbody>
    </table>
  </section>
</main>
</body></html>
"""
    report_path = os.path.join(out_dir, "report.html")
    with open(report_path, "w") as f:
        f.write(doc)
    return report_path, loads, worker_counts


def try_matplotlib(rows, agg, out_dir, workers, loads, worker_counts):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:  # noqa: BLE001
        print(f"matplotlib not available ({e}); skipping PNG output")
        return []

    written = []
    wcount = workers if workers in worker_counts else max(worker_counts)

    def grouped(metric, scale, ylabel, fname, title):
        fig, ax = plt.subplots(figsize=(max(7, len(loads) * 1.1), 4.4))
        n = len(POLICIES)
        idx = range(len(loads))
        width = 0.8 / n
        for i, p in enumerate(POLICIES):
            vals = [agg.get((w, p, wcount), {}).get(metric, 0.0) * scale for w in loads]
            xs = [x + i * width - 0.4 + width / 2 for x in idx]
            ax.bar(xs, vals, width=width, label=p, color=POLICY_COLORS[p])
        ax.set_xticks(list(idx))
        ax.set_xticklabels(loads, rotation=30, ha="right")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        path = os.path.join(out_dir, fname)
        fig.savefig(path, dpi=130)
        plt.close(fig)
        written.append(path)

    grouped("makespan_ns", 1e-6, "makespan (ms)", "makespan.png",
            f"Makespan by workload ({wcount} workers)")
    grouped("p99_ns", 1e-6, "p99 latency (ms)", "p99.png",
            f"p99 tail latency by workload ({wcount} workers)")
    grouped("utilization", 1.0, "utilization", "utilization.png",
            f"CPU utilization by workload ({wcount} workers)")
    grouped("steals", 1.0, "steals", "steals.png",
            f"Work-steals per run ({wcount} workers)")
    return written


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", nargs="?", default="data/pulse_results.csv")
    ap.add_argument("--out", default="data/report")
    ap.add_argument("--workers", type=int, default=0,
                    help="worker count to highlight (default: max present)")
    ap.add_argument("--scaling", default="dataflow-8k",
                    help="workload to chart across worker counts")
    ap.add_argument("--matplotlib", action="store_true",
                    help="also emit PNG charts if matplotlib is installed")
    args = ap.parse_args(argv)

    if not os.path.exists(args.csv):
        print(f"error: {args.csv} not found; run the benchmark first", file=sys.stderr)
        return 1

    rows = load_rows(args.csv)
    if not rows:
        print("error: no rows in CSV", file=sys.stderr)
        return 1
    agg = aggregate(rows)
    report_path, loads, worker_counts = build_report(
        rows, agg, args.out, args.workers, args.scaling)
    print(f"wrote {report_path}")

    if args.matplotlib:
        for p in try_matplotlib(rows, agg, args.out, args.workers, loads, worker_counts):
            print(f"wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
