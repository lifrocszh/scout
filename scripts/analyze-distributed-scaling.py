#!/usr/bin/env python3
"""Aggregate a Kubernetes scaling benchmark (benchmark-distributed-kubernetes.py) into tables and a chart.

    analyze-distributed-scaling.py results.json [--markdown] [--svg chart.svg]

Everything printed or drawn is computed from results.json; nothing is typed in. Python stdlib only.
"""

import argparse
import json
import math
import pathlib
import statistics

# The default palette (dataviz reference): categorical slots 1-4, light and dark, and the chart chrome.
SERIES = {
    "light": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"],
    "dark": ["#3987e5", "#d95926", "#199e70", "#c98500"],
}
CHROME = {
    "light": {
        "surface": "#fcfcfb",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
    },
    "dark": {
        "surface": "#1a1a19",
        "ink": "#f0efec",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
    },
}
ROLES = [
    ("coordinator", "coordinator"),
    ("workers_total", "workers (all)"),
    ("postgres", "postgres"),
    ("fixture", "origin pod"),
]
FLATTEN_BELOW = 1.3  # doubling the workers must buy at least this much throughput


def mean(values):
    values = [v for v in values if v is not None]
    return statistics.fmean(values) if values else None


def aggregate(results):
    """One row per worker count, in ascending order."""
    runs = results["runs"]
    rows = []
    for workers in sorted({run["workers"] for run in runs}):
        group = [run for run in runs if run["workers"] == workers]
        rates = [run["pages_per_second"] for run in group if run["pages_per_second"] is not None]
        rows.append(
            {
                "workers": workers,
                "runs": len(group),
                "rates": rates,
                "pages_per_second": mean(rates),
                "min": min(rates, default=None),
                "max": max(rates, default=None),
                "run_pages_per_second": mean(run["run_pages_per_second"] for run in group),
                "p50_ms": mean(run["task_latency"].get("p50_ms") for run in group),
                "p95_ms": mean(run["task_latency"].get("p95_ms") for run in group),
                "pool_mean_wait_ms": mean(
                    run.get("db_pool", {}).get("mean_wait_ms") for run in group
                ),
                "pool_p95_wait_ms": mean(
                    run.get("db_pool", {}).get("p95_wait_ms") for run in group
                ),
                "pool_connections": mean(
                    run.get("db_pool", {}).get("connections") for run in group
                ),
                "pool_max_in_use": max(
                    (run.get("db_pool", {}).get("max_in_use_sampled") or 0 for run in group),
                    default=None,
                ),
                "cpu": {
                    key: mean(run["cpu_cores"].get(key) for run in group)
                    for key in ("coordinator", "postgres", "fixture", "workers_total", "all_roles")
                },
                "window_seconds": mean(run["window_seconds"] for run in group),
                "warnings": sorted({w for run in group for w in run["warnings"]}),
            }
        )
    base = rows[0]["pages_per_second"] if rows and rows[0]["workers"] == 1 else None
    for row in rows:
        row["speedup"] = (
            row["pages_per_second"] / base if base and row["pages_per_second"] else None
        )
        row["efficiency"] = row["speedup"] / row["workers"] if row["speedup"] else None
    return rows


def flattening(rows):
    """The first step where doubling workers buys less than FLATTEN_BELOW x, or None."""
    for before, after in zip(rows, rows[1:]):
        if not before["pages_per_second"] or not after["pages_per_second"]:
            continue
        ratio = after["pages_per_second"] / before["pages_per_second"]
        step = after["workers"] / before["workers"]
        # Normalise to a doubling, so 1 -> 2 and 4 -> 8 compare alike.
        per_doubling = ratio ** (1 / math.log2(step)) if step > 1 else ratio
        if per_doubling < FLATTEN_BELOW:
            return {
                "after_workers": before["workers"],
                "to_workers": after["workers"],
                "ratio": ratio,
                "per_doubling": per_doubling,
            }
    return None


def fmt(value, digits=1, unit=""):
    return "n/a" if value is None else f"{value:,.{digits}f}{unit}"


def markdown(results, rows):
    env = results.get("environment") or {}
    node = env.get("node", {})
    out = []
    out.append(
        f"Node: {node.get('cpus', '?')} vCPU, {node.get('memory', '?')} memory, kubelet {node.get('kubelet', '?')}. "
        f"Page budget {env.get('parameters', {}).get('pages', '?')} per run, "
        f"{env.get('parameters', {}).get('origins', '?')} origins, "
        f"{env.get('parameters', {}).get('latency_ms', '?')} ms per page.\n"
    )
    out.append(
        "| workers | runs | pages/s (mean) | min-max | speedup | efficiency | task p50 | task p95 |"
    )
    out.append("|--:|--:|--:|--:|--:|--:|--:|--:|")
    for row in rows:
        out.append(
            f"| {row['workers']} | {row['runs']} | {fmt(row['pages_per_second'])} | "
            f"{fmt(row['min'])}-{fmt(row['max'])} | {fmt(row['speedup'], 2, 'x')} | "
            f"{fmt((row['efficiency'] or 0) * 100 if row['efficiency'] else None, 0, '%')} | "
            f"{fmt(row['p50_ms'], 0, ' ms')} | {fmt(row['p95_ms'], 0, ' ms')} |"
        )
    out.append(
        "\n| workers | DB pool wait (mean) | DB pool wait (p95) | pool size | max in use | coordinator CPU | postgres CPU | workers CPU | origin pod CPU | all roles |"
    )
    out.append("|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for row in rows:
        c = row["cpu"]
        out.append(
            f"| {row['workers']} | {fmt(row['pool_mean_wait_ms'], 3, ' ms')} | {fmt(row['pool_p95_wait_ms'], 2, ' ms')} | "
            f"{fmt(row['pool_connections'], 0)} | {fmt(row['pool_max_in_use'], 0)} | "
            f"{fmt(c['coordinator'], 2, ' cores')} | {fmt(c['postgres'], 2, ' cores')} | "
            f"{fmt(c['workers_total'], 2, ' cores')} | {fmt(c['fixture'], 2, ' cores')} | {fmt(c['all_roles'], 2, ' cores')} |"
        )
    out.extend(diagnostics_markdown(results))
    flat = flattening(rows)
    if flat:
        out.append(
            f"\nThe curve flattens after {flat['after_workers']} workers: going to {flat['to_workers']} multiplies "
            f"throughput by {flat['ratio']:.2f} (per doubling {flat['per_doubling']:.2f}, below {FLATTEN_BELOW})."
        )
    else:
        out.append(
            f"\nEvery doubling of workers bought at least {FLATTEN_BELOW}x: the curve has not flattened in this range."
        )
    for row in rows:
        for warning in row["warnings"]:
            out.append(f"\n- warning at {row['workers']} workers: {warning}")
    return "\n".join(out)


def diagnostics_markdown(results, top=6):
    """Tables from the diagnostic runs: where the coordinator and Postgres spent the window."""
    runs = sorted(results.get("diagnostics") or [], key=lambda run: run["workers"])
    if not runs:
        return []
    out = [
        "\n#### Diagnostic runs (RPC timing on, so slower than the headline runs)\n",
        "| workers | pages/s | frontier pending (mean / min) | frontier leased (mean) | Postgres commits/s | active backends (mean) | idle in transaction (mean) |",
        "|--:|--:|--:|--:|--:|--:|--:|",
    ]
    for run in runs:
        frontier, postgres = run.get("frontier") or {}, run.get("postgres") or {}
        waits = postgres.get("backends_by_state_and_wait") or {}
        active = sum(v for k, v in waits.items() if k.startswith("active:"))
        idle = sum(v for k, v in waits.items() if k.startswith("idle in transaction:"))
        out.append(
            f"| {run['workers']} | {fmt(run['pages_per_second'])} | {fmt(frontier.get('pending_mean'), 0)} / {fmt(frontier.get('pending_min'), 0)} | "
            f"{fmt(frontier.get('leased_mean'), 1)} | {fmt(postgres.get('commits_per_second'), 0)} | {fmt(active, 2)} | {fmt(idle, 2)} |"
        )
    for run in runs:
        waits = (run.get("postgres") or {}).get("backends_by_state_and_wait") or {}
        if waits:
            ranked = ", ".join(f"`{k}` {v:.2f}" for k, v in list(waits.items())[:5])
            out.append(
                f"\n{run['workers']} workers, Postgres backends by state and wait event (mean per sample): {ranked}"
            )
    for run in runs:
        operations = run.get("rpc_timings") or {}
        if not operations:
            continue
        out.append(
            f"\n{run['workers']} workers, coordinator RPCs by occupancy (service time per second of window; 1.0 is one core-equivalent):\n"
        )
        out.append(
            "| RPC | calls/s | service p50 | service p95 | server total p95 | core-lock wait p95 | DB-pool wait p95 | occupancy |"
        )
        out.append("|--|--:|--:|--:|--:|--:|--:|--:|")
        for name, op in sorted(operations.items(), key=lambda item: -item[1]["occupancy"])[:top]:
            out.append(
                f"| {name} | {fmt(op['calls_per_second'])} | {fmt(op['service'].get('p50_ms'), 2, ' ms')} | {fmt(op['service'].get('p95_ms'), 2, ' ms')} | "
                f"{fmt(op['server_total'].get('p95_ms'), 2, ' ms')} | {fmt(op['core_lock_wait'].get('p95_ms'), 2, ' ms')} | "
                f"{fmt(op['db_pool_wait'].get('p95_ms'), 3, ' ms')} | {fmt(op['occupancy'], 2)} |"
            )
    return out


# ---- chart ---------------------------------------------------------------------------------------
def nice_axis(peak, max_ticks=6):
    """(step, top) of a zero-based axis with at most max_ticks steps and round tick values."""
    magnitude = 10 ** math.floor(math.log10(peak / max_ticks))
    for factor in (1, 2, 2.5, 5, 10, 20):
        step = magnitude * factor
        if math.ceil(peak / step) <= max_ticks:
            return step, math.ceil(peak / step) * step
    return peak, peak


def svg(results, rows):
    """Two panels: throughput against the linear ideal, and CPU by role. Light and dark via CSS."""
    width, height = 980, 440
    node_cpus = (results.get("environment") or {}).get("node", {}).get("cpus")
    node_cpus = int(node_cpus) if node_cpus else None
    xs = [row["workers"] for row in rows]
    left = {"x0": 64, "x1": 450, "y0": 88, "y1": 352}
    right = {"x0": 570, "x1": 930, "y0": 88, "y1": 352}

    def scale_x(panel, workers):
        return (
            panel["x0"]
            + (workers - 0.5) / (max(xs) + 0.5 - 0.5) * (panel["x1"] - panel["x0"]) * 0.94
        )

    def scale_y(panel, value, top):
        return panel["y1"] - value / top * (panel["y1"] - panel["y0"])

    base = rows[0]["pages_per_second"] if rows and rows[0]["workers"] == 1 else None
    ideal_top = (base or 0) * max(xs)
    peak = max([row["max"] or 0 for row in rows] + [ideal_top])
    step_a, top_a = nice_axis(peak)
    cpu_peak = max([row["cpu"]["all_roles"] or 0 for row in rows] + [node_cpus or 0, 1])
    step_b, top_b = nice_axis(cpu_peak)

    css = ""
    for mode in ("light", "dark"):
        palette = {**{f"s{i + 1}": color for i, color in enumerate(SERIES[mode])}, **CHROME[mode]}
        block = "".join(f"--{key}:{value};" for key, value in palette.items())
        if mode == "light":
            css += f":root{{color-scheme:light dark;{block}}}"
        else:
            css += f"@media (prefers-color-scheme:dark){{:root{{{block}}}}}"
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-labelledby="t d" '
        f'font-family="system-ui,-apple-system,Segoe UI,sans-serif" font-size="12">',
        '<title id="t">Scout crawl throughput against worker count, with the CPU each role used</title>',
        f'<desc id="d">Measured pages per second for {", ".join(str(x) for x in xs)} workers against linear scaling, and cores used by '
        "the coordinator, the workers, Postgres and the origin pod. The same numbers are in the report's tables.</desc>",
        f"<style>{css}text{{fill:var(--ink2)}}.t{{fill:var(--ink);font-size:14px;font-weight:600}}.m{{fill:var(--muted)}}"
        ".g{stroke:var(--grid);stroke-width:1}.a{stroke:var(--axis);stroke-width:1}"
        ".l{fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}"
        ".dot{stroke:var(--surface);stroke-width:2}</style>",
        f'<rect width="{width}" height="{height}" fill="var(--surface)"/>',
    ]

    def axes(panel, top, step, label, fmt_tick, xtitle="workers"):
        value = 0
        while value <= top + 1e-9:
            y = scale_y(panel, value, top)
            out.append(
                f'<line class="{"a" if value == 0 else "g"}" x1="{panel["x0"]}" x2="{panel["x1"]}" y1="{y:.1f}" y2="{y:.1f}"/>'
            )
            out.append(
                f'<text class="m" x="{panel["x0"] - 8}" y="{y + 4:.1f}" text-anchor="end">{fmt_tick(value)}</text>'
            )
            value += step
        for workers in xs:
            out.append(
                f'<text class="m" x="{scale_x(panel, workers):.1f}" y="{panel["y1"] + 20}" text-anchor="middle">{workers}</text>'
            )
        out.append(
            f'<text class="m" x="{(panel["x0"] + panel["x1"]) / 2:.0f}" y="{panel["y1"] + 40}" text-anchor="middle">{xtitle}</text>'
        )
        out.append(f'<text class="t" x="{panel["x0"]}" y="{panel["y0"] - 56}">{label}</text>')

    axes(left, top_a, step_a, "Pages per second", lambda v: f"{v:g}")
    if base:
        out.append(
            f'<line class="l" stroke="var(--muted)" x1="{scale_x(left, 1):.1f}" y1="{scale_y(left, base, top_a):.1f}" '
            f'x2="{scale_x(left, max(xs)):.1f}" y2="{scale_y(left, ideal_top, top_a):.1f}"/>'
        )
        out.append(
            f'<text class="m" x="{scale_x(left, max(xs)) - 6:.1f}" y="{scale_y(left, ideal_top, top_a) - 8:.1f}" text-anchor="end">linear scaling</text>'
        )
    points = [
        (scale_x(left, r["workers"]), scale_y(left, r["pages_per_second"], top_a))
        for r in rows
        if r["pages_per_second"]
    ]
    for row in rows:
        if row["pages_per_second"] is None:
            continue
        x = scale_x(left, row["workers"])
        out.append(
            f'<line stroke="var(--s1)" stroke-width="1.5" x1="{x:.1f}" x2="{x:.1f}" y1="{scale_y(left, row["min"], top_a):.1f}" y2="{scale_y(left, row["max"], top_a):.1f}"/>'
        )
    out.append(
        '<polyline class="l" stroke="var(--s1)" points="'
        + " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        + '"/>'
    )
    for row in rows:
        if row["pages_per_second"] is None:
            continue
        x, y = scale_x(left, row["workers"]), scale_y(left, row["pages_per_second"], top_a)
        out.append(
            f'<circle class="dot" cx="{x:.1f}" cy="{y:.1f}" r="5" fill="var(--s1)"><title>{row["workers"]} workers: {row["pages_per_second"]:.1f} pages/s '
            f"(min {row['min']:.1f}, max {row['max']:.1f}, {row['runs']} runs)</title></circle>"
        )
    # Every point carries its value, below the point: the measured line stays under the ideal one.
    for row in rows:
        if row["pages_per_second"] is None:
            continue
        x, y = scale_x(left, row["workers"]), scale_y(left, row["pages_per_second"], top_a)
        # The last label starts at its point and runs right, clear of the line that arrives from the left.
        last = row is rows[-1]
        out.append(
            f'<text x="{x - 8 if last else x:.1f}" y="{y + 22:.1f}" text-anchor="{"start" if last else "middle"}" '
            f'style="fill:var(--ink)">{row["pages_per_second"]:.1f}{" pages/s" if last else ""}</text>'
        )
    out.append(
        f'<text class="m" x="{left["x0"]}" y="{height - 14}">mean of {rows[0]["runs"]} runs per point; the bar is the min-max range</text>'
    )

    axes(right, top_b, step_b, "CPU cores used, by role", lambda v: f"{v:g}")
    for index, (key, name) in enumerate(ROLES):
        color = f"var(--s{index + 1})"
        pts = [
            (scale_x(right, r["workers"]), scale_y(right, r["cpu"][key], top_b))
            for r in rows
            if r["cpu"].get(key) is not None
        ]
        if not pts:
            continue
        out.append(
            f'<polyline class="l" stroke="{color}" points="'
            + " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            + '"/>'
        )
        for row in rows:
            if row["cpu"].get(key) is None:
                continue
            out.append(
                f'<circle class="dot" cx="{scale_x(right, row["workers"]):.1f}" cy="{scale_y(right, row["cpu"][key], top_b):.1f}" r="4" fill="{color}">'
                f"<title>{name}, {row['workers']} workers: {row['cpu'][key]:.2f} cores</title></circle>"
            )
        column, row = index % 2, index // 2
        lx, ly = right["x0"] + column * 150, right["y0"] - 34 + row * 18
        out.append(
            f'<line x1="{lx}" x2="{lx + 18}" y1="{ly - 4}" y2="{ly - 4}" stroke="{color}" stroke-width="2" stroke-linecap="round"/>'
        )
        out.append(f'<text x="{lx + 24}" y="{ly}">{name}</text>')
    if node_cpus:
        y = scale_y(right, node_cpus, top_b)
        out.append(
            f'<line class="l" stroke="var(--muted)" x1="{right["x0"]}" x2="{right["x1"]}" y1="{y:.1f}" y2="{y:.1f}"/>'
        )
        out.append(
            f'<text class="m" x="{right["x1"]}" y="{y + 15:.1f}" text-anchor="end">node capacity: {node_cpus} vCPU</text>'
        )
    out.append("</svg>")
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--markdown", action="store_true", help="print the tables")
    parser.add_argument("--svg", type=pathlib.Path, help="write the chart here")
    args = parser.parse_args()
    results = json.loads(args.results.read_text())
    rows = aggregate(results)
    if args.markdown:
        print(markdown(results, rows))
    if args.svg:
        args.svg.write_text(svg(results, rows))
    if not args.markdown and not args.svg:
        print(json.dumps({"rows": rows, "flattening": flattening(rows)}, indent=2))


if __name__ == "__main__":
    main()
