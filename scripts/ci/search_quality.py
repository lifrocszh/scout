#!/usr/bin/env python3
"""Helpers for the search-quality regression gate (scripts/ci/search-quality.sh).

The metrics themselves come from `scout evaluate` (src/evaluation); this script
only shapes its input and output.

  package   build the `scout evaluate` package from the committed labels
  summarize condense an evaluation report into results.json
  compare   compare results.json with the baseline; exit 1 on a regression
  baseline  write results.json over data/eval/baseline.json

Standard library only, so the gate needs nothing beyond Python 3.
"""
import argparse
import json
import os
import sys
from pathlib import Path

SCHEMA = "scout.search-quality.v1"
MODES = ("keyword", "semantic", "hybrid")
METRICS = ("ndcg_at_10", "recall_at_10", "mrr_at_10")
LABELS = {"ndcg_at_10": "NDCG@10", "recall_at_10": "Recall@10", "mrr_at_10": "MRR"}
DEFAULT_MAX_DROP = 0.02
# Metrics are rounded to 4 places, so allow for float noise at the boundary: a
# drop of exactly the threshold passes.
EPSILON = 1e-9
EVAL_DIR = Path(__file__).resolve().parents[2] / "data" / "eval"
BASELINE = EVAL_DIR / "baseline.json"


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def fail(message):
    print(f"search-quality: {message}", file=sys.stderr)
    sys.exit(1)


def admitted_pages(crawl_log):
    """Final URL -> Page ID for every page the crawl admitted."""
    pages = {}
    for line in Path(crawl_log).read_text().splitlines():
        if '"page.admitted"' not in line:
            continue
        event = json.loads(line)
        if event.get("event") == "page.admitted":
            pages[event["data"]["final_url"]] = event["data"]["page_id"]
    return pages


def cmd_package(args):
    """Resolve path-based labels to Page IDs and write the evaluation package.

    Labels are keyed by URL path so a person can read and edit them; the Page ID
    is whatever the crawl assigned to that URL. A label that points at a page the
    crawl did not admit is an error, not a silent grade-0 page.
    """
    pages = admitted_pages(args.crawl_log)
    judgments, missing = [], set()
    for row in read_jsonl(EVAL_DIR / "judgments.jsonl"):
        page_id = pages.get(args.origin + row["path"])
        if page_id is None:
            missing.add(row["path"])
            continue
        judgments.append({"query_id": row["query_id"], "page_id": page_id, "grade": row["grade"]})
    if missing:
        fail(f"judged pages missing from the crawl: {', '.join(sorted(missing))}")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "queries.jsonl").write_text((EVAL_DIR / "queries.jsonl").read_text())
    (out / "judgments.jsonl").write_text("".join(json.dumps(row) + "\n" for row in judgments))
    metadata = json.loads((EVAL_DIR / "metadata.json").read_text())
    metadata.update(
        corpus_snapshot_id=args.snapshot,
        build_id=args.generation,
        model_digest=args.model_digest,
    )
    write_json(out / "metadata.json", metadata)
    print(f"package: {len(pages)} pages crawled, {len(judgments)} judgments -> {out}")


def summarize(report):
    modes = {
        mode: {metric: round(report["metrics"][mode][metric], 4) for metric in METRICS}
        for mode in MODES
        if mode in report["metrics"]
    }
    return {"schema_version": SCHEMA, "query_count": report["query_count"], "modes": modes}


def cmd_summarize(args):
    results = summarize(json.loads(Path(args.report).read_text()))
    write_json(args.out, results)
    print(json.dumps(results, indent=2, sort_keys=True))


def compare(baseline, results, max_drop, modes=None):
    """Return (rows, problems). Each row is (mode, metric, baseline, current, status)."""
    modes = [m for m in MODES if modes is None or m in modes]
    rows, problems = [], []
    if baseline["query_count"] != results["query_count"]:
        problems.append(
            f"query count changed: baseline {baseline['query_count']}, results {results['query_count']}"
        )
    for mode in modes:
        old, new = baseline["modes"].get(mode), results["modes"].get(mode)
        if old is None and new is None:
            continue
        if old is None:
            problems.append(f"{mode}: results have no baseline (update it deliberately, see docs/search-quality-gate.md)")
            continue
        if new is None:
            problems.append(f"{mode}: baseline has it but the results do not")
            continue
        for metric in METRICS:
            delta = new[metric] - old[metric]
            if -delta > max_drop + EPSILON:
                status = "REGRESSED"
                problems.append(
                    f"{mode} {LABELS[metric]} fell {-delta:.4f} (baseline {old[metric]:.4f}, "
                    f"now {new[metric]:.4f}); the limit is {max_drop}"
                )
            elif delta > max_drop + EPSILON:
                status = "improved"
            else:
                status = "ok"
            rows.append((mode, metric, old[metric], new[metric], status))
    return rows, problems


def format_table(rows, max_drop, markdown=False):
    header = ("mode", "metric", "baseline", "current", "delta", "status")
    body = [
        (mode, LABELS[metric], f"{old:.4f}", f"{new:.4f}", f"{new - old:+.4f}", status)
        for mode, metric, old, new, status in rows
    ]
    title = f"Search quality vs baseline (a drop of more than {max_drop} in any metric fails)"
    if markdown:
        lines = [f"### {title}", "", "| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        lines += [
            "| " + " | ".join(f"**{cell}**" if cell == "REGRESSED" else cell for cell in row) + " |"
            for row in body
        ]
        return "\n".join(lines) + "\n"
    widths = [max(len(row[i]) for row in [header, *body]) for i in range(len(header))]
    lines = [title]
    for row in [header, *body]:
        lines.append("  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip())
    return "\n".join(lines)


def cmd_compare(args):
    if not Path(args.baseline).is_file():
        fail(f"no baseline at {args.baseline}; create it with scripts/ci/search-quality.sh --update-baseline")
    baseline = json.loads(Path(args.baseline).read_text())
    results = json.loads(Path(args.results).read_text())
    modes = args.modes.split(",") if args.modes else None
    rows, problems = compare(baseline, results, args.max_drop, modes)
    print(format_table(rows, args.max_drop))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as handle:
            handle.write(format_table(rows, args.max_drop, markdown=True))
            for problem in problems:
                handle.write(f"\n- :x: {problem}")
            handle.write("\n")
    if problems:
        print("\nsearch-quality: FAIL", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "If the change is intended, update the baseline: scripts/ci/search-quality.sh "
            "--update-baseline (see docs/search-quality-gate.md).",
            file=sys.stderr,
        )
        return 1
    print("\nsearch-quality: PASS")
    return 0


def cmd_baseline(args):
    results = json.loads(Path(args.results).read_text())
    missing = [mode for mode in MODES if mode not in results["modes"]]
    if missing:
        fail(f"results lack {', '.join(missing)}; rebuild with SEARCH_QUALITY_MODE=hybrid before updating the baseline")
    write_json(BASELINE, results)
    print(f"baseline: wrote {BASELINE}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    package = sub.add_parser("package")
    package.add_argument("--crawl-log", required=True)
    package.add_argument("--origin", required=True)
    package.add_argument("--snapshot", required=True)
    package.add_argument("--generation", required=True)
    package.add_argument("--model-digest", required=True)
    package.add_argument("--out", required=True)
    package.set_defaults(run=cmd_package)

    summ = sub.add_parser("summarize")
    summ.add_argument("--report", required=True)
    summ.add_argument("--out", required=True)
    summ.set_defaults(run=cmd_summarize)

    comp = sub.add_parser("compare")
    comp.add_argument("--baseline", default=str(BASELINE))
    comp.add_argument("--results", required=True)
    comp.add_argument(
        "--max-drop",
        type=float,
        default=float(os.environ.get("SEARCH_QUALITY_MAX_DROP", DEFAULT_MAX_DROP)),
        help="largest tolerated drop per metric (default 0.02, or $SEARCH_QUALITY_MAX_DROP)",
    )
    comp.add_argument("--modes", help="comma-separated modes to gate (default: all of them)")
    comp.set_defaults(run=cmd_compare)

    base = sub.add_parser("baseline")
    base.add_argument("--results", required=True)
    base.set_defaults(run=cmd_baseline)

    args = parser.parse_args(argv)
    return args.run(args) or 0


if __name__ == "__main__":
    sys.exit(main())
