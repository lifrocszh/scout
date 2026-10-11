#!/usr/bin/env python3
"""Drill-down screens for the existing local distributed crawl menu."""

import argparse
import json
import math
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import termios
import textwrap
import time
import tty

from distributed_local_metrics import Monitor, clean, safe_url, timestamp


VIEWS = {'o': 'overview', 'f': 'frontier', 'p': 'pages', 'w': 'workers',
         't': 'throughput', 'c': 'constraints', 'g': 'generation'}
FILTERS = ('all', 'captured', 'rejected', 'failed', 'pending', 'leased')
PAGE_SIZE = 5
VIEW_HELP = {
    'overview': 'Progress toward the page target. Captures can include duplicate content.',
    'frontier': 'The crawl queue: URLs waiting, assigned to workers, completed, or failed.',
    'pages': 'Browse individual URL outcomes. Use v to filter and n/b to change pages.',
    'workers': 'What each worker is doing. A running process may still be waiting or stalled.',
    'throughput': 'Recent crawl speed and estimated time to the unique-content target.',
    'constraints': 'Request limits and machine resources that can slow or block the crawl.',
    'generation': 'A Generation is a search index. Follow its build, validation, and activation.',
}


def age(value):
    seconds = timestamp(value)
    return duration(time.time() - seconds) if seconds is not None else '?'


def duration(seconds):
    if seconds is None:
        return '?'
    seconds = max(0, int(seconds))
    return f'{seconds // 3600}h {(seconds % 3600) // 60}m' if seconds >= 3600 else (
        f'{seconds // 60}m {seconds % 60}s' if seconds >= 60 else f'{seconds}s')


def size(value):
    return '?' if value is None else f'{value / 1024 ** 2:.1f} MiB'


def process_line(name, process):
    cpu = process.get('cpu_percent')
    return (f'{name}: {process["state"]}  PID {process.get("pid") or "-"}  '
            f'CPU {cpu:.1f}%  RSS {size(process.get("rss_bytes"))}' if cpu is not None else
            f'{name}: {process["state"]}  PID {process.get("pid") or "-"}  '
            f'CPU ?  RSS {size(process.get("rss_bytes"))}')


def quotas(sample):
    counts = (sample.get('database') or {}).get('groups_by_source', {})
    return [(source['source_id'], counts.get(source['source_id'], 0),
             source.get('minimum_page_quota', 0)) for source in sample['sources']]


def rates(sample):
    database = sample['database']
    created = timestamp(database['run'].get('created_at'))
    sampled = timestamp(database.get('sampled_at'))
    elapsed = max(1, sampled - created) if created is not None and sampled is not None else None
    return [(row['seconds'], {key: row[key] / min(row['seconds'], elapsed or row['seconds'])
                             for key in ('tasks', 'captures', 'unique_content')})
            for row in database['rates']]


def crawl_conditions(sample):
    database, target = sample['database'], sample['target']
    counts, unique = database['counts'], database['unique_content']
    quota_known = bool(sample['sources'])
    missing = [(source, minimum - done) for source, done, minimum in quotas(sample) if done < minimum]
    maximum = sample['policy'].get('max_duration_seconds')
    created = timestamp(database['run'].get('created_at'))
    elapsed = timestamp(database['sampled_at']) - created if created else None
    conditions = []
    if database['run'].get('status') == 'failed':
        conditions.append('run failed')
    if target is not None and unique >= target:
        conditions.append('target met' + ('; quotas unmet' if missing else
                                         ('; quotas met' if quota_known else '; quotas unknown')))
    if maximum is not None and elapsed is not None and elapsed >= maximum:
        conditions.append('duration limit reached')
    if counts['pending'] + counts['leased'] == 0:
        conditions.append('frontier exhausted')
    if not conditions:
        conditions.append('crawl unfinished')
    if not sample['processes']['coordinator'].get('pid'):
        conditions.append('coordinator stopped / unavailable')
    return '; '.join(conditions)


def overview(sample):
    database, target = sample['database'], sample['target']
    counts, outcomes, unique = database['counts'], database['outcomes'], database['unique_content']
    lines = [f'Unique content:    {unique} / {target if target is not None else "?"}',
             f'Accepted captures: {outcomes["captures"]}  Rejected tasks: {outcomes["rejected"]}',
             f'URL tasks:         {counts["succeeded"]} completed; {counts["failed"]} terminal failures',
             f'Frontier:          {counts["pending"]} pending ({counts["delayed"]} delayed); '
             f'{counts["in_flight"]} in flight; {counts["expired"]} expired leases']
    if target is not None:
        lines.append(f'Target remaining:  {max(0, target - unique)}  Overshoot: {max(0, unique - target)}')
    lines.append('Conditions:        ' + crawl_conditions(sample))
    for source, done, minimum in quotas(sample):
        if minimum and done < minimum:
            lines.append(f'Quota remaining:   {clean(source)} {minimum - done} ({done}/{minimum})')
    lines += throughput(sample, compact=True)
    lines.append('Captured tasks can share content. Rejected tasks count as completed URL tasks.')
    return lines


def frontier(sample):
    database = sample['database']
    counts = database['counts']
    lines = [f'Tasks {counts["total"]}: pending {counts["pending"]} / leased {counts["leased"]} / '
             f'succeeded {counts["succeeded"]} / failed {counts["failed"]}',
             f'Ready {counts["ready"]}; delayed retries {counts["delayed"]}; expired leases {counts["expired"]}',
             'SOURCE                      pending leased succeeded failed | unique / quota']
    minimums = {source: minimum for source, _, minimum in quotas(sample)}
    sources = {row['source_id']: row for row in database['sources']}
    for source in minimums:
        sources.setdefault(source, {'pending': 0, 'leased': 0, 'succeeded': 0, 'failed': 0})
    for source, row in sorted(sources.items()):
        lines.append(f'{clean(source, 27):27} {row["pending"]:7} {row["leased"]:6} '
                     f'{row["succeeded"]:9} {row["failed"]:6} | '
                     f'{database["groups_by_source"].get(source, 0)} / {minimums.get(source, "?")}')
    lines += ['', 'ORIGIN                      pending leased succeeded failed']
    for row in database['origins']:
        lines.append(f'{clean(row["origin"], 27):27} {row["pending"]:7} {row["leased"]:6} '
                     f'{row["succeeded"]:9} {row["failed"]:6}')
    lines.append(f'Discoveries queued {database["outcomes"]["discoveries"]}; '
                 f'discoveries rejected {database["outcomes"]["rejected_discoveries"]}')
    return lines


def pages(sample, page_filter, offset):
    if not sample['pages_current']:
        return ['Page selection unavailable; cached snapshot belongs to another view/filter.']
    rows = sample['database']['pages']
    lines = [f'URL tasks: {page_filter}, page {offset // PAGE_SIZE + 1}  [v] filter  [n]/[b] next/previous']
    for row in rows[:PAGE_SIZE]:
        reason = row.get('rejection_reason') or row.get('last_error') or row.get('title') or ''
        lines += [f'#{row["task_id"]} {row["outcome"]} | {clean(row["source_id"])} | '
                  f'HTTP {row.get("http_status") or "-"} | '
                  f'{row.get("body_size") if row.get("body_size") is not None else "?"} bytes | '
                  f'attempt {row["task_attempt_count"]}/{row["max_task_attempts"]}',
                  '  ' + safe_url(row.get('final_url') or row['normalized_url']),
                  f'  {age(row["outcome_at"])} ago  {clean(reason)}']
        if row.get('final_url') and row['final_url'] != row['normalized_url']:
            lines.append('  requested: ' + safe_url(row['normalized_url']))
        if row.get('content_hash'):
            lines.append(f'  content {row["content_hash"][:12]} / body {row["body_hash"][:12]}; '
                         f'discoveries queued {row["accepted_discovered_count"]}, '
                         f'rejected {row["rejected_discovered_count"]}')
    if not rows:
        lines.append('No tasks in this selection.')
    lines.append('More pages available.' if len(rows) > PAGE_SIZE else 'End of current selection.')
    lines.append('Live rows are URL-task outcomes; sealed Corpus Pages may merge duplicate content.')
    return lines


def workers(sample):
    database = sample['database'] or {}
    lines = [process_line('Coordinator process', sample['processes']['coordinator']),
             f'Last coordinator event: {age(sample["last_activity"])} ago (activity, not a health probe)']
    for worker in (f'local-worker-{n}' for n in range(1, 4)):
        lines += ['', process_line(worker, sample['processes'][worker])]
        leases = [lease for lease in database.get('leases', []) if lease['lease_owner'] == worker]
        permits = [permit for permit in database.get('permits', []) if permit['worker_id'] == worker]
        permit_text = (f'active {sum(p["active"] for p in permits)}, '
                       f'waiting {sum(p["waiting"] for p in permits)}' if database else 'unavailable')
        lines.append(f'  permits {permit_text}; last activity {age(sample["worker_activity"].get(worker))} ago')
        for lease in leases:
            claim = sample['claims'].get(lease['task_id'], {})
            if claim.get('data', {}).get('lease_epoch') != lease['lease_epoch']:
                claim = {}
            deadline = timestamp(lease['lease_deadline'])
            lines += [f'  task #{lease["task_id"]} age {age(claim.get("timestamp"))}; '
                      f'epoch {lease["lease_epoch"]}; '
                      f'lease {"expired" if deadline is not None and deadline <= time.time() else "valid"} '
                      f'({duration(deadline - time.time() if deadline else None)} left); '
                      f'last lease update {age(lease["updated_at"])} ago',
                      '  ' + safe_url(lease['normalized_url'])]
        if not leases:
            lines.append('  no recorded lease' if database else '  lease state unavailable')
    lines += ['', 'Recent failure events:']
    for event in sample['failures'][-5:]:
        data = event['data']
        lines.append(f'{age(event.get("timestamp"))} ago {clean(data.get("worker_id"))} '
                     f'task #{data.get("task_id", "?")} {clean(data.get("error_code", event["event"]))}')
    if not sample['failures']:
        lines.append('None observed in coordinator log.')
    return lines


def throughput(sample, compact=False):
    lines = []
    windows = rates(sample)
    for seconds, values in windows:
        lines.append(f'{seconds}s rates: tasks {values["tasks"] * 60:.1f}/min; '
                     f'captures {values["captures"] * 60:.1f}/min; '
                     f'unique content {values["unique_content"] * 60:.1f}/min')
    target, database = sample['target'], sample['database']
    missing = max(0, target - database['unique_content']) if target is not None else None
    rate = windows[0][1]['unique_content']
    stopped = ('duration limit reached' in crawl_conditions(sample) or
               'frontier exhausted' in crawl_conditions(sample) or
               database['run'].get('status') == 'failed' or
               not sample['processes']['coordinator'].get('pid'))
    estimate = 'target met' if missing == 0 else (
        duration(missing / rate) if missing is not None and rate > 0 and not stopped else
        'unavailable (no current progress / stopped / target unknown)')
    lines.append('Target ETA: ' + estimate)
    if not compact:
        lines += ['ETA uses unique content, excludes time needed for unmet source quotas.',
                  'Task rate counts completed outcomes and terminal failures; retries/HTTP requests stay separate.',
                  'Windows use durable DB timestamps, including progress before opening this monitor.']
    return lines


def resource_lines(sample):
    lines = [process_line(name, process) for name, process in sample['processes'].items()]
    host = sample['host']
    lines += ['CPU samples cover listed processes; 100% = one core. First sample has no CPU delta.',
              'Host: ' + clean(host.get('memory', 'unavailable')),
              'Swap: ' + clean(host.get('swap', 'unavailable')),
              f'Disk free (data filesystem): {size(host.get("disk_free_bytes"))}']
    if host.get('page_size') and 'Pages occupied by compressor' in host:
        lines.append('Compressed memory: ' + size(host['Pages occupied by compressor'] * host['page_size']))
    for name in ('Swapins', 'Swapouts'):
        rate = host.get(name + '_per_second')
        if rate is not None:
            lines.append(f'{name}: {rate:.1f} pages/s (host-wide)')
    return lines


def constraints(sample):
    database = sample['database'] or {}
    policy = database.get('policy') or {}
    permits = database.get('permits', [])
    active = sum(p['active'] for p in permits) if database else '?'
    waiting = sum(p['waiting'] for p in permits) if database else '?'
    lines = [f'Global permits: {active} / {policy.get("global_concurrency", "?")} active; '
             f'{waiting} / {policy.get("max_pending_requests", "?")} waiting',
             f'HTTP attempts: {policy.get("total_attempts", "?")} / {policy.get("max_total_attempts", "?")}; '
             f'start spacing {policy.get("min_start_spacing_ms", "?")} ms',
             f'Duration limit: {sample["policy"].get("max_duration_seconds", "?")}s; '
             f'frontier limit: {sample["policy"].get("max_frontier", "?")} unfinished tasks']
    lines.append(f'Request timeout: {policy.get("request_timeout_ms", "?")} ms; '
                 f'body limit: {sample["policy"].get("max_decoded_body_bytes", "?")} bytes; '
                 f'extraction limit: {sample["policy"].get("max_extraction_bytes", "?")} bytes / '
                 f'{sample["policy"].get("max_extraction_millis", "?")} ms')
    origins = sorted({p['origin'] for p in permits})
    for origin in origins:
        rows = [p for p in permits if p['origin'] == origin]
        oldest = min((p['oldest_wait'] for p in rows if p['oldest_wait']), default=None)
        lines.append(f'{clean(origin)}: {sum(p["active"] for p in rows)} / '
                     f'{policy.get("origin_concurrency", "?")} active; '
                     f'{sum(p["waiting"] for p in rows)} waiting ({age(oldest)} oldest); '
                     f'{sum(p["expired"] for p in rows)} expired records')
    for row in database.get('spacing', []):
        until = timestamp(row['next_allowed_at'])
        lines.append(f'Spacing wait {clean(row["schedule_key"])}: '
                     f'{max(0, until - time.time()):.2f}s' if until else 'Spacing time unavailable')
    lines += ['', *resource_lines(sample)]
    return lines


def generation(sample):
    build = sample['generation']
    state = build.get('state', '?')
    if state.startswith('in progress') and not sample['processes']['Generation'].get('pid'):
        state = 'unfinished; process ' + sample['processes']['Generation']['state']
    lines = [f'Build status: {state}; phase: {build.get("phase", "?")}',
             f'Mode: {build.get("mode", "?")}  Build run: {build.get("run_id", "?")}',
             f'Corpus: {build.get("snapshot_id", "?")}',
             f'Generation: {build.get("generation_id") or "not assigned / still building"}',
             f'Manifest: {build.get("manifest_status") or "not published"}; '
             f'CURRENT: {build.get("current", {}).get("generation_id", "none")}']
    progress, summary = build.get('progress', {}), build.get('summary', {})
    if build.get('mode') == 'keyword-only':
        done = summary.get('passage_count', build.get('keyword_progress', {}).get('passages_completed', 0))
        lines.append(f'Passages: {done} / {build.get("total", "?")}; embeddings N/A')
    else:
        done = progress.get('passages_completed', summary.get('passage_count', 0))
        lines.append(f'Passages: {done} / {build.get("total", "?")}')
        reuse = summary.get('embedding_reuse', {})
        created = progress.get('embedding_cache_vectors_created', reuse.get('cache_vectors_created'))
        semantic = summary.get('semantic') or {}
        milliseconds = progress.get('passage_embedding_ms', (semantic.get('stage_timings') or {}).get('passage_embedding_ms'))
        if created is not None and milliseconds:
            lines.append(f'Embedding compute rate: {created / (milliseconds / 1000):.1f} vectors/s')
        else:
            lines.append('Embedding compute rate: N/A (no new vectors measured yet)')
        for field, label, final in (
            ('embedding_cache_vectors_reused', 'Cache reused', 'cache_vectors_reused'),
            ('embedding_cache_vectors_created', 'New vectors', 'cache_vectors_created'),
            ('build_checkpoint_vectors_reused', 'Checkpoint reused', 'build_checkpoint_vectors_reused'),
            ('local_duplicate_vectors_reused', 'Local duplicates reused', 'local_duplicate_vectors_reused')):
            lines.append(f'{label}: {progress.get(field, reuse.get(final, "?"))}')
    lifecycle = build.get('lifecycle', {})
    if lifecycle:
        lines.append(f'Last managed build/activation/verify: {clean(lifecycle.get("phase"))}')
    if build.get('error'):
        lines.append('Build error: ' + clean(build['error']))
    lines += ['', *resource_lines(sample)]
    return lines


def render(sample, view, page_filter, offset):
    lines = ['SCOUT  /  LOCAL DISTRIBUTED CRAWL',
             f'Run ID: {sample["run_id"] or "none"}  View: {view}',
             VIEW_HELP[view],
             f'Updated: {clean(sample["sampled_at"])}',
             process_line('Coordinator', sample['processes']['coordinator']),
             f'Workers: {sum(bool(sample["processes"][f"local-worker-{n}"].get("pid")) for n in range(1, 4))} / 3', '']
    if sample['database_error']:
        stale = f'; cached DB age {duration(sample["database_age"])}' if sample['database'] else ''
        lines += [sample['database_error'] + stale, '']
    if sample['invalid_log_lines']:
        lines.append(f'Ignored malformed log records: {sample["invalid_log_lines"]}')
    if view in ('workers', 'constraints', 'generation'):
        lines += {'workers': workers, 'constraints': constraints, 'generation': generation}[view](sample)
    elif not sample['database']:
        lines.append('Durable crawl data unavailable; process/resource/Generation views still work.')
    elif view == 'pages':
        lines += pages(sample, page_filter, offset)
    else:
        lines += {'overview': overview, 'frontier': frontier, 'throughput': throughput}[view](sample)
    return lines


def wrap_lines(lines, width):
    return [wrapped for line in lines for wrapped in
            (textwrap.wrap(clean(line, 1000), max(1, width - 1), break_on_hyphens=False) or [''])]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=Path(__file__).resolve().parent.parent / 'target/release/scout')
    parser.add_argument('--view', choices=VIEWS.values(), default='overview')
    parser.add_argument('--refresh', type=float, default=5)
    parser.add_argument('--once', action='store_true', help='Print one snapshot without controlling the terminal')
    parser.add_argument('--json', action='store_true', help='Print one JSON snapshot')
    args = parser.parse_args()
    if not math.isfinite(args.refresh) or args.refresh <= 0:
        parser.error('--refresh must be finite and positive')
    monitor = Monitor(args.data_dir, args.binary)
    if args.once or args.json:
        sample = monitor.sample(args.view)
        print(json.dumps(sample) if args.json else '\n'.join(render(sample, args.view, 'all', 0)))
        return
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        parser.error('Use an interactive terminal or --once')
    original = termios.tcgetattr(sys.stdin)
    view, page_filter, offset, scroll = args.view, 'all', 0, 0
    launcher = Path(__file__).with_name('distributed-local.sh')
    try:
        tty.setcbreak(sys.stdin)
        while True:
            sample = monitor.sample(view, page_filter, offset)
            width, height = shutil.get_terminal_size((100, 32))
            lines = wrap_lines(render(sample, view, page_filter, offset), width)
            footer = wrap_lines([
                '[o] overview [f] frontier [p] pages [w] workers [t] rates [c] constraints [g] Generation',
                '[m] back to menu [s] stop crawl [j]/[k] scroll | pages: [v] filter [n]/[b] next/previous'], width)
            available = max(1, height - len(footer) - 2)
            scroll = min(scroll, max(0, len(lines) - available))
            print('\033[2J\033[H' + '\n'.join(lines[scroll:scroll + available]))
            print('\n'.join(footer))
            print(f'Refresh {args.refresh:g}s; rows {scroll + 1}-{min(len(lines), scroll + available)}/{len(lines)}')
            sys.stdout.flush()
            if not select.select([sys.stdin], [], [], args.refresh)[0]:
                continue
            key = os.read(sys.stdin.fileno(), 1).decode(errors='ignore').lower()
            if not key:
                return
            if key == 'm':
                return
            if key == 's':
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, original)
                subprocess.run([str(launcher), 'stop', str(monitor.data_dir)], check=False)
                tty.setcbreak(sys.stdin)
            elif key in VIEWS:
                view, scroll = VIEWS[key], 0
            elif view == 'pages' and key == 'v':
                page_filter = FILTERS[(FILTERS.index(page_filter) + 1) % len(FILTERS)]
                offset, scroll = 0, 0
            elif view == 'pages' and key == 'n' and sample['pages_current'] and sample['database'] and len(sample['database']['pages']) > PAGE_SIZE:
                offset, scroll = offset + PAGE_SIZE, 0
            elif view == 'pages' and key == 'b':
                offset, scroll = max(0, offset - PAGE_SIZE), 0
            elif key == 'j':
                scroll += max(1, available // 2)
            elif key == 'k':
                scroll = max(0, scroll - max(1, available // 2))
    except KeyboardInterrupt:
        return
    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, original)


if __name__ == '__main__':
    main()
