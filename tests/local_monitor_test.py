"""Run with python3 -B tests/local_monitor_test.py; no crawl/build or DB writes."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import distributed_local_metrics as metrics

spec = importlib.util.spec_from_file_location('local_monitor', ROOT / 'scripts/distributed-local-monitor.py')
ui = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ui)


def event(name, data=None, at='2026-10-11T01:00:00Z'):
    return {'event': name, 'timestamp': at, 'data': data or {}}


def write_events(path, *events):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(json.dumps(item) + '\n' for item in events))


class LocalMonitorTests(unittest.TestCase):
    def test_incremental_log_keeps_partial_record_and_original_claim_age(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'run.jsonl'
            claim = event('task.claimed', {'task_id': 7, 'lease_epoch': 2, 'worker_id': 'local-worker-1'})
            write_events(path, claim)
            renewal = json.dumps(event('task.lease_renewed', {'task_id': 7, 'lease_epoch': 2},
                                      '2026-10-11T01:01:00Z'))
            with path.open('a') as stream:
                stream.write(renewal[:30])
            log = metrics.EventLog(path)
            log.update()
            self.assertEqual(log.claims[7]['timestamp'], claim['timestamp'])
            self.assertNotIn('task.lease_renewed', log.latest)
            with path.open('a') as stream:
                stream.write(renewal[30:] + '\nnot-json\n')
            log.update()
            log.update()
            self.assertEqual(log.claims[7]['timestamp'], claim['timestamp'])
            self.assertEqual(log.invalid_lines, 1)
            write_events(path, event('capture.accepted', {'task_id': 7}))
            log.update()
            self.assertFalse(log.claims)
            self.assertEqual(log.invalid_lines, 0)

    def test_target_counts_unique_content_and_rejections_remain_completed_tasks(self):
        sample = {
            'target': 500, 'sources': [{'source_id': 'rust', 'minimum_page_quota': 1}],
            'policy': {}, 'processes': {'coordinator': {'pid': None}},
            'database': {'counts': {'succeeded': 513, 'failed': 0, 'pending': 3634,
                                    'leased': 0, 'delayed': 0, 'in_flight': 0, 'expired': 0},
                         'outcomes': {'captures': 503, 'rejected': 10}, 'unique_content': 502,
                         'groups_by_source': {'rust': 502},
                         'run': {'status': 'active', 'created_at': '2026-10-11T00:00:00Z'},
                         'sampled_at': '2026-10-11T01:00:00Z',
                         'rates': [{'seconds': n, 'tasks': 0, 'captures': 0, 'unique_content': 0}
                                   for n in (60, 300)]}}
        output = '\n'.join(ui.overview(sample))
        self.assertIn('502 / 500', output)
        self.assertIn('Overshoot: 2', output)
        self.assertIn('503', output)
        self.assertIn('513 completed', output)
        sample['database']['unique_content'] = 499
        output = '\n'.join(ui.overview(sample))
        self.assertIn('Target remaining:  1', output)
        self.assertIn('unavailable', output)

    def test_database_failure_retains_labeled_snapshot_but_not_wrong_page_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory)
            state = data / 'distributed-local/run-state'
            state.parent.mkdir()
            state.write_text('run_id=local-1234\n')
            with patch.object(metrics, 'find_psql', return_value=None), \
                 patch.object(metrics.Processes, 'sample', return_value={}), \
                 patch.object(metrics, 'database_snapshot', side_effect=[{'pages': [1], 'leases': []}, ValueError('offline')]):
                monitor = metrics.Monitor(data, data / 'scout')
                first = monitor.sample('pages', 'captured')
                second = monitor.sample('pages', 'failed')
            self.assertEqual(first['run_id'], 'local-1234')
            self.assertEqual(second['database'], first['database'])
            self.assertIn('offline', second['database_error'])
            self.assertGreaterEqual(second['database_age'], 0)
            self.assertFalse(second['pages_current'])

    def test_pid_reuse_resets_cpu_baseline_and_cli_build_is_discovered(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory)
            (data / 'distributed-local').mkdir()
            (data / 'distributed-local/coordinator.pid').write_text('123')
            binary = data / 'scout'
            def row(cpu, started):
                return (f'123 S {cpu} 2048 Sun Oct 11 {started} 2026 {binary} coordinator --data-dir {data}\n'
                        f'124 S 0:01.00 4096 Sun Oct 11 09:00:00 2026 {binary} index build --data-dir {data}')
            process = metrics.Processes(data, binary)
            with patch.object(metrics, 'command', side_effect=[row('0:10.00', '09:00:00'),
                                                               row('0:20.00', '09:00:00'),
                                                               row('0:01.00', '09:01:00')]), \
                 patch.object(metrics.time, 'monotonic', side_effect=[100, 105, 110]):
                first, second, reused = process.sample(), process.sample(), process.sample()
            self.assertIsNone(first['coordinator']['cpu_percent'])
            self.assertEqual(second['coordinator']['cpu_percent'], 200)
            self.assertIsNone(reused['coordinator']['cpu_percent'])
            self.assertEqual(first['Generation']['pid'], 124)
            self.assertEqual(first['coordinator']['rss_bytes'], 2048 * 1024)

    def test_resume_ignores_prior_attempt_phase_and_semantic_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory)
            build_id, invocation_id = 'run-' + 'a' * 64, 'run-' + 'b' * 64
            write_events(data / f'logs/{build_id}.jsonl',
                         event('index.started', {'build_run_id': build_id, 'mode': 'hybrid'}))
            write_events(data / f'logs/{build_id}.build-progress.jsonl',
                         event('index.phase', {'phase': 'validated'}))
            write_events(data / f'logs/{invocation_id}.jsonl',
                         event('index.started', {'build_run_id': build_id, 'mode': 'hybrid', 'resume': True},
                               '2026-10-11T02:00:00Z'))
            write_events(data / f'generations/.staging/{build_id}/semantic-build-progress.jsonl',
                         {'event': 'passage_chunk_completed', 'passages_completed': 99,
                          'timestamp': '2026-10-11T01:30:00Z'})
            sample = metrics.Generations(data).sample()
            self.assertNotEqual(sample['phase'], 'validated')
            self.assertEqual(sample['progress'], {})
            self.assertFalse(sample['summary'])

    def test_unavailable_permits_are_unknown_and_urls_cannot_control_terminal(self):
        sample = {'database': None, 'policy': {}, 'host': {}, 'processes': {}}
        output = '\n'.join(ui.constraints(sample))
        self.assertIn('Global permits: ? / ? active', output)
        self.assertNotIn('Global permits: 0', output)
        self.assertEqual(metrics.safe_url('https://user:secret@example.com/p?q=secret#fragment'),
                         'https://example.com/p')
        self.assertNotIn('\033', metrics.clean('title\033[2J\n'))
        wrapped = ui.wrap_lines(['Generation: generation-' + 'a' * 64], 60)
        self.assertGreater(len(wrapped), 1)
        self.assertEqual(''.join(wrapped), 'Generation: generation-' + 'a' * 64)

    def test_managed_build_never_activates_failed_or_unidentified_output(self):
        wrapper = ROOT / 'scripts/distributed-local-build.sh'
        for failure in ('none', 'build', 'invalid-id', 'activate', 'verify'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                data = Path(directory)
                binary = data / 'scout'
                # A protocol fixture, not an actual Scout build or persisted Generation.
                binary.write_text('''#!/bin/bash
printf '%s\\n' "$2" >> "$(dirname "$0")/actions"
case "$2" in
  build)
    [[ "$MOCK_FAILURE" == build ]] && exit 1
    [[ "$MOCK_FAILURE" == invalid-id ]] && { echo 'missing ID'; exit 0; }
    printf 'index build complete: generation_id=generation-%064d pages=1 passages=1\\n' 0 ;;
  activate|verify) [[ "$MOCK_FAILURE" == "$2" ]] && exit 1 ;;
esac
exit 0
''')
                binary.chmod(0o700)
                import os
                result = subprocess.run(['bash', str(wrapper), str(binary), 'snapshot-fixture', str(data)],
                                        env={**os.environ, 'MOCK_FAILURE': failure},
                                        capture_output=True, text=True, timeout=5)
                actions = (data / 'actions').read_text().splitlines()
                state = (data / 'distributed-local-ui/build-state').read_text()
                if failure == 'none':
                    self.assertEqual(result.returncode, 0)
                    self.assertEqual(actions, ['build', 'activate', 'verify'])
                    self.assertIn('phase=ready', state)
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('phase=failed', state)
                    if failure in ('build', 'invalid-id'):
                        self.assertEqual(actions, ['build'])
                    if failure == 'activate':
                        self.assertEqual(actions, ['build', 'activate'])
                self.assertFalse((data / 'distributed-local-ui/build.lock').exists())
                self.assertFalse((data / 'distributed-local-ui/build.pid').exists())


if __name__ == '__main__':
    unittest.main()
