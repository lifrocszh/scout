"""Read local Scout monitoring sources without changing crawl or Generation state."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit


def clean(value, limit=180):
    """Keep untrusted page titles, errors, and URLs from controlling the terminal."""
    text = str(value if value is not None else '?')
    return ''.join(c if c.isprintable() else ' ' for c in text)[:limit]


def safe_url(value):
    try:
        url = urlsplit(value)
        host = url.hostname or ''
        if ':' in host:
            host = '[' + host + ']'
        return clean(urlunsplit((url.scheme, host + (f':{url.port}' if url.port else ''),
                                url.path, '', '')), 240)
    except (ValueError, TypeError):
        return '[invalid-url]'


def timestamp(value):
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    except (ValueError, TypeError, AttributeError):
        return None


def read_json(path):
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def command(args, timeout=2, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout,
                          check=True, **kwargs).stdout.strip()


class EventLog:
    """Read once, then consume only complete appended JSONL records."""

    def __init__(self, path):
        self.path = path
        self.identity = None
        self.offset = 0
        self.latest = {}
        self.claims = {}
        self.workers = {}
        self.failures = deque(maxlen=12)
        self.last_timestamp = None
        self.invalid_lines = 0

    def update(self):
        try:
            stat = self.path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if identity != self.identity or stat.st_size < self.offset:
                self.__init__(self.path)
                self.identity = identity
            with self.path.open('rb') as stream:
                stream.seek(self.offset)
                while True:
                    line = stream.readline()
                    if not line.endswith(b'\n'):
                        break
                    self.offset = stream.tell()
                    try:
                        event = json.loads(line)
                        name, data = event['event'], event.get('data', {})
                        if not isinstance(name, str) or not isinstance(data, dict):
                            raise ValueError('event name/data has the wrong type')
                    except (ValueError, KeyError, TypeError):
                        self.invalid_lines += 1
                        continue
                    self.latest[name] = event
                    self.last_timestamp = event.get('timestamp')
                    worker = data.get('worker_id')
                    if isinstance(worker, str) and worker:
                        self.workers[worker] = event.get('timestamp')
                    task_id = data.get('task_id')
                    if isinstance(task_id, int):
                        if name == 'task.claimed':
                            self.claims[task_id] = event
                        elif name in ('capture.accepted', 'page.rejected', 'task.failed', 'fetch.failed'):
                            self.claims.pop(task_id, None)
                    if name in ('task.failed', 'fetch.failed', 'worker.task_failed', 'worker.task_stale'):
                        self.failures.append(event)
        except OSError:
            pass

    def data(self, name):
        return self.latest.get(name, {}).get('data', {})


def find_psql():
    for path in ('/opt/homebrew/opt/postgresql@17/bin/psql',
                 '/usr/local/opt/postgresql@17/bin/psql', shutil.which('psql')):
        if path and os.access(path, os.X_OK):
            return path
    try:
        prefix = command(['brew', '--prefix', 'postgresql@17'])
        path = Path(prefix) / 'bin/psql'
        if os.access(path, os.X_OK):
            return str(path)
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def database_snapshot(psql, run_id, view, page_filter, offset):
    if not psql:
        raise ValueError('PostgreSQL client unavailable; run launcher setup')
    if not run_id:
        raise ValueError('No saved crawl run')
    sql = Path(__file__).with_name('distributed-local-monitor.sql')
    output = command([psql, '-XAtq', '-h', '/tmp', '-d', 'scout',
                      '-v', 'ON_ERROR_STOP=1', '-v', f'run_id={run_id}',
                      '-v', f'view={view}', '-v', f'page_filter={page_filter}',
                      '-v', f'page_offset={offset}', '-f', str(sql)], timeout=4,
                     env={**os.environ, 'PGCONNECT_TIMEOUT': '2'})
    snapshot = json.loads(output)
    if not snapshot.get('run'):
        raise ValueError('Saved run absent from PostgreSQL')
    return snapshot


def cpu_seconds(value):
    days, _, clock = value.rpartition('-')
    fields = [float(part) for part in clock.split(':')]
    total = 0.0
    for part in fields:
        total = total * 60 + part
    return total + (float(days) * 86400 if days else 0)


class Processes:
    def __init__(self, data_dir, binary):
        self.data_dir = data_dir
        self.binary = binary
        self.previous = {}

    def sample(self):
        roles = {'coordinator': self.data_dir / 'distributed-local/coordinator.pid',
                 **{f'local-worker-{n}': self.data_dir / f'distributed-local/worker-{n}.pid'
                    for n in range(1, 4)},
                 'Generation': self.data_dir / 'distributed-local-ui/build.pid'}
        requested = {}
        samples = {role: {'state': 'down', 'pid': None} for role in roles}
        for role, path in roles.items():
            try:
                pid = path.read_text().strip()
                if pid.isdigit() and int(pid) > 0:
                    requested[int(pid)] = role
            except OSError:
                pass
        try:
            # CLI builds have no launcher PID file. Discover only matching Scout index
            # processes while validating managed crawl PIDs from the same native snapshot.
            output = command(['ps', '-ax', '-o',
                              'pid=,stat=,time=,rss=,lstart=,command='])
        except subprocess.CalledProcessError as error:
            if not error.stderr.strip():
                self.previous.clear()
                return samples
            for role in requested.values():
                samples[role]['state'] = 'unavailable'
            return samples
        except (OSError, subprocess.SubprocessError):
            for role in requested.values():
                samples[role]['state'] = 'unavailable'
            return samples
        now = time.monotonic()
        observed = {}
        for line in output.splitlines():
            parts = line.split(None, 9)
            if len(parts) != 10:
                continue
            try:
                pid, state, cpu, rss = int(parts[0]), parts[1], cpu_seconds(parts[2]), int(parts[3])
                role = requested.get(pid, 'Generation')
            except (ValueError, KeyError):
                continue
            process_role = 'worker' if role.startswith('local-worker') else (
                'index build' if role == 'Generation' else 'coordinator')
            invocation = parts[9]
            if role == 'Generation':
                matches_role = any(invocation.startswith(f'{self.binary} index {operation} ')
                                   for operation in ('build', 'activate', 'verify'))
            else:
                matches_role = invocation.startswith(f'{self.binary} {process_role} ')
            if not matches_role:
                continue
            if not re.search(r'--data-dir ' + re.escape(str(self.data_dir)) + r'(?:\s|$)', invocation):
                continue
            if role.startswith('local-worker') and f'--worker-id {role}' not in invocation:
                continue
            identity = (pid, ' '.join(parts[4:9]))
            previous = self.previous.get(identity)
            utilization = None
            if previous and now > previous[0] and cpu >= previous[1]:
                utilization = (cpu - previous[1]) / (now - previous[0]) * 100
            samples[role] = {'pid': pid, 'state': state, 'rss_bytes': rss * 1024,
                             'cpu_percent': utilization, 'cpu_seconds': cpu}
            observed[identity] = (now, cpu)
        self.previous = observed
        return samples


def host_resources(data_dir):
    values = {}
    commands = {'disk': ['df', '-Pk', str(data_dir)],
                'memory': ['memory_pressure', '-Q'],
                'swap': ['sysctl', 'vm.swapusage'], 'vm': ['vm_stat']}
    for key, args in commands.items():
        try:
            output = command(args)
            if key == 'disk':
                fields = output.splitlines()[-1].split()
                values['disk_free_bytes'] = int(fields[3]) * 1024
            elif key == 'memory':
                values['memory'] = next((line for line in output.splitlines()
                                         if 'System-wide memory free percentage' in line), 'unavailable')
            elif key == 'swap':
                values['swap'] = output.partition(':')[2].strip()
            else:
                page_size = re.search(r'page size of (\d+) bytes', output)
                values['page_size'] = int(page_size[1]) if page_size else None
                for name in ('Swapins', 'Swapouts', 'Pages occupied by compressor'):
                    match = re.search(re.escape(name) + r':\s*(\d+)', output)
                    if match:
                        values[name] = int(match[1])
        except (OSError, ValueError, IndexError, subprocess.SubprocessError):
            values[key] = 'unavailable'
    return values


class Generations:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.first = {}
        self.logs = {}
        self.selected = None
        self.progress = None

    def sample(self):
        lifecycle = {}
        try:
            for line in (self.data_dir / 'distributed-local-ui/build-state').read_text().splitlines():
                key, _, value = line.partition('=')
                lifecycle[key] = value
        except OSError:
            pass
        candidates = []
        for path in (self.data_dir / 'logs').glob('run-*.jsonl'):
            if path not in self.first or self.first[path] is None:
                try:
                    with path.open() as stream:
                        first = json.loads(stream.readline())
                    self.first[path] = first if isinstance(first, dict) and first.get('event') == 'index.started' else False
                except (OSError, ValueError):
                    self.first[path] = None
            first = self.first.get(path)
            if first:
                candidates.append((timestamp(first.get('timestamp')) or 0, path, first))
        if not candidates:
            return {'state': 'No recorded index build', 'current': read_json(self.data_dir / 'CURRENT'),
                    'lifecycle': lifecycle}
        _, path, first = max(candidates, key=lambda item: item[0])
        start = first['data']
        run_id = start.get('build_run_id', first.get('run_id'))
        if not re.fullmatch(r'run-[a-fA-F0-9]{64}', str(run_id)):
            return {'state': 'Invalid build run ID'}
        log = self.logs.setdefault(path, EventLog(path))
        log.update()
        phase_path = self.data_dir / 'logs' / f'{run_id}.build-progress.jsonl'
        phases = self.logs.setdefault(phase_path, EventLog(phase_path))
        phases.update()
        staging = self.data_dir / 'generations/.staging' / run_id
        progress_path = staging / 'semantic-build-progress.jsonl'
        if self.selected != path:
            self.selected = path
            self.progress = EventLog(progress_path)
        self.progress.update()
        phase_event = phases.latest.get('index.phase', {})
        # A resume must not inherit an older attempt's final phase or completion.
        if (timestamp(phase_event.get('timestamp')) or 0) < (timestamp(first.get('timestamp')) or 0):
            phase_event = {}
        summary = log.data('index.completed')
        failed = log.data('index.failed')
        manifest = read_json(staging / 'generation-manifest.json')
        generation_id = summary.get('generation_id') or manifest.get('generation_id')
        if generation_id and re.fullmatch(r'generation-[a-fA-F0-9]{64}', generation_id):
            manifest = read_json(self.data_dir / 'generations' / generation_id / 'generation-manifest.json') or manifest
        current = read_json(self.data_dir / 'CURRENT')
        snapshot = start.get('corpus_snapshot_id', '')
        corpus = read_json(self.data_dir / 'corpora' / snapshot / 'snapshot-manifest.json') if re.fullmatch(
            r'snapshot-[a-fA-F0-9]{64}', snapshot) else {}
        chunk = self.progress.latest.get('passage_chunk_completed', {})
        # Ignore old semantic progress until the new attempt has recreated its file.
        if start.get('resume') and (timestamp(chunk.get('timestamp')) or 0) < (timestamp(first.get('timestamp')) or 0):
            chunk = {}
        phase = phase_event.get('data', {}).get('phase', 'complete (phase history unavailable)' if summary else
                                             'building (phase unavailable)')
        if ((timestamp(self.progress.last_timestamp) or 0) >= (timestamp(first.get('timestamp')) or 0)
                and phase == 'semantic'):
            phase = next(reversed(self.progress.latest), 'semantic')
        state = 'failed' if failed else ('validated' if summary else 'in progress / process state below')
        if generation_id and current.get('generation_id') == generation_id:
            state = 'active'
        keyword = phases.data('index.progress')
        if (timestamp(phases.latest.get('index.progress', {}).get('timestamp')) or 0) < (timestamp(first.get('timestamp')) or 0):
            keyword = {}
        return {'state': state, 'phase': phase, 'run_id': run_id,
                'snapshot_id': snapshot, 'generation_id': generation_id,
                'mode': start.get('mode', '?'), 'total': corpus.get('passage_count'),
                'progress': chunk.get('data', chunk), 'summary': summary,
                'manifest_status': manifest.get('status'), 'current': current,
                'error': failed.get('message'), 'last_activity': log.last_timestamp,
                'profile': read_json(staging / 'build-profile.json'), 'keyword_progress': keyword,
                'lifecycle': lifecycle if lifecycle.get('snapshot_id') == snapshot and (
                    not lifecycle.get('generation_id') or lifecycle['generation_id'] == generation_id) else {}}


class Monitor:
    def __init__(self, data_dir, binary):
        self.data_dir = data_dir.resolve()
        self.binary = binary.resolve()
        state = {}
        try:
            for line in (self.data_dir / 'distributed-local/run-state').read_text().splitlines():
                key, _, value = line.partition('=')
                state[key] = value
        except OSError:
            pass
        self.run_id = state.get('run_id', '')
        # Older local launchers used local-<UUID>; IDs must remain safe path components.
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,255}', self.run_id):
            self.run_id = ''
        self.log = EventLog(self.data_dir / 'logs' / f'{self.run_id}.jsonl')
        self.processes = Processes(self.data_dir, self.binary)
        self.generations = Generations(self.data_dir)
        self.psql = find_psql()
        self.database = None
        self.database_at = None
        self.page_key = None
        self.previous_vm = None

    def sample(self, view='overview', page_filter='all', offset=0):
        self.log.update()
        error = None
        try:
            self.database = database_snapshot(self.psql, self.run_id, view, page_filter, offset)
            self.database_at = time.time()
            self.page_key = (view, page_filter, offset)
            live_tasks = {lease['task_id'] for lease in self.database['leases']}
            self.log.claims = {task: claim for task, claim in self.log.claims.items() if task in live_tasks}
        except (OSError, ValueError, subprocess.SubprocessError) as failure:
            if isinstance(failure, subprocess.CalledProcessError):
                reason = (failure.stderr.strip().splitlines() or ['query failed'])[0]
            elif isinstance(failure, subprocess.TimeoutExpired):
                reason = 'query/connection timed out'
            else:
                reason = str(failure)
            error = 'PostgreSQL snapshot unavailable: ' + clean(reason)
            if not self.run_id:
                error = 'No saved crawl run; Generation monitoring remains available'
        host = host_resources(self.data_dir) if view in ('constraints', 'generation') else {}
        now = time.monotonic()
        if host and self.previous_vm:
            for name in ('Swapins', 'Swapouts'):
                if name in host and name in self.previous_vm[1] and host[name] >= self.previous_vm[1][name]:
                    host[name + '_per_second'] = (host[name] - self.previous_vm[1][name]) / (now - self.previous_vm[0])
        if host:
            self.previous_vm = (now, host)
        return {'run_id': self.run_id, 'database': self.database, 'database_error': error,
                'database_age': time.time() - self.database_at if self.database_at else None,
                'pages_current': self.page_key == (view, page_filter, offset),
                'policy': self.log.data('crawl.policy'),
                'sources': self.log.data('crawl.config').get('sources', []),
                'target': self.log.data('coordinator.started').get('page_target'),
                'claims': self.log.claims, 'worker_activity': self.log.workers,
                'failures': list(self.log.failures), 'last_activity': self.log.last_timestamp,
                'invalid_log_lines': self.log.invalid_lines, 'processes': self.processes.sample(),
                'host': host, 'generation': self.generations.sample() if view == 'generation' else {},
                'sampled_at': datetime.now(timezone.utc).isoformat()}
