'use strict';

/* Scout demo UI — vanilla JS, no build step. Talks only to demo/server.py. */

const $ = (id) => document.getElementById(id);
const POLL_BUSY = 1500;
const POLL_IDLE = 5000;
const FETCH_TIMEOUT = 10000;
const PHASES = ['verify_corpus', 'catalog', 'keyword', 'semantic', 'manifest', 'validation', 'validated'];
const DASH = '—';

const ui = {};
for (const id of ['banner', 'live', 'crawl-badge', 'crawl-meta', 'crawl-log', 'configs-body', 'configs-note',
  'stop-crawl', 'snapshots-body', 'snapshots-note', 'build-btn', 'build-badge', 'build-phases', 'build-meta',
  'build-note', 'build-error', 'build-log', 'activate-btn', 'activate-result', 'serve-badge', 'serve-meta',
  'serve-form', 'serve-root', 'serve-port', 'serve-start', 'serve-stop', 'serve-note', 'generation-body',
  'search-form', 'search-query', 'search-source', 'search-limit', 'search-mode', 'search-submit',
  'search-status', 'search-meta', 'search-results', 'path-prebuilt', 'path-work']) {
  ui[id.replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = $(id);
}
ui.refresh = $('refresh');

const state = {
  server: null, offline: false, configs: null, snapshots: null, generation: null, generationError: null,
  selected: null, sig: {}, busy: {}, pollCount: 0, crawlElapsed: null, buildElapsed: null,
};

/* ---------- DOM helpers (textContent only — never innerHTML) ---------- */

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined && text !== null) n.textContent = String(text);
  return n;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

const show = (v) => (v === null || v === undefined || v === '' ? DASH : String(v));

/* crawled titles/urls/paths are untrusted: only http(s) links may be clickable */
function safeHref(url) {
  try {
    const u = new URL(String(url));
    return (u.protocol === 'http:' || u.protocol === 'https:') ? u.href : '';
  } catch (_) {
    return '';
  }
}

function kv(label, value, opts) {
  const o = opts || {};
  const box = el('div', 'kv');
  box.appendChild(el('span', 'kv-k', label));
  const v = el('span', o.mono ? 'kv-v mono' : 'kv-v', show(value));
  if (o.title && value !== null && value !== undefined) v.title = String(value);
  if (o.id) v.id = o.id;
  box.appendChild(v);
  return box;
}

function kvGrid(items) {
  const grid = el('div', 'grid');
  for (const it of items) grid.appendChild(kv(it[0], it[1], it[2]));
  return grid;
}

function badge(text, cls) {
  return el('span', 'badge ' + (cls || 'idle'), text);
}

function mmss(total) {
  if (total === null || total === undefined || !Number.isFinite(total)) return DASH;
  const s = Math.max(0, Math.floor(total));
  return String(Math.floor(s / 60)).padStart(2, '0') + ':' + String(s % 60).padStart(2, '0');
}

function humanAge(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) return DASH;
  const s = Math.max(0, Math.floor(seconds));
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm ago';
  return Math.floor(s / 86400) + 'd ago';
}

function shortId(id) {
  if (!id) return DASH;
  const s = String(id);
  return s.length > 22 ? s.slice(0, 19) + '…' : s;
}

/* keep the view pinned to the bottom unless the user scrolled up */
function setLog(pre, lines) {
  const atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 8;
  const text = Array.isArray(lines) && lines.length ? lines.join('\n') : 'unavailable';
  if (pre.textContent !== text) pre.textContent = text;
  if (atBottom) pre.scrollTop = pre.scrollHeight;
}

/* ---------- fetch: 10 s timeout, error envelope, offline detection ---------- */

function apiError(code, message) {
  const err = new Error(message || code);
  err.code = code;
  return err;
}

async function api(path, opts) {
  const o = opts || {};
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), o.timeout || FETCH_TIMEOUT);
  try {
    const init = { method: o.method || 'GET', signal: ctl.signal };
    if (o.body !== undefined) {
      init.headers = { 'content-type': 'application/json' };
      init.body = JSON.stringify(o.body);
    }
    const res = await fetch(path, init);
    const raw = await res.text();
    let data = null;
    if (raw) {
      try { data = JSON.parse(raw); } catch (_) { data = null; }
    }
    if (!res.ok) {
      const env = (data && data.error) || {};
      throw apiError(env.code || 'http_' + res.status, env.message || ('HTTP ' + res.status));
    }
    if (data === null) throw apiError('invalid_response', 'empty or non-JSON response');
    return data;
  } finally {
    clearTimeout(timer);
  }
}

/* network failure or abort, i.e. no server error envelope came back */
function isOffline(err) {
  return !err || err.code === undefined || err.name === 'AbortError';
}

function failText(err) {
  return err && err.code ? err.code + ' — ' + (err.message || '') : String((err && err.message) || err);
}

function setBanner(text, isError) {
  ui.banner.textContent = text || '';
  ui.banner.hidden = !text;
  ui.banner.className = 'banner' + (isError ? ' error' : '');
}

/* ---------- header ---------- */

function anyJobRunning(s) {
  return !!(s && ((s.crawl && s.crawl.state === 'running') ||
    (s.build && s.build.state === 'running') ||
    (s.serve && s.serve.state === 'starting')));
}

const crawlRunning = () => !!(state.server && state.server.crawl && state.server.crawl.state === 'running');
const buildRunning = () => !!(state.server && state.server.build && state.server.build.state === 'running');

function renderHeader() {
  const paths = (state.server && state.server.paths) || {};
  for (const [node, value] of [[ui.pathPrebuilt, paths.prebuilt_data_dir], [ui.pathWork, paths.work_dir]]) {
    node.textContent = show(value);
    node.title = value || '';
  }
  const online = !!state.server && !state.offline;
  const running = online && anyJobRunning(state.server);
  ui.live.textContent = !online ? '● offline' : (running ? '● live' : '● idle');
  ui.live.className = 'live ' + (!online ? 'failed' : (running ? 'running' : 'idle'));
}

/* re-rendering a card every poll would close <details> and steal focus — skip identical payloads.
   ponytail: JSON signature compare, swap for a deep diff only if it ever shows up in a profile. */
function changed(key, data) {
  const sig = JSON.stringify(data);
  if (state.sig[key] === sig) return false;
  state.sig[key] = sig;
  return true;
}

/* ---------- configs ---------- */

function renderConfigs(force) {
  if (!force && !changed('configs', [state.configs, !!state.busy.crawl, crawlRunning()])) return;
  clear(ui.configsBody);
  ui.stopCrawl.disabled = !crawlRunning() || !!state.busy.crawl;
  if (!state.configs) {
    ui.configsBody.appendChild(el('p', 'state', 'unavailable — /api/configs unreachable'));
    return;
  }
  const list = state.configs.configs || [];
  if (!list.length) ui.configsBody.appendChild(el('p', 'state', 'no configs found'));
  for (const cfg of list) {
    const row = el('div', 'config');
    const head = el('div', 'config-head');
    head.appendChild(el('span', 'mono id', show(cfg.id)));
    head.appendChild(cfg.valid ? badge('OK', 'ok') : badge('invalid', 'failed'));
    head.appendChild(el('span', 'spacer'));
    const btn = el('button', 'small', 'start crawl');
    btn.type = 'button';
    btn.disabled = crawlRunning() || !cfg.valid || !!state.busy.crawl;
    btn.title = crawlRunning() ? 'a crawl is already running' : (cfg.valid ? '' : 'config is invalid');
    btn.addEventListener('click', () => startCrawl(cfg.id, btn));
    head.appendChild(btn);
    row.appendChild(head);

    const meta = el('div', 'config-meta');
    meta.appendChild(kv('page_target', cfg.page_target, { mono: true }));
    meta.appendChild(kv('source_count', cfg.source_count, { mono: true }));
    meta.appendChild(kv('embedding_model', cfg.embedding_model, { mono: true }));
    meta.appendChild(kv('state', cfg.valid ? 'valid' : 'invalid', {}));
    row.appendChild(meta);

    const origins = Array.isArray(cfg.origins) ? cfg.origins : [];
    const det = el('details', 'origins');
    det.appendChild(el('summary', null, 'origins (' + origins.length + ')'));
    const ol = el('ol', 'origin-list');
    for (const o of origins) ol.appendChild(el('li', 'mono', String(o)));
    det.appendChild(ol);
    row.appendChild(det);
    if (!cfg.valid && cfg.error) row.appendChild(el('p', 'error-text', cfg.error));
    ui.configsBody.appendChild(row);
  }
}

/* ---------- job cards ---------- */

function jobBadge(node, job, forcedLabel) {
  if (!job || !job.state) {
    node.textContent = 'unavailable';
    node.className = 'badge idle';
    return false;
  }
  node.textContent = job.state === 'stale' ? 'stale — process is gone, identity kept' : (forcedLabel || job.state);
  node.className = 'badge ' + job.state;
  return true;
}

/* elapsed_seconds comes from the server; tick locally between polls */
function trackElapsed(job) {
  return Number.isFinite(job.elapsed_seconds)
    ? { base: job.elapsed_seconds, at: Date.now(), running: job.state === 'running' }
    : null;
}

function renderCrawl() {
  const job = (state.server && state.server.crawl) || null;
  clear(ui.crawlMeta);
  if (!jobBadge(ui.crawlBadge, job)) {
    ui.crawlMeta.appendChild(el('p', 'state', 'unavailable — no crawl state from server'));
    setLog(ui.crawlLog, []);
    return;
  }
  state.crawlElapsed = trackElapsed(job);
  const c = job.counters || {};
  const items = [
    ['config_id', job.config_id, { mono: true }],
    ['elapsed', mmss(job.elapsed_seconds), { mono: true, id: 'crawl-elapsed' }],
    ['captures', c.captures, { mono: true }],
    ['admitted', c.admitted, { mono: true }],
    ['rejected', c.rejected, { mono: true }],
    ['failed', c.failed, { mono: true }],
    ['run_id', job.run_id, { mono: true, title: job.run_id }],
    ['snapshot_id', job.snapshot_id, { mono: true, title: job.snapshot_id }],
    ['pid', job.pid, { mono: true }],
    ['exit_code', job.exit_code, { mono: true }],
    ['last_event', job.last_event, {}],
  ];
  if (job.error) items.push(['error', job.error, {}]);
  ui.crawlMeta.appendChild(kvGrid(items));
  setLog(ui.crawlLog, job.log_tail);
}

function phaseState(name, job) {
  const seen = Array.isArray(job.phases_seen) ? job.phases_seen : [];
  if (job.phase === name) return job.state === 'failed' ? 'failed' : 'current';
  if (job.state === 'completed') return 'done';
  if (job.state === 'failed') return 'failed';
  return seen.indexOf(name) === -1 ? 'pending' : 'done';
}

function renderBuild() {
  const job = (state.server && state.server.build) || null;
  clear(ui.buildMeta);
  clear(ui.buildPhases);
  ui.activateBtn.hidden = true;
  if (!jobBadge(ui.buildBadge, job)) {
    ui.buildMeta.appendChild(el('p', 'state', 'unavailable — no build state from server'));
    setLog(ui.buildLog, []);
    return;
  }
  state.buildElapsed = trackElapsed(job);
  const seen = Array.isArray(job.phases_seen) ? job.phases_seen : [];
  PHASES.forEach((p, i) => {
    if (i) ui.buildPhases.appendChild(el('span', 'phase-sep', '→'));
    const node = el('span', 'phase ' + phaseState(p, job), p);
    if (p === job.phase) node.setAttribute('aria-current', 'step');
    ui.buildPhases.appendChild(node);
  });
  ui.buildPhases.appendChild(el('span', 'muted small', ' (seen: ' + (seen.length ? seen.join(', ') : 'none') + ')'));

  const items = [
    ['snapshot_id', job.snapshot_id, { mono: true, title: job.snapshot_id }],
    ['elapsed', mmss(job.elapsed_seconds), { mono: true, id: 'build-elapsed' }],
    ['phase', job.phase, { mono: true }],
    ['page_count', job.page_count, { mono: true }],
    ['passage_count', job.passage_count, { mono: true }],
    ['generation_id', job.generation_id, { mono: true, title: job.generation_id }],
    ['pid', job.pid, { mono: true }],
    ['exit_code', job.exit_code, { mono: true }],
  ];
  if (job.error) items.push(['error', job.error, {}]);
  ui.buildMeta.appendChild(kvGrid(items));

  ui.activateBtn.hidden = !(job.state === 'completed' && !!job.generation_id);
  ui.activateBtn.disabled = !!state.busy.activate;
  ui.activateBtn.textContent = state.busy.activate ? 'activating…' : 'activate + verify';
  setLog(ui.buildLog, job.log_tail);
}

function renderActivateResult(result) {
  clear(ui.activateResult);
  if (!result) return;
  if (result.error) {
    ui.activateResult.className = 'result-box error';
    ui.activateResult.appendChild(el('p', 'error-text', failText(result.error)));
    return;
  }
  const v = result.verify || {};
  const ok = result.activated === true && v.ok === true;
  ui.activateResult.className = 'result-box ' + (ok ? 'ok' : 'warn');
  ui.activateResult.appendChild(kvGrid([
    ['activated', result.activated === true ? 'yes' : 'no', {}],
    ['verify', ok ? 'ok' : (v.ok === false ? 'failed' : 'unavailable'), {}],
    ['exit_code', v.exit_code, { mono: true }],
  ]));
  ui.activateResult.appendChild(el('pre', 'log small', v.stdout ? String(v.stdout) : 'unavailable'));
  ui.activateResult.appendChild(el('p', 'note', ok ? 'activated and verified' : 'not verified — serve was not confirmed ready'));
}

function renderServe() {
  const serve = (state.server && state.server.serve) || null;
  clear(ui.serveMeta);
  const known = !!serve && !!serve.state;
  ui.serveBadge.textContent = known ? serve.state : 'unavailable';
  ui.serveBadge.className = 'badge ' + (known ? (serve.state === 'running' && serve.ready ? 'ok' : serve.state) : 'idle');
  if (!known) {
    ui.serveMeta.appendChild(el('p', 'state', 'unavailable — no serve state from server'));
    ui.serveStop.disabled = true;
    ui.serveStart.disabled = !!state.busy.serve;
    return;
  }
  const items = [
    ['root', serve.root, { mono: true }],
    ['port', serve.port, { mono: true }],
    ['generation_id', serve.generation_id, { mono: true, title: serve.generation_id }],
    ['ready', serve.ready === true ? 'yes' : (serve.ready === false ? 'no' : DASH), {}],
    ['data_dir', serve.data_dir, { mono: true, title: serve.data_dir }],
    ['pid', serve.pid, { mono: true }],
  ];
  if (serve.error) items.push(['error', serve.error, {}]);
  ui.serveMeta.appendChild(kvGrid(items));
  const live = serve.state === 'running' || serve.state === 'starting';
  const stale = serve.state === 'stale';
  if (stale) ui.serveMeta.appendChild(el('p', 'state', 'stale — adopted across restart; stop and start again'));
  ui.serveStop.disabled = !(live || stale) || !!state.busy.serve;
  ui.serveStart.disabled = live || stale || !!state.busy.serve;
}

/* ---------- snapshots ---------- */

function selectedIs(s) {
  return !!state.selected && state.selected.snapshot_id === s.snapshot_id && state.selected.root === s.root;
}

function renderSnapshots(force) {
  if (!force && !changed('snapshots', [state.snapshots, state.selected, !!state.busy.build, buildRunning()])) return;
  clear(ui.snapshotsBody);
  const snaps = state.snapshots && state.snapshots.snapshots;
  ui.snapshotsNote.textContent = snaps ? snaps.length + ' snapshot(s)' : '';
  if (!snaps) {
    ui.snapshotsBody.appendChild(el('p', 'state', 'unavailable — /api/snapshots unreachable'));
    ui.buildBtn.disabled = true;
    return;
  }
  if (!snaps.length) {
    ui.snapshotsBody.appendChild(el('p', 'state', 'no snapshots yet'));
  } else {
    const table = el('table', 'snap-table');
    const hr = el('tr');
    for (const h of ['snapshot', 'root', 'pages', 'passages', 'created', 'buildable', 'index']) hr.appendChild(el('th', null, h));
    const thead = el('thead');
    thead.appendChild(hr);
    table.appendChild(thead);
    const tbody = el('tbody');
    for (const s of snaps) {
      const tr = el('tr', 'snap-row');
      const hit = selectedIs(s);
      if (hit) tr.classList.add('selected');
      if (s.buildable) tr.classList.add('selectable');
      tr.tabIndex = 0;
      tr.setAttribute('role', 'button');
      tr.setAttribute('aria-pressed', hit ? 'true' : 'false');

      const idCell = el('td', 'mono');
      const idSpan = el('span', null, shortId(s.snapshot_id));
      idSpan.title = show(s.snapshot_id);
      idCell.appendChild(idSpan);
      tr.appendChild(idCell);
      const rootCell = el('td');
      rootCell.appendChild(badge(s.root === 'prebuilt' ? 'prebuilt (read-only)' : 'work', s.root === 'prebuilt' ? 'idle' : 'ok'));
      tr.appendChild(rootCell);
      tr.appendChild(el('td', 'mono', show(s.page_count)));
      tr.appendChild(el('td', 'mono', show(s.passage_count)));
      const created = el('td');
      created.appendChild(el('div', 'mono small', show(s.created_at_utc)));
      created.appendChild(el('div', 'muted small', humanAge(s.age_seconds)));
      tr.appendChild(created);
      tr.appendChild(el('td', null, s.buildable ? 'yes' : 'no'));

      const btn = el('button', 'small', s.buildable ? 'build' : 'read-only');
      btn.type = 'button';
      btn.disabled = !s.buildable || buildRunning() || !!state.busy.build;
      if (s.buildable) {
        btn.addEventListener('click', (ev) => {
          ev.stopPropagation();
          selectSnapshot(s);
          startBuild(btn);
        });
      } else {
        btn.title = 'read-only';
        btn.setAttribute('aria-label', 'build keyword index — read-only');
      }
      const cell = el('td');
      cell.appendChild(btn);
      tr.appendChild(cell);

      if (s.buildable) {
        const choose = () => selectSnapshot(s);
        tr.addEventListener('click', choose);
        tr.addEventListener('keydown', (ev) => {
          if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); choose(); }
        });
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    ui.snapshotsBody.appendChild(table);
  }

  const sel = state.selected;
  ui.buildBtn.disabled = !(sel && sel.buildable) || buildRunning() || !!state.busy.build;
  ui.buildNote.textContent = !sel ? 'select a snapshot first'
    : (sel.buildable ? 'selected ' + shortId(sel.snapshot_id) + ' (' + sel.root + ')' : 'selected snapshot is read-only');
}

function selectSnapshot(s) {
  if (!s.buildable) return;
  state.selected = { snapshot_id: s.snapshot_id, root: s.root, buildable: true };
  try { sessionStorage.setItem('scout.selectedSnapshot', JSON.stringify(state.selected)); } catch (_) { /* private mode */ }
  renderSnapshots();
}

function restoreSelection() {
  try {
    const parsed = JSON.parse(sessionStorage.getItem('scout.selectedSnapshot'));
    if (parsed && parsed.snapshot_id && parsed.root) {
      state.selected = { snapshot_id: parsed.snapshot_id, root: parsed.root, buildable: true };
    }
  } catch (_) { /* nothing remembered */ }
}

/* ---------- generation ---------- */

function generationKind(gen) {
  const kw = Number(gen.keyword_count) || 0;
  const sem = Number(gen.semantic_count) || 0;
  if (sem > 0 && kw > 0) return { label: 'keyword + semantic', cls: 'ok' };
  if (sem > 0) return { label: 'semantic only', cls: 'warn' };
  return { label: 'keyword only', cls: 'idle' };
}

function renderGeneration() {
  clear(ui.generationBody);
  const gen = state.generation;
  if (!gen) {
    ui.generationBody.appendChild(badge('unavailable', 'failed'));
    ui.generationBody.appendChild(el('p', state.generationError ? 'error-text' : 'state',
      state.generationError ? failText(state.generationError) : 'unavailable — start serve to expose a Generation'));
    return;
  }
  const kind = generationKind(gen);
  ui.generationBody.appendChild(badge(kind.label, kind.cls));
  if (gen.status && gen.status !== 'ready') ui.generationBody.appendChild(badge(gen.status, 'warn'));
  ui.generationBody.appendChild(kvGrid([
    ['generation_id', gen.generation_id, { mono: true, title: gen.generation_id }],
    ['corpus_snapshot_id', gen.corpus_snapshot_id, { mono: true, title: gen.corpus_snapshot_id }],
    ['manifest_sha256', shortId(gen.manifest_sha256), { mono: true, title: gen.manifest_sha256 }],
    ['page_count', gen.page_count, { mono: true }],
    ['passage_count', gen.passage_count, { mono: true }],
    ['keyword_count', gen.keyword_count, { mono: true }],
    ['semantic_count', gen.semantic_count, { mono: true }],
  ]));
}

/* ---------- search ---------- */

function setSearchStatus(text, cls) {
  ui.searchStatus.textContent = text || '';
  ui.searchStatus.className = 'note' + (cls ? ' ' + cls : '');
}

function renderSearch(body) {
  clear(ui.searchMeta);
  clear(ui.searchResults);
  if (body) {
    ui.searchMeta.appendChild(kvGrid([
      ['generation_id', body.generation_id, { mono: true, title: body.generation_id }],
      ['mode', body.mode, { mono: true }],
    ]));
  }
  const results = (body && Array.isArray(body.results)) ? body.results : [];
  if (!results.length) {
    ui.searchResults.appendChild(el('p', 'state', 'no results'));
    return;
  }
  for (const r of results) {
    const card = el('article', 'result');
    const head = el('div', 'result-head');
    head.appendChild(el('span', 'rank mono', '#' + show(r.rank)));
    const title = el('a', 'result-title', show(r.title));
    const href = safeHref(r.url);
    if (href) {
      title.href = href;
      title.target = '_blank';
      title.rel = 'noreferrer';
    }
    head.appendChild(title);
    const score = Number(r.score);
    head.appendChild(el('span', 'score mono', Number.isFinite(score) ? score.toFixed(4) : DASH));
    card.appendChild(head);
    const urlLine = el('div', 'result-url mono small', show(r.url));
    urlLine.title = r.url ? String(r.url) : '';
    card.appendChild(urlLine);
    const meta = el('div', 'result-meta small');
    meta.appendChild(el('span', 'mono', 'source: ' + show(r.source_id)));
    if (Array.isArray(r.heading_path) && r.heading_path.length) {
      meta.appendChild(el('span', null, r.heading_path.map(String).join(' › ')));
    }
    card.appendChild(meta);
    card.appendChild(el('p', 'snippet', show(r.snippet)));
    ui.searchResults.appendChild(card);
  }
}

async function doSearch(ev) {
  if (ev) ev.preventDefault();
  const query = ui.searchQuery.value.trim();
  clear(ui.searchMeta);
  clear(ui.searchResults);
  if (!query) {
    setSearchStatus('invalid_request — query is required', 'warn');
    return;
  }
  /* mode is always sent explicitly: an omitted mode defaults to hybrid and 503s on a keyword-only Generation */
  const body = { query, mode: ui.searchMode.value, limit: Number(ui.searchLimit.value) || 10 };
  const source = ui.searchSource.value.trim();
  if (source) body.source = source;

  ui.searchSubmit.disabled = true;
  setSearchStatus('searching…');
  try {
    let data;
    try {
      data = await api('/api/search', { method: 'POST', body });
    } catch (err) {
      /* scout serve handles one request at a time: overloaded is transient, retry once */
      if (err.code !== 'overloaded') throw err;
      await new Promise((r) => setTimeout(r, 400));
      data = await api('/api/search', { method: 'POST', body });
    }
    renderSearch(data);
    const n = Array.isArray(data.results) ? data.results.length : 0;
    setSearchStatus(n ? n + ' result(s)' : 'no results', n ? 'ok' : '');
  } catch (err) {
    const text = err.code === 'overloaded'
      ? "Scout's server serves one request at a time; retry"
      : failText(err);
    setSearchStatus(text, 'error');
    clear(ui.searchResults);
    ui.searchResults.appendChild(el('p', 'state', 'search failed — ' + text));
  } finally {
    ui.searchSubmit.disabled = false;
  }
}

/* ---------- actions ---------- */

function ensureServer() {
  if (!state.server) state.server = { paths: {}, crawl: null, build: null, serve: null };
  return state.server;
}

async function withBusy(name, button, label, note, fn) {
  state.busy[name] = true;
  const oldText = button ? button.textContent : null;
  if (button) {
    button.disabled = true;
    button.textContent = label;
  }
  if (note) note.textContent = '';
  try {
    await fn();
  } finally {
    state.busy[name] = false;
    if (button) {
      button.disabled = false;
      button.textContent = oldText;
    }
    renderAll();
  }
}

async function post(path, body, apply, failTo) {
  try {
    const data = await api(path, { method: 'POST', body });
    apply(data);
    return true;
  } catch (err) {
    if (failTo) failTo.textContent = failText(err);
    else setBanner(failText(err), true);
    return false;
  }
}

function startCrawl(configId, button) {
  return withBusy('crawl', button, 'starting…', ui.configsNote, async () => {
    await post('/api/crawl/start', { config_id: configId },
      (d) => { if (d.crawl) ensureServer().crawl = d.crawl; }, ui.configsNote);
  }).then(pollNow);
}

function stopCrawl() {
  return withBusy('crawl', ui.stopCrawl, 'stopping…', ui.configsNote, async () => {
    await post('/api/crawl/stop', {}, (d) => { if (d.crawl) ensureServer().crawl = d.crawl; }, ui.configsNote);
  }).then(pollNow);
}

function startBuild(button) {
  const sel = state.selected;
  if (!sel) return Promise.resolve();
  return withBusy('build', button, 'building…', ui.buildError, async () => {
    await post('/api/build/start', { snapshot_id: sel.snapshot_id, root: sel.root },
      (d) => {
        if (d.build) ensureServer().build = d.build;
        renderActivateResult(null);
      }, ui.buildError);
  }).then(pollNow);
}

function activate() {
  const job = state.server && state.server.build;
  if (!job || !job.generation_id) return Promise.resolve();
  return withBusy('activate', ui.activateBtn, 'activating…', null, async () => {
    try {
      renderActivateResult(await api('/api/index/activate', { method: 'POST', body: { generation_id: job.generation_id } }));
    } catch (err) {
      renderActivateResult({ error: err });
    }
  }).then(() => poll(true));
}

function startServe(ev) {
  if (ev) ev.preventDefault();
  const port = Number(ui.servePort.value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    ui.serveNote.textContent = 'invalid_request — port must be 1..65535';
    return;
  }
  return withBusy('serve', ui.serveStart, 'starting…', ui.serveNote, async () => {
    await post('/api/serve/start', { root: ui.serveRoot.value, port },
      (d) => { if (d.serve) ensureServer().serve = d.serve; }, ui.serveNote);
  }).then(() => poll(true));
}

function stopServe() {
  return withBusy('serve', ui.serveStop, 'stopping…', ui.serveNote, async () => {
    await post('/api/serve/stop', {}, (d) => { if (d.serve) ensureServer().serve = d.serve; }, ui.serveNote);
  }).then(() => poll(true));
}

/* ---------- polling ---------- */

function renderAll(force) {
  renderHeader();
  renderConfigs(force);
  renderCrawl();
  renderSnapshots(force);
  renderBuild();
  renderServe();
  renderGeneration();
}

let pollTimer = null;

async function refreshGeneration() {
  try {
    state.generation = await api('/api/generation');
    state.generationError = null;
  } catch (err) {
    if (isOffline(err)) return; // keep the last known panel, the banner already says why
    state.generation = null;
    state.generationError = err;
  }
}

function refreshSnapshots() {
  return api('/api/snapshots').then((d) => { state.snapshots = d; }, (err) => { if (!isOffline(err)) state.snapshots = null; });
}

function refreshConfigs() {
  return api('/api/configs').then((d) => { state.configs = d; }, () => { state.configs = null; });
}

async function poll(force) {
  try {
    state.server = await api('/api/state');
    state.offline = false;
    if (force || state.pollCount % 3 === 0 || !state.generation) {
      await Promise.all([refreshGeneration(), refreshSnapshots(), refreshConfigs()]);
    }
    setBanner(null);
  } catch (err) {
    state.offline = true;
    setBanner(isOffline(err) ? 'server unreachable — retrying' : 'state poll failed — ' + failText(err), true);
  }
  state.pollCount += 1;
  renderAll();
  schedule();
}

function pollNow() {
  if (pollTimer) clearTimeout(pollTimer);
  pollTimer = setTimeout(() => poll(false), 0);
}

function schedule() {
  if (pollTimer) clearTimeout(pollTimer);
  pollTimer = setTimeout(() => poll(false), anyJobRunning(state.server) ? POLL_BUSY : POLL_IDLE);
}

function tickElapsed() {
  for (const [key, id] of [['crawlElapsed', 'crawl-elapsed'], ['buildElapsed', 'build-elapsed']]) {
    const entry = state[key];
    const node = $(id);
    if (entry && node) node.textContent = mmss(entry.base + (entry.running ? (Date.now() - entry.at) / 1000 : 0));
  }
}

/* ---------- wire up ---------- */

function init() {
  restoreSelection();
  ui.refresh.addEventListener('click', () => poll(true));
  ui.stopCrawl.addEventListener('click', stopCrawl);
  ui.buildBtn.addEventListener('click', () => startBuild(ui.buildBtn));
  ui.activateBtn.addEventListener('click', activate);
  ui.serveForm.addEventListener('submit', startServe);
  ui.serveStop.addEventListener('click', stopServe);
  ui.searchForm.addEventListener('submit', doSearch);
  renderAll(true);
  poll(true);
  setInterval(tickElapsed, 1000);
}

document.addEventListener('DOMContentLoaded', init);
