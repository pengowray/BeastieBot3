// Frontend wiring for the BeastieBot3 web UI (Phase 1).
//
//   - Click "Load paths"            -> GET /api/paths and render table
//   - Click an enqueue button       -> POST /api/jobs, attach to its SSE stream
//   - SSE chunks                    -> queued into the job-dock terminal (terminal.js)
//
// The terminal handles \r (carriage return) by replacing the current line:
// this lets Spectre.Console progress bars look reasonable in the browser
// without us having to model a full terminal screen.

(function () {
  const $ = (sel) => document.querySelector(sel);
  // The browser's network error text without a trailing full stop (Firefox ends its
  // message with one), so it can go in the middle of a sentence.
  const fetchErrorText = (e) => String((e && e.message) || e).replace(/\.+$/, '');

  // The error text of a failed API response. The endpoints answer with JSON such as
  // { "error": "...", "hint": "..." }; anything else (an HTML or plain-text 500) is shown raw,
  // cut to its first line.
  async function responseErrorText(res) {
    let text = '';
    try { text = await res.text(); } catch (_) { /* no body */ }
    try {
      const body = JSON.parse(text);
      if (body && typeof body.error === 'string' && body.error) {
        return body.error + (typeof body.hint === 'string' && body.hint ? ' ' + body.hint : '');
      }
    } catch (_) { /* not JSON */ }
    const firstLine = text.trim().split('\n')[0].trim();
    if (firstLine) return firstLine.length > 200 ? firstLine.slice(0, 200) + '…' : firstLine;
    return 'HTTP error ' + res.status;
  }

  // --- Settings table -------------------------------------------------

  $('#load-paths').addEventListener('click', async () => {
    const res = await fetch('/api/paths');
    if (!res.ok) { alert('Could not load paths from paths.ini: HTTP error ' + res.status + '.'); return; }
    const data = await res.json();
    $('#paths-source').textContent = data.source;
    const tbody = document.querySelector('#paths-table tbody');
    tbody.innerHTML = '';
    for (const [k, v] of Object.entries(data.values)) {
      const tr = document.createElement('tr');
      const tdK = document.createElement('td'); tdK.textContent = k;
      const tdV = document.createElement('td'); tdV.textContent = v;
      tr.appendChild(tdK); tr.appendChild(tdV);
      tbody.appendChild(tr);
    }
    $('#paths-table').hidden = false;
  });

  // --- Job runner -----------------------------------------------------

  const jobDock = $('#job-dock');
  const dockToggle = $('#dock-toggle');
  const dockClose = $('#dock-close');
  const jobTitle = $('#job-title');
  const jobStatus = $('#job-status');
  const jobOutput = $('#job-output');
  const jobCancel = $('#job-cancel');
  let currentEventSource = null;
  let currentJobId = null;
  // Goes up by one each time the dock switches to another job (run or reopened). A response
  // that arrives after a switch belongs to the job shown before it and is ignored.
  let dockGeneration = 0;

  // --- Persistent dock --------------------------------------------------
  // The dock lives outside the view container so a running job stays visible
  // (and streaming) while the user navigates between views — "run a task,
  // browse elsewhere, flip back".

  function setDockExpanded(expanded) {
    jobDock.classList.toggle('collapsed', !expanded);
    dockToggle.textContent = expanded ? '▾' : '▸';
  }
  function showDock(expanded) {
    jobDock.hidden = false;
    setDockExpanded(expanded !== false);
  }
  dockToggle.addEventListener('click', () => {
    setDockExpanded(jobDock.classList.contains('collapsed'));
  });
  dockClose.addEventListener('click', () => {
    if (currentEventSource) { currentEventSource.close(); currentEventSource = null; }
    jobDock.hidden = true;
  });

  const terminal = createTerminal(jobOutput);

  // A job's error field: the message of the exception that stopped the command, or
  // "[interrupted by server restart]" for a job that was running when serve stopped. The
  // brackets are dropped for display.
  function jobErrorText(error) {
    if (!error) return '';
    return String(error).trim().replace(/^\[([^\]]*)\]$/, '$1');
  }

  // First line of a text, cut to at most max characters.
  function shorten(text, max) {
    const line = String(text).split('\n')[0].trim();
    return line.length > max ? line.slice(0, max - 1) + '…' : line;
  }

  // "failed (exit code 1)". A succeeded job always exits with 0, so the exit code is shown
  // only for the others.
  function jobStatusText(j) {
    return j.status + (j.exitCode != null && j.status !== 'succeeded' ? ' (exit code ' + j.exitCode + ')' : '');
  }

  // The dock bar has room for a short error only (Cancel and Close sit beside it); the full
  // text is in the tooltip.
  function setStatus(status, error) {
    const err = jobErrorText(error);
    jobStatus.textContent = status + (err ? ': ' + shorten(err, 60) : '');
    jobStatus.title = err;
    jobStatus.className = 'status ' + status;
    const cancellable = status === 'pending' || status === 'running';
    jobCancel.hidden = !cancellable;
    jobCancel.disabled = !cancellable;
  }

  jobCancel.addEventListener('click', async () => {
    if (!currentJobId) return;
    jobCancel.disabled = true;
    try {
      const res = await fetch('/api/jobs/' + currentJobId + '/cancel', { method: 'POST' });
      if (!res.ok) {
        alert('Cancel failed: ' + await responseErrorText(res));
        jobCancel.disabled = false;
      }
      // Otherwise wait for the SSE 'status' event to flip us to cancelled.
    } catch (e) {
      alert('Cancel error: ' + e.message);
      jobCancel.disabled = false;
    }
  });

  // Posts a job and opens it in the job panel at the bottom of the window. Returns the queued job
  // ({ id, ... }), or null when the job did not start (the job panel then shows the error).
  // Also used by rules-editor.js through window.Beastie.
  async function enqueue(command, args) {
    if (currentEventSource) {
      currentEventSource.close();
      currentEventSource = null;
    }
    // The new job has no id until the POST returns. Until then Cancel does nothing, rather than
    // cancelling the job the dock showed before.
    currentJobId = null;
    const generation = ++dockGeneration;
    showDock(true);
    jobTitle.textContent = '$ beastiebot3 ' + command + (args && args.length ? ' ' + args.join(' ') : '');
    terminal.reset();
    setStatus('pending');

    let res;
    try {
      res = await fetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command: command, args: args || [] }),
      });
    } catch (e) {
      if (generation !== dockGeneration) return null;
      terminal.setText('Failed to start job: no reply from `serve` (' + fetchErrorText(e) + '). Check that `serve` is running.\n');
      setStatus('failed');
      return null;
    }
    if (!res.ok) {
      const errText = await responseErrorText(res);
      if (generation !== dockGeneration) return null;
      terminal.setText('Failed to start job: ' + errText + '\n');
      setStatus('failed');
      return null;
    }
    const job = await res.json();
    // The job was queued, but another job was opened in the dock meanwhile. Leave the dock on
    // that one; the new job is in the Jobs list.
    if (generation !== dockGeneration) { refreshJobList(); return job; }
    setStatus('running');
    attachStream(job.id);
    refreshJobList();
    return job;
  }

  function attachStream(jobId) {
    // Never leave two streams writing into the one terminal.
    if (currentEventSource) currentEventSource.close();
    currentJobId = jobId;
    const es = new EventSource('/api/jobs/' + jobId + '/stream');
    currentEventSource = es;
    // The server replays the whole history on every connection, and EventSource
    // reconnects by itself, so a dropped connection on a long job would append
    // a second copy of everything. Start over on any open after the first.
    let opened = false;
    es.onopen = () => {
      if (opened) terminal.reset();
      opened = true;
    };
    // Chunks arrive JSON-encoded so \r survives SSE's own line framing.
    es.addEventListener('chunk', (e) => {
      try { terminal.append(JSON.parse(e.data)); } catch (_) { terminal.append(e.data); }
    });
    es.addEventListener('status', (e) => {
      try {
        const j = JSON.parse(e.data);
        setStatus(j.status, j.error);
      } catch (_) { /* ignore */ }
    });
    es.addEventListener('done', () => {
      terminal.finish();
      es.close();
      currentEventSource = null;
      refreshJobList();
    });
    es.onerror = () => {
      // Either the job finished and the server closed the stream, or the
      // connection dropped. EventSource will auto-retry; the 'done' event
      // (if it fired) already triggered close() above.
    };
  }

  // (Legacy hardcoded enqueue buttons removed; the command browser below
  //  generates buttons dynamically from /api/commands.)

  // --- Recent jobs ----------------------------------------------------

  async function refreshJobList() {
    const res = await fetch('/api/jobs');
    if (!res.ok) return;
    const jobs = await res.json();
    const ul = $('#job-list');
    ul.innerHTML = '';
    if (jobs.length === 0) {
      const li = document.createElement('li');
      li.className = 'muted';
      li.textContent = 'No jobs yet.';
      ul.appendChild(li);
      return;
    }
    for (const j of jobs.slice(0, 25)) {
      const li = document.createElement('li');
      const left = document.createElement('span');
      left.className = 'job-cmd';
      const link = document.createElement('a');
      link.href = '#';
      link.textContent = j.commandLine;
      link.addEventListener('click', (e) => {
        e.preventDefault();
        replayJob(j.id, j.commandLine);
      });
      left.appendChild(link);

      // Why the job failed, when the server recorded a reason (the full text is in the tooltip).
      const err = jobErrorText(j.error);
      if (err) {
        const errEl = document.createElement('div');
        errEl.className = 'small error';
        errEl.textContent = shorten(err, 160);
        errEl.title = err;
        left.appendChild(errEl);
      }

      const right = document.createElement('span');
      right.className = 'job-meta';

      const time = document.createElement('time');
      time.className = 'job-time muted small';
      if (j.createdAt) {
        time.dateTime = j.createdAt;
        time.title = jobTimesTooltip(j);
        const dur = jobDuration(j);
        time.textContent = formatRelative(j.createdAt) + (dur ? ' · ' + dur : '');
      }

      const status = document.createElement('span');
      status.className = 'status ' + j.status;
      status.textContent = jobStatusText(j);

      right.appendChild(time);
      right.appendChild(status);
      li.appendChild(left);
      li.appendChild(right);
      ul.appendChild(li);
    }
  }

  // Wall-clock run time: started→completed, or started→now while running.
  // A job that was running when serve stopped is marked failed at the next start, with no exit
  // code (every job that ends normally or is cancelled has one). Its completedAt is that restart.
  function wasInterrupted(j) {
    return j.status === 'failed' && j.exitCode == null;
  }

  function jobDuration(j) {
    // Start to restart would overstate how long an interrupted job ran.
    if (!j.startedAt || wasInterrupted(j)) return '';
    const start = new Date(j.startedAt).getTime();
    const end = j.completedAt ? new Date(j.completedAt).getTime() : Date.now();
    return formatDuration(end - start);
  }

  function formatDuration(ms) {
    if (ms == null || ms < 0) return '';
    const s = ms / 1000;
    if (s < 1) return Math.round(ms) + 'ms';
    if (s < 60) return s.toFixed(s < 10 ? 1 : 0) + 's';
    const m = Math.floor(s / 60);
    if (m < 60) return m + 'm ' + Math.round(s % 60) + 's';
    const h = Math.floor(m / 60);
    return h + 'h ' + (m % 60) + 'm';
  }

  function formatAbsolute(isoDate) {
    return isoDate ? new Date(isoDate).toLocaleString() : '';
  }

  function jobTimesTooltip(j) {
    const lines = [];
    if (j.createdAt) lines.push('queued ' + formatAbsolute(j.createdAt));
    if (j.startedAt) lines.push('started ' + formatAbsolute(j.startedAt));
    if (j.completedAt) lines.push((wasInterrupted(j) ? 'marked interrupted at server restart ' : 'finished ') + formatAbsolute(j.completedAt));
    return lines.join('\n');
  }

  // commandLine is optional: callers that have it show it at once instead of leaving the
  // previous job's command above this job's output until the job record arrives.
  function replayJob(id, commandLine) {
    if (currentEventSource) {
      currentEventSource.close();
      currentEventSource = null;
    }
    const generation = ++dockGeneration;
    showDock(true);
    jobTitle.textContent = commandLine ? '$ beastiebot3 ' + commandLine : '';
    terminal.reset();
    setStatus('running');
    attachStream(id);
    fetch('/api/jobs/' + id).then(r => r.ok ? r.json() : null).then(j => {
      // Another job may have been opened while this request was in flight.
      if (!j || generation !== dockGeneration) return;
      jobTitle.textContent = '$ beastiebot3 ' + j.commandLine;
      // A finished job's status cannot change, so show it (and its error) now rather than
      // "running" until the stream has replayed the whole output.
      if (j.status === 'succeeded' || j.status === 'failed' || j.status === 'cancelled') {
        setStatus(j.status, j.error);
      }
    }).catch(() => { /* the stream's status event still sets the status */ });
  }

  $('#refresh-jobs').addEventListener('click', refreshJobList);
  refreshJobList();

  // --- Status dashboard ----------------------------------------------

  function formatBytes(n) {
    if (n == null) return '';
    if (n < 1024) return n + ' B';
    const units = ['KB', 'MB', 'GB', 'TB'];
    let v = n / 1024, i = 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return v.toFixed(v >= 10 ? 0 : 1) + ' ' + units[i];
  }

  function formatRelative(isoDate) {
    if (!isoDate) return '';
    const t = new Date(isoDate).getTime();
    const diff = (Date.now() - t) / 1000;
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    if (diff < 86400 * 30) return Math.floor(diff / 86400) + 'd ago';
    if (diff < 86400 * 365) return Math.floor(diff / (86400 * 30)) + 'mo ago';
    return Math.floor(diff / (86400 * 365)) + 'y ago';
  }

  function formatNumber(n) {
    if (n == null) return '—';
    return n.toLocaleString();
  }

  // Counts that turn a data source's pill amber when above 0: Wikidata items waiting to be
  // downloaded, IUCN assessments waiting to be downloaded, and failed IUCN API requests. The
  // other counts are records that stay above 0 for good (Wikidata items linked to taxa by name,
  // titles with no Wikipedia article), so they set no pill.
  // Keys are the metric labels from DataSourceDescriptor.cs, exactly; values are the pill text
  // that follows the count, e.g. "3 failed requests". The pill shows the first of these counts
  // above 0, in the card's metric order.
  const ATTENTION_PILL_TEXT = new Map([
    ['pending download', 'items to download'],
    ['assessments to download', 'assessments to download'],
    ['failed requests', 'failed requests'],
  ]);

  function statusKind(s) {
    if (!s.exists) return { cls: 'missing', text: 'missing' };
    if (s.error) return { cls: 'err', text: 'error' };
    // The pill shows the first attention count above 0, e.g. "3 failed requests".
    for (const m of (s.metrics || [])) {
      if (ATTENTION_PILL_TEXT.has(m.label) && m.value && m.value > 0) {
        return { cls: 'warn', text: formatNumber(m.value) + ' ' + ATTENTION_PILL_TEXT.get(m.label) };
      }
      if (m.error) return { cls: 'err', text: 'error' };
    }
    return { cls: 'ok', text: 'ok' };
  }

  function renderStatusItem(s) {
    const li = document.createElement('li');
    li.className = 'status-item';

    const head = document.createElement('div');
    head.className = 'status-item-head';

    const icon = document.createElement('span');
    icon.className = 'status-icon';
    icon.textContent = s.kind === 'directory' ? '📁' : '🗄';
    head.appendChild(icon);

    const name = document.createElement('span');
    name.className = 'name';
    name.textContent = s.name;
    head.appendChild(name);

    if (s.description) {
      const d = document.createElement('span');
      d.className = 'desc';
      d.textContent = s.description;
      head.appendChild(d);
    }

    const kind = statusKind(s);
    const pill = document.createElement('span');
    pill.className = 'status-pill ' + kind.cls;
    pill.textContent = kind.text;
    head.appendChild(pill);

    li.appendChild(head);

    // Meta: path · size · mtime
    const meta = document.createElement('div');
    meta.className = 'status-meta';
    const parts = [];
    if (s.path) parts.push('<span title="' + escapeHtml(s.path) + '">' + escapeHtml(s.path) + '</span>');
    if (s.sizeBytes != null) parts.push(formatBytes(s.sizeBytes));
    if (s.lastModified) parts.push('updated ' + formatRelative(s.lastModified));
    if (!s.exists) parts.push('(not present)');
    meta.innerHTML = parts.join('<span class="sep">·</span>');
    li.appendChild(meta);

    // Metrics grid
    if (s.metrics && s.metrics.length > 0) {
      const grid = document.createElement('div');
      grid.className = 'status-metrics';
      for (const m of s.metrics) {
        const row = document.createElement('div');
        row.className = 'row';
        const lbl = document.createElement('span');
        lbl.className = 'label';
        lbl.textContent = m.label;
        const val = document.createElement('span');
        val.className = 'value';
        if (m.error) {
          val.classList.add('error');
          val.textContent = 'err';
          val.title = m.error;
        } else if (m.value == null) {
          val.classList.add('na');
          val.textContent = m.note || 'n/a';
          if (m.note) val.title = m.note;
        } else {
          val.textContent = formatNumber(m.value);
          if (m.value === 0) val.classList.add('zero');
          if (ATTENTION_PILL_TEXT.has(m.label) && m.value > 0) {
            val.classList.add('attention');
          }
        }
        row.appendChild(lbl);
        row.appendChild(val);
        grid.appendChild(row);
      }
      li.appendChild(grid);
    }

    if (s.error) {
      const err = document.createElement('div');
      err.className = 'status-error';
      err.textContent = s.error;
      li.appendChild(err);
    }

    return li;
  }

  async function refreshStatus() {
    const generatedEl = $('#status-generated');
    generatedEl.textContent = 'Refreshing…';
    try {
      const res = await fetch('/api/status');
      if (!res.ok) { generatedEl.textContent = 'Refresh failed: HTTP error ' + res.status; return; }
      const data = await res.json();
      const ul = $('#status-list');
      ul.innerHTML = '';
      for (const s of data.sources) ul.appendChild(renderStatusItem(s));
      generatedEl.textContent = 'Generated ' + new Date(data.generatedAt).toLocaleTimeString();
    } catch (e) {
      generatedEl.textContent = 'Refresh failed: ' + fetchErrorText(e) + '. Check that `serve` is running.';
    }
    // IUCN version (local-only, no live call) + dataset comparison ride along
    // with the sources refresh.
    refreshIucnVersion();
    refreshColVersion();
    refreshDatasetCompare();
  }

  // --- IUCN version freshness + CSV-vs-API dataset comparison --------------

  async function refreshIucnVersion(opts) {
    const body = $('#iucn-version-body');
    if (!body) return;
    const refresh = opts && opts.refresh;
    if (refresh) body.textContent = 'Checking live IUCN API…';
    try {
      const res = await fetch('/api/iucn-version' + (refresh ? '?refresh=1' : ''));
      if (!res.ok) { body.textContent = 'Could not load this card: HTTP error ' + res.status + '. Click Refresh to retry.'; return; }
      renderIucnVersion(await res.json());
    } catch (e) { body.textContent = 'Could not load this card: ' + fetchErrorText(e) + '. Check that `serve` is running.'; }
  }

  function renderIucnVersion(d) {
    const body = $('#iucn-version-body');
    if (!body) return;
    let pillCls = 'missing', pillText = 'not checked';
    if (d.fresh === true) { pillCls = 'ok'; pillText = 'up to date'; }
    else if (d.fresh === false) { pillCls = 'warn'; pillText = 'out of date'; }
    const bits = [];
    bits.push('<span>Local imported version: <strong>' + escapeHtml(String(d.local || '(none imported)')) + '</strong></span>');
    if (d.latest) bits.push('<span class="sep">·</span><span>Latest published: <strong>' + escapeHtml(d.latest) + '</strong></span>');
    bits.push('<span class="status-pill ' + pillCls + '">' + pillText + '</span>');
    let html = '<div class="iucn-version-row">' + bits.join(' ') + '</div>';
    if (!d.hasToken) html += '<p class="small muted">Set IUCN_API_TOKEN in <code>.env</code> to compare against the live IUCN API.</p>';
    else if (d.error) html += '<p class="small reason">Live check error: ' + escapeHtml(d.error) + '</p>';
    else if (!d.latest) html += '<p class="small muted">Click “Check live version” to query the IUCN API.</p>';
    else if (d.checkedAt) html += '<p class="small muted">Checked ' + formatRelative(d.checkedAt) + '.</p>';
    body.innerHTML = html;
  }

  async function refreshDatasetCompare() {
    const body = $('#dataset-compare-body');
    if (!body) return;
    try {
      const res = await fetch('/api/dataset-compare');
      if (!res.ok) { body.textContent = 'Could not load this card: HTTP error ' + res.status + '. Click Refresh to retry.'; return; }
      renderDatasetCompare(await res.json());
    } catch (e) { body.textContent = 'Could not load this card: ' + fetchErrorText(e) + '. Check that `serve` is running.'; }
  }

  function renderDatasetCompare(d) {
    const body = $('#dataset-compare-body');
    if (!body) return;
    const csv = d.csv || {}, api = d.api || {};
    if (!csv.exists && !api.exists) { body.textContent = 'Neither IUCN dataset is available.'; return; }
    const num = (v) => (v == null ? '—' : Number(v).toLocaleString());
    let html = '<table class="compare-table"><thead><tr><th></th><th>CSV release</th><th>API projection</th><th>API minus CSV</th></tr></thead><tbody>';
    // "built" is false for a missing file, an unreadable one, and an API projection whose last
    // build did not finish (project-view empties the projection first, so it holds nothing).
    const apiUnfinished = !api.built && api.unfinishedBuildStartedAt;
    html += '<tr><td class="ct-label">Version</td><td>' + escapeHtml(String(csv.built ? (csv.version || '—') : '—')) +
            '</td><td>' + escapeHtml(String(api.built ? (api.version || '—') : 'not built')) + '</td><td></td></tr>';
    html += '<tr><td class="ct-label">Updated</td><td>' + (csv.lastModified ? formatRelative(csv.lastModified) : '—') +
            '</td><td>' + (api.lastModified ? formatRelative(api.lastModified) : '—') + '</td><td></td></tr>';
    // Coverage: the CSV release is always complete; the API projection may be partial
    // when some taxa's latest assessment JSON wasn't downloaded before project-view ran.
    const apiCoverage = apiUnfinished ? '<span class="agree warn">empty: the last build did not finish</span>'
      : !api.built ? '—'
      : api.partial === true
        ? '<span class="agree warn">partial' + (api.latestNotDownloaded != null ? ' (' + num(api.latestNotDownloaded) + ' assessments not downloaded)' : '') + '</span>'
        : api.partial === false ? '<span class="agree ok">complete</span>' : '—';
    html += '<tr><td class="ct-label">Coverage</td><td>' + (csv.built ? '<span class="agree ok">complete</span>' : '—') +
            '</td><td>' + apiCoverage + '</td><td></td></tr>';
    for (const row of (d.comparison || [])) {
      let mark = '';
      if (row.csv != null && row.api != null) {
        mark = row.equal
          ? '<span class="agree ok">✓</span>'
          : '<span class="agree warn">' + (row.delta > 0 ? '+' : '') + Number(row.delta).toLocaleString() + '</span>';
      }
      html += '<tr class="' + (row.category ? 'ct-cat' : 'ct-metric') + '"><td class="ct-label">' +
              escapeHtml(row.label) + '</td><td>' + num(row.csv) + '</td><td>' + num(row.api) + '</td><td>' + mark + '</td></tr>';
    }
    html += '</tbody></table>';
    if (apiUnfinished) {
      html += '<p class="small reason">API projection is empty: its last build, started ' +
              escapeHtml(formatAbsolute(api.unfinishedBuildStartedAt)) + ' (' + formatRelative(api.unfinishedBuildStartedAt) +
              '), did not finish. Run <code>iucn api project-view</code> to build it again.</p>';
    } else if (!api.built) {
      html += '<p class="small muted">API projection not built. Run <code>iucn api project-view</code> ' +
              '(after <code>iucn api cache-all</code>) to enable <code>--dataset api</code>.</p>';
    } else if (api.partial === true) {
      html += '<p class="small muted">API projection is <strong>partial</strong>' +
              (api.latestNotDownloaded != null ? ' — ' + num(api.latestNotDownloaded) + ' assessments not downloaded' : '') +
              '. Run <code>iucn api cache-assessments</code> then <code>iucn api project-view</code> for full coverage.</p>';
    }
    body.innerHTML = html;
  }

  const iucnVersionCheck = $('#iucn-version-check');
  if (iucnVersionCheck) iucnVersionCheck.addEventListener('click', () => refreshIucnVersion({ refresh: true }));

  // --- Catalogue of Life version freshness (offline: loaded DB vs input folder) ---

  function colVersionPill(d) {
    switch (d.status) {
      case 'fresh':            return { cls: 'ok',      text: 'up to date' };
      case 'update-available': return { cls: 'warn',    text: 'update available' };
      case 'not-imported':     return { cls: 'missing', text: 'not imported' };
      case 'incomplete':       return { cls: 'err',     text: 'import incomplete' };
      case 'no-input':         return { cls: 'missing', text: 'input folder missing' };
      default:                 return { cls: 'missing', text: 'unknown' };
    }
  }

  // Shared markup for both the Data sources card and the col-update flow banner.
  function colVersionHtml(d) {
    const p = colVersionPill(d);
    const bits = [];
    bits.push('<span>Loaded: <strong>' + escapeHtml(String(d.loaded || '(none imported)')) + '</strong></span>');
    if (d.input) bits.push('<span class="sep">·</span><span>Input folder: <strong>' + escapeHtml(String(d.input)) + '</strong></span>');
    bits.push('<span class="status-pill ' + p.cls + '">' + p.text + '</span>');
    let html = '<div class="iucn-version-row">' + bits.join(' ') + '</div>';
    if (d.message) html += '<p class="small ' + (d.fresh === false ? 'reason' : 'muted') + '">' + escapeHtml(d.message) + '</p>';
    return html;
  }

  function renderColVersion(d) {
    const body = $('#col-version-body');
    if (body) body.innerHTML = colVersionHtml(d);
  }

  async function refreshColVersion(opts) {
    const body = $('#col-version-body');
    if (!body) return;
    const refresh = opts && opts.refresh;
    if (refresh) body.textContent = 'Re-reading input folder…';
    try {
      const res = await fetch('/api/col-version' + (refresh ? '?refresh=1' : ''));
      if (!res.ok) { body.textContent = 'Could not load this card: HTTP error ' + res.status + '. Click Refresh to retry.'; return; }
      renderColVersion(await res.json());
    } catch (e) { body.textContent = 'Could not load this card: ' + fetchErrorText(e) + '. Check that `serve` is running.'; }
  }

  const colVersionCheck = $('#col-version-check');
  if (colVersionCheck) colVersionCheck.addEventListener('click', () => refreshColVersion({ refresh: true }));

  $('#refresh-status').addEventListener('click', refreshStatus);
  refreshStatus();

  // Polling is owned by router.js, which refreshes the active view (plus the
  // always-cheap status/jobs/flow) on an interval and pauses on hidden tabs.

  async function refreshActiveFlow() {
    // Re-fetch the snapshot for the currently-selected flow tab so step
    // timestamps, running indicators and latest-output links stay live.
    // Expanded steps are preserved across re-renders so an open drawer
    // does not flicker shut underneath the user.
    if (!activeFlowId) return;
    try {
      const res = await fetch('/api/flows/' + encodeURIComponent(activeFlowId));
      if (!res.ok) return;
      renderFlow(await res.json());
    } catch (_) { /* silent — next tick will retry */ }
  }

  // --- Command browser ----------------------------------------------
  //
  // Fetches /api/commands once, then renders a filterable tree where each
  // row expands into an auto-generated form built from the command's
  // [CommandOption] properties on its Settings type.

  let allCommands = [];
  let expandedPath = null;

  async function loadCommands() {
    try {
      const res = await fetch('/api/commands');
      if (!res.ok) {
        $('#command-tree').textContent = 'Failed to load commands: HTTP error ' + res.status;
        return;
      }
      allCommands = await res.json();
      renderCommandTree();
    } catch (e) {
      $('#command-tree').textContent = 'Failed to load commands: ' + fetchErrorText(e) + '. Check that `serve` is running.';
    }
  }

  function activeKinds() {
    return new Set(
      Array.from(document.querySelectorAll('#cmd-kind-filter input:checked'))
        .map(cb => cb.value)
    );
  }

  function renderCommandTree() {
    const root = $('#command-tree');
    root.innerHTML = '';
    const search = $('#cmd-search').value.trim().toLowerCase();
    const kinds = activeKinds();
    const filtered = allCommands.filter(c =>
      kinds.has(c.kind) &&
      (!search ||
       c.path.toLowerCase().includes(search) ||
       (c.description || '').toLowerCase().includes(search))
    );

    if (filtered.length === 0) {
      const p = document.createElement('p');
      p.className = 'muted';
      p.textContent = 'No commands match.';
      root.appendChild(p);
      return;
    }

    // Group by branch (everything except the final segment).
    const groups = new Map();
    for (const c of filtered) {
      const key = c.branch || '(top level)';
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(c);
    }

    for (const [branch, cmds] of groups) {
      const sec = document.createElement('div');
      sec.className = 'cmd-branch';
      const h = document.createElement('p');
      h.className = 'cmd-branch-name';
      h.textContent = branch;
      sec.appendChild(h);
      for (const c of cmds) sec.appendChild(renderCommandRow(c));
      root.appendChild(sec);
    }
  }

  // Human-readable "what happens if I run this (again)?" hint, keyed off the
  // command's `rerun` effect (served by /api/commands; RerunEffect in
  // CommandClassification.cs). Orthogonal to `kind`. Each hint must be true for every
  // command with that effect; command-specific detail goes in the command's rerunNote.
  // CommandClassificationTests checks that every RerunEffect has an entry here.
  const EFFECTS = {
    readonly:       { label: 'read-only', cls: 'readonly', icon: '👁', hint: 'Reads data and shows the results or writes them to files. Changes none of the downloaded or imported data.' },
    idempotentadd:  { label: 'adds or updates records', cls: 'add', icon: '＋', hint: 'A run adds the records missing from this command\'s cache, store or download queue, and can update records already there.' },
    discovers:      { label: 'finds new records', cls: 'discovers', icon: '🔍', hint: 'Searches the IUCN Red List API or Wikidata, and adds the records it finds to the cache or to the download queue.' },
    rebuilds:       { label: 'rebuilds output', cls: 'rebuilds', icon: '🔁', hint: 'Each run rebuilds the output from data already stored locally, and replaces the output of the previous run.' },
    plansdownloads: { label: 'changes later downloads', cls: 'queue', icon: '📋', hint: 'Changes which records the IUCN API or Wikipedia download commands fetch on later runs. Downloads nothing, and deletes no downloaded data.' },
    clearscache:    { label: 'clears cache', cls: 'fresh', icon: '🧹', hint: 'Deletes downloaded data from the cache.' },
    imports:        { label: 're-import needs --force', cls: 'fresh', icon: '🗄', hint: 'Imports downloaded files into a database. Running the command again skips files already imported. With --force, the command deletes the imported data and imports the files again.' },
    publishes:      { label: 'edits Wikipedia', cls: 'fresh', icon: '✎', hint: 'Saves pages on English Wikipedia, logged in with the bot password in WIKIPEDIA_BOT_USERNAME and WIKIPEDIA_BOT_PASSWORD. Changes none of the downloaded or imported data.' },
  };
  function effectInfo(cmd) { return EFFECTS[cmd.rerun] || null; }

  // Whether a run with these options only reports (CommandInfo ReportOnlyWith / ChangesOnlyWith,
  // served with every alias): "iucn api cache-all --full --status", or "wikipedia prune-queue"
  // without --apply. Used for Workflows buttons, whose options are fixed.
  function isReportOnlyRun(meta, args) {
    const names = args.filter(a => a.startsWith('-')).map(a => a.split('=')[0]);
    const hasAny = (list) => (list || []).some(o => names.includes(o));
    if (hasAny(meta.reportOnlyWith)) return true;
    return (meta.changesOnlyWith || []).length > 0 && !hasAny(meta.changesOnlyWith);
  }

  function renderCommandRow(cmd) {
    const wrap = document.createElement('div');

    const row = document.createElement('div');
    row.className = 'cmd-row';
    if (cmd.path === expandedPath) row.classList.add('expanded');

    const caret = document.createElement('span');
    caret.className = 'caret';
    caret.textContent = cmd.path === expandedPath ? '▾' : '▸';
    row.appendChild(caret);

    const name = document.createElement('span');
    name.className = 'name';
    // Show only the last segment in the row (branch label is above).
    const idx = cmd.path.lastIndexOf(' ');
    name.textContent = idx < 0 ? cmd.path : cmd.path.substring(idx + 1);
    row.appendChild(name);

    const desc = document.createElement('span');
    desc.className = 'desc';
    desc.textContent = cmd.description || '';
    row.appendChild(desc);

    const badge = document.createElement('span');
    badge.className = 'kind-badge ' + cmd.kind;
    badge.textContent = cmd.kind;
    row.appendChild(badge);

    // Secondary effect pill (skip for read-only — `kind` already conveys it).
    const eff = effectInfo(cmd);
    if (eff && cmd.rerun !== 'readonly') {
      const effBadge = document.createElement('span');
      effBadge.className = 'effect-badge ' + eff.cls;
      effBadge.textContent = eff.label;
      effBadge.title = eff.hint;
      row.appendChild(effBadge);
    }

    row.addEventListener('click', () => {
      expandedPath = expandedPath === cmd.path ? null : cmd.path;
      renderCommandTree();
    });

    wrap.appendChild(row);
    if (cmd.path === expandedPath) wrap.appendChild(buildForm(cmd));
    return wrap;
  }

  function buildForm(cmd) {
    const form = document.createElement('form');
    form.className = 'cmd-form';
    form.addEventListener('click', (e) => e.stopPropagation());

    const fullDesc = document.createElement('p');
    fullDesc.className = 'full-desc';
    fullDesc.textContent = cmd.description || '';
    form.appendChild(fullDesc);

    // Which runs of a destructive command delete what. Hidden while the preflight box is
    // showing, because that box says what this particular run deletes.
    let reasonEl = null;
    if (cmd.kind === 'destructive' && cmd.reason) {
      const r = document.createElement('p');
      r.className = 'reason';
      r.textContent = '⚠ ' + cmd.reason;
      r.hidden = cmd.confirm === 'always' || cmd.confirm === 'files';
      form.appendChild(r);
      reasonEl = r;
    }

    // "What happens if I run this?" effect hint + any command-specific note.
    const eff = effectInfo(cmd);
    if (eff) {
      const h = document.createElement('p');
      h.className = 'effect-hint ' + eff.cls;
      h.textContent = eff.icon + ' ' + eff.hint + (cmd.rerunNote ? ' ' + cmd.rerunNote : '');
      form.appendChild(h);
    }

    const fields = cmd.form.fields || [];
    if (fields.length === 0) {
      const empty = document.createElement('p');
      empty.className = 'empty-form';
      empty.textContent = 'No options. Click Run.';
      form.appendChild(empty);
    } else {
      const grid = document.createElement('div');
      grid.className = 'fields';
      for (const f of fields) renderField(grid, f);
      form.appendChild(grid);
    }

    // Contextual redundancy warning: --force / --max-age-hours re-fetch already-
    // cached entries. Driven by the live form state (no per-command metadata).
    // Hidden while the preflight box below is showing: that box describes this run exactly.
    const forceWarn = document.createElement('p');
    forceWarn.className = 'force-warn';
    forceWarn.hidden = true;
    form.appendChild(forceWarn);
    let preflightShown = false;
    const updateForceWarn = () => {
      if (preflightShown) { forceWarn.hidden = true; return; }
      const inputs = Array.from(form.querySelectorAll('[data-field-name]'));
      // A command's prompt option (wikidata reset-cache --force) only skips its terminal
      // prompt, so the re-download warning would be wrong for it.
      const forced = inputs.find(i => i.dataset.fieldKind === 'Flag' && i.checked &&
        /force/i.test(i.dataset.fieldName) && i.dataset.fieldName !== cmd.promptOption);
      const aged = inputs.find(i => /max-age/i.test(i.dataset.fieldName) && (i.value || '').trim() !== '');
      if (forced) {
        forceWarn.hidden = false;
        forceWarn.textContent = '⚠ ' + forced.dataset.fieldName + ' re-downloads, re-imports or rebuilds everything ' +
          'already in the cache or database. Use it only when the source data has changed.';
      } else if (aged) {
        const hours = aged.value.trim();
        // Only the iucn api download commands have a fixed-date option; the Wikidata ones do not.
        const hasRefreshBefore = inputs.some(i => i.dataset.fieldName === '--refresh-before');
        forceWarn.hidden = false;
        forceWarn.textContent = 'ℹ ' + aged.dataset.fieldName + ' ' + hours + ': re-downloads records cached more than ' + hours +
          ' hours before this run, so a refresh spread over several runs downloads some records twice.' +
          (hasRefreshBefore ? ' For a fixed cutoff date, use --refresh-before.' : '');
      } else {
        forceWarn.hidden = true;
      }
    };

    const actions = document.createElement('div');
    actions.className = 'actions';
    const runBtn = document.createElement('button');
    runBtn.type = 'submit';
    runBtn.className = 'run-btn ' + cmd.kind;
    // Says whether clicking will ask first. Set from the command's confirmation mode now, then
    // from the preflight for the options chosen (see updatePreflight).
    const setRunLabel = (asks) => { runBtn.textContent = asks ? 'Run (confirm)' : 'Run'; };
    setRunLabel(cmd.confirm === 'always');
    actions.appendChild(runBtn);
    const preview = document.createElement('span');
    preview.className = 'preview';
    actions.appendChild(preview);
    form.appendChild(actions);

    // Whether this run, with the options chosen, will ask for confirmation, and what it would
    // delete or do to the files as they are right now; refreshed as options change. Hidden for
    // runs that do not ask and have nothing to report (most commands).
    const preflight = document.createElement('div');
    preflight.className = 'preflight';
    preflight.hidden = true;
    form.insertBefore(preflight, actions);
    let preflightSeq = 0;
    const updatePreflight = async () => {
      const seq = ++preflightSeq;
      const result = await fetchPreflight(cmd.path, readForm(form, cmd));
      if (seq !== preflightSeq) return;
      const data = result.ok ? result.data : null;
      renderPreflight(preflight, data);
      preflightShown = !!data;
      if (reasonEl) reasonEl.hidden = preflightShown;
      setRunLabel(result.ok ? !!(data && data.confirm) : cmd.confirm !== 'never');
      updateForceWarn();
    };

    const updatePreview = () => {
      const args = readForm(form, cmd);
      preview.textContent = '$ beastiebot3 ' + cmd.path + (args.length ? ' ' + args.join(' ') : '');
    };
    updatePreview();
    updateForceWarn();
    updatePreflight();
    form.addEventListener('input', () => { updatePreview(); updateForceWarn(); updatePreflight(); });
    form.addEventListener('change', () => { updatePreview(); updateForceWarn(); updatePreflight(); });

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const runArgs = await confirmRun(cmd.path, readForm(form, cmd), cmd);
      if (runArgs) enqueue(cmd.path, runArgs);
    });

    return form;
  }

  // --- Preflight: does this run need confirmation, and what would it delete? ----
  // The server decides (CommandPreflight, rule above CommandKind): only a run that deletes
  // downloaded or imported data asks. For iucn import it also inspects the real files, so it
  // can say "creates a new database" instead of warning about data that is not there.

  // { ok: true, data } where data is null when the run asks nothing and shows nothing;
  // { ok: false } when the server could not be asked.
  async function fetchPreflight(path, args) {
    try {
      const url = '/api/commands/preflight?path=' + encodeURIComponent(path) +
                  '&args=' + encodeURIComponent(args.join(' '));
      const r = await fetch(url);
      if (!r.ok) return { ok: false };
      const d = await r.json();
      return { ok: true, data: d && d.supported ? d : null };
    } catch (_) {
      return { ok: false };
    }
  }

  function renderPreflight(box, data) {
    if (!data) { box.hidden = true; box.textContent = ''; return; }
    box.hidden = false;
    box.className = 'preflight' + (data.confirm ? ' at-risk' : '');
    box.textContent = '';

    const head = document.createElement('p');
    head.className = 'preflight-headline';
    head.textContent = (data.confirm ? '⚠ ' : '') + data.headline;
    box.appendChild(head);

    for (const line of (data.details || [])) {
      const p = document.createElement('p');
      p.className = 'preflight-detail';
      p.textContent = line;
      box.appendChild(p);
    }
    if (data.warning) {
      const w = document.createElement('p');
      w.className = 'preflight-warning';
      w.textContent = data.warning;
      box.appendChild(w);
    }
  }

  // Returns the args to run (the preflight may add the command's prompt option, such as
  // wikidata reset-cache --force, because a web job cannot answer a terminal prompt), or null
  // when the user cancels. If the server cannot be asked, any command that can ask does ask.
  async function confirmRun(path, args, meta) {
    const result = await fetchPreflight(path, args);
    let pre = result.ok ? result.data : null;
    if (!result.ok && meta && meta.confirm && meta.confirm !== 'never') {
      pre = {
        confirm: true,
        headline: meta.reason || 'This run can delete downloaded or imported data.',
        addArgs: meta.promptOption && !args.includes(meta.promptOption) ? [meta.promptOption] : [],
      };
    }
    if (!pre || !pre.confirm) return args;

    const runArgs = args.concat(pre.addArgs || []);
    const lines = ['Run "' + path + (runArgs.length ? ' ' + runArgs.join(' ') : '') + '"?', '', pre.headline];
    if (pre.details && pre.details.length) lines.push('', pre.details.join('\n'));
    if (pre.warning) lines.push('', pre.warning);
    return confirm(lines.join('\n')) ? runArgs : null;
  }

  function renderField(grid, f) {
    const labelEl = document.createElement('label');
    labelEl.className = 'field-label';
    labelEl.textContent = f.name;
    labelEl.title = f.altNames && f.altNames.length ? 'aliases: ' + f.altNames.join(', ') : '';
    grid.appendChild(labelEl);

    const ctrl = document.createElement('div');
    ctrl.className = 'field-control';
    let input;
    if (f.kind === 'Flag') {
      input = document.createElement('input');
      input.type = 'checkbox';
      input.dataset.fieldName = f.name;
      input.dataset.fieldKind = f.kind;
      if (f.defaultValue === 'True' || f.defaultValue === 'true') input.checked = true;
      // The label position differs for checkboxes: replace label-text styling.
      labelEl.textContent = '';
      const wrap = document.createElement('label');
      wrap.style.cursor = 'pointer';
      wrap.appendChild(input);
      const lab = document.createElement('span');
      lab.style.marginLeft = '0.5em';
      lab.textContent = f.name;
      wrap.appendChild(lab);
      ctrl.appendChild(wrap);
    } else if (f.kind === 'Choice') {
      input = document.createElement('select');
      input.dataset.fieldName = f.name;
      input.dataset.fieldKind = f.kind;
      const blank = document.createElement('option');
      blank.value = '';
      blank.textContent = '(default)';
      input.appendChild(blank);
      for (const c of f.choices || []) {
        const o = document.createElement('option');
        o.value = c;
        o.textContent = c;
        if (f.defaultValue && f.defaultValue.toLowerCase() === c.toLowerCase()) o.selected = true;
        input.appendChild(o);
      }
      ctrl.appendChild(input);
    } else {
      input = document.createElement('input');
      input.dataset.fieldName = f.name;
      input.dataset.fieldKind = f.kind;
      input.type = (f.kind === 'Integer' || f.kind === 'Number') ? 'number' : 'text';
      if (f.kind === 'Number') input.step = 'any';
      if (f.placeholder) input.placeholder = f.placeholder;
      else if (f.kind === 'List') input.placeholder = 'comma,separated,values';
      ctrl.appendChild(input);
    }

    const hint = document.createElement('span');
    hint.className = 'field-hint';
    const bits = [];
    // Descriptions are plain text that may contain <angle brackets> (e.g. <Datastore:reports_dir>).
    if (f.description) bits.push(codeHtml(f.description));
    if (f.hasDefault && f.defaultValue != null && f.defaultValue !== '' && f.kind !== 'Flag') {
      bits.push('<span class="default">default: ' + escapeHtml(f.defaultValue) + '</span>');
    }
    hint.innerHTML = bits.join(' · ');
    if (bits.length) ctrl.appendChild(hint);

    grid.appendChild(ctrl);
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // Workflow text and option descriptions mark commands and file names with `backticks`;
  // escape the text and show those as inline code.
  function codeHtml(text) {
    return escapeHtml(text).replace(/`([^`]+)`/g, '<code>$1</code>');
  }

  function setTextWithCode(el, text) {
    el.innerHTML = codeHtml(text);
  }

  // Fill a generated form from an args[] — used to open a workflow step's options already
  // set to what that step would run, so changing one setting doesn't mean retyping the rest.
  function applyArgsToForm(formEl, args) {
    if (!args || args.length === 0) return;
    const byName = {};
    formEl.querySelectorAll('[data-field-name]').forEach(i => { byName[i.dataset.fieldName] = i; });
    for (let i = 0; i < args.length; i++) {
      const inp = byName[args[i]];
      if (!inp) continue;
      if (inp.dataset.fieldKind === 'Flag') { inp.checked = true; continue; }
      const v = args[i + 1];
      if (v !== undefined && v.slice(0, 2) !== '--') { inp.value = v; i++; }
    }
    formEl.dispatchEvent(new Event('input', { bubbles: true }));
  }

  // Build a CLI args[] from the current form values. We never emit empty
  // strings — empty input means "use the command's default".
  function readForm(formEl, cmd) {
    const args = [];
    const inputs = formEl.querySelectorAll('[data-field-name]');
    for (const inp of inputs) {
      const name = inp.dataset.fieldName;
      const kind = inp.dataset.fieldKind;
      if (kind === 'Flag') {
        if (inp.checked) args.push(name);
      } else if (kind === 'List') {
        const raw = inp.value.trim();
        if (!raw) continue;
        for (const v of raw.split(',').map(s => s.trim()).filter(Boolean)) {
          args.push(name, v);
        }
      } else {
        const v = inp.value.trim();
        if (!v) continue;
        args.push(name, v);
      }
    }
    return args;
  }

  $('#cmd-search').addEventListener('input', renderCommandTree);
  document.querySelectorAll('#cmd-kind-filter input').forEach(cb =>
    cb.addEventListener('change', renderCommandTree));
  loadCommands();

  // --- Workflows ------------------------------------------------------

  let allFlows = [];
  let activeFlowId = null;
  const expandedStepIds = new Set();

  async function loadFlowsList() {
    try {
      const res = await fetch('/api/flows');
      if (!res.ok) return;
      allFlows = await res.json();
      renderFlowTabs();
      if (allFlows.length > 0) selectFlow(allFlows[0].id);
    } catch (e) {
      $('#flow-content').textContent = 'Could not load the list of workflows. Check that `serve` is running, then reload the page. Error: ' + e.message;
    }
  }

  function renderFlowTabs() {
    const tabs = $('#flow-tabs');
    tabs.innerHTML = '';
    for (const f of allFlows) {
      const btn = document.createElement('button');
      btn.className = 'flow-tab' + (f.id === activeFlowId ? ' active' : '');
      btn.textContent = f.title;
      btn.addEventListener('click', () => selectFlow(f.id));
      tabs.appendChild(btn);
    }
  }

  async function selectFlow(id) {
    activeFlowId = id;
    expandedStepIds.clear();
    renderFlowTabs();
    $('#flow-content').innerHTML = '<p class="muted">Loading…</p>';
    try {
      const res = await fetch('/api/flows/' + encodeURIComponent(id));
      if (!res.ok) {
        $('#flow-content').textContent = 'Could not load the selected workflow (HTTP status ' + res.status + '). See the `serve` console output.';
        return;
      }
      const snap = await res.json();
      renderFlow(snap);
    } catch (e) {
      $('#flow-content').textContent = 'Error: ' + e.message;
    }
  }

  function renderFlow(snap) {
    const root = $('#flow-content');
    // A poll re-renders the whole flow, which would silently fold away every section the
    // user had opened (Maintenance, a step's walkthrough). Remember which were open by key
    // and reopen them below, so live updates never cost the reader their place.
    const openDetails = new Set(
      Array.from(root.querySelectorAll('details[open][data-key]')).map(d => d.dataset.key)
    );
    root.innerHTML = '';

    const desc = document.createElement('p');
    desc.className = 'muted';
    setTextWithCode(desc, snap.description);
    root.appendChild(desc);

    // For the CoL-update flow, show a live freshness banner up top so the operator
    // immediately sees whether the imported CoL release matches the input folder
    // (i.e. whether a re-import + repoint is still pending). Offline check.
    if (snap.id === 'col-update') {
      const banner = document.createElement('div');
      banner.className = 'flow-col-version';
      banner.textContent = 'Checking Catalogue of Life freshness…';
      root.appendChild(banner);
      fetch('/api/col-version')
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (d) banner.innerHTML = colVersionHtml(d); else banner.remove(); })
        .catch(() => banner.remove());
    }

    // Split steps into pipeline (core path), step-by-step (the pieces a one-button pipeline
    // step runs, for running one at a time) and maintenance (only-when-needed).
    const pipelineSteps = snap.steps.filter(s => (s.section || 'pipeline') === 'pipeline');
    const stepByStepSteps = snap.steps.filter(s => s.section === 'stepbystep');
    const maintenanceSteps = snap.steps.filter(s => s.section === 'maintenance');

    const pipeline = document.createElement('div');
    pipeline.className = 'flow-pipeline';
    // Steps may carry an optional `group` heading; emit a header whenever it changes so a
    // single flow can present clearly-separated routes (e.g. CSV / API / Compare).
    let lastGroup = null;
    for (const step of pipelineSteps) {
      const g = step.group || null;
      if (g && g !== lastGroup) {
        const h = document.createElement('div');
        h.className = 'flow-group-header';
        h.textContent = g;
        pipeline.appendChild(h);
      }
      lastGroup = g;
      pipeline.appendChild(renderStep(step, snap, openDetails));
    }
    root.appendChild(pipeline);

    if (stepByStepSteps.length > 0) {
      // Same fold as Maintenance, but these are not repairs: they are the pieces the Update
      // step runs, kept for running or tuning one at a time. The summary says how many have
      // work left so a closed fold still answers "is anything outstanding in here".
      const withWork = stepByStepSteps.filter(s => s.status === 'todo' || s.status === 'backlog').length;
      const wrap = document.createElement('details');
      wrap.className = 'flow-maintenance';
      wrap.dataset.key = 'stepbystep';
      wrap.open = openDetails.has('stepbystep');
      const summary = document.createElement('summary');
      summary.innerHTML = '<span class="flow-maintenance-title">Step by step</span> ' +
                          '<span class="small muted">' + stepByStepSteps.length + ' separate step' +
                          (stepByStepSteps.length === 1 ? '' : 's') + ', ' +
                          (withWork > 0 ? withWork : 'none') +
                          ' marked "to do" or "more to do"</span>';
      wrap.appendChild(summary);
      const pipe = document.createElement('div');
      pipe.className = 'flow-pipeline';
      let lastSbsGroup = null;
      for (const step of stepByStepSteps) {
        const g = step.group || null;
        if (g && g !== lastSbsGroup) {
          const h = document.createElement('div');
          h.className = 'flow-group-header';
          h.textContent = g;
          pipe.appendChild(h);
        }
        lastSbsGroup = g;
        pipe.appendChild(renderStep(step, snap, openDetails));
      }
      wrap.appendChild(pipe);
      root.appendChild(wrap);
    }

    if (maintenanceSteps.length > 0) {
      const wrap = document.createElement('details');
      wrap.className = 'flow-maintenance';
      wrap.dataset.key = 'maintenance';
      wrap.open = openDetails.has('maintenance');
      const summary = document.createElement('summary');
      summary.innerHTML = '<span class="flow-maintenance-title">Maintenance</span> ' +
                          '<span class="small muted">' + maintenanceSteps.length + ' step' +
                          (maintenanceSteps.length === 1 ? '' : 's') +
                          ', only needed when coverage drops or a cache needs repair</span>';
      wrap.appendChild(summary);
      const pipe = document.createElement('div');
      pipe.className = 'flow-pipeline';
      for (const step of maintenanceSteps) pipe.appendChild(renderStep(step, snap, openDetails));
      wrap.appendChild(pipe);
      root.appendChild(wrap);
    }

    // Side panels: templates + outputs
    const sidebar = document.createElement('div');
    sidebar.className = 'flow-sidebar';
    if (snap.templates && snap.templates.length > 0) {
      sidebar.appendChild(renderResourceList('Templates & config', snap.templates));
    }
    if (snap.outputs && snap.outputs.length > 0) {
      sidebar.appendChild(renderResourceList('Outputs', snap.outputs));
    }
    if (sidebar.children.length > 0) root.appendChild(sidebar);
  }

  // Split a flow command string into its registered command (longest path prefix) + trailing args,
  // so a step can carry args like "iucn api cache-infraranks --from-csv".
  function splitFlowCommand(c) {
    let best = null;
    for (const cmd of allCommands) {
      if (c === cmd.path || c.startsWith(cmd.path + ' ')) {
        if (!best || cmd.path.length > best.path.length) best = cmd;
      }
    }
    if (!best) return { meta: null, path: c, args: [] };
    const rest = c.slice(best.path.length).trim();
    return { meta: best, path: best.path, args: rest ? rest.split(/\s+/) : [] };
  }

  function renderStep(step, snap, openDetails) {
    // A step marked optional but reported as still to do is not optional today — show it at
    // full strength and drop the "(optional)" suffix, or the two readings contradict each other.
    const optional = step.optional && step.status !== 'todo';

    const wrap = document.createElement('div');
    wrap.className = 'flow-step status-' + step.status + (optional ? ' optional' : '');

    const head = document.createElement('div');
    head.className = 'flow-step-head';

    const dot = document.createElement('span');
    dot.className = 'flow-step-dot status-' + step.status;
    head.appendChild(dot);

    // Title, plus the one-line on-disk state from the step's probe (release found, imported,
    // pointed at) so it reads without expanding the step.
    const titles = document.createElement('div');
    titles.className = 'flow-step-titles';
    const title = document.createElement('span');
    title.className = 'flow-step-title';
    title.textContent = step.title + (optional ? '  (optional)' : '');
    titles.appendChild(title);
    if (step.detail) {
      const detail = document.createElement('div');
      detail.className = 'flow-step-detail status-' + step.status;
      detail.textContent = step.detail;
      titles.appendChild(detail);
    }
    head.appendChild(titles);

    const status = document.createElement('span');
    status.className = 'flow-step-status status-' + step.status;
    if (step.status === 'blocked') {
      status.textContent = 'blocked';
    } else if (step.status === 'running') {
      status.textContent = '● running';
    } else if (step.status === 'todo') {
      status.textContent = 'to do';
    } else if (step.status === 'backlog') {
      // A queue worked down over time rather than something overdue; the count is in the
      // detail line under the title.
      status.textContent = 'more to do';
    } else if (step.status === 'manual') {
      status.textContent = 'by hand';
    } else if (step.status === 'never-run') {
      status.textContent = 'not run';
    } else if (step.lastRunAt) {
      status.textContent = formatRelative(step.lastRunAt);
      status.title = new Date(step.lastRunAt).toLocaleString();
    } else {
      status.textContent = 'done';
    }
    head.appendChild(status);

    wrap.appendChild(head);

    // Any number of steps may be open at once, and the set survives a re-render: a poll
    // that folded away what the reader had opened is the whole reason this is a Set.
    head.addEventListener('click', () => {
      const open = !expandedStepIds.has(step.id);
      if (open) expandedStepIds.add(step.id); else expandedStepIds.delete(step.id);
      // Avoid a full reflow — just toggle this step's body.
      const body = wrap.querySelector('.flow-step-body');
      if (body) body.hidden = !open;
      wrap.classList.toggle('expanded', open);
    });

    const body = document.createElement('div');
    body.className = 'flow-step-body';
    const isOpen = expandedStepIds.has(step.id);
    wrap.classList.toggle('expanded', isOpen);
    body.hidden = !isOpen;

    const d = document.createElement('p');
    d.className = 'muted small';
    setTextWithCode(d, step.description);
    body.appendChild(d);

    if (step.note) {
      const n = document.createElement('p');
      n.className = 'flow-step-note';
      setTextWithCode(n, step.note);
      body.appendChild(n);
    }

    // Collapsible numbered walkthrough for steps done by hand (downloads, config edits).
    if (step.guideSteps && step.guideSteps.length > 0) {
      const g = document.createElement('details');
      g.className = 'flow-step-guide';
      g.dataset.key = 'guide:' + step.id;
      g.open = !!(openDetails && openDetails.has('guide:' + step.id));
      const sum = document.createElement('summary');
      sum.textContent = step.guideTitle || 'Step by step';
      g.appendChild(sum);
      const ol = document.createElement('ol');
      for (const line of step.guideSteps) {
        const li = document.createElement('li');
        setTextWithCode(li, line);
        ol.appendChild(li);
      }
      g.appendChild(ol);
      body.appendChild(g);
    }

    if (step.missingInputs && step.missingInputs.length > 0) {
      const m = document.createElement('p');
      m.className = 'flow-step-missing';
      m.textContent = 'Missing inputs: ' + step.missingInputs.join(', ');
      body.appendChild(m);
    }

    if (step.inputSourceIds && step.inputSourceIds.length > 0) {
      body.appendChild(renderSourceList('Inputs', step.inputSourceIds, snap.sources || {}));
    }
    if (step.outputSourceIds && step.outputSourceIds.length > 0) {
      body.appendChild(renderSourceList('Outputs', step.outputSourceIds, snap.sources || {}));
    }

    // Running jobs: surface in-flight job ids with links to open them.
    if (step.runningJobs && step.runningJobs.length > 0) {
      const r = document.createElement('div');
      r.className = 'flow-step-running';
      const lbl = document.createElement('span');
      lbl.className = 'small muted';
      lbl.textContent = 'In flight:';
      r.appendChild(lbl);
      for (const rj of step.runningJobs) {
        const link = document.createElement('a');
        link.href = '#';
        link.className = 'running-job-link';
        link.textContent = '● ' + rj.command + ' (job ' + rj.jobId + ')';
        link.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          replayJob(rj.jobId);
        });
        r.appendChild(link);
      }
      body.appendChild(r);
    }

    // Latest output files surfaced from OutputPatterns.
    if (step.latestOutputs && step.latestOutputs.length > 0) {
      const out = document.createElement('div');
      out.className = 'flow-step-outputs';
      const lbl = document.createElement('span');
      lbl.className = 'small muted';
      lbl.textContent = 'Latest output:';
      out.appendChild(lbl);
      for (const f of step.latestOutputs) {
        const link = document.createElement('a');
        link.href = '#';
        link.className = 'latest-output-link';
        link.textContent = f.label + ': ' + f.path;
        link.title = f.root + '/' + f.path + '  ·  ' + formatBytes(f.size) + '  ·  ' + new Date(f.modified).toLocaleString();
        link.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          openFile(f.root, f.path);
        });
        out.appendChild(link);
        const meta = document.createElement('span');
        meta.className = 'small muted';
        meta.textContent = formatRelative(f.modified);
        out.appendChild(meta);
      }
      body.appendChild(out);
    }

    // Commands with Run buttons.
    if (step.commands && step.commands.length > 0) {
      const cmdRow = document.createElement('div');
      cmdRow.className = 'flow-step-cmds';
      const lbl = document.createElement('span');
      lbl.className = 'small muted';
      lbl.textContent = 'Commands:';
      cmdRow.appendChild(lbl);
      for (const c of step.commands) {
        // A flow command may carry trailing args (e.g. "iucn api cache-infraranks --from-csv").
        // Match the longest registered command path that prefixes it; the rest are args.
        const { meta: cmdMeta, path, args } = splitFlowCommand(c);
        // A button whose options make the run only report ("--status", or prune-queue without
        // --apply) looks like a read-only command's button and has no effect pill.
        const readOnlyRun = !!cmdMeta && (cmdMeta.rerun === 'readonly' || isReportOnlyRun(cmdMeta, args));
        const btn = document.createElement('button');
        btn.className = 'flow-cmd-btn ' + (readOnlyRun ? 'readonly' : cmdMeta ? cmdMeta.kind : 'mutates');
        btn.textContent = c;
        if (readOnlyRun) btn.title = EFFECTS.readonly.hint;
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          if (!cmdMeta) {
            alert('Unknown command: ' + c);
            return;
          }
          confirmRun(path, args, cmdMeta).then((runArgs) => { if (runArgs) enqueue(path, runArgs); });
        });
        cmdRow.appendChild(btn);

        // What a re-run of this command does (adds / discovers / rebuilds / clears), from the
        // same metadata the Run command page shows. Answers "will this replace what I have?"
        // without opening the step's options.
        const cmdEff = cmdMeta ? effectInfo(cmdMeta) : null;
        if (cmdEff && !readOnlyRun) {
          const pill = document.createElement('span');
          pill.className = 'effect-badge ' + cmdEff.cls;
          pill.textContent = cmdEff.label;
          pill.title = cmdEff.hint + (cmdMeta.rerunNote ? ' ' + cmdMeta.rerunNote : '');
          cmdRow.appendChild(pill);
        }

        // One click stays the normal way to run a step, but some of them genuinely have a
        // setting worth changing here — a cutoff date, a limit — and sending people to a
        // different page to set it makes the workflow the long way round. Same form the Run
        // command page generates, opened with this step's own settings already in it.
        const optionCount = cmdMeta && cmdMeta.form && cmdMeta.form.fields ? cmdMeta.form.fields.length : 0;
        if (optionCount > 0) {
          const toggle = document.createElement('button');
          toggle.className = 'flow-cmd-options';
          toggle.textContent = 'Options';
          toggle.title = 'Change the settings for this run';
          let form = null;
          toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            if (form) {
              form.hidden = !form.hidden;
              toggle.classList.toggle('open', !form.hidden);
              return;
            }
            form = buildForm(cmdMeta);
            form.classList.add('flow-cmd-form');
            applyArgsToForm(form, args);
            cmdRow.insertAdjacentElement('afterend', form);
            toggle.classList.add('open');
          });
          cmdRow.appendChild(toggle);
        }
      }
      body.appendChild(cmdRow);
    }

    wrap.appendChild(body);
    return wrap;
  }

  function renderSourceList(label, ids, sourcesById) {
    const wrap = document.createElement('div');
    wrap.className = 'flow-step-sources';
    const lbl = document.createElement('span');
    lbl.className = 'small muted';
    lbl.textContent = label + ':';
    wrap.appendChild(lbl);
    for (const id of ids) {
      const info = sourcesById[id];
      const chip = document.createElement('span');
      chip.className = 'source-chip' + (info ? (info.exists ? ' ok' : ' missing') : '');
      const name = document.createElement('span');
      name.className = 'source-name';
      name.textContent = info ? info.name : id;
      chip.appendChild(name);
      if (info && info.headline) {
        const meta = document.createElement('span');
        meta.className = 'source-meta';
        meta.textContent = info.headline;
        chip.appendChild(meta);
      }
      chip.title = (info && info.path ? info.path : id) + (info && !info.exists ? '  (missing)' : '');
      wrap.appendChild(chip);
    }
    return wrap;
  }

  function renderResourceList(title, items) {
    const wrap = document.createElement('div');
    wrap.className = 'flow-resource-list';
    const h = document.createElement('h3');
    h.textContent = title;
    wrap.appendChild(h);
    for (const r of items) {
      const row = document.createElement('div');
      row.className = 'flow-resource';
      const a = document.createElement('a');
      a.href = '#';
      a.textContent = r.label;
      a.addEventListener('click', (e) => {
        e.preventDefault();
        if (r.kind === 'directory') {
          openDir(r.root, r.path);
        } else {
          openFile(r.root, r.path);
        }
      });
      row.appendChild(a);
      const path = document.createElement('span');
      path.className = 'flow-resource-path small muted';
      path.textContent = ' — ' + r.root + (r.path ? '/' + r.path : '/');
      row.appendChild(path);
      if (r.description) {
        const d = document.createElement('div');
        d.className = 'small muted';
        setTextWithCode(d, r.description);
        row.appendChild(d);
      }
      wrap.appendChild(row);
    }
    return wrap;
  }

  // --- File viewer modal --------------------------------------------

  const viewer = $('#file-viewer');
  const viewerTitle = $('#file-viewer-title');
  const viewerMeta = $('#file-viewer-meta');
  const viewerBody = $('#file-viewer-body');
  const viewerCopy = $('#file-viewer-copy');
  let viewerContent = null;

  $('#file-viewer-close').addEventListener('click', closeViewer);
  $('#file-viewer .modal-backdrop').addEventListener('click', closeViewer);
  viewerCopy.addEventListener('click', async () => {
    if (viewerContent == null) return;
    try {
      await navigator.clipboard.writeText(viewerContent);
      const prev = viewerCopy.textContent;
      viewerCopy.textContent = 'Copied!';
      setTimeout(() => { viewerCopy.textContent = prev; }, 1500);
    } catch {
      viewerCopy.textContent = 'Copy failed';
      setTimeout(() => { viewerCopy.textContent = 'Copy all'; }, 1500);
    }
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !viewer.hidden) closeViewer();
  });

  function closeViewer() {
    viewer.hidden = true;
    viewerBody.innerHTML = '';
    viewerContent = null;
    viewerCopy.hidden = true;
  }

  function showViewer() {
    viewer.hidden = false;
    viewerContent = null;
    viewerCopy.hidden = true;
  }

  async function openFile(root, path) {
    showViewer();
    viewerTitle.textContent = root + '/' + path;
    viewerMeta.textContent = '';
    viewerBody.innerHTML = '<p class="muted">Loading…</p>';
    try {
      const res = await fetch('/api/files/read?root=' + encodeURIComponent(root) + '&path=' + encodeURIComponent(path));
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        viewerBody.innerHTML = '<p class="error">' + MarkdownRenderer.escape(err.error || ('HTTP ' + res.status)) + '</p>';
        return;
      }
      const data = await res.json();
      viewerMeta.textContent = formatBytes(data.size) + ' · ' + formatRelative(data.modified);
      viewerContent = data.content;
      viewerCopy.hidden = false;
      renderFileContent(path, data.content);
    } catch (e) {
      viewerBody.innerHTML = '<p class="error">' + MarkdownRenderer.escape(e.message) + '</p>';
    }
  }

  function renderFileContent(path, content) {
    viewerBody.innerHTML = '';
    const ext = (path.match(/\.([a-z0-9]+)$/i) || [, ''])[1].toLowerCase();
    const renderers = [];
    if (ext === 'md') {
      renderers.push({ name: 'Rendered', html: () => '<div class="markdown">' + MarkdownRenderer.toHtml(content) + '</div>' });
    } else if (ext === 'csv') {
      renderers.push({ name: 'Table', html: () => MarkdownRenderer.csvToHtml(content) });
    }
    renderers.push({ name: 'Raw', html: () => '<pre class="file-raw">' + MarkdownRenderer.escape(content) + '</pre>' });

    if (renderers.length === 1) {
      const div = document.createElement('div');
      div.innerHTML = renderers[0].html();
      viewerBody.appendChild(div);
      return;
    }

    // Two-tab view (rendered + raw).
    const tabs = document.createElement('div');
    tabs.className = 'view-tabs';
    const body = document.createElement('div');
    body.className = 'view-tab-body';
    renderers.forEach((r, idx) => {
      const btn = document.createElement('button');
      btn.className = 'view-tab' + (idx === 0 ? ' active' : '');
      btn.textContent = r.name;
      btn.addEventListener('click', () => {
        tabs.querySelectorAll('.view-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        body.innerHTML = r.html();
      });
      tabs.appendChild(btn);
    });
    body.innerHTML = renderers[0].html();
    viewerBody.appendChild(tabs);
    viewerBody.appendChild(body);
  }

  async function openDir(root, subdir) {
    showViewer();
    viewerTitle.textContent = root + '/' + (subdir || '');
    viewerMeta.textContent = '';
    viewerBody.innerHTML = '<p class="muted">Loading…</p>';
    try {
      const qs = 'root=' + encodeURIComponent(root) + (subdir ? '&subdir=' + encodeURIComponent(subdir) : '');
      const res = await fetch('/api/files/list?' + qs);
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        viewerBody.innerHTML = '<p class="error">' + MarkdownRenderer.escape(err.error || ('HTTP ' + res.status)) + '</p>';
        return;
      }
      const data = await res.json();
      renderDirListing(root, subdir || '', data.entries);
    } catch (e) {
      viewerBody.innerHTML = '<p class="error">' + MarkdownRenderer.escape(e.message) + '</p>';
    }
  }

  function renderDirListing(root, subdir, entries) {
    viewerBody.innerHTML = '';

    // Breadcrumb: root + each subdir segment.
    if (subdir) {
      const crumb = document.createElement('div');
      crumb.className = 'breadcrumb small';
      const rootLink = document.createElement('a');
      rootLink.href = '#';
      rootLink.textContent = root;
      rootLink.addEventListener('click', (e) => { e.preventDefault(); openDir(root, ''); });
      crumb.appendChild(rootLink);
      const parts = subdir.split('/').filter(Boolean);
      let acc = '';
      for (let i = 0; i < parts.length; i++) {
        const sep = document.createElement('span');
        sep.className = 'breadcrumb-sep';
        sep.textContent = ' / ';
        crumb.appendChild(sep);
        acc = acc ? acc + '/' + parts[i] : parts[i];
        if (i === parts.length - 1) {
          const last = document.createElement('span');
          last.textContent = parts[i];
          crumb.appendChild(last);
        } else {
          const link = document.createElement('a');
          link.href = '#';
          link.textContent = parts[i];
          const cur = acc;
          link.addEventListener('click', (e) => { e.preventDefault(); openDir(root, cur); });
          crumb.appendChild(link);
        }
      }
      viewerBody.appendChild(crumb);
    }

    if (entries.length === 0) {
      const p = document.createElement('p');
      p.className = 'muted';
      p.textContent = '(empty)';
      viewerBody.appendChild(p);
      return;
    }

    const list = document.createElement('ul');
    list.className = 'dir-listing';
    for (const e of entries) {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = '#';
      a.textContent = (e.kind === 'directory' ? '📁 ' : '📄 ') + e.name;
      a.addEventListener('click', (ev) => {
        ev.preventDefault();
        if (e.kind === 'directory') openDir(root, e.path);
        else openFile(root, e.path);
      });
      li.appendChild(a);
      const meta = document.createElement('span');
      meta.className = 'small muted';
      meta.textContent = (e.size != null ? '  ' + formatBytes(e.size) : '') + '  · ' + formatRelative(e.modified);
      li.appendChild(meta);
      list.appendChild(li);
    }
    viewerBody.appendChild(list);
  }

  loadFlowsList();

  // --- Public surface for router.js / rules-editor.js / dashboard -----
  // Exposes the handful of cross-cutting actions and helpers the router, the
  // Taxa grouping and Rules editor pages, and the dashboard need: starting jobs
  // in the dock and attaching to them, opening files, and the live
  // data getters/refreshers. Everything else stays private to this IIFE.
  window.Beastie = {
    enqueue, replayJob, openFile, openDir,
    refreshStatus, refreshJobList, refreshActiveFlow, loadFlowsList, selectFlow,
    formatBytes, formatRelative, formatNumber, statusKind,
    jobDuration, jobTimesTooltip, jobStatusText, jobErrorText,
    getFlows: () => allFlows,
    getCommands: () => allCommands,
  };
})();
