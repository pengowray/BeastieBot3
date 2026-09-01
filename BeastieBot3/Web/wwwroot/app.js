// Frontend wiring for the BeastieBot3 web UI (Phase 1).
//
//   - Click "Load paths"            -> GET /api/paths and render table
//   - Click an enqueue button       -> POST /api/jobs, attach to its SSE stream
//   - SSE chunks                    -> parsed by AnsiRenderer, appended to <pre>
//
// The output pane handles \r (carriage return) by replacing the current line:
// this lets Spectre.Console progress bars look reasonable in the browser
// without us having to model a full terminal screen.

(function () {
  const $ = (sel) => document.querySelector(sel);

  // --- Settings table -------------------------------------------------

  $('#load-paths').addEventListener('click', async () => {
    const res = await fetch('/api/paths');
    if (!res.ok) { alert('Failed: ' + res.status); return; }
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

  // Accumulated text for the line currently being built (so \r overwrites work).
  let pendingLine = '';

  // The terminal keeps only the last MAX_TERMINAL_LINES committed lines. A long
  // download emits hundreds of thousands of lines, and keeping every one alive
  // as a DOM node killed the browser outright on multi-hour jobs. Older lines
  // are dropped from the top and replaced by a single notice.
  const MAX_TERMINAL_LINES = 5000;
  const TERMINAL_TRIM_SLACK = 500;   // trim in batches rather than on every line
  const MAX_PENDING_CHARS = 16384;   // force-commit output that never sends \n
  let committedLines = 0;

  function resetTerminal() {
    jobOutput.innerHTML = '';
    pendingLine = '';
    committedLines = 0;
  }

  function showTrimNotice() {
    if (jobOutput.querySelector('span[data-trimmed="1"]')) return;
    const el = document.createElement('span');
    el.dataset.trimmed = '1';
    el.className = 'ansi-dim';
    el.textContent = '[earlier output trimmed]\n';
    jobOutput.insertBefore(el, jobOutput.firstChild);
  }

  // Each committed line is a <span> followed by a "\n" text node; drop whole
  // pairs from the top. The in-progress preview span is always last, so it is
  // never at risk here.
  function trimTerminal() {
    if (committedLines <= MAX_TERMINAL_LINES + TERMINAL_TRIM_SLACK) return;
    let toRemove = committedLines - MAX_TERMINAL_LINES;
    while (toRemove > 0) {
      let node = jobOutput.firstChild;
      if (node && node.nodeType === 1 && node.dataset && node.dataset.trimmed === '1') {
        node = node.nextSibling;
      }
      if (!node) break;
      const newline = node.nextSibling;
      jobOutput.removeChild(node);
      if (newline && newline.nodeType === 3) jobOutput.removeChild(newline);
      committedLines--;
      toRemove--;
    }
    showTrimNotice();
  }

  // Reattaching to a job replays its whole history as one chunk. Rendering 200k
  // lines and *then* trimming still hangs the browser, so cut the chunk first.
  function clampChunk(text) {
    let lines = 0;
    for (let i = 0; i < text.length; i++) {
      if (text.charCodeAt(i) === 10) lines++;
    }
    if (lines <= MAX_TERMINAL_LINES) return text;
    let pos = -1;
    for (let i = lines - MAX_TERMINAL_LINES; i > 0; i--) pos = text.indexOf('\n', pos + 1);
    showTrimNotice();
    return text.substring(pos + 1);
  }

  function commitLine(html) {
    const span = document.createElement('span');
    span.innerHTML = html;
    jobOutput.appendChild(span);
    jobOutput.appendChild(document.createTextNode('\n'));
    committedLines++;
  }

  function dropPreview() {
    const preview = jobOutput.querySelector('span[data-preview="1"]');
    if (preview) jobOutput.removeChild(preview);
  }

  function setStatus(status) {
    jobStatus.textContent = status;
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
        const err = await res.text();
        alert('Cancel failed: ' + err);
        jobCancel.disabled = false;
      }
      // Otherwise wait for the SSE 'status' event to flip us to cancelled.
    } catch (e) {
      alert('Cancel error: ' + e.message);
      jobCancel.disabled = false;
    }
  });

  function appendChunk(chunk) {
    // Drop \x1b]2;...title... OSC sequences before splitting, they would
    // otherwise leave artefacts the SGR parser doesn't strip.
    // (Already handled by ansi.js but a defensive guard is cheap.)
    let text = clampChunk(chunk);

    // Follow the tail only when the user is already there, so scrolling back
    // through a running job's log isn't yanked to the bottom by every chunk.
    const pinned = jobOutput.scrollHeight - jobOutput.scrollTop - jobOutput.clientHeight < 40;

    // Split into segments by \r and \n. Each \n flushes pendingLine as a new
    // permanent line; \r without \n replaces pendingLine in place. This
    // approximates terminal line redraw without modelling a full TTY.
    while (text.length > 0) {
      const nlIdx = text.indexOf('\n');
      const crIdx = text.indexOf('\r');
      let cut;
      let kind;
      if (nlIdx < 0 && crIdx < 0) {
        pendingLine += text;
        text = '';
        kind = 'append';
      } else if (nlIdx >= 0 && (crIdx < 0 || nlIdx < crIdx)) {
        pendingLine += text.substring(0, nlIdx);
        text = text.substring(nlIdx + 1);
        kind = 'newline';
      } else {
        pendingLine += text.substring(0, crIdx);
        text = text.substring(crIdx + 1);
        // \r\n is one logical newline; absorb the following \n if present.
        if (text.startsWith('\n')) {
          text = text.substring(1);
          kind = 'newline';
        } else {
          kind = 'cr';
        }
      }
      if (kind === 'newline') {
        // pendingLine holds everything that survived the last \r, so the preview
        // span is superseded: drop it, or it lingers above the committed line as
        // a duplicate.
        dropPreview();
        commitLine(AnsiRenderer.toHtml(pendingLine).html);
        pendingLine = '';
      } else if (kind === 'cr') {
        // Replace the in-progress line (overwrite preview span).
        renderPreview();
        pendingLine = '';
      } else if (pendingLine.length > MAX_PENDING_CHARS) {
        // A line that never ends re-renders the whole preview on every chunk,
        // which is quadratic. Cut it loose once it gets long.
        dropPreview();
        commitLine(AnsiRenderer.toHtml(pendingLine).html);
        pendingLine = '';
      } else {
        // append mode: update preview only.
        renderPreview();
      }
    }
    trimTerminal();
    if (pinned) jobOutput.scrollTop = jobOutput.scrollHeight;
  }

  // The last child of jobOutput, when in 'preview' state, is the not-yet-
  // committed line. We mark it with a data attribute so we can replace it.
  function renderPreview() {
    let preview = jobOutput.querySelector('span[data-preview="1"]');
    if (!preview) {
      preview = document.createElement('span');
      preview.dataset.preview = '1';
      jobOutput.appendChild(preview);
    }
    preview.innerHTML = AnsiRenderer.toHtml(pendingLine).html;
  }

  function commitPreviewIfAny() {
    const preview = jobOutput.querySelector('span[data-preview="1"]');
    if (preview) {
      delete preview.dataset.preview;
    }
  }

  async function enqueue(command, args) {
    if (currentEventSource) {
      currentEventSource.close();
      currentEventSource = null;
    }
    showDock(true);
    jobTitle.textContent = '$ beastiebot3 ' + command + (args && args.length ? ' ' + args.join(' ') : '');
    resetTerminal();
    setStatus('pending');

    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ command: command, args: args || [] }),
    });
    if (!res.ok) {
      const err = await res.text();
      jobOutput.textContent = 'Failed to enqueue: ' + err;
      setStatus('failed');
      return;
    }
    const job = await res.json();
    setStatus('running');
    attachStream(job.id);
    refreshJobList();
  }

  function attachStream(jobId) {
    currentJobId = jobId;
    const es = new EventSource('/api/jobs/' + jobId + '/stream');
    currentEventSource = es;
    // Chunks arrive JSON-encoded so \r survives SSE's own line framing.
    es.addEventListener('chunk', (e) => {
      try { appendChunk(JSON.parse(e.data)); } catch (_) { appendChunk(e.data); }
    });
    es.addEventListener('status', (e) => {
      try {
        const j = JSON.parse(e.data);
        setStatus(j.status);
      } catch (_) { /* ignore */ }
    });
    es.addEventListener('done', () => {
      commitPreviewIfAny();
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
        replayJob(j.id);
      });
      left.appendChild(link);

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
      status.textContent = j.status + (j.exitCode != null ? ' (' + j.exitCode + ')' : '');

      right.appendChild(time);
      right.appendChild(status);
      li.appendChild(left);
      li.appendChild(right);
      ul.appendChild(li);
    }
  }

  // Wall-clock run time: started→completed, or started→now while running.
  function jobDuration(j) {
    if (!j.startedAt) return '';
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
    if (j.createdAt) lines.push('enqueued ' + formatAbsolute(j.createdAt));
    if (j.startedAt) lines.push('started ' + formatAbsolute(j.startedAt));
    if (j.completedAt) lines.push('finished ' + formatAbsolute(j.completedAt));
    return lines.join('\n');
  }

  function replayJob(id) {
    if (currentEventSource) {
      currentEventSource.close();
      currentEventSource = null;
    }
    showDock(true);
    resetTerminal();
    setStatus('running');
    attachStream(id);
    fetch('/api/jobs/' + id).then(r => r.json()).then(j => {
      jobTitle.textContent = '$ beastiebot3 ' + j.commandLine;
    });
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

  // Labels whose non-zero value indicates something needs attention.
  const ATTENTION_LABELS = new Set([
    'backlog (pending)', 'pending download', 'pending matches',
    'failed requests', 'missing titles', 'conflicts',
  ]);

  function statusKind(s) {
    if (!s.exists) return { cls: 'missing', text: 'missing' };
    if (s.error) return { cls: 'err', text: 'error' };
    // Any non-zero metric in ATTENTION_LABELS surfaces a "pending" pill.
    for (const m of (s.metrics || [])) {
      if (ATTENTION_LABELS.has(m.label) && m.value && m.value > 0) {
        return { cls: 'warn', text: 'pending work' };
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
    if (s.path) parts.push(`<span title="${s.path}">${s.path}</span>`);
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
          if (ATTENTION_LABELS.has(m.label) && m.value > 0) {
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
      if (!res.ok) { generatedEl.textContent = 'Failed: ' + res.status; return; }
      const data = await res.json();
      const ul = $('#status-list');
      ul.innerHTML = '';
      for (const s of data.sources) ul.appendChild(renderStatusItem(s));
      generatedEl.textContent = 'Generated ' + new Date(data.generatedAt).toLocaleTimeString();
    } catch (e) {
      generatedEl.textContent = 'Error: ' + e.message;
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
      if (!res.ok) { body.textContent = 'Failed: ' + res.status; return; }
      renderIucnVersion(await res.json());
    } catch (e) { body.textContent = 'Error: ' + e.message; }
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
      if (!res.ok) { body.textContent = 'Failed: ' + res.status; return; }
      renderDatasetCompare(await res.json());
    } catch (e) { body.textContent = 'Error: ' + e.message; }
  }

  function renderDatasetCompare(d) {
    const body = $('#dataset-compare-body');
    if (!body) return;
    const csv = d.csv || {}, api = d.api || {};
    if (!csv.exists && !api.exists) { body.textContent = 'Neither IUCN dataset is available.'; return; }
    const num = (v) => (v == null ? '—' : Number(v).toLocaleString());
    let html = '<table class="compare-table"><thead><tr><th></th><th>CSV release</th><th>API projection</th><th></th></tr></thead><tbody>';
    html += '<tr><td class="ct-label">Version</td><td>' + escapeHtml(String(csv.version || '—')) +
            '</td><td>' + escapeHtml(String(api.version || (api.exists ? '—' : 'not built'))) + '</td><td></td></tr>';
    html += '<tr><td class="ct-label">Updated</td><td>' + (csv.lastModified ? formatRelative(csv.lastModified) : '—') +
            '</td><td>' + (api.lastModified ? formatRelative(api.lastModified) : '—') + '</td><td></td></tr>';
    // Coverage: the CSV release is always complete; the API projection may be partial
    // when some taxa's latest assessment JSON wasn't downloaded before project-view ran.
    const apiCoverage = !api.exists ? '—'
      : api.partial === true
        ? '<span class="agree warn">partial' + (api.latestNotDownloaded != null ? ' (−' + num(api.latestNotDownloaded) + ')' : '') + '</span>'
        : api.partial === false ? '<span class="agree ok">complete</span>' : '—';
    html += '<tr><td class="ct-label">Coverage</td><td>' + (csv.exists ? '<span class="agree ok">complete</span>' : '—') +
            '</td><td>' + apiCoverage + '</td><td></td></tr>';
    for (const row of (d.comparison || [])) {
      let mark = '';
      if (row.csv != null && row.api != null) {
        mark = row.equal
          ? '<span class="agree ok">✓</span>'
          : '<span class="agree warn">Δ ' + (row.delta > 0 ? '+' : '') + Number(row.delta).toLocaleString() + '</span>';
      }
      html += '<tr class="' + (row.category ? 'ct-cat' : 'ct-metric') + '"><td class="ct-label">' +
              escapeHtml(row.label) + '</td><td>' + num(row.csv) + '</td><td>' + num(row.api) + '</td><td>' + mark + '</td></tr>';
    }
    html += '</tbody></table>';
    if (!api.exists) {
      html += '<p class="small muted">API projection not built. Run <code>iucn api project-view</code> ' +
              '(after <code>iucn api cache-all</code>) to enable <code>--dataset api</code>.</p>';
    } else if (api.partial === true) {
      html += '<p class="small muted">API projection is <strong>partial</strong>' +
              (api.latestNotDownloaded != null ? ' — ' + num(api.latestNotDownloaded) + ' taxa missing (latest assessment not downloaded)' : '') +
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
      if (!res.ok) { body.textContent = 'Failed: ' + res.status; return; }
      renderColVersion(await res.json());
    } catch (e) { body.textContent = 'Error: ' + e.message; }
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
        $('#command-tree').textContent = 'Failed to load commands: ' + res.status;
        return;
      }
      allCommands = await res.json();
      renderCommandTree();
    } catch (e) {
      $('#command-tree').textContent = 'Error: ' + e.message;
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
  // command's `rerun` effect (served by /api/commands). Orthogonal to `kind`.
  const EFFECTS = {
    readonly:      { label: 'read-only',  cls: 'readonly',  icon: '👁', hint: 'Read-only — produces output/reports; never changes cached data.' },
    idempotentadd: { label: 'adds new',   cls: 'add',       icon: '＋', hint: 'Safe to re-run — skips entries already present and only fetches/adds new ones.' },
    discovers:     { label: 'discovers',  cls: 'discovers', icon: '🔍', hint: 'Discovers new entries — scans an external source for items not yet cached locally.' },
    rebuilds:      { label: 'rebuilds',   cls: 'rebuilds',  icon: '🔁', hint: 'Rebuilds a derived artifact from data already held locally.' },
    clearscache:   { label: 'clears cache', cls: 'fresh',   icon: '🧹', hint: 'Deletes cached payloads in place; the seed/queue is kept so the next fetch re-downloads.' },
    freshdataset:  { label: 'fresh DB',   cls: 'fresh',     icon: '🗄', hint: 'Establishes or replaces a dataset — a new release belongs in a fresh database file.' },
  };
  function effectInfo(cmd) { return EFFECTS[cmd.rerun] || null; }

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

    if (cmd.kind === 'destructive' && cmd.reason) {
      const r = document.createElement('p');
      r.className = 'reason';
      r.textContent = '⚠ ' + cmd.reason;
      form.appendChild(r);
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
    const forceWarn = document.createElement('p');
    forceWarn.className = 'force-warn';
    forceWarn.hidden = true;
    form.appendChild(forceWarn);
    const updateForceWarn = () => {
      const inputs = Array.from(form.querySelectorAll('[data-field-name]'));
      const forced = inputs.some(i => i.dataset.fieldKind === 'Flag' && i.checked && /force/i.test(i.dataset.fieldName));
      const aged = inputs.some(i => /max-age/i.test(i.dataset.fieldName) && (i.value || '').trim() !== '');
      if (forced) {
        forceWarn.hidden = false;
        forceWarn.textContent = '⚠ --force re-downloads / recreates everything already present — redundant work unless the source data has changed.';
      } else if (aged) {
        forceWarn.hidden = false;
        forceWarn.textContent = 'ℹ --max-age-hours re-fetches entries older than the threshold (some redundant downloads).';
      } else {
        forceWarn.hidden = true;
      }
    };

    const actions = document.createElement('div');
    actions.className = 'actions';
    const runBtn = document.createElement('button');
    runBtn.type = 'submit';
    runBtn.className = 'run-btn ' + cmd.kind;
    runBtn.textContent = cmd.kind === 'destructive' ? 'Run (confirm)' : 'Run';
    actions.appendChild(runBtn);
    const preview = document.createElement('span');
    preview.className = 'preview';
    actions.appendChild(preview);
    form.appendChild(actions);

    // What this run would do to the files as they are right now, refreshed as options change.
    // Commands without a preflight leave the box hidden and fall back to the fixed warning.
    const preflight = document.createElement('div');
    preflight.className = 'preflight';
    preflight.hidden = true;
    form.insertBefore(preflight, actions);
    let preflightSeq = 0;
    const updatePreflight = async () => {
      const seq = ++preflightSeq;
      const data = await fetchPreflight(cmd.path, readForm(form, cmd));
      if (seq !== preflightSeq) return;
      renderPreflight(preflight, data);
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
      const args = readForm(form, cmd);
      if (!await confirmRun(cmd.path, args, cmd)) return;
      enqueue(cmd.path, args);
    });

    return form;
  }

  // --- Preflight: what a command would actually do, given the current files ----
  // The command catalogue's warning text is fixed at build time, so on its own it can
  // claim data will be dropped when there is none. Where the server can inspect the
  // real state, it says so instead, and we only interrupt when something is at risk.

  async function fetchPreflight(path, args) {
    try {
      const url = '/api/commands/preflight?path=' + encodeURIComponent(path) +
                  '&args=' + encodeURIComponent(args.join(' '));
      const r = await fetch(url);
      if (!r.ok) return null;
      const d = await r.json();
      return d && d.supported ? d : null;
    } catch (_) {
      return null;
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

  async function confirmRun(path, args, meta) {
    const label = 'Run "' + path + (args.length ? ' ' + args.join(' ') : '') + '"?';
    const pre = await fetchPreflight(path, args);

    if (pre) {
      if (!pre.confirm) return true;
      const lines = [label, '', pre.headline];
      if (pre.details && pre.details.length) lines.push('', pre.details.join('\n'));
      if (pre.warning) lines.push('', pre.warning);
      return confirm(lines.join('\n'));
    }

    if (meta && meta.kind === 'destructive') {
      return confirm(label + '\n\n' + (meta.reason || 'This command makes destructive changes.'));
    }
    return true;
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
    if (f.description) bits.push(f.description);
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
      $('#flow-content').textContent = 'Error loading flows: ' + e.message;
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
        $('#flow-content').textContent = 'Failed: ' + res.status;
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
    desc.textContent = snap.description;
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
                          '<span class="small muted">what Update runs, as ' + stepByStepSteps.length +
                          ' separate steps' +
                          (withWork > 0 ? ' — ' + withWork + ' with work left' : ' — all caught up') +
                          '</span>';
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
                          ' — only run when coverage drops or caches need repair</span>';
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
    d.textContent = step.description;
    body.appendChild(d);

    if (step.note) {
      const n = document.createElement('p');
      n.className = 'flow-step-note';
      n.textContent = step.note;
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
        li.textContent = line;
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
        const btn = document.createElement('button');
        btn.className = 'flow-cmd-btn ' + (cmdMeta ? cmdMeta.kind : 'mutates');
        btn.textContent = c;
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          if (!cmdMeta) {
            alert('Unknown command: ' + c);
            return;
          }
          confirmRun(path, args, cmdMeta).then((ok) => { if (ok) enqueue(path, args); });
        });
        cmdRow.appendChild(btn);

        // What a re-run of this command does (adds / discovers / rebuilds / clears), from the
        // same metadata the Run command page shows. Answers "will this replace what I have?"
        // without opening the step's options.
        const cmdEff = cmdMeta ? effectInfo(cmdMeta) : null;
        if (cmdEff && cmdMeta.rerun !== 'readonly') {
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
        d.textContent = r.description;
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

  // --- Public surface for router.js / dashboard ----------------------
  // Exposes the handful of cross-cutting actions and helpers the router and
  // dashboard need: attaching jobs to the dock, opening files, and the live
  // data getters/refreshers. Everything else stays private to this IIFE.
  window.Beastie = {
    enqueue, replayJob, openFile, openDir,
    refreshStatus, refreshJobList, refreshActiveFlow, loadFlowsList, selectFlow,
    formatBytes, formatRelative, formatNumber, statusKind,
    jobDuration, jobTimesTooltip,
    getFlows: () => allFlows,
    getCommands: () => allCommands,
  };
})();
