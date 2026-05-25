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

  const activeJob = $('#active-job');
  const jobTitle = $('#job-title');
  const jobStatus = $('#job-status');
  const jobOutput = $('#job-output');
  const jobCancel = $('#job-cancel');
  let currentEventSource = null;
  let currentJobId = null;

  // Accumulated text for the line currently being built (so \r overwrites work).
  let pendingLine = '';

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
    let text = chunk;

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
        // Emit pendingLine permanently followed by a real <br>.
        const rendered = AnsiRenderer.toHtml(pendingLine).html;
        const span = document.createElement('span');
        span.innerHTML = rendered;
        jobOutput.appendChild(span);
        jobOutput.appendChild(document.createTextNode('\n'));
        pendingLine = '';
      } else if (kind === 'cr') {
        // Replace the in-progress line (overwrite preview span).
        renderPreview();
        pendingLine = '';
      } else {
        // append mode — update preview only.
        renderPreview();
      }
    }
    jobOutput.scrollTop = jobOutput.scrollHeight;
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
    activeJob.hidden = false;
    jobTitle.textContent = '$ beastiebot3 ' + command + (args && args.length ? ' ' + args.join(' ') : '');
    jobOutput.innerHTML = '';
    pendingLine = '';
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
    es.addEventListener('chunk', (e) => appendChunk(e.data));
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
      const link = document.createElement('a');
      link.href = '#';
      link.textContent = j.commandLine;
      link.addEventListener('click', (e) => {
        e.preventDefault();
        replayJob(j.id);
      });
      left.appendChild(link);
      const right = document.createElement('span');
      right.className = 'status ' + j.status;
      right.textContent = j.status + (j.exitCode != null ? ' (' + j.exitCode + ')' : '');
      li.appendChild(left);
      li.appendChild(right);
      ul.appendChild(li);
    }
  }

  function replayJob(id) {
    if (currentEventSource) {
      currentEventSource.close();
      currentEventSource = null;
    }
    activeJob.hidden = false;
    jobOutput.innerHTML = '';
    pendingLine = '';
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
  }

  $('#refresh-status').addEventListener('click', refreshStatus);
  refreshStatus();

  // Auto-refresh: poll every 10s, but pause while the tab is hidden so we
  // don't burn cycles on background tabs. Catches up immediately when the
  // user returns. 10s feels live for backlog counts without spamming the
  // server during dashboard idle time.
  const STATUS_POLL_MS = 10000;
  setInterval(() => {
    if (document.hidden) return;
    refreshStatus();
    refreshJobList();
  }, STATUS_POLL_MS);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) { refreshStatus(); refreshJobList(); }
  });

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

    const updatePreview = () => {
      const args = readForm(form, cmd);
      preview.textContent = '$ beastiebot3 ' + cmd.path + (args.length ? ' ' + args.join(' ') : '');
    };
    updatePreview();
    form.addEventListener('input', updatePreview);
    form.addEventListener('change', updatePreview);

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const args = readForm(form, cmd);
      if (cmd.kind === 'destructive') {
        const msg = 'Run "' + cmd.path + (args.length ? ' ' + args.join(' ') : '') + '"?\n\n' +
                    (cmd.reason || 'This command makes destructive changes.');
        if (!confirm(msg)) return;
      }
      enqueue(cmd.path, args);
    });

    return form;
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
  let expandedStepId = null;

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
    expandedStepId = null;
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
    root.innerHTML = '';

    const desc = document.createElement('p');
    desc.className = 'muted';
    desc.textContent = snap.description;
    root.appendChild(desc);

    // Pipeline
    const pipeline = document.createElement('div');
    pipeline.className = 'flow-pipeline';
    for (const step of snap.steps) pipeline.appendChild(renderStep(step, snap));
    root.appendChild(pipeline);

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

  function renderStep(step, snap) {
    const wrap = document.createElement('div');
    wrap.className = 'flow-step status-' + step.status + (step.optional ? ' optional' : '');

    const head = document.createElement('div');
    head.className = 'flow-step-head';

    const dot = document.createElement('span');
    dot.className = 'flow-step-dot status-' + step.status;
    head.appendChild(dot);

    const title = document.createElement('span');
    title.className = 'flow-step-title';
    title.textContent = step.title + (step.optional ? '  (optional)' : '');
    head.appendChild(title);

    const status = document.createElement('span');
    status.className = 'flow-step-status status-' + step.status;
    if (step.status === 'blocked') {
      status.textContent = 'blocked';
    } else if (step.status === 'never-run') {
      status.textContent = 'not run';
    } else if (step.lastRunAt) {
      status.textContent = formatRelative(step.lastRunAt);
      status.title = new Date(step.lastRunAt).toLocaleString();
    } else {
      status.textContent = 'ready';
    }
    head.appendChild(status);

    wrap.appendChild(head);

    head.addEventListener('click', () => {
      expandedStepId = expandedStepId === step.id ? null : step.id;
      // Avoid a full reflow — just toggle this step's body.
      const body = wrap.querySelector('.flow-step-body');
      if (body) body.hidden = expandedStepId !== step.id;
      wrap.classList.toggle('expanded', expandedStepId === step.id);
    });

    const body = document.createElement('div');
    body.className = 'flow-step-body';
    body.hidden = expandedStepId !== step.id;

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

    if (step.missingInputs && step.missingInputs.length > 0) {
      const m = document.createElement('p');
      m.className = 'flow-step-missing';
      m.textContent = 'Missing inputs: ' + step.missingInputs.join(', ');
      body.appendChild(m);
    }

    if (step.inputSourceIds && step.inputSourceIds.length > 0) {
      body.appendChild(renderSourceList('Inputs', step.inputSourceIds));
    }
    if (step.outputSourceIds && step.outputSourceIds.length > 0) {
      body.appendChild(renderSourceList('Outputs', step.outputSourceIds));
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
        const btn = document.createElement('button');
        const cmdMeta = allCommands.find(x => x.path === c);
        btn.className = 'flow-cmd-btn ' + (cmdMeta ? cmdMeta.kind : 'mutates');
        btn.textContent = c;
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          if (!cmdMeta) {
            alert('Unknown command: ' + c);
            return;
          }
          if (cmdMeta.kind === 'destructive' && cmdMeta.reason) {
            if (!confirm('Run "' + c + '"?\n\n' + cmdMeta.reason)) return;
          }
          enqueue(c, []);
        });
        cmdRow.appendChild(btn);
      }
      body.appendChild(cmdRow);
    }

    wrap.appendChild(body);
    return wrap;
  }

  function renderSourceList(label, ids) {
    const wrap = document.createElement('div');
    wrap.className = 'flow-step-sources';
    const lbl = document.createElement('span');
    lbl.className = 'small muted';
    lbl.textContent = label + ':';
    wrap.appendChild(lbl);
    for (const id of ids) {
      const chip = document.createElement('span');
      chip.className = 'source-chip';
      chip.textContent = id;
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

  $('#file-viewer-close').addEventListener('click', closeViewer);
  $('#file-viewer .modal-backdrop').addEventListener('click', closeViewer);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !viewer.hidden) closeViewer();
  });

  function closeViewer() {
    viewer.hidden = true;
    viewerBody.innerHTML = '';
  }

  function showViewer() {
    viewer.hidden = false;
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
      renderFileContent(path, data.content);
    } catch (e) {
      viewerBody.innerHTML = '<p class="error">' + MarkdownRenderer.escape(e.message) + '</p>';
    }
  }

  function renderFileContent(path, content) {
    viewerBody.innerHTML = '';
    const isMarkdown = /\.md$/i.test(path);
    if (isMarkdown) {
      const wrap = document.createElement('div');
      wrap.className = 'markdown';
      wrap.innerHTML = MarkdownRenderer.toHtml(content);
      viewerBody.appendChild(wrap);
    } else {
      const pre = document.createElement('pre');
      pre.className = 'file-raw';
      pre.textContent = content;
      viewerBody.appendChild(pre);
    }
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
})();
