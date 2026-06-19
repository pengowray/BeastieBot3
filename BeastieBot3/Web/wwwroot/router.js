// Client-side router + dashboard for the BeastieBot3 web UI.
//
// Responsibilities (kept separate from app.js's job/command/flow logic):
//   - Hash routing (#/dashboard, #/sources, …) toggles which <section.view>
//     is visible and highlights the matching sidebar item. Views all exist in
//     the DOM at load; routing just shows one at a time, so app.js and
//     rules-editor.js can wire their elements once on load regardless of view.
//   - The Dashboard view, a summary of the other views with jump-in links.
//   - A single poller that refreshes the active view (app.js no longer polls).
//
// Depends on window.Beastie, published by app.js.

(function () {
  const $ = (sel) => document.querySelector(sel);
  const B = () => window.Beastie || {};

  const VIEWS = new Set([
    'dashboard', 'sources', 'run', 'workflows', 'jobs', 'outputs', 'grouping', 'rules', 'guides', 'settings',
  ]);
  const DEFAULT_VIEW = 'dashboard';

  // --- Routing --------------------------------------------------------

  function currentView() {
    const raw = (location.hash || '').replace(/^#\/?/, '').trim();
    return VIEWS.has(raw) ? raw : DEFAULT_VIEW;
  }

  function showView(name) {
    document.querySelectorAll('.view').forEach((v) => {
      v.classList.toggle('active', v.dataset.view === name);
    });
    document.querySelectorAll('#sidebar-nav .nav-item').forEach((a) => {
      a.classList.toggle('active', a.dataset.view === name);
    });
    // Refresh the freshly-shown view immediately so it isn't stale.
    refreshView(name);
  }

  function navigate(name, after) {
    const target = '#/' + name;
    if (location.hash === target) {
      showView(name);
    } else {
      location.hash = target;
    }
    if (typeof after === 'function') after();
  }

  window.addEventListener('hashchange', () => showView(currentView()));

  // --- Per-view refresh + polling ------------------------------------

  function refreshView(name) {
    const b = B();
    switch (name) {
      case 'dashboard': renderDashboard(); break;
      case 'sources':   b.refreshStatus && b.refreshStatus(); break;
      case 'jobs':      b.refreshJobList && b.refreshJobList(); break;
      case 'outputs':   window.BeastieOutputs && window.BeastieOutputs.load(); break;
      case 'workflows': b.refreshActiveFlow && b.refreshActiveFlow(); break;
    }
  }

  // Poll the active view every 10s; pause on hidden tabs and catch up on return.
  const POLL_MS = 10000;
  setInterval(() => {
    if (document.hidden) return;
    refreshView(currentView());
  }, POLL_MS);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refreshView(currentView());
  });

  // --- Header clock ---------------------------------------------------

  function tickClock() {
    const el = $('#header-clock');
    if (el) el.textContent = new Date().toLocaleTimeString();
  }
  setInterval(tickClock, 1000);
  tickClock();

  // --- Dashboard ------------------------------------------------------

  $('#dash-refresh').addEventListener('click', renderDashboard);

  function tile(title, headerRight) {
    const card = document.createElement('section');
    card.className = 'card dash-tile';
    const head = document.createElement('div');
    head.className = 'dash-tile-head';
    const h = document.createElement('h3');
    h.textContent = title;
    head.appendChild(h);
    if (headerRight) head.appendChild(headerRight);
    card.appendChild(head);
    return card;
  }

  function jumpLink(label, viewName, after) {
    const a = document.createElement('a');
    a.href = '#/' + viewName;
    a.className = 'dash-jump';
    a.textContent = label;
    a.addEventListener('click', (e) => {
      e.preventDefault();
      navigate(viewName, after);
    });
    return a;
  }

  async function renderDashboard() {
    const grid = $('#dash-grid');
    const b = B();
    let statusData, jobs, flows;
    try {
      [statusData, jobs, flows] = await Promise.all([
        fetch('/api/status').then((r) => r.json()),
        fetch('/api/jobs').then((r) => r.json()),
        fetch('/api/flows').then((r) => r.json()),
      ]);
    } catch (e) {
      grid.innerHTML = '<p class="error">Failed to load dashboard: ' + e.message + '</p>';
      return;
    }
    const snaps = await Promise.all(
      (flows || []).map((f) =>
        fetch('/api/flows/' + encodeURIComponent(f.id)).then((r) => r.json()).catch(() => null))
    );

    grid.innerHTML = '';
    grid.appendChild(buildSourcesTile(statusData, b));
    grid.appendChild(buildJobsTile(jobs, b));
    for (const snap of snaps) {
      if (snap) grid.appendChild(buildFlowTile(snap, b));
    }
    grid.appendChild(buildQuickTile());
  }

  function buildSourcesTile(statusData, b) {
    const sources = (statusData && statusData.sources) || [];
    const counts = { ok: 0, warn: 0, err: 0, missing: 0 };
    const attention = [];
    for (const s of sources) {
      const k = b.statusKind ? b.statusKind(s) : { cls: s.exists ? 'ok' : 'missing' };
      counts[k.cls] = (counts[k.cls] || 0) + 1;
      if (k.cls !== 'ok') attention.push({ s, k });
    }

    const card = tile('Data sources', jumpLink('View all →', 'sources'));

    const summary = document.createElement('div');
    summary.className = 'dash-stat-row';
    summary.appendChild(stat(sources.length, 'sources'));
    summary.appendChild(stat(counts.ok || 0, 'ok', 'ok'));
    if (counts.warn) summary.appendChild(stat(counts.warn, 'pending', 'warn'));
    if (counts.err) summary.appendChild(stat(counts.err, 'error', 'err'));
    if (counts.missing) summary.appendChild(stat(counts.missing, 'missing', 'missing'));
    card.appendChild(summary);

    if (attention.length > 0) {
      const ul = document.createElement('ul');
      ul.className = 'dash-mini-list';
      for (const { s, k } of attention.slice(0, 6)) {
        const li = document.createElement('li');
        const pill = document.createElement('span');
        pill.className = 'status-pill ' + k.cls;
        pill.textContent = k.text;
        const name = document.createElement('span');
        name.className = 'dash-mini-name';
        name.textContent = s.name;
        li.appendChild(pill);
        li.appendChild(name);
        ul.appendChild(li);
      }
      card.appendChild(ul);
    } else {
      const p = document.createElement('p');
      p.className = 'muted small';
      p.textContent = 'All sources healthy.';
      card.appendChild(p);
    }
    return card;
  }

  function buildJobsTile(jobs, b) {
    const card = tile('Recent jobs', jumpLink('View all →', 'jobs'));
    if (!jobs || jobs.length === 0) {
      const p = document.createElement('p');
      p.className = 'muted small';
      p.textContent = 'No jobs yet.';
      card.appendChild(p);
      return card;
    }
    const ul = document.createElement('ul');
    ul.className = 'dash-job-list';
    for (const j of jobs.slice(0, 5)) {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = '#';
      a.className = 'dash-job-link';
      a.textContent = j.commandLine;
      a.title = 'Open in dock';
      a.addEventListener('click', (e) => {
        e.preventDefault();
        if (b.replayJob) b.replayJob(j.id);
      });
      const meta = document.createElement('span');
      meta.className = 'dash-job-meta';

      const time = document.createElement('time');
      time.className = 'job-time muted small';
      if (j.createdAt && b.formatRelative) {
        time.dateTime = j.createdAt;
        if (b.jobTimesTooltip) time.title = b.jobTimesTooltip(j);
        const dur = b.jobDuration ? b.jobDuration(j) : '';
        time.textContent = b.formatRelative(j.createdAt) + (dur ? ' · ' + dur : '');
      }

      const st = document.createElement('span');
      st.className = 'status ' + j.status;
      st.textContent = j.status + (j.exitCode != null ? ' (' + j.exitCode + ')' : '');

      meta.appendChild(time);
      meta.appendChild(st);
      li.appendChild(a);
      li.appendChild(meta);
      ul.appendChild(li);
    }
    card.appendChild(ul);
    return card;
  }

  function buildFlowTile(snap, b) {
    const pipeline = (snap.steps || []).filter((s) => (s.section || 'pipeline') === 'pipeline');
    const total = pipeline.length;
    const ok = pipeline.filter((s) => s.status === 'ok').length;
    const blocked = pipeline.filter((s) => s.status === 'blocked').length;
    const running = pipeline.filter((s) => s.status === 'running').length;
    const pct = total ? Math.round((ok / total) * 100) : 0;

    const right = jumpLink('Open →', 'workflows', () => {
      if (b.selectFlow) b.selectFlow(snap.id);
    });
    const card = tile(snap.title, right);

    const bar = document.createElement('div');
    bar.className = 'dash-progress';
    const fill = document.createElement('div');
    fill.className = 'dash-progress-fill';
    fill.style.width = pct + '%';
    bar.appendChild(fill);
    card.appendChild(bar);

    const meta = document.createElement('div');
    meta.className = 'dash-flow-meta small';
    const bits = [ok + '/' + total + ' steps ready'];
    if (running) bits.push(running + ' running');
    if (blocked) bits.push(blocked + ' blocked');
    meta.textContent = bits.join(' · ');
    if (running) meta.classList.add('running-accent');
    else if (blocked) meta.classList.add('blocked-accent');
    card.appendChild(meta);
    return card;
  }

  function buildQuickTile() {
    const card = tile('Quick links');
    const row = document.createElement('div');
    row.className = 'dash-quick';
    row.appendChild(jumpButton('Run a command', 'run'));
    row.appendChild(jumpButton('Workflows', 'workflows'));
    row.appendChild(jumpButton('Rules editor', 'rules'));
    row.appendChild(jumpButton('Taxa grouping', 'grouping'));
    card.appendChild(row);
    return card;
  }

  function jumpButton(label, viewName) {
    const btn = document.createElement('button');
    btn.className = 'ghost';
    btn.textContent = label;
    btn.addEventListener('click', () => navigate(viewName));
    return btn;
  }

  function stat(value, label, cls) {
    const wrap = document.createElement('div');
    wrap.className = 'dash-stat' + (cls ? ' ' + cls : '');
    const v = document.createElement('span');
    v.className = 'dash-stat-val';
    v.textContent = value;
    const l = document.createElement('span');
    l.className = 'dash-stat-label';
    l.textContent = label;
    wrap.appendChild(v);
    wrap.appendChild(l);
    return wrap;
  }

  // --- Boot -----------------------------------------------------------

  // Expose navigate so flows/jobs deep-links can use it if needed later.
  window.BeastieRouter = { navigate, showView };

  showView(currentView());
})();
