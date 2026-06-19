// Wikitext outputs view: generated *.wikitext lists in two layouts —
//   - Table:   one row per file, with a cached taxa count, byte size and mtime.
//   - By taxa: lists grouped under their taxa group in hierarchy order (e.g. sharks/rays under
//              fish), each status list shown as a chip ordered Extinct → … → DD.
// Both offer an on-demand Wikipedia preview (server proxies the file through the MediaWiki
// action=parse API) and a raw-source view. Taxa counts come from the generator's
// structure-metrics.json cache (see /api/wikitext/list), never by parsing the files here.
// Depends on window.Beastie (formatBytes / formatRelative / openFile) published by app.js.

(function () {
  const $ = (sel) => document.querySelector(sel);
  let loaded = false;
  let allFiles = [];
  let groups = [];            // [{name, displayName, children[], filters[], isParent}]
  let groupById = new Map();
  let effChildren = new Map();   // group id -> effective child ids (explicit + filter-inferred)
  let effChildIds = new Set();   // group ids that are nested under some other group
  let generatedAt = null;
  let taxaMaxSize = 0;           // largest visible list size, for the by-taxa heat scale

  // Status (preset) display order + labels for the by-taxa chips. Anything not listed sorts last.
  const PRESET_ORDER = ['ex', 'ew', 'threatened', 'endangered-combined', 'cr', 'en', 'vu', 'nt', 'lc', 'dd', 'all-status', 'conservation-dependent'];
  const PRESET_LABEL = {
    ex: 'Extinct', ew: 'EW', threatened: 'Threatened', 'endangered-combined': 'EN+CR',
    cr: 'CR', en: 'EN', vu: 'VU', nt: 'NT', lc: 'LC', dd: 'DD',
    'all-status': 'By status', 'conservation-dependent': 'LR/cd',
  };

  // View + stat preferences persist so the user's chosen layout is the default next visit.
  let view = localStorage.getItem('wt.view') || 'table';   // 'table' | 'taxa'
  let stat = localStorage.getItem('wt.stat') || 'count';   // 'count' | 'size' | 'off'
  let heat = localStorage.getItem('wt.heat') === '1';      // color-code largest lists by size

  async function load() {
    const tbody = $('#wt-tbody');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="5" class="muted">Loading&hellip;</td></tr>';
    try {
      const [list, grp] = await Promise.all([
        fetch('/api/wikitext/list').then((r) => r.json()),
        fetch('/api/grouping/groups').then((r) => r.json()).catch(() => ({ groups: [] })),
      ]);
      allFiles = list.files || [];
      generatedAt = list.generatedAt || null;
      groups = (grp && grp.groups) || [];
      groupById = new Map(groups.map((g) => [g.name, g]));
      buildTree();

      const count = $('#wt-count');
      if (count) count.textContent = allFiles.length + ' files';
      renderCacheNote();
      render();
      loaded = true;
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="5" class="error">' + escapeHtml(e.message) + '</td></tr>';
    }
  }

  function renderCacheNote() {
    const note = $('#wt-cache-note');
    if (!note) return;
    const B = window.Beastie || {};
    if (generatedAt) {
      note.hidden = false;
      note.textContent = 'Taxa counts cached from the last generation ' +
        (B.formatRelative ? B.formatRelative(generatedAt) : '') +
        '. A “~” marks a list rebuilt since then (count may be stale).';
    } else {
      note.hidden = false;
      note.textContent = 'No structure-metrics.json cache found — run a generation to populate taxa counts.';
    }
  }

  function currentFilter() {
    return (($('#wt-search') || {}).value || '').toLowerCase();
  }
  function matches(f, q) {
    return !q || f.title.toLowerCase().includes(q) || f.name.toLowerCase().includes(q);
  }

  function render() {
    // Toggle which layout is visible + which controls are relevant.
    $('#wt-table-wrap').hidden = view !== 'table';
    $('#wt-taxa').hidden = view !== 'taxa';
    // The per-list stat only applies to the by-taxa chips, so hide it on the table.
    const statGroup = $('.wt-stat-group');
    if (statGroup) statGroup.style.display = view === 'taxa' ? '' : 'none';
    const heatBox = $('#wt-heat');
    if (heatBox) heatBox.checked = heat;
    syncActive('#wt-view', '.view-tab', 'view', view);
    syncActive('#wt-stat', '.seg-btn', 'stat', stat);
    if (view === 'taxa') renderTaxa();
    else renderTable();
  }

  // Heat level 0–3 for a file's byte size relative to the largest list currently shown.
  function heatLevel(size, max) {
    if (!heat || !max) return 0;
    const r = size / max;
    if (r >= 0.66) return 3;
    if (r >= 0.33) return 2;
    if (r >= 0.12) return 1;
    return 0;
  }

  function syncActive(sel, btnSel, attr, value) {
    const seg = $(sel);
    if (!seg) return;
    for (const btn of seg.querySelectorAll(btnSel)) {
      btn.classList.toggle('active', btn.dataset[attr] === value);
    }
  }

  // --- Table layout ----------------------------------------------------

  function renderTable() {
    const tbody = $('#wt-tbody');
    if (!tbody) return;
    const B = window.Beastie || {};
    const q = currentFilter();
    const rows = allFiles.filter((f) => matches(f, q));

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="muted">No matching outputs.</td></tr>';
      return;
    }
    const maxSize = rows.reduce((m, f) => Math.max(m, f.size || 0), 0);
    tbody.innerHTML = '';
    for (const f of rows) {
      const tr = document.createElement('tr');

      const title = document.createElement('td');
      title.textContent = f.title;

      const taxa = document.createElement('td');
      taxa.className = 'wt-num muted';
      taxa.appendChild(taxaCountNode(f));

      const size = document.createElement('td');
      const lvl = heatLevel(f.size, maxSize);
      if (lvl) size.className = 'heat-' + lvl;
      size.textContent = B.formatBytes ? B.formatBytes(f.size) : f.size + ' B';

      const mod = document.createElement('td');
      mod.className = 'muted small';
      mod.textContent = B.formatRelative ? B.formatRelative(f.modified) : '';

      const act = document.createElement('td');
      act.className = 'wt-actions';
      act.appendChild(button('Preview', 'primary', () => preview(f)));
      act.appendChild(button('Source', '', () => { if (B.openFile) B.openFile('wikipedia-output', f.name); }));

      tr.appendChild(title);
      tr.appendChild(taxa);
      tr.appendChild(size);
      tr.appendChild(mod);
      tr.appendChild(act);
      tbody.appendChild(tr);
    }
  }

  // A taxa-count cell/inline node: the number, an em-dash when uncached, and a "~" stale marker.
  function taxaCountNode(f) {
    const span = document.createElement('span');
    if (f.taxa == null) {
      span.textContent = '—';
      span.title = 'No cached count — regenerate this list.';
      return span;
    }
    span.textContent = f.taxa.toLocaleString();
    if (f.taxaStale) {
      const s = document.createElement('span');
      s.className = 'wt-stale';
      s.textContent = '~';
      s.title = 'Rebuilt since the cached count was taken — count may be out of date.';
      span.appendChild(s);
    }
    return span;
  }

  // --- Taxa-group tree -------------------------------------------------
  // The YAML only declares `children` for true parent lists (fish, invertebrates). To also nest
  // display-only relationships (conifers/cycads under plants, marine-mammals under mammals) without
  // changing what the generator produces, we infer extra parent→child links from filter containment:
  // a group nests under the most-specific other group whose filters are a strict subset of its own
  // and that itself has lists (or declared children).

  function buildTree() {
    effChildren = new Map(groups.map((g) => [g.name, (g.children || []).slice()]));
    effChildIds = new Set();
    for (const g of groups) for (const c of (g.children || [])) effChildIds.add(c);

    const fileGroupIds = new Set(allFiles.map((f) => f.taxaGroup).filter(Boolean));
    for (const c of groups) {
      if (effChildIds.has(c.name)) continue;       // already nested via explicit children
      const cSig = sigSet(c);
      if (!cSig.size) continue;
      let best = null, bestSize = -1;
      for (const p of groups) {
        if (p.name === c.name) continue;
        const pSig = sigSet(p);
        if (!pSig.size || pSig.size >= cSig.size) continue;  // parent must be strictly less specific
        if (!isSubset(pSig, cSig)) continue;
        const qualifies = (p.children && p.children.length) || fileGroupIds.has(p.name);
        if (qualifies && pSig.size > bestSize) { best = p; bestSize = pSig.size; }
      }
      if (best) { effChildren.get(best.name).push(c.name); effChildIds.add(c.name); }
    }
  }

  function filterSig(f) {
    if (f.system) return 'system|' + f.system;
    if (f.exclude && f.exclude.length) return f.rank + '|exclude=' + f.exclude.slice().sort().join(',');
    if (f.values && f.values.length) return f.rank + '|values=' + f.values.slice().sort().join(',');
    return f.rank + '|' + (f.value || '');
  }
  function sigSet(g) { return new Set((g.filters || []).map(filterSig)); }
  function isSubset(a, b) { for (const x of a) if (!b.has(x)) return false; return true; }

  // --- By-taxa layout --------------------------------------------------

  function renderTaxa() {
    const root = $('#wt-taxa');
    if (!root) return;
    const q = currentFilter();
    const visible = allFiles.filter((f) => matches(f, q));
    taxaMaxSize = visible.reduce((m, f) => Math.max(m, f.size || 0), 0);

    // Bucket files by taxa-group id; unmapped/explicit lists fall into the catch-all.
    const byGroup = new Map();
    const ungrouped = [];
    for (const f of visible) {
      if (f.taxaGroup && groupById.has(f.taxaGroup)) {
        if (!byGroup.has(f.taxaGroup)) byGroup.set(f.taxaGroup, []);
        byGroup.get(f.taxaGroup).push(f);
      } else {
        ungrouped.push(f);
      }
    }

    root.innerHTML = '';
    if (!visible.length) {
      root.innerHTML = '<p class="muted">No matching outputs.</p>';
      return;
    }

    const hasAny = (id) => {
      if ((byGroup.get(id) || []).length) return true;
      return (effChildren.get(id) || []).some(hasAny);
    };

    // Top-level groups = those that aren't nested under another group, in YAML order.
    for (const g of groups) {
      if (effChildIds.has(g.name)) continue;
      if (!hasAny(g.name)) continue;
      root.appendChild(renderGroupNode(g, byGroup, 0, hasAny));
    }

    if (ungrouped.length) {
      const wrap = document.createElement('div');
      wrap.className = 'wt-tg';
      const head = document.createElement('div');
      head.className = 'wt-tg-head';
      const name = document.createElement('span');
      name.className = 'wt-tg-name';
      name.textContent = 'Other lists';
      head.appendChild(name);
      head.appendChild(chipRow(ungrouped));
      wrap.appendChild(head);
      root.appendChild(wrap);
    }
  }

  function renderGroupNode(g, byGroup, depth, hasAny) {
    const wrap = document.createElement('div');
    wrap.className = 'wt-tg';
    wrap.style.marginLeft = depth ? '1.25rem' : '0';

    const head = document.createElement('div');
    head.className = 'wt-tg-head';

    const name = document.createElement('span');
    name.className = 'wt-tg-name';
    name.textContent = g.displayName || g.name;
    head.appendChild(name);

    // Deep-link to the Taxa-grouping editor focused on this group, where status lists can be
    // combined (one "Threatened" page) or split (separate CR/EN/VU), then regenerated.
    const sj = document.createElement('a');
    sj.href = '#/grouping';
    sj.className = 'wt-tg-edit';
    sj.textContent = '⚙ split/join';
    sj.title = 'Combine or split this group’s status lists in the Taxa-grouping editor';
    sj.addEventListener('click', (e) => {
      e.preventDefault();
      if (window.BeastieRouter) {
        window.BeastieRouter.navigate('grouping', () => {
          if (window.BeastieGrouping) window.BeastieGrouping.focus(g.name);
        });
      }
    });
    head.appendChild(sj);

    const files = (byGroup.get(g.name) || []).slice().sort(presetCompare);
    if (files.length) head.appendChild(chipRow(files));
    wrap.appendChild(head);

    for (const childId of (effChildren.get(g.name) || [])) {
      const child = groupById.get(childId);
      if (child && hasAny(childId)) {
        wrap.appendChild(renderGroupNode(child, byGroup, depth + 1, hasAny));
      }
    }
    return wrap;
  }

  function chipRow(files) {
    const row = document.createElement('span');
    row.className = 'wt-chips';
    for (const f of files.slice().sort(presetCompare)) row.appendChild(chip(f));
    return row;
  }

  function chip(f) {
    const B = window.Beastie || {};
    const b = document.createElement('button');
    b.className = 'wt-chip';
    if (f.isParent) b.classList.add('parent');
    const lvl = heatLevel(f.size, taxaMaxSize);
    if (lvl) b.classList.add('heat-' + lvl);

    const label = document.createElement('span');
    label.className = 'wt-chip-label';
    label.textContent = chipLabel(f);
    b.appendChild(label);

    const s = statText(f);
    if (s) {
      const st = document.createElement('span');
      st.className = 'wt-chip-stat';
      st.textContent = s;
      b.appendChild(st);
    }

    const bits = [f.title];
    if (f.taxa != null) bits.push(f.taxa.toLocaleString() + ' taxa' + (f.taxaStale ? ' (stale)' : ''));
    if (B.formatBytes) bits.push(B.formatBytes(f.size));
    if (B.formatRelative) bits.push(B.formatRelative(f.modified));
    b.title = bits.join('  ·  ');

    b.addEventListener('click', () => preview(f));
    return b;
  }

  function chipLabel(f) {
    if (f.preset && PRESET_LABEL[f.preset]) return PRESET_LABEL[f.preset];
    if (f.preset) return f.preset;
    return f.title; // explicit/cross-taxa lists with no preset
  }

  function presetCompare(a, b) {
    const ia = a.preset ? PRESET_ORDER.indexOf(a.preset) : -1;
    const ib = b.preset ? PRESET_ORDER.indexOf(b.preset) : -1;
    const ra = ia < 0 ? 999 : ia;
    const rb = ib < 0 ? 999 : ib;
    if (ra !== rb) return ra - rb;
    return a.title.localeCompare(b.title);
  }

  function statText(f) {
    const B = window.Beastie || {};
    if (stat === 'off') return '';
    if (stat === 'size') return B.formatBytes ? B.formatBytes(f.size) : f.size + ' B';
    // count
    if (f.taxa == null) return '';
    return f.taxa.toLocaleString() + (f.taxaStale ? '~' : '');
  }

  function button(label, extra, onClick) {
    const b = document.createElement('button');
    b.className = 'ghost small' + (extra ? ' ' + extra : '');
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  }

  // --- preview modal ---------------------------------------------------

  const modal = $('#wt-preview');
  const frame = $('#wt-preview-frame');
  const ptitle = $('#wt-preview-title');
  const popen = $('#wt-preview-open');

  if (modal) {
    $('#wt-preview-close').addEventListener('click', closePreview);
    $('#wt-preview .modal-backdrop').addEventListener('click', closePreview);
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && !modal.hidden) closePreview();
    });
  }

  function closePreview() {
    modal.hidden = true;
    frame.srcdoc = '';
  }

  function wikiUrl(title) {
    return 'https://en.wikipedia.org/wiki/' + encodeURIComponent((title || '').replace(/ /g, '_'));
  }

  async function preview(f) {
    modal.hidden = false;
    ptitle.textContent = f.title + ' — Wikipedia preview';
    // Link to the same-named Wikipedia article up front, before the render lands.
    popen.href = wikiUrl(f.title);
    popen.hidden = false;
    frame.srcdoc = placeholderDoc('Rendering through Wikipedia&hellip;', '#555');
    try {
      const data = await fetch('/api/wikitext/preview?file=' + encodeURIComponent(f.name)).then(async (r) => {
        if (!r.ok) {
          const err = await r.json().catch(() => ({}));
          throw new Error(err.error || ('HTTP ' + r.status));
        }
        return r.json();
      });
      frame.srcdoc = buildDoc(data.title, data.html);
      if (data.title) popen.href = wikiUrl(data.title);
    } catch (e) {
      frame.srcdoc = placeholderDoc('Preview failed: ' + escapeHtml(e.message), '#b00');
    }
  }

  function buildDoc(title, html) {
    // Render inside an iframe with Wikipedia's content stylesheet for fidelity. The <base> makes the
    // article's relative /wiki/ links resolve to Wikipedia.
    return '<!doctype html><html><head><meta charset="utf-8">'
      + '<base href="https://en.wikipedia.org/wiki/">'
      + '<link rel="stylesheet" href="https://en.wikipedia.org/w/load.php?lang=en&modules=site.styles&only=styles&skin=vector-2022">'
      + '<style>html,body{margin:0}body{padding:20px 28px;background:#fff;color:#202122;'
      + 'font-family:sans-serif;font-size:14px;line-height:1.6}'
      + '.mw-parser-output{max-width:60em}'
      + 'h1.fp{font-family:"Linux Libertine",Georgia,serif;font-weight:normal;border-bottom:1px solid #a2a9b1;'
      + 'margin:0 0 .4em;padding-bottom:.25em;font-size:1.8em}</style>'
      + '</head><body><h1 class="fp">' + escapeHtml(title) + '</h1>'
      + '<div class="mw-parser-output">' + html + '</div></body></html>';
  }

  function placeholderDoc(message, color) {
    return '<!doctype html><body style="margin:0;padding:24px;font-family:sans-serif;color:'
      + color + '">' + message + '</body>';
  }

  function escapeHtml(s) {
    return (s || '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  }

  // --- wiring ----------------------------------------------------------

  const refreshBtn = $('#wt-refresh');
  if (refreshBtn) refreshBtn.addEventListener('click', load);
  const search = $('#wt-search');
  if (search) search.addEventListener('input', render);

  const viewSeg = $('#wt-view');
  if (viewSeg) viewSeg.addEventListener('click', (e) => {
    const btn = e.target.closest('.view-tab');
    if (!btn) return;
    view = btn.dataset.view;
    localStorage.setItem('wt.view', view);
    render();
  });
  const statSeg = $('#wt-stat');
  if (statSeg) statSeg.addEventListener('click', (e) => {
    const btn = e.target.closest('.seg-btn');
    if (!btn) return;
    stat = btn.dataset.stat;
    localStorage.setItem('wt.stat', stat);
    render();
  });
  const heatBox = $('#wt-heat');
  if (heatBox) heatBox.addEventListener('change', () => {
    heat = heatBox.checked;
    localStorage.setItem('wt.heat', heat ? '1' : '0');
    render();
  });

  // Router calls this when the view is shown; load once, then on explicit refresh.
  window.BeastieOutputs = {
    load: () => { if (!loaded) load(); },
    reload: load,
  };
})();
