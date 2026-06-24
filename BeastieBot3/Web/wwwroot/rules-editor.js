// Self-contained wiring for the Taxa-grouping and Rules-editor cards.
// Talks only to the new endpoints (/api/grouping/*, /api/rules*, /api/rules-draft/*).
// Kept separate from app.js so it doesn't touch the existing job-runner IIFE.

(function () {
  const $ = (sel) => document.querySelector(sel);
  const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));

  async function getJson(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }
  async function postJson(url, body) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    });
    const data = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, data };
  }

  // ===================== Taxa grouping =====================

  let lastCounts = null;

  async function loadGroups() {
    try {
      const { groups } = await getJson('/api/grouping/groups');
      const sel = $('#grp-parent');
      sel.innerHTML = '';
      for (const g of groups) {
        const opt = document.createElement('option');
        opt.value = g.name;
        opt.textContent = g.displayName + (g.isParent ? ' (parent)' : '');
        sel.appendChild(opt);
      }
    } catch (e) {
      $('#grp-msg').textContent = 'Failed to load groups: ' + e.message;
    }
  }

  // Deep-link entry point: select a group and show its counts + impact/knobs panel. Called by the
  // Wikitext-outputs "split/join" links via window.BeastieGrouping.focus(group).
  async function focusGroup(groupId) {
    const sel = $('#grp-parent');
    if (!sel) return;
    if (!sel.options.length) await loadGroups();
    if (groupId && Array.from(sel.options).some((o) => o.value === groupId)) {
      sel.value = groupId;
    } else if (groupId) {
      $('#grp-msg').textContent = `Group '${groupId}' is not a configured taxa group.`;
      return;
    }
    await loadCounts();
  }

  async function loadCounts() {
    const group = $('#grp-parent').value;
    const rank = $('#grp-rank').value;
    $('#grp-msg').textContent = 'Loading counts…';
    try {
      const data = await getJson(`/api/grouping/children-counts?group=${encodeURIComponent(group)}&childRank=${encodeURIComponent(rank)}`);
      lastCounts = data;
      renderCounts(data);
      $('#grp-save').hidden = false;
      $('#grp-msg').textContent = `${data.rows.length} sub-taxa at rank '${data.childRank}', grand total ${data.grandTotal}. Tick existing groups to make them sub-lists.`;
      loadImpact(group, rank);
    } catch (e) {
      $('#grp-msg').textContent = 'Failed: ' + e.message;
    }
  }

  // Counts-only page-size impact for the selected group (GET /api/lists/impact). Shows the cost of
  // combining categories vs separate pages, and which sub-pages would bust the group's size budget.
  // `candidateBudget` (optional) re-runs the verdicts against a what-if budget without saving anything.
  async function loadImpact(group, rank, candidateBudget) {
    const el = $('#grp-impact');
    if (!el) return;
    el.innerHTML = '<p class="muted small">Sizing pages…</p>';
    try {
      let url = `/api/lists/impact?group=${encodeURIComponent(group)}&splitRank=${encodeURIComponent(rank)}`;
      if (candidateBudget) url += `&budget=${encodeURIComponent(candidateBudget)}`;
      const d = await getJson(url);
      el.innerHTML = renderImpact(d, rank, candidateBudget);
    } catch (e) {
      el.innerHTML = `<p class="muted small">No size impact for this group — ${esc(e.message || e)}</p>`;
    }
  }

  function setImpactMsg(text) {
    const m = $('#imp-msg');
    if (m) m.textContent = text;
  }

  // Generate a single list into the output dir (reuses the job runner). Reads SOURCE rules so any
  // applied knob edits take effect without a rebuild. The file is then previewable in Wikitext outputs.
  async function generateList(listId) {
    setImpactMsg(`Starting generate-lists --list ${listId}…`);
    const loc = await getJson('/api/rules/locations').catch(() => null);
    const args = ['--list', listId];
    if (loc && !loc.isBuildOutputFallback) {
      args.push('--config', loc.sourceRulesDir + '/wikipedia-lists.yml', '--rules', loc.sourceRulesDir + '/rules-list.txt');
    }
    const { ok, data } = await postJson('/api/jobs', { command: 'wikipedia generate-lists', args });
    setImpactMsg(ok
      ? `Started generate-lists --list ${listId} (job ${data.id}). Watch the "Run a command" card, then preview it under "Wikitext outputs".`
      : 'Failed to start: ' + (data.error || ''));
  }

  // Persist the tuning knobs to the draft rules (size_budget on the group, category_split on the list
  // entry). The user then reviews via the Rules-editor diff and Applies to source.
  async function saveKnobs() {
    const group = $('#grp-parent').value;
    const body = { group };
    const budget = $('#imp-knob-budget') ? $('#imp-knob-budget').value.trim() : '';
    if (budget) body.sizeBudgetMaxEntries = parseInt(budget, 10);
    // Only write category_split when the user actually changed it away from the current setting,
    // so a budget-only save doesn't rewrite the (unchanged) split line.
    const splitSel = $('#imp-knob-split');
    const split = splitSel ? splitSel.value : '';
    const curSplit = splitSel ? (splitSel.dataset.current || '') : '';
    if (split && split !== curSplit) body.categorySplit = split;
    if (body.sizeBudgetMaxEntries == null && !body.categorySplit) {
      setImpactMsg('Nothing to save — change the category split and/or set a budget first.');
      return;
    }
    const { ok, data } = await postJson('/api/grouping/knobs', body);
    setImpactMsg(ok
      ? `Saved to draft: ${(data.changed || []).join(', ') || 'no change'}. Review in the Rules editor (Diff) → Apply to source → then "Regenerate group".`
      : 'Save failed: ' + (data.error || '') + (data.hint ? ' — ' + data.hint : ''));
  }

  // Regenerate every list for the selected taxa group from SOURCE rules (so an applied split/join takes
  // effect without a rebuild). Apply the draft first — this does NOT read the draft.
  async function regenerateGroup() {
    const group = $('#grp-parent').value;
    setImpactMsg(`Starting generate-lists --taxa-group ${group}…`);
    const loc = await getJson('/api/rules/locations').catch(() => null);
    const args = ['--taxa-group', group];
    if (loc && !loc.isBuildOutputFallback) {
      args.push('--config', loc.sourceRulesDir + '/wikipedia-lists.yml', '--rules', loc.sourceRulesDir + '/rules-list.txt');
    }
    const { ok, data } = await postJson('/api/jobs', { command: 'wikipedia generate-lists', args });
    setImpactMsg(ok
      ? `Started generate-lists --taxa-group ${group} (job ${data.id}). Reads SOURCE rules, so Apply your draft first if you changed the split. Watch "Run a command", then preview under "Wikitext outputs".`
      : 'Failed to start: ' + (data.error || ''));
  }

  function onImpactClick(ev) {
    const gen = ev.target.closest('[data-gen-list]');
    if (gen) { generateList(gen.getAttribute('data-gen-list')); return; }
    if (ev.target.closest('[data-apply-budget]')) {
      const v = $('#imp-budget') ? $('#imp-budget').value.trim() : '';
      loadImpact($('#grp-parent').value, $('#grp-rank').value, v ? parseInt(v, 10) : null);
      return;
    }
    if (ev.target.closest('[data-save-knobs]')) { saveKnobs(); return; }
    if (ev.target.closest('[data-regen-group]')) { regenerateGroup(); }
  }

  function renderImpact(d, rank, candidateBudget) {
    const num = (n) => (n || 0).toLocaleString();
    const verdict = (o) => o.overBudget == null ? ''
      : (o.overBudget ? `<span class="feat-no">exceeds ${num(d.budget)}</span>` : `<span class="feat-yes">fits</span>`);
    const struct = (o) => {
      const s = o.structure;
      if (!s) return '<span class="muted">—</span>';
      let t = `${num(s.headings)} hd · depth ${s.maxDepth}`;
      if (s.singleItemHeadings) t += ` · ${s.singleItemHeadings}&times; single`;
      if (s.problems && s.problems.length) {
        t = `<span class="feat-no" title="${esc(s.problems.join('; '))}">${t}</span>`;
      }
      if (s.fileBytes) {
        const sz = s.fileBytes >= 1e6 ? (s.fileBytes / 1e6).toFixed(1) + ' MB' : (s.fileBytes / 1000).toFixed(0) + ' KB';
        t += ` · <span class="${s.fileBytes > 2e6 ? 'feat-no' : 'muted'}">${sz}</span>`;
      }
      return t;
    };
    const action = (o) => o.listId
      ? `<button class="ghost xsmall" data-gen-list="${esc(o.listId)}" title="Generate ${esc(o.listId)} into the output dir">Generate</button>`
      : '';
    const opts = d.options.map((o) =>
      `<tr><td class="wt-left">${esc(o.label)}</td><td>${num(o.bullets)}</td><td>${num(o.species)}</td>`
      + `<td>${verdict(o)}</td><td class="wt-left">${struct(o)}</td><td>${action(o)}</td></tr>`).join('');

    let sub = '';
    if (d.subPages && d.subPages.length) {
      const over = d.subPages.filter((s) => s.overBudget);
      const offenders = over.slice(0, 8).map((s) => `${esc(titleCase(s.child))} (${num(s.total)})`).join(', ');
      sub = `<p class="muted small">Split at ${esc(rank)}: ${d.subPages.length} sub-page(s)`
        + (d.budget ? `, <strong>${over.length}</strong> exceed ${num(d.budget)} bullets${over.length ? ' — ' + offenders : ''}` : '')
        + '.</p>';
    }

    // Candidate-budget what-if (A/B): re-runs the verdicts against a trial budget without saving.
    const candNote = candidateBudget
      ? ` <span class="feat-no">— verdicts shown at candidate ${num(candidateBudget)}</span>` : '';
    const budgetNote = d.budget ? ` <span class="muted small">(budget ${num(d.budget)} bullets)</span>` : '';
    const whatIf = `<div class="imp-controls">`
      + `<label>What-if budget <input id="imp-budget" type="number" min="0" step="500" `
      + `value="${candidateBudget || ''}" placeholder="${d.budget || 'e.g. 5000'}"></label>`
      + `<button class="ghost xsmall" data-apply-budget>Preview verdicts</button></div>`;

    // Tuning knobs → draft rules (size_budget on the group, category_split on the list entry).
    // The split <select> is pre-set to the group's current setting (from the draft), so it shows what's
    // in effect; saveKnobs only writes it when the user changes it (see data-current).
    const cur = d.currentSplit || '';
    const splitOpt = (val, label) =>
      `<option value="${val}"${val === cur ? ' selected' : ''}>${label}${val === cur ? ' — current' : ''}</option>`;
    const knobs = `<div class="imp-controls">`
      + `<label>Set budget <input id="imp-knob-budget" type="number" min="0" step="500" placeholder="${d.budget || 'max_entries'}"></label>`
      + `<label>Category split <select id="imp-knob-split" data-current="${esc(cur)}">`
      + (cur ? '' : `<option value="" selected>(keep current)</option>`)
      + splitOpt('default', 'default (per-status pages)')
      + splitOpt('separate', 'separate')
      + splitOpt('combined-threatened', 'combined-threatened')
      + splitOpt('merged', 'merged (threatened + extinct combined)')
      + splitOpt('all-status', 'all-status') + `</select></label>`
      + `<button class="ghost xsmall" data-save-knobs>Save knobs to draft</button>`
      + `<button class="ghost xsmall" data-regen-group title="Run generate-lists for this taxa group from source rules (Apply your draft first)">Regenerate group</button></div>`;

    return `<h4 class="grp-impact-title">Page-size impact${budgetNote}${candNote}</h4>`
      + `<p class="muted small">Bullets = species + subspecies/varieties rendered; species = the prose headline. Counts only — nothing is generated.</p>`
      + `<div class="feature-table-wrap"><table class="feature-table"><thead><tr>`
      + `<th class="wt-left">Page option</th><th>Bullets</th><th>Species</th><th>Verdict</th>`
      + `<th class="wt-left">Structure (last gen)</th><th></th></tr></thead>`
      + `<tbody>${opts}</tbody></table></div>${sub}`
      + whatIf + knobs
      + `<p class="muted small" id="imp-msg"></p>`;
  }

  function titleCase(s) {
    return s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : s;
  }

  function renderCounts(data) {
    const head = ['', 'Sub-taxon', 'Group', ...data.columns, 'Total'];
    const rows = data.rows.map((r) => {
      const cb = r.existingGroup
        ? `<input type="checkbox" class="grp-child" value="${esc(r.existingGroup)}" ${r.isChild ? 'checked' : ''}>`
        : '';
      // Rows without a matching taxa-group get a "＋ define" button that pre-fills the create panel.
      const grpCell = r.existingGroup
        ? esc(r.existingGroup)
        : `<button class="ghost xsmall" data-make-group="${esc(r.key)}" title="Define a new taxa-group for ${esc(r.key)}">＋ define</button>`;
      const cells = r.counts.map((c) => `<td>${c}</td>`).join('');
      return `<tr><td>${cb}</td><td>${esc(r.key)}</td><td>${grpCell}</td>${cells}<td><strong>${r.total}</strong></td></tr>`;
    });
    $('#grp-counts').innerHTML =
      `<table><thead><tr>${head.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table>`;
  }

  // ---- create-group: dynamic filter rows (rank value/any-of/not, or system tags OR'd) ----

  const RANK_OPTS = ['class', 'order', 'family', 'genus', 'phylum', 'kingdom'];

  function filterRowEl() {
    const row = document.createElement('div');
    row.className = 'cg-filter-row filter-bar';
    row.innerHTML =
      `<select class="cgf-type"><option value="rank">rank</option><option value="system">system</option></select>`
      + `<span class="cgf-rank-fields">`
      + `<select class="cgf-rank">${RANK_OPTS.map((r) => `<option value="${r}">${r}</option>`).join('')}</select>`
      + `<select class="cgf-op"><option value="is">is</option><option value="any">any of</option><option value="not">not</option></select>`
      + `<input class="cgf-value" placeholder="MAGNOLIOPSIDA (comma-separate for any-of / not)">`
      + `</span>`
      + `<span class="cgf-system-fields" hidden><input class="cgf-systems" placeholder="Marine, Freshwater (comma = OR)"></span>`
      + `<button class="cgf-remove ghost xsmall" type="button" title="Remove filter">×</button>`;
    return row;
  }

  function syncRowType(row) {
    const isSystem = row.querySelector('.cgf-type').value === 'system';
    row.querySelector('.cgf-rank-fields').hidden = isSystem;
    row.querySelector('.cgf-system-fields').hidden = !isSystem;
  }

  function addFilterRow(opts) {
    const cont = $('#cg-filters');
    if (!cont) return null;
    const row = filterRowEl();
    cont.appendChild(row);
    if (opts) {
      if (opts.type) row.querySelector('.cgf-type').value = opts.type;
      if (opts.rank) row.querySelector('.cgf-rank').value = opts.rank;
      if (opts.op) row.querySelector('.cgf-op').value = opts.op;
      if (opts.value != null) row.querySelector('.cgf-value').value = opts.value;
      if (opts.systems != null) row.querySelector('.cgf-systems').value = opts.systems;
    }
    syncRowType(row);
    return row;
  }

  function onFiltersClick(ev) {
    const rm = ev.target.closest('.cgf-remove');
    if (rm) { const row = rm.closest('.cg-filter-row'); if (row) row.remove(); }
  }
  function onFiltersChange(ev) {
    if (ev.target.classList.contains('cgf-type')) syncRowType(ev.target.closest('.cg-filter-row'));
  }

  // Read the filter rows into the API shape: {rank, value|values|exclude} or {system|systems}.
  function collectFilters() {
    const out = [];
    for (const row of document.querySelectorAll('#cg-filters .cg-filter-row')) {
      if (row.querySelector('.cgf-type').value === 'system') {
        const tags = (row.querySelector('.cgf-systems').value || '').split(',').map((s) => s.trim()).filter(Boolean);
        if (tags.length) out.push(tags.length === 1 ? { system: tags[0] } : { systems: tags });
      } else {
        const rank = row.querySelector('.cgf-rank').value;
        const op = row.querySelector('.cgf-op').value;
        const vals = (row.querySelector('.cgf-value').value || '').split(',').map((s) => s.trim()).filter(Boolean);
        if (!vals.length) continue;
        if (op === 'is') out.push({ rank, value: vals[0] });
        else if (op === 'any') out.push({ rank, values: vals });
        else out.push({ rank, exclude: vals });
      }
    }
    return out;
  }

  // Pre-fill the create panel's FIRST filter row from a counts-table sub-taxon, and suggest key/name.
  function prefillCreate(value) {
    const panel = $('#grp-create');
    if (!panel) return;
    panel.open = true;
    let row = document.querySelector('#cg-filters .cg-filter-row') || addFilterRow();
    row.querySelector('.cgf-type').value = 'rank';
    syncRowType(row);
    row.querySelector('.cgf-rank').value = $('#grp-rank').value;
    row.querySelector('.cgf-op').value = 'is';
    row.querySelector('.cgf-value').value = value || '';
    const slug = (value || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
    if (!$('#cg-key').value) $('#cg-key').value = slug;
    if (!$('#cg-name').value && value) {
      $('#cg-name').value = value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
    }
    $('#cg-key').focus();
  }

  function onCountsClick(ev) {
    const mk = ev.target.closest('[data-make-group]');
    if (mk) prefillCreate(mk.getAttribute('data-make-group'));
  }

  // Create a brand-new taxa-group (+ its list entry) in the draft, then refresh so it shows up as a
  // tickable sub-group of the selected parent.
  async function createGroup() {
    const key = $('#cg-key').value.trim();
    const name = $('#cg-name').value.trim();
    const filters = collectFilters();
    if (!key || !name) { $('#cg-msg').textContent = 'Key and Name are required.'; return; }
    if (!filters.length && !$('#cg-inherit').checked) {
      $('#cg-msg').textContent = 'Add at least one filter, or tick "Inherit parent\'s filters".';
      return;
    }
    const body = {
      key, name,
      adjective: $('#cg-adj').value.trim() || null,
      listingStyle: $('#cg-style').value || null,
      parentGroup: $('#grp-parent').value || null,
      inheritFilters: $('#cg-inherit').checked,
      filters,
      categorySplit: $('#cg-split').value || null,
    };
    $('#cg-msg').textContent = 'Creating…';
    const { ok, data } = await postJson('/api/grouping/create-group', body);
    if (!ok) {
      $('#cg-msg').textContent = 'Failed: ' + (data.error || '') + (data.hint ? ' — ' + data.hint : '');
      return;
    }
    $('#cg-msg').textContent =
      `Created '${data.group}' → wrote ${(data.changed || []).join(', ')} (pages: ${data.pagePlan}). ${data.hint || ''}`;
    // Reset for the next create.
    ['#cg-key', '#cg-name', '#cg-adj'].forEach((s) => { if ($(s)) $(s).value = ''; });
    $('#cg-filters').innerHTML = '';
    addFilterRow();
    const parent = body.parentGroup;
    await loadGroups();
    if (parent) $('#grp-parent').value = parent;
    await loadCounts();
  }

  async function saveChildren() {
    if (!lastCounts) return;
    const group = $('#grp-parent').value;
    const children = Array.from(document.querySelectorAll('.grp-child:checked')).map((c) => c.value);
    const { ok, data } = await postJson('/api/grouping/children', { group, children });
    $('#grp-msg').textContent = ok
      ? `Saved ${children.length} sub-group(s) to draft taxa-groups.yml. Use the Rules editor to Apply to source.`
      : `Save failed: ${data.error || ''} ${data.hint || ''}`;
  }

  // ===================== Rules editor =====================

  async function loadLocations() {
    try {
      const loc = await getJson('/api/rules/locations');
      const warn = loc.isBuildOutputFallback
        ? ' <strong style="color:#b00">⚠ source rules dir not found — Apply is disabled. Set [Dirs] rules_source_dir in paths.ini.</strong>'
        : '';
      $('#rules-locations').innerHTML =
        `Source: <code>${esc(loc.sourceRulesDir)}</code> &middot; Draft: <code>${esc(loc.draftRoot)}</code>${warn}`;
      $('#rules-apply').disabled = !!loc.isBuildOutputFallback;
    } catch (e) {
      $('#rules-locations').textContent = 'Failed to load locations: ' + e.message;
    }
  }

  let currentMtime = null;

  async function loadFileList() {
    try {
      const { entries } = await getJson('/api/rules-draft/list');
      const sel = $('#rules-file');
      sel.innerHTML = '';
      for (const e of entries.filter((x) => x.editable)) {
        const opt = document.createElement('option');
        opt.value = e.path;
        opt.textContent = e.path;
        sel.appendChild(opt);
      }
      if (sel.value) await loadFile();
    } catch (e) {
      $('#rules-msg').textContent = 'Failed to list draft: ' + e.message;
    }
  }

  async function loadFile() {
    const path = $('#rules-file').value;
    if (!path) return;
    try {
      const data = await getJson('/api/rules-draft/read?path=' + encodeURIComponent(path));
      $('#rules-text').value = data.content;
      currentMtime = data.modified;
      $('#rules-msg').textContent = `Loaded ${path} (${data.size} bytes).`;
    } catch (e) {
      $('#rules-msg').textContent = 'Failed to read: ' + e.message;
    }
  }

  async function saveFile() {
    const path = $('#rules-file').value;
    if (!path) return;
    const { ok, status, data } = await postJson('/api/rules-draft/write', {
      path, content: $('#rules-text').value, baseModifiedUtc: currentMtime,
    });
    if (ok) {
      currentMtime = data.modified;
      $('#rules-msg').textContent = `Saved draft ${path} (${data.size} bytes).`;
    } else if (status === 409) {
      $('#rules-msg').textContent = 'Conflict: the draft changed underneath. Reload before saving.';
    } else {
      $('#rules-msg').textContent = 'Save failed: ' + (data.error || status);
    }
  }

  async function revertFile() {
    const path = $('#rules-file').value;
    if (!path) return;
    const { ok, data } = await postJson('/api/rules-draft/revert', { path });
    $('#rules-msg').textContent = ok ? `Reverted ${path} from source.` : 'Revert failed: ' + (data.error || '');
    if (ok) await loadFile();
  }

  async function showDiff() {
    try {
      const data = await getJson('/api/rules/diff');
      const changed = data.files.filter((f) => f.status !== 'unchanged');
      if (changed.length === 0) {
        $('#rules-diff-out').innerHTML = '<p class="muted small">No differences between draft and source.</p>';
        return;
      }
      const blocks = changed.map((f) => {
        const diff = f.diff ? `<pre class="terminal">${esc(f.diff)}</pre>` : '<p class="muted small">(no inline diff — git unavailable)</p>';
        return `<div><strong>${esc(f.path)}</strong> <span class="muted small">[${f.status}]</span>${diff}</div>`;
      });
      $('#rules-diff-out').innerHTML = blocks.join('');
    } catch (e) {
      $('#rules-diff-out').textContent = 'Diff failed: ' + e.message;
    }
  }

  async function applyToSource() {
    try {
      const data = await getJson('/api/rules/diff');
      const changed = data.files.filter((f) => f.status === 'modified' || f.status === 'draft-only').map((f) => f.path);
      if (changed.length === 0) { $('#rules-msg').textContent = 'Nothing to apply.'; return; }
      if (!confirm(`Apply ${changed.length} changed file(s) to the source rules/ tree?\n\n${changed.join('\n')}`)) return;
      const res = await postJson('/api/rules/apply', { paths: changed });
      if (!res.ok) { $('#rules-msg').textContent = 'Apply failed: ' + (res.data.error || res.status); return; }
      $('#rules-msg').textContent = `Applied ${res.data.applied.length} file(s) to source. Rebuild or use "Run generate-lists (from source)".`;
    } catch (e) {
      $('#rules-msg').textContent = 'Apply failed: ' + e.message;
    }
  }

  async function runGenerate() {
    // Trigger generation reading the SOURCE rules so applied edits take effect without a rebuild.
    const loc = await getJson('/api/rules/locations').catch(() => null);
    const args = [];
    if (loc && !loc.isBuildOutputFallback) {
      args.push('--config', loc.sourceRulesDir + '/wikipedia-lists.yml', '--rules', loc.sourceRulesDir + '/rules-list.txt');
    }
    const { ok, data } = await postJson('/api/jobs', { command: 'wikipedia generate-lists', args });
    $('#rules-msg').textContent = ok
      ? `Started generate-lists (job ${data.id}). Watch output in the "Run a command" card.`
      : 'Failed to start: ' + (data.error || '');
  }

  // ===================== wire up =====================

  function init() {
    if ($('#grp-load')) {
      loadGroups();
      $('#grp-load').addEventListener('click', loadCounts);
      $('#grp-save').addEventListener('click', saveChildren);
      // Counts table is re-rendered as innerHTML, so delegate its "＋ define" clicks from the container.
      const counts = $('#grp-counts');
      if (counts) counts.addEventListener('click', onCountsClick);
      if ($('#cg-create')) $('#cg-create').addEventListener('click', createGroup);
      if ($('#cg-add-filter')) {
        $('#cg-add-filter').addEventListener('click', () => addFilterRow());
        $('#cg-filters').addEventListener('click', onFiltersClick);
        $('#cg-filters').addEventListener('change', onFiltersChange);
        if (!document.querySelector('#cg-filters .cg-filter-row')) addFilterRow(); // one default row
      }
      // Impact panel is re-rendered as innerHTML, so delegate its button clicks from the container.
      const imp = $('#grp-impact');
      if (imp) imp.addEventListener('click', onImpactClick);
    }
    if ($('#rules-file')) {
      loadLocations();
      loadFileList();
      $('#rules-file').addEventListener('change', loadFile);
      $('#rules-reload').addEventListener('click', loadFile);
      $('#rules-save').addEventListener('click', saveFile);
      $('#rules-revert').addEventListener('click', revertFile);
      $('#rules-diff').addEventListener('click', showDiff);
      $('#rules-apply').addEventListener('click', applyToSource);
      $('#rules-run').addEventListener('click', runGenerate);
    }
  }

  // Public surface: the Wikitext-outputs view deep-links here to combine/split a group's status lists.
  window.BeastieGrouping = { focus: focusGroup };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
