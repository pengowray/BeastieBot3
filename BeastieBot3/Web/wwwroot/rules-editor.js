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
    const split = $('#imp-knob-split') ? $('#imp-knob-split').value : '';
    if (split) body.categorySplit = split;
    if (body.sizeBudgetMaxEntries == null && !body.categorySplit) {
      setImpactMsg('Nothing to save — set a budget and/or a category split first.');
      return;
    }
    const { ok, data } = await postJson('/api/grouping/knobs', body);
    setImpactMsg(ok
      ? `Saved to draft: ${(data.changed || []).join(', ') || 'no change'}. Review in the Rules editor (Diff), then Apply to source.`
      : 'Save failed: ' + (data.error || '') + (data.hint ? ' — ' + data.hint : ''));
  }

  function onImpactClick(ev) {
    const gen = ev.target.closest('[data-gen-list]');
    if (gen) { generateList(gen.getAttribute('data-gen-list')); return; }
    if (ev.target.closest('[data-apply-budget]')) {
      const v = $('#imp-budget') ? $('#imp-budget').value.trim() : '';
      loadImpact($('#grp-parent').value, $('#grp-rank').value, v ? parseInt(v, 10) : null);
      return;
    }
    if (ev.target.closest('[data-save-knobs]')) { saveKnobs(); }
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
    const knobs = `<div class="imp-controls">`
      + `<label>Set budget <input id="imp-knob-budget" type="number" min="0" step="500" placeholder="${d.budget || 'max_entries'}"></label>`
      + `<label>Category split <select id="imp-knob-split">`
      + `<option value="">(keep current)</option>`
      + `<option value="default">default (per-status pages)</option>`
      + `<option value="separate">separate</option>`
      + `<option value="combined-threatened">combined-threatened</option>`
      + `<option value="all-status">all-status</option></select></label>`
      + `<button class="ghost xsmall" data-save-knobs>Save knobs to draft</button></div>`;

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
      const cells = r.counts.map((c) => `<td>${c}</td>`).join('');
      return `<tr><td>${cb}</td><td>${esc(r.key)}</td><td>${esc(r.existingGroup || '')}</td>${cells}<td><strong>${r.total}</strong></td></tr>`;
    });
    $('#grp-counts').innerHTML =
      `<table><thead><tr>${head.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.join('')}</tbody></table>`;
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
