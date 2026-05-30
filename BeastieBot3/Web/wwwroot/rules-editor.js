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
    } catch (e) {
      $('#grp-msg').textContent = 'Failed: ' + e.message;
    }
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
