// The CSS and JS for the static audit bundle, embedded so the generator emits a self-contained
// site (no build step, no external assets). audit.css is a clean light theme suited to a formal
// document; audit.js gives every table click-to-sort and a filter box with no dependencies.

namespace BeastieBot3.Audit.Rendering;

internal static class AuditAssets {
    public const string Css = """
:root {
  --ink: #1d2125;
  --ink-soft: #4a5560;
  --line: #d9dee3;
  --line-soft: #eceff2;
  --bg: #ffffff;
  --bg-soft: #f6f8fa;
  --accent: #2a6f97;
  --accent-soft: #e7f0f5;
  --breaking: #b54034;
  --fixable: #b5862a;
  --advisory: #3a7a4a;
  --max: 1180px;
  --max-wide: 1680px;
}
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0;
  font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  color: var(--ink);
  background: var(--bg-soft);
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
code { font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; font-size: 0.88em; }

.wrap { max-width: var(--max); margin: 0 auto; padding: 0 20px; }
/* Full-list pages opt into the wider measure so dense tables can use the page width. */
body.wide .wrap { max-width: var(--max-wide); }
header.site { background: var(--bg); border-bottom: 1px solid var(--line); }
header.site .wrap { padding-top: 22px; padding-bottom: 18px; }
header.site h1 { margin: 0 0 4px; font-size: 1.5rem; letter-spacing: -0.01em; }
header.site .release { color: var(--ink-soft); font-size: 0.95rem; }

nav.crumbs { font-size: 0.9rem; color: var(--ink-soft); margin: 14px 0 0; }
nav.crumbs a { color: var(--ink-soft); }

.disclaimer {
  background: #fff8ec;
  border: 1px solid #f0dca8;
  border-radius: 8px;
  padding: 12px 16px;
  margin: 18px 0;
  font-size: 0.92rem;
  color: #5b4a25;
}
.disclaimer strong { color: #4a3a18; }

main { padding: 8px 0 56px; }
section { background: var(--bg); border: 1px solid var(--line); border-radius: 10px; padding: 20px 22px; margin: 18px 0; }
section > h2 { margin-top: 0; font-size: 1.2rem; }
section p:first-child { margin-top: 0; }
h2, h3, h4 { line-height: 1.3; }
.lede { font-size: 1.02rem; color: var(--ink-soft); }

.meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px 24px; font-size: 0.92rem; }
.meta-grid dt { color: var(--ink-soft); }
.meta-grid dd { margin: 0 0 8px; }

/* Report index cards */
.report-grid { display: grid; grid-template-columns: 1fr; gap: 0; }
table.index { width: 100%; border-collapse: collapse; }
table.index th, table.index td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line-soft); vertical-align: top; }
table.index th { color: var(--ink-soft); font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.04em; }
table.index td.count, table.index th.count { text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
table.index .report-title { font-weight: 600; }
table.index .report-desc { color: var(--ink-soft); font-size: 0.9rem; margin-top: 2px; }
table.index .links { white-space: nowrap; font-size: 0.88rem; }

.badge { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 0.74rem; font-weight: 600; letter-spacing: 0.02em; vertical-align: middle; }
.badge.breaking { background: #f7e4e1; color: var(--breaking); }
.badge.fixable { background: #f6ecd6; color: var(--fixable); }
.badge.advisory { background: #e2f0e6; color: var(--advisory); }
.badge.tier { background: var(--accent-soft); color: var(--accent); }

.status-badge { display: inline-block; min-width: 2.6em; text-align: center; padding: 1px 7px; border-radius: 5px; font-size: 0.78rem; font-weight: 600; }

/* Data tables */
.table-controls { display: flex; align-items: center; gap: 14px; margin: 4px 0 10px; flex-wrap: wrap; }
.table-filter { flex: 1 1 240px; max-width: 360px; padding: 7px 10px; border: 1px solid var(--line); border-radius: 7px; font-size: 0.92rem; }
.row-count { color: var(--ink-soft); font-size: 0.88rem; font-variant-numeric: tabular-nums; }

.table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
table.audit-table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
table.audit-table th, table.audit-table td { padding: 7px 11px; text-align: left; border-bottom: 1px solid var(--line-soft); vertical-align: top; }
table.audit-table thead th { background: var(--bg-soft); position: sticky; top: 0; z-index: 1; color: var(--ink-soft); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; white-space: nowrap; }
table.audit-table.sortable thead th { cursor: pointer; user-select: none; }
table.audit-table.sortable thead th:hover { color: var(--ink); }
table.audit-table.sortable thead th[aria-sort="ascending"]::after { content: " ▲"; font-size: 0.7em; }
table.audit-table.sortable thead th[aria-sort="descending"]::after { content: " ▼"; font-size: 0.7em; }
table.audit-table td.num, table.audit-table th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
table.audit-table tbody tr:hover { background: var(--accent-soft); }
/* Narrative cells stay on one line (ellipsis + full text on hover) so a long detail never
   stretches its row and leaves giant gaps beside the short rows around it. */
table.audit-table td.longtext { max-width: 620px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
table.audit-table td.ws-cell { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.82rem; white-space: pre-wrap; word-break: break-word; }
.ws { color: #b9c1c9; }
.ws-bad { color: var(--breaking); font-weight: 700; }
.ws-empty { color: var(--ink-soft); font-style: italic; }

table.summary { border-collapse: collapse; width: 100%; font-size: 0.9rem; margin: 6px 0 4px; }
table.summary th, table.summary td { padding: 6px 11px; border-bottom: 1px solid var(--line-soft); text-align: left; }
table.summary td.num, table.summary th.num { text-align: right; font-variant-numeric: tabular-nums; }
table.summary thead th { color: var(--ink-soft); font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.03em; }

.commentary { background: var(--accent-soft); border-left: 3px solid var(--accent); border-radius: 0 8px 8px 0; padding: 12px 16px; margin: 16px 0; }
.commentary h3 { margin-top: 0; font-size: 0.95rem; color: var(--accent); }
.commentary p:last-child { margin-bottom: 0; }

.note-list { font-size: 0.92rem; color: var(--ink-soft); }
.preview-foot { margin: 12px 0 0; font-size: 0.92rem; }
.preview-foot a { font-weight: 600; }
blockquote { margin: 10px 0; padding: 2px 14px; border-left: 3px solid var(--line); color: var(--ink-soft); }

.group-nav { display: flex; flex-wrap: wrap; gap: 6px 8px; margin: 10px 0 4px; font-size: 0.9rem; }
.group-nav a { background: var(--bg-soft); border: 1px solid var(--line); border-radius: 6px; padding: 3px 9px; }

/* Modal viewer (HTML vs plain-text comparison) */
.view-cell { font: inherit; font-size: 0.82rem; padding: 2px 11px; border: 1px solid var(--line); border-radius: 6px; background: var(--bg-soft); color: var(--accent); cursor: pointer; white-space: nowrap; }
.view-cell:hover { background: var(--accent-soft); border-color: var(--accent); }
.audit-modal { position: fixed; inset: 0; z-index: 60; display: flex; align-items: center; justify-content: center; padding: 22px; }
.audit-modal[hidden] { display: none; }
.audit-modal-backdrop { position: absolute; inset: 0; background: rgba(20, 25, 30, 0.55); }
.audit-modal-card { position: relative; background: var(--bg); border-radius: 10px; width: 100%; max-width: 1040px; max-height: 88vh; overflow: auto; padding: 20px 24px 26px; box-shadow: 0 12px 44px rgba(0, 0, 0, 0.32); }
.audit-modal-x { position: absolute; top: 8px; right: 12px; border: none; background: none; font-size: 1.6rem; line-height: 1; color: var(--ink-soft); cursor: pointer; }
.audit-modal-x:hover { color: var(--ink); }
.audit-modal-title { margin: 0 30px 4px 0; font-size: 1.05rem; }
.audit-modal-title em { font-style: italic; }
.audit-modal-meta { margin: 0 0 14px; color: var(--ink-soft); font-size: 0.88rem; }
.audit-modal-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.audit-modal h4 { margin: 12px 0 6px; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; color: var(--ink-soft); }
.audit-pane { background: var(--bg-soft); border: 1px solid var(--line); border-radius: 7px; padding: 10px 12px; margin: 0; max-height: 40vh; overflow: auto; white-space: pre-wrap; word-break: break-word; font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; font-size: 0.8rem; line-height: 1.5; }
.audit-pane.html { max-height: 46vh; }
.audit-pane mark { background: #fde7c2; color: #5b4a25; border-radius: 2px; }
.audit-pane .tok-tag { color: var(--accent); }
.audit-pane .audit-empty { color: var(--ink-soft); font-style: italic; }
@media (max-width: 720px) { .audit-modal-grid { grid-template-columns: 1fr; } }

footer.site { border-top: 1px solid var(--line); background: var(--bg); color: var(--ink-soft); font-size: 0.88rem; }
footer.site .wrap { padding: 22px 20px 40px; }
footer.site a { color: var(--accent); }

@media (max-width: 640px) {
  header.site h1 { font-size: 1.25rem; }
  section { padding: 16px; border-radius: 8px; }
}
@media print {
  body { background: #fff; }
  .table-controls, .group-nav, nav.crumbs, .view-cell, .audit-modal { display: none; }
  section { border: none; padding: 0; }
  .table-wrap { overflow: visible; }
}
""";

    public const string Js = """
(function () {
  "use strict";

  function cmpFactory(colIndex, numeric, dir) {
    var sign = dir === "descending" ? -1 : 1;
    return function (a, b) {
      var ca = a.children[colIndex], cb = b.children[colIndex];
      var va = ca ? (ca.getAttribute("data-sort") || ca.textContent) : "";
      var vb = cb ? (cb.getAttribute("data-sort") || cb.textContent) : "";
      if (numeric) {
        var na = parseFloat(va), nb = parseFloat(vb);
        if (isNaN(na)) na = -Infinity;
        if (isNaN(nb)) nb = -Infinity;
        return (na - nb) * sign;
      }
      return va.localeCompare(vb) * sign;
    };
  }

  function sortBy(table, th) {
    var colIndex = parseInt(th.getAttribute("data-col"), 10);
    var numeric = th.getAttribute("data-numeric") === "true";
    var current = th.getAttribute("aria-sort");
    var dir = current === "ascending" ? "descending" : "ascending";
    table.querySelectorAll("thead th").forEach(function (h) { h.removeAttribute("aria-sort"); });
    th.setAttribute("aria-sort", dir);
    var tbody = table.tBodies[0];
    var rows = Array.prototype.slice.call(tbody.rows);
    rows.sort(cmpFactory(colIndex, numeric, dir));
    var frag = document.createDocumentFragment();
    rows.forEach(function (r) { frag.appendChild(r); });
    tbody.appendChild(frag);
  }

  document.querySelectorAll("table.audit-table.sortable thead th").forEach(function (th) {
    th.addEventListener("click", function () { sortBy(th.closest("table"), th); });
    th.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " " || e.key === "Spacebar") {
        e.preventDefault();
        sortBy(th.closest("table"), th);
      }
    });
  });

  document.querySelectorAll(".table-filter").forEach(function (input) {
    var id = input.getAttribute("data-table");
    var table = document.getElementById(id);
    if (!table) return;
    var counter = document.querySelector('.row-count[data-table="' + id + '"]');
    input.addEventListener("input", function () {
      var q = input.value.trim().toLowerCase();
      var shown = 0;
      var rows = table.tBodies[0].rows;
      for (var i = 0; i < rows.length; i++) {
        var match = q === "" || rows[i].textContent.toLowerCase().indexOf(q) !== -1;
        rows[i].style.display = match ? "" : "none";
        if (match) shown++;
      }
      if (counter) counter.textContent = shown.toLocaleString() + " rows";
    });
  });

  // Modal viewer: compares a narrative field's plain-text export with the text from its HTML,
  // and shows the raw HTML source colour-coded so empty-tag runs are obvious. Built lazily.
  function escapeHtml(s) {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
  function commonPrefixLen(a, b) {
    var n = Math.min(a.length, b.length), i = 0;
    while (i < n && a[i] === b[i]) i++;
    return i;
  }
  function splitHighlight(text, cut) {
    if (!text) return '<span class="audit-empty">(empty)</span>';
    var shared = escapeHtml(text.slice(0, cut));
    var rest = escapeHtml(text.slice(cut));
    return shared + (rest ? '<mark>' + rest + '</mark>' : '');
  }
  function highlightHtml(src) {
    if (!src) return '<span class="audit-empty">(empty)</span>';
    // Escape first, then wrap each tag (lazily, up to the first &gt;) so runs of empty markup show
    // up as coloured tokens. The lookahead lets attribute values that contain entities still match.
    return escapeHtml(src).replace(/&lt;(\/?[a-zA-Z](?:(?!&gt;)[\s\S])*?)&gt;/g, '<span class="tok-tag">&lt;$1&gt;</span>');
  }

  var modal = null;
  function ensureModal() {
    if (modal) return modal;
    modal = document.createElement("div");
    modal.className = "audit-modal";
    modal.setAttribute("hidden", "");
    modal.innerHTML =
      '<div class="audit-modal-backdrop" data-close></div>' +
      '<div class="audit-modal-card" role="dialog" aria-modal="true" aria-label="Field comparison">' +
        '<button type="button" class="audit-modal-x" data-close aria-label="Close">×</button>' +
        '<h3 class="audit-modal-title"></h3>' +
        '<p class="audit-modal-meta"></p>' +
        '<div class="audit-modal-grid">' +
          '<section><h4>Plain-text field (normalised)</h4><pre class="audit-pane plain"></pre></section>' +
          '<section><h4>Text from HTML (normalised)</h4><pre class="audit-pane readable"></pre></section>' +
        '</div>' +
        '<section><h4>HTML source</h4><pre class="audit-pane html"></pre></section>' +
      '</div>';
    document.body.appendChild(modal);
    modal.querySelectorAll("[data-close]").forEach(function (el) {
      el.addEventListener("click", closeModal);
    });
    return modal;
  }
  function closeModal() {
    if (modal) modal.setAttribute("hidden", "");
  }
  function openModal(btn) {
    var m = ensureModal();
    var d = btn.dataset;
    var plain = d.viewPlain || "", readable = d.viewReadable || "";
    var cut = commonPrefixLen(plain, readable);
    var name = d.viewName || "(unnamed)";
    m.querySelector(".audit-modal-title").innerHTML =
      '<em>' + escapeHtml(name) + '</em>' + (d.viewField ? ' · ' + escapeHtml(d.viewField) : '');
    var meta = [];
    if (d.viewIssue) meta.push(escapeHtml(d.viewIssue));
    if (d.viewHtmllen) meta.push("HTML " + escapeHtml(d.viewHtmllen) + " chars");
    if (d.viewRatio) meta.push(escapeHtml(d.viewRatio) + "× readable size");
    m.querySelector(".audit-modal-meta").innerHTML = meta.join(" · ");
    m.querySelector(".audit-pane.plain").innerHTML = splitHighlight(plain, cut);
    m.querySelector(".audit-pane.readable").innerHTML = splitHighlight(readable, cut);
    m.querySelector(".audit-pane.html").innerHTML = highlightHtml(d.viewHtml || "");
    m.removeAttribute("hidden");
  }

  document.querySelectorAll(".view-cell").forEach(function (btn) {
    btn.addEventListener("click", function () { openModal(btn); });
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") closeModal();
  });
})();
""";
}
