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
table.audit-table td.longtext { max-width: 460px; }
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

footer.site { border-top: 1px solid var(--line); background: var(--bg); color: var(--ink-soft); font-size: 0.88rem; }
footer.site .wrap { padding: 22px 20px 40px; }
footer.site a { color: var(--accent); }

@media (max-width: 640px) {
  header.site h1 { font-size: 1.25rem; }
  section { padding: 16px; border-radius: 8px; }
}
@media print {
  body { background: #fff; }
  .table-controls, .group-nav, nav.crumbs { display: none; }
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
})();
""";
}
