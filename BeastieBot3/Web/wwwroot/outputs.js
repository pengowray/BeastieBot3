// Wikitext outputs view: a table of generated *.wikitext lists, with an on-demand Wikipedia
// preview (server proxies the file through the MediaWiki action=parse API) and a raw-source view.
// Depends on window.Beastie (formatBytes / formatRelative / openFile) published by app.js.

(function () {
  const $ = (sel) => document.querySelector(sel);
  let loaded = false;
  let allFiles = [];

  async function load() {
    const tbody = $('#wt-tbody');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="4" class="muted">Loading&hellip;</td></tr>';
    try {
      const data = await fetch('/api/wikitext/list').then((r) => r.json());
      allFiles = data.files || [];
      const count = $('#wt-count');
      if (count) count.textContent = allFiles.length + ' files';
      render();
      loaded = true;
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="4" class="error">' + escapeHtml(e.message) + '</td></tr>';
    }
  }

  function render() {
    const tbody = $('#wt-tbody');
    if (!tbody) return;
    const B = window.Beastie || {};
    const q = (($('#wt-search') || {}).value || '').toLowerCase();
    const rows = allFiles.filter(
      (f) => !q || f.title.toLowerCase().includes(q) || f.name.toLowerCase().includes(q));

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="4" class="muted">No matching outputs.</td></tr>';
      return;
    }
    tbody.innerHTML = '';
    for (const f of rows) {
      const tr = document.createElement('tr');

      const title = document.createElement('td');
      title.textContent = f.title;

      const size = document.createElement('td');
      size.textContent = B.formatBytes ? B.formatBytes(f.size) : f.size + ' B';

      const mod = document.createElement('td');
      mod.className = 'muted small';
      mod.textContent = B.formatRelative ? B.formatRelative(f.modified) : '';

      const act = document.createElement('td');
      act.className = 'wt-actions';
      const prev = button('Preview', 'primary', () => preview(f));
      const src = button('Source', '', () => { if (B.openFile) B.openFile('wikipedia-output', f.name); });
      act.appendChild(prev);
      act.appendChild(src);

      tr.appendChild(title);
      tr.appendChild(size);
      tr.appendChild(mod);
      tr.appendChild(act);
      tbody.appendChild(tr);
    }
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

  // Router calls this when the view is shown; load once, then on explicit refresh.
  window.BeastieOutputs = {
    load: () => { if (!loaded) load(); },
    reload: load,
  };
})();
