// Minimal Markdown -> HTML renderer for the file viewer.
//
// Handles the subset our reports actually use:
//   - ATX headings (# H1 .. ###### H6)
//   - Fenced code blocks (```lang ... ```)
//   - Inline code (`code`)
//   - Bold (**x**), italic (*x* and _x_)
//   - Unordered (-, *, +) and ordered (1.) lists, with nesting by indent
//   - Tables with header row + separator (| col | col |)
//   - Links [text](url) — only http(s) URLs become real links; everything
//     else is rendered as plain text. Attribute values are HTML-escaped.
//   - Blank-line paragraph separation.
//
// No raw HTML pass-through. Output is built by string concatenation with
// every untrusted value HTML-escaped, so a malicious or malformed report
// can't break out of the viewer.

(function (global) {
  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // Inline pass: code, bold, italic, links. Runs on already-escaped text;
  // we re-encode the inner values of bold/italic to ensure backslash-escapes
  // don't slip through.
  function renderInline(escaped) {
    let s = escaped;

    // Inline code first so its content is treated literally.
    s = s.replace(/`([^`]+)`/g, (_, c) => '<code>' + c + '</code>');

    // Links: [text](url). Only allow http(s) urls; otherwise drop the URL.
    s = s.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_, text, url) => {
      const safe = /^https?:\/\//i.test(url);
      return safe
        ? '<a href="' + url + '" target="_blank" rel="noopener">' + text + '</a>'
        : text;
    });

    // Bold
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/__([^_]+)__/g, '<strong>$1</strong>');

    // Italic — match *...* and _..._ but only at word boundaries to avoid
    // chewing on snake_case identifiers in code blocks (those should be in
    // <code> already; this is defence in depth).
    s = s.replace(/(^|[\s(])\*([^*\n]+)\*(?=[\s).,;:!?]|$)/g, '$1<em>$2</em>');
    s = s.replace(/(^|[\s(])_([^_\n]+)_(?=[\s).,;:!?]|$)/g, '$1<em>$2</em>');

    return s;
  }

  // Block-level pass. Walks lines, accumulating list items, table rows, code
  // fences and paragraphs into closed HTML blocks.
  function renderBlocks(text) {
    const lines = text.replace(/\r\n?/g, '\n').split('\n');
    const out = [];
    let i = 0;

    function flushParagraph(buf) {
      if (buf.length === 0) return;
      out.push('<p>' + renderInline(escapeHtml(buf.join(' '))) + '</p>');
    }

    while (i < lines.length) {
      const line = lines[i];

      // Blank line.
      if (/^\s*$/.test(line)) { i++; continue; }

      // Fenced code block.
      const fence = line.match(/^\s*```(\S*)\s*$/);
      if (fence) {
        const lang = fence[1] || '';
        const code = [];
        i++;
        while (i < lines.length && !/^\s*```\s*$/.test(lines[i])) {
          code.push(lines[i]);
          i++;
        }
        if (i < lines.length) i++; // consume closing fence
        const classAttr = lang ? ' class="lang-' + escapeHtml(lang) + '"' : '';
        out.push('<pre><code' + classAttr + '>' + escapeHtml(code.join('\n')) + '</code></pre>');
        continue;
      }

      // ATX heading.
      const h = line.match(/^(#{1,6})\s+(.+?)\s*#*\s*$/);
      if (h) {
        const level = h[1].length;
        out.push('<h' + level + '>' + renderInline(escapeHtml(h[2])) + '</h' + level + '>');
        i++;
        continue;
      }

      // Horizontal rule.
      if (/^\s*(?:---|\*\*\*|___)\s*$/.test(line)) {
        out.push('<hr>');
        i++;
        continue;
      }

      // Table: header row | --- | --- | followed by body rows.
      if (/^\s*\|.*\|\s*$/.test(line) && i + 1 < lines.length
          && /^\s*\|?\s*:?-+:?\s*(\|\s*:?-+:?\s*)+\|?\s*$/.test(lines[i + 1])) {
        const header = splitCells(line);
        i += 2; // skip header + separator
        const rows = [];
        while (i < lines.length && /^\s*\|.*\|\s*$/.test(lines[i])) {
          rows.push(splitCells(lines[i]));
          i++;
        }
        const thead = '<thead><tr>' + header.map(c => '<th>' + renderInline(escapeHtml(c)) + '</th>').join('') + '</tr></thead>';
        const tbody = '<tbody>' + rows.map(r =>
          '<tr>' + r.map(c => '<td>' + renderInline(escapeHtml(c)) + '</td>').join('') + '</tr>').join('') + '</tbody>';
        out.push('<table>' + thead + tbody + '</table>');
        continue;
      }

      // List (un/ordered). Group adjacent list lines.
      const ulMatch = line.match(/^(\s*)([-*+])\s+(.+)$/);
      const olMatch = line.match(/^(\s*)(\d+)\.\s+(.+)$/);
      if (ulMatch || olMatch) {
        const ordered = !!olMatch;
        const tag = ordered ? 'ol' : 'ul';
        const items = [];
        while (i < lines.length) {
          const um = lines[i].match(/^(\s*)([-*+])\s+(.+)$/);
          const om = lines[i].match(/^(\s*)(\d+)\.\s+(.+)$/);
          if (!um && !om) break;
          if (ordered !== !!om) break;
          items.push(renderInline(escapeHtml((um || om)[3])));
          i++;
        }
        out.push('<' + tag + '>' + items.map(c => '<li>' + c + '</li>').join('') + '</' + tag + '>');
        continue;
      }

      // Paragraph: accumulate consecutive non-blank, non-special lines.
      const buf = [];
      while (i < lines.length && lines[i].trim().length > 0
             && !/^\s*```/.test(lines[i])
             && !/^#{1,6}\s/.test(lines[i])
             && !/^\s*[-*+]\s/.test(lines[i])
             && !/^\s*\d+\.\s/.test(lines[i])
             && !/^\s*\|.*\|\s*$/.test(lines[i])) {
        buf.push(lines[i].trim());
        i++;
      }
      flushParagraph(buf);
    }
    return out.join('\n');
  }

  function splitCells(line) {
    let s = line.trim();
    if (s.startsWith('|')) s = s.slice(1);
    if (s.endsWith('|')) s = s.slice(0, -1);
    return s.split('|').map(c => c.trim());
  }

  // Minimal CSV -> HTML table renderer. Handles RFC-4180-style quoting
  // ("...","field with, comma","line\nbreak","quote ""inside"") well enough
  // for the reports we produce. First row is treated as the header.
  function csvToHtml(text) {
    const rows = parseCsv(text);
    if (rows.length === 0) return '<p class="muted">(empty)</p>';
    const header = rows[0];
    const body = rows.slice(1);
    const thead = '<thead><tr>' + header.map(c => '<th>' + escapeHtml(c) + '</th>').join('') + '</tr></thead>';
    const tbody = '<tbody>' + body.map(r =>
      '<tr>' + r.map(c => '<td>' + escapeHtml(c) + '</td>').join('') + '</tr>').join('') + '</tbody>';
    return '<div class="csv-summary muted small">' + body.length + ' rows · ' + header.length + ' columns</div>' +
           '<table class="csv-table">' + thead + tbody + '</table>';
  }

  function parseCsv(text) {
    // Character-by-character pass — small and correct for quoted fields.
    const rows = [];
    let row = [];
    let field = '';
    let inQuote = false;
    let i = 0;
    while (i < text.length) {
      const c = text[i];
      if (inQuote) {
        if (c === '"') {
          if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
          inQuote = false;
          i++;
          continue;
        }
        field += c;
        i++;
        continue;
      }
      if (c === '"') { inQuote = true; i++; continue; }
      if (c === ',') { row.push(field); field = ''; i++; continue; }
      if (c === '\r') { i++; continue; }
      if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
      field += c;
      i++;
    }
    if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
    // Drop a trailing empty row produced by a final newline.
    if (rows.length > 0 && rows[rows.length - 1].length === 1 && rows[rows.length - 1][0] === '') {
      rows.pop();
    }
    return rows;
  }

  global.MarkdownRenderer = {
    toHtml: renderBlocks,
    csvToHtml: csvToHtml,
    escape: escapeHtml,
  };
})(window);
