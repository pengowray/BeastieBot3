// Baseline: the job-dock terminal as it was before terminal.js (app.js at cf34463),
// kept so the benchmark can compare against it. Not loaded by the app.
(function(global){ const jobOutput=document.getElementById('job-output');
  // Accumulated text for the line currently being built (so \r overwrites work).
  let pendingLine = '';

  // The terminal keeps only the last MAX_TERMINAL_LINES committed lines. A long
  // download emits hundreds of thousands of lines, and keeping every one alive
  // as a DOM node killed the browser outright on multi-hour jobs. Older lines
  // are dropped from the top and replaced by a single notice.
  const MAX_TERMINAL_LINES = 5000;
  const TERMINAL_TRIM_SLACK = 500;   // trim in batches rather than on every line
  const MAX_PENDING_CHARS = 16384;   // force-commit output that never sends \n
  let committedLines = 0;

  function resetTerminal() {
    jobOutput.innerHTML = '';
    pendingLine = '';
    committedLines = 0;
  }

  function showTrimNotice() {
    if (jobOutput.querySelector('span[data-trimmed="1"]')) return;
    const el = document.createElement('span');
    el.dataset.trimmed = '1';
    el.className = 'ansi-dim';
    el.textContent = '[earlier output trimmed]\n';
    jobOutput.insertBefore(el, jobOutput.firstChild);
  }

  // Each committed line is a <span> followed by a "\n" text node; drop whole
  // pairs from the top. The in-progress preview span is always last, so it is
  // never at risk here.
  function trimTerminal() {
    if (committedLines <= MAX_TERMINAL_LINES + TERMINAL_TRIM_SLACK) return;
    let toRemove = committedLines - MAX_TERMINAL_LINES;
    while (toRemove > 0) {
      let node = jobOutput.firstChild;
      if (node && node.nodeType === 1 && node.dataset && node.dataset.trimmed === '1') {
        node = node.nextSibling;
      }
      if (!node) break;
      const newline = node.nextSibling;
      jobOutput.removeChild(node);
      if (newline && newline.nodeType === 3) jobOutput.removeChild(newline);
      committedLines--;
      toRemove--;
    }
    showTrimNotice();
  }

  // Reattaching to a job replays its whole history as one chunk. Rendering 200k
  // lines and *then* trimming still hangs the browser, so cut the chunk first.
  function clampChunk(text) {
    let lines = 0;
    for (let i = 0; i < text.length; i++) {
      if (text.charCodeAt(i) === 10) lines++;
    }
    if (lines <= MAX_TERMINAL_LINES) return text;
    let pos = -1;
    for (let i = lines - MAX_TERMINAL_LINES; i > 0; i--) pos = text.indexOf('\n', pos + 1);
    showTrimNotice();
    return text.substring(pos + 1);
  }

  function commitLine(html) {
    const span = document.createElement('span');
    span.innerHTML = html;
    jobOutput.appendChild(span);
    jobOutput.appendChild(document.createTextNode('\n'));
    committedLines++;
  }

  function dropPreview() {
    const preview = jobOutput.querySelector('span[data-preview="1"]');
    if (preview) jobOutput.removeChild(preview);
  }

  function appendChunk(chunk) {
    // Drop \x1b]2;...title... OSC sequences before splitting, they would
    // otherwise leave artefacts the SGR parser doesn't strip.
    // (Already handled by ansi.js but a defensive guard is cheap.)
    let text = clampChunk(chunk);

    // Follow the tail only when the user is already there, so scrolling back
    // through a running job's log isn't yanked to the bottom by every chunk.
    const pinned = jobOutput.scrollHeight - jobOutput.scrollTop - jobOutput.clientHeight < 40;

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
        // pendingLine holds everything that survived the last \r, so the preview
        // span is superseded: drop it, or it lingers above the committed line as
        // a duplicate.
        dropPreview();
        commitLine(AnsiRenderer.toHtml(pendingLine).html);
        pendingLine = '';
      } else if (kind === 'cr') {
        // Replace the in-progress line (overwrite preview span).
        renderPreview();
        pendingLine = '';
      } else if (pendingLine.length > MAX_PENDING_CHARS) {
        // A line that never ends re-renders the whole preview on every chunk,
        // which is quadratic. Cut it loose once it gets long.
        dropPreview();
        commitLine(AnsiRenderer.toHtml(pendingLine).html);
        pendingLine = '';
      } else {
        // append mode: update preview only.
        renderPreview();
      }
    }
    trimTerminal();
    if (pinned) jobOutput.scrollTop = jobOutput.scrollHeight;
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

global.oldTerminal={reset:resetTerminal,append:appendChunk,flush:()=>{},finish:commitPreviewIfAny,lineCount:()=>committedLines};})(window);
