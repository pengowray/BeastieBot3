// Job-dock terminal: turns a stream of ANSI text into a bounded, scrollable log.
//
// The stream from a multi-hour job is large (hundreds of thousands of lines)
// and fast (Spectre writes single characters; the server coalesces them into
// events up to 64K). Three things keep the browser responsive regardless of
// either:
//
//   - Incoming chunks are queued and rendered at most every FLUSH_MS, so the
//     layout cost is paid a few times a second, not once per event. If the
//     queue outgrows MAX_QUEUED_CHARS (a hidden tab whose timers are throttled)
//     it is cut back to its tail before anything is rendered.
//   - Every line is its own block element. In a single <pre> the whole log was
//     one inline formatting context, and each append re-laid-out every line
//     already on screen.
//   - Only the last MAX_LINES lines are kept; a replay chunk is clamped before
//     it is rendered, and the queue is clamped before it is joined.
//
// \r replaces the line being built (a progress redraw); \n commits it. A \r
// that ends one chunk waits for the next before deciding whether it was \r\n.

(function (global) {
  const MAX_LINES = 5000;           // committed lines kept in the DOM
  const TRIM_SLACK = 500;           // trim in batches, not on every line
  const MAX_PENDING_CHARS = 16384;  // force-commit a line that never ends
  const MAX_QUEUED_CHARS = 1 << 20; // unrendered input kept while waiting to flush
  const FLUSH_MS = 100;

  function createTerminal(root) {
    let pendingLine = '';
    let committed = 0;
    let previewEl = null;   // last child while a line is still being built
    let trimEl = null;      // the "[earlier output trimmed]" notice, if shown
    let crPending = false;  // last chunk ended in \r
    let queue = [];
    let queued = 0;
    let timer = null;
    let inputTrimmed = false;

    function reset() {
      if (timer !== null) { clearTimeout(timer); timer = null; }
      root.textContent = '';
      pendingLine = '';
      committed = 0;
      previewEl = null;
      trimEl = null;
      crPending = false;
      queue = [];
      queued = 0;
      inputTrimmed = false;
    }

    function showTrimNotice() {
      if (trimEl) return;
      trimEl = document.createElement('div');
      trimEl.className = 'tline ansi-dim';
      trimEl.textContent = '[earlier output trimmed]';
      root.insertBefore(trimEl, root.firstChild);
    }

    // Keep the last MAX_LINES lines and at most MAX_QUEUED_CHARS characters,
    // cutting on a line boundary. Walks back from the end, so the cost is the
    // part kept, not the part thrown away.
    function clamp(text) {
      let cut = 0;
      if (text.length > MAX_QUEUED_CHARS) {
        cut = text.indexOf('\n', text.length - MAX_QUEUED_CHARS) + 1;
      }
      let pos = text.length;
      for (let lines = 0; lines <= MAX_LINES; lines++) {
        // lastIndexOf treats a negative start as 0, which would re-find a
        // newline at index 0 forever.
        if (pos <= 0) { pos = -1; break; }
        pos = text.lastIndexOf('\n', pos - 1);
        if (pos < cut) { pos = -1; break; }
      }
      if (pos >= 0) cut = pos + 1;
      if (cut === 0) return text;
      inputTrimmed = true;
      return text.substring(cut);
    }

    function commit() {
      const html = AnsiRenderer.toHtml(pendingLine).html;
      if (previewEl) {
        previewEl.innerHTML = html;
        previewEl = null;
      } else {
        const el = document.createElement('div');
        el.className = 'tline';
        el.innerHTML = html;
        root.appendChild(el);
      }
      committed++;
      pendingLine = '';
    }

    function renderPreview() {
      if (!previewEl) {
        previewEl = document.createElement('div');
        previewEl.className = 'tline';
        root.appendChild(previewEl);
      }
      previewEl.innerHTML = AnsiRenderer.toHtml(pendingLine).html;
    }

    function trim() {
      if (committed <= MAX_LINES + TRIM_SLACK) return;
      showTrimNotice();
      let n = committed - MAX_LINES;
      while (n-- > 0) {
        const node = trimEl.nextSibling;
        if (!node || node === previewEl) break;
        root.removeChild(node);
        committed--;
      }
    }

    function render(text) {
      const pinned = root.scrollHeight - root.scrollTop - root.clientHeight < 40;
      if (inputTrimmed) { showTrimNotice(); inputTrimmed = false; }
      const n = text.length;
      let i = 0;
      if (crPending) {
        crPending = false;
        if (n > 0 && text.charCodeAt(0) === 10) {
          commit();
          i = 1;
        } else {
          renderPreview();
          pendingLine = '';
        }
      }
      while (i < n) {
        let j = i;
        while (j < n) {
          const c = text.charCodeAt(j);
          if (c === 10 || c === 13) break;
          j++;
        }
        if (j > i) pendingLine += text.substring(i, j);
        if (j === n) {
          if (pendingLine.length > MAX_PENDING_CHARS) commit();
          else renderPreview();
          break;
        }
        if (text.charCodeAt(j) === 10) {
          commit();
          i = j + 1;
        } else if (j + 1 === n) {
          crPending = true;
          i = n;
        } else if (text.charCodeAt(j + 1) === 10) {
          commit();
          i = j + 2;
        } else {
          renderPreview();
          pendingLine = '';
          i = j + 1;
        }
      }
      trim();
      if (pinned) root.scrollTop = root.scrollHeight;
    }

    function flush() {
      if (timer !== null) { clearTimeout(timer); timer = null; }
      if (queue.length === 0) return;
      const text = clamp(queue.join(''));
      queue = [];
      queued = 0;
      render(text);
    }

    function append(chunk) {
      if (!chunk) return;
      queue.push(chunk);
      queued += chunk.length;
      if (queued > MAX_QUEUED_CHARS) {
        const text = clamp(queue.join(''));
        queue = [text];
        queued = text.length;
      }
      if (timer === null) timer = setTimeout(flush, FLUSH_MS);
    }

    // The stream has ended: render what is queued and settle the last line.
    function finish() {
      flush();
      if (previewEl || pendingLine) commit();
    }

    function setText(text) {
      reset();
      append(text);
      flush();
    }

    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') flush();
      });
    }

    return { reset, append, flush, finish, setText, lineCount: () => committed };
  }

  global.createTerminal = createTerminal;
})(window);
