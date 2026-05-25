// Minimal ANSI escape -> HTML converter.
//
// Supports the SGR (Select Graphic Rendition) subset Spectre.Console actually
// emits: reset, bold, dim, italic, underline, 8/16-color foreground.
//   - 256-color (38;5;n) and truecolor (38;2;r;g;b) are recognised and emitted
//     as inline styles.
//   - Background colors are recognised but ignored for now (terminal-on-dark
//     palette already provides enough contrast).
//   - Cursor-movement, scroll, and erase escapes are stripped silently so
//     progress-bar redraws don't poison the log.
//
// Handles \r (carriage return) by allowing callers to detect "replace last
// line" cases; the renderer in app.js does the actual line-splice.

(function (global) {
  const ESC = '\x1b';

  // Match either a CSI sequence (\x1b[ ... letter) or an OSC sequence
  // (\x1b] ... BEL or ST). We then dispatch on the trailing letter.
  const ESC_RE = /\x1b(?:\[([0-9;?]*)([A-Za-z])|\]([^\x07\x1b]*)(?:\x07|\x1b\\))/;

  function spanFor(state) {
    const classes = [];
    if (state.bold) classes.push('ansi-bold');
    if (state.dim) classes.push('ansi-dim');
    if (state.italic) classes.push('ansi-italic');
    if (state.underline) classes.push('ansi-underline');
    if (state.fgClass) classes.push(state.fgClass);
    let style = '';
    if (state.fgStyle) style = ' style="color:' + state.fgStyle + '"';
    if (classes.length === 0 && !style) return null;
    return '<span class="' + classes.join(' ') + '"' + style + '>';
  }

  function escapeHtml(s) {
    return s.replace(/[&<>]/g, c => c === '&' ? '&amp;' : c === '<' ? '&lt;' : '&gt;');
  }

  function applySgr(state, params) {
    // No params == reset (\x1b[m)
    if (params.length === 0) params = [0];
    for (let i = 0; i < params.length; i++) {
      const n = params[i];
      if (n === 0) {
        state.bold = state.dim = state.italic = state.underline = false;
        state.fgClass = null;
        state.fgStyle = null;
      } else if (n === 1) state.bold = true;
      else if (n === 2) state.dim = true;
      else if (n === 3) state.italic = true;
      else if (n === 4) state.underline = true;
      else if (n === 22) { state.bold = state.dim = false; }
      else if (n === 23) state.italic = false;
      else if (n === 24) state.underline = false;
      else if ((n >= 30 && n <= 37) || (n >= 90 && n <= 97)) {
        state.fgClass = 'ansi-fg-' + n;
        state.fgStyle = null;
      } else if (n === 38) {
        // Compound: 38;5;N (256-color) or 38;2;R;G;B (truecolor)
        const mode = params[i + 1];
        if (mode === 5 && params.length > i + 2) {
          state.fgClass = null;
          state.fgStyle = ansi256(params[i + 2]);
          i += 2;
        } else if (mode === 2 && params.length > i + 4) {
          state.fgClass = null;
          state.fgStyle = 'rgb(' + params[i + 2] + ',' + params[i + 3] + ',' + params[i + 4] + ')';
          i += 4;
        }
      } else if (n === 39) {
        state.fgClass = null;
        state.fgStyle = null;
      }
      // 40-47 / 100-107 / 48 (background) currently ignored.
    }
  }

  // Standard xterm 256-color palette approximation. The 6x6x6 color cube
  // (16-231) and the greyscale ramp (232-255) are computed; 0-15 reuse the
  // basic 16 colors but we just return a reasonable hex.
  const BASIC_16 = [
    '#000000','#cc0000','#4e9a06','#c4a000','#3465a4','#75507b','#06989a','#d3d7cf',
    '#555753','#ef2929','#8ae234','#fce94f','#729fcf','#ad7fa8','#34e2e2','#eeeeec'
  ];
  function ansi256(n) {
    if (n < 16) return BASIC_16[n];
    if (n >= 232) {
      const v = 8 + (n - 232) * 10;
      return 'rgb(' + v + ',' + v + ',' + v + ')';
    }
    n -= 16;
    const r = Math.floor(n / 36) % 6;
    const g = Math.floor(n / 6) % 6;
    const b = n % 6;
    const cube = v => v === 0 ? 0 : 55 + v * 40;
    return 'rgb(' + cube(r) + ',' + cube(g) + ',' + cube(b) + ')';
  }

  // Convert a string containing ANSI escapes to HTML.
  // - Returns { html, state } so callers can stream chunks with persistent
  //   formatting across boundaries (though for Phase 1 each chunk resets).
  function ansiToHtml(input, prevState) {
    const state = prevState || { bold: false, dim: false, italic: false, underline: false, fgClass: null, fgStyle: null };
    let out = '';
    let spanOpen = false;

    function openSpan() {
      const tag = spanFor(state);
      if (tag) {
        out += tag;
        spanOpen = true;
      }
    }
    function closeSpan() {
      if (spanOpen) {
        out += '</span>';
        spanOpen = false;
      }
    }

    let text = input;
    while (text.length > 0) {
      const idx = text.indexOf(ESC);
      if (idx < 0) {
        // No more escapes — flush rest.
        if (!spanOpen) openSpan();
        out += escapeHtml(text);
        break;
      }
      if (idx > 0) {
        if (!spanOpen) openSpan();
        out += escapeHtml(text.substring(0, idx));
      }
      const rest = text.substring(idx);
      const m = ESC_RE.exec(rest);
      if (!m || m.index !== 0) {
        // Lone ESC with no following sequence — skip the byte.
        text = rest.substring(1);
        continue;
      }
      const csiParams = m[1];
      const csiFinal = m[2];
      // const osc = m[3]; // OSC payloads are dropped.
      if (csiFinal === 'm') {
        closeSpan();
        const params = csiParams ? csiParams.split(';').map(p => parseInt(p, 10) || 0) : [];
        applySgr(state, params);
        openSpan();
      }
      // Strip every other CSI/OSC (cursor moves, erase, scroll, OSC titles).
      text = rest.substring(m[0].length);
    }
    closeSpan();
    return { html: out, state: state };
  }

  global.AnsiRenderer = {
    toHtml: ansiToHtml,
  };
})(window);
