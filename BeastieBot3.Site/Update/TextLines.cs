namespace BeastieBot3.Site.Update;

/// The lines of a text, 1-based, with their ends before "\r\n" or "\n".
internal sealed class TextLines {
    private readonly string _text;
    private readonly List<int> _starts = [0];

    public TextLines(string text) {
        _text = text;
        for (var i = 0; i < text.Length; i++) {
            if (text[i] == '\n') {
                _starts.Add(i + 1);
            }
        }
    }

    public int Start(int line) => _starts[Math.Clamp(line - 1, 0, _starts.Count - 1)];

    public int End(int line) {
        var next = line < _starts.Count ? _starts[line] - 1 : _text.Length;
        return next > 0 && next <= _text.Length && next - 1 >= Start(line) && _text[next - 1] == '\r' ? next - 1 : next;
    }

    public string Text(int line) => _text[Start(line)..End(line)];

    public int Count => _starts.Count;

    // The last of a line and the lines after it that belong to it: lines with more bullet
    // markers (its subspecies) or starting with ":" (a note under it). A blank line, a line with
    // as many markers or fewer, or the end of a template ends it.
    public int LastOfBlock(int line) {
        var first = Text(line);
        var start = ListPlacement.ListStart(first);
        var depth = ListPlacement.Markers(start < 0 ? first : first[start..]);
        var last = line;
        for (var next = line + 1; next <= _starts.Count; next++) {
            var t = Text(next);
            if (t.Length == 0 || t.StartsWith("}}", StringComparison.Ordinal)) {
                break;
            }
            if (ListPlacement.Markers(t) > depth || (t[0] == ':' && depth > 0)) {
                last = next;
                continue;
            }
            break;
        }
        return last;
    }
}
