using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// A span of the text: Start inclusive, End exclusive.
public readonly record struct TextSpan(int Start, int End) {
    public int Length => End - Start;
    public bool Contains(int position) => position >= Start && position < End;
}

/// One parameter of a template: the text between its "|" and the next "|" or the closing "}}".
/// Name is the trimmed name of a named parameter ("year"), null for a positional one. Value is the
/// part after the "=" (or the whole text of a positional parameter), untrimmed.
public sealed record TemplateParameter(int PipePosition, TextSpan Whole, string? Name, TextSpan Value);

/// One template: Span covers "{{" to "}}". Name is normalised (NormalizeName).
public sealed record WikiTemplate(TextSpan Span, string Name, IReadOnlyList<TemplateParameter> Parameters) {
    /// The named parameter, matched after NormalizeParameterName; the first one when repeated.
    public TemplateParameter? Named(string name) =>
        Parameters.FirstOrDefault(p => p.Name is not null && WikitextScanner.NormalizeParameterName(p.Name) == name);

    /// The positional parameter n (1-based), counting unnamed parameters, or the parameter named
    /// "n=" when there is one.
    public TemplateParameter? Positional(int n) {
        var named = Named(n.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (named is not null) {
            return named;
        }
        var i = 0;
        foreach (var p in Parameters) {
            if (p.Name is null && ++i == n) {
                return p;
            }
        }
        return null;
    }
}

/// Wikitext read with brace counting rather than one large regular expression. Comments, nowiki,
/// pre, syntaxhighlight, source and math elements are masked first (their characters become spaces,
/// newlines kept), so nothing inside them is found or changed. Every position refers to the
/// original text, which has the same length as the masked one.
public sealed partial class WikitextScanner {
    public string Text { get; }

    /// The text with the masked elements replaced by spaces.
    public string Masked { get; }

    /// Every template, outer and nested, in order of their start.
    public IReadOnlyList<WikiTemplate> Templates { get; }

    private readonly List<TextSpan> _outerTemplates;
    private readonly int[] _lineStarts;

    public WikitextScanner(string text) {
        Text = text;
        Masked = Mask(text);
        var (templates, outer) = FindTemplates(Masked);
        Templates = templates;
        _outerTemplates = outer;
        _lineStarts = LineStarts(text);
    }

    /// 1-based line number of a position.
    public int LineOf(int position) {
        var i = Array.BinarySearch(_lineStarts, position);
        return (i >= 0 ? i : ~i - 1) + 1;
    }

    private List<TextSpan>? _refs;

    /// The outermost template or <ref>...</ref> element that the position is inside (after its first
    /// character and before its last), leaving out the templates named in transparent (list layout
    /// templates, inside which list lines are written); null when there is none. Text put at the
    /// position would land inside it.
    public TextSpan? ContainerAt(int position, IReadOnlySet<string>? transparent = null) {
        _refs ??= [.. RefElement().Matches(Masked).Select(m => new TextSpan(m.Index, m.Index + m.Length))];
        TextSpan? found = null;
        foreach (var r in _refs) {
            if (r.Start >= position) {
                break;
            }
            if (position < r.End) {
                found = r;
                break;
            }
        }
        if (OuterTemplateAt(position) is { } outer) {
            var template = transparent is not null && transparent.Contains(outer.Name)
                ? TemplatesWithin(outer.Span).Where(t => t != outer && t.Span.Start < position && position < t.Span.End && !transparent.Contains(t.Name))
                    .MinBy(t => t.Span.Start)
                : outer;
            if (template is not null && (found is null || template.Span.Start < found.Value.Start)) {
                found = template.Span;
            }
        }
        return found;
    }

    [GeneratedRegex(@"<ref\b[^>]*?(?<!/)>.*?</ref\s*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex RefElement();

    /// The position where a 1-based line starts.
    public int LineStart(int line) => _lineStarts[Math.Clamp(line - 1, 0, _lineStarts.Length - 1)];

    /// The templates that lie wholly inside the span, outer and nested, in order of their start.
    public IEnumerable<WikiTemplate> TemplatesWithin(TextSpan span) {
        var lo = 0;
        var hi = Templates.Count;
        while (lo < hi) {
            var mid = (lo + hi) / 2;
            if (Templates[mid].Span.Start < span.Start) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        for (var i = lo; i < Templates.Count && Templates[i].Span.Start < span.End; i++) {
            if (Templates[i].Span.End <= span.End) {
                yield return Templates[i];
            }
        }
    }

    /// True when the position is inside a template (after its "{{" and before its "}}").
    public bool InsideTemplate(int position) {
        var lo = 0;
        var hi = _outerTemplates.Count - 1;
        while (lo <= hi) {
            var mid = (lo + hi) / 2;
            var span = _outerTemplates[mid];
            if (position <= span.Start) {
                hi = mid - 1;
            } else if (position >= span.End - 1) {
                lo = mid + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    /// The outermost template the position is inside (after its "{{" and before its "}}"), or null.
    public WikiTemplate? OuterTemplateAt(int position) {
        var lo = 0;
        var hi = _outerTemplates.Count - 1;
        while (lo <= hi) {
            var mid = (lo + hi) / 2;
            var span = _outerTemplates[mid];
            if (position <= span.Start) {
                hi = mid - 1;
            } else if (position >= span.End - 1) {
                lo = mid + 1;
            } else {
                return TemplatesWithin(span).FirstOrDefault(t => t.Span == span);
            }
        }
        return null;
    }

    /// The span with masked text and whitespace trimmed from both ends; an empty span at its start
    /// when nothing is left.
    public TextSpan Core(TextSpan span) {
        var start = span.Start;
        var end = span.End;
        while (start < end && char.IsWhiteSpace(Masked[start])) {
            start++;
        }
        while (end > start && char.IsWhiteSpace(Masked[end - 1])) {
            end--;
        }
        return new TextSpan(start, end);
    }

    /// The text of a span from the masked text (comments as spaces), trimmed.
    public string CoreText(TextSpan span) {
        var core = Core(span);
        return Masked[core.Start..core.End];
    }

    public string Original(TextSpan span) => Text[span.Start..span.End];

    /// "IUCN status" for "Template:IUCN_status", " iucn  status ": underscores to spaces, runs of
    /// whitespace to one space, "Template:" removed, lower case.
    public static string NormalizeName(string name) {
        var text = WhitespaceRun().Replace(name.Replace('_', ' '), " ").Trim();
        if (text.StartsWith("template:", StringComparison.OrdinalIgnoreCase)) {
            text = text["template:".Length..].Trim();
        }
        return text.ToLowerInvariant();
    }

    /// Parameter names are compared in lower case with underscores and spaces kept apart only as
    /// the templates themselves do: "status_ref" and "status ref" are different names, so only
    /// trimming and lower case are applied.
    public static string NormalizeParameterName(string name) => name.Trim().ToLowerInvariant();

    // Templates nested deeper than this are read as text. MediaWiki stops expanding at 100 levels,
    // and the cap keeps a text of nothing but "{{" from holding a million open frames.
    private const int MaxDepth = 100;

    private static (List<WikiTemplate> All, List<TextSpan> Outer) FindTemplates(string masked) {
        var all = new List<WikiTemplate>();
        var outer = new List<TextSpan>();
        var stack = new Stack<Frame>();
        var i = 0;
        while (i < masked.Length) {
            var c = masked[i];
            if (c == '{' && At(masked, i + 1, '{') && stack.Count < MaxDepth) {
                stack.Push(new Frame(i));
                i += 2;
                continue;
            }
            if (c == '}' && At(masked, i + 1, '}') && stack.Count > 0) {
                var frame = stack.Pop();
                var span = new TextSpan(frame.Start, i + 2);
                all.Add(Build(masked, frame, span));
                if (stack.Count == 0) {
                    outer.Add(span);
                }
                i += 2;
                continue;
            }
            if (stack.Count > 0) {
                var top = stack.Peek();
                if (c == '[' && At(masked, i + 1, '[')) {
                    top.LinkDepth++;
                    i += 2;
                    continue;
                }
                if (c == ']' && At(masked, i + 1, ']') && top.LinkDepth > 0) {
                    top.LinkDepth--;
                    i += 2;
                    continue;
                }
                if (top.LinkDepth == 0) {
                    if (c == '|') {
                        top.Pipes.Add(i);
                    } else if (c == '=') {
                        top.EqualSigns.Add(i);
                    }
                }
            }
            i++;
        }
        all.Sort((a, b) => a.Span.Start.CompareTo(b.Span.Start));
        outer.Sort((a, b) => a.Start.CompareTo(b.Start));
        return (all, outer);
    }

    private static WikiTemplate Build(string masked, Frame frame, TextSpan span) {
        var contentEnd = span.End - 2;
        var nameEnd = frame.Pipes.Count > 0 ? frame.Pipes[0] : contentEnd;
        var name = NormalizeName(masked[(frame.Start + 2)..nameEnd]);
        var parameters = new List<TemplateParameter>(frame.Pipes.Count);
        // Pipes and "=" signs are both in text order, so one pass over the "=" signs finds the first
        // one of each parameter.
        var e = 0;
        for (var k = 0; k < frame.Pipes.Count; k++) {
            var pipe = frame.Pipes[k];
            var end = k + 1 < frame.Pipes.Count ? frame.Pipes[k + 1] : contentEnd;
            var whole = new TextSpan(pipe + 1, end);
            while (e < frame.EqualSigns.Count && frame.EqualSigns[e] < pipe) {
                e++;
            }
            var equals = e < frame.EqualSigns.Count && frame.EqualSigns[e] < end ? frame.EqualSigns[e] : -1;
            parameters.Add(equals < 0
                ? new TemplateParameter(pipe, whole, null, whole)
                : new TemplateParameter(pipe, whole, masked[whole.Start..equals].Trim(), new TextSpan(equals + 1, end)));
        }
        return new WikiTemplate(span, name, parameters);
    }

    private static bool At(string text, int i, char c) => i < text.Length && text[i] == c;

    private sealed class Frame(int start) {
        public int Start { get; } = start;
        public List<int> Pipes { get; } = [];
        public List<int> EqualSigns { get; } = [];
        public int LinkDepth { get; set; }
    }

    private static int[] LineStarts(string text) {
        var starts = new List<int> { 0 };
        for (var i = 0; i < text.Length; i++) {
            if (text[i] == '\n') {
                starts.Add(i + 1);
            }
        }
        return [.. starts];
    }

    // Comments, and the elements whose content MediaWiki does not parse as wikitext. An element
    // with no closing tag, or a comment with no end, runs to the end of the text, as in MediaWiki.
    private static string Mask(string text) {
        var matches = Inert().Matches(text);
        if (matches.Count == 0) {
            return text;
        }
        var sb = new StringBuilder(text);
        foreach (Match m in matches) {
            for (var i = m.Index; i < m.Index + m.Length; i++) {
                if (sb[i] != '\n') {
                    sb[i] = ' ';
                }
            }
        }
        return sb.ToString();
    }

    [GeneratedRegex(@"<!--.*?(?:-->|\z)|<(nowiki|pre|syntaxhighlight|source|math)\b[^>]*?/>|<(nowiki|pre|syntaxhighlight|source|math)\b[^>]*>.*?(?:</\2\s*>|\z)",
        RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex Inert();

    [GeneratedRegex(@"\s+")]
    private static partial Regex WhitespaceRun();
}
