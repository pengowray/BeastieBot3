using System.Net;
using System.Text;
using System.Text.RegularExpressions;

// A rough HTML preview of the wikitext a list page writes, so the preview shows what Wikipedia
// shows: only the wikilinked part of a line is a link (to the Wikipedia article), '' and ''' are
// italics and bold, and {{IUCN status}} is the category code linked to its article, followed by a
// superscript "IUCN <year>" link to the assessment, as the template renders it. Headings, "*" and
// "**" lists, and plain lines are handled; any other template is shown as written.

namespace BeastieBot3.Site.Lists;

public static class WikitextPreview {
    private const string WikipediaBase = "https://en.wikipedia.org/wiki/";

    // The article {{IUCN status}} links each code to.
    private static readonly Dictionary<string, (string Article, string Css)> Categories = new(StringComparer.OrdinalIgnoreCase) {
        ["EX"] = ("Extinct", "cat-ex"),
        ["EW"] = ("Extinct in the wild", "cat-ew"),
        ["CR"] = ("Critically endangered", "cat-cr"),
        ["CR(PE)"] = ("Critically endangered", "cat-cr"),
        ["CR(PEW)"] = ("Critically endangered", "cat-cr"),
        ["EN"] = ("Endangered species", "cat-en"),
        ["VU"] = ("Vulnerable species", "cat-vu"),
        ["NT"] = ("Near-threatened species", "cat-nt"),
        ["LR/nt"] = ("Near-threatened species", "cat-nt"),
        ["LR/cd"] = ("Conservation dependent", "cat-nt"),
        ["LC"] = ("Least-concern species", "cat-lc"),
        ["LR/lc"] = ("Least-concern species", "cat-lc"),
        ["DD"] = ("Data deficient", "cat-grey"),
        ["NE"] = ("Not evaluated", "cat-grey"),
    };

    private static readonly Regex Heading = new(@"^(={2,6})\s*(.*?)\s*\1\s*$", RegexOptions.Compiled);

    /// lineSuffixes: HTML added after each bullet line, in order (null: nothing); used for the links
    /// to the Catalogue of Life and Wikidata of species from those sources (GroupListSources).
    public static string ToHtml(string wikitext, IReadOnlyList<string?>? lineSuffixes = null) {
        var html = new StringBuilder();
        var depth = 0;
        var bullet = 0;
        var refs = new PreviewRefs();
        var inRefList = false;
        foreach (var rawLine in wikitext.Replace("\r", string.Empty).Split('\n')) {
            // The {{reflist|refs=...}} block of list-defined references: its citations are listed at the end.
            if (rawLine.StartsWith("{{reflist|refs=", StringComparison.OrdinalIgnoreCase)) {
                inRefList = true;
                continue;
            }
            if (inRefList) {
                if (rawLine.Trim() == "}}") {
                    inRefList = false;
                } else {
                    refs.Define(rawLine);
                }
                continue;
            }
            var line = refs.Mark(rawLine);
            var stars = line.TakeWhile(c => c == '*').Count();
            for (; depth > stars; depth--) {
                html.Append("</li></ul>");
            }
            if (stars > 0) {
                if (depth == stars) {
                    html.Append("</li>");
                }
                for (; depth < stars; depth++) {
                    html.Append("<ul class=\"preview-lines\">");
                }
                html.Append("<li>").Append(Inline(line[stars..].TrimStart()));
                if (lineSuffixes is not null && bullet < lineSuffixes.Count && lineSuffixes[bullet] is { } suffix) {
                    html.Append(suffix);
                }
                bullet++;
                continue;
            }
            if (line.Trim().Length == 0) {
                continue;
            }
            if (Heading.Match(line) is { Success: true } heading) {
                var tag = "h" + Math.Min(6, heading.Groups[1].Length + 1);
                html.Append('<').Append(tag).Append(" class=\"preview-heading\">").Append(Inline(heading.Groups[2].Value))
                    .Append("</").Append(tag).Append('>');
                continue;
            }
            html.Append("<p>").Append(Inline(line)).Append("</p>");
        }
        for (; depth > 0; depth--) {
            html.Append("</li></ul>");
        }
        return refs.Finish(html.ToString());
    }

    // References in a list (ListReferences): each <ref> becomes a superscript number, one per ref name,
    // and the citations are listed at the end as written, since the preview does not render templates.
    private sealed class PreviewRefs {
        private static readonly Regex Ref = new("<ref name=\"([^\"]*)\"(?:\\s*/>|>(.*?)</ref>)", RegexOptions.Compiled);
        private readonly List<string> _order = new();
        private readonly Dictionary<string, string?> _citations = new(StringComparer.Ordinal);

        // A placeholder that Inline passes through unchanged.
        private const char Open = '';
        private const char Close = '';

        public string Mark(string line) => Ref.Replace(line, m => {
            var name = m.Groups[1].Value;
            if (!_citations.ContainsKey(name)) {
                _order.Add(name);
                _citations[name] = null;
            }
            if (m.Groups[2].Success) {
                _citations[name] = m.Groups[2].Value;
            }
            return $"{Open}{_order.IndexOf(name) + 1}{Close}";
        });

        public void Define(string line) {
            foreach (Match m in Ref.Matches(line)) {
                if (m.Groups[2].Success && _citations.ContainsKey(m.Groups[1].Value)) {
                    _citations[m.Groups[1].Value] = m.Groups[2].Value;
                }
            }
        }

        public string Finish(string html) {
            if (_order.Count == 0) {
                return html;
            }
            var sb = new StringBuilder(Regex.Replace(html, $"{Open}(\\d+){Close}", "<sup class=\"preview-ref\">[$1]</sup>"));
            sb.Append("<ol class=\"preview-refs\">");
            foreach (var name in _order) {
                sb.Append("<li>").Append(Encode(_citations[name] ?? string.Empty)).Append("</li>");
            }
            sb.Append("</ol>");
            return sb.ToString();
        }
    }

    /// One line's inline markup: wikilinks, italics and bold, {{IUCN status}}.
    public static string Inline(string text) {
        var html = new StringBuilder();
        var italic = false;
        var bold = false;
        var i = 0;
        while (i < text.Length) {
            if (At(text, i, "[[") && text.IndexOf("]]", i + 2, StringComparison.Ordinal) is var end and > 0) {
                var inner = text[(i + 2)..end];
                var pipe = inner.IndexOf('|');
                var target = pipe < 0 ? inner : inner[..pipe];
                var label = pipe < 0 ? inner : inner[(pipe + 1)..];
                html.Append("<a href=\"").Append(Encode(WikipediaUrl(target))).Append("\">").Append(Inline(label)).Append("</a>");
                i = end + 2;
            } else if (At(text, i, "{{") && text.IndexOf("}}", i + 2, StringComparison.Ordinal) is var close and > 0) {
                html.Append(Template(text[(i + 2)..close]));
                i = close + 2;
            } else if (At(text, i, "<small>") || At(text, i, "</small>")) {
                // The authority in small text (SpeciesListLine).
                var closing = text[i + 1] == '/';
                html.Append(closing ? "</small>" : "<small>");
                i += closing ? 8 : 7;
            } else if (At(text, i, "<nowiki>") && text.IndexOf("</nowiki>", i + 8, StringComparison.Ordinal) is var nowikiEnd and > 0) {
                html.Append(Encode(text[(i + 8)..nowikiEnd]));
                i = nowikiEnd + 9;
            } else if (At(text, i, "'''")) {
                html.Append(bold ? "</b>" : "<b>");
                bold = !bold;
                i += 3;
            } else if (At(text, i, "''")) {
                html.Append(italic ? "</i>" : "<i>");
                italic = !italic;
                i += 2;
            } else {
                html.Append(Encode(text[i].ToString()));
                i++;
            }
        }
        if (italic) {
            html.Append("</i>");
        }
        if (bold) {
            html.Append("</b>");
        }
        return html.ToString();
    }

    // {{IUCN status|CODE|taxonId/assessmentId|1|year=YYYY}}; any other template as written.
    private static string Template(string body) {
        var parts = body.Split('|');
        if (!string.Equals(parts[0].Trim(), "IUCN status", StringComparison.OrdinalIgnoreCase) || parts.Length < 2) {
            return Encode("{{" + body + "}}");
        }
        var code = parts[1].Trim();
        var (article, css) = Categories.GetValueOrDefault(code, (code, "cat-grey"));
        var html = new StringBuilder();
        html.Append("<a class=\"preview-status\" href=\"").Append(Encode(WikipediaUrl(article))).Append("\"><span class=\"badge ")
            .Append(css).Append("\">").Append(Encode(code)).Append("</span></a>");
        var ids = parts.Length > 2 ? parts[2].Trim() : string.Empty;
        var year = parts.Skip(2).Select(p => p.Trim()).FirstOrDefault(p => p.StartsWith("year=", StringComparison.Ordinal))?[5..];
        if (Regex.IsMatch(ids, @"^\d+/\d+$")) {
            html.Append("<sup> <a href=\"https://www.iucnredlist.org/species/").Append(ids).Append("\">IUCN")
                .Append(year is { Length: > 0 } ? " " + Encode(year) : string.Empty).Append("</a></sup>");
        }
        return html.ToString();
    }

    private static string WikipediaUrl(string title) =>
        WikipediaBase + Uri.EscapeDataString(title.Trim().Replace(' ', '_')).Replace("%2F", "/").Replace("%3A", ":");

    private static bool At(string text, int i, string token) => string.CompareOrdinal(text, i, token, 0, token.Length) == 0;

    // Not SiteHtml.Encode: that escapes private-use characters, so the reference placeholders
    // (PreviewRefs.Open and Close) would not reach PreviewRefs.Finish.
    private static string Encode(string text) => WebUtility.HtmlEncode(text);
}
