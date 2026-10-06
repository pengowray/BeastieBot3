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

    public static string ToHtml(string wikitext) {
        var html = new StringBuilder();
        var depth = 0;
        foreach (var line in wikitext.Replace("\r", string.Empty).Split('\n')) {
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
        return html.ToString();
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

    private static string Encode(string text) => WebUtility.HtmlEncode(text);
}
