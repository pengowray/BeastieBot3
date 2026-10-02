using System.Net;
using System.Text;
using System.Text.RegularExpressions;

// Finds {{cite iucn}} templates in article wikitext and reads their parameters, for comparing
// en-wiki's citations with the parts the site builds (`site check-citations`).
//
// Matches {{cite iucn}}, {{Cite IUCN}}, {{cite_iucn}} and other letter cases. A template ends at
// its matching "}}", counting nested templates and links, so "|author=[[BirdLife International]]"
// and "|title=''{{lang|la|Ursus}}''" stay inside one parameter. HTML comments are removed first.
// Named parameters are keyed in lower case; positional ones are ignored.

namespace BeastieBot3.SiteBuild;

internal sealed record WikiCiteIucn(IReadOnlyDictionary<string, string> Parameters, string Text) {
    public string? Get(string name) =>
        Parameters.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;
}

internal static class CiteIucnTemplateScanner {
    private static readonly Regex Start = new(@"\{\{\s*cite[ _]+iucn\s*(?=\||\}\})", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Comment = new(@"<!--.*?-->", RegexOptions.Singleline | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ArticleNumber = new(@"\be\.?\s*T(?<t>\d+)\s*A(?<a>\d+)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Link = new(@"\[\[(?:[^\[\]|]*\|)?(?<text>[^\[\]]*)\]\]", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex HtmlTag = new(@"<[^>]*>", RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static IEnumerable<WikiCiteIucn> Find(string? wikitext) {
        if (string.IsNullOrEmpty(wikitext) || wikitext.IndexOf("iucn", StringComparison.OrdinalIgnoreCase) < 0) yield break;
        var text = wikitext.Contains("<!--", StringComparison.Ordinal) ? Comment.Replace(wikitext, string.Empty) : wikitext;
        var match = Start.Match(text);
        while (match.Success) {
            var end = FindEnd(text, match.Index);
            if (end < 0) yield break;
            var body = text.Substring(match.Index + match.Length, end - (match.Index + match.Length));
            yield return new WikiCiteIucn(ReadParameters(body), text.Substring(match.Index, end + 2 - match.Index));
            match = Start.Match(text, end + 2);
        }
    }

    /// The taxon and assessment ids in |article-number=, or |page= when there is none.
    public static (long TaxonId, long AssessmentId)? ArticleIds(WikiCiteIucn cite) {
        var value = cite.Get("article-number") ?? cite.Get("page");
        if (value is null) return null;
        var match = ArticleNumber.Match(value);
        if (!match.Success
            || !long.TryParse(match.Groups["t"].Value, out var taxonId)
            || !long.TryParse(match.Groups["a"].Value, out var assessmentId)) {
            return null;
        }
        return (taxonId, assessmentId);
    }

    /// A parameter value as a reader sees it: links reduced to their text, italics and HTML tags
    /// removed, entities decoded, whitespace collapsed.
    public static string PlainText(string? value) {
        if (string.IsNullOrEmpty(value)) return string.Empty;
        var text = Link.Replace(value, m => m.Groups["text"].Value);
        text = text.Replace("'''", string.Empty, StringComparison.Ordinal).Replace("''", string.Empty, StringComparison.Ordinal);
        text = WebUtility.HtmlDecode(HtmlTag.Replace(text, string.Empty));
        return Whitespace.Replace(text, " ").Trim();
    }

    // Index of the "}}" closing the template that opens at start, or -1.
    private static int FindEnd(string text, int start) {
        var braces = 0;
        var brackets = 0;
        var i = start;
        while (i < text.Length - 1) {
            if (text[i] == '{' && text[i + 1] == '{') {
                braces++;
                i += 2;
            } else if (text[i] == '}' && text[i + 1] == '}') {
                braces--;
                if (braces == 0) return i;
                i += 2;
            } else if (text[i] == '[' && text[i + 1] == '[') {
                brackets++;
                i += 2;
            } else if (text[i] == ']' && text[i + 1] == ']') {
                if (brackets > 0) brackets--;
                i += 2;
            } else {
                i++;
            }
        }
        return -1;
    }

    private static Dictionary<string, string> ReadParameters(string body) {
        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var part in SplitTopLevel(body)) {
            var equals = part.IndexOf('=');
            if (equals <= 0) continue;
            var name = part[..equals].Trim().ToLowerInvariant();
            if (name.Length == 0 || name.Contains("{{", StringComparison.Ordinal) || name.Contains("[[", StringComparison.Ordinal)) continue;
            parameters[name] = part[(equals + 1)..].Trim();
        }
        return parameters;
    }

    // Splits at "|" outside nested templates and links.
    private static List<string> SplitTopLevel(string body) {
        var parts = new List<string>();
        var current = new StringBuilder();
        var braces = 0;
        var brackets = 0;
        var i = 0;
        while (i < body.Length) {
            var c = body[i];
            var next = i + 1 < body.Length ? body[i + 1] : '\0';
            if (c == '{' && next == '{') { braces++; current.Append("{{"); i += 2; continue; }
            if (c == '}' && next == '}' && braces > 0) { braces--; current.Append("}}"); i += 2; continue; }
            if (c == '[' && next == '[') { brackets++; current.Append("[["); i += 2; continue; }
            if (c == ']' && next == ']' && brackets > 0) { brackets--; current.Append("]]"); i += 2; continue; }
            if (c == '|' && braces == 0 && brackets == 0) {
                parts.Add(current.ToString());
                current.Clear();
                i++;
                continue;
            }
            current.Append(c);
            i++;
        }
        parts.Add(current.ToString());
        return parts;
    }
}
