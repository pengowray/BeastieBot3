using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

/// A Wikipedia page named in a search box: a URL ("https://en.wikipedia.org/wiki/List_of_parrots",
/// the mobile site, "/w/index.php?title=...&oldid=...") or a wikilink ("[[List of parrots]]",
/// "[[List of parrots|parrots]]", "[[List of parrots#Cockatoos]]"). Title: the page title with
/// spaces, percent-decoding undone and no section. RevisionId: the oldid of a URL that names one.
/// English: whether the page is on English Wikipedia (a wikilink is taken to be). The URL or
/// wikilink may be wrapped in quotation marks or angle brackets ("\"[[List of parrots]]\"",
/// "<https://en.wikipedia.org/wiki/List_of_parrots>").
public sealed partial record WikipediaPageInput(string Title, long? RevisionId, bool English, string Language) {
    /// The page the text names, or null when the text is not a Wikipedia URL or a wikilink (plain
    /// text such as "Momotidae", which is searched as a name).
    public static WikipediaPageInput? Parse(string? text) {
        var t = Unwrap(text?.Trim() ?? string.Empty);
        if (t.Length == 0) {
            return null;
        }
        if (WikiLink().Match(t) is { Success: true } link) {
            var title = Clean(link.Groups["title"].Value);
            return title.Length == 0 ? null : new WikipediaPageInput(title, null, true, "en");
        }
        if (!t.Contains("wikipedia.org", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }
        if (!Uri.TryCreate(t.Contains("://", StringComparison.Ordinal) ? t : "https://" + t, UriKind.Absolute, out var uri)
            || uri.Host.ToLowerInvariant() is not { } host
            || HostPattern().Match(host) is not { Success: true } hostMatch) {
            return null;
        }
        var language = hostMatch.Groups["lang"].Value;
        var query = ParseQuery(uri.Query);
        string? raw = null;
        if (uri.AbsolutePath.StartsWith("/wiki/", StringComparison.Ordinal)) {
            raw = uri.AbsolutePath["/wiki/".Length..];
        } else if (query.TryGetValue("title", out var queryTitle)) {
            // In a query string "+" is a space; in a path it is a "+" ("C++").
            raw = queryTitle.Replace('+', ' ');
        }
        if (raw is null) {
            return null;
        }
        var pageTitle = Clean(Uri.UnescapeDataString(raw));
        if (pageTitle.Length == 0) {
            return null;
        }
        long? revision = query.TryGetValue("oldid", out var oldid) && long.TryParse(oldid, out var r) && r > 0 ? r : null;
        return new WikipediaPageInput(pageTitle, revision, language == "en", language);
    }

    // The opening and closing marks a URL or wikilink may be wrapped in, as pasted from a message
    // or a document: straight and curly quotation marks, guillemets, backticks, angle brackets.
    private static readonly (char Open, char Close)[] Wrappers = [
        ('"', '"'), ('\'', '\''), ('\u201C', '\u201D'), ('\u2018', '\u2019'), ('\u201E', '\u201C'), ('\u00AB', '\u00BB'), ('`', '`'), ('<', '>'),
    ];

    // The text without the pairs of marks around it, outermost first ("\"<...>\"").
    private static string Unwrap(string text) {
        while (text.Length >= 2 && Array.Exists(Wrappers, w => text[0] == w.Open && text[^1] == w.Close)) {
            text = text[1..^1].Trim();
        }
        return text;
    }

    private static string Clean(string title) {
        var hash = title.IndexOf('#');
        if (hash >= 0) {
            title = title[..hash];
        }
        return Regex.Replace(title.Replace('_', ' '), @"\s+", " ").Trim();
    }

    private static Dictionary<string, string> ParseQuery(string query) {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)) {
            var eq = pair.IndexOf('=');
            var key = eq < 0 ? pair : pair[..eq];
            var value = eq < 0 ? string.Empty : pair[(eq + 1)..];
            result.TryAdd(Uri.UnescapeDataString(key), value);
        }
        return result;
    }

    // [[Title]], [[Title|label]], [[:Title]], [[Title#Section]]; a wikilink must be the whole text.
    [GeneratedRegex(@"^\[\[:?(?<title>[^\[\]|]+)(?:\|[^\[\]]*)?\]\]$")]
    private static partial Regex WikiLink();

    // en.wikipedia.org, en.m.wikipedia.org, de.wikipedia.org.
    [GeneratedRegex(@"^(?<lang>[a-z][a-z0-9-]*)(?:\.m)?\.wikipedia\.org$")]
    private static partial Regex HostPattern();
}
