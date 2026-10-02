using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// Makes a text value safe to place inside a template parameter. IUCN's strings are plain text, but a
// few carry HTML (<i>et al.</i>), newlines or narrow no-break spaces, and nothing guarantees a value
// can never contain a pipe or a brace.

internal static partial class WikitextValue {
    /// One line, single spaces, no HTML tags, no "|" (written as &#124;) and no braces that could open
    /// or close a template ("{{" and "}}" are removed, a lone brace is written as an entity).
    public static string Clean(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return string.Empty;
        }
        var text = HtmlTag().Replace(value, string.Empty);
        text = CollapseWhitespace(text);

        // Removing one pair can join two others ("{{{{" or "{{{"), so repeat until none is left.
        string previous;
        do {
            previous = text;
            text = text.Replace("{{", string.Empty, StringComparison.Ordinal)
                       .Replace("}}", string.Empty, StringComparison.Ordinal);
        } while (text.Length != previous.Length);

        // A lone brace is harmless on its own, but one at the end of the last value would join the
        // template's closing "}}".
        text = text.Replace("{", "&#123;", StringComparison.Ordinal)
                   .Replace("}", "&#125;", StringComparison.Ordinal)
                   .Replace("|", "&#124;", StringComparison.Ordinal);
        return CollapseWhitespace(text);
    }

    /// Every run of whitespace (newlines, tabs, no-break spaces) becomes one space; trimmed.
    public static string CollapseWhitespace(string text) {
        var sb = new StringBuilder(text.Length);
        var pendingSpace = false;
        foreach (var c in text) {
            if (char.IsWhiteSpace(c)) {
                pendingSpace = sb.Length > 0;
                continue;
            }
            if (pendingSpace) {
                sb.Append(' ');
                pendingSpace = false;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }

    [GeneratedRegex(@"<[^<>]*>")]
    private static partial Regex HtmlTag();
}
