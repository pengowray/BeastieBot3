using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// Makes a text value safe to place inside a template parameter. IUCN's strings are plain text, but a
// few carry HTML (<i>et al.</i>), newlines or narrow no-break spaces, and nothing guarantees a value
// can never contain a pipe or a brace.

internal static partial class WikitextValue {
    /// One line, single spaces, no HTML tags, no "|" (written as &#124;) and no braces.
    public static string Clean(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return string.Empty;
        }
        var text = HtmlTag().Replace(value, string.Empty);

        // Every brace is removed, not only "{{" and "}}": removing one pair can join two others
        // ("{{{{"), and a lone brace at the end of the last value would join the template's closing
        // "}}". No IUCN name or author contains a brace. An entity such as &#125; would be safe too,
        // but its digits and semicolon make CS1 report an author name as numeric or as several names.
        var sb = new StringBuilder(text.Length);
        foreach (var c in text) {
            if (c is '{' or '}') {
                continue;
            }
            sb.Append(c == '|' ? "&#124;" : c.ToString());
        }
        return CollapseWhitespace(sb.ToString());
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
