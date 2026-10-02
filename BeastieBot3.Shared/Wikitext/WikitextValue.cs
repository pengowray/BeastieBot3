using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// Makes a text value safe to place inside a template parameter. IUCN's strings are plain text, but a
// few carry HTML (<i>et al.</i>), newlines or narrow no-break spaces, and nothing guarantees a value
// can never contain a pipe or a brace.

internal static partial class WikitextValue {
    /// One line, single spaces, no HTML tags, no "|" (written as &#124;), no braces and none of the
    /// invisible characters CS1 reports as an error (see IsInvisible).
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
            if (c is '{' or '}' || IsInvisible(c)) {
                continue;
            }
            sb.Append(c == '|' ? "&#124;" : c.ToString());
        }
        return CollapseWhitespace(sb.ToString());
    }

    // The characters in CS1's invisible_chars list (Module:Citation/CS1/Configuration) that are not
    // whitespace, which CollapseWhitespace already turns into spaces (tab, line feed, carriage return,
    // no-break space, hair space): C0 and C1 controls, DEL, zero width space, zero width joiner and
    // soft hyphen. Also the word joiner and the byte order mark, which are just as invisible. CS1 lets
    // a zero width joiner through in some scripts that need it, but IUCN's names and titles are in the
    // Latin script. The replacement character U+FFFD is kept on purpose: it stands for a letter that
    // was lost, and dropping it would turn a visible error into a misspelling nobody notices
    // (`site build-db` repairs those it can and `site check-citations` lists the rest).
    private static bool IsInvisible(char c) =>
        (char.IsControl(c) && !char.IsWhiteSpace(c))
        || c is '\u200B' or '\u200D' or '\u00AD' or '\u2060' or '\uFEFF';

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
