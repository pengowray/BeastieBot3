using System.Globalization;
using System.Text;

namespace BeastieBot3.Site.Data;

/// Turns what a visitor typed into an FTS5 MATCH expression for name_fts. FTS5 has its own query
/// language (AND, OR, NOT, NEAR, column filters "a:b", "^" and "*", quotes), so user input is never
/// passed through as it is: every character other than a letter, digit or combining mark becomes a
/// space, each remaining word is quoted, and the last one gets "*" so a half-typed word still
/// matches. "panthera ti" becomes "panthera" "ti"*. The table's unicode61 tokenizer folds case and
/// diacritics, and treats the same punctuation as word breaks, so nothing that could match is lost.
public static class FtsQuery {
    /// More words than this are ignored; no IUCN name needs them.
    public const int MaxTokens = 8;

    /// The MATCH expression, or null when the input has no letters or digits (then no FTS query
    /// should be run at all: an empty phrase is an error in FTS5).
    public static string? Build(string input) {
        var tokens = Tokens(input);
        if (tokens.Count == 0) {
            return null;
        }
        var sb = new StringBuilder();
        for (var i = 0; i < tokens.Count; i++) {
            if (i > 0) {
                sb.Append(' ');
            }
            sb.Append('"').Append(tokens[i]).Append('"');
            if (i == tokens.Count - 1) {
                sb.Append('*');
            }
        }
        return sb.ToString();
    }

    internal static List<string> Tokens(string input) {
        var normalized = input.Normalize(NormalizationForm.FormC);
        var sb = new StringBuilder(normalized.Length);
        foreach (var c in normalized) {
            var category = CharUnicodeInfo.GetUnicodeCategory(c);
            var keep = char.IsLetterOrDigit(c)
                || category is UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark;
            sb.Append(keep ? c : ' ');
        }
        return sb.ToString()
            .Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Take(MaxTokens)
            .ToList();
    }
}
