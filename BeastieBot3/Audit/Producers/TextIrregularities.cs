using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

// Shared detectors for the small set of text irregularities the audit reports on, plus one
// canonical Clean() that produces the normalised suggestion. Both the taxonomy-field cleanup
// report and the synonym formatting reports classify values through here, so "leading whitespace",
// "double spaces", "curly quotes", etc. mean the same thing everywhere. Detectors are deliberately
// specific: plain non-ASCII letters (accents, ñ, ü) are never flagged on their own.

namespace BeastieBot3.Audit.Producers;

internal static class TextIrregularities {
    private static readonly Regex ShortTag = new("<[^<>]{1,64}>", RegexOptions.Compiled);
    private static readonly Regex HtmlEntityPattern =
        new(@"&(#\d{1,7}|#x[0-9a-fA-F]{1,6}|[A-Za-z][A-Za-z0-9]{1,31});", RegexOptions.Compiled);

    // Curly/typographic quotes and primes that stand in for straight ' and ".
    private const string CurlyQuoteChars = "‘’‚‛“”„‟′″";

    // -- whitespace family -----------------------------------------------------------------

    public static bool HasLeadingWhitespace(string s) => s.Length > 0 && char.IsWhiteSpace(s[0]);

    public static bool HasTrailingWhitespace(string s) => s.Length > 0 && char.IsWhiteSpace(s[^1]);

    // Two or more consecutive ASCII spaces.
    public static bool HasDoubleSpace(string s) {
        for (var i = 1; i < s.Length; i++) {
            if (s[i] == ' ' && s[i - 1] == ' ') {
                return true;
            }
        }
        return false;
    }

    // Any whitespace that is not a plain ASCII space: non-breaking and narrow spaces, tabs, CR, LF.
    public static bool HasSpecialWhitespace(string s) => s.Any(c => char.IsWhiteSpace(c) && c != ' ');

    // A space immediately after "(" or immediately before ")", e.g. "( Herre, 1924)" / "(Herre, 1924 )".
    public static bool HasSpaceInsideParentheses(string s) {
        for (var i = 0; i < s.Length; i++) {
            if (s[i] == '(' && i + 1 < s.Length && char.IsWhiteSpace(s[i + 1])) {
                return true;
            }
            if (s[i] == ')' && i > 0 && char.IsWhiteSpace(s[i - 1])) {
                return true;
            }
        }
        return false;
    }

    // A space immediately before a comma, e.g. "(Herre , 1924)".
    public static bool HasSpaceBeforeComma(string s) {
        for (var i = 1; i < s.Length; i++) {
            if (s[i] == ',' && char.IsWhiteSpace(s[i - 1])) {
                return true;
            }
        }
        return false;
    }

    // -- other (non-whitespace) family -----------------------------------------------------

    // A short "<...>" run, the shape of stray markup like <i> or <em>.
    public static bool HasMarkup(string s) => ShortTag.IsMatch(s);

    // A stray HTML entity such as &amp; or &#39;.
    public static bool HasHtmlEntity(string s) => HtmlEntityPattern.IsMatch(s);

    public static bool HasCurlyQuotes(string s) => s.Any(c => CurlyQuoteChars.IndexOf(c) >= 0);

    // Encoding artefacts: the replacement character, zero-width/format characters, or control
    // characters other than tab/CR/LF. (Tab/CR/LF are covered by HasSpecialWhitespace.)
    public static bool HasUnusualCharacter(string s) => s.Any(IsUnusual);

    private static bool IsUnusual(char c) {
        if (c == '�') {
            return true;                                   // replacement character (mojibake)
        }
        if (c is '​' or '‌' or '‍' or '﻿' or '­') {
            return true;                                   // zero-width / soft hyphen
        }
        return char.IsControl(c) && c is not ('\t' or '\r' or '\n');
    }

    // -- canonical cleaning ----------------------------------------------------------------

    // The normalised target for a value: markup and stray entities removed, curly quotes
    // straightened, encoding artefacts dropped, whitespace collapsed and trimmed, and spacing
    // around parentheses and commas tidied. Returns "" for blank/whitespace-only input.
    public static string Clean(string value) {
        var s = ShortTag.Replace(value, "");
        s = WebUtility.HtmlDecode(s);
        s = ShortTag.Replace(s, "");                       // in case an entity decoded into a tag
        s = StraightenQuotes(s);
        s = DropUnusual(s);
        s = NormalizeWhitespace(s);
        s = FixPunctuationSpacing(s);
        return NormalizeWhitespace(s);
    }

    private static string StraightenQuotes(string s) {
        if (!s.Any(c => CurlyQuoteChars.IndexOf(c) >= 0)) {
            return s;
        }
        var sb = new StringBuilder(s.Length);
        foreach (var c in s) {
            sb.Append(c switch {
                '‘' or '’' or '‚' or '‛' or '′' => '\'',
                '“' or '”' or '„' or '‟' or '″' => '"',
                _ => c,
            });
        }
        return sb.ToString();
    }

    private static string DropUnusual(string s) {
        if (!s.Any(IsUnusual)) {
            return s;
        }
        var sb = new StringBuilder(s.Length);
        foreach (var c in s) {
            if (!IsUnusual(c)) {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    private static string NormalizeWhitespace(string value) {
        var sb = new StringBuilder(value.Length);
        var prevSpace = false;
        foreach (var ch in value) {
            if (char.IsWhiteSpace(ch)) {
                if (!prevSpace) {
                    sb.Append(' ');
                    prevSpace = true;
                }
            } else {
                sb.Append(ch);
                prevSpace = false;
            }
        }
        return sb.ToString().Trim();
    }

    private static string FixPunctuationSpacing(string s) =>
        s.Replace("( ", "(").Replace(" )", ")").Replace(" ,", ",");
}
