using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

// HTML-to-text conversion for IUCN assessment narratives. Handles:
// - Tag stripping with special cases (<p>, <br>, <li> → newlines)
// - HTML entity decoding (&amp;, &#123;, etc.)
// - IUCN-specific patterns (non-breaking spaces, line breaking)
// - Two modes: "Exact" for comparison, "Friendly" for display
// Used by IucnHtmlConsistencyCommand for field validation.

namespace BeastieBot3.Iucn;

internal static class IucnHtmlUtilities {
    private enum PlainTextFlavor {
        Exact,
        Friendly
    }

    private const string AttributeFragment = "(?:\"[^\"]*\"|'[^']*'|[^'\"<>])*";
    private const string TagNamePattern = "[A-Za-z][A-Za-z0-9:_-]*";

    private static readonly Regex CommentRegex = new("<!--.*?-->", RegexOptions.Compiled | RegexOptions.Singleline);
    private static readonly Regex CDataRegex = new("<!\\[CDATA\\[.*?\\]\\]>", RegexOptions.Compiled | RegexOptions.Singleline);
    private static readonly Regex ScriptBlockRegex = new($"<script\\b{AttributeFragment}>.*?</script>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    private static readonly Regex StyleBlockRegex = new($"<style\\b{AttributeFragment}>.*?</style>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    private static readonly Regex BreakTagRegex = new($"<br\\b{AttributeFragment}>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex BlockTagRegex = new($"</?(?:p|div|section|article|blockquote|ul|ol|li|table|thead|tbody|tfoot|tr|th|td|h[1-6])\\b{AttributeFragment}>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex GenericTagRegex = new($"</?{TagNamePattern}\\b{AttributeFragment}>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex SupTagRegex = new("<sup\\b[^>]*>(.*?)</sup>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
    private static readonly Regex NumericEntityRegex = new("&#(?:(?<dec>[0-9]+)|x(?<hex>[0-9a-fA-F]+));", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex RandomEmailToRemove = new(@"""[^""]*?<[a-z]+@yahoo\.com\.br>.*?""[^""/]*?/", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Dictionary<char, char> SuperscriptMap = new() {
        ['0'] = '⁰',
        ['1'] = '¹',
        ['2'] = '²',
        ['3'] = '³',
        ['4'] = '⁴',
        ['5'] = '⁵',
        ['6'] = '⁶',
        ['7'] = '⁷',
        ['8'] = '⁸',
        ['9'] = '⁹',
        ['+'] = '⁺',
        ['-'] = '⁻',
        ['='] = '⁼',
        ['('] = '⁽',
        [')'] = '⁾',
        ['n'] = 'ⁿ',
        ['N'] = 'ᴺ',
        ['i'] = 'ⁱ'
    };

    // Inline elements that carry no text on their own. Two forms of these are redundant markup that
    // can be dropped without changing the rendered text: an instance wrapping only whitespace, and a
    // run of identical instances nested or repeated back-to-back. The rich-text editor that produced
    // these narratives emits both — long runs of empty <span>s and deep stacks of identical <span>s.
    private const string InlineTagNames = "span|b|i|em|strong|u|s|strike|font|sub|sup|small|big|mark|a|label|abbr|acronym|cite|q|tt|ins|del|var|kbd|samp|o:p";
    private static readonly Regex EmptyInlineTagRegex = new(
        "<(" + InlineTagNames + ")\\b" + AttributeFragment + ">((?:\\s|&nbsp;|&#160;|&#xA0;|\\u00A0|\\u200B|\\u200C|\\uFEFF)*)</\\1>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    // A run of byte-identical adjacent opening inline tags (and likewise closing tags) collapses to a
    // single tag: the duplicates only deepen the nesting, they add no text and no distinct styling.
    private static readonly Regex DuplicateOpenTagRegex = new(
        "(<(?:" + InlineTagNames + ")\\b" + AttributeFragment + ">)\\1+",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    private static readonly Regex DuplicateCloseTagRegex = new(
        "(</(?:" + InlineTagNames + ")>)\\1+",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex MultipleSpacesRegex = new("[ \\t]{2,}", RegexOptions.Compiled);
    private static readonly Regex BlankLineRunRegex = new("(?:\\n[ \\t]*){3,}", RegexOptions.Compiled);

    // Produces a suggested tidy of a narrative HTML field by removing redundant empty inline markup
    // and collapsing runs of identical adjacent inline tags (the nested empty/duplicate <span> stacks
    // that swamp some fields), then tidying the whitespace left behind. Structural tags (<p>, <br>,
    // <li>, …) are left intact, and the removed markup carries no text, so the readable content is
    // unchanged. It is still a best-effort suggestion: callers should verify the extracted text still
    // matches and note that identical rendering has not been double-checked.
    public static string? CleanRedundantMarkup(string? html) {
        if (string.IsNullOrEmpty(html)) {
            return html;
        }

        var working = NormalizeLineEndings(html);
        working = CommentRegex.Replace(working, string.Empty);

        // Each collapse can expose a newly-redundant tag (an emptied parent, or opens/closes that
        // become adjacent once an inner layer is gone), so repeat until stable, bounded for safety.
        // An empty tag that wrapped whitespace is replaced with a single space, not nothing, so it
        // cannot silently fuse the words on either side.
        for (var pass = 0; pass < 200; pass++) {
            var next = EmptyInlineTagRegex.Replace(working, m => m.Groups[2].Value.Length == 0 ? string.Empty : " ");
            next = DuplicateOpenTagRegex.Replace(next, "$1");
            next = DuplicateCloseTagRegex.Replace(next, "$1");
            if (string.Equals(next, working, StringComparison.Ordinal)) {
                break;
            }
            working = next;
        }

        working = MultipleSpacesRegex.Replace(working, " ");
        working = BlankLineRunRegex.Replace(working, "\n\n");
        return working.Trim();
    }

    public static string? ConvertHtmlToPlainTextNeater(string? html) => ConvertHtmlToPlain(html, PlainTextFlavor.Friendly);

    public static string? ConvertHtmlToExactPlainText(string? html) => ConvertHtmlToPlain(html, PlainTextFlavor.Exact);

    public static string? NormalizePlainTextExact(string? value) {
        if (value is null) {
            return null;
        }

        var normalized = NormalizeLineEndings(value);
        normalized = NormalizeNonBreakingSpaces(normalized, PlainTextFlavor.Exact);
        normalized = DecodeNumericEntities(normalized);
        return RemoveInvisibleCharacters(normalized);
    }

    public static bool NormalizedEquals(string? left, string? right) {
        if (left is null && right is null) {
            return true;
        }
        if (left is null || right is null) {
            return false;
        }
        return string.Equals(left, right, StringComparison.Ordinal);
    }

    public static string ShortenForDisplay(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return value ?? string.Empty;
        }

        const int maxLength = 160;
        var normalized = value.Replace("\r", " ", StringComparison.Ordinal)
                              .Replace("\n", " ", StringComparison.Ordinal);
        if (normalized.Length <= maxLength) {
            return normalized;
        }

        return normalized[..maxLength] + "…";
    }

    private static string? ConvertHtmlToPlain(string? html, PlainTextFlavor flavor) {
        if (html is null) {
            return null;
        }

        if (html.Length == 0) {
            return string.Empty;
        }

        var working = NormalizeLineEndings(html);

        working = CommentRegex.Replace(working, string.Empty);
        working = CDataRegex.Replace(working, string.Empty);
        working = ScriptBlockRegex.Replace(working, string.Empty);
        working = StyleBlockRegex.Replace(working, string.Empty);

        working = RandomEmailToRemove.Replace(working, string.Empty); // fix assessmentId: 104125629 (contains broken nested tags)

        working = ReplaceSupTags(working, flavor);
        working = ReplaceStructuralTags(working, flavor);
        working = GenericTagRegex.Replace(working, string.Empty);

        working = WebUtility.HtmlDecode(working);
        working = DecodeNumericEntities(working);
        working = NormalizeLineEndings(working);

        working = NormalizeNonBreakingSpaces(working, flavor);
        working = RemoveInvisibleCharacters(working);
        working = CollapsePlainWhitespace(working, flavor);
        working = TrimPlainWhitespace(working, flavor);

        if (flavor == PlainTextFlavor.Exact) {
            return EncodeReservedCharacters(working);
        }

        return working;
    }

    private static string ReplaceSupTags(string value, PlainTextFlavor flavor) {
        return SupTagRegex.Replace(value, match => {
            var inner = match.Groups[1].Value;
            if (inner.Length == 0) {
                return string.Empty;
            }

            if (flavor == PlainTextFlavor.Friendly) {
                return ConvertToSuperscript(inner);
            }

            return inner;
        });
    }

    private static string ReplaceStructuralTags(string value, PlainTextFlavor flavor) {
    var breakReplacement = flavor == PlainTextFlavor.Friendly ? "\n" : string.Empty;
        value = BreakTagRegex.Replace(value, breakReplacement);

    var blockReplacement = flavor == PlainTextFlavor.Friendly ? "\n" : string.Empty;
        return BlockTagRegex.Replace(value, blockReplacement);
    }

    private static string ConvertToSuperscript(string value) {
        var leadingSpace = value.Length > 0 && char.IsWhiteSpace(value[0]);
        var trailingSpace = value.Length > 0 && char.IsWhiteSpace(value[^1]);

        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return value;
        }

        var builder = new StringBuilder(trimmed.Length);
        foreach (var ch in trimmed) {
            if (SuperscriptMap.TryGetValue(ch, out var sup)) {
                builder.Append(sup);
            } else {
                builder.Append(ch);
            }
        }

        if (leadingSpace) {
            builder.Insert(0, ' ');
        }

        if (trailingSpace) {
            builder.Append(' ');
        }

        return builder.ToString();
    }

    private static string NormalizeLineEndings(string value) {
        return value.Replace("\r\n", "\n", StringComparison.Ordinal)
                    .Replace('\r', '\n');
    }

    private static string NormalizeNonBreakingSpaces(string value, PlainTextFlavor flavor) {
        var builder = new StringBuilder(value.Length);
        var changed = false;

        foreach (var ch in value) {
            switch (ch) {
                case '\u00A0':
                case '\u2007':
                case '\u202F':
                case '\u2009':
                    builder.Append(flavor == PlainTextFlavor.Exact ? '\u202F' : ' ');
                    changed = true;
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }

        return changed ? builder.ToString() : value;
    }

    private static string RemoveInvisibleCharacters(string value) {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value) {
            switch (ch) {
                case '\u200B':
                case '\u200C':
                case '\u200D':
                case '\u200E':
                case '\u200F':
                case '\u2060':
                case '\uFEFF':
                case '\u00AD':
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }
        return builder.ToString();
    }

    private static string CollapsePlainWhitespace(string value, PlainTextFlavor flavor) {
        var builder = new StringBuilder(value.Length);
        var previousWasSpace = false;

        foreach (var ch in value) {
            if (flavor == PlainTextFlavor.Exact && ShouldPreserveExactWhitespace(ch)) {
                builder.Append(ch);
                previousWasSpace = false;
                continue;
            }

            if (IsNonBreakingSpace(ch) && flavor == PlainTextFlavor.Exact) {
                builder.Append(ch);
                previousWasSpace = false;
                continue;
            }

            if (char.IsWhiteSpace(ch)) {
                if (!previousWasSpace) {
                    builder.Append(' ');
                    previousWasSpace = true;
                }
            } else {
                builder.Append(ch);
                previousWasSpace = false;
            }
        }

        return builder.ToString();
    }

    private static string TrimPlainWhitespace(string value, PlainTextFlavor flavor) {
        if (value.Length == 0) {
            return value;
        }

        var start = 0;
        while (start < value.Length && IsTrimmableWhitespace(value[start], flavor)) {
            start++;
        }

        var end = value.Length - 1;
        while (end >= start && IsTrimmableWhitespace(value[end], flavor)) {
            end--;
        }

        return start == 0 && end == value.Length - 1
            ? value
            : value[start..(end + 1)];
    }

    private static bool IsTrimmableWhitespace(char ch, PlainTextFlavor flavor) {
        if (flavor == PlainTextFlavor.Exact && IsNonBreakingSpace(ch)) {
            return false;
        }
        if (flavor == PlainTextFlavor.Exact && ShouldPreserveExactWhitespace(ch)) {
            return false;
        }
        return char.IsWhiteSpace(ch);
    }

    private static bool IsNonBreakingSpace(char ch) => ch == '\u00A0' || ch == '\u202F' || ch == '\u2007';

    private static bool ShouldPreserveExactWhitespace(char ch) => ch is '\u2028' or '\u2029' or '\u0085' or '\u000B' or '\u200A';

    // needed to preserve exact plain text output; don't use for friendly plain text
    private static string EncodeReservedCharacters(string value) {
        if (value.Length == 0) {
            return value;
        }

        var builder = new StringBuilder(value.Length);
        foreach (var ch in value) {
            switch (ch) {
                case '&':
                    builder.Append("&amp;");
                    break;
                case '<':
                    builder.Append("&lt;");
                    break;
                case '>':
                    builder.Append("&gt;");
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }
        return builder.ToString();
    }

    private static string DecodeNumericEntities(string value) {
        if (string.IsNullOrEmpty(value)) {
            return value;
        }

        return NumericEntityRegex.Replace(value, match => {
            if (match.Groups["dec"].Success && int.TryParse(match.Groups["dec"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var decCode)) {
                return ConvertFromCodePoint(decCode) ?? match.Value;
            }

            if (match.Groups["hex"].Success && int.TryParse(match.Groups["hex"].Value, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var hexCode)) {
                return ConvertFromCodePoint(hexCode) ?? match.Value;
            }

            return match.Value;
        });
    }

    private static string? ConvertFromCodePoint(int codePoint) {
        if (codePoint <= 0 || codePoint > 0x10FFFF) {
            return null;
        }

        return char.ConvertFromUtf32(codePoint);
    }
}
