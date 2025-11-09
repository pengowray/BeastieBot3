using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3;

internal static class IucnTextUtilities {
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
