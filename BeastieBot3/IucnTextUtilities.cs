using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3;

internal static class IucnTextUtilities {
    private static readonly Regex BlockTagRegex = new("</?(?:p|br|div|li|ul|ol|table|tr|td|th|blockquote|section|article|h[1-6])[^>]*>", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex TagRegex = new("</?[^>]+?>", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex NumericEntityRegex = new("&#(?:(?<dec>[0-9]+)|x(?<hex>[0-9a-fA-F]+));", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static string ResolveDatabasePath(string? overridePath, PathsService paths) {
        var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
            ? overridePath
            : paths.GetIucnDatabasePath();

        if (string.IsNullOrWhiteSpace(configuredPath)) {
            throw new InvalidOperationException("IUCN SQLite database path is not configured. Set Datastore:IUCN_sqlite_from_cvs or pass --database.");
        }

        try {
            return Path.GetFullPath(configuredPath);
        }
        catch (Exception ex) {
            throw new InvalidOperationException($"Failed to resolve database path {configuredPath}: {ex.Message}", ex);
        }
    }

    public static string? ConvertHtmlToPlainText(string? html) {
        if (html is null) {
            return null;
        }

        var decoded = WebUtility.HtmlDecode(html);
        if (string.IsNullOrEmpty(decoded)) {
            return decoded;
        }

        decoded = NormalizeLineEndings(decoded);
        decoded = BlockTagRegex.Replace(decoded, "\n");
        decoded = TagRegex.Replace(decoded, string.Empty);
        decoded = decoded.Replace('\u00A0', ' ').Replace('\u202F', ' ').Replace('\u2007', ' ');
        return RemoveInvisibleCharacters(decoded);
    }

    public static string? ConvertHtmlToExactPlainText(string? html) {
        if (html is null) {
            return null;
        }

        var working = NormalizeLineEndings(html);
        working = BlockTagRegex.Replace(working, string.Empty);
        working = TagRegex.Replace(working, string.Empty);
        working = DecodeEntitiesForExactPlain(working);
        working = RemoveInvisibleCharacters(working);
        return CollapsePlainWhitespace(working);
    }

    public static string? NormalizePlainTextExact(string? value) {
        if (value is null) {
            return null;
        }

        var normalized = NormalizeLineEndings(value);
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
        var normalized = value.Replace("\r", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal);
        if (normalized.Length <= maxLength) {
            return normalized;
        }
        return normalized[..maxLength] + "…";
    }

    private static string NormalizeLineEndings(string value) {
        return value.Replace("\r\n", "\n", StringComparison.Ordinal)
                    .Replace('\r', '\n');
    }

    private static string RemoveInvisibleCharacters(string value) {
        return value.Replace("\u200B", string.Empty, StringComparison.Ordinal)
                    .Replace("\u200C", string.Empty, StringComparison.Ordinal)
                    .Replace("\u200D", string.Empty, StringComparison.Ordinal)
                    .Replace("\uFEFF", string.Empty, StringComparison.Ordinal);
    }

    private static string CollapsePlainWhitespace(string value) {
        var builder = new StringBuilder(value.Length);
        var previousWasSpace = false;

        foreach (var ch in value) {
            if (ch == '\u00A0') {
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

        return TrimPlainWhitespace(builder.ToString());
    }

    private static string TrimPlainWhitespace(string value) {
        if (value.Length == 0) {
            return value;
        }

        var start = 0;
        while (start < value.Length && IsTrimmablePlainChar(value[start])) {
            start++;
        }

        var end = value.Length - 1;
        while (end >= start && IsTrimmablePlainChar(value[end])) {
            end--;
        }

        return start == 0 && end == value.Length - 1
            ? value
            : value[start..(end + 1)];
    }

    private static bool IsTrimmablePlainChar(char ch) {
        return ch != '\u00A0' && char.IsWhiteSpace(ch);
    }

    private static string DecodeEntitiesForExactPlain(string value) {
        if (string.IsNullOrEmpty(value)) {
            return value;
        }

        // Handle common named entities that the plain-text columns materialize as characters.
        var result = value.Replace("&nbsp;", "\u00A0", StringComparison.Ordinal)
                          .Replace("&thinsp;", "\u202F", StringComparison.Ordinal)
                          .Replace("&ensp;", "\u2002", StringComparison.Ordinal)
                          .Replace("&emsp;", "\u2003", StringComparison.Ordinal);

        // Convert numeric entities while leaving named entities like &gt; untouched so we preserve the stored form.
        result = NumericEntityRegex.Replace(result, match => {
            var decGroup = match.Groups["dec"];
            if (decGroup.Success && int.TryParse(decGroup.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var decCode)) {
                return ConvertFromCodePoint(decCode) ?? match.Value;
            }

            var hexGroup = match.Groups["hex"];
            if (hexGroup.Success && int.TryParse(hexGroup.Value, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var hexCode)) {
                return ConvertFromCodePoint(hexCode) ?? match.Value;
            }

            return match.Value;
        });

        return result;
    }

    private static string? ConvertFromCodePoint(int codePoint) {
        if (codePoint <= 0) {
            return null;
        }

        if (codePoint <= 0x10FFFF) {
            return char.ConvertFromUtf32(codePoint);
        }

        return null;
    }
}
