using System;
using System.Globalization;
using System.Text;

namespace BeastieBot3;

internal static class WikipediaTitleHelper {
    public static string Normalize(string? title) {
        if (string.IsNullOrWhiteSpace(title)) {
            return string.Empty;
        }

        var text = title.Trim();
        var hashIndex = text.IndexOf('#');
        if (hashIndex >= 0) {
            text = text[..hashIndex];
        }

        text = text.Replace('_', ' ');
        text = CollapseWhitespace(text);
        if (text.Length == 0) {
            return string.Empty;
        }

        var first = text[0];
        if (char.IsLetter(first)) {
            var builder = new StringBuilder(text.Length);
            builder.Append(char.ToUpper(first, CultureInfo.InvariantCulture));
            if (text.Length > 1) {
                builder.Append(text.AsSpan(1));
            }

            text = builder.ToString();
        }

        return text;
    }

    public static string ToSlug(string title) {
        var normalized = Normalize(title);
        if (normalized.Length == 0) {
            return string.Empty;
        }

        var underscored = normalized.Replace(' ', '_');
        return Uri.EscapeDataString(underscored);
    }

    private static string CollapseWhitespace(string value) {
        var builder = new StringBuilder(value.Length);
        var span = value.AsSpan();
        var pendingSpace = false;
        foreach (var ch in span) {
            if (char.IsWhiteSpace(ch)) {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace) {
                builder.Append(' ');
                pendingSpace = false;
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }
}
