using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

// Small HTML helpers for the static audit site: escaping, a whitespace visualiser (so cleanup
// findings can show otherwise-invisible characters), text truncation, and a tiny Markdown
// subset for the neutral descriptions and the commentary file. No external Markdown dependency.

namespace BeastieBot3.Audit.Rendering;

internal static class HtmlText {
    public static string Escape(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return "";
        }
        var sb = new StringBuilder(value.Length + 16);
        foreach (var c in value) {
            switch (c) {
                case '&': sb.Append("&amp;"); break;
                case '<': sb.Append("&lt;"); break;
                case '>': sb.Append("&gt;"); break;
                case '"': sb.Append("&quot;"); break;
                case '\'': sb.Append("&#39;"); break;
                default: sb.Append(c); break;
            }
        }
        return sb.ToString();
    }

    // Renders a value with otherwise-invisible characters made visible: a middle dot for spaces,
    // explicit tokens for NBSP / tab / newline. Used in cleanup "current value" cells so a stray
    // double space or non-breaking space is apparent. The result is already HTML-escaped.
    public static string Visualise(string? value) {
        if (string.IsNullOrEmpty(value)) {
            return "<span class=\"ws-empty\">(empty)</span>";
        }
        var sb = new StringBuilder();
        foreach (var c in value) {
            switch (c) {
                case ' ': sb.Append("<span class=\"ws\">·</span>"); break;
                case ' ': sb.Append("<span class=\"ws ws-bad\" title=\"non-breaking space\">⍽</span>"); break;
                case '\t': sb.Append("<span class=\"ws ws-bad\" title=\"tab\">→</span>"); break;
                case '\r': sb.Append("<span class=\"ws ws-bad\" title=\"carriage return\">↵</span>"); break;
                case '\n': sb.Append("<span class=\"ws ws-bad\" title=\"newline\">¶</span>"); break;
                case '&': sb.Append("&amp;"); break;
                case '<': sb.Append("&lt;"); break;
                case '>': sb.Append("&gt;"); break;
                case '"': sb.Append("&quot;"); break;
                case '\'': sb.Append("&#39;"); break;
                default: sb.Append(c); break;
            }
        }
        return sb.ToString();
    }

    public static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..Math.Max(0, max - 1)].TrimEnd() + "…";

    // True when a raw (un-escaped) href targets a safe scheme: http(s), mailto, a relative path, or
    // a fragment. Blocks javascript:/data:/vbscript: even if the value is otherwise attribute-safe.
    public static bool IsSafeHref(string? url) {
        if (string.IsNullOrWhiteSpace(url)) {
            return false;
        }
        var u = url.Trim();
        if (u.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            u.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
            u.StartsWith("mailto:", StringComparison.OrdinalIgnoreCase) ||
            u.StartsWith("/", StringComparison.Ordinal) ||
            u.StartsWith("./", StringComparison.Ordinal) ||
            u.StartsWith("#", StringComparison.Ordinal)) {
            return true;
        }
        // A relative path with no scheme (no colon before the first slash) is safe.
        var slash = u.IndexOf('/');
        var colon = u.IndexOf(':');
        return colon < 0 || (slash >= 0 && slash < colon);
    }

    // A deliberately small Markdown subset for descriptions and commentary:
    //   blank-line separated paragraphs, "## " / "### " headings, "- " unordered lists,
    //   "> " blockquotes, **bold**, *italic*/_italic_, `code`, [text](url).
    // Input is escaped before inline formatting is applied, so raw HTML cannot leak through.
    // Links are restricted to http(s) and relative targets.
    public static string Markdown(string? markdown) {
        if (string.IsNullOrWhiteSpace(markdown)) {
            return "";
        }

        var lines = markdown.Replace("\r\n", "\n").Replace("\r", "\n").Split('\n');
        var html = new StringBuilder();
        var paragraph = new List<string>();
        var listItems = new List<string>();
        var quoteLines = new List<string>();

        void FlushParagraph() {
            if (paragraph.Count == 0) {
                return;
            }
            html.Append("<p>").Append(Inline(string.Join(" ", paragraph))).Append("</p>\n");
            paragraph.Clear();
        }
        void FlushList() {
            if (listItems.Count == 0) {
                return;
            }
            html.Append("<ul>\n");
            foreach (var item in listItems) {
                html.Append("<li>").Append(Inline(item)).Append("</li>\n");
            }
            html.Append("</ul>\n");
            listItems.Clear();
        }
        void FlushQuote() {
            if (quoteLines.Count == 0) {
                return;
            }
            html.Append("<blockquote>").Append(Inline(string.Join(" ", quoteLines))).Append("</blockquote>\n");
            quoteLines.Clear();
        }
        void FlushAll() { FlushParagraph(); FlushList(); FlushQuote(); }

        foreach (var raw in lines) {
            var line = raw.TrimEnd();
            if (line.Length == 0) {
                FlushAll();
                continue;
            }
            if (line.StartsWith("### ", StringComparison.Ordinal)) {
                FlushAll();
                html.Append("<h4>").Append(Inline(line[4..])).Append("</h4>\n");
            } else if (line.StartsWith("## ", StringComparison.Ordinal)) {
                FlushAll();
                html.Append("<h3>").Append(Inline(line[3..])).Append("</h3>\n");
            } else if (line.StartsWith("- ", StringComparison.Ordinal)) {
                FlushParagraph(); FlushQuote();
                listItems.Add(line[2..]);
            } else if (line.StartsWith("> ", StringComparison.Ordinal)) {
                FlushParagraph(); FlushList();
                quoteLines.Add(line[2..]);
            } else {
                FlushList(); FlushQuote();
                paragraph.Add(line);
            }
        }
        FlushAll();
        return html.ToString();
    }

    private static string Inline(string text) {
        var s = Escape(text);
        // [text](url) -> anchor, restricted to safe schemes.
        s = Regex.Replace(s, @"\[([^\]]+)\]\(([^)\s]+)\)", m => {
            var label = m.Groups[1].Value;
            var url = m.Groups[2].Value;
            return SafeUrl(url) is { } safe
                ? $"<a href=\"{safe}\" rel=\"noopener\">{label}</a>"
                : label;
        });
        s = Regex.Replace(s, @"\*\*([^*]+)\*\*", "<strong>$1</strong>");
        s = Regex.Replace(s, @"(?<!\w)_([^_]+)_(?!\w)", "<em>$1</em>");
        s = Regex.Replace(s, @"\*([^*]+)\*", "<em>$1</em>");
        s = Regex.Replace(s, @"`([^`]+)`", "<code>$1</code>");
        return s;
    }

    private static string? SafeUrl(string url) {
        // url here is already HTML-escaped, so &amp; etc. may be present; compare on a decoded copy.
        var probe = url.Replace("&amp;", "&");
        if (probe.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            probe.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
            probe.StartsWith("/", StringComparison.Ordinal) ||
            probe.StartsWith("./", StringComparison.Ordinal) ||
            probe.StartsWith("#", StringComparison.Ordinal) ||
            (!probe.Contains(':') && (probe.EndsWith(".html", StringComparison.OrdinalIgnoreCase) || probe.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)))) {
            return url;
        }
        return null;
    }
}
