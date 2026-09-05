using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
    // An open tag whose ">" never arrives: attributes run straight into the next tag. Spreadsheet
    // pastes produce these (a <span data-sheets-value=...> whose remaining text became attribute
    // names). Without this, every tag regex fails on it and the whole tag leaks into the plain text.
    private static readonly Regex BrokenOpenTagRegex = new($"<{TagNamePattern}\\b(?=\\s)(?:{AttributeFragment}=){AttributeFragment}(?=<)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    // Words trapped as valueless attribute names (`the="" emergence="" salamander=""`), the signature
    // of a spreadsheet paste that turned a paragraph into markup. Twelve in a row is well past anything
    // a real tag carries.
    private static readonly Regex TrappedWordsRegex = new("(?:\\s+[^\\s=\"'<>]+=\"\"){12,}", RegexOptions.Compiled | RegexOptions.CultureInvariant);
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

    // Inline elements that carry no text on their own. Several forms of these are redundant markup
    // that can be dropped without changing the rendered text: an instance wrapping only whitespace, a
    // run of identical instances nested inside one another, and a <span> that carries no styling at
    // all (bare, or only editor-noise attributes like tabindex/lang). The rich-text editor that
    // produced these narratives emits all of them — long runs of empty <span>s, deep stacks of
    // identical <span>s, and thousands of `<span tabindex="0" lang="en">` wrappers around every word.
    private const string InlineTagNames = "span|b|i|em|strong|u|s|strike|font|sub|sup|small|big|mark|a|label|abbr|acronym|cite|q|tt|ins|del|var|kbd|samp|o:p";
    private static readonly HashSet<string> InlineTagSet = new(
        InlineTagNames.Split('|'), StringComparer.OrdinalIgnoreCase);
    private static readonly Regex EmptyInlineTagRegex = new(
        "<(" + InlineTagNames + ")\\b" + AttributeFragment + ">((?:\\s|&nbsp;|&#160;|&#xA0;|\\u00A0|\\u200B|\\u200C|\\uFEFF)*)</\\1>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    // A single tag, split into close-marker, name, attributes and self-closing marker, used to walk
    // the markup and unwrap redundant inline nesting with a stack (regex alone cannot pair tags).
    private static readonly Regex TagRegex = new(
        "<(?<close>/?)(?<name>" + TagNamePattern + ")(?<attrs>" + AttributeFragment + ")>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline);
    private static readonly Regex AttrNameRegex = new(
        "(?<name>[A-Za-z_:][-\\w:.]*)\\s*(?:=\\s*(?<value>\"[^\"]*\"|'[^']*'|[^\\s\"'>]*))?",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex AnyWhitespaceRegex = new("\\s+", RegexOptions.Compiled);
    private static readonly Regex MultipleSpacesRegex = new("[ \\t]{2,}", RegexOptions.Compiled);
    private static readonly Regex BlankLineRunRegex = new("(?:\\n[ \\t]*){3,}", RegexOptions.Compiled);
    private static readonly Regex TrailingBreaksRegex = new("(?:<br\\b[^>]*>\\s*)+$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    // <span> attributes that carry no rendered styling, so a span whose attributes are only these
    // (or none at all) can be unwrapped entirely. tabindex/lang/dir are accessibility/locale hints
    // the editor sprays onto every wrapper; id and data-* are paste artefacts (Google Docs' guid
    // span, Google Sheets' data-sheets-value); an attribute with an empty value renders nothing
    // either. Dropping them leaves the readable text untouched.
    private static readonly HashSet<string> ValuelessSpanAttributes = new(StringComparer.OrdinalIgnoreCase) {
        "tabindex", "lang", "dir", "xml:lang", "role", "id",
    };
    private static readonly HashSet<string> StylingAttributes = new(StringComparer.OrdinalIgnoreCase) {
        "style", "class", "title", "hidden",
    };

    // Produces a suggested tidy of a narrative HTML field by removing redundant inline markup, then
    // tidying the whitespace left behind. Three kinds of redundancy are removed, none of which change
    // the rendered text: empty inline tags; inline tags nested inside an identical one (same name and
    // attributes, so the inner adds nothing); and <span>s that carry no styling at all (bare, or only
    // editor-noise attributes such as tabindex/lang). Stray zero-width and soft-hyphen characters are
    // deleted as well, so the suggestion does not leave invisible artefacts behind. Structural tags
    // (<p>, <br>, <li>, …) are left intact. It is still a best-effort suggestion: callers should
    // verify the extracted text still matches and note that identical rendering has not been checked.
    public static string? CleanRedundantMarkup(string? html) {
        if (string.IsNullOrEmpty(html)) {
            return html;
        }

        var working = NormalizeLineEndings(html);
        working = CommentRegex.Replace(working, string.Empty);
        working = RemoveInvisibleCharacters(working);
        working = BrokenOpenTagRegex.Replace(working, string.Empty);

        // Empty-tag removal can expose a newly-empty parent (an emptied wrapper), so repeat until
        // stable, bounded for safety. An empty tag that wrapped whitespace is replaced with a single
        // space, not nothing, so it cannot silently fuse the words on either side.
        working = RemoveEmptyInlineTags(working);

        // One stack walk unwraps redundant nesting at any depth: a duplicate-of-ancestor inline tag
        // and a styling-free <span> are dropped together with their matching close tag.
        working = UnwrapRedundantInlineTags(working);

        // The unwrap can leave a kept wrapper around nothing; sweep empties once more.
        working = RemoveEmptyInlineTags(working);

        working = MultipleSpacesRegex.Replace(working, " ");
        working = BlankLineRunRegex.Replace(working, "\n\n");
        // Line breaks at the very end of a field render nothing.
        working = TrailingBreaksRegex.Replace(working.Trim(), string.Empty);
        return working.Trim();
    }

    private static string RemoveEmptyInlineTags(string html) {
        // An empty tag is replaced by its own inner whitespace (group 2) rather than a generic space:
        // that still keeps any words on either side apart, but preserves the exact character — a
        // non-breaking or narrow space stays itself instead of silently becoming an ASCII space.
        for (var pass = 0; pass < 200; pass++) {
            var next = EmptyInlineTagRegex.Replace(html, m => m.Groups[2].Value);
            if (string.Equals(next, html, StringComparison.Ordinal)) {
                break;
            }
            html = next;
        }
        return html;
    }

    // Walks the markup keeping a stack of open inline tags. An inline open tag is dropped when an
    // identical one (same name and attributes) is already open above it, or when it is a styling-free
    // <span>; in either case its matching close tag is dropped too. Everything else — text, structural
    // tags, and inline tags that do carry styling — is passed through unchanged, so only tags that add
    // no rendered effect are removed. A close with no open is dropped, since it renders nothing.
    //
    // The output is always balanced, even when the source is not. These narratives routinely carry
    // far more opens than closes (Euonymus corymbosus' population field has 2,063 <span> opens to
    // 1,031 closes), and the suggestion is meant to be pasted back, so leaving a kept wrapper hanging
    // open would hand the reader broken markup. Kept tags left open by a mis-nested close are closed
    // before that close, and anything still open at the end is closed there — both are where a browser
    // implicitly closes them, so the rendered text is unchanged.
    private static string UnwrapRedundantInlineTags(string html) {
        var sb = new StringBuilder(html.Length);
        var stack = new List<(string Name, string Key, bool Dropped)>();
        var pos = 0;

        foreach (Match m in TagRegex.Matches(html)) {
            if (m.Index > pos) {
                sb.Append(html, pos, m.Index - pos);
            }
            pos = m.Index + m.Length;

            var name = m.Groups["name"].Value;
            var raw = m.Value;
            var isClose = m.Groups["close"].Value.Length > 0;
            var isSelfClose = raw.EndsWith("/>", StringComparison.Ordinal);

            if (!InlineTagSet.Contains(name) || isSelfClose) {
                // Structural/unknown tags and inline self-closing tags are not tracked; pass through.
                sb.Append(raw);
                continue;
            }

            if (!isClose) {
                var key = NormalizeTagKey(name, m.Groups["attrs"].Value);
                var drop = IsValuelessSpan(name, m.Groups["attrs"].Value);
                if (!drop) {
                    foreach (var fr in stack) {
                        if (string.Equals(fr.Key, key, StringComparison.Ordinal)) { drop = true; break; }
                    }
                }
                stack.Add((name.ToLowerInvariant(), key, drop));
                if (!drop) {
                    sb.Append(raw);
                }
                continue;
            }

            // Close tag: pop down to the nearest open of the same name. Any inner tags left unclosed
            // are discarded from tracking; their kept opens stay in the output (mirroring the source).
            var idx = -1;
            for (var k = stack.Count - 1; k >= 0; k--) {
                if (string.Equals(stack[k].Name, name, StringComparison.OrdinalIgnoreCase)) { idx = k; break; }
            }
            if (idx < 0) {
                // A close with no matching open renders nothing; dropping it also tidies the close
                // left behind when a broken open tag (see BrokenOpenTagRegex) was removed above.
                continue;
            }
            // Inner tags left unclosed by this mis-nested close (`<b><i></b>`) are closed here, in
            // innermost-first order, so the output stays balanced.
            for (var k = stack.Count - 1; k > idx; k--) {
                if (!stack[k].Dropped) {
                    sb.Append("</").Append(stack[k].Name).Append('>');
                }
            }
            var dropped = stack[idx].Dropped;
            stack.RemoveRange(idx, stack.Count - idx);
            if (!dropped) {
                sb.Append(raw);
            }
        }

        if (pos < html.Length) {
            sb.Append(html, pos, html.Length - pos);
        }

        // Whatever the source never closed, close now.
        for (var k = stack.Count - 1; k >= 0; k--) {
            if (!stack[k].Dropped) {
                sb.Append("</").Append(stack[k].Name).Append('>');
            }
        }
        return sb.ToString();
    }

    // A stable identity for an inline tag: lowercased name plus its attributes with runs of
    // whitespace collapsed, so cosmetic spacing differences do not defeat the duplicate-nesting check.
    private static string NormalizeTagKey(string name, string attributes) =>
        name.ToLowerInvariant() + "|" + AnyWhitespaceRegex.Replace(attributes.Trim(), " ");

    // True for a <span> that applies no styling: no attributes at all, or only attributes that have
    // no rendered effect (tabindex/lang/dir/role). Other inline tags (b, i, a, …) are never valueless.
    private static bool IsValuelessSpan(string name, string attributes) {
        if (!name.Equals("span", StringComparison.OrdinalIgnoreCase)) {
            return false;
        }
        var trimmed = attributes.Trim();
        if (trimmed.Length == 0) {
            return true;
        }
        var any = false;
        foreach (Match a in AttrNameRegex.Matches(trimmed)) {
            any = true;
            var attrName = a.Groups["name"].Value;
            if (StylingAttributes.Contains(attrName)) {
                return false;
            }
            var value = a.Groups["value"].Value.Trim('"', '\'');
            var valueless = ValuelessSpanAttributes.Contains(attrName)
                || attrName.StartsWith("data-", StringComparison.OrdinalIgnoreCase)
                || value.Length == 0;
            if (!valueless) {
                return false;
            }
        }
        return any;
    }

    // Words that survive only as attribute names, when a pasted tag has swallowed a paragraph
    // (`the="" emergence="" salamander=""`). Returns the run as readable words, or null when there is
    // none. The text is not part of the narrative in any export, so callers can only report it.
    public static string? TextTrappedInAttributes(string? html) {
        if (string.IsNullOrEmpty(html)) {
            return null;
        }
        var m = TrappedWordsRegex.Match(html);
        if (!m.Success) {
            return null;
        }
        var words = m.Value.Replace("=\"\"", string.Empty);
        words = WebUtility.HtmlDecode(words);
        return AnyWhitespaceRegex.Replace(words, " ").Trim();
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
        working = BrokenOpenTagRegex.Replace(working, string.Empty);
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
