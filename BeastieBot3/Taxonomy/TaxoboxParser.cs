using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Wikipedia;

// Extracts taxonomy data from Wikipedia wikitext taxobox templates. Supported
// templates: {{Taxobox}}, {{Automatic taxobox}}, {{Speciesbox}}, {{Subspeciesbox}},
// {{Insectbox}}, {{Plantbox}}, {{Fishbox}}, {{Birdbox}}. Extracts: taxon, name,
// genus, species, binomial, parent, familia, classis. Used by Wikipedia cache
// commands for matching species articles to IUCN taxa. TemplateNames is the single
// source of truth -- WikipediaPageFetcher.HasTaxobox iterates the same list so the
// has_taxobox flag and the parser can't drift apart.

namespace BeastieBot3.Taxonomy;

internal static class TaxoboxParser {
    private static readonly string[] TemplateCandidates = {
        "taxobox",
        "automatic taxobox",
        "speciesbox",
        "subspeciesbox",
        "insectbox",
        "plantbox",
        "fishbox",
        "birdbox"
    };

    /// <summary>The taxobox template names this parser recognises (lowercase, no braces).</summary>
    internal static IReadOnlyList<string> TemplateNames => TemplateCandidates;

    public static WikiTaxoboxData? TryParse(long pageRowId, string? wikitext) {
        if (string.IsNullOrWhiteSpace(wikitext)) {
            return null;
        }

        foreach (var template in TemplateCandidates) {
            var snippet = ExtractTemplate(wikitext, template);
            if (snippet is null) {
                continue;
            }

            var fields = ParseFields(snippet.Value.TemplateText);
            if (fields.Count == 0) {
                return null;
            }

            var scientific = GetFirst(fields, "taxon", "name", "binomial", "scientific_name");
            var rank = DetermineRank(snippet.Value.TemplateName, fields, scientific);
            var kingdom = GetFirst(fields, "kingdom", "regnum");
            var phylum = GetFirst(fields, "phylum", "divisio", "division");
            var className = GetFirst(fields, "class", "classis");
            var orderName = GetFirst(fields, "order", "ordo");
            var family = GetFirst(fields, "family", "familia");
            var subfamily = GetFirst(fields, "subfamily", "subfamilia");
            var tribe = GetFirst(fields, "tribe", "tribus");
            var genus = GetFirst(fields, "genus");
            var species = GetFirst(fields, "species");
            if (string.IsNullOrWhiteSpace(species)) {
                species = TryInferSpecies(scientific);
            }

            var isMonotypic = ParseBoolean(GetFirst(fields, "monotypic"));
            var dataJson = JsonSerializer.Serialize(fields);

            return new WikiTaxoboxData(
                pageRowId,
                scientific,
                rank,
                kingdom,
                phylum,
                className,
                orderName,
                family,
                subfamily,
                tribe,
                genus,
                species,
                isMonotypic,
                dataJson);
        }

        return null;
    }

    private static (string TemplateName, string TemplateText)? ExtractTemplate(string text, string templateName) {
        var index = CultureInfo.InvariantCulture.CompareInfo
            .IndexOf(text, "{{" + templateName, CompareOptions.IgnoreCase);
        if (index < 0) {
            return null;
        }

        // The body between the taxobox's own braces. A nested template keeps both of its braces
        // ("{{sfn|Groves|2005}}"); it used to keep only one ("{sfn|Groves|2005}").
        var builder = new StringBuilder();
        var depth = 0;
        for (var i = index; i < text.Length; i++) {
            if (i + 1 < text.Length && text[i] == '{' && text[i + 1] == '{') {
                depth++;
                i++;
                if (depth > 1) {
                    builder.Append("{{");
                }
                continue;
            }
            if (i + 1 < text.Length && text[i] == '}' && text[i + 1] == '}') {
                depth--;
                i++;
                if (depth == 0) {
                    break;
                }
                builder.Append("}}");
                continue;
            }

            if (depth >= 1) {
                builder.Append(text[i]);
            }
        }

        if (builder.Length == 0) {
            return null;
        }

        var templateBody = builder.ToString();
        var nameEnd = templateBody.IndexOfAny(new[] { '|', '\n' });
        var name = nameEnd > 0
            ? templateBody[..nameEnd].Trim()
            : templateName;

        return (name, templateBody);
    }

    // The template's named parameters. Parameters are separated by the "|" characters that are
    // outside nested templates, links, comments and <ref>/<nowiki> elements, the way MediaWiki
    // splits them, so "| name = Downy oak| image = ..." on one line and a parameter on a line
    // starting " |" are both found. A parameter without "=" (a positional one) is skipped. A
    // value's lines are trimmed and joined with single spaces, and empty lines are dropped.
    private static Dictionary<string, string> ParseFields(string template) {
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var segments = SplitTopLevel(template, '|');
        // segments[0] is the template's name.
        for (var s = 1; s < segments.Count; s++) {
            var segment = segments[s];
            var eqIndex = IndexOfTopLevel(segment, '=', 0);
            if (eqIndex < 0) {
                continue;
            }
            fields[segment[..eqIndex].Trim()] = JoinLines(segment[(eqIndex + 1)..]);
        }
        return fields;
    }

    private static string JoinLines(string value) {
        var builder = new StringBuilder();
        foreach (var rawLine in value.Split('\n')) {
            var line = rawLine.Trim();
            if (line.Length == 0) {
                continue;
            }
            if (builder.Length > 0) {
                builder.Append(' ');
            }
            builder.Append(line);
        }
        return builder.ToString();
    }

    private static List<string> SplitTopLevel(string text, char separator) {
        var parts = new List<string>();
        var start = 0;
        int index;
        while ((index = IndexOfTopLevel(text, separator, start)) >= 0) {
            parts.Add(text[start..index]);
            start = index + 1;
        }
        parts.Add(text[start..]);
        return parts;
    }

    private static readonly Regex OpaqueTagStart = new(@"\G<(ref|nowiki)\b[^>]*?(/?)>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    // The first <paramref name="c"/> at or after <paramref name="start"/> that is outside "{{...}}",
    // "[[...]]", "<!--...-->" and a closed <ref>...</ref> or <nowiki>...</nowiki>; -1 when none.
    // An unclosed comment runs to the end of the text; an unclosed <ref> is ordinary text, as in
    // MediaWiki.
    private static int IndexOfTopLevel(string text, char c, int start) {
        var braces = 0;
        var brackets = 0;
        var i = start;
        while (i < text.Length) {
            if (string.CompareOrdinal(text, i, "<!--", 0, 4) == 0) {
                var end = text.IndexOf("-->", i + 4, StringComparison.Ordinal);
                i = end < 0 ? text.Length : end + 3;
                continue;
            }
            if (text[i] == '<' && OpaqueTagStart.Match(text, i) is { Success: true } tag) {
                var afterTag = i + tag.Length;
                if (tag.Groups[2].Value == "/") {
                    i = afterTag;
                    continue;
                }
                var close = text.IndexOf("</" + tag.Groups[1].Value, afterTag, StringComparison.OrdinalIgnoreCase);
                var closeEnd = close < 0 ? -1 : text.IndexOf('>', close);
                if (closeEnd >= 0) {
                    i = closeEnd + 1;
                    continue;
                }
            }
            if (i + 1 < text.Length) {
                var pair = text.AsSpan(i, 2);
                if (pair.SequenceEqual("{{")) { braces++; i += 2; continue; }
                if (pair.SequenceEqual("[[")) { brackets++; i += 2; continue; }
                if (pair.SequenceEqual("}}") && braces > 0) { braces--; i += 2; continue; }
                if (pair.SequenceEqual("]]") && brackets > 0) { brackets--; i += 2; continue; }
            }
            if (text[i] == c && braces == 0 && brackets == 0) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private static string? GetFirst(Dictionary<string, string> fields, params string[] keys) {
        foreach (var key in keys) {
            if (fields.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value)) {
                return value.Trim();
            }
        }

        return null;
    }

    private static string? DetermineRank(string templateName, Dictionary<string, string> fields, string? scientific) {
        var rank = GetFirst(fields, "rank");
        if (!string.IsNullOrWhiteSpace(rank)) {
            return NormalizeRank(rank);
        }

        var normalizedTemplate = templateName?.Trim().ToLowerInvariant();
        if (!string.IsNullOrWhiteSpace(normalizedTemplate)) {
            if (normalizedTemplate.Contains("subspeciesbox", StringComparison.Ordinal)) {
                return "subspecies";
            }

            if (normalizedTemplate.Contains("speciesbox", StringComparison.Ordinal)) {
                return "species";
            }

            if (normalizedTemplate.Contains("genusbox", StringComparison.Ordinal)) {
                return "genus";
            }
        }

        if (!string.IsNullOrWhiteSpace(scientific)) {
            var parts = scientific.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            return parts.Length switch {
                1 => "genus",
                2 => "species",
                3 => "subspecies",
                _ => null
            };
        }

        return null;
    }

    private static string? NormalizeRank(string value) {
        var lower = value.Trim().ToLowerInvariant();
        if (lower.Contains("subspecies", StringComparison.Ordinal) || lower.Contains("ssp", StringComparison.Ordinal)) {
            return "subspecies";
        }

        if (lower.Contains("species", StringComparison.Ordinal)) {
            return "species";
        }

        if (lower.Contains("genus", StringComparison.Ordinal)) {
            return "genus";
        }

        if (lower.Contains("family", StringComparison.Ordinal)) {
            return "family";
        }

        if (lower.Contains("order", StringComparison.Ordinal) || lower.Contains("ordo", StringComparison.Ordinal)) {
            return "order";
        }

        return lower;
    }

    private static string? TryInferSpecies(string? scientific) {
        if (string.IsNullOrWhiteSpace(scientific)) {
            return null;
        }

        var parts = scientific.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 2) {
            return string.Join(' ', parts[..2]);
        }

        return null;
    }

    private static bool? ParseBoolean(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch {
            "yes" or "y" or "true" or "1" => true,
            "no" or "n" or "false" or "0" => false,
            _ => null
        };
    }
}
