using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace BeastieBot3;

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

        var builder = new StringBuilder();
        var depth = 0;
        for (var i = index; i < text.Length; i++) {
            if (i + 1 < text.Length && text[i] == '{' && text[i + 1] == '{') {
                depth++;
                i++;
                if (depth == 1) {
                    continue;
                }
            }
            else if (i + 1 < text.Length && text[i] == '}' && text[i + 1] == '}') {
                depth--;
                i++;
                if (depth == 0) {
                    break;
                }
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

    private static Dictionary<string, string> ParseFields(string template) {
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var lines = template.Split('\n');
        string? currentKey = null;
        var currentValue = new StringBuilder();

        void Commit() {
            if (currentKey is null) {
                return;
            }

            var value = currentValue.ToString().Trim();
            fields[currentKey] = value;
            currentKey = null;
            currentValue.Clear();
        }

        foreach (var rawLine in lines) {
            var line = rawLine.TrimEnd();
            if (line.Length == 0) {
                continue;
            }

            if (line.StartsWith("|", StringComparison.Ordinal)) {
                Commit();
                var eqIndex = line.IndexOf('=');
                if (eqIndex < 0) {
                    continue;
                }

                currentKey = line[1..eqIndex].Trim();
                var value = line[(eqIndex + 1)..].Trim();
                currentValue.Append(value);
            }
            else if (currentKey is not null) {
                if (currentValue.Length > 0) {
                    currentValue.Append(' ');
                }
                currentValue.Append(line.Trim());
            }
        }

        Commit();
        return fields;
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
