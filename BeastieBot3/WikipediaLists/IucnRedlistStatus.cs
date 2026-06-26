using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

// Maps IUCN Red List category codes to display names and sort order.
// Categories: EX (Extinct), EW, CR, CR(PE), CR(PEW), EN, VU, NT, LC, DD, NE.
// Provides TriStateFilter for possibly-extinct flags (PE, PEW).
// Used by WikipediaListGenerator for status display and ordering.

namespace BeastieBot3.WikipediaLists;

internal static class IucnRedlistStatus {
    private static readonly Dictionary<string, RedlistStatusDescriptor> Map = new(StringComparer.OrdinalIgnoreCase) {
        ["EX"] = new("EX", "EX", "Extinct", "Extinct"),
        ["EW"] = new("EW", "EW", "Extinct in the wild", "Extinct in the Wild"),
        ["CR(PE)"] = new("CR(PE)", "CR", "Possibly extinct", "Critically Endangered", TriStateFilter.True, TriStateFilter.False),
        ["CR(PEW)"] = new("CR(PEW)", "CR", "Possibly extinct in the wild", "Critically Endangered", TriStateFilter.False, TriStateFilter.True),
        ["CR"] = new("CR", "CR", "Critically endangered", "Critically Endangered", TriStateFilter.False, TriStateFilter.False),
        ["EN"] = new("EN", "EN", "Endangered", "Endangered"),
        ["VU"] = new("VU", "VU", "Vulnerable", "Vulnerable"),
        ["NT"] = new("NT", "NT", "Near threatened", "Near Threatened"),
        ["LC"] = new("LC", "LC", "Least concern", "Least Concern"),
        ["DD"] = new("DD", "DD", "Data deficient", "Data Deficient"),
        ["LR/lc"] = new("LR/lc", "LR/lc", "Lower risk/least concern", "Lower Risk/least concern"),
        ["LR/nt"] = new("LR/nt", "LR/nt", "Lower risk/near threatened", "Lower Risk/near threatened"),
        ["LR/cd"] = new("LR/cd", "LR/cd", "Conservation dependent", "Lower Risk/conservation dependent"),
        ["NA"] = new("NA", "NA", "Not applicable", "Not Applicable"),
        ["RE"] = new("RE", "RE", "Regionally extinct", "Regionally Extinct")
    };

    private static readonly IReadOnlyList<RedlistStatusDescriptor> AllDescriptors = Map.Values.ToList();

    public static RedlistStatusDescriptor Describe(string? code) {
        if (TryGetDescriptor(code, out var descriptor)) {
            return descriptor;
        }

        var normalized = Normalize(code ?? string.Empty);
        if (TryGetDescriptor(normalized, out descriptor)) {
            return descriptor;
        }

        var fallback = normalized.Trim();
        return new RedlistStatusDescriptor(fallback, fallback, fallback, fallback);
    }

    public static bool TryGetDescriptor(string? code, [NotNullWhen(true)] out RedlistStatusDescriptor? descriptor) {
        descriptor = null;
        if (string.IsNullOrWhiteSpace(code)) {
            return false;
        }

        if (Map.TryGetValue(code, out var direct)) {
            descriptor = direct;
            return true;
        }

        var normalized = Normalize(code);
        if (Map.TryGetValue(normalized, out var normalizedDescriptor)) {
            descriptor = normalizedDescriptor;
            return true;
        }

        return false;
    }

    public static RedlistStatusDescriptor ResolveFromDatabase(string category, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        foreach (var descriptor in AllDescriptors) {
            if (MatchesDescriptor(descriptor, category, possiblyExtinct, possiblyExtinctInTheWild)) {
                return descriptor;
            }
        }

        var fallback = category.Trim();
        return new RedlistStatusDescriptor(fallback, fallback, fallback, fallback);
    }

    private static bool MatchesDescriptor(RedlistStatusDescriptor descriptor, string category, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        if (!descriptor.Category.Equals(category, StringComparison.OrdinalIgnoreCase)) {
            return false;
        }

        if (!MatchesTriState(descriptor.PossiblyExtinctFilter, possiblyExtinct)) {
            return false;
        }

        if (!MatchesTriState(descriptor.PossiblyExtinctInTheWildFilter, possiblyExtinctInTheWild)) {
            return false;
        }

        return true;
    }

    private static bool MatchesTriState(TriStateFilter filter, string? raw) {
        if (filter == TriStateFilter.Any) {
            return true;
        }

        var value = IsTrue(raw);
        return filter == TriStateFilter.True ? value : !value;
    }

    private static bool IsTrue(string? raw) => !string.IsNullOrWhiteSpace(raw) && raw.Equals("true", StringComparison.OrdinalIgnoreCase);

    private static string Normalize(string raw) {
        var trimmed = raw.Trim();
        var paren = trimmed.IndexOf('(');
        if (paren > 0) {
            return trimmed[..paren];
        }

        return trimmed;
    }

    /// <summary>
    /// Builds the full <c>{{IUCN status|CODE|taxonId/assessmentId|1|year=YYYY}}</c> template, mapping the
    /// base code + PE/PEW flags to the Wikipedia template code (CR → CR(PE)/CR(PEW)) and omitting the year
    /// for extinct categories. Shared by the IUCN lists and the SPRAT Australia lists so both emit a
    /// byte-identical badge.
    /// </summary>
    public static string BuildStatusTemplate(string baseCode, string? possiblyExtinct, string? possiblyExtinctInTheWild,
        long taxonId, long assessmentId, string? yearPublished, bool yearAsBareLabel = false) {
        var statusCode = ToWikipediaTemplateCode(baseCode, possiblyExtinct, possiblyExtinctInTheWild);
        var sb = new StringBuilder();
        sb.Append("{{IUCN status|").Append(statusCode).Append('|')
          .Append(taxonId).Append('/').Append(assessmentId).Append("|1"); // 1 = make link visible
        if (!IsExtinctTemplateCode(statusCode) && !string.IsNullOrWhiteSpace(yearPublished)) {
            // year= renders the link as "IUCN <year>"; label= renders just "<year>". The bare-label form
            // is used where the surrounding text already says "IUCN:" (the SPRAT Australia lists), so the
            // "IUCN" prefix would be redundant; the standalone IUCN lists keep "IUCN <year>".
            sb.Append(yearAsBareLabel ? "|label=" : "|year=").Append(yearPublished);
        }
        sb.Append("}}");
        return sb.ToString();
    }

    /// <summary>
    /// Maps an IUCN status code to its Wikipedia {{IUCN status}} template code. Uses the PE/PEW database
    /// flags for CR species to produce CR(PE)/CR(PEW), and maps legacy LR/* codes (LR/cd stays LR/cd).
    /// </summary>
    public static string ToWikipediaTemplateCode(string code, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        var normalized = code.ToUpperInvariant();

        if (normalized == "CR" || normalized == "CRITICALLY ENDANGERED") {
            if (string.Equals(possiblyExtinct, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PE)";
            }
            if (string.Equals(possiblyExtinctInTheWild, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PEW)";
            }
            return "CR";
        }

        return normalized switch {
            "CR(PE)" or "PE" => "CR(PE)",
            "CR(PEW)" or "PEW" => "CR(PEW)",
            "LR/CD" or "CD" => "LR/cd",
            "LR/NT" => "LR/nt",
            "LR/LC" => "LR/lc",
            _ => normalized
        };
    }

    private static bool IsExtinctTemplateCode(string code) => code.ToUpperInvariant() is "EX" or "EW";
}

internal sealed record RedlistStatusDescriptor(
    string Code,
    string TemplateName,
    string? Label,
    string Category,
    TriStateFilter PossiblyExtinctFilter = TriStateFilter.Any,
    TriStateFilter PossiblyExtinctInTheWildFilter = TriStateFilter.Any);

internal enum TriStateFilter {
    Any,
    True,
    False
}
