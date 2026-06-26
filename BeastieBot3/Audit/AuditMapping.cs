using System;
using System.Globalization;
using System.Net;
using BeastieBot3.WikipediaLists;

// Small mapping helpers shared by every producer: deriving rank/full-species from the
// infraType + subpopulationName pair (the same rule as TaxonFilterSql.GlobalSpeciesPredicate),
// normalising a status to a short code, and a sort key that orders status from most to least
// threatened. Centralised so the producers stay consistent and the column library can reuse them.

namespace BeastieBot3.Audit;

internal static class AuditMapping {
    public static (string Rank, bool IsFullSpecies) Rank(string? infraType, string? subpopulationName) {
        var hasSub = !string.IsNullOrWhiteSpace(subpopulationName);
        var hasInfra = !string.IsNullOrWhiteSpace(infraType);
        if (hasSub) {
            return ("subpopulation", false);
        }
        if (hasInfra) {
            var t = infraType!.Trim().ToLowerInvariant();
            return t.Contains("var") ? ("variety", false) : ("subspecies", false);
        }
        return ("species", true);
    }

    // From the CSV full-text category ("Critically Endangered"), folding PE/PEW flags.
    public static string? CodeFromCategory(string? category, string? possiblyExtinct = null, string? possiblyExtinctInTheWild = null) {
        if (string.IsNullOrWhiteSpace(category)) {
            return null;
        }
        return IucnRedlistStatus.ResolveFromDatabase(category, possiblyExtinct, possiblyExtinctInTheWild).Code;
    }

    // From a short API code ("CR", "EN", ...). Returns the canonical code.
    public static string? CodeFromCode(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return null;
        }
        return IucnRedlistStatus.Describe(code).Code;
    }

    public static string? CategoryText(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return null;
        }
        return IucnRedlistStatus.Describe(code).Category;
    }

    // Two-digit sort key so ascending order runs most-threatened to least.
    public static string StatusSortKey(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return "99";
        }
        var order = code.ToUpperInvariant() switch {
            "EX" => 0,
            "EW" => 1,
            "RE" => 2,
            "CR(PE)" => 3,
            "CR(PEW)" => 4,
            "CR" => 5,
            "EN" => 6,
            "VU" => 7,
            "NT" => 8,
            "LR/CD" => 9,
            "LR/NT" => 10,
            "LC" => 11,
            "LR/LC" => 12,
            "DD" => 13,
            "NE" => 14,
            "NA" => 15,
            _ => 90,
        };
        return order.ToString("D2", CultureInfo.InvariantCulture);
    }

    // The IUCN _html view stores '&', '<', '>' as HTML entities. Decode them for display and for
    // comparison so the audit shows real text (e.g. "Brandt & Ratzeburg" rather than the literal
    // "Brandt &amp; Ratzeburg") and does not report spurious differences.
    public static string? Decode(string? value) =>
        string.IsNullOrEmpty(value) ? value : WebUtility.HtmlDecode(value);

    public static string? LongToString(long? value) =>
        value?.ToString(CultureInfo.InvariantCulture);

    public static string BoolToYesNo(bool? value) => value switch {
        true => "yes",
        false => "no",
        null => "",
    };
}
