using System;
using System.Globalization;
using System.Text.RegularExpressions;

// Reads the values in the cells of IUCN's summary tables: Red List categories with their Possibly
// Extinct tags ("CR (PE)", "CR(PEW)"), reasons for change (G, N, E) and Red List versions
// ("2010.2", "2019‐3" with a Unicode hyphen, "2007"). Each reader keeps the printed text apart from
// the value, so a typo is stored as printed and can be found later.

namespace BeastieBot3.Iucn.SummaryTables;

/// A Red List category as a table prints it. Category is the code ("CR", "LR/nt"); Tag is "PE",
/// "PEW" or null. Both are null when the text is not a category.
internal sealed record TableCategory(string? Category, string? Tag);

internal static partial class SummaryTableValues {
    /// Codes the tables use, with the capitalisation IUCN gives them.
    private static readonly string[] Categories = {
        "EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NR", "NE", "RE", "NA", "LR/cd", "LR/nt", "LR/lc",
    };

    [GeneratedRegex(@"^(?<cat>[A-Za-z]{2}(?:/[A-Za-z]{2})?)\s*(?:\(\s*(?<tag>PEW?)\s*\))?$", RegexOptions.IgnoreCase)]
    private static partial Regex CategoryPattern();

    /// Hyphens and dashes that IUCN's PDFs print in place of "-".
    public static string NormalizeDashes(string text) => text
        .Replace('\u2010', '-').Replace('\u2011', '-').Replace('\u2012', '-')
        .Replace('\u2013', '-').Replace('\u2014', '-').Replace('\u2212', '-');

    public static TableCategory ReadCategory(string? text) {
        if (string.IsNullOrWhiteSpace(text)) return new TableCategory(null, null);
        // "V U" in the 2013-2 table; "CR (PE)" in the 2007 one.
        var match = CategoryPattern().Match(text.Replace(" ", "", StringComparison.Ordinal));
        if (!match.Success) return new TableCategory(null, null);
        var code = Array.Find(Categories, c => string.Equals(c, match.Groups["cat"].Value, StringComparison.OrdinalIgnoreCase));
        if (code is null) return new TableCategory(null, null);
        var tag = match.Groups["tag"].Success ? match.Groups["tag"].Value.ToUpperInvariant() : null;
        // Only Critically Endangered takes a Possibly Extinct tag.
        return tag is not null && code != "CR" ? new TableCategory(null, null) : new TableCategory(code, tag);
    }

    /// True for "CR(PE)", "CR (PE)", "CR(PEW)": the category cell of Table 9.
    public static bool IsPossiblyExtinct(string text) => ReadCategory(text) is { Category: "CR", Tag: not null };

    /// G (genuine change), N (non-genuine change), E (the previous listing was an error), or null for
    /// anything else, such as "synonym of A. nigriceps" or "hybrid" in the 2007 table, whose rows
    /// say why a species was dropped.
    public static string? ReadReason(string? text) {
        if (string.IsNullOrWhiteSpace(text)) return null;
        var t = NormalizeDashes(text.Trim());
        if (t.Length == 1) {
            return t.ToUpperInvariant() switch { "G" => "G", "N" => "N", "E" => "E", _ => null };
        }
        var lower = t.ToLowerInvariant();
        // "Non-genuien" is in one table.
        if (lower.StartsWith("non-genuin", StringComparison.Ordinal) || lower.StartsWith("non genuin", StringComparison.Ordinal)
            || lower.StartsWith("non-genuie", StringComparison.Ordinal)) return "N";
        if (lower.StartsWith("genuine", StringComparison.Ordinal)) return "G";
        if (lower.StartsWith("error", StringComparison.Ordinal)) return "E";
        return null;
    }

    [GeneratedRegex(@"^(?<year>(?:19|20)\d{2})(?:[.\-](?<n>\d))?$")]
    private static partial Regex VersionPattern();

    /// "2019-3" for "2019-3", "2019.3" or "2019‐3"; "2007" for "2007". Null when the text is not a version.
    public static string? ReadVersion(string? text) {
        if (string.IsNullOrWhiteSpace(text)) return null;
        // "2019 \u20103" and "2013..1" are in the 2019-3 and 2013-1 tables.
        var compact = NormalizeDashes(text).Replace(" ", "", StringComparison.Ordinal).Replace("..", ".", StringComparison.Ordinal);
        var match = VersionPattern().Match(compact);
        if (!match.Success) return null;
        return match.Groups["n"].Success ? $"{match.Groups["year"].Value}-{match.Groups["n"].Value}" : match.Groups["year"].Value;
    }

    /// The year of a version from ReadVersion.
    public static int? VersionYear(string? version) =>
        version is not null && version.Length >= 4 && int.TryParse(version.AsSpan(0, 4), NumberStyles.None, CultureInfo.InvariantCulture, out var year)
            ? year : null;

    [GeneratedRegex(@"^(?:19|20)\d{2}$")]
    private static partial Regex YearPattern();

    public static int? ReadYear(string? text) =>
        text is not null && YearPattern().IsMatch(text.Trim()) ? int.Parse(text.Trim(), CultureInfo.InvariantCulture) : null;

    /// IUCN's kingdom for a group heading ("MAMMALS", "PLANTS", "FUNGI"), or null when the group can
    /// be in more than one kingdom ("FUNGI & PROTISTS") or is not known.
    public static string? KingdomOfGroup(string? group) {
        if (string.IsNullOrWhiteSpace(group)) return null;
        var g = group.ToUpperInvariant();
        if (g.Contains("PROTIST", StringComparison.Ordinal) || g.Contains("CHROMIS", StringComparison.Ordinal)
            || g.Contains("ALGAE", StringComparison.Ordinal)) return null;
        if (g.Contains("PLANT", StringComparison.Ordinal)) return "PLANTAE";
        if (g.Contains("FUNG", StringComparison.Ordinal) || g.Contains("LICHEN", StringComparison.Ordinal)
            || g.Contains("MUSHROOM", StringComparison.Ordinal)) return "FUNGI";
        string[] animals = {
            "MAMMAL", "BIRD", "REPTILE", "AMPHIBIAN", "FISH", "INSECT", "MOLLUSC", "INVERTEBRATE", "CRUSTACEAN",
            "CORAL", "ARACHNID", "SPIDER", "SCORPION", "DRAGONFL", "BUTTERFL", "BEE", "SNAIL", "SHARK", "RAY",
            "ANIMAL", "MILLIPEDE", "CENTIPEDE", "WORM", "SPONGE", "ECHINODERM", "CNIDARIA", "ANNELID",
        };
        foreach (var word in animals) {
            if (g.Contains(word, StringComparison.Ordinal)) return "ANIMALIA";
        }
        return null;
    }
}
