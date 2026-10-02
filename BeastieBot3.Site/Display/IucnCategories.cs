using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

/// How a category looks on the page: the code shown in the badge, the full label, and the CSS class
/// that gives the badge its colours (site.css, taken from en-wiki Module:IUCN status so badges look
/// like the ones in articles).
public sealed record CategoryDisplay(string Code, string BadgeText, string Label, string CssClass);

/// IUCN category codes as the site database stores them. Codes are compared with their case:
/// "nt" (Not Threatened, a pre-1994 category) is not "NT" (Near Threatened), and "Ex" is not "EX".
public static class IucnCategories {
    // Labels are IUCN's own names for each code, as the Red List API gives them.
    private static readonly Dictionary<string, (string Label, string Css)> Current = new(StringComparer.Ordinal) {
        ["EX"] = ("Extinct", "cat-ex"),
        ["EW"] = ("Extinct in the Wild", "cat-ew"),
        ["CR"] = ("Critically Endangered", "cat-cr"),
        ["EN"] = ("Endangered", "cat-en"),
        ["VU"] = ("Vulnerable", "cat-vu"),
        ["NT"] = ("Near Threatened", "cat-nt"),
        ["LC"] = ("Least Concern", "cat-lc"),
        ["DD"] = ("Data Deficient", "cat-grey"),
        ["RE"] = ("Regionally Extinct", "cat-ew"),
        ["NA"] = ("Not Applicable", "cat-grey"),
        ["NE"] = ("Not Evaluated", "cat-grey"),
        ["NR"] = ("Not Recognized", "cat-other"),
        // The 1994 categories (criteria version 2.3). Module:IUCN status shows LR/nt and LR/cd in the
        // Near Threatened colours and LR/lc in the Least Concern colours.
        ["LR/lc"] = ("Lower Risk/least concern", "cat-lc"),
        ["LR/nt"] = ("Lower Risk/near threatened", "cat-nt"),
        ["LR/cd"] = ("Lower Risk/conservation dependent", "cat-nt"),
    };

    // Categories used before 1994, with IUCN's names for them.
    private static readonly Dictionary<string, string> Old = new(StringComparer.Ordinal) {
        ["Ex"] = "Extinct",
        ["Ex?"] = "Extinct?",
        ["Ex/E"] = "Extinct/Endangered",
        ["E"] = "Endangered",
        ["V"] = "Vulnerable",
        ["R"] = "Rare",
        ["I"] = "Indeterminate",
        ["K"] = "Insufficiently Known",
        ["T"] = "Threatened",
        ["CT"] = "Commercially Threatened",
        ["O"] = "Out of Danger",
        ["nt"] = "Not Threatened",
        ["A"] = "Abundant",
        ["N/A"] = "Unknown",
        ["CUSTOM"] = "Unknown",
    };

    // Codes Template:IUCN status accepts (Module:IUCN status), in the case the site stores them.
    private static readonly HashSet<string> StatusTemplateCodes = new(StringComparer.Ordinal) {
        "EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "RE", "NA", "NE", "NR", "LR/lc", "LR/nt", "LR/cd",
    };

    // Codes the taxobox's Module:Conservation status accepts under IUCN3.1 or IUCN2.3. RE is regional
    // and accepted by neither.
    private static readonly HashSet<string> TaxoboxCodes = new(StringComparer.Ordinal) {
        "EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NA", "NE", "NR", "LR/lc", "LR/nt", "LR/cd",
    };

    public static CategoryDisplay Describe(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild) {
        var code = category.Trim();
        if (code == "CR" && possiblyExtinct) {
            return new CategoryDisplay(code, "CR (PE)", SiteText.CategoryCrPe, "cat-cr");
        }
        if (code == "CR" && possiblyExtinctInTheWild) {
            return new CategoryDisplay(code, "CR (PEW)", SiteText.CategoryCrPew, "cat-cr");
        }
        if (Current.TryGetValue(code, out var current)) {
            return new CategoryDisplay(code, code, current.Label, current.Css);
        }
        if (Old.TryGetValue(code, out var old)) {
            return new CategoryDisplay(code, code, SiteText.OldCategoryLabel(old), "cat-other");
        }
        return new CategoryDisplay(code, code, SiteText.UnknownCategoryLabel(code), "cat-other");
    }

    /// How an assessment's category looks. An earlier-version row (IsEarlierVersionCode) coded "NT"
    /// gets a grey badge that keeps the code and says IUCN gives it no name: IUCN's API describes
    /// these rows as "Unknown", and they may be the pre-1994 Not Threatened. Other codes keep their
    /// label ("EX" is Extinct in every version of the categories).
    public static CategoryDisplay Describe(AssessmentRow assessment) {
        var code = assessment.Category.Trim();
        if (code == "NT" && IsEarlierVersionCode(assessment)) {
            return new CategoryDisplay(code, code, SiteText.OldCategoryLabel(SiteText.CategoryNotNamed), "cat-other");
        }
        return Describe(assessment.Category, assessment.PossiblyExtinct, assessment.PossiblyExtinctInTheWild);
    }

    /// True for an assessment that is not the latest, has no criteria version (IUCN's "Earlier
    /// Version", stored as NULL) and yet has a code of the current categories ("NT", "EX"). In the
    /// 2026-1 data that is 12 rows from 1986 to 1998. Their category system is not the one the
    /// templates mean by these codes, so they get no {{IUCN status}} or taxobox wikitext. The LR
    /// codes are not included: they exist only in the 1994 categories, so the code itself says which
    /// version it is. A latest assessment is never treated this way.
    public static bool IsEarlierVersionCode(AssessmentRow assessment) {
        var code = assessment.Category.Trim();
        return !assessment.IsLatest
            && string.IsNullOrWhiteSpace(assessment.CriteriaVersion)
            && Current.ContainsKey(code)
            && !code.StartsWith("LR/", StringComparison.Ordinal);
    }

    /// Whether {{IUCN status}} has a code for this category.
    public static bool HasStatusTemplateCode(string category) => StatusTemplateCodes.Contains(category.Trim());

    /// Whether the taxobox status parameters have a code for this category.
    public static bool HasTaxoboxCode(string category) => TaxoboxCodes.Contains(category.Trim());

    /// Whether {{IUCN status}} wikitext is given for this assessment.
    public static bool HasStatusTemplateCode(AssessmentRow assessment) =>
        !IsEarlierVersionCode(assessment) && HasStatusTemplateCode(assessment.Category);

    /// Whether taxobox status wikitext is given for this assessment (global assessments only).
    public static bool HasTaxoboxCode(AssessmentRow assessment) =>
        assessment.IsGlobal && !IsEarlierVersionCode(assessment) && HasTaxoboxCode(assessment.Category);

    /// EPBC Act categories as the Act names them.
    public static string? EpbcLabel(string? code) => code?.Trim().ToUpperInvariant() switch {
        null or "" => null,
        "EX" => "Extinct",
        "EW" => "Extinct in the wild",
        "CR" => "Critically Endangered",
        "EN" => "Endangered",
        "VU" => "Vulnerable",
        "CD" => "Conservation Dependent",
        var other => other,
    };
}
