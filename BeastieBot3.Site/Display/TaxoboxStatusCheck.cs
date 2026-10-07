using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

public enum TaxoboxStatusOutcome {
    /// The taxobox has no IUCN status.
    NoStatus,
    /// Another code than the latest assessment's taxobox code.
    OtherCategory,
    /// The same code under another status_system (IUCN2.3 for an assessment under criteria 3.1).
    OtherSystem,
    /// The same code and system; the reference cites the latest assessment.
    CitesLatest,
    /// The same code and system; the reference cites another assessment (CitedAssessment).
    CitesOther,
    /// The same code and system; the reference names no assessment.
    CitesNone,
}

/// The status in the taxobox of the taxon's English Wikipedia article compared with the latest
/// global assessment. Wikipedia: the taxobox's status as a category (null for NoStatus).
/// CitedAssessment: the taxon's assessment that the reference cites, when it cites another one than
/// the latest; CitedAssessmentId is set even when the assessment is not this taxon's.
public sealed record TaxoboxStatusCheck(
    TaxoboxStatusOutcome Outcome,
    CategoryDisplay? Wikipedia,
    string? WikipediaSystem,
    string LatestSystem,
    AssessmentRow? CitedAssessment,
    long? CitedAssessmentId,
    DateOnly? Downloaded) {

    public static TaxoboxStatusCheck For(EnwikiTaxoboxStatusRow row, AssessmentRow latest, IReadOnlyList<AssessmentRow> globalHistory) {
        var code = SpeciesboxStatus.ToStatusCode(latest.Category, latest.PossiblyExtinct, latest.PossiblyExtinctInTheWild);
        var system = SpeciesboxStatus.ToStatusSystem(code, latest.CriteriaVersion);
        var downloaded = DateOnly.TryParse(row.Downloaded, System.Globalization.CultureInfo.InvariantCulture, out var d) ? d : (DateOnly?)null;
        if (row.Status is not { } status) {
            return new(TaxoboxStatusOutcome.NoStatus, null, null, system, null, null, downloaded);
        }
        var wikipedia = Describe(status);
        var rowSystem = row.StatusSystem ?? "";
        if (!string.Equals(status.Trim(), code, StringComparison.OrdinalIgnoreCase)) {
            return new(TaxoboxStatusOutcome.OtherCategory, wikipedia, rowSystem, system, null, null, downloaded);
        }
        if (!string.Equals(rowSystem, system, StringComparison.OrdinalIgnoreCase)) {
            return new(TaxoboxStatusOutcome.OtherSystem, wikipedia, rowSystem, system, null, null, downloaded);
        }
        if (row.RefAssessmentId is not { } cited) {
            return new(TaxoboxStatusOutcome.CitesNone, wikipedia, rowSystem, system, null, null, downloaded);
        }
        if (cited == latest.AssessmentId) {
            return new(TaxoboxStatusOutcome.CitesLatest, wikipedia, rowSystem, system, null, cited, downloaded);
        }
        return new(TaxoboxStatusOutcome.CitesOther, wikipedia, rowSystem, system,
            globalHistory.FirstOrDefault(a => a.AssessmentId == cited), cited, downloaded);
    }

    /// A taxobox code as a category: PE and PEW are CR, possibly extinct (in the wild); the LR codes
    /// in IUCN's spelling.
    public static CategoryDisplay Describe(string status) {
        var code = status.Trim().ToUpperInvariant();
        return code switch {
            "PE" => IucnCategories.Describe("CR", true, false),
            "PEW" => IucnCategories.Describe("CR", false, true),
            "LR/LC" or "LR/NT" or "LR/CD" => IucnCategories.Describe("LR/" + code[3..].ToLowerInvariant(), false, false),
            _ => IucnCategories.Describe(code, false, false),
        };
    }
}
