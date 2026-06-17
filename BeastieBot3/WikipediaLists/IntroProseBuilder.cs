using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BeastieBot3.Iucn;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.RecordClassification;

namespace BeastieBot3.WikipediaLists;

// Generates the English-prose intro for a list: classifies the record set by rank, computes
// percentages and "possibly extinct" counts, builds the subspecies/subpopulation/threatened/
// data-deficient/notes paragraphs, and assembles the mustache context dictionary consumed by the
// header and footer templates. Extracted from WikipediaListGenerator (the R2 god-class carve-up):
// the prose-and-counts concern lives here, the tree rendering stays in the generator.
internal sealed class IntroProseBuilder {
    private readonly IucnListQueryService _queryService;

    public IntroProseBuilder(IucnListQueryService queryService) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
    }

    // Builds the full template context (header/footer variables) for one list, including all
    // pre-rendered intro paragraphs. `allRecords` is the flattened set of records across sections.
    public Dictionary<string, object?> BuildContext(
        WikipediaListDefinition definition,
        IReadOnlyList<IucnSpeciesRecord> allRecords,
        int totalCount,
        string scopeLabel,
        string sectionSummary,
        string datasetVersion,
        string datasetYear) {
        // Classify records by rank. Exclude regional assessments so the species count matches the
        // global-only list body (SectionBodyRenderer drops regional) and the percentage denominator
        // (CountEvaluatedSpecies is global-only) — otherwise the printed "% of evaluated species"
        // divides a regional-inclusive numerator by a global denominator.
        var speciesCount = allRecords.Count(r => !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName) && !RecordClassification.IsRegionalAssessment(r));
        var subspeciesCount = allRecords.Count(IsSubspecies);
        var varietyCount = allRecords.Count(IsVariety);
        var subpopCount = allRecords.Count(r => !string.IsNullOrWhiteSpace(r.SubpopulationName));

        var taxaAdj = definition.TaxaAdjective ?? "";
        var taxaNameLower = definition.TaxaNameLower ?? "";
        var statusText = definition.StatusText ?? "";
        var statusWikiLink = definition.StatusWikiLink ?? "";

        var isExtinct = statusText == "extinct" || statusText == "extinct in the wild";
        var hasIntroMetadata = !string.IsNullOrEmpty(definition.TaxaAdjective) && !string.IsNullOrEmpty(definition.StatusText);

        // Compute percentage of evaluated species (skip for extinct lists and lists without intro metadata)
        string? percentageText = null;
        if (hasIntroMetadata && !isExtinct) {
            var evaluatedTotal = _queryService.CountEvaluatedSpecies(definition.Filters);
            if (evaluatedTotal > 0 && speciesCount > 0) {
                percentageText = $"{FormatPercentage(speciesCount, evaluatedTotal)} of all evaluated {taxaAdj} species are listed as {statusText}.";
            }
        }

        // Build pre-rendered intro paragraphs (skip when no intro metadata, suppress subpops for extinct)
        string? subspeciesParagraph = null;
        string? subpopulationParagraph = null;
        string? threatenedContext = null;
        string? ddInfo = null;
        string? notesParagraph = null;
        if (hasIntroMetadata) {
            subspeciesParagraph = BuildSubspeciesParagraph(subspeciesCount, varietyCount, taxaAdj, statusText);
            subpopulationParagraph = isExtinct ? null : BuildSubpopulationParagraph(subpopCount, taxaNameLower, statusText);
            threatenedContext = BuildThreatenedContext(definition, taxaAdj, taxaNameLower);
            ddInfo = BuildDataDeficientInfo(definition, taxaAdj);
            notesParagraph = BuildNotesParagraph(speciesCount, subspeciesCount, varietyCount, subpopCount, taxaAdj, statusText);
        }

        // CR-specific: possibly extinct counts
        string? peText = null;
        if (statusText == "critically endangered") {
            var peCount = allRecords.Count(r => r.StatusCode == "CR(PE)" && !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName));
            var pewCount = allRecords.Count(r => r.StatusCode == "CR(PEW)" && !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName));
            var combinedPe = peCount + pewCount;
            if (combinedPe > 0) {
                if (peCount > 0 && pewCount == 0) {
                    peText = $", including {NewspaperNumber(peCount)} which are tagged as ''possibly extinct''";
                } else {
                    peText = $", including {NewspaperNumber(combinedPe)} which are tagged as ''possibly extinct'' or ''possibly extinct in the wild''";
                }
            } else {
                peText = ", none of which are tagged as ''possibly extinct''";
            }
        }

        // Format the generation date as day-month-year ("18 June 2026") and a "Month yyyy" stamp so
        // both the citation access-date and the {{Use dmy dates}} maintenance tag match the article's
        // declared dmy style (MOS:DATEUNIFY) and reflect the real generation month.
        var generatedNow = DateTimeOffset.UtcNow;
        return new Dictionary<string, object?> {
            ["title"] = definition.Title,
            ["description"] = definition.Description,
            ["categories"] = definition.Categories.Count > 0 ? string.Join("\n", definition.Categories) : null,
            ["scope_label"] = scopeLabel,
            ["dataset_version"] = datasetVersion,
            ["dataset_year"] = datasetYear,
            ["generated_at"] = generatedNow.ToString("d MMMM yyyy", CultureInfo.InvariantCulture),
            ["generated_month_year"] = generatedNow.ToString("MMMM yyyy", CultureInfo.InvariantCulture),
            ["total_entries"] = totalCount,
            ["sections_summary"] = sectionSummary,
            // Intro text variables
            ["species_count"] = NewspaperNumber(speciesCount),
            ["taxa_adjective"] = string.IsNullOrEmpty(taxaAdj) ? null : taxaAdj,
            ["taxa_name_lower"] = taxaNameLower,
            ["status_text"] = statusText,
            ["status_wiki_link"] = statusWikiLink,
            ["percentage_text"] = percentageText,
            ["pe_text"] = peText,
            ["subspecies_paragraph"] = subspeciesParagraph,
            ["subpopulation_paragraph"] = subpopulationParagraph,
            ["threatened_context"] = threatenedContext,
            ["dd_info"] = ddInfo,
            ["notes_paragraph"] = notesParagraph,
            ["simple_intro"] = hasIntroMetadata ? null : "1",
        };
    }

    private static string? BuildSubspeciesParagraph(int subspeciesCount, int varietyCount, string? taxaAdj, string? statusText) {
        if (subspeciesCount == 0 && varietyCount == 0) return null;
        var varietyNoun = varietyCount == 1 ? "variety" : "varieties";
        string what;
        if (subspeciesCount > 0 && varietyCount > 0)
            what = $"{NewspaperNumber(subspeciesCount)} subspecies and {NewspaperNumber(varietyCount)} {varietyNoun}";
        else if (varietyCount > 0)
            what = $"{NewspaperNumber(varietyCount)} {taxaAdj} {varietyNoun}";
        else
            what = $"{NewspaperNumber(subspeciesCount)} {taxaAdj} subspecies";

        if (!string.IsNullOrEmpty(statusText))
            return $"The IUCN also lists {what} as {statusText}.";
        return $"The IUCN has also evaluated {what}.";
    }

    private static string? BuildSubpopulationParagraph(int subpopCount, string? taxaNameLower, string? statusText) {
        if (subpopCount == 0) {
            if (!string.IsNullOrEmpty(statusText))
                return $"No subpopulations of {taxaNameLower} have been evaluated as {statusText} by the IUCN.";
            return null;
        }
        var have = subpopCount == 1 ? "has" : "have";
        var subpops = subpopCount == 1 ? "a subpopulation" : "subpopulations";
        if (!string.IsNullOrEmpty(statusText))
            return $"Of the subpopulations of {taxaNameLower} evaluated by the IUCN, {NewspaperNumber(subpopCount)} {have} been assessed as {statusText}.";
        return $"Of the subpopulations of {taxaNameLower} evaluated by the IUCN, {NewspaperNumber(subpopCount)} {have} been assessed.";
    }

    private string? BuildThreatenedContext(WikipediaListDefinition definition, string? taxaAdj, string? taxaNameLower) {
        var statusText = definition.StatusText;
        if (statusText == "endangered") {
            var crCount = _queryService.CountSpeciesByStatus(definition.Filters, "CR");
            var enCount = _queryService.CountSpeciesByStatus(definition.Filters, "EN");
            var combined = crCount + enCount;
            var crListTitle = $"List of critically endangered {taxaNameLower}";
            return "For a species to be considered endangered by the IUCN it must meet certain quantitative criteria which are designed to classify taxa facing \"a very high risk of extinction\". "
                + "An even higher risk is faced by ''critically endangered'' species, which meet the quantitative criteria for endangered species. "
                + $"[[{crListTitle}|Critically endangered {taxaNameLower}]] are listed separately. "
                + $"There are {NewspaperNumber(combined)} {taxaAdj} species which are endangered or critically endangered.";
        }

        if (statusText == "vulnerable") {
            var crListTitle = $"List of critically endangered {taxaNameLower}";
            var enListTitle = $"List of endangered {taxaNameLower}";
            return "For a species to be assessed as vulnerable to extinction the best available evidence must meet quantitative criteria set by the IUCN designed to reflect \"a high risk of extinction in the wild\". "
                + $"''Endangered'' and ''critically endangered'' species also meet the quantitative criteria of ''vulnerable'' species, and are listed separately. See: [[{enListTitle}]], [[{crListTitle}]]. "
                + "Vulnerable, endangered and critically endangered species are collectively referred to as ''[[threatened species]]'' by the IUCN.";
        }

        return null;
    }

    private string? BuildDataDeficientInfo(WikipediaListDefinition definition, string? taxaAdj) {
        var statusText = definition.StatusText;
        // Only show DD info for threatened statuses
        if (statusText != "critically endangered" && statusText != "endangered" && statusText != "vulnerable" && statusText != "threatened")
            return null;

        var ddCount = _queryService.CountSpeciesByStatus(definition.Filters, "DD");
        if (ddCount == 0) return null;

        var evaluatedTotal = _queryService.CountEvaluatedSpecies(definition.Filters);
        var ddPercent = evaluatedTotal > 0 ? FormatPercentage(ddCount, evaluatedTotal) : "";

        return $"Additionally {NewspaperNumber(ddCount)} {taxaAdj} species ({ddPercent} of those evaluated) are listed as [[data deficient]], meaning there is insufficient information for a full assessment of conservation status. "
            + "As these species typically have small distributions and/or populations, they are intrinsically likely to be threatened, according to the IUCN."
            + "<ref>{{cite web|title=Limitations of the Data|url=http://www.iucnredlist.org/initiatives/mammals/description/limitations|website=The IUCN Red List of Threatened Species|publisher=International Union for Conservation of Nature and Natural Resources (IUCN)|access-date=11 January 2016}}</ref>"
            + " While the category of ''data deficient'' indicates that no assessment of extinction risk has been made for the taxa, the IUCN notes that it may be appropriate to give them \"the same degree of attention as threatened taxa, at least until their status can be assessed.\""
            + "<ref>{{cite web|title=2001 Categories & Criteria (version 3.1)|url=http://www.iucnredlist.org/static/categories_criteria_3_1|website=The IUCN Red List of Threatened Species|publisher=International Union for Conservation of Nature and Natural Resources (IUCN)|access-date=11 January 2016}}</ref>";
    }

    private static string BuildNotesParagraph(int speciesCount, int subspeciesCount, int varietyCount, int subpopCount, string? taxaAdj, string? statusText) {
        var whats = "species";
        if (subspeciesCount > 0 && varietyCount > 0)
            whats = "species, subspecies and varieties";
        else if (subspeciesCount > 0)
            whats = "species and subspecies";

        var statusPhrase = !string.IsNullOrEmpty(statusText) ? $"{statusText} " : "";
        var note = $"This is a complete list of {statusPhrase}{taxaAdj} {whats} as evaluated by the IUCN.";

        if (statusText == "critically endangered")
            note += " Species considered possibly extinct by the IUCN are marked as such.";

        if (subpopCount > 0 && !string.IsNullOrEmpty(statusText))
            note += $" {char.ToUpperInvariant(whats[0])}{whats[1..]} which have {statusText} subpopulations (or stocks) are indicated.";

        note += " Where possible common names for taxa are given while links point to the scientific name used by the IUCN.";

        return note;
    }
}
