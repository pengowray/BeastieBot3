using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// The Wikidata page of a taxon (/species/{id}/wikidata): for one of its assessments, the
/// assessment's Wikidata item and the QuickStatements commands that create the item or add what it
/// lacks; for the latest global assessment, the taxon item's IUCN conservation status (P141) with the
/// commands that bring it up to date. The assessment tables pick the assessment (?assessment=). Nothing
/// here depends on the date, so the page has no options and no form.
[OutputCache(PolicyName = SiteCachePolicies.SpeciesWikidata)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesWikidataModel : AssessmentToolModel {
    public SpeciesWikidataModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options, ILogger<SpeciesWikidataModel> logger)
        : base(db, queries, options, logger) {
    }

    /// The selected assessment's Wikidata item and the commands for it; null when no assessment is selected.
    public WikidataCiteView? Wikidata { get; private set; }

    /// The taxon item's IUCN status compared with the latest global assessment; null unless that is
    /// the assessment shown.
    public WikidataStatusView? WikidataStatus { get; private set; }

    public IActionResult OnGet(long taxonId, long? assessment) {
        if (!LoadTaxon(taxonId)) {
            return Page();
        }
        SelectAssessment(assessment);
        if (Selected is { } selected) {
            Parts = ReadParts(selected.CitationJson);
            Wikidata = BuildWikidataCite(WikitextOptions.Default.ToCiteQOptions(DateOnly.FromDateTime(DateTime.UtcNow), Downloaded));
            WikidataStatus = BuildWikidataStatus();
        }
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, WikidataPath(Taxon!.TaxonId));
        return Page();
    }

    /// The heading and the row of choices, Wikidata chosen. The links to each Wikipedia keep the
    /// assessment only: this page has no citation options to carry.
    public CiteForChooser Chooser => BuildChooser(null, edition => {
        var query = new List<string>();
        if (SelectedIdForLinks is { } id) {
            query.Add("assessment=" + id.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        if (!edition.IsEnglish) {
            query.Add(WikitextOptions.WikiKey + "=" + edition.Code);
        }
        return WikipediaPath(Taxon?.TaxonId ?? RequestedTaxonId, query.Count == 0 ? "" : "?" + string.Join('&', query));
    }, wikipediaLinksKeepOptions: false);

    public override string OptionsUrl(long? assessmentId) =>
        WikidataPath(Taxon?.TaxonId ?? RequestedTaxonId, assessmentId is { } id ? $"?assessment={id}" : "");

    public override string OtherIdOptionsUrl(CombinedRow row) => WikidataPath(row.Id.TaxonId, $"?assessment={row.Assessment.AssessmentId}");

    public override string ToolColumnHeading => SiteText.ColWikidata;
    public override string ShowLinkText => SiteText.ShowWikidata;
    public override string ShowLinkAccessible(string? region, int? year, string? versionNote) => SiteText.ShowWikidataAccessible(region, year, versionNote);
    public override string ShowOtherIdText(long taxonId) => SiteText.ShowWikitextOtherId(taxonId);
    public override string ShowOtherIdAccessible(long taxonId, int? year, string? versionNote) => SiteText.ShowWikidataOtherIdAccessible(taxonId, year, versionNote);
}
