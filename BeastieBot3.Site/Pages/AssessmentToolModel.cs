using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// What the two tool pages of a taxon share: the Wikipedia page (/species/{id}/wikitext,
/// SpeciesWikitextModel) and the Wikidata page (/species/{id}/wikidata, SpeciesWikidataModel). Both
/// show the output for one of the taxon's assessments (?assessment=, else the one the taxon page shows
/// first), and their assessment tables have a column that links each assessment's output.
public abstract class AssessmentToolModel : TaxonPageModel {
    protected readonly ILogger _logger;

    protected AssessmentToolModel(SiteDatabase db, SiteQueries queries, Microsoft.Extensions.Options.IOptions<SiteOptions> options, ILogger logger)
        : base(db, queries, options) {
        _logger = logger;
    }

    /// The assessment the page's output is for.
    public AssessmentRow? Selected { get; private set; }
    public bool SelectedIsDefault => Selected is not null && Selected.AssessmentId == StatusAssessment?.AssessmentId;

    /// The citation parts of the selected assessment; null when it has none.
    public IucnCitationParts? Parts { get; protected set; }

    /// The date this site downloaded the selected assessment's details, from its citation parts.
    protected DateOnly? Downloaded => Parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;

    /// The selected assessment's Wikidata item, its {{cite Q}} and the QuickStatements commands that
    /// create or fix the item (WikidataCite.Build); null when no assessment is selected.
    protected WikidataCiteView? BuildWikidataCite(CiteQOptions citeQOptions) => Selected is not { } selected ? null
        : WikidataCite.Build(selected, Parts, Taxon?.WikidataQid, ReadItemModel(), citeQOptions,
            (what, e) => _logger.LogWarning(e, "WikidataCitation.{Method} failed for assessment {AssessmentId}", what, selected.AssessmentId),
            hasPage: id => Assessments.Any(a => a.AssessmentId == id), taxonItemDoubt: TaxonItemDoubt.Of(Taxon));

    /// The taxon item's IUCN status (P141) compared with the latest global assessment, with commands;
    /// null unless the selected assessment is the latest global assessment of a taxon in the release.
    protected WikidataStatusView? BuildWikidataStatus() =>
        Taxon is { InRelease: true } taxon && LatestGlobal is { } latest && Selected?.AssessmentId == latest.AssessmentId
            ? WikidataCite.BuildStatus(taxon, latest, Parts,
                (what, e) => _logger.LogWarning(e, "{Method} failed for taxon {TaxonId}", what, taxon.TaxonId))
            : null;

    // The assessment item model `site build-db` stored; the defaults when it stored none. Null when
    // the stored model cannot be read, so the page offers no QuickStatements commands rather than
    // commands for another model.
    private WikidataItemModel? ReadItemModel() {
        try {
            return WikidataItemModel.FromJson(_db.Snapshot?.Get(SiteDbSchema.MetaKeys.WikidataItemModel));
        } catch (JsonException e) {
            _logger.LogWarning(e, "The site database's {Key} cannot be read", SiteDbSchema.MetaKeys.WikidataItemModel);
            return null;
        }
    }

    /// The Wikipedia page of the taxon: "/species/22732/wikitext" with the query (which starts with "?" or is empty).
    public static string WikipediaPath(long taxonId, string query = "") => $"/species/{taxonId}/wikitext{query}";

    /// The Wikidata page of the taxon: "/species/22732/wikidata" with the query.
    public static string WikidataPath(long taxonId, string query = "") => $"/species/{taxonId}/wikidata{query}";

    /// Picks the assessment: the requested one when the taxon has it, else the one in the status summary.
    protected void SelectAssessment(long? requested) =>
        Selected = (requested is { } id ? Assessments.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;

    /// This page for the given assessment (null: the default one), with the page's options.
    public abstract string OptionsUrl(long? assessmentId);

    /// This kind of page of another IUCN id for an assessment of a combined history row.
    public abstract string OtherIdOptionsUrl(CombinedRow row);

    /// The heading of the assessment tables' column that links each assessment's output ("Wikitext").
    public abstract string ToolColumnHeading { get; }

    /// The text of that column's links ("Show wikitext"), and their accessible names.
    public abstract string ShowLinkText { get; }
    public abstract string ShowLinkAccessible(string? region, int? year, string? versionNote);
    public abstract string ShowOtherIdText(long taxonId);
    public abstract string ShowOtherIdAccessible(long taxonId, int? year, string? versionNote);

    /// The key of a link in that column, so site.js can update its address after the options change.
    public static string OptionsLinkKey(long? assessmentId) =>
        assessmentId is { } id ? id.ToString(System.Globalization.CultureInfo.InvariantCulture) : "default";

    /// The key of a combined history row's link under another IUCN id, which cannot be one of this
    /// page's own keys (an assessment id, or "default").
    public static string OtherIdOptionsLinkKey(CombinedRow row) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{row.Id.TaxonId}-{row.Assessment.AssessmentId}");
}
