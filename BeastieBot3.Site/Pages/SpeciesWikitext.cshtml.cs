using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// One wikitext box: its label, the template name used in the copy button's accessible name, and
/// the text. CopyName replaces that accessible name, for a box that holds something other than
/// wikitext.
public sealed record WikitextBox(string Id, string Label, string Template, string Text, int Rows, string? CopyName = null) {
    public string CopyAccessibleName => CopyName ?? SiteText.CopyAccessible(Template);
}

/// How many of a citation's authors have full given names (CitationAuthor.GivenNames), out of the
/// authors who are people or names kept as published, with the first of them as an example.
public sealed record GivenNamesCoverage(int WithGivenNames, int People, CitationAuthor Example) {
    /// Null when no author has full given names, so the option is not shown.
    public static GivenNamesCoverage? Of(IucnCitationParts parts) {
        var named = parts.Authors.Where(a => !string.IsNullOrWhiteSpace(a.GivenNames)).ToList();
        if (named.Count == 0) {
            return null;
        }
        var people = parts.Authors.Count(a => a.Kind != CitationAuthorKind.Organisation || !string.IsNullOrWhiteSpace(a.GivenNames));
        return new GivenNamesCoverage(named.Count, people, named[0]);
    }
}

/// The wikitext page of a taxon (/species/{id}/wikitext): {{cite iucn}}, {{IUCN status}} and taxobox
/// status lines for one of its assessments with the citation options, the {{cite iucn}} of its Green
/// Status assessment, {{cite Q}} and QuickStatements commands for the assessment's Wikidata item,
/// and its IUCN status on Wikidata. The assessment tables pick the assessment (?assessment=).
[OutputCache(PolicyName = SiteCachePolicies.SpeciesWikitext)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesWikitextModel : TaxonPageModel {
    private readonly ILogger<SpeciesWikitextModel> _logger;

    public SpeciesWikitextModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options, ILogger<SpeciesWikitextModel> logger)
        : base(db, queries, options) {
        _logger = logger;
    }

    /// The assessment the wikitext is for.
    public AssessmentRow? Selected { get; private set; }
    public bool SelectedIsDefault => Selected is not null && Selected.AssessmentId == StatusAssessment?.AssessmentId;

    public WikitextOptions Options { get; private set; } = WikitextOptions.Default;
    public IucnCitationParts? Parts { get; private set; }
    public IReadOnlyList<WikitextBox> Boxes { get; private set; } = [];
    public string? DoiNote { get; private set; }
    public IReadOnlyList<string> UnsplitAuthors { get; private set; } = [];
    public string? DownloadDateText { get; private set; }
    public string TodayText { get; private set; } = string.Empty;

    /// The people and organisations IUCN credits for the selected assessment; null when it has none.
    public CreditsView? Credits { get; private set; }

    /// For the full given names option, which is shown only when this is not null.
    public GivenNamesCoverage? GivenNames { get; private set; }

    /// The "{{cite Q}} citation from Wikidata" part of the wikitext section; null when no assessment
    /// is selected.
    public WikidataCiteView? Wikidata { get; private set; }

    /// The "IUCN conservation status on Wikidata" part; null unless the wikitext shown is for the
    /// latest global assessment of a taxon in the release.
    public WikidataStatusView? WikidataStatus { get; private set; }

    /// This page with the current options, as a link to it would give them.
    public string CurrentOptionsUrl => OptionsUrl(SelectedIsDefault || Selected is null ? null : Selected.AssessmentId);

    /// The key of a "Show wikitext" link, so site.js can update its address after the options change.
    public static string OptionsLinkKey(long? assessmentId) =>
        assessmentId is { } id ? id.ToString(System.Globalization.CultureInfo.InvariantCulture) : "default";

    /// The taxobox an article about this taxon most likely uses, which names the status parameters box.
    public TaxoboxTemplate Taxobox { get; private set; } = TaxoboxTemplate.Speciesbox;

    /// The wikitext page of a taxon: "/species/22732/wikitext" with the query (which starts with "?" or is empty).
    public static string PathFor(long taxonId, string query = "") => $"/species/{taxonId}/wikitext{query}";

    public IActionResult OnGet(long taxonId, long? assessment, string? authors, string? access, string? opts,
        [FromQuery(Name = "ref")] string? wrapRef, string? refname, string? amp, string? fullnames,
        [FromQuery(Name = IucnReference.QueryKey)] string? cite = null,
        [FromQuery(Name = WikitextOptions.GreenStatusYearKey)] string? gsyear = null) {
        if (!LoadTaxon(taxonId)) {
            return Page();
        }
        Selected = (assessment is { } id ? Assessments.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;
        Options = WikitextOptions.FromQuery(authors, access, opts, wrapRef, refname, amp,
            Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected), fullnames) with {
            Template = IucnReference.FromQuery(cite),
            GreenStatusYear = WikitextOptions.ReadGreenStatusYear(gsyear),
        };
        Taxobox = TaxoboxTemplate.For(Taxon!.Kind, Taxon.Kingdom);
        BuildWikitext();
        // Every set of options has its own address; search engines get the page without them.
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, PathFor(Taxon.TaxonId));
        return Page();
    }

    /// The URL of this page with the current options and the given assessment (null: the default
    /// one). The ref name goes along only when the visitor chose it; otherwise that assessment's
    /// own default applies.
    public string OptionsUrl(long? assessmentId) {
        var target = (assessmentId is { } id ? Assessments.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;
        var targetDefault = target is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(target);
        return PathFor(Taxon?.TaxonId ?? RequestedTaxonId, Options.ToQuery(assessmentId, targetDefault));
    }

    private string DefaultRefNameFor(AssessmentRow assessment) =>
        DefaultRefNames.For(assessment, LatestGlobal?.AssessmentId, GlobalHistory);

    /// The "Show wikitext" link of a combined history row under another IUCN id: that id's wikitext
    /// page with the assessment and the current options. The ref name goes along only when the
    /// visitor chose it, as on this page; otherwise the default that page gives the assessment applies.
    public string OtherIdOptionsUrl(CombinedRow row) {
        var other = row.Id.Taxon;
        var targetDefault = DefaultRefNames.For(row.Assessment, other.InRelease ? other.LatestGlobalAssessmentId : null, row.Id.Global);
        return PathFor(other.TaxonId, Options.ToQuery(row.Assessment.AssessmentId, targetDefault));
    }

    /// The data-options-link key of a combined history row under another IUCN id, which cannot be
    /// one of this page's own keys (an assessment id, or "default").
    public static string OtherIdOptionsLinkKey(CombinedRow row) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{row.Id.TaxonId}-{row.Assessment.AssessmentId}");

    private void BuildWikitext() {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        TodayText = SiteFormat.Date(today);
        if (Selected is null) {
            return;
        }
        Parts = ReadParts(Selected.CitationJson);
        Credits = CreditsView.Build(_queries.GetCredits(Selected.AssessmentId));
        var boxes = new List<WikitextBox>();
        CiteIucnOptions? citeOptions = null;
        DateOnly? downloaded = Parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;

        if (Parts is not null) {
            DownloadDateText = downloaded is { } d ? SiteFormat.Date(d) : null;
            citeOptions = Options.ToCiteIucnOptions(today, downloaded);
            GivenNames = GivenNamesCoverage.Of(Parts);
            var cite = CiteIucnRenderer.Render(Parts, citeOptions);
            boxes.Add(new WikitextBox("wikitext-cite", SiteText.LabelCite, "{{cite iucn}}", cite, Rows: 5));

            DoiNote = cite.Contains("|doi=", StringComparison.Ordinal)
                ? Parts.DoiSource switch {
                    DoiSource.Gbif => SiteText.DoiGbif,
                    DoiSource.Wikidata => SiteText.DoiWikidata,
                    DoiSource.Resolved => SiteText.DoiResolved,
                    _ => null,
                }
                : SiteText.NoDoi;
            UnsplitAuthors = Parts.Authors
                .Where(a => a.Kind == CitationAuthorKind.Verbatim && !string.IsNullOrWhiteSpace(a.Display))
                .Select(a => a.Display.Trim())
                .ToList();
        }

        if (IucnCategories.HasStatusTemplateCode(Selected)) {
            var status = IucnStatusTemplate.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.TaxonId, Selected.AssessmentId, Selected.YearPublished?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            boxes.Add(new WikitextBox("wikitext-status", SiteText.LabelStatus, "{{IUCN status}}", status, Rows: 2));
        }

        if (Parts is not null && citeOptions is not null && IucnCategories.HasTaxoboxCode(Selected)) {
            // status_ref is always a <ref>, whatever the citation box shows. It holds {{cite Q}} when
            // the reader chose it and the assessment has a Wikidata item.
            var statusRef = IucnReference.Render(Options.Template, Parts, Selected.WikidataItemQid, Selected.WikidataItemProperties,
                citeOptions with { WrapInRef = true }, Options.ToCiteQOptions(today, downloaded) with { WrapInRef = true })!;
            var lines = SpeciesboxStatus.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.CriteriaVersion, statusRef);
            boxes.Add(new WikitextBox("wikitext-speciesbox", Taxobox.Label, Taxobox.Name, lines, Rows: 5));
        }
        if (GreenStatus is { } green && IucnCitationParts.FromJson(green.CitationJson) is { } greenParts) {
            var greenDownloaded = SiteFormat.TryParseDate(_db.Snapshot?.Get(SiteDbSchema.MetaKeys.GreenStatusFetched), out var g) ? g : (DateOnly?)null;
            var year = BeastieBot3.Shared.Wikitext.GreenStatusYear.For(Options.GreenStatusYear, green.AssessedYear, green.PublishedYear, green.RedListYear);
            var greenOptions = Options.ToCiteIucnOptions(today, greenDownloaded) with { RefName = GreenStatusRefName };
            boxes.Add(new WikitextBox("wikitext-green", SiteText.GreenStatusCiteLabel, "{{cite iucn}}",
                CiteIucnRenderer.RenderGreenStatus(greenParts with { Year = year }, green.Url, greenOptions), Rows: 4));
        }
        Boxes = boxes;

        Wikidata = WikidataCite.Build(Selected, Parts, Taxon?.WikidataQid, ReadItemModel(), Options.ToCiteQOptions(today, downloaded),
            (what, e) => _logger.LogWarning(e, "WikidataCitation.{Method} failed for assessment {AssessmentId}", what, Selected.AssessmentId),
            hasPage: id => Assessments.Any(a => a.AssessmentId == id), taxonItemDoubt: TaxonItemDoubt.Of(Taxon));
        if (Taxon is not null && Taxon.InRelease && LatestGlobal is not null && Selected.AssessmentId == LatestGlobal.AssessmentId) {
            WikidataStatus = WikidataCite.BuildStatus(Taxon, LatestGlobal, Parts,
                (what, e) => _logger.LogWarning(e, "{Method} failed for taxon {TaxonId}", what, Taxon.TaxonId));
        }
    }

    /// The ref name of the Green Status citation, beside the Red List assessment's "iucn".
    public const string GreenStatusRefName = "iucn-green";

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
}
