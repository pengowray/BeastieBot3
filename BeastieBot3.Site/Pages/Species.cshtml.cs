using System.Text.Json;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

public sealed record EnglishCommonName(string Name, IReadOnlyList<string> Sources, bool IsIucnMain);

public sealed record LanguageGroup(string Language, IReadOnlyList<string> Names);

/// One wikitext box: its label, the template name used in the copy button's accessible name, and
/// the text.
public sealed record WikitextBox(string Id, string Label, string Template, string Text, int Rows);

[OutputCache(PolicyName = SiteCachePolicies.Species)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesModel : PageModel {
    /// Name lists longer than this show the rest behind "Show N more".
    public const int ShortListLength = 10;

    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;
    private readonly SiteOptions _options;

    public SpeciesModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options) {
        _db = db;
        _queries = queries;
        _options = options.Value;
    }

    public long RequestedTaxonId { get; private set; }
    public string? Version { get; private set; }
    public TaxonRow? Taxon { get; private set; }
    public TaxonSummary? Parent { get; private set; }

    public IReadOnlyList<AssessmentRow> GlobalHistory { get; private set; } = [];
    public IReadOnlyList<AssessmentRow> RegionalLatest { get; private set; } = [];
    public AssessmentRow? LatestGlobal { get; private set; }

    /// The assessment in the status summary: the latest global one, or the latest regional one.
    public AssessmentRow? StatusAssessment { get; private set; }

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

    public IReadOnlyList<EnglishCommonName> EnglishNames { get; private set; } = [];
    public IReadOnlyList<LanguageGroup> OtherLanguages { get; private set; } = [];
    public IReadOnlyList<string> Synonyms { get; private set; } = [];
    public IReadOnlyList<TaxonListItem> Children { get; private set; } = [];

    /// Set when the visitor arrived from a search for a synonym or common name of this taxon.
    public string? ArrivedQuery { get; private set; }
    public string? ArrivedNameType { get; private set; }

    public string? DataDateRange { get; private set; }

    public IActionResult OnGet(long taxonId, long? assessment, string? authors, string? access, string? opts,
        [FromQuery(Name = "ref")] string? wrapRef, string? refname, string? amp, string? q) {
        RequestedTaxonId = taxonId;
        var snapshot = _db.Snapshot;
        Version = snapshot?.IucnRelease;
        Taxon = _queries.GetTaxon(taxonId);
        if (Taxon is null) {
            Response.StatusCode = StatusCodes.Status404NotFound;
            return Page();
        }

        if (Taxon.ParentTaxonId is { } parentId) {
            Parent = _queries.GetSummary(parentId);
        }
        LoadAssessments(assessment);
        Options = WikitextOptions.FromQuery(authors, access, opts, wrapRef, refname, amp,
            Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected));
        BuildWikitext();
        LoadNames();
        Children = _queries.GetChildren(Taxon.TaxonId).Select(c => new TaxonListItem(c)).ToList();
        LoadArrival(q);
        DataDateRange = ReadDataDateRange(snapshot);
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, $"/species/{Taxon.TaxonId}");
        return Page();
    }

    /// The URL of this page with the current options and the given assessment (null: the default
    /// one). The ref name goes along only when the visitor chose it; otherwise that assessment's
    /// own default applies.
    public string OptionsUrl(long? assessmentId) {
        var target = (assessmentId is { } id ? _assessments.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;
        var targetDefault = target is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(target);
        return $"/species/{Taxon?.TaxonId}{Options.ToQuery(assessmentId, targetDefault)}";
    }

    public string DefaultRefNameFor(AssessmentRow assessment) =>
        DefaultRefNames.For(assessment, LatestGlobal?.AssessmentId, GlobalHistory);

    private IReadOnlyList<AssessmentRow> _assessments = [];

    private void LoadAssessments(long? requested) {
        var all = _queries.GetAssessments(Taxon!.TaxonId);
        _assessments = all;
        GlobalHistory = all.Where(a => a.IsGlobal).ToList();
        LatestGlobal = GlobalHistory.FirstOrDefault(a => a.AssessmentId == Taxon.LatestGlobalAssessmentId)
            ?? (Taxon.LatestGlobalAssessmentId is null ? null : GlobalHistory.FirstOrDefault(a => a.IsLatest));

        // Latest per region: the row flagged latest, or the newest one when none is.
        RegionalLatest = all.Where(a => !a.IsGlobal)
            .GroupBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(g => g.FirstOrDefault(a => a.IsLatest) ?? g.First())
            .OrderBy(a => a.Scope, StringComparer.OrdinalIgnoreCase)
            .ToList();

        StatusAssessment = LatestGlobal ?? RegionalLatest
            .OrderByDescending(a => a.YearPublished ?? 0)
            .ThenByDescending(a => a.AssessmentDate)
            .FirstOrDefault();

        Selected = (requested is { } id ? all.FirstOrDefault(a => a.AssessmentId == id) : null) ?? StatusAssessment;
    }

    private void BuildWikitext() {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        TodayText = SiteFormat.Date(today);
        if (Selected is null) {
            return;
        }
        Parts = ReadParts(Selected.CitationJson);
        var boxes = new List<WikitextBox>();
        CiteIucnOptions? citeOptions = null;

        if (Parts is not null) {
            DateOnly? downloaded = Parts.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;
            DownloadDateText = downloaded is { } d ? SiteFormat.Date(d) : null;
            citeOptions = new CiteIucnOptions {
                AuthorStyle = Options.AuthorStyle,
                AccessDate = Options.Access switch {
                    WikitextOptions.AccessToday => today,
                    WikitextOptions.AccessNone => null,
                    _ => downloaded,
                },
                WrapInRef = Options.WrapInRef,
                RefName = Options.RefName,
                NameListStyleAmp = Options.Amp,
            };
            var cite = CiteIucnRenderer.Render(Parts, citeOptions);
            boxes.Add(new WikitextBox("wikitext-cite", SiteText.LabelCite, "{{cite iucn}}", cite, Rows: 5));

            DoiNote = cite.Contains("|doi=", StringComparison.Ordinal)
                ? Parts.DoiSource switch {
                    DoiSource.Gbif => SiteText.DoiGbif,
                    DoiSource.Wikidata => SiteText.DoiWikidata,
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
            // status_ref is always a <ref>, whatever the citation box shows.
            var statusRef = CiteIucnRenderer.Render(Parts, citeOptions with { WrapInRef = true });
            var lines = SpeciesboxStatus.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.CriteriaVersion, statusRef);
            boxes.Add(new WikitextBox("wikitext-speciesbox", SiteText.LabelSpeciesbox, "{{Speciesbox}}", lines, Rows: 5));
        }
        Boxes = boxes;
    }

    // citation_json written by `site build-db`. Unknown properties are ignored, so nothing but the
    // citation parts can reach the page.
    private static IucnCitationParts? ReadParts(string? json) {
        try {
            return IucnCitationParts.FromJson(json);
        } catch (JsonException) {
            return null;
        }
    }

    private void LoadNames() {
        var names = _queries.GetNames(Taxon!.TaxonId);

        EnglishNames = names
            .Where(n => n.NameType == NameTypes.Common && IsEnglish(n.Language))
            .GroupBy(n => SiteNameKey.Fold(n.Name))
            .Select(g => {
                var rows = g.ToList();
                var shown = rows.FirstOrDefault(r => r.Source == "iucn" && r.IsPreferred) ?? rows[0];
                var sources = rows.Select(r => r.Source).Distinct().OrderBy(SourceOrder).Select(SiteText.SourceLabel).ToList();
                return new EnglishCommonName(shown.Name, sources, rows.Any(r => r.Source == "iucn" && r.IsPreferred));
            })
            .OrderByDescending(n => n.IsIucnMain)
            .ThenByDescending(n => Taxon.CommonNameEn is not null && SiteNameKey.Fold(n.Name) == SiteNameKey.Fold(Taxon.CommonNameEn))
            .ThenBy(n => n.Name, StringComparer.CurrentCultureIgnoreCase)
            .ToList();

        OtherLanguages = names
            .Where(n => n.NameType == NameTypes.Common && !IsEnglish(n.Language))
            .GroupBy(n => string.IsNullOrWhiteSpace(n.Language) ? SiteText.LanguageNotGiven : LanguageNames.Name(n.Language))
            .Select(g => new LanguageGroup(g.Key, g
                .GroupBy(n => SiteNameKey.Fold(n.Name))
                .Select(ng => ng.First().Name)
                .OrderBy(n => n, StringComparer.CurrentCultureIgnoreCase)
                .ToList()))
            .OrderBy(g => g.Language == SiteText.LanguageNotGiven)
            .ThenBy(g => g.Language, StringComparer.CurrentCultureIgnoreCase)
            .ToList();

        Synonyms = names
            .Where(n => n.NameType == NameTypes.Synonym)
            .GroupBy(n => SiteNameKey.Fold(n.Name))
            .Select(g => g.First().Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
    }

    private static bool IsEnglish(string? language) =>
        string.Equals(language?.Trim(), "en", StringComparison.OrdinalIgnoreCase)
        || (language?.Trim().StartsWith("en-", StringComparison.OrdinalIgnoreCase) ?? false);

    private static int SourceOrder(string source) => source switch {
        "iucn" => 0,
        "wikidata" => 1,
        "col" => 2,
        "wikipedia" => 3,
        _ => 4,
    };

    private void LoadArrival(string? q) {
        var text = SiteEndpoints.NormalizeQuery(q);
        if (text.Length < 2) {
            return;
        }
        // Only say "X is a synonym of this taxon" when it is one, so the line cannot be used to put
        // arbitrary text on the page.
        var type = _queries.NameTypeFor(Taxon!.TaxonId, text);
        if (type is NameTypes.Synonym or NameTypes.Common) {
            ArrivedQuery = text;
            ArrivedNameType = type;
        }
    }

    private static string? ReadDataDateRange(SiteSnapshot? snapshot) {
        if (snapshot is null
            || !SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom), out var from)
            || !SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedTo), out var to)) {
            return null;
        }
        return SiteFormat.DateRange(from, to);
    }
}
