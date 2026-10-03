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

/// Common names in one language. Lang: the code for the lang attribute, or null.
public sealed record LanguageGroup(string Language, string? Lang, IReadOnlyList<string> Names, bool NotGiven = false);

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

    /// False for a taxon that is not in the release: an old IUCN id, or a taxon IUCN no longer
    /// assesses. Its page has no status summary and no latest assessment.
    public bool InRelease => Taxon?.InRelease ?? true;

    /// For a taxon not in the release: the taxon in the release with the same scientific name.
    public TaxonSummary? CurrentTaxon { get; private set; }

    /// For a taxon in the release: the taxa not in the release that have its scientific name.
    public IReadOnlyList<TaxonSummary> EarlierIds { get; private set; } = [];

    /// SPRAT profiles and EPBC Act listings: the whole taxon's first, then populations'.
    public IReadOnlyList<EpbcListingRow> EpbcListings { get; private set; } = [];

    public IReadOnlyList<AssessmentRow> GlobalHistory { get; private set; } = [];
    /// The rows of the Regional assessments table. For a taxon in the release, the latest assessment
    /// in each region. For a taxon not in the release, none is current, so every regional assessment,
    /// by region and then newest first.
    public IReadOnlyList<AssessmentRow> RegionalRows { get; private set; } = [];
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

    /// The taxobox an article about this taxon most likely uses, which names the status parameters box.
    public TaxoboxTemplate Taxobox { get; private set; } = TaxoboxTemplate.Speciesbox;

    public IReadOnlyList<EnglishCommonName> EnglishNames { get; private set; } = [];
    public IReadOnlyList<LanguageGroup> OtherLanguages { get; private set; } = [];
    public IReadOnlyList<string> Synonyms { get; private set; } = [];
    public IReadOnlyList<TaxonListItem> Children { get; private set; } = [];

    /// Set when the visitor arrived from a search for a synonym or common name of this taxon.
    public string? ArrivedQuery { get; private set; }
    public string? ArrivedNameType { get; private set; }

    /// True when the visitor searched for the common name shown under the heading. The page then
    /// leaves out the sentence saying it is a common name of this taxon and keeps only the link to
    /// all the results, as the search list leaves out its match note in the same case.
    public bool ArrivedNameIsShown { get; private set; }

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
        if (!Taxon.InRelease && Taxon.CurrentTaxonId is { } currentId) {
            CurrentTaxon = _queries.GetSummary(currentId);
        }
        if (Taxon.InRelease) {
            EarlierIds = _queries.GetEarlierIds(Taxon.TaxonId);
        }
        EpbcListings = _queries.GetEpbcListings(Taxon.TaxonId);
        LoadAssessments(assessment);
        Options = WikitextOptions.FromQuery(authors, access, opts, wrapRef, refname, amp,
            Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected));
        Taxobox = TaxoboxTemplate.For(Taxon.Kind, Taxon.Kingdom);
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
        LatestGlobal = !Taxon.InRelease ? null
            : GlobalHistory.FirstOrDefault(a => a.AssessmentId == Taxon.LatestGlobalAssessmentId)
                ?? (Taxon.LatestGlobalAssessmentId is null ? null : GlobalHistory.FirstOrDefault(a => a.IsLatest));

        var regional = all.Where(a => !a.IsGlobal);
        RegionalRows = Taxon.InRelease
            // Latest per region: the row flagged latest, or the newest one when none is.
            ? regional
                .GroupBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
                .Select(g => g.FirstOrDefault(a => a.IsLatest) ?? g.First())
                .OrderBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
                .ToList()
            : regional
                .OrderBy(a => a.Scope.Trim(), StringComparer.OrdinalIgnoreCase)
                .ThenByDescending(a => a.YearPublished ?? 0)
                .ThenByDescending(a => a.AssessmentDate, StringComparer.Ordinal)
                .ThenByDescending(a => a.AssessmentId)
                .ToList();

        // A taxon not in the release has no status summary: none of its assessments is current.
        StatusAssessment = !Taxon.InRelease ? null : LatestGlobal ?? RegionalRows
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
            // status_ref is always a <ref>, whatever the citation box shows.
            var statusRef = CiteIucnRenderer.Render(Parts, citeOptions with { WrapInRef = true });
            var lines = SpeciesboxStatus.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.CriteriaVersion, statusRef);
            boxes.Add(new WikitextBox("wikitext-speciesbox", Taxobox.Label, Taxobox.Name, lines, Rows: 5));
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

    private readonly Dictionary<long, IucnCitationParts?> _partsById = [];

    private IucnCitationParts? PartsOf(AssessmentRow assessment) {
        if (!_partsById.TryGetValue(assessment.AssessmentId, out var parts)) {
            parts = ReadParts(assessment.CitationJson);
            _partsById[assessment.AssessmentId] = parts;
        }
        return parts;
    }

    /// A short note for the assessment tables when the assessment is an errata or amended version,
    /// or was replaced by one ("Replaced by the errata version"); null otherwise. An errata version
    /// keeps the year of the assessment it replaces, so without the note the two rows look the same.
    public string? VersionNote(AssessmentRow assessment) {
        if (assessment.ReplacedByAssessmentId is { } replacedBy) {
            var replacing = _assessments.FirstOrDefault(a => a.AssessmentId == replacedBy);
            var replacingParts = replacing is null ? null : PartsOf(replacing);
            return replacingParts?.ErrataYear is not null ? SiteText.ReplacedByErrata
                : replacingParts?.AmendsYear is not null ? SiteText.ReplacedByAmended
                : SiteText.ReplacedByLater;
        }
        var parts = PartsOf(assessment);
        if (parts?.ErrataYear is { } errataYear) {
            return SiteText.ErrataVersion(errataYear);
        }
        if (parts?.AmendsYear is { } amendsYear) {
            return SiteText.AmendedVersion(amendsYear);
        }
        return null;
    }

    private void LoadNames() {
        var names = _queries.GetNames(Taxon!.TaxonId);

        EnglishNames = names
            .Where(n => n.NameType == NameTypes.Common && LanguageNames.IsEnglish(n.Language))
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

        // Grouped by code, not by display name, so each group can carry its lang attribute.
        // Names with no language ("und" included) come last.
        OtherLanguages = names
            .Where(n => n.NameType == NameTypes.Common && !LanguageNames.IsEnglish(n.Language))
            .GroupBy(n => LanguageNames.Key(n.Language))
            .Select(g => new LanguageGroup(LanguageNames.Name(g.Key), LanguageNames.LangAttribute(g.Key), g
                .GroupBy(n => SiteNameKey.Fold(n.Name))
                .Select(ng => ng.First().Name)
                .OrderBy(n => n, StringComparer.CurrentCultureIgnoreCase)
                .ToList(), NotGiven: g.Key.Length == 0))
            .OrderBy(g => g.NotGiven)
            .ThenBy(g => g.Language, StringComparer.CurrentCultureIgnoreCase)
            .ToList();

        Synonyms = names
            .Where(n => n.NameType == NameTypes.Synonym)
            .GroupBy(n => SiteNameKey.Fold(n.Name))
            .Select(g => g.First().Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
    }

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
            ArrivedNameIsShown = type == NameTypes.Common && Taxon.CommonNameEn is not null
                && SiteNameKey.Fold(Taxon.CommonNameEn) == SiteNameKey.Fold(text);
        }
    }

    /// True when the EPBC Act lists the whole taxon under a name other than its IUCN name
    /// (the southern cassowary is listed as Casuarius casuarius johnsonii).
    public bool ListedUnderOtherName(EpbcListingRow listing) =>
        !listing.IsPopulation && Taxon is not null
        && SiteNameKey.Fold(ScientificNameMarkupWords(listing.ListedName)) != SiteNameKey.Fold(ScientificNameMarkupWords(Taxon.ScientificName));

    // The name without IUCN's rank markers, so "Panthera pardus ssp. orientalis" equals SPRAT's
    // "Panthera pardus orientalis".
    private static string ScientificNameMarkupWords(string name) =>
        string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries).Where(w => w is not ("ssp." or "subsp." or "var.")));

    /// A name the EPBC Act lists a taxon under, as HTML: wholly italic when it is a plain binomial or
    /// trinomial ("Casuarius casuarius johnsonii", which SPRAT writes without a rank marker), else
    /// marked up as IUCN names are.
    public static string ListedNameHtml(string name) {
        var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var plain = words.Length is 2 or 3 && words.Skip(1).All(w => w.All(c => char.IsLower(c) || c == '-'));
        return plain ? "<i>" + SiteHtml.Encode(string.Join(' ', words)) + "</i>" : ScientificNameMarkup.ToHtml(name);
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
