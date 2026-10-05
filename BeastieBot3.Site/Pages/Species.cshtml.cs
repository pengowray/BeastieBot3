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

/// A synonym with the authority shown beside it (the first source's that gives one) and its sources.
/// A source whose authority differs from the one shown has it in OtherAuthority.
public sealed record SynonymSource(string Label, string? OtherAuthority);

public sealed record SynonymRow(string Name, string? Authority, IReadOnlyList<SynonymSource> Sources);

/// Common names in one language. Lang: the code for the lang attribute, or null.
public sealed record LanguageGroup(string Language, string? Lang, IReadOnlyList<string> Names, bool NotGiven = false);

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

[OutputCache(PolicyName = SiteCachePolicies.Species)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesModel : PageModel {
    /// Name lists longer than this show the rest behind "Show N more".
    public const int ShortListLength = 10;

    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;
    private readonly SiteOptions _options;
    private readonly ILogger<SpeciesModel> _logger;

    public SpeciesModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options, ILogger<SpeciesModel> logger) {
        _db = db;
        _queries = queries;
        _options = options.Value;
        _logger = logger;
    }

    public long RequestedTaxonId { get; private set; }
    public string? Version { get; private set; }
    public TaxonRow? Taxon { get; private set; }
    public TaxonSummary? Parent { get; private set; }

    /// The groups the taxon is in, kingdom first, with the Catalogue of Life groups between IUCN's
    /// ranks. Empty for a taxon not in the release, whose page lists its ranks as IUCN gave them.
    public IReadOnlyList<GroupRow> Classification { get; private set; } = [];

    /// The Catalogue of Life's English names of the classification's groups that have no English name of their own.
    public IReadOnlyDictionary<int, IReadOnlyList<string>> ClassificationColNames { get; private set; } =
        new Dictionary<int, IReadOnlyList<string>>();

    /// False for a taxon that is not in the release: an old IUCN id, or a taxon IUCN no longer
    /// assesses. Its page has no status summary and no latest assessment.
    public bool InRelease => Taxon?.InRelease ?? true;

    /// The taxa linked to this one in taxon_link. For a taxon not in the release: the taxa in the
    /// release with its name, or that IUCN lists its name as a synonym of. For a taxon in the
    /// release: the taxa not in the release (old ids) linked to it in those ways.
    public IReadOnlyList<TaxonLinkRow> LinkedTaxa { get; private set; } = [];

    /// The global assessments of this taxon and of the linked taxa in one table, shown in place of
    /// the assessment history; null when no linked taxon has a global assessment.
    public CombinedHistory? Combined { get; private set; }

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

    public IReadOnlyList<EnglishCommonName> EnglishNames { get; private set; } = [];
    public IReadOnlyList<LanguageGroup> OtherLanguages { get; private set; } = [];
    public IReadOnlyList<SynonymRow> Synonyms { get; private set; } = [];

    /// For a subspecies, variety or subpopulation: its species, with that species' latest global assessment.
    public RelatedTaxonRow? SpeciesRow { get; private set; }

    /// For a subspecies or variety in the release with no parent taxon: IUCN has not assessed its
    /// species. The species' name ("Asellus aquaticus").
    public string? UnassessedSpeciesName { get; private set; }

    /// For a species: its subspecies, varieties and subpopulations. For any other taxon: the other
    /// subspecies, varieties and subpopulations of its species.
    public IReadOnlyList<RelatedTaxonRow> RelatedTaxa { get; private set; } = [];

    public bool IsSpecies => Taxon?.Kind == TaxonKinds.Species;

    /// Set when the visitor arrived from a search for a synonym or common name of this taxon.
    public string? ArrivedQuery { get; private set; }
    public string? ArrivedNameType { get; private set; }

    /// True when the visitor searched for the common name shown under the heading. The page then
    /// leaves out the sentence saying it is a common name of this taxon and keeps only the link to
    /// all the results, as the search list leaves out its match note in the same case.
    public bool ArrivedNameIsShown { get; private set; }

    public string? DataDateRange { get; private set; }

    public IActionResult OnGet(long taxonId, long? assessment, string? authors, string? access, string? opts,
        [FromQuery(Name = "ref")] string? wrapRef, string? refname, string? amp, string? fullnames, string? q) {
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
        if (Taxon.NodeId is { } nodeId) {
            Classification = _queries.GetGroupPath(nodeId);
            ClassificationColNames = _queries.GetGroupColNames(Classification.Where(g => g.CommonNameEn is null).Select(g => g.NodeId).ToList());
        }
        LinkedTaxa = _queries.GetLinkedTaxa(Taxon.TaxonId);
        EpbcListings = _queries.GetEpbcListings(Taxon.TaxonId);
        LoadAssessments(assessment);
        Combined = CombinedHistory.Build(Taxon, LinkedTaxa,
            id => id == Taxon.TaxonId ? _assessments : _queries.GetAssessments(id), _queries.GetTaxonomicNotesFlags);
        Options = WikitextOptions.FromQuery(authors, access, opts, wrapRef, refname, amp,
            Selected is null ? DefaultRefNames.LatestGlobal : DefaultRefNameFor(Selected), fullnames);
        Taxobox = TaxoboxTemplate.For(Taxon.Kind, Taxon.Kingdom);
        BuildWikitext();
        LoadNames();
        LoadRelatedTaxa();
        LoadArrival(q);
        DataDateRange = ReadDataDateRange(snapshot);
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, $"/species/{Taxon.TaxonId}");
        return Page();
    }

    private void LoadRelatedTaxa() {
        var taxon = Taxon!;
        if (taxon.Kind == TaxonKinds.Species) {
            RelatedTaxa = _queries.GetChildren(taxon.TaxonId);
        } else if (taxon.ParentTaxonId is { } parentId) {
            SpeciesRow = _queries.GetRelated(parentId);
            RelatedTaxa = _queries.GetChildren(parentId).Where(r => r.Taxon.TaxonId != taxon.TaxonId).ToList();
        } else if (taxon.InRelease && taxon.Genus is not null && taxon.SpeciesEpithet is not null) {
            UnassessedSpeciesName = $"{taxon.Genus} {taxon.SpeciesEpithet}";
            RelatedTaxa = _queries.GetUnassessedSpeciesSiblings(taxon);
        }
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

    /// The "Show wikitext" link of a combined history row under another IUCN id: that id's page with
    /// the assessment and the current options. The ref name goes along only when the visitor chose
    /// it, as on this page; otherwise the default that page gives the assessment applies.
    public string OtherIdOptionsUrl(CombinedRow row) {
        var other = row.Id.Taxon;
        var targetDefault = DefaultRefNames.For(row.Assessment, other.InRelease ? other.LatestGlobalAssessmentId : null, row.Id.Global);
        return $"/species/{other.TaxonId}{Options.ToQuery(row.Assessment.AssessmentId, targetDefault)}";
    }

    /// The data-options-link key of a combined history row under another IUCN id, which cannot be
    /// one of this page's own keys (an assessment id, or "default").
    public static string OtherIdOptionsLinkKey(CombinedRow row) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{row.Id.TaxonId}-{row.Assessment.AssessmentId}");

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
            // status_ref is always a <ref>, whatever the citation box shows.
            var statusRef = CiteIucnRenderer.Render(Parts, citeOptions with { WrapInRef = true });
            var lines = SpeciesboxStatus.Render(Selected.Category, Selected.PossiblyExtinct, Selected.PossiblyExtinctInTheWild,
                Selected.CriteriaVersion, statusRef);
            boxes.Add(new WikitextBox("wikitext-speciesbox", Taxobox.Label, Taxobox.Name, lines, Rows: 5));
        }
        Boxes = boxes;

        Wikidata = WikidataCite.Build(Selected, Parts, Taxon?.WikidataQid, ReadItemModel(), Options.ToCiteQOptions(today, downloaded),
            (what, e) => _logger.LogWarning(e, "WikidataCitation.{Method} failed for assessment {AssessmentId}", what, Selected.AssessmentId),
            hasPage: id => _assessments.Any(a => a.AssessmentId == id), taxonItemDoubt: TaxonItemDoubt.Of(Taxon));
        if (Taxon is not null && Taxon.InRelease && LatestGlobal is not null && Selected.AssessmentId == LatestGlobal.AssessmentId) {
            WikidataStatus = WikidataCite.BuildStatus(Taxon, LatestGlobal, Parts,
                (what, e) => _logger.LogWarning(e, "{Method} failed for taxon {TaxonId}", what, Taxon.TaxonId));
        }
    }

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
            var replacing = _assessments.FirstOrDefault(a => a.AssessmentId == replacedBy)
                ?? Combined?.Rows.FirstOrDefault(r => r.Assessment.AssessmentId == replacedBy)?.Assessment;
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

        Synonyms = SynonymRows(names);
    }

    /// The synonyms, one row per name (folded), sources in SourceOrder, by name.
    public static IReadOnlyList<SynonymRow> SynonymRows(IEnumerable<NameRow> names) => names
        .Where(n => n.NameType == NameTypes.Synonym)
        .GroupBy(n => SiteNameKey.Fold(n.Name))
        .Select(g => {
            var rows = g.OrderBy(r => SourceOrder(r.Source)).ThenBy(r => r.NameId).ToList();
            var authority = rows.Select(r => r.Authority).FirstOrDefault(a => a is not null);
            var sources = rows
                .GroupBy(r => r.Source)
                .Select(s => {
                    var own = s.Select(r => r.Authority).FirstOrDefault(a => a is not null);
                    var other = own is not null && authority is not null && !SameAuthority(own, authority) ? own : null;
                    return new SynonymSource(SiteText.SourceLabel(s.Key), other);
                })
                .ToList();
            return new SynonymRow(rows[0].Name, authority, sources);
        })
        .OrderBy(s => s.Name, StringComparer.Ordinal)
        .ToList();

    // Authorities that differ only in spacing or case are the same.
    private static bool SameAuthority(string a, string b) => SiteNameKey.Fold(a) == SiteNameKey.Fold(b);

    private static int SourceOrder(string source) => source switch {
        "iucn" => 0,
        "wikidata" => 1,
        "col" => 2,
        "wikipedia" => 3,
        "wikipedia-taxobox" => 4,
        _ => 5,
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
