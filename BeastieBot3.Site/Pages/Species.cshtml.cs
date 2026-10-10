using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

/// The taxobox status comparison and the latest global assessment it compares with, for _TaxoboxStatus.
public sealed record TaxoboxStatusView(TaxoboxStatusCheck Check, AssessmentRow? Latest);

/// A taxon page (/species/{id}): the taxon's status, its assessments, other statuses, names,
/// classifications and links, with links to the tools at the bottom. The wikitext for citing its
/// assessments is on its wikitext page (SpeciesWikitextModel); an address with wikitext options
/// (from before that page existed) is sent there.
[OutputCache(PolicyName = SiteCachePolicies.Species)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class SpeciesModel : TaxonPageModel {
    /// Name lists longer than this show the rest behind "Show N more".
    public const int ShortListLength = 10;

    public SpeciesModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options) : base(db, queries, options) {
    }

    /// Species from the Catalogue of Life and Wikidata that may be the same as this taxon.
    public IReadOnlyList<ExtraPairRow> ExtraPairs { get; private set; } = [];

    /// ExtraPairs without the ones the page shows elsewhere: a provisional name's pair under Possible
    /// synonyms, and a "_new" record's pair in the note under the name.
    public IReadOnlyList<ExtraPairRow> DuplicatePairs =>
        ExtraPairs.Where(p => p.Reason is not (ExtraOverlapReasons.ProvisionalName or ExtraOverlapReasons.WorkingName)).ToList();

    /// For a taxon IUCN named with "_new" after a name ("Aquilegia ottonis_new"): the name without
    /// it. Null for any other name.
    public string? WorkingNameBase { get; private set; }

    /// For such a taxon when IUCN has no taxon with the name without "_new": the species from the
    /// Catalogue of Life or Wikidata with that name, or null.
    public ExtraSpeciesRow? WorkingNameExtra { get; private set; }
    /// The groups the taxon is in, kingdom first, with the Catalogue of Life groups between IUCN's
    /// ranks. Empty for a taxon not in the release, whose page lists its ranks as IUCN gave them.
    public IReadOnlyList<GroupRow> Classification { get; private set; } = [];

    /// The taxon's ranks in IUCN, on this site, in the Catalogue of Life and in Wikidata, side by
    /// side; empty when only IUCN's are known.
    public IReadOnlyList<ComparisonRow> ComparativeClassification { get; private set; } = [];
    public IReadOnlyList<LadderColumn> RankColumns { get; private set; } = [];

    private IReadOnlyList<ComparisonRow> BuildComparativeClassification(TaxonRow taxon) {
        var leafRank = taxon.Kind switch { TaxonKinds.Species => "species", TaxonKinds.Variety => "variety", TaxonKinds.Subspecies => "subspecies", _ => null };
        if (leafRank is null) {
            return [];
        }
        var leaf = new LadderStep(leafRank, taxon.ScientificName);
        string?[] iucnRanks = [taxon.Kingdom, taxon.Phylum, taxon.ClassName, taxon.OrderName, taxon.Family, taxon.Genus];
        // IUCN's ranks, each linked to this site's page of the group.
        string? GroupUrl(string rank, string name) => Classification
            .FirstOrDefault(g => g.Rank == rank && string.Equals(g.Name, name, StringComparison.OrdinalIgnoreCase)) is { } g ? Web.SiteUrls.Group(g) : null;
        var iucn = iucnRanks.Select((name, i) => name is null ? null
                : new LadderStep(ClassificationComparison.MainRanks[i], SiteFormat.TitleCase(name), GroupUrl(ClassificationComparison.MainRanks[i], name)))
            .OfType<LadderStep>().Append(leaf).ToList();
        var columns = new List<LadderColumn> { new(SiteText.RanksIucn, null, iucn, Backbone: true) };
        if (taxon.ColId is { } colId && _queries.GetLadder("col", colId) is { Count: > 0 } col) {
            columns.Add(new LadderColumn(SiteText.RanksCol, SiteFormat.CatalogueOfLifeUrl(colId),
                [.. col.Select(s => new LadderStep(s.Rank == "unranked" ? null : s.Rank, s.Name, SiteFormat.CatalogueOfLifeUrl(s.Id)))], Backbone: true));
        }
        if (taxon.WikidataQid is { } qid && _queries.GetLadder("wikidata", qid) is { Count: > 0 } wikidata) {
            columns.Add(new LadderColumn(SiteText.RanksWikidata, SiteFormat.WikidataUrl(qid),
                [.. wikidata.Select(s => new LadderStep(s.Rank, s.Name, SiteFormat.WikidataUrl(s.Id)))]));
        }
        if (taxon.EnwikiTitle is { } article && _queries.GetLadder("wikipedia", "article:" + article) is { Count: > 0 } wikipedia) {
            columns.Add(new LadderColumn(SiteText.RanksWikipedia, SiteFormat.WikipediaUrl(article),
                [.. wikipedia.Select(s => new LadderStep(s.Rank, s.Name,
                    s.Id.StartsWith("article:", StringComparison.Ordinal) ? null : SiteFormat.WikipediaUrl("Template:Taxonomy/" + s.Id)))],
                Help: SiteText.RanksWikipediaHelp));
        }
        // Wikispecies: the page of IUCN's name, through any redirect (site build-db keys it by that name).
        if (_queries.GetLadder("wikispecies", "page:" + taxon.ScientificName) is { Count: > 0 } wikispecies) {
            WikispeciesTitle = wikispecies[^1].Name;
            columns.Add(new LadderColumn(SiteText.RanksWikispecies, SiteFormat.WikispeciesUrl(wikispecies[^1].Name),
                [.. wikispecies.Select(s => new LadderStep(s.Rank, s.Name, SiteFormat.WikispeciesUrl(s.Name)))], Help: SiteText.RanksWikispeciesHelp));
        }
        if (columns.Count < 2) {
            return [];
        }
        RankColumns = columns;
        return ClassificationComparison.Build(columns);
    }

    /// The Catalogue of Life's English names of the classification's groups that have no English name of their own.
    public IReadOnlyDictionary<int, IReadOnlyList<string>> ClassificationColNames { get; private set; } =
        new Dictionary<int, IReadOnlyList<string>>();

    /// SPRAT profiles and EPBC Act listings: the whole taxon's first, then populations'.
    public IReadOnlyList<EpbcListingRow> EpbcListings { get; private set; } = [];

    /// Statuses in lists other than the IUCN Red List (other_status), grouped by country in the order
    /// of OtherStatusSystems.All.
    private IReadOnlyList<OtherStatusRow> OtherStatuses { get; set; } = [];

    /// The "Other conservation statuses" section built from OtherStatuses; null when there are none.
    public OtherStatusSection? OtherStatusSection { get; private set; }

    /// The date the site database's copy of a source of the other statuses was downloaded
    /// ("25 June 2026"); null when unknown.
    private string? OtherStatusSourceDate(string source) {
        var snapshot = _db.Snapshot;
        switch (source) {
            case OtherStatusSources.Sprat: {
                var report = snapshot?.Get(SiteDbSchema.MetaKeys.SpratReport);
                // SpratReportDate gives the file name back when the name has no date.
                return AboutModel.SpratReportDate(report) is { } date && date != Path.GetFileName(report!.Trim()) ? date : null;
            }
            case OtherStatusSources.Ecos:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.EcosFetched), out var ecos) ? SiteFormat.Date(ecos) : null;
            case OtherStatusSources.NatureServe:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.NatureServeFetched), out var ns) ? SiteFormat.Date(ns) : null;
            case OtherStatusSources.France:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.FranceFetched), out var france) ? SiteFormat.Date(france) : null;
            case OtherStatusSources.Japan:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.JapanFetched), out var japan) ? SiteFormat.Date(japan) : null;
            case OtherStatusSources.Cites:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.CitesFetched), out var cites) ? SiteFormat.Date(cites) : null;
            case OtherStatusSources.Salve:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.SalveFetched), out var br) ? SiteFormat.Date(br) : null;
            case OtherStatusSources.Nztcs:
                return SiteFormat.TryParseDate(snapshot?.Get(SiteDbSchema.MetaKeys.NztcsFetched), out var nz) ? SiteFormat.Date(nz) : null;
            default:
                return null;
        }
    }

    /// The taxon's Wikimedia Commons gallery and category ("Category:Panthera leo"), from its Wikidata item.
    public string? CommonsGallery { get; private set; }
    public string? CommonsCategory { get; private set; }

    /// The taxon's Wikispecies page (from its classification there), or null.
    public string? WikispeciesTitle { get; private set; }

    /// Links to the taxon in other databases (from the ids on its Wikidata item), in the order of
    /// ExternalDatabases, one link per database.
    public IReadOnlyList<(string Name, string Url)> ExternalLinks { get; private set; } = [];

    /// The citation parts of the assessment in the status summary, for IUCN's own citation text.
    public IucnCitationParts? StatusParts { get; private set; }

    /// The people and organisations IUCN credits for the assessment in the status summary; null when it has none.
    public CreditsView? StatusCredits { get; private set; }

    /// The genus this taxon is in, for the link to the genus's list page; null when the site has no group for it.
    public GroupRow? GenusGroup => Classification.LastOrDefault(g => g.IsGenus);

    /// English common names, names in other languages and synonyms, with their sources.
    public TaxonNames Names { get; private set; } = new([], [], []);

    /// For a species in the release: its subspecies and varieties from IUCN and the other sources in
    /// infraspecific_name; null for any other taxon and when no source lists one.
    public SubspeciesList? SubspeciesList { get; private set; }

    /// For a subspecies, variety or subpopulation: its species, with that species' latest global assessment.
    public RelatedTaxonRow? SpeciesRow { get; private set; }

    /// For a subspecies or variety in the release with no parent taxon: IUCN has not assessed its
    /// species. The species' name ("Asellus aquaticus").
    public string? UnassessedSpeciesName { get; private set; }

    /// For a species: its subspecies, varieties and subpopulations. For any other taxon: the other
    /// subspecies, varieties and subpopulations of its species.
    public IReadOnlyList<RelatedTaxonRow> RelatedTaxa { get; private set; } = [];

    /// Set when the visitor arrived from a search for a synonym or common name of this taxon.
    public string? ArrivedQuery { get; private set; }
    public string? ArrivedNameType { get; private set; }
    /// The name that matched, as stored ("CROW" for "bird code: crow"), and its source.
    public string? ArrivedName { get; private set; }
    public string? ArrivedNameSource { get; private set; }

    /// True when the visitor searched for the common name shown under the heading. The page then
    /// leaves out the sentence saying it is a common name of this taxon and keeps only the link to
    /// all the results, as the search list leaves out its match note in the same case.
    public bool ArrivedNameIsShown { get; private set; }


    public IActionResult OnGet(long taxonId, string? q) {
        // An address with wikitext options goes to the wikitext page.
        if (SiteCachePolicies.SpeciesQueryKeys.Any(Request.Query.ContainsKey)) {
            var query = QueryString.Create(Request.Query.Where(p => p.Key != "q"));
            return Redirect(SpeciesWikitextModel.PathFor(taxonId, query.ToUriComponent()));
        }
        if (!LoadTaxon(taxonId)) {
            return Page();
        }
        var taxon = Taxon!;
        if (taxon.NodeId is { } nodeId) {
            Classification = _queries.GetGroupPath(nodeId);
            ClassificationColNames = _queries.GetGroupColNames(Classification.Where(g => g.CommonNameEn is null).Select(g => g.NodeId).ToList());
        }
        ComparativeClassification = BuildComparativeClassification(taxon);
        EpbcListings = _queries.GetEpbcListings(taxon.TaxonId);
        OtherStatuses = _queries.GetOtherStatuses(taxon.TaxonId);
        OtherStatusSection = OtherStatuses.Count == 0 ? null : Display.OtherStatusSection.Build(OtherStatuses, taxon.Kind, OtherStatusSourceDate);
        ExtraPairs = _queries.GetExtraOverlapsOfTaxon(taxon.TaxonId);
        WorkingNameBase = SiteFormat.WorkingNameBase(taxon.ScientificName);
        WorkingNameExtra = WorkingNameOf is null
            ? ExtraPairs.FirstOrDefault(p => p.Reason == ExtraOverlapReasons.WorkingName)?.Extra
            : null;
        var externalIds = _queries.GetExternalIds(taxon.TaxonId);
        (CommonsGallery, CommonsCategory) = BeastieBot3.Shared.SiteData.ExternalDatabases.Commons(externalIds);
        ExternalLinks = BeastieBot3.Shared.SiteData.ExternalDatabases.Links(externalIds);
        if (StatusAssessment is { } status) {
            StatusParts = PartsOf(status);
            StatusCredits = CreditsView.Build(_queries.GetCredits(status.AssessmentId));
        }
        Names = TaxonNames.Build(_queries.GetNames(taxon.TaxonId), taxon.CommonNameEn);
        Names = Names with { PossibleSynonyms = PossibleSynonyms(Names.Synonyms) };
        if (taxon.InRelease) {
            // An old id's IUCN synonyms are already links in the combined history (taxon_link).
            var iucnSynonyms = Names.Synonyms.Where(s => s.FromIucn).Select(s => s.Name).ToList();
            Names = Names.WithSameNameTaxa(_queries.GetTaxaNamed(iucnSynonyms, taxon.Kingdom, taxon.TaxonId)) with {
                ListedAsSynonymBy = _queries.GetTaxaWithIucnSynonym(taxon.ScientificName, taxon.Kingdom, taxon.TaxonId),
            };
        }
        // An old id's notes often name the taxa it was split into, so its page lists them too.
        Names = Names with {
            TaxonId = taxon.TaxonId,
            ScientificName = taxon.ScientificName,
            SubpopulationName = taxon.SubpopulationName,
            NotesTaxa = NotesTaxa.Build(taxon.TaxonId, _queries.GetNotesTaxa(taxon.TaxonId)),
            Version = Version,
        };
        SubspeciesList = SubspeciesRows.Load(_queries, taxon);
        LoadRelatedTaxa();
        LoadArrival(q);
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, $"/species/{taxon.TaxonId}");
        return Page();
    }

    // The other IUCN taxa linked by a provisional name, then the species from the Catalogue of Life
    // and Wikidata named with this taxon's quoted epithet; not a name that a source lists as a
    // synonym (Heptapleurum nanocephalum is in its taxobox's synonyms).
    private List<PossibleSynonym> PossibleSynonyms(IReadOnlyList<SynonymRow> synonyms) {
        var listed = synonyms.Select(s => SiteNameKey.Fold(s.Name)).ToHashSet(StringComparer.Ordinal);
        var list = LinkedTaxa.Where(l => l.IsProvisionalName)
            .Select(l => new PossibleSynonym(l.Taxon.ScientificName, $"/species/{l.Taxon.TaxonId}", IsProvisional: !l.IsFrom, l.Taxon.TaxonId))
            .ToList();
        list.AddRange(ExtraPairs.Where(p => p.Reason == ExtraOverlapReasons.ProvisionalName && p.Extra is not null)
            .Select(p => new PossibleSynonym(p.Extra!.ScientificName, SiteUrls.Extra(p.Extra), IsProvisional: false, null, p.Extra.InCol, p.Extra.InWikidata)));
        return list.Where(p => !listed.Contains(SiteNameKey.Fold(p.Name))).ToList();
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

    private void LoadArrival(string? q) {
        var text = SiteEndpoints.NormalizeQuery(q);
        if (text.Length < 2) {
            return;
        }
        // Only say "X is a synonym of this taxon" when it is one, so the line cannot be used to put
        // arbitrary text on the page.
        if (_queries.NameMatchFor(Taxon!.TaxonId, text) is not { } found) {
            return;
        }
        var type = found.Type;
        if (type is NameTypes.Synonym or NameTypes.Common or NameTypes.Code) {
            ArrivedQuery = text;
            ArrivedNameType = type;
            ArrivedName = found.Name;
            ArrivedNameSource = found.Source;
            ArrivedNameIsShown = type == NameTypes.Common && Taxon.CommonNameEn is not null
                && SiteNameKey.Fold(Taxon.CommonNameEn) == SiteNameKey.Fold(text);
        }
    }
}
