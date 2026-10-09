using System.Diagnostics;
using System.Globalization;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.WikipediaLists.Legacy;
using Microsoft.Data.Sqlite;
using Spectre.Console;

// Builds the public site's database from the local caches (`site build-db`). The phases, in order:
//
//   1. IUCN CSV export: every taxon (the backbone) and its latest assessments.
//   2. IUCN API taxon records: assessment history, IUCN common names and synonyms, and the taxa
//      that are only in the API cache (not in the release), each linked to the taxa in the release
//      that have its name or list it as a synonym (SiteTaxonLinks).
//   3. Plan the assessment rows.
//   4. DOI sources: GBIF's checklist, Wikidata, and the DOIs `iucn resolve-dois` found in Crossref's
//      list of IUCN DOIs or at doi.org. Also the Wikidata items for assessments.
//   5. IUCN API assessment payloads: citation parts; the assessment rows are written here.
//   6. Common names store: English names, the best English name, CoL synonyms.
//   7. Links: English Wikipedia, Wikidata, Catalogue of Life (and the release's citation), SPRAT
//      (with the EPBC Act and state and territory statuses of its profiles).
//   8. Parents, then the taxon, name and taxon link rows, the classification in other sources, the
//      subspecies and varieties of each species in CoL and Wikidata (SiteSubspecies), then meta.
//   9. Name keys, indexes, full-text index, ANALYZE, VACUUM.
//
// The database is written to "<output>.building" and moved over the output only when every phase
// has finished, so a failed or cancelled build leaves the old database as it was.

namespace BeastieBot3.SiteBuild;

internal sealed class SiteDbBuild {
    private readonly SiteBuildInputs _inputs;
    private readonly IAnsiConsole _console;
    private readonly SiteBuildStats _stats = new();
    // The lists of the status systems that have several (other_status_list), from the status list readers.
    private readonly List<OtherStatusList> _otherStatusLists = new();

    public SiteDbBuild(SiteBuildInputs inputs, IAnsiConsole console) {
        _inputs = inputs;
        _console = console;
    }

    public static string TempPathFor(string output) => output + ".building";

    public SiteBuildStats Run(CancellationToken cancellationToken) {
        var total = Stopwatch.StartNew();
        var tempPath = TempPathFor(_inputs.Output);
        try {
            BuildInto(tempPath, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            SqliteConnection.ClearAllPools();
            // A journal left beside the old file would be applied to the new one when it is next
            // opened, so remove any before the new file takes the name.
            foreach (var sibling in new[] { "-journal", "-wal", "-shm" }) {
                if (File.Exists(_inputs.Output + sibling)) {
                    File.Delete(_inputs.Output + sibling);
                }
            }
            File.Move(tempPath, _inputs.Output, overwrite: true);
            _stats.FileBytes = new FileInfo(_inputs.Output).Length;
        } finally {
            SqliteConnection.ClearAllPools();
            if (File.Exists(tempPath)) {
                SiteDbWriter.DeleteDatabaseFiles(tempPath);
            }
        }
        _stats.Phases.Add(("Total", total.Elapsed));
        return _stats;
    }

    private void BuildInto(string tempPath, CancellationToken ct) {
        using var writer = SiteDbWriter.Create(tempPath);

        // 1. The CSV export.
        List<SiteTaxon> taxonList = null!;
        List<SiteAssessment> csvAssessments = null!;
        Phase("Reading the IUCN CSV export", () => {
            using var csv = SiteIucnCsvReader.OpenReadOnly(_inputs.IucnDatabase);
            _stats.IucnRelease = SiteIucnCsvReader.ReadRelease(csv);
            taxonList = SiteIucnCsvReader.ReadTaxa(csv, _inputs.Limit, ct);
            var byId = taxonList.ToDictionary(t => t.TaxonId);
            csvAssessments = SiteIucnCsvReader.ReadAssessments(csv, byId, _stats, ct);
            return $"{taxonList.Count:N0} taxa, {csvAssessments.Count:N0} latest assessments, release {_stats.IucnRelease}";
        });
        var taxa = taxonList.ToDictionary(t => t.TaxonId);
        foreach (var taxon in taxonList) {
            _stats.Count(_stats.TaxaByKind, taxon.Kind);
        }

        using var cache = SiteIucnCsvReader.OpenReadOnly(_inputs.ApiCache);

        // 2. API taxon records, and the taxa only in the API cache.
        var apiTaxa = new SiteApiTaxaReader(taxa, _stats);
        Phase("Reading IUCN API taxon records", () => {
            apiTaxa.Read(cache, readAll: _inputs.Limit is null, ct);
            return $"{apiTaxa.Records.Count:N0} taxa have their own record, {apiTaxa.NotInRelease.Count:N0} of them not in the release";
        });
        taxonList.AddRange(apiTaxa.NotInRelease);
        taxonList.Sort((a, b) => a.TaxonId.CompareTo(b.TaxonId));
        var taxonLinks = SiteTaxonLinks.Find(taxonList, _stats);

        // 3. Plan.
        var assessments = new SiteAssessmentPass(taxa, apiTaxa.Records, _stats);
        Dictionary<long, List<SiteHistoryEntry>> globalHistory = null!;
        Phase("Planning assessment rows", () => {
            assessments.Plan(csvAssessments);
            csvAssessments = null!;
            globalHistory = assessments.GlobalHistory();
            return $"{assessments.PlannedCount:N0} assessments";
        });

        // 4. DOI sources.
        var dois = new SiteDoiSources();
        Optional("GBIF checklist", _inputs.GbifChecklist, path => {
            var gbif = SiteLinkReaders.ReadGbif(path, taxa, dois, ct);
            _stats.GbifVersion = gbif.Version;
            _stats.GbifPublished = gbif.Published;
            _stats.GbifCitation = gbif.Citation;
            _stats.GbifDoi = gbif.Doi;
            return $"{dois.Gbif.Count:N0} DOIs, version {gbif.Version}, published {gbif.Published}, dataset DOI {gbif.Doi}";
        });
        Optional("Wikidata cache", _inputs.WikidataCache, path => {
            SiteLinkReaders.ReadWikidata(path, taxa, _stats, dois, ct);
            return $"{_stats.QidsFromP627 + _stats.QidsFromNameMatch:N0} taxa with an item, {dois.Wikidata.Count:N0} assessments with a DOI, "
                + $"{_stats.WikidataItems.ByAssessment.Count:N0} items for assessments";
        });
        Optional("Wikidata cache: taxon synonyms (P1420)", _inputs.WikidataCache, path => {
            SiteLinkReaders.ReadWikidataSynonyms(path, taxa, _stats, ct);
            return $"{_stats.WikidataSynonymItems:N0} synonym items, {_stats.WikidataSynonymsNamed:N0} with a name in the cache";
        });
        Optional("checklists store: Mammal Diversity Database and AmphibiaWeb names", _inputs.Checklists, path => {
            SiteChecklistNames.Read(path, taxa, _stats);
            return $"{_stats.ChecklistTaxa:N0} taxa matched, {_stats.ChecklistCommonNames:N0} English names, {_stats.ChecklistSynonyms:N0} synonyms";
        });
        Optional("DOI cache (iucn resolve-dois)", _inputs.DoiCache, path => {
            SiteLinkReaders.ReadDoiCache(path, dois, _stats, ct);
            var newest = _stats.DoiCheckedTo is { } at ? at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : "none";
            return $"{_stats.DoiChecksRead:N0} assessments checked, {_stats.DoiChecksWithDoi:N0} with a DOI, newest check {newest}";
        });

        // 5. Payloads; the assessment rows are written here.
        Phase("Reading IUCN API assessment payloads", () => {
            assessments.ReadApiNotFound(cache);
            assessments.WriteAll(cache, writer, dois, ct);
            return $"{_stats.CitationsParsed:N0} citations parsed, {_stats.AssessmentsGlobalLatest + _stats.AssessmentsRegionalLatest + _stats.AssessmentsHistory:N0} assessments written";
        });

        // 6. Common names store.
        var colCrossReferences = new Dictionary<long, string>();
        Optional("common names store", _inputs.CommonNames, path => {
            LegacyTaxaRuleList? overrides = null;
            if (_inputs.RulesList is { } rules && File.Exists(rules)) {
                overrides = new LegacyTaxaRuleList(rules);
            } else {
                _stats.MissingSources.Add("rules-list.txt");
            }
            SiteCommonNamesReader.Read(path, taxa, overrides, _stats, colCrossReferences, ct);
            return $"{_stats.CommonNameEn:N0} taxa with an English name for display";
        });

        // 7. Links.
        Optional("Wikipedia cache", _inputs.WikipediaCache, path => {
            SiteLinkReaders.ReadWikipedia(path, taxa, _stats, ct);
            return $"{_stats.EnwikiTitles:N0} taxa with an article";
        });
        Optional("Wikipedia cache: taxobox names and synonyms", _inputs.WikipediaCache, path => {
            SiteLinkReaders.ReadWikipediaTaxoboxSynonyms(path, taxa, _stats, ct);
            return $"{_stats.WikipediaTaxoboxSynonyms:N0} names, {_stats.WikipediaTaxoboxPagesShared:N0} pages of several taxa skipped";
        });
        Optional("Catalogue of Life placement file", _inputs.ColPlacement, path => {
            _stats.ColRelease = SiteLinkReaders.ReadColPlacement(path, taxa, _stats, ct);
            return $"{_stats.ColIdsFromPlacement:N0} species with a Catalogue of Life id";
        });
        SiteLinkReaders.ApplyColCrossReferences(taxa, colCrossReferences, _stats);
        Optional("Catalogue of Life database: synonym authorities", _inputs.ColDatabase, path => {
            SiteLinkReaders.ReadColSynonymAuthorities(path, taxa, _stats, ct);
            return $"{_stats.ColSynonymAuthorities:N0} CoL synonyms with an authority";
        });
        _stats.ColRelease ??= SiteBuildRules.ColReleaseFromPath(_inputs.ColDatabase);
        Optional("Catalogue of Life ColDP folder", _inputs.ColDir, path => {
            var col = ColReleaseCitation.Find(path, _stats.ColRelease, out var warning);
            if (col is null) {
                _stats.Warnings.Add(warning!);
                return "no citation";
            }
            _stats.ColRelease ??= col.Alias;
            _stats.ColCitation = col.Citation;
            _stats.ColDoi = col.Doi;
            return $"citation of {col.Alias}, DOI {col.Doi ?? "none"}";
        });
        Optional("SPRAT database", _inputs.SpratDatabase, path => {
            _stats.SpratReport = SiteLinkReaders.ReadSprat(path, taxa, _stats, ct);
            return $"{_stats.SpratMatched:N0} taxa matched by name, {_stats.EpbcStatuses:N0} with an EPBC status, "
                + $"{_stats.SpratPopulationProfiles:N0} population profiles ({_stats.EpbcPopulationListings:N0} listed), "
                + $"{_stats.StateStatuses:N0} state and territory statuses";
        });

        Optional("status lists store", _inputs.StatusListsDatabase, path => {
            (_stats.NatureServeFetched, _stats.EcosFetched, _stats.NztcsFetched) = SiteLinkReaders.ReadStatusLists(path, taxa, _stats, _otherStatusLists, ct);
            var natureServeMatched = _stats.NatureServeByName + _stats.NatureServeBySynonym + _stats.NatureServeByIucnSynonym;
            return $"{natureServeMatched:N0} taxa matched to NatureServe records ({_stats.NatureServeRanks:N0} global ranks, "
                + $"{_stats.CosewicStatuses:N0} COSEWIC, {_stats.SaraStatuses:N0} SARA), "
                + $"{_stats.EcosMatched:N0} of {_stats.EcosListings:N0} ECOS listings matched, "
                + $"{_stats.NztcsMatched:N0} of {_stats.NztcsAssessments:N0} NZTCS assessments matched, "
                + $"{_stats.SalveMatched:N0} of {_stats.SalveAssessments:N0} SALVE assessments matched";
        });

        // IUCN's summary tables, before the names are written, which clears the synonym lists the name
        // index reads.
        Optional("IUCN summary tables (iucn summary-tables)", _inputs.SummaryTables, path => {
            var result = ReadSummaryTables(path, taxa, globalHistory, writer);
            if (result is null) return "no tables stored";
            _stats.SummaryTables = result;
            return $"{result.Changes.Count:N0} assessments with a reason for their change of category "
                + $"({result.ChangeRowsLinked:N0} of {result.ChangeRows:N0} Table 7 rows linked), "
                + $"{result.Listings.Count:N0} Possibly Extinct listings ({result.ListedWithoutTag:N0} on assessments without the tag)";
        });
        globalHistory = null!;

        // Common names in other languages, after every synonym list is read: the rules compare the
        // names with the synonyms. A name that starts with the genus is a binomial only when its next
        // word is an epithet in the common names store.
        Func<string, bool>? isEpithet = null;
        if (_inputs.CommonNames is { } commonNamesPath && File.Exists(commonNamesPath)) {
            using var store = CommonNames.CommonNameStore.OpenReadOnly(commonNamesPath);
            var words = store.LoadNameWordSets();
            isEpithet = word => words.IsEpithet(word.ToLowerInvariant());
        }
        Optional("Catalogue of Life database: common names in other languages", _inputs.ColDatabase, path => {
            if (!SiteOtherLanguageNames.ReadCol(path, taxa, _stats.ColOtherNames, isEpithet, ct)) {
                _stats.Warnings.Add("The Catalogue of Life database has no vernacularname table, so the site has no Catalogue of Life names in languages other than English.");
                return "no vernacularname table";
            }
            return OtherNamesSummary(_stats.ColOtherNames);
        });
        Optional("Wikidata cache: common names in other languages and Wikipedia titles", _inputs.WikidataCache, path => {
            SiteOtherLanguageNames.ReadWikidata(path, taxa, _stats.WikidataOtherNames, _stats.WikipediaOtherNames, isEpithet, ct);
            return $"Wikidata: {OtherNamesSummary(_stats.WikidataOtherNames)}; Wikipedia titles: {OtherNamesSummary(_stats.WikipediaOtherNames)}";
        });

        // 8. Parents, the tree of groups and list links, then taxa, names, meta.
        SetParents(taxonList, taxa, apiTaxa.SubpopulationParents);
        SiteTaxonTree tree = null!;
        Phase("Building the tree of groups", () => {
            var placement = ReadPlacement(ct);
            tree = SiteTaxonTree.Build(taxonList, placement, _inputs.NotAssignedRules);
            _stats.TreeNodes = tree.Nodes.Count;
            _stats.TreeColGroups = tree.Nodes.Count(n => n.Source == GroupSources.Col);
            _stats.TreeRuleGroups = tree.Nodes.Count(n => n.Source == GroupSources.IucnRule);
            _stats.TreeTaxaUnderRuleOrder = tree.TaxaUnderRuleOrder;
            _stats.TreeTaxaUnderRuleFamily = tree.TaxaUnderRuleFamily;
            _stats.TreeTaxaWithUnassignedRank = tree.TaxaWithUnassignedRank;
            _stats.TreeTaxaWithoutKingdom = tree.TaxaWithoutKingdom;
            return $"{tree.Nodes.Count:N0} groups, {_stats.TreeColGroups:N0} of them from the Catalogue of Life";
        });
        Phase("Naming the groups and finding the articles list lines link", () => {
            NameGroupsAndLinks(taxonList, tree, ct);
            return $"{_stats.GroupCommonNames:N0} groups with an English name, {_stats.GroupArticles:N0} with an article, "
                + $"{_stats.GroupsWithColNames:N0} with Catalogue of Life English names, {_stats.GroupsWithWikipediaNames:N0} with names from English Wikipedia; "
                + $"{_stats.ListArticleTitles:N0} taxa with an article for list lines";
        });

        // Species from the Catalogue of Life and Wikidata that IUCN does not have. Before the names are
        // written, which clears the synonym lists the overlap rules read.
        ExtraSpecies.SiteExtraSpeciesBuild? extras = null;
        if (_inputs.ExtraSpecies != ExtraSpecies.ExtraPlacement.None) {
            Phase("Adding species from the Catalogue of Life and Wikidata", () => {
                extras = ExtraSpecies.SiteExtraSpeciesBuild.Run(taxonList, taxa, tree, _inputs.ExtraSpecies, _inputs.ColDatabase,
                    _inputs.ColPlacement, _inputs.WikidataCache, step => _console.MarkupLineInterpolated($"[grey]  {step}...[/]"), ct);
                _stats.ExtraSpecies = extras.Stats;
                _stats.Warnings.AddRange(extras.Warnings);
                var e = extras.Stats;
                return $"{extras.Entries.Count:N0} species ({e.ColEntries:N0} only in CoL, {e.WikidataEntries:N0} only in Wikidata, "
                    + $"{e.BothEntries:N0} in both), {extras.Overlaps.Count:N0} possible overlaps";
            });
        }

        var greenStatuses = new List<SiteGreenStatus>();
        Phase("Reading IUCN Green Status assessments", () => {
            greenStatuses = SiteGreenStatusReader.Read(_inputs.ApiCache, taxa, _stats, ct);
            return $"{_stats.GreenStatusTaxa:N0} of {_stats.GreenStatusRecords:N0} taxa with a Green Status assessment are on the site";
        });

        Phase("Writing taxa and names", () => {
            writer.AddHigherTaxa(tree.Nodes);
            foreach (var taxon in taxonList) {
                ct.ThrowIfCancellationRequested();
                writer.AddTaxon(taxon);
                foreach (var listing in taxon.EpbcListings) {
                    writer.AddEpbcListing(taxon.TaxonId, listing);
                }
                foreach (var status in taxon.OtherStatuses) {
                    writer.AddOtherStatus(taxon.TaxonId, status);
                }
                WriteNames(writer, taxon);
            }
            foreach (var link in taxonLinks) {
                writer.AddTaxonLink(link);
            }
            writer.AddGreenStatuses(greenStatuses);
            foreach (var list in _otherStatusLists) {
                writer.AddOtherStatusList(list);
            }
            if (extras is not null) {
                ExtraSpecies.ExtraSpeciesWriter.Write(writer, extras);
            }
            return $"{taxonList.Count:N0} taxa, {writer.NameCount:N0} names, {taxonLinks.Count:N0} links from old ids";
        });

        Optional("Wikidata cache: ids in other databases", _inputs.WikidataCache, path => {
            var rows = SiteExternalIds.Read(path,
                taxonList.Where(t => t.InRelease && t.WikidataQid is not null).Select(t => (t.TaxonId, t.WikidataQid!)), ct);
            writer.InsertRows("INSERT OR IGNORE INTO taxon_external_id (taxon_id, property, value) VALUES (@t, @p, @v)", ["@t", "@p", "@v"],
                rows.Select(r => new object?[] { r.TaxonId, r.Property, r.Value }));
            return $"{rows.Select(r => r.TaxonId).Distinct().Count():N0} taxa, {rows.Count:N0} ids";
        });
        Optional("Wikipedia cache: taxobox statuses", _inputs.WikipediaCache, path => {
            var rows = SiteTaxoboxStatusReader.Read(path, taxonList.Where(t => t.InRelease && t.SubpopulationName is null), ct);
            writer.InsertRows(
                "INSERT INTO enwiki_taxobox_status (taxon_id, status, status_system, ref_assessment_id, revision_id, downloaded) VALUES (@t, @s, @y, @a, @r, @d)",
                ["@t", "@s", "@y", "@a", "@r", "@d"],
                rows.Select(r => new object?[] { r.TaxonId, r.Status, r.StatusSystem, r.RefAssessmentId, r.RevisionId, r.Downloaded }));
            return $"{rows.Count(r => r.Status is not null):N0} taxa whose article's taxobox has an IUCN status, {rows.Count(r => r.Status is null):N0} with none";
        });

        // The classification in other sources, for the comparison of ranks on the species page.
        const string ladderInsert = "INSERT OR IGNORE INTO ladder_node (source, id, parent_id, rank, name) VALUES (@source, @id, @parent, @rank, @name)";
        string[] ladderParameters = ["@source", "@id", "@parent", "@rank", "@name"];
        Optional("Catalogue of Life database: classification", _inputs.ColDatabase, path => {
            var nodes = SiteLadders.ReadCol(path, taxonList.Select(t => t.ColId).OfType<string>(), ct);
            writer.InsertRows(ladderInsert, ladderParameters, nodes.Select(n => new object?[] { n.Source, n.Id, n.ParentId, n.Rank, n.Name }));
            _stats.ColLadderNodes = nodes.Count;
            return $"{nodes.Count:N0} nodes";
        });
        Optional("Wikidata cache: classification", _inputs.WikidataCache, path => {
            var ranks = SiteLadders.ReadRankNames(_inputs.WikidataRanks);
            var nodes = SiteLadders.ReadWikidata(path, taxonList.Select(t => t.WikidataQid).OfType<string>(), ranks, ct);
            writer.InsertRows(ladderInsert, ladderParameters, nodes.Select(n => new object?[] { n.Source, n.Id, n.ParentId, n.Rank, n.Name }));
            _stats.WikidataLadderNodes = nodes.Count;
            return $"{nodes.Count:N0} nodes, {ranks.Count:N0} rank names";
        });
        Optional("Wikipedia cache: taxobox classification", _inputs.WikipediaCache, path => {
            var nodes = SiteLadders.ReadWikipedia(path, taxonList.Select(t => t.EnwikiTitle).OfType<string>(), ct);
            writer.InsertRows(ladderInsert, ladderParameters, nodes.Select(n => new object?[] { n.Source, n.Id, n.ParentId, n.Rank, n.Name }));
            return $"{nodes.Count(n => n.Id.StartsWith(SiteLadders.ArticlePrefix, StringComparison.Ordinal)):N0} articles, {nodes.Count:N0} nodes";
        });
        Optional("Wikispecies cache: classification", _inputs.WikispeciesCache, path => {
            var nodes = SiteLadders.ReadWikispecies(path, taxonList.Select(t => t.ScientificName), ct);
            writer.InsertRows(ladderInsert, ladderParameters, nodes.Select(n => new object?[] { n.Source, n.Id, n.ParentId, n.Rank, n.Name }));
            return $"{nodes.Count(n => n.Id.StartsWith(SiteLadders.PagePrefix, StringComparison.Ordinal) && !n.Id.Contains('#')):N0} taxon pages, {nodes.Count:N0} nodes";
        });

        // The subspecies and varieties of each species in the Catalogue of Life and Wikidata, for the
        // species page's list, which adds IUCN's own from the taxon table.
        var subspecies = _stats.Subspecies;
        Optional("Catalogue of Life database: subspecies and varieties", _inputs.ColDatabase, path => {
            var rows = SiteSubspecies.ReadCol(path, taxonList, subspecies, ct);
            writer.InsertRows(SiteSubspecies.Insert, SiteSubspecies.InsertParameters, rows.Select(SiteSubspecies.InsertValues));
            return $"{subspecies.ColRows:N0} subspecies and varieties of {subspecies.ColSpecies.Count:N0} species "
                + $"({subspecies.ColSpeciesRead:N0} species with a CoL ID read, {subspecies.ColUnreadable:N0} names not read)";
        });
        Optional("Wikidata cache: subspecies and varieties", _inputs.WikidataCache, path => {
            var rows = SiteSubspecies.ReadWikidata(path, taxonList, subspecies, out var warning, ct);
            if (warning is not null) {
                _stats.Warnings.Add(warning);
            }
            if (rows is null) {
                return "no taxon sweep";
            }
            writer.InsertRows(SiteSubspecies.Insert, SiteSubspecies.InsertParameters, rows.Select(SiteSubspecies.InsertValues));
            return $"{subspecies.WikidataRows:N0} subspecies and varieties of {subspecies.WikidataSpecies.Count:N0} species "
                + $"({subspecies.WikidataLeftOutAsSynonym:N0} items named as a synonym and {subspecies.WikidataLeftOutByInstance:N0} synonym or fossil items left out, "
                + $"{subspecies.WikidataKeptAsMutualSynonym:N0} items that name each other as a synonym kept, "
                + $"{subspecies.WikidataUnreadable:N0} names not read)";
        });
        SiteSubspecies.CountLists(taxonList, taxa, subspecies);
        var subspeciesSummary = $"{subspecies.SpeciesWithList:N0} species with a list ({subspecies.SpeciesWithSeveralSources:N0} from two or more sources); "
            + $"rows from IUCN {subspecies.IucnRows:N0}, the Catalogue of Life {subspecies.ColRows:N0}, Wikidata {subspecies.WikidataRows:N0}";
        _console.MarkupLineInterpolated($"  Subspecies and varieties: {subspeciesSummary}");
        WriteMeta(writer, taxonList.Count);

        // 9. Indexes and compaction.
        writer.Finish((name, body) => Phase(name, () => {
            ct.ThrowIfCancellationRequested();
            body();
            return null;
        }));
    }

    // ------------------------------------------------------------ tree of groups

    // The CoL groups of the placement built from this IUCN database and the current rules. When
    // the placement file only has one built from an older copy of the file, an older CoL file or
    // older rules, that one is used and the build warns.
    private SitePlacement ReadPlacement(CancellationToken ct) {
        var placement = SiteGroupTree.ReadPlacement(_inputs.IucnDatabase, _inputs.ColDatabase, _inputs.ColPlacement,
            _inputs.NotAssignedRules, out var state, out var warning, ct);
        if (state is not null) {
            _stats.ColPlacementState = state;
        }
        if (warning is not null) {
            _stats.Warnings.Add(warning);
        }
        return placement;
    }

    // English names and articles of the groups, as the Wikipedia list headings choose them, and the
    // articles list lines link for each taxon. Uses whichever of the common names store, the
    // Wikipedia cache and the CoL database are there.
    private void NameGroupsAndLinks(List<SiteTaxon> taxonList, SiteTaxonTree tree, CancellationToken ct) {
        var legacy = _inputs.RulesList is { } rulesPath && File.Exists(rulesPath)
            ? new LegacyTaxaRuleList(rulesPath)
            : LegacyTaxaRuleList.Empty();
        var taxonRules = _inputs.TaxonRules is { } yaml ? WikipediaLists.TaxonRulesService.Load(yaml) : null;
        var wikiCache = _inputs.WikipediaCache is { } wiki && File.Exists(wiki) ? wiki : null;
        using var provider = _inputs.CommonNames is { } names && File.Exists(names)
            ? new WikipediaLists.StoreBackedCommonNameProvider(names, wikiCache)
            : null;
        using var titles = wikiCache is null ? null : WikipediaLists.EnwikiTitleCheck.OpenReadOnly(wikiCache);
        var headings = new WikipediaLists.HeadingFormatter(legacy, taxonRules, provider);
        IReadOnlyDictionary<string, string> capsRules = new Dictionary<string, string>();
        if (_inputs.CommonNames is { } storePath && File.Exists(storePath)) {
            using var store = CommonNames.CommonNameStore.OpenReadOnly(storePath);
            capsRules = store.GetAllCapsRules();
        }
        SiteGroupNames.Resolve(tree.Nodes, headings, titles, _inputs.ColDatabase, capsRules, _stats, ct);
        if (wikiCache is not null && Wikipedia.WikipediaCacheStore.OpenReadOnly(wikiCache) is { } cache) {
            using (cache) {
                var scientificNames = new HashSet<string>(StringComparer.Ordinal);
                foreach (var node in tree.Nodes) {
                    scientificNames.Add(Shared.SiteData.SiteNameKey.Fold(node.Name));
                }
                var taxonArticles = new HashSet<string>(StringComparer.Ordinal);
                var englishNamePositions = new Dictionary<string, List<int>>(StringComparer.Ordinal);
                foreach (var taxon in taxonList) {
                    scientificNames.Add(Shared.SiteData.SiteNameKey.Fold(taxon.ScientificName));
                    if (taxon.EnwikiTitle is { } article) {
                        taxonArticles.Add(Wikipedia.WikipediaTitleHelper.Normalize(article));
                    }
                    if (taxon.CommonNameEn is { } english && taxon.TreePos is { } pos) {
                        var key = Shared.SiteData.SiteNameKey.Fold(english);
                        if (!englishNamePositions.TryGetValue(key, out var list)) {
                            englishNamePositions[key] = list = new List<int>();
                        }
                        list.Add(pos);
                    }
                }
                SiteGroupWikipediaNames.Resolve(tree.Nodes, cache, scientificNames, taxonArticles, englishNamePositions, _stats, ct);
            }
        }
        var lines = new WikipediaLists.SpeciesLineFormatter(legacy, provider, commonNameProvider: null);
        SiteListLinks.Resolve(taxonList, lines, _stats, ct);
    }

    // ------------------------------------------------------------ parents

    // The species of an infraspecific taxon, by "Genus species" among the database's species (same
    // kingdom first, taxa in the release first); the taxon a subpopulation belongs to, by its name
    // without the subpopulation part. When the name finds nothing, the API record's species
    // (species_taxa) or the record that lists the subpopulation. For a taxon not in the release, the
    // API record's species comes first when that species is in the release. Only a taxon that is in
    // the site database is stored as the parent.
    private void SetParents(List<SiteTaxon> taxonList, IReadOnlyDictionary<long, SiteTaxon> taxa,
        IReadOnlyDictionary<long, long> subpopulationParents) {
        var byName = new Dictionary<string, List<SiteTaxon>>(StringComparer.Ordinal);
        foreach (var taxon in taxonList.OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            if (taxon.Kind == SiteTaxonKind.Subpopulation) {
                continue;
            }
            if (!byName.TryGetValue(taxon.ScientificName, out var list)) {
                byName[taxon.ScientificName] = list = new List<SiteTaxon>();
            }
            list.Add(taxon);
        }

        foreach (var taxon in taxonList) {
            string? parentName = taxon.Kind switch {
                SiteTaxonKind.Subpopulation when taxon.SubpopulationName is not null =>
                    SiteBuildRules.SubpopulationParentName(taxon.ScientificName, taxon.SubpopulationName),
                SiteTaxonKind.Subspecies or SiteTaxonKind.Variety when taxon.Genus is not null && taxon.SpeciesEpithet is not null =>
                    $"{taxon.Genus} {taxon.SpeciesEpithet}",
                _ => null,
            };
            if (taxon.Kind == SiteTaxonKind.Species) {
                continue;
            }
            if (!taxon.InRelease && taxon.ApiSpeciesId is { } apiSpecies && apiSpecies != taxon.TaxonId
                && taxa.TryGetValue(apiSpecies, out var species) && species.InRelease) {
                taxon.ParentTaxonId = apiSpecies;
                _stats.ParentsFromApi++;
                continue;
            }
            if (parentName is not null && byName.TryGetValue(parentName, out var candidates)) {
                var parent = candidates.FirstOrDefault(c => c.TaxonId != taxon.TaxonId && c.Kingdom == taxon.Kingdom)
                    ?? candidates.FirstOrDefault(c => c.TaxonId != taxon.TaxonId);
                if (parent is not null) {
                    taxon.ParentTaxonId = parent.TaxonId;
                    _stats.ParentsByName++;
                    continue;
                }
            }
            long? fallback = taxon.Kind == SiteTaxonKind.Subpopulation
                ? (subpopulationParents.TryGetValue(taxon.TaxonId, out var root) ? root : null)
                : taxon.ApiSpeciesId;
            if (fallback is { } id && id != taxon.TaxonId && taxa.ContainsKey(id)) {
                taxon.ParentTaxonId = id;
                _stats.ParentsFromApi++;
            } else {
                _stats.ParentsMissing++;
            }
        }
    }

    // ------------------------------------------------------------ names

    // Order: the scientific name, IUCN's common names (main first), the other English names, the
    // other sources' names in other languages (CoL, Wikidata, Wikipedia), IUCN's synonyms, then
    // CoL's. The order sets name_id, which the site uses to order names within a list.
    private void WriteNames(SiteDbWriter writer, SiteTaxon taxon) {
        var names = new SiteNameSet();
        names.Add(taxon.ScientificName, SiteNameType.Scientific, null, SiteNameSource.Iucn, isPreferred: true);
        foreach (var name in taxon.IucnCommonNames) {
            names.Add(name.Name, SiteNameType.Common, name.Language, SiteNameSource.Iucn, name.IsMain);
        }
        foreach (var (name, source, preferred) in taxon.EnglishNames
                     .OrderBy(n => SourceOrder(n.Source))) {
            names.Add(name, SiteNameType.Common, "en", source, preferred);
        }
        foreach (var name in taxon.OtherLanguageNames.OrderBy(n => OtherLanguageSourceOrder(n.Source))) {
            names.Add(name.Name, SiteNameType.Common, name.Language, name.Source);
        }
        foreach (var synonym in taxon.IucnSynonyms) {
            names.Add(synonym.Name, SiteNameType.Synonym, null, SiteNameSource.Iucn, authority: synonym.Authority);
        }
        foreach (var synonym in taxon.ColSynonyms) {
            names.Add(synonym.Name, SiteNameType.Synonym, null, SiteNameSource.Col, authority: synonym.Authority);
        }
        foreach (var synonym in taxon.WikidataSynonyms) {
            names.Add(synonym.Name, SiteNameType.Synonym, null, SiteNameSource.Wikidata, authority: synonym.Authority);
        }
        foreach (var synonym in taxon.WikipediaSynonyms) {
            names.Add(synonym.Name, SiteNameType.Synonym, null, SiteNameSource.WikipediaTaxobox, authority: synonym.Authority);
        }
        foreach (var (name, type, authority, source) in taxon.ChecklistNames) {
            names.Add(name, type == "common" ? SiteNameType.Common : SiteNameType.Synonym, type == "common" ? "en" : null, source, authority: authority);
        }
        var merged = new HashSet<(string?, string)>();
        foreach (var name in names.Names) {
            writer.AddName(taxon.TaxonId, name);
            _stats.Count(_stats.NamesByType, name.NameType);
            if (name.NameType == SiteNameType.Common && name.Language == "en") {
                _stats.CommonNamesEnglish++;
            } else if (name.NameType == SiteNameType.Common) {
                _stats.Count(_stats.OtherLanguageNameRows, name.Source);
                if (merged.Add((name.Language, SiteNameKey.CaseFold(name.Name)))) {
                    _stats.OtherLanguageNamesMerged++;
                }
                if (name.Language is { } language) {
                    _stats.OtherLanguages.Add(language);
                }
            }
        }
        _stats.CommonNamesJunk += names.JunkCommonNames;
        _stats.CommonNamesRepaired += names.RepairedCommonNames;
        // The lists are not needed again.
        taxon.IucnCommonNames = new List<IucnCommonName>();
        taxon.IucnSynonyms = new List<SiteSynonym>();
        taxon.EnglishNames.Clear();
        taxon.ColSynonyms.Clear();
        taxon.WikidataSynonyms.Clear();
        taxon.WikipediaSynonyms.Clear();
        taxon.OtherLanguageNames.Clear();
        taxon.OtherLanguageNames.TrimExcess();
    }

    private static int OtherLanguageSourceOrder(string source) => source switch {
        SiteNameSource.Col => 0,
        SiteNameSource.Wikidata => 1,
        _ => 2,
    };

    private static string OtherNamesSummary(OtherNameCounts counts) =>
        $"{counts.Kept:N0} of {counts.Read:N0} names kept ({counts.English:N0} English, {counts.LanguageLeftOut:N0} with no language or "
        + $"one the site cannot name, {counts.Dropped.Values.Sum():N0} scientific names)";

    private static int SourceOrder(string source) => source switch {
        SiteNameSource.Iucn => 0,
        SiteNameSource.Wikipedia => 1,
        SiteNameSource.WikipediaTaxobox => 2,
        SiteNameSource.Wikidata => 3,
        SiteNameSource.Col => 4,
        _ => 5,
    };

    // ------------------------------------------------------------ meta

    private void WriteMeta(SiteDbWriter writer, int taxonCount) {
        var assessmentCount = writer.Scalar("SELECT COUNT(*) FROM assessment");
        writer.SetMeta(SiteDbSchema.MetaKeys.SchemaVersion, SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.BuiltAtUtc, DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnRelease, _stats.IucnRelease);
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom, _stats.DownloadedFrom?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnApiDownloadedTo, _stats.DownloadedTo?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistVersion, _stats.GbifVersion);
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistPublished, _stats.GbifPublished);
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistCitation, _stats.GbifCitation);
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistDoi, _stats.GbifDoi);
        writer.SetMeta(SiteDbSchema.MetaKeys.ColRelease, _stats.ColRelease);
        writer.SetMeta(SiteDbSchema.MetaKeys.MddVersion, _stats.MddVersion);
        writer.SetMeta(SiteDbSchema.MetaKeys.AmphibiaWebVersion, _stats.AmphibiaWebVersion);
        writer.SetMeta(SiteDbSchema.MetaKeys.ColCitation, _stats.ColCitation);
        writer.SetMeta(SiteDbSchema.MetaKeys.ColDoi, _stats.ColDoi);
        writer.SetMeta(SiteDbSchema.MetaKeys.SpratReport, _stats.SpratReport);
        writer.SetMeta(SiteDbSchema.MetaKeys.NatureServeFetched, _stats.NatureServeFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.EcosFetched, _stats.EcosFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.NztcsFetched, _stats.NztcsFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.SalveFetched, _stats.SalveFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.CitesFetched, _stats.CitesFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.CitesCitation, _stats.CitesCitation);
        writer.SetMeta(SiteDbSchema.MetaKeys.JnccFetched, _stats.JnccFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.JapanFetched, _stats.JapanFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.FranceFetched, _stats.FranceFetched);
        writer.SetMeta(SiteDbSchema.MetaKeys.JnccFileDate, _stats.JnccFileDate);
        writer.SetMeta(SiteDbSchema.MetaKeys.JnccAttribution, _stats.JnccAttribution);
        writer.SetMeta(SiteDbSchema.MetaKeys.GreenStatusFetched, _stats.GreenStatusFetched);
        if (_stats.SummaryTables is { } summaryTables) {
            // Tables lists each table's files in release order.
            string? First(int table) => summaryTables.Tables.FirstOrDefault(t => t.Table == table)?.Release;
            string? Last(int table) => summaryTables.Tables.LastOrDefault(t => t.Table == table)?.Release;
            writer.SetMeta(SiteDbSchema.MetaKeys.Table7FirstVersion, First(7));
            writer.SetMeta(SiteDbSchema.MetaKeys.Table7LastVersion, Last(7));
            writer.SetMeta(SiteDbSchema.MetaKeys.Table9FirstVersion, First(9));
            writer.SetMeta(SiteDbSchema.MetaKeys.Table9LastVersion, Last(9));
        }
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnDoiCheckedTo, _stats.DoiCheckedTo?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.WikidataItemModel, _inputs.WikidataItemModel.ToJson());
        _stats.WikidataItemModelSource = _inputs.WikidataItemModelSource;
        writer.SetMeta(SiteDbSchema.MetaKeys.NotAssignedRules, _inputs.NotAssignedRules.IsEmpty ? null : _inputs.NotAssignedRules.Fingerprint);
        writer.SetMeta(SiteDbSchema.MetaKeys.ColPlacementState, _stats.ColPlacementState);
        if (_stats.ExtraSpecies is { } extraStats) {
            writer.SetMeta(SiteDbSchema.MetaKeys.ExtraSpeciesPlacement, extraStats.Placement.ToString().ToLowerInvariant());
            writer.SetMeta(SiteDbSchema.MetaKeys.WikidataSweepFinished, extraStats.WikidataSweepFinished);
        }
        writer.SetMeta(SiteDbSchema.MetaKeys.TaxonCount, taxonCount.ToString(CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.AssessmentCount, assessmentCount);
    }

    // ------------------------------------------------------------ IUCN summary tables

    private static SiteSummaryTablesResult? ReadSummaryTables(string path, IReadOnlyDictionary<long, SiteTaxon> taxa,
        IReadOnlyDictionary<long, List<SiteHistoryEntry>> globalHistory, SiteDbWriter writer) {
        using var store = Iucn.SummaryTables.SummaryTableStore.OpenReadOnly(path);
        if (store is null) return null;
        var sources = store.GetSources();
        if (sources.Count == 0) return null;
        var result = SiteSummaryTables.Link(sources.Values, store.ReadCategoryChanges(), store.ReadPossiblyExtinct(),
            new StatusListNameIndex(taxa.Values), globalHistory);
        writer.InsertRows(
            "INSERT INTO summary_table (summary_table_id, table_no, release, url, last_updated) VALUES (@id, @t, @r, @u, @d)",
            ["@id", "@t", "@r", "@u", "@d"],
            result.Tables.Select(t => new object?[] { t.Id, t.Table, t.Release, t.Url, t.LastUpdated }));
        writer.InsertRows(
            "INSERT INTO category_change (assessment_id, taxon_id, reason, previous_assessment_id, old_category, new_category, red_list_version, summary_table_id) "
            + "VALUES (@a, @t, @r, @p, @o, @n, @v, @s)",
            ["@a", "@t", "@r", "@p", "@o", "@n", "@v", "@s"],
            result.Changes.Select(c => new object?[] { c.AssessmentId, c.TaxonId, c.Reason, c.PreviousAssessmentId, c.OldCategory,
                c.NewCategory, c.RedListVersion, c.SummaryTableId }));
        writer.InsertRows(
            "INSERT INTO possibly_extinct_listing (assessment_id, tag, taxon_id, first_release, last_release, tables, summary_table_id) "
            + "VALUES (@a, @g, @t, @f, @l, @b, @s)",
            ["@a", "@g", "@t", "@f", "@l", "@b", "@s"],
            result.Listings.Select(l => new object?[] { l.AssessmentId, l.Tag, l.TaxonId, l.FirstRelease, l.LastRelease, l.Tables, l.SummaryTableId }));
        return result;
    }

    // ------------------------------------------------------------ phases

    private void Phase(string name, Func<string?> body) {
        var watch = Stopwatch.StartNew();
        _console.MarkupLineInterpolated($"[grey]{name}...[/]");
        var detail = body();
        watch.Stop();
        _stats.Phases.Add((name, watch.Elapsed));
        var seconds = watch.Elapsed.TotalSeconds.ToString("N1", CultureInfo.InvariantCulture);
        if (detail is null) {
            _console.MarkupLineInterpolated($"  {name}: done in {seconds} s");
        } else {
            _console.MarkupLineInterpolated($"  {name}: {detail} ({seconds} s)");
        }
    }

    private void Optional(string sourceName, string? path, Func<string, string?> body) {
        if (string.IsNullOrWhiteSpace(path) || !(File.Exists(path) || Directory.Exists(path))) {
            _stats.MissingSources.Add(sourceName);
            var where = string.IsNullOrWhiteSpace(path) ? "no path is configured" : $"{path} does not exist";
            _console.MarkupLineInterpolated($"[yellow]Skipped the {sourceName}: {where}. The site database gets no data from it.[/]");
            return;
        }
        Phase($"Reading the {sourceName}", () => body(path));
    }
}
