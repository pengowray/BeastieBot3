using System.ComponentModel;
using System.Globalization;
using BeastieBot3.Col;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `site build-db`: builds the public species site's database (Datastore:site_sqlite) from the local
// caches. See SiteDbBuild for the phases and BeastieBot3.Shared/SiteData/SiteDbSchema.cs for the
// schema the site reads.

namespace BeastieBot3.SiteBuild;

[CommandInfo("site build-db", CommandKind.Mutates,
    "Build the public species site's database (Datastore:site_sqlite) from the IUCN CSV export, the IUCN API cache, GBIF's copy of the IUCN checklist, the common names store, the Wikidata and Wikipedia caches, the Catalogue of Life placement file and release metadata, the SPRAT database, the DOIs found by iucn resolve-dois, and IUCN's summary tables of category changes and Possibly Extinct species (iucn summary-tables). Taxa that are in the API cache but not in the CSV export get pages too, with their earlier assessments. The new database is written beside the old one and replaces it only when the build finishes. No assessment narrative text is stored.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Builds the whole database again from the data stored locally and replaces the previous one. A running site switches to the new file within about 30 seconds, without a restart.",
    Examples = new[] {
        "site build-db",
        "site build-db --limit 2000",
        "site build-db --output /tmp/site-test.sqlite",
    })]
internal sealed class SiteBuildDbCommand : Command<SiteBuildDbCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-o|--output <PATH>")]
        [Description("Site database to write. Default: Datastore:site_sqlite in paths.ini, or site.sqlite in datastore_dir.")]
        public string? OutputPath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Build from only the first N taxa by IUCN taxon id, for testing.")]
        public int? Limit { get; init; }

        [CommandOption("--iucn-db <PATH>")]
        [Description("IUCN CSV export database. Default: Datastore:IUCN_sqlite_from_cvs in paths.ini.")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--cache <PATH>")]
        [Description("IUCN API cache database. Default: Datastore:IUCN_api_cache_sqlite in paths.ini.")]
        public string? CacheDatabase { get; init; }

        [CommandOption("--common-names-db <PATH>")]
        [Description("Common names store. Default: Datastore:common_names_sqlite in paths.ini.")]
        public string? CommonNamesDatabase { get; init; }

        [CommandOption("--wikidata-cache <PATH>")]
        [Description("Wikidata cache database. Default: Datastore:wikidata_cache_sqlite in paths.ini.")]
        public string? WikidataCache { get; init; }

        [CommandOption("--wiki-cache <PATH>")]
        [Description("Wikipedia cache database. Default: Datastore:enwiki_cache_sqlite in paths.ini.")]
        public string? WikipediaCache { get; init; }

        [CommandOption("--wikispecies-cache <PATH>")]
        [Description("Wikispecies cache database, filled by wikispecies fetch. Default: Datastore:wikispecies_cache_sqlite in paths.ini, else wikispecies_cache.sqlite in the datastore folder.")]
        public string? WikispeciesCache { get; init; }

        [CommandOption("--col-placement <PATH>")]
        [Description("Catalogue of Life placement file, built by col build-placement. Default: the file beside Datastore:COL_sqlite.")]
        public string? ColPlacement { get; init; }

        [CommandOption("--col-dir <PATH>")]
        [Description("Folder with the Catalogue of Life ColDP zip, read for the release's citation and DOI. Default: Datasets:COL_dir in paths.ini.")]
        public string? ColDir { get; init; }

        [CommandOption("--status-lists-db <PATH>")]
        [Description("Status lists store (statuses natureserve-fetch, statuses ecos-import). Default: Datastore:status_lists_sqlite in paths.ini, else status_lists.sqlite in the datastore folder.")]
        public string? StatusListsDatabase { get; init; }

        [CommandOption("--summary-tables-db <PATH>")]
        [Description("IUCN's summary tables 7 and 9 (iucn summary-tables). Default: Datastore:IUCN_summary_tables_sqlite in paths.ini, else iucn_summary_tables.sqlite in the datastore folder.")]
        public string? SummaryTablesDatabase { get; init; }

        [CommandOption("--sprat-db <PATH>")]
        [Description("SPRAT database. Default: Datastore:SPRAT_sqlite in paths.ini.")]
        public string? SpratDatabase { get; init; }

        [CommandOption("--rules <PATH>")]
        [Description("rules-list.txt, whose common name lines override the best English name as they do in the Wikipedia lists. Default: rules/rules-list.txt beside the program, as wikipedia generate-lists uses.")]
        public string? RulesList { get; init; }

        [CommandOption("--gbif-zip <PATH>")]
        [Description("GBIF's copy of the IUCN checklist. Default: the newest zip in Datasets:GBIF_IUCN_dir.")]
        public string? GbifChecklist { get; init; }

        [CommandOption("--doi-cache <PATH>")]
        [Description("DOIs that iucn resolve-dois found in Crossref's list of IUCN DOIs or at doi.org. Default: Datastore:IUCN_doi_cache_sqlite in paths.ini.")]
        public string? DoiCache { get; init; }

        [CommandOption("--extra-species <PLACEMENT>")]
        [Description("Species from the Catalogue of Life and Wikidata that IUCN does not have, for the group pages' lists: family (the default: species whose genus IUCN has, under that genus, and species whose family IUCN has, under that family), genus (only species whose genus IUCN has) or none.")]
        public string? ExtraSpecies { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        if (settings.Limit is <= 0) {
            AnsiConsole.MarkupLine("[red]--limit must be at least 1.[/]");
            return -1;
        }
        var paths = settings.CreatePaths();
        if (!TryParseExtraSpecies(settings.ExtraSpecies, out var extraSpecies)) {
            AnsiConsole.MarkupLine("[red]--extra-species must be genus, family or none.[/]");
            return -1;
        }
        SiteBuildInputs inputs;
        try {
            var colDatabase = paths.GetColSqlitePath();
            var output = settings.OutputPath ?? paths.GetSiteDatabasePath()
                ?? throw new InvalidOperationException(paths.NotConfiguredMessage("Site database", "Datastore:site_sqlite", "--output"));
            // The Wikidata status dry run's settings file: its assessment item model is what the
            // site's QuickStatements batches follow.
            var rulesList = Full(settings.RulesList ?? Path.Combine(paths.BaseDirectory, "rules", "rules-list.txt"))!;
            var wikidataConfig = WikidataIucnEditConfig.LoadFromRules(paths, out var wikidataConfigPath);
            inputs = new SiteBuildInputs {
                IucnDatabase = paths.ResolveIucnDatabasePath(settings.IucnDatabase, "--iucn-db"),
                ApiCache = paths.ResolveIucnApiCachePath(settings.CacheDatabase),
                CommonNames = Full(settings.CommonNamesDatabase ?? paths.GetCommonNameStorePath()),
                WikidataCache = Full(settings.WikidataCache ?? paths.GetWikidataCachePath()),
                WikipediaCache = Full(settings.WikipediaCache ?? paths.GetWikipediaCachePath()),
                WikispeciesCache = Full(settings.WikispeciesCache ?? paths.GetWikispeciesCachePath()),
                ColPlacement = Full(settings.ColPlacement
                    ?? (string.IsNullOrWhiteSpace(colDatabase) ? null : TaxonPlacementStore.SidecarPath(colDatabase))),
                ColDatabase = Full(colDatabase),
                ColDir = Full(settings.ColDir ?? paths.GetColDir()),
                SpratDatabase = Full(settings.SpratDatabase ?? paths.GetSpratDatabasePath()),
                StatusListsDatabase = Full(settings.StatusListsDatabase ?? paths.GetStatusListsPath()),
                SummaryTables = Full(settings.SummaryTablesDatabase ?? paths.GetIucnSummaryTablesPath()),
                GbifChecklist = Full(settings.GbifChecklist ?? GbifIucnChecklistReader.FindNewest(paths.GetGbifIucnDir())),
                DoiCache = Full(settings.DoiCache ?? paths.GetIucnDoiCachePath()),
                RulesList = rulesList,
                TaxonRules = Full(Path.Combine(Path.GetDirectoryName(rulesList)!, "taxon-rules.yml")),
                WikidataRanks = Full(Path.Combine(Path.GetDirectoryName(rulesList)!, "wikidata-taxon-ranks.csv")),
                Checklists = Full(paths.GetChecklistsPath()),
                NotAssignedRules = Iucn.IucnNotAssignedRules.LoadForPaths(paths),
                ProbableScopes = Iucn.IucnProbableScopes.LoadForPaths(paths),
                WikidataItemModel = wikidataConfig.ToItemModel(),
                WikidataItemModelSource = wikidataConfigPath,
                Output = Path.GetFullPath(output),
                Limit = settings.Limit,
                ExtraSpecies = extraSpecies,
            };
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{ex.Message}[/]");
            return -1;
        }
        foreach (var (label, path) in new[] { ("IUCN CSV export", inputs.IucnDatabase), ("IUCN API cache", inputs.ApiCache) }) {
            if (!File.Exists(path)) {
                AnsiConsole.MarkupLineInterpolated($"[red]{label} not found:[/] {path}");
                return -1;
            }
        }

        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN CSV export:[/] {inputs.IucnDatabase}");
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN API cache:[/] {inputs.ApiCache}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Site database:[/] {inputs.Output}");
        if (inputs.Limit is { } limit) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Building from the first {limit:N0} taxa only (--limit).[/]");
        }

        SiteBuildStats stats;
        try {
            stats = new SiteDbBuild(inputs, AnsiConsole.Console).Run(cancellationToken);
        } catch (OperationCanceledException) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Cancelled. The site database was not changed:[/] {inputs.Output}");
            return 130;
        } catch (Exception ex) when (ex is SqliteException or IOException or InvalidOperationException or InvalidDataException) {
            AnsiConsole.MarkupLineInterpolated($"[red]The build failed, so the site database was not changed:[/] {ex.Message}");
            return 1;
        }

        WriteSummary(stats);
        foreach (var ((from, to), count) in stats.AuthorNameRepairs.OrderByDescending(p => p.Value)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Author name repaired:[/] {from} [grey]to[/] {to} [grey]({count:N0})[/]");
        }
        foreach (var (name, count) in stats.AuthorNamesNotRepaired.OrderByDescending(p => p.Value)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Author name with a lost letter, kept as IUCN wrote it:[/] {name} ({count:N0})");
        }
        foreach (var warning in stats.Warnings.Take(20)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{warning}[/]");
        }
        AnsiConsole.MarkupLineInterpolated($"[green]Site database written:[/] {inputs.Output}");
        AnsiConsole.MarkupLine("[grey]A running site switches to the new file within about 30 seconds, without a restart.[/]");
        return 0;
    }

    private static bool TryParseExtraSpecies(string? text, out ExtraSpecies.ExtraPlacement placement) {
        placement = ExtraSpecies.ExtraPlacement.Family;
        if (string.IsNullOrWhiteSpace(text)) {
            return true;
        }
        return Enum.TryParse(text.Trim(), ignoreCase: true, out placement) && Enum.IsDefined(placement);
    }

    private static string? Full(string? path) => string.IsNullOrWhiteSpace(path) ? null : Path.GetFullPath(path);

    private static bool HasAuthors(SiteWikidataItem item) {
        var properties = item.Properties.Split(' ');
        return properties.Contains("P2093") || properties.Contains("P50");
    }

    private static void WriteSummary(SiteBuildStats s) {
        var table = new Table().AddColumn("Measure").AddColumn(new TableColumn("Value").RightAligned());
        void Row(string label, long value) => table.AddRow(Markup.Escape(label), value.ToString("N0", CultureInfo.InvariantCulture));
        void Text(string label, string? value) => table.AddRow(Markup.Escape(label), Markup.Escape(value ?? "-"));
        void Section(string label) => table.AddRow($"[bold]{Markup.Escape(label)}[/]", string.Empty);

        Section("Taxa");
        Row("Species", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Species));
        Row("Subspecies", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Subspecies));
        Row("Varieties", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Variety));
        Row("Subpopulations", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Subpopulation));
        Row("Parent taxon found by name", s.ParentsByName);
        Row("Parent taxon found from the IUCN API record", s.ParentsFromApi);
        Row("Parent taxon not in the database", s.ParentsMissing);
        Row("Not in the release (only in the API cache): species", s.NotInReleaseByKind.GetValueOrDefault(SiteTaxonKind.Species));
        Row("Not in the release: subspecies", s.NotInReleaseByKind.GetValueOrDefault(SiteTaxonKind.Subspecies));
        Row("Not in the release: varieties", s.NotInReleaseByKind.GetValueOrDefault(SiteTaxonKind.Variety));
        Row("Not in the release: subpopulations", s.NotInReleaseByKind.GetValueOrDefault(SiteTaxonKind.Subpopulation));
        Row("Not in the release, with a taxon of the same name in the release", s.NotInReleaseWithCurrentTaxon);
        Row("Not in the release, linked to the one taxon in the release whose IUCN synonyms list its name", s.NotInReleaseSynonymLinks);
        Row("Not in the release, with a name that two or more taxa in the release list as a synonym (not linked)", s.NotInReleaseSynonymOfSeveral);
        Row("Taxa named with \"_new\" linked to the taxon named without it", s.WorkingNameLinks);
        Row("Taxa with a provisional name (sp. nov.) linked to the taxon named with its quoted epithet", s.ProvisionalNameLinks);
        Row("Taxa whose global assessments' taxonomic notes name another taxon in the release", s.TaxaWithNotesTaxa);
        Row("Taxa named in another taxon's taxonomic notes (pairs of taxa)", s.NotesTaxa);
        Row("Taxa named in an assessment's taxonomic notes (notes_taxon rows)", s.NotesTaxonRows);
        Row("Taxa with a Red List Authority (IUCN SSC group) in their API record", s.TaxaWithRedListAuthority);
        Row("Names in taxonomic notes that fit two or more taxa (left out)", s.NotesNamesOfSeveralTaxa);
        Row("Assessments with no scope given a probable scope (rules/iucn-probable-scopes.yml)", s.ProbableScopes);
        Row("Assessments with no scope and no probable scope", s.NoScopeWithoutProbableScope);
        Row("API taxon records not in the release with no scientific name (left out)", s.NotInReleaseRecordsUnusable);

        Section("Assessments");
        Row("Latest global", s.AssessmentsGlobalLatest);
        Row("Latest regional", s.AssessmentsRegionalLatest);
        Row("Earlier (history)", s.AssessmentsHistory);
        Row("Stored with no scope: CSV rows with no scope", s.CsvAssessmentsNoScope);
        Row("Stored with no scope: assessments in API taxon records with no scope (not in the CSV)", s.ApiHeadersNoScope);
        Row("Assessments the IUCN API answered 404 (not found) for, stored with that flag", s.AssessmentsApiNotFound);
        Row("Left out: assessments in API taxon records with no year published", s.ApiHeadersUnpublished);
        Row("Left out: assessments in API taxon records for another taxon id", s.ApiHeadersOtherTaxon);
        Row("Flagged latest by the API, but the CSV has that scope (stored as earlier)", s.ApiLatestCoveredByCsv);
        Row("Flagged latest by the API and missing from the CSV (stored as latest)", s.ApiLatestNotInCsv);
        Row("Flagged latest by the API, for a taxon not in the release (stored as earlier)", s.NotInReleaseLatestHeaders);
        Row("Category in the CSV differs from the API taxon record", s.CsvCategoryDiffersFromApi);
        Row("Replaced by an errata version (replaced_by_assessment_id set)", s.ReplacedByErrata);
        Row("Replaced by an amended version (replaced_by_assessment_id set)", s.ReplacedByAmended);
        Row("Errata versions whose replaced assessment was found from their DOI (another scope or year)", s.ReplacedFoundFromDoi);
        Row("Errata or amended versions with no earlier assessment to link", s.ReplacedNoCandidate);
        Row("Errata or amended versions with several earlier assessments that could be the one replaced", s.ReplacedSeveralCandidates);
        Row("Earlier assessments named by two newer versions (not linked)", s.ReplacedClaimedTwice);

        if (s.SummaryTables is { } t) {
            Section("IUCN summary tables (Table 7: reasons for change; Table 9: Possibly Extinct)");
            Row("Tables read", t.Tables.Count);
            Row("Table 7 rows", t.ChangeRows);
            Row("Table 7 rows with no reason code or no new category (left out)", t.ChangeRowsWithoutReason);
            Row("Table 7 rows whose name matches no taxon", t.ChangeRowsNoTaxon);
            Row("Table 7 rows matched by an IUCN synonym", t.ChangeRowsBySynonym);
            Row("Table 7 rows with no global assessment of the new category in the version's year or the year after", t.ChangeRowsNoAssessment);
            Row("Table 7 rows linked to an assessment", t.ChangeRowsLinked);
            Row("Linked to an assessment published the year after the version's year", t.ChangeRowsYearAfter);
            Row("Linked, but the assessment before it has another category than the table's previous category", t.ChangeRowsPreviousDiffers);
            Row("Assessments with a reason for change (category_change)", t.Changes.Count);
            Row("Assessments whose tables give different reasons (the latest table's is kept)", t.ReasonConflicts);
            Row("Table 9 rows", t.ListingRows);
            Row("Table 9 rows whose name matches no taxon", t.ListingRowsNoTaxon);
            Row("Table 9 rows with no CR assessment at that release or in the first year", t.ListingRowsNoAssessment);
            Row("Assessments listed as Possibly Extinct (possibly_extinct_listing)", t.Listings.Count);
            Row("Listed assessments whose own record has no Possibly Extinct tag", t.ListedWithoutTag);
        }

        Section("Citations");
        Row("Citations parsed from cached API assessments", s.CitationsParsed);
        Row("Assessments not in the API cache (no citation)", s.CitationsNotCached);
        Row("Cached assessments with taxonomic notes (has_taxonomic_notes = 1)", s.PayloadsWithTaxonomicNotes);
        Row("Taxa with names from the Mammal Diversity Database or AmphibiaWeb", s.ChecklistTaxa);
        Row("Taxa whose latest global assessment codes countries or areas", s.TaxaWithAreas);
        Row("Taxon and area rows (taxon_area)", s.TaxonAreaRows);
        Row("Countries and areas (area)", s.Areas);
        Row("Assessments with credits (assessment.credits)", s.AssessmentsWithCredits);
        Row("Credit entries in those assessments", s.CreditEntries);
        Row("Distinct credit entries (credit_name rows)", s.CreditNames);
        Row("Citations that could not be parsed", s.CitationFailures.Values.Sum() + s.PayloadsUnreadable);
        Row("DOIs from IUCN's citation text", s.DoisBySource.GetValueOrDefault(DoiSource.Citation));
        Row("DOIs from GBIF", s.DoisBySource.GetValueOrDefault(DoiSource.Gbif));
        Row("DOIs from Wikidata", s.DoisBySource.GetValueOrDefault(DoiSource.Wikidata));
        Row("DOIs from Crossref's list or doi.org (iucn resolve-dois)", s.DoisBySource.GetValueOrDefault(DoiSource.Resolved));
        Row("Citations with no DOI", s.DoisBySource.GetValueOrDefault(DoiSource.None));
        Row("Author names repaired: a letter lost to an encoding error, restored from other assessor credits", s.AuthorNameRepairs.Values.Sum());
        Row("Author names with a lost letter that could not be repaired", s.AuthorNamesNotRepaired.Values.Sum());
        Text("API assessments downloaded", s.DownloadedFrom is null ? null
            : $"{s.DownloadedFrom:yyyy-MM-dd} to {s.DownloadedTo:yyyy-MM-dd}");

        Section("Names");
        Row("Scientific names", s.NamesByType.GetValueOrDefault(SiteNameType.Scientific));
        Row("Common names", s.NamesByType.GetValueOrDefault(SiteNameType.Common));
        Row("Common names in English", s.CommonNamesEnglish);
        Row("Common names left out as junk (wiki markup, author citations, OCR errors)", s.CommonNamesJunk);
        Row("Codes kept for search only (\"Species code: Po\", bird codes such as \"CROW\")", s.SpeciesCodes);
        Row("Common names repaired before storing (wiki markup or extra text removed, OCR errors fixed)", s.CommonNamesRepaired);
        Row("Synonyms (one row per source that gives the name)", s.NamesByType.GetValueOrDefault(SiteNameType.Synonym));
        Row("Catalogue of Life synonyms with an authority from the CoL database", s.ColSynonymAuthorities);
        Row("Wikidata taxon synonym (P1420) items of the taxa's items", s.WikidataSynonymItems);
        Row("Of those, items the Wikidata cache has a scientific name for", s.WikidataSynonymsNamed);
        Row("Wikipedia taxobox names and synonyms read (before repeats are dropped)", s.WikipediaTaxoboxSynonyms);
        Row("Wikipedia pages matched to several taxa, none with the taxobox's name (no synonyms taken)", s.WikipediaTaxoboxPagesShared);
        Row("Taxa with an English name for display", s.CommonNameEn);
        Row("Of those, names set in rules-list.txt", s.CommonNameEnFromRules);
        Row("English names not used: the scientific name again, or a working name", s.CommonNameEnUnusable);

        Section("Common names in languages other than English");
        foreach (var (source, counts) in new[] {
                     ("Catalogue of Life", s.ColOtherNames), ("Wikidata", s.WikidataOtherNames), ("Wikipedia titles", s.WikipediaOtherNames),
                 }) {
            Row($"{source}: names read for the site's taxa", counts.Read);
            Row($"{source}: English names (not read here)", counts.English);
            Row($"{source}: names with no language, several, or a language code the site cannot name", counts.LanguageLeftOut);
            Row($"{source}: the taxon's scientific name or a synonym", counts.Dropped.GetValueOrDefault(OtherNameDrop.ScientificName));
            Row($"{source}: the taxon's genus", counts.Dropped.GetValueOrDefault(OtherNameDrop.Genus));
            Row($"{source}: starting with the genus and a lower-case word", counts.Dropped.GetValueOrDefault(OtherNameDrop.Binomial));
            Row($"{source}: the genus's initial and the species epithet", counts.Dropped.GetValueOrDefault(OtherNameDrop.AbbreviatedBinomial));
            Row($"{source}: kept, before junk and repeats are left out", counts.Kept);
            if (counts.LanguagesLeftOut.Count > 0) {
                Text($"{source}: language codes left out (most names first)", string.Join(", ", counts.LanguagesLeftOut
                    .OrderByDescending(p => p.Value).Take(12).Select(p => $"{p.Key} {p.Value:N0}")));
            }
        }
        foreach (var (source, rows) in s.OtherLanguageNameRows.OrderByDescending(p => p.Value)) {
            Row($"Name rows written from {source}", rows);
        }
        Row("Names after merging the sources' rows (per taxon, language and name)", s.OtherLanguageNamesMerged);
        Row("Languages", s.OtherLanguages.Count);

        Section("Subspecies and varieties of the species (species page list)");
        var sub = s.Subspecies;
        Row("Species with a list from any source", sub.SpeciesWithList);
        Row("Of those, with names from two or more sources", sub.SpeciesWithSeveralSources);
        Row("IUCN subspecies and varieties in the release under a species in the release (read by the site from the taxon table)", sub.IucnRows);
        Row("Catalogue of Life: species with a CoL ID read", sub.ColSpeciesRead);
        Row("Catalogue of Life: accepted subspecies and varieties stored", sub.ColRows);
        Row("Catalogue of Life: species with one or more", sub.ColSpecies.Count);
        Row("Catalogue of Life: names left out (not genus, species and one more epithet)", sub.ColUnreadable);
        Row("Wikidata: subspecies and variety items in the taxon sweep", sub.WikidataItemsRead);
        Row("Wikidata: rows stored (one per species the item is under)", sub.WikidataRows);
        Row("Wikidata: species with one or more", sub.WikidataSpecies.Count);
        Row("Wikidata: items under a site species left out as an instance of synonym, fossil taxon, unavailable or original combination", sub.WikidataLeftOutByInstance);
        Row("Wikidata: items under a site species left out because another item names them as a taxon synonym (P1420)", sub.WikidataLeftOutAsSynonym);
        Row("Wikidata: items kept because each item that names them as a taxon synonym is named as a synonym by them", sub.WikidataKeptAsMutualSynonym);
        Row("Wikidata: names left out (not genus, species and one more epithet)", sub.WikidataUnreadable);
        foreach (var (title, c) in new[] { ("Mammal Diversity Database", sub.Mdd), ("Reptile Database", sub.ReptileDb) }) {
            Row($"{title}: its species with subspecies", c.SourceSpecies);
            Row($"{title}: of those, matched to an IUCN species", c.SourceSpeciesMatched);
            Row($"{title}: subspecies stored", c.Rows);
            Row($"{title}: IUCN species with one or more", c.Species.Count);
            Row($"{title}: fossil subspecies left out", c.LeftOutFossil);
            Row($"{title}: names left out (not genus, species and one more epithet)", c.Unreadable);
        }

        Section("Links");
        Row("Taxa with an English Wikipedia article", s.EnwikiTitles);
        Row("Taxa with a Wikidata item that states their IUCN taxon id (P627)", s.QidsFromP627);
        Row("Of those, items chosen from several", s.QidTieBreaks);
        if (s.QidsP627Deprecated is { } deprecatedIds) {
            Row("Of those, items that state the id only at deprecated rank (no status commands)", deprecatedIds);
        }
        Row("Of those, items downloaded to the Wikidata cache (IUCN status statements known)", s.QidsWithP141Known);
        Row("Of those, items with no IUCN status (P141)", s.QidsWithNoP141);
        if (s.RedListEditions is { } editions) {
            Row("Editions of the IUCN Red List on Wikidata (a P141 reference stated in one cites IUCN)", editions);
        }
        Row("Items whose JSON was read for P141 references that the cache's index does not record", s.P141ItemsReadAsJson);
        Row("Of their P141 statements, ones that cite IUCN by a reference URL on iucnredlist.org", s.P141CitesIucnByUrl);
        Row("Of their P141 statements, ones that cite IUCN in a reference's second or later stated in (P248)", s.P141CitesIucnByLaterStatedIn);
        Row("Taxa with a Wikidata item matched by name", s.QidsFromNameMatch);
        Row("Wikidata items matched by name but left out: the item is a taxon in another kingdom", s.QidsNameMatchOtherKingdom);
        var items = s.WikidataItems;
        Row("Wikidata items for IUCN assessments in the Wikidata cache", items.Read);
        foreach (var (label, count) in items.KeptByClass.OrderByDescending(p => p.Value)) {
            Row($"Of those, kept as publications: {label}", count);
        }
        foreach (var (classes, count) in items.DroppedByClasses.OrderByDescending(p => p.Value)) {
            Row($"Of those, left out as not a publication: instance of {classes}", count);
        }
        if (items.WithoutIds > 0) Row("Items kept with no taxon or assessment id (not used)", items.WithoutIds);
        if (items.SecondItemForAnAssessment > 0) Row("Items for an assessment that already has an item (not used)", items.SecondItemForAnAssessment);
        if (items.TitlesNotRecorded > 0) Row("Items kept whose title statements are not recorded (run wikidata iucn-assessment-items)", items.TitlesNotRecorded);
        Row("Assessments with their own Wikidata item", s.AssessmentsWithOwnItem);
        Row("Errata versions sharing the item of the assessment they correct (same DOI)", s.AssessmentsWithItemThroughDoi);
        Row("Citations with a title registered with Crossref for their DOI", s.RegisteredNames);
        Row("Of those, titles with a name other than IUCN's citation name (ssp./subsp., brackets and spaces ignored)", s.RegisteredNamesDiffer);
        Row("Items for an assessment not in the site database (not used)", items.ByAssessment.Count - items.Used.Count);
        Row("Items used with no author (P50 or P2093)", items.ByAssessment.Values.Count(i => items.Used.Contains(i.Qid) && !HasAuthors(i)));
        Row("Items used with no main subject (P921)", items.ByAssessment.Values.Count(i => items.Used.Contains(i.Qid) && !i.Properties.Split(' ').Contains("P921")));
        Row("Catalogue of Life ids from the placement file", s.ColIdsFromPlacement);
        Row("Catalogue of Life ids from the common names store", s.ColIdsFromCrossReference);
        Row("Taxa matched to SPRAT by name", s.SpratMatched);
        Row("Taxa with an EPBC status", s.EpbcStatuses);
        Row("SPRAT profiles of a population of a taxon", s.SpratPopulationProfiles);
        Row("Of those, listed under the EPBC Act", s.EpbcPopulationListings);
        Row("SPRAT names with a voucher or other text in brackets after a taxon's name (not linked)", s.SpratBracketsNotPopulation);
        Row("Australian state and territory statuses from those SPRAT profiles", s.StateStatuses);
        Row("NatureServe records in the status lists store", s.NatureServeRecords);
        Row("Taxa matched to a NatureServe record by its scientific name", s.NatureServeByName);
        Row("Taxa matched to a NatureServe record by a NatureServe synonym", s.NatureServeBySynonym);
        Row("Taxa matched to a NatureServe record by an IUCN synonym", s.NatureServeByIucnSynonym);
        Row("NatureServe global ranks (unranked and not applicable left out)", s.NatureServeRanks);
        Row("NatureServe national ranks (US, Canada; unranked left out)", s.NatureServeNationalRanks);
        Row("French statuses (national red list, national and overseas protection)", s.FranceRows);
        Row("French taxa matched by TAXREF's IUCN id", s.FranceByTaxrefLink);
        Row("French taxa matched by name", s.FranceByName);
        Row("French rows on the site", s.FranceSiteRows);
        Row("Japan's Red List rows (taxa and local populations)", s.JapanRows);
        Row("Japan's Red List taxa matched to a site taxon", s.JapanMatched);
        Row("Japan's Red List rows on the site", s.JapanSiteRows);
        Row("National red list statuses (GBIF)", s.RedListStatuses);
        Row("Site taxa with a national red list status (counted per list)", s.RedListMatched);
        Row("National red list rows on the site", s.RedListRows);
        Row("CITES taxa (species, subspecies, varieties)", s.CitesTaxa);
        Row("CITES taxa matched to a site taxon", s.CitesMatched);
        Row("Site taxa covered by a CITES genus, family or order listing", s.CitesByHigherTaxon);
        Row("CITES rows on the site (one per listing)", s.CitesRows);
        Row("JNCC designations for the UK and its countries", s.JnccDesignations);
        Row("JNCC taxa matched to a site taxon", s.JnccMatched);
        Row("JNCC rows on the site (one per list and area)", s.JnccRows);
        Row("NatureServe state, province and territory ranks (unranked left out)", s.NatureServeSubnationalRanks);
        Row("COSEWIC statuses (from NatureServe)", s.CosewicStatuses);
        Row("SARA statuses (from NatureServe)", s.SaraStatuses);
        Row("ECOS listings (US Endangered Species Act)", s.EcosListings);
        Row("ECOS listings matched to a taxon", s.EcosMatched);
        Row("NZTCS assessments with a scientific name and a status", s.NztcsAssessments);
        Row("NZTCS assessments matched to a taxon", s.NztcsMatched);
        Row("SALVE assessments (Brazil's fauna) with a known category", s.SalveAssessments);
        Row("SALVE assessments matched to a taxon", s.SalveMatched);
        Row("Taxa with an IUCN Green Status assessment in the API cache", s.GreenStatusRecords);
        Row("Of those, taxa on the site", s.GreenStatusTaxa);

        Section("Groups (higher taxa)");
        Row("Groups", s.TreeNodes);
        Row("Of those, Catalogue of Life groups between IUCN ranks", s.TreeColGroups);
        Row("Of those, orders and families from rules/iucn-not-assigned.yml (IUCN: NOT ASSIGNED)", s.TreeRuleGroups);
        Row("Taxa with an order from rules/iucn-not-assigned.yml", s.TreeTaxaUnderRuleOrder);
        Row("Taxa with a family from rules/iucn-not-assigned.yml", s.TreeTaxaUnderRuleFamily);
        Row("Taxa with a rank still NOT ASSIGNED (placed in the group above)", s.TreeTaxaWithUnassignedRank);
        Row("Taxa with no kingdom (left out of the groups)", s.TreeTaxaWithoutKingdom);
        Text("Catalogue of Life placement", s.ColPlacementState ?? "not used");
        Row("Groups with an English name (rules or Wikipedia)", s.GroupCommonNames);
        Row("Groups with an English Wikipedia page", s.GroupArticles);
        Row("Groups with a Catalogue of Life id", s.GroupColIds);
        Row("Groups with Catalogue of Life English names", s.GroupsWithColNames);
        Row("Groups whose downloaded English Wikipedia article is about the group", s.GroupWikipediaArticles);
        Row("  of them with no redirect list downloaded (wikipedia fetch-group-titles)", s.GroupWikipediaArticlesWithoutRedirects);
        Row("Groups with names from English Wikipedia (article title and redirects)", s.GroupsWithWikipediaNames);
        Row("Names from English Wikipedia for groups", s.GroupWikipediaNames);
        Row("Groups with no English name that have a name from English Wikipedia that is not a scientific name", s.GroupsGainingEnglishNameCandidate);
        Row("Taxa with an article for Wikipedia list lines", s.ListArticleTitles);

        if (s.ExtraSpecies is { } x) {
            Section("Species from the Catalogue of Life and Wikidata (not in IUCN)");
            Text("Placed under", x.Placement == ExtraSpecies.ExtraPlacement.Family ? "IUCN genera and families" : "IUCN genera");
            Row("CoL accepted species read in IUCN genera" + (x.Placement == ExtraSpecies.ExtraPlacement.Family ? " and families" : string.Empty), x.ColRead);
            Row("Of those, fossil species (left out)", x.ColExtinct);
            Row("Of those, fossil or extinct species on Wikidata (left out)", x.ColFossilOnWikidata);
            Row("Of those, the same as an IUCN species (CoL id or name)", x.ColSameAsIucn);
            Row("Wikidata species items read", x.WikidataRead);
            Row("Of those, left out as fossil taxa, synonyms or extinct taxa (instance of)", x.WikidataLeftOutByInstance);
            Row("Of those, names that are not a plain binomial (left out)", x.WikidataNotBinomial);
            Row("Of those, with no IUCN genus or family to go under (left out)", x.WikidataUnplaced);
            Row("Of those, the same as an IUCN species (item, P627, P10585 or name)", x.WikidataSameAsIucn);
            Row("Of those, kingdom unknown and the genus name used in two kingdoms (left out)", x.WikidataAmbiguousKingdom);
            Row("Of those, merged with a CoL species (P10585 or name)", x.WikidataMergedWithCol);
            Row("Of those, a second item with a name already used (left out)", x.WikidataSameNameRepeat);
            Row("Extra species: only in CoL", x.ColEntries);
            Row("Extra species: only in Wikidata", x.WikidataEntries);
            Row("Extra species: in both", x.BothEntries);
            Row("Placed under an IUCN genus", x.PlacedUnderGenus);
            Row("Placed under an IUCN family", x.PlacedUnderFamily);
            Row("With an English name (Wikidata label)", x.CommonNames);
            Row("With an English Wikipedia article (Wikidata sitelink)", x.Articles);
            foreach (var (reason, count) in x.OverlapsByReason.OrderByDescending(p => p.Value)) {
                Row($"Possible overlaps: {reason}", count);
            }
            Row("IUCN species with another name in CoL or Wikidata", x.TaxonSourceNames);
            Row("CoL species-rank synonyms read", x.ColSynonymsRead);
            Text("Wikidata taxon sweep finished", x.WikidataSweepFinished ?? "never");
        }

        Section("Sources");
        Text("IUCN release", s.IucnRelease);
        Text("GBIF checklist", s.GbifVersion is null ? null : $"{s.GbifVersion}, published {s.GbifPublished}");
        Text("GBIF checklist DOI", s.GbifDoi);
        Text("Catalogue of Life release", s.ColRelease);
        Text("Catalogue of Life release DOI", s.ColDoi);
        Text("SPRAT report", s.SpratReport);
        Text("Wikidata assessment item model", s.WikidataItemModelSource ?? "built-in defaults (no iucn-status.yml found)");
        Text("DOI checks (iucn resolve-dois)", s.DoiChecksRead == 0 ? null
            : $"{s.DoiChecksRead:N0} assessments, {s.DoiChecksWithDoi:N0} with a DOI, newest check {s.DoiCheckedTo:yyyy-MM-dd}");
        if (s.MissingSources.Count > 0) {
            Text("Sources not found (skipped)", string.Join(", ", s.MissingSources));
        }

        Section("File");
        Text("Size", $"{s.FileBytes / 1_000_000.0:N1} MB");
        var total = s.Phases.LastOrDefault(p => p.Phase == "Total").Elapsed;
        Text("Total time", $"{total.TotalSeconds:N0} s");
        AnsiConsole.Write(table);
    }
}
