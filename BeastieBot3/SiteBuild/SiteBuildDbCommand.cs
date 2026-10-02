using System.ComponentModel;
using System.Globalization;
using BeastieBot3.Col;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `site build-db`: builds the public species site's database (Datastore:site_sqlite) from the local
// caches. See SiteDbBuild for the phases and BeastieBot3.Shared/SiteData/SiteDbSchema.cs for the
// schema the site reads.

namespace BeastieBot3.SiteBuild;

[CommandInfo("site build-db", CommandKind.Mutates,
    "Build the public species site's database (Datastore:site_sqlite) from the IUCN CSV export, the IUCN API cache, GBIF's copy of the IUCN checklist, the common names store, the Wikidata and Wikipedia caches, the Catalogue of Life placement file and the SPRAT database. The new database is written beside the old one and replaces it only when the build finishes. No assessment narrative text is stored.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Builds the whole database again from the data stored locally and replaces the previous one. A running site keeps reading the old file until it is restarted.",
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

        [CommandOption("--col-placement <PATH>")]
        [Description("Catalogue of Life placement file, built by col build-placement. Default: the file beside Datastore:COL_sqlite.")]
        public string? ColPlacement { get; init; }

        [CommandOption("--sprat-db <PATH>")]
        [Description("SPRAT database. Default: Datastore:SPRAT_sqlite in paths.ini.")]
        public string? SpratDatabase { get; init; }

        [CommandOption("--gbif-zip <PATH>")]
        [Description("GBIF's copy of the IUCN checklist. Default: the newest zip in Datasets:GBIF_IUCN_dir.")]
        public string? GbifChecklist { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        if (settings.Limit is <= 0) {
            AnsiConsole.MarkupLine("[red]--limit must be at least 1.[/]");
            return -1;
        }
        var paths = settings.CreatePaths();
        SiteBuildInputs inputs;
        try {
            var colDatabase = paths.GetColSqlitePath();
            var output = settings.OutputPath ?? paths.GetSiteDatabasePath()
                ?? throw new InvalidOperationException(paths.NotConfiguredMessage("Site database", "Datastore:site_sqlite", "--output"));
            inputs = new SiteBuildInputs {
                IucnDatabase = paths.ResolveIucnDatabasePath(settings.IucnDatabase, "--iucn-db"),
                ApiCache = paths.ResolveIucnApiCachePath(settings.CacheDatabase),
                CommonNames = Full(settings.CommonNamesDatabase ?? paths.GetCommonNameStorePath()),
                WikidataCache = Full(settings.WikidataCache ?? paths.GetWikidataCachePath()),
                WikipediaCache = Full(settings.WikipediaCache ?? paths.GetWikipediaCachePath()),
                ColPlacement = Full(settings.ColPlacement
                    ?? (string.IsNullOrWhiteSpace(colDatabase) ? null : TaxonPlacementStore.SidecarPath(colDatabase))),
                ColDatabase = Full(colDatabase),
                SpratDatabase = Full(settings.SpratDatabase ?? paths.GetSpratDatabasePath()),
                GbifChecklist = Full(settings.GbifChecklist ?? GbifIucnChecklistReader.FindNewest(paths.GetGbifIucnDir())),
                Output = Path.GetFullPath(output),
                Limit = settings.Limit,
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
        foreach (var warning in stats.Warnings.Take(20)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{warning}[/]");
        }
        AnsiConsole.MarkupLineInterpolated($"[green]Site database written:[/] {inputs.Output}");
        AnsiConsole.MarkupLine("[grey]A running site keeps reading the old file until it is restarted.[/]");
        return 0;
    }

    private static string? Full(string? path) => string.IsNullOrWhiteSpace(path) ? null : Path.GetFullPath(path);

    private static void WriteSummary(SiteBuildStats s) {
        var table = new Table().AddColumn("What").AddColumn(new TableColumn("Count").RightAligned());
        void Row(string label, long value) => table.AddRow(Markup.Escape(label), value.ToString("N0", CultureInfo.InvariantCulture));
        void Text(string label, string? value) => table.AddRow(Markup.Escape(label), Markup.Escape(value ?? "-"));
        void Section(string label) => table.AddRow($"[bold]{Markup.Escape(label)}[/]", string.Empty);

        Section("Taxa");
        Row("Species", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Species));
        Row("Subspecies", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Subspecies));
        Row("Varieties", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Variety));
        Row("Subpopulations", s.TaxaByKind.GetValueOrDefault(SiteTaxonKind.Subpopulation));
        Row("Parent found by name", s.ParentsByName);
        Row("Parent found from the API record", s.ParentsFromApi);
        Row("Parent not in the database", s.ParentsMissing);

        Section("Assessments");
        Row("Latest global", s.AssessmentsGlobalLatest);
        Row("Latest regional", s.AssessmentsRegionalLatest);
        Row("Earlier (history)", s.AssessmentsHistory);
        Row("Left out: CSV row with no scope", s.CsvAssessmentsNoScope);
        Row("Left out: API header with no scope", s.ApiHeadersNoScope);
        Row("Left out: API header with no year (unpublished)", s.ApiHeadersUnpublished);
        Row("Left out: API header for another taxon id", s.ApiHeadersOtherTaxon);
        Row("API latest, but the CSV has that scope (stored as earlier)", s.ApiLatestCoveredByCsv);
        Row("API latest, not in the CSV (stored as latest)", s.ApiLatestNotInCsv);
        Row("CSV category differs from the API header", s.CsvCategoryDiffersFromApi);

        Section("Citations");
        Row("Parsed from the cached payload", s.CitationsParsed);
        Row("No payload cached (citation left empty)", s.CitationsNotCached);
        Row("Payload could not be parsed", s.CitationFailures.Values.Sum() + s.PayloadsUnreadable);
        Row("DOI from IUCN's citation", s.DoisBySource.GetValueOrDefault(DoiSource.Citation));
        Row("DOI from GBIF", s.DoisBySource.GetValueOrDefault(DoiSource.Gbif));
        Row("DOI from Wikidata", s.DoisBySource.GetValueOrDefault(DoiSource.Wikidata));
        Row("No DOI", s.DoisBySource.GetValueOrDefault(DoiSource.None));
        Text("Payloads downloaded", s.DownloadedFrom is null ? null
            : $"{s.DownloadedFrom:yyyy-MM-dd} to {s.DownloadedTo:yyyy-MM-dd}");

        Section("Names");
        Row("Scientific names", s.NamesByType.GetValueOrDefault(SiteNameType.Scientific));
        Row("Common names", s.NamesByType.GetValueOrDefault(SiteNameType.Common));
        Row("... of which English", s.CommonNamesEnglish);
        Row("Synonyms", s.NamesByType.GetValueOrDefault(SiteNameType.Synonym));
        Row("Taxa with a best English name", s.CommonNameEn);

        Section("Links");
        Row("English Wikipedia articles", s.EnwikiTitles);
        Row("Wikidata items stating the IUCN id (P627)", s.QidsFromP627);
        Row("... of which chosen from several items", s.QidTieBreaks);
        Row("Wikidata items matched by name", s.QidsFromNameMatch);
        Row("Catalogue of Life ids from the placement file", s.ColIdsFromPlacement);
        Row("Catalogue of Life ids from the common names store", s.ColIdsFromCrossReference);
        Row("SPRAT taxa matched", s.SpratMatched);
        Row("EPBC statuses", s.EpbcStatuses);

        Section("Sources");
        Text("IUCN release", s.IucnRelease);
        Text("GBIF checklist", s.GbifVersion is null ? null : $"{s.GbifVersion}, published {s.GbifPublished}");
        Text("Catalogue of Life release", s.ColRelease);
        Text("SPRAT report", s.SpratReport);
        if (s.MissingSources.Count > 0) {
            Text("Skipped (not found)", string.Join(", ", s.MissingSources));
        }

        Section("File");
        Text("Size", $"{s.FileBytes / 1024.0 / 1024.0:N1} MB");
        var total = s.Phases.LastOrDefault(p => p.Phase == "Total").Elapsed;
        Text("Total time", $"{total.TotalSeconds:N0} s");
        AnsiConsole.Write(table);
    }
}
