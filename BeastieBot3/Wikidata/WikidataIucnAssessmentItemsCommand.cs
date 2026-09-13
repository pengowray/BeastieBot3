using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.WikidataEdits;

// Finds the Wikidata items that are IUCN Red List assessment publications and stores them in the
// Wikidata cache (wikidata_iucn_assessment_items), so the status dry run cites an existing item
// for an assessment instead of proposing a duplicate. Read-only against Wikidata: SPARQL only.
//
// Routes (see WikidataAssessmentItemQueries for timings and the queries that didn't work):
//   published-in  P1433 = IUCN Red List or one of its editions, on both graphs
//   doi-search    search index, P356 prefix 10.2305/IUCN.UK, sliced by release year
//   doi-scan      P356 prefix scan, main graph only
//   dataset-url   data set items linking iucnredlist.org, main graph
// Then each item's statements are read from the graph it lives in.

namespace BeastieBot3.Wikidata;

public sealed class WikidataIucnAssessmentItemsSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Fetch and store at most N items, lowest item ids first. Finding items still runs in full.")]
    public int? Limit { get; init; }

    [CommandOption("--status")]
    [Description("Print what is stored and exit, without querying Wikidata.")]
    public bool Status { get; init; }

    [CommandOption("--iucn-database <PATH>")]
    [Description("IUCN CSV release database to compare assessment ids against (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? IucnDatabase { get; init; }

    [CommandOption("--iucn-api-cache <PATH>")]
    [Description("IUCN API cache to compare assessment ids against (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? IucnApiCache { get; init; }
}

[CommandInfo("wikidata iucn-assessment-items", CommandKind.Mutates,
    "Find the Wikidata items for individual IUCN Red List assessments (by DOI, published in, or Red List URL) and store them in the Wikidata cache, so the status dry run can cite them.",
    Reason = "Writes the wikidata_iucn_assessment_items table of the Wikidata cache. Only reads from Wikidata (SPARQL).",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Re-reads every item and replaces its row; rows for items no longer found are kept.",
    Examples = new[] {
        "wikidata iucn-assessment-items",
        "wikidata iucn-assessment-items --limit 200",
        "wikidata iucn-assessment-items --status",
    })]
public sealed class WikidataIucnAssessmentItemsCommand : AsyncCommand<WikidataIucnAssessmentItemsSettings> {
    private const int DetailBatchSize = 250;
    private const int PublishedInPageSize = 5000;

    private static readonly IReadOnlyDictionary<string, string> KnownLabels = new Dictionary<string, string>(StringComparer.Ordinal) {
        ["Q13442814"] = "scholarly article",
        ["Q1172284"] = "data set",
        ["Q16521"] = "taxon",
        ["Q1379672"] = "evaluation",
        ["Q55808"] = "seabird",
        ["Q32059"] = "IUCN Red List",
        ["Q17165819"] = "IUCN Red List 2014.1",
        ["Q25354282"] = "IUCN Red List 2016.1",
    };

    public override Task<int> ExecuteAsync(CommandContext context, WikidataIucnAssessmentItemsSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    internal static async Task<int> RunAsync(WikidataIucnAssessmentItemsSettings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveWikidataCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikidata cache:[/] {cachePath}");

        if (settings.Status) {
            var stored = ExistingAssessmentItemReader.Read(cachePath);
            if (!stored.TableFound || stored.Rows.Count == 0) {
                AnsiConsole.WriteLine("No assessment items stored yet. Run without --status to find them.");
                return 0;
            }

            var lastFetch = stored.Rows.Max(r => r.FetchedAtUtc);
            AnsiConsole.WriteLine($"Last looked up {lastFetch:yyyy-MM-dd HH:mm} UTC");
            PrintSummary(stored.Rows, LoadRelease(paths, settings), LoadApiBacklog(paths, settings));
            return 0;
        }

        var configuration = WikidataConfiguration.FromEnvironment();
        using var mainClient = new WikidataApiClient(configuration);
        using var scholarlyClient = new WikidataApiClient(configuration with { SparqlEndpoint = WikidataAssessmentItemQueries.ScholarlyEndpoint });
        WikidataApiClient Client(WikidataGraph graph) => graph == WikidataGraph.Scholarly ? scholarlyClient : mainClient;

        var runStartedUtc = DateTime.UtcNow;
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine("Finding assessment items on Wikidata");
        var discovery = await DiscoverAsync(mainClient, scholarlyClient, cancellationToken).ConfigureAwait(false);
        AnsiConsole.WriteLine($"  found by any route: {discovery.Tags.Count:N0}");

        var ordered = discovery.Tags.Keys.OrderBy(NumericId).ToList();
        if (settings.Limit is > 0 && ordered.Count > settings.Limit.Value) {
            ordered = ordered.Take(settings.Limit.Value).ToList();
        }

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine($"Reading statements for {ordered.Count:N0} items");
        using var store = WikidataCacheStore.Open(cachePath);
        var detail = await FetchDetailsAsync(ordered, discovery, Client, store, cancellationToken).ConfigureAwait(false);
        AnsiConsole.WriteLine($"  scholarly graph: {detail.FoundIn[WikidataGraph.Scholarly]:N0}");
        AnsiConsole.WriteLine($"  main graph: {detail.FoundIn[WikidataGraph.Main]:N0}");
        if (detail.NotFound.Count > 0) {
            // Deleted or merged since the search index last saw them.
            AnsiConsole.WriteLine($"  in neither graph (deleted or merged since they were found): {detail.NotFound.Count:N0}");
            AnsiConsole.WriteLine($"    {Examples(detail.NotFound)}");
        }

        var rows = store.ReadAssessmentItems();
        var complete = discovery.Failures.Count == 0 && ordered.Count == discovery.Tags.Count && detail.Failed == 0;
        if (complete) {
            var notSeen = rows.Count(r => r.FetchedAtUtc < runStartedUtc);
            if (notSeen > 0) {
                AnsiConsole.WriteLine($"  stored earlier but not found this run: {notSeen:N0}");
            }
        }
        else {
            AnsiConsole.WriteLine(discovery.Failures.Count > 0 || detail.Failed > 0
                ? "  Some queries failed, so this run is incomplete. Run it again to fill the gaps."
                : $"  Stopped at --limit {settings.Limit}.");
        }

        PrintSummary(rows, LoadRelease(paths, settings), LoadApiBacklog(paths, settings));
        return discovery.Failures.Count > 0 || detail.Failed > 0 ? 1 : 0;
    }

    // ------------------------------------------------------------------ discovery

    private sealed class Discovery {
        public Dictionary<string, SortedSet<string>> Tags { get; } = new(StringComparer.Ordinal);
        public List<string> Failures { get; } = new();

        public void Add(IEnumerable<string> qids, string tag) {
            foreach (var qid in qids) {
                if (!Tags.TryGetValue(qid, out var set)) {
                    Tags[qid] = set = new SortedSet<string>(StringComparer.Ordinal);
                }

                set.Add(tag);
            }
        }
    }

    private static async Task<Discovery> DiscoverAsync(WikidataApiClient main, WikidataApiClient scholarly, CancellationToken ct) {
        var discovery = new Discovery();
        var publications = new List<string> { WikidataAssessmentItemTable.RedListQid };

        var editions = await RouteAsync(discovery, "Red List editions (main graph)", async () => {
            var json = await main.QuerySparqlAsync(WikidataAssessmentItemQueries.Editions(), ct).ConfigureAwait(false);
            return WikidataAssessmentItemQueries.ParseItemIds(json, "item");
        }).ConfigureAwait(false);
        if (editions is not null) {
            publications.AddRange(editions.Where(e => e != WikidataAssessmentItemTable.RedListQid));
        }

        foreach (var graph in new[] { WikidataGraph.Scholarly, WikidataGraph.Main }) {
            var client = graph == WikidataGraph.Scholarly ? scholarly : main;
            var found = await RouteAsync(discovery, $"published in the Red List or an edition ({WikidataAssessmentItemQueries.GraphName(graph)} graph)",
                () => PublishedInAsync(client, publications, ct)).ConfigureAwait(false);
            if (found is not null) {
                discovery.Add(found, WikidataAssessmentItemQueries.RouteTag(WikidataAssessmentItemQueries.RoutePublishedIn, graph));
            }
        }

        var slices = WikidataAssessmentItemQueries.DoiSearchSlices(DateTime.UtcNow.Year + 1)
            .Append(IucnAssessmentRefParser.DoiPrefix.TrimEnd('.'))
            .ToList();
        var searchFound = await RouteAsync(discovery, $"DOI search ({slices.Count} prefix slices)", async () => {
            var union = new HashSet<string>(StringComparer.Ordinal);
            var failed = 0;
            foreach (var slice in slices) {
                try {
                    var json = await main.QuerySparqlAsync(WikidataAssessmentItemQueries.DoiSearch(slice), ct).ConfigureAwait(false);
                    union.UnionWith(WikidataAssessmentItemQueries.ParseItemIds(json, "title"));
                }
                catch (WikidataApiException ex) {
                    failed++;
                    AnsiConsole.WriteLine($"    search for {slice} failed: {ex.Message}");
                }
            }

            if (failed > 0) {
                discovery.Failures.Add($"DOI search ({failed} slices)");
            }

            return union.ToList();
        }).ConfigureAwait(false);
        if (searchFound is not null) {
            discovery.Add(searchFound, WikidataAssessmentItemQueries.RouteDoiSearch);
        }

        var scanFound = await RouteAsync(discovery, "DOI prefix scan (main graph)", async () => {
            var json = await main.QuerySparqlAsync(WikidataAssessmentItemQueries.DoiScan(), ct).ConfigureAwait(false);
            return WikidataAssessmentItemQueries.ParseItemIds(json, "item");
        }).ConfigureAwait(false);
        if (scanFound is not null) {
            discovery.Add(scanFound, WikidataAssessmentItemQueries.RouteTag(WikidataAssessmentItemQueries.RouteDoiScan, WikidataGraph.Main));
        }

        var datasetFound = await RouteAsync(discovery, "data sets linking iucnredlist.org (main graph)", async () => {
            var json = await main.QuerySparqlAsync(WikidataAssessmentItemQueries.DatasetUrl(), ct).ConfigureAwait(false);
            return WikidataAssessmentItemQueries.ParseItemIds(json, "item");
        }).ConfigureAwait(false);
        if (datasetFound is not null) {
            discovery.Add(datasetFound, WikidataAssessmentItemQueries.RouteTag(WikidataAssessmentItemQueries.RouteDatasetUrl, WikidataGraph.Main));
        }

        return discovery;
    }

    private static async Task<IReadOnlyList<string>?> RouteAsync(Discovery discovery, string label, Func<Task<IReadOnlyList<string>>> run) {
        var watch = Stopwatch.StartNew();
        try {
            var ids = (await run().ConfigureAwait(false)).Distinct(StringComparer.Ordinal).ToList();
            AnsiConsole.WriteLine($"  {label}: {ids.Count:N0}, {Elapsed(watch.Elapsed)}");
            return ids;
        }
        catch (WikidataApiException ex) {
            AnsiConsole.WriteLine($"  {label}: failed after {Elapsed(watch.Elapsed)}. {ex.Message}");
            discovery.Failures.Add(label);
            return null;
        }
    }

    private static async Task<IReadOnlyList<string>> PublishedInAsync(WikidataApiClient client, IReadOnlyCollection<string> publications, CancellationToken ct) {
        var all = new List<string>();
        var cursor = 0L;
        while (true) {
            var json = await client.QuerySparqlAsync(WikidataAssessmentItemQueries.PublishedIn(publications, cursor, PublishedInPageSize), ct).ConfigureAwait(false);
            var page = WikidataAssessmentItemQueries.ParseItemIds(json, "qid");
            all.AddRange(page);
            if (page.Count < PublishedInPageSize) {
                return all;
            }

            cursor = NumericId(page[^1]);
        }
    }

    // ------------------------------------------------------------------ details

    private sealed record DetailResult(Dictionary<WikidataGraph, int> FoundIn, List<string> NotFound, int Failed);

    private static async Task<DetailResult> FetchDetailsAsync(
        IReadOnlyList<string> qids,
        Discovery discovery,
        Func<WikidataGraph, WikidataApiClient> client,
        WikidataCacheStore store,
        CancellationToken ct) {
        var foundIn = new Dictionary<WikidataGraph, int> { [WikidataGraph.Scholarly] = 0, [WikidataGraph.Main] = 0 };
        var notFound = new List<string>();
        var failed = 0;

        // An item's statements are in exactly one graph. Ask the graph a route found it in first
        // (the search route doesn't say, and nearly everything is a scholarly article), then the other.
        var firstTry = qids.GroupBy(q => GraphHint(discovery.Tags[q]) ?? WikidataGraph.Scholarly);
        var secondTry = new Dictionary<WikidataGraph, List<string>> { [WikidataGraph.Scholarly] = new(), [WikidataGraph.Main] = new() };

        foreach (var group in firstTry) {
            var other = group.Key == WikidataGraph.Scholarly ? WikidataGraph.Main : WikidataGraph.Scholarly;
            var missing = await FetchGraphAsync(group.Key, group.ToList()).ConfigureAwait(false);
            secondTry[other].AddRange(missing);
        }

        foreach (var (graph, list) in secondTry) {
            if (list.Count > 0) {
                notFound.AddRange(await FetchGraphAsync(graph, list).ConfigureAwait(false));
            }
        }

        return new DetailResult(foundIn, notFound, failed);

        async Task<List<string>> FetchGraphAsync(WikidataGraph graph, List<string> batch) {
            var missing = new List<string>();
            var done = 0;
            foreach (var chunk in batch.Chunk(DetailBatchSize)) {
                string json;
                try {
                    json = await client(graph).QuerySparqlAsync(WikidataAssessmentItemQueries.Details(chunk), ct).ConfigureAwait(false);
                }
                catch (WikidataApiException ex) {
                    failed += chunk.Length;
                    AnsiConsole.WriteLine($"    statements for {chunk.Length} items ({WikidataAssessmentItemQueries.GraphName(graph)} graph) failed: {ex.Message}");
                    continue;
                }

                var fetchedAt = DateTime.UtcNow;
                var triples = WikidataAssessmentItemQueries.ParseDetails(json);
                var rows = new List<WikidataAssessmentItemRow>();
                foreach (var qid in chunk) {
                    if (!triples.TryGetValue(qid, out var itemTriples) || itemTriples.Count == 0) {
                        missing.Add(qid);
                        continue;
                    }

                    rows.Add(WikidataAssessmentItemBuilder.Build(qid, itemTriples, discovery.Tags[qid].ToList(),
                        WikidataAssessmentItemQueries.GraphName(graph), fetchedAt));
                }

                store.UpsertAssessmentItems(rows);
                foundIn[graph] += rows.Count;
                done += chunk.Length;
                if (batch.Count > DetailBatchSize * 4 && done % (DetailBatchSize * 8) == 0) {
                    AnsiConsole.WriteLine($"    {WikidataAssessmentItemQueries.GraphName(graph)} graph: {done:N0} of {batch.Count:N0}");
                }
            }

            return missing;
        }
    }

    private static WikidataGraph? GraphHint(IEnumerable<string> tags) {
        foreach (var tag in tags) {
            if (tag.EndsWith(":scholarly", StringComparison.Ordinal)) {
                return WikidataGraph.Scholarly;
            }

            if (tag.EndsWith(":main", StringComparison.Ordinal)) {
                return WikidataGraph.Main;
            }
        }

        return null;
    }

    // ------------------------------------------------------------------ IUCN id sets

    private static IucnReleaseIds? LoadRelease(PathsService paths, WikidataIucnAssessmentItemsSettings settings) {
        var path = settings.IucnDatabase ?? paths.GetIucnDatabasePath();
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) {
            return null;
        }

        using var connection = OpenReadOnly(path);
        if (!TableExists(connection, "assessments_html")) {
            return null;
        }

        var assessments = new HashSet<long>();
        var taxa = new HashSet<long>();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT assessmentId, taxonId FROM assessments_html";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            assessments.Add(reader.GetInt64(0));
            taxa.Add(reader.GetInt64(1));
        }

        return new IucnReleaseIds(Path.GetFileName(path), assessments, taxa);
    }

    private static IucnApiBacklogIds? LoadApiBacklog(PathsService paths, WikidataIucnAssessmentItemsSettings settings) {
        var path = settings.IucnApiCache ?? paths.GetIucnApiCachePath();
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) {
            return null;
        }

        using var connection = OpenReadOnly(path);
        if (!TableExists(connection, "taxa_assessment_backlog")) {
            return null;
        }

        var latest = new HashSet<long>();
        var all = new HashSet<long>();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT assessment_id, latest FROM taxa_assessment_backlog";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            all.Add(id);
            if (reader.GetInt64(1) == 1) {
                latest.Add(id);
            }
        }

        return new IucnApiBacklogIds(latest, all);
    }

    private static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    private static bool TableExists(SqliteConnection connection, string name) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=@name";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }

    // ------------------------------------------------------------------ output

    private static void PrintSummary(IReadOnlyList<WikidataAssessmentItemRow> rows, IucnReleaseIds? release, IucnApiBacklogIds? api) {
        var s = WikidataAssessmentItemSummary.Build(rows, release, api);

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine($"Stored items: {s.Total:N0} ({string.Join(", ", s.ByEndpoint.Select(e => $"{e.Endpoint} graph {e.Count:N0}"))})");

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine($"  {"Found by",-24} {"items",7}  {"only this route",15}");
        foreach (var (route, count, only) in s.ByRoute) {
            AnsiConsole.WriteLine($"  {route,-24} {count,7:N0}  {only,15:N0}");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine("IUCN ids read from the items");
        AnsiConsole.WriteLine($"  taxon and assessment id: {s.WithTaxonAndAssessmentId:N0} (from the DOI {s.IdsFromDoi:N0}, from a URL {s.IdsFromUrl:N0})");
        AnsiConsole.WriteLine($"  taxon id only: {s.TaxonIdOnly:N0}");
        AnsiConsole.WriteLine($"  IUCN DOI that no ids could be read from: {s.IucnDoiNotParsed:N0}");
        AnsiConsole.WriteLine($"  no IUCN DOI or Red List URL: {s.NoIucnDoiOrUrl:N0}");
        AnsiConsole.WriteLine($"  DOIs not in upper case (Wikidata stores DOIs upper-case): {s.DoiNotUpperCase:N0}");
        if (s.EarliestRelease is not null) {
            AnsiConsole.WriteLine($"  Red List versions in DOIs: {s.EarliestRelease} to {s.LatestRelease}");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine("Instance of (P31)");
        foreach (var (qid, count) in s.InstanceOf) {
            AnsiConsole.WriteLine($"  {Label(qid),-32} {count,7:N0}");
        }

        if (s.NoInstanceOf > 0) {
            AnsiConsole.WriteLine($"  {"none",-32} {s.NoInstanceOf,7:N0}");
        }

        if (s.TaxonItems.Count > 0) {
            AnsiConsole.WriteLine($"  Taxon items carrying an assessment DOI (not publications, left out of the comparisons below): {string.Join(", ", s.TaxonItems)}");
        }

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine("Published in (P1433)");
        foreach (var (qid, count) in s.PublishedIn) {
            AnsiConsole.WriteLine($"  {Label(qid),-32} {count,7:N0}");
        }

        AnsiConsole.WriteLine($"  {"none",-32} {s.NoPublishedIn,7:N0}");

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine($"Main subject (P921): {s.MainSubjectPresent:N0} with, {s.MainSubjectAbsent:N0} without");
        AnsiConsole.WriteLine("Authors");
        AnsiConsole.WriteLine($"  author items (P50): {s.WithAuthorItems:N0}");
        AnsiConsole.WriteLine($"  author name strings (P2093): {s.WithAuthorStrings:N0}");
        AnsiConsole.WriteLine($"  no author statements: {s.WithNoAuthorStatements:N0}");

        // Taxon items are left out of both comparisons and the duplicate check.
        AnsiConsole.WriteLine();
        if (release is not null) {
            AnsiConsole.WriteLine($"Publication items with an assessment id, compared with {release.Label} ({release.AssessmentIds.Count:N0} assessments)");
            AnsiConsole.WriteLine($"  cite an assessment in the release: {s.LatestInRelease:N0}");
            AnsiConsole.WriteLine($"  cite an older assessment of a taxon in the release: {s.OlderAssessmentOfTaxonInRelease:N0}");
            AnsiConsole.WriteLine($"  taxon not in the release: {s.TaxonNotInRelease:N0}");
        }
        else {
            AnsiConsole.WriteLine("No IUCN CSV release database found to compare with.");
        }

        if (api is not null) {
            AnsiConsole.WriteLine($"Compared with the IUCN API cache ({api.LatestAssessmentIds.Count:N0} latest assessments)");
            AnsiConsole.WriteLine($"  cite a latest assessment: {s.LatestInApiCache:N0}");
            AnsiConsole.WriteLine($"  cite a superseded assessment: {s.SupersededInApiCache:N0}");
            AnsiConsole.WriteLine($"  not in the cache: {s.NotInApiCache:N0}");
        }

        AnsiConsole.WriteLine();
        if (s.Duplicates.Count == 0) {
            AnsiConsole.WriteLine("Assessment ids on more than one item: none");
        }
        else {
            AnsiConsole.WriteLine($"Assessment ids on more than one item: {s.Duplicates.Count:N0} ids, {s.Duplicates.Sum(d => d.Qids.Count):N0} items");
            foreach (var duplicate in s.Duplicates.Take(10)) {
                AnsiConsole.WriteLine($"  {duplicate.AssessmentId}: {string.Join(", ", duplicate.Qids)}");
            }
        }
    }

    private static string Label(string qid) => KnownLabels.TryGetValue(qid, out var label) ? $"{qid} {label}" : qid;

    private static string Examples(IReadOnlyList<string> qids) =>
        string.Join(", ", qids.Take(10)) + (qids.Count > 10 ? ", ..." : string.Empty);

    private static string Elapsed(TimeSpan elapsed) =>
        elapsed.TotalMinutes >= 1
            ? $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds}s"
            : elapsed.TotalSeconds.ToString("0.0", CultureInfo.InvariantCulture) + "s";

    private static long NumericId(string qid) =>
        long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : long.MaxValue;
}
