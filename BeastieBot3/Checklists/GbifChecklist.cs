using System.ComponentModel;
using System.Globalization;
using System.IO.Compression;
using System.Text.Json;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// GBIF occurrence counts by country, as a fifth checklist:
//   `checklists gbif-fetch`  asks GBIF's occurrence API, once per country, how many records of each
//                            species it has there (facet on speciesKey), leaving out fossils, living
//                            specimens (zoos, gardens) and records with location issues, and stores
//                            the counts by GBIF species key (gbif_species_country). Resumable: a
//                            country is asked again only after --refresh-days.
//   GbifBackbone             matches IUCN species to GBIF species keys through GBIF's backbone
//                            (simple.txt.gz, about 490 MB, downloaded once): same name, same kingdom,
//                            an accepted name before a synonym; a name that leads to two keys is not matched.
// `checklists import --source gbif` then writes the counts of the matched species as checklist rows
// (origin "recorded"), which checklists crosscheck reads as presence, not as native range.

namespace BeastieBot3.Checklists;

internal static class GbifChecklist {
    public const string BackboneUrl = "https://hosted-datasets.gbif.org/datasets/backbone/current/simple.txt.gz";
    public const string BackboneFile = "gbif-backbone-simple.txt.gz";
    public const string Source = "gbif";
    public const string Title = "GBIF occurrence records";

    // basisOfRecord values kept: everything but FOSSIL_SPECIMEN and LIVING_SPECIMEN.
    private static readonly string[] Bases = ["HUMAN_OBSERVATION", "PRESERVED_SPECIMEN", "MATERIAL_SAMPLE", "MACHINE_OBSERVATION",
        "OCCURRENCE", "MATERIAL_CITATION", "OBSERVATION"];

    public static string FacetUrl(string country) =>
        "https://api.gbif.org/v1/occurrence/search?limit=0&facet=speciesKey&facetLimit=1000000&occurrenceStatus=PRESENT&hasGeospatialIssue=false"
        + $"&country={country}" + string.Concat(Bases.Select(b => "&basisOfRecord=" + b));

    /// The facet counts of one answer: species key and records.
    internal static List<(long SpeciesKey, long Records)> ReadFacet(Stream json) {
        using var doc = JsonDocument.Parse(json);
        var list = new List<(long, long)>();
        if (doc.RootElement.TryGetProperty("facets", out var facets)) {
            foreach (var facet in facets.EnumerateArray()) {
                foreach (var count in facet.GetProperty("counts").EnumerateArray()) {
                    if (long.TryParse(count.GetProperty("name").GetString(), NumberStyles.None, CultureInfo.InvariantCulture, out var key)) {
                        list.Add((key, count.GetProperty("count").GetInt64()));
                    }
                }
            }
        }
        return list;
    }

    /// GBIF's kingdom keys, by IUCN's kingdom name.
    internal static readonly Dictionary<string, long> KingdomKeys = new(StringComparer.OrdinalIgnoreCase) {
        ["ANIMALIA"] = 1, ["PLANTAE"] = 6, ["FUNGI"] = 5, ["CHROMISTA"] = 4, ["PROTOZOA"] = 7, ["BACTERIA"] = 3,
    };

    /// GBIF species key for each wanted (name, GBIF kingdom key), from the backbone's simple file:
    /// columns 0 id, 3 is_synonym, 4 status, 5 rank, 10 kingdom key, 16 species key, 19 canonical name.
    internal static Dictionary<(string Name, long Kingdom), long> MatchBackbone(TextReader backbone, IReadOnlySet<(string Name, long Kingdom)> wanted) {
        var best = new Dictionary<(string, long), (int Rank, long Key, bool Ambiguous)>();
        for (var line = backbone.ReadLine(); line is not null; line = backbone.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length < 20 || f[5] != "SPECIES" || !long.TryParse(f[10], out var kingdom)) {
                continue;
            }
            var key = (f[19], kingdom);
            if (!wanted.Contains(key) || !long.TryParse(f[16], out var species)) {
                continue;
            }
            var rank = f[4] switch { "ACCEPTED" => 0, "DOUBTFUL" => 1, _ => 2 };
            if (!best.TryGetValue(key, out var known) || rank < known.Rank) {
                best[key] = (rank, species, false);
            } else if (rank == known.Rank && species != known.Key) {
                best[key] = known with { Ambiguous = true };
            }
        }
        return best.Where(p => !p.Value.Ambiguous).ToDictionary(p => p.Key, p => p.Value.Key);
    }

    /// The checklist rows: for each IUCN species matched to a GBIF key, its countries with records.
    public static List<ChecklistArea> Rows(ChecklistStore store, SqliteConnection site, string backbonePath, out int matched, out int iucnSpecies) {
        var taxa = new List<(string Name, long Kingdom)>();
        using (var command = site.CreateCommand()) {
            command.CommandText = "SELECT scientific_name, kingdom FROM taxon WHERE in_release = 1 AND kind = 'species' AND latest_global_assessment_id IS NOT NULL";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (!reader.IsDBNull(1) && KingdomKeys.TryGetValue(reader.GetString(1), out var k)) {
                    taxa.Add((reader.GetString(0), k));
                }
            }
        }
        iucnSpecies = taxa.Count;
        Dictionary<(string Name, long Kingdom), long> keys;
        using (var stream = new GZipStream(File.OpenRead(backbonePath), CompressionMode.Decompress))
        using (var reader = new StreamReader(stream)) {
            keys = MatchBackbone(reader, taxa.ToHashSet());
        }
        // A key two IUCN species lead to (a lump in GBIF's backbone) is left out.
        var nameOfKey = keys.GroupBy(p => p.Value).Where(g => g.Count() == 1).ToDictionary(g => g.Key, g => g.First().Key.Name);
        matched = nameOfKey.Count;
        var rows = new List<ChecklistArea>();
        foreach (var (key, country, records) in store.GbifCounts()) {
            if (nameOfKey.TryGetValue(key, out var name)) {
                rows.Add(new ChecklistArea(name, country, ChecklistSchemes.Iso2, ChecklistOrigins.Recorded, records));
            }
        }
        return rows;
    }
}

[CommandInfo("checklists gbif-fetch", CommandKind.Mutates,
    "Download from GBIF's occurrence API how many records of each species GBIF has in each country (no fossils, zoos or gardens, or records with location issues), for checklists import --source gbif. About 250 requests; a stopped run carries on.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Asks only for the countries not downloaded yet, and those downloaded more than --refresh-days ago.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] { "checklists gbif-fetch --status", "checklists gbif-fetch --limit 5", "checklists gbif-fetch", "checklists gbif-fetch --refresh-days 90" })]
internal sealed class ChecklistsGbifFetchCommand : AsyncCommand<ChecklistsGbifFetchCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--site-db <FILE>")]
        [Description("The public site's database, for the list of countries. Default: Datastore:site_sqlite in paths.ini.")]
        public string? SiteDatabase { get; init; }

        [CommandOption("--store <PATH>")]
        [Description("Checklists store. Default: Datastore:checklists_sqlite, else checklists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Download at most N countries in this run. 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Also download again the countries downloaded more than this many days ago.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how many countries are downloaded and left, and download nothing.")]
        public bool Status { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = settings.StorePath ?? paths.GetChecklistsPath();
        var sitePath = ChecklistPaths.ExpandHome(settings.SiteDatabase ?? paths.GetSiteDatabasePath());
        if (storePath is null || sitePath is null || !File.Exists(sitePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Site database or checklists store not found:[/] {sitePath ?? "(not set)"}");
            return -1;
        }
        List<string> countries;
        using (var site = ChecklistPaths.OpenReadOnly(sitePath)) {
            using var command = site.CreateCommand();
            command.CommandText = "SELECT code FROM area WHERE length(code) = 2 ORDER BY code";
            using var reader = command.ExecuteReader();
            countries = [];
            while (reader.Read()) {
                countries.Add(reader.GetString(0));
            }
        }
        using var store = ChecklistStore.Open(storePath);
        var fetched = store.GbifFetched();
        DateTime? refreshBefore = settings.RefreshDays is > 0 ? DateTime.UtcNow.AddDays(-settings.RefreshDays.Value) : null;
        var due = countries.Where(c => !fetched.TryGetValue(c, out var at) || (refreshBefore is { } cutoff && at < cutoff)).ToList();
        AnsiConsole.MarkupLineInterpolated($"Countries: {countries.Count:N0}. Downloaded: {countries.Count - due.Count:N0}. To download: {due.Count:N0}.");
        if (settings.Status || due.Count == 0) {
            return 0;
        }
        if (settings.Limit > 0) {
            due = due.Take(settings.Limit).ToList();
        }
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(3) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd("BeastieBot3/1.0 (+https://en.wikipedia.org/wiki/User:Beastie_Bot)");
        var failures = 0;
        await ProgressConsole.RunAsync("Countries", due.Count, async progress => {
            foreach (var country in due) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    using var response = await http.GetAsync(GbifChecklist.FacetUrl(country), cancellationToken);
                    response.EnsureSuccessStatusCode();
                    await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
                    var counts = GbifChecklist.ReadFacet(body);
                    store.SaveGbifCountry(country, counts, DateTime.UtcNow);
                    failures = 0;
                } catch (Exception e) when (e is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                    AnsiConsole.MarkupLineInterpolated($"[yellow]{country} not downloaded:[/] {e.Message}");
                    if (++failures >= 5) {
                        AnsiConsole.MarkupLine("[red]Stopped: five requests in a row failed.[/] Run the command again to carry on.");
                        break;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
                }
                progress.Increment(1);
                // GBIF does not promise any rate: a pause keeps the load low.
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }, cancellationToken);
        return failures > 0 ? 1 : 0;
    }
}

internal static class ChecklistPaths {
    public static string? ExpandHome(string? path) =>
        path is not null && path.StartsWith('~') ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), path.TrimStart('~', '/')) : path;

    public static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
        connection.Open();
        return connection;
    }
}
