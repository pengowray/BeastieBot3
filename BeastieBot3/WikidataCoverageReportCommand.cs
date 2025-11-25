using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataCoverageReportSettings : CommonSettings {
    [CommandOption("--iucn-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? IucnDatabase { get; init; }

    [CommandOption("--iucn-api-cache <PATH>")]
    [Description("Override path to the IUCN API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite). Used for synonyms.")]
    public string? IucnApiCache { get; init; }

    [CommandOption("--col-db <PATH>")]
    [Description("Override path to the Catalogue of Life SQLite database (defaults to Datastore:COL_sqlite). Used for synonyms.")]
    public string? ColDatabase { get; init; }

    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }

    [CommandOption("--limit <N>")]
    [Description("Limit the number of IUCN rows inspected (0 = all).")]
    public long Limit { get; init; }

    [CommandOption("--include-subpopulations")]
    [Description("Include subpopulation/regional assessments in the analysis.")]
    public bool IncludeSubpopulations { get; init; }

    [CommandOption("--sample-count <N>")]
    [Description("Number of unmatched samples to display (default 10).")]
    public int SampleCount { get; init; } = 10;
}

public sealed class WikidataCoverageReportCommand : AsyncCommand<WikidataCoverageReportSettings> {
    private static readonly WikiSiteDescriptor[] WikiSites = new[] {
        new WikiSiteDescriptor("enwiki", "English Wikipedia"),
        new WikiSiteDescriptor("commonswiki", "Wikimedia Commons"),
        new WikiSiteDescriptor("specieswiki", "Wikispecies")
    };

    public override Task<int> ExecuteAsync(CommandContext context, WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        if (settings.SampleCount < 0) {
            AnsiConsole.MarkupLine("[red]--sample-count must be zero or greater.[/]");
            return -1;
        }

        var paths = new PathsService(settings.IniFile, settings.SettingsDir);
        string iucnDb;
        string wikidataDb;
        try {
            iucnDb = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            wikidataDb = paths.ResolveWikidataCachePath(settings.WikidataCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(iucnDb)) {
            AnsiConsole.MarkupLine($"[red]IUCN SQLite database not found:[/] {Markup.Escape(iucnDb)}");
            return -3;
        }

        if (!File.Exists(wikidataDb)) {
            AnsiConsole.MarkupLine($"[red]Wikidata cache SQLite database not found:[/] {Markup.Escape(wikidataDb)}");
            return -4;
        }

        static string? ResolveOptionalPath(string? overridePath, string? configuredPath, string description) {
            var candidate = !string.IsNullOrWhiteSpace(overridePath) ? overridePath : configuredPath;
            if (string.IsNullOrWhiteSpace(candidate)) {
                return null;
            }

            try {
                var resolved = Path.GetFullPath(candidate);
                if (!File.Exists(resolved)) {
                    AnsiConsole.MarkupLineInterpolated($"[yellow]{description} not found at {Markup.Escape(resolved)}; skipping.[/]");
                    return null;
                }

                return resolved;
            }
            catch (Exception ex) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Unable to resolve {description} '{Markup.Escape(candidate)}': {Markup.Escape(ex.Message)}[/]");
                return null;
            }
        }

        var iucnApiCachePath = ResolveOptionalPath(settings.IucnApiCache, paths.GetIucnApiCachePath(), "IUCN API cache SQLite database");
        var colDbPath = ResolveOptionalPath(settings.ColDatabase, paths.GetColSqlitePath(), "Catalogue of Life SQLite database");

        var iucnConnectionString = new SqliteConnectionStringBuilder {
            DataSource = iucnDb,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        var wikidataConnectionString = new SqliteConnectionStringBuilder {
            DataSource = wikidataDb,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var iucnConnection = new SqliteConnection(iucnConnectionString);
        using var wikidataConnection = new SqliteConnection(wikidataConnectionString);
        iucnConnection.Open();
        wikidataConnection.Open();
        using var synonymService = new IucnSynonymService(iucnApiCachePath, colDbPath);

        if (!synonymService.HasIucnApiCache) {
            AnsiConsole.MarkupLine("[yellow]IUCN API cache not available; only locally derived names will be used for synonyms.[/]");
        }

        if (!synonymService.HasColDatabase) {
            AnsiConsole.MarkupLine("[yellow]Catalogue of Life database not available; COL synonyms will be skipped.[/]");
        }

        var repository = new IucnTaxonomyRepository(iucnConnection);
        if (!repository.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]Missing view view_assessments_html_taxonomy_html in the IUCN database.[/]");
            return -5;
        }

        var siteKeys = WikiSites.Select(s => s.Key).ToArray();
        var indexes = WikidataIndexBundle.Load(wikidataConnection, siteKeys);
        var stats = new CoverageStats(settings.SampleCount, WikiSites);

        foreach (var row in repository.ReadRows(settings.Limit, cancellationToken)) {
            if (!settings.IncludeSubpopulations && IsPopulationOrRegional(row)) {
                continue;
            }

            stats.Total++;
            var taxonId = row.InternalTaxonId?.Trim();
            var normalizedName = NormalizeScientificName(row);
            var isSubspecies = IsSubspecies(row);

            if (!string.IsNullOrEmpty(taxonId) && indexes.P627Claims.TryGetValue(taxonId, out var claimMatches)) {
                stats.Record(CoverageMatchMethod.P627Claim, row, isSubspecies);
                RecordWikiPresence(stats, isSubspecies, viaSynonym: false, claimMatches, indexes.SiteLinks);
                continue;
            }

            if (!string.IsNullOrEmpty(taxonId) && indexes.P627References.TryGetValue(taxonId, out var referenceMatches)) {
                stats.Record(CoverageMatchMethod.P627Reference, row, isSubspecies);
                RecordWikiPresence(stats, isSubspecies, viaSynonym: false, referenceMatches, indexes.SiteLinks);
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedName) && indexes.ScientificNames.TryGetValue(normalizedName, out var nameMatches)) {
                stats.Record(CoverageMatchMethod.ScientificName, row, isSubspecies);
                RecordWikiPresence(stats, isSubspecies, viaSynonym: false, nameMatches, indexes.SiteLinks);
                continue;
            }

            if (TryMatchAlternateNames(row, synonymService, indexes, stats, isSubspecies, cancellationToken)) {
                continue;
            }

            stats.Record(CoverageMatchMethod.None, row, isSubspecies);
        }

        RenderReport(stats, iucnDb, wikidataDb, settings);
        return 0;
    }

    private static void RenderReport(CoverageStats stats, string iucnPath, string wikidataPath, WikidataCoverageReportSettings settings) {
        AnsiConsole.MarkupLine($"[grey]IUCN DB:[/] {Markup.Escape(iucnPath)}");
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(wikidataPath)}");
        AnsiConsole.MarkupLine($"[grey]Rows considered:[/] {stats.Total}");

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Method");
        table.AddColumn("Matches");
        table.AddColumn("Percent");

        table.AddRow("P627 claim", stats.P627Claim.ToString(), stats.Percent(stats.P627Claim));
        table.AddRow("P627 reference", stats.P627Reference.ToString(), stats.Percent(stats.P627Reference));
        table.AddRow("Scientific name", stats.ScientificName.ToString(), stats.Percent(stats.ScientificName));
        table.AddRow("Synonym match", stats.SynonymMatches.ToString(), stats.Percent(stats.SynonymMatches));
        table.AddRow("Unmatched", stats.Unmatched.ToString(), stats.Percent(stats.Unmatched));

        AnsiConsole.Write(table);

        RenderWikiCoverage(stats);

        if (stats.UnmatchedSamples.Count > 0) {
            var sampleTable = new Table().Title("Unmatched Samples").Border(TableBorder.Minimal);
            sampleTable.AddColumn("IUCN Taxon ID");
            sampleTable.AddColumn("Scientific Name");
            sampleTable.AddColumn("RedList Version");

            foreach (var sample in stats.UnmatchedSamples.Take(settings.SampleCount)) {
                sampleTable.AddRow(sample.TaxonId ?? "?", sample.ScientificName ?? "?", sample.RedlistVersion);
            }

            AnsiConsole.Write(sampleTable);
        }
    }

    private static void RenderWikiCoverage(CoverageStats stats) {
        if (stats.Matched == 0) {
            AnsiConsole.MarkupLine("[grey]Wiki coverage stats require at least one matched row.[/]");
            return;
        }

        var wikiTable = new Table().Title("Wiki coverage (matched rows)").Border(TableBorder.Minimal);
        wikiTable.AddColumn("Category");
        foreach (var site in WikiSites) {
            wikiTable.AddColumn(site.DisplayName);
        }

        var rows = new[] {
            new WikiCoverageRow("All matched", WikiCoverageBucket.All, stats.Matched),
            new WikiCoverageRow("Species", WikiCoverageBucket.SpeciesDirect, stats.MatchedSpeciesDirect),
            new WikiCoverageRow("Species (via synonym)", WikiCoverageBucket.SpeciesSynonym, stats.MatchedSpeciesSynonym),
            new WikiCoverageRow("Subspecies", WikiCoverageBucket.SubspeciesDirect, stats.MatchedSubspeciesDirect),
            new WikiCoverageRow("Subspecies (via synonym)", WikiCoverageBucket.SubspeciesSynonym, stats.MatchedSubspeciesSynonym)
        };

        foreach (var row in rows) {
            var counts = stats.GetSiteCounts(row.Bucket);
            var values = new List<string> { row.Label };
            foreach (var site in WikiSites) {
                counts.TryGetValue(site.Key, out var count);
                values.Add(FormatCountPercent(count, row.Denominator));
            }

            wikiTable.AddRow(values.ToArray());
        }

        AnsiConsole.Write(wikiTable);
    }

    private static string FormatCountPercent(long count, long total) {
        if (total <= 0) {
            return $"{count} (0.0%)";
        }

        var pct = (double)count / total * 100;
        return $"{count} ({pct:0.0}%)";
    }

    private static void RecordWikiPresence(CoverageStats stats, bool isSubspecies, bool viaSynonym, IEnumerable<string>? entityIds, WikidataSiteLinkCache siteLinks) {
        var presence = entityIds is null ? WikiSiteLinkPresence.Empty : siteLinks.GetPresence(entityIds);
        stats.RecordSiteLinks(isSubspecies, viaSynonym, presence);
    }

    private static bool TryMatchAlternateNames(
        IucnTaxonomyRow row,
        IucnSynonymService synonymService,
        WikidataIndexBundle indexes,
        CoverageStats stats,
        bool isSubspecies,
        CancellationToken cancellationToken) {
        var candidates = synonymService.GetCandidates(row, cancellationToken)
            .Where(c => c.IsAlternateMatch);

        foreach (var candidate in candidates) {
            var normalized = ScientificNameHelper.Normalize(candidate.Name);
            if (string.IsNullOrEmpty(normalized)) {
                continue;
            }

            if (!indexes.ScientificNames.TryGetValue(normalized, out var entityIds)) {
                continue;
            }

            stats.Record(CoverageMatchMethod.Synonym, row, isSubspecies);
            RecordWikiPresence(stats, isSubspecies, viaSynonym: true, entityIds, indexes.SiteLinks);
            return true;
        }

        return false;
    }

    private static bool IsSubspecies(IucnTaxonomyRow row) => !string.IsNullOrWhiteSpace(row.InfraName);

    private static bool IsPopulationOrRegional(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.SubpopulationName)) {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(row.InfraType)) {
            var infra = row.InfraType.Trim();
            if (infra.Contains("population", StringComparison.OrdinalIgnoreCase)
                || infra.Contains("subpopulation", StringComparison.OrdinalIgnoreCase)
                || infra.Contains("regional", StringComparison.OrdinalIgnoreCase)) {
                return true;
            }
        }

        return false;
    }

    private static string? NormalizeScientificName(IucnTaxonomyRow row) {
        var raw = row.ScientificNameTaxonomy ?? row.ScientificNameAssessments ?? BuildNameFromParts(row);
        if (string.IsNullOrWhiteSpace(raw)) {
            return null;
        }

        var parts = raw.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0) {
            return null;
        }

        return string.Join(' ', parts).ToLowerInvariant();
    }

    private static string? BuildNameFromParts(IucnTaxonomyRow row) {
        var pieces = new List<string>();
        if (!string.IsNullOrWhiteSpace(row.GenusName)) {
            pieces.Add(row.GenusName.Trim());
        }

        if (!string.IsNullOrWhiteSpace(row.SpeciesName)) {
            pieces.Add(row.SpeciesName.Trim());
        }

        if (!string.IsNullOrWhiteSpace(row.InfraName)) {
            pieces.Add(row.InfraName.Trim());
        }

        return pieces.Count > 0 ? string.Join(' ', pieces) : null;
    }
}

internal enum CoverageMatchMethod {
    None,
    P627Claim,
    P627Reference,
    ScientificName,
    Synonym
}

internal sealed class CoverageStats {
    private static readonly IReadOnlyDictionary<string, long> EmptyCounts = new Dictionary<string, long>();

    public CoverageStats(int maxSamples, IReadOnlyList<WikiSiteDescriptor> sites) {
        MaxSamples = maxSamples;
        _sites = sites ?? throw new ArgumentNullException(nameof(sites));
    }

    public long Total { get; set; }
    public long P627Claim { get; private set; }
    public long P627Reference { get; private set; }
    public long ScientificName { get; private set; }
    public long SynonymMatches { get; private set; }
    public long Unmatched { get; private set; }
    public List<CoverageSample> UnmatchedSamples { get; } = new();
    public long Matched => P627Claim + P627Reference + ScientificName + SynonymMatches;
    public long MatchedSpeciesDirect => _matchedSpeciesDirect;
    public long MatchedSpeciesSynonym => _matchedSpeciesSynonym;
    public long MatchedSubspeciesDirect => _matchedSubspeciesDirect;
    public long MatchedSubspeciesSynonym => _matchedSubspeciesSynonym;

    private int MaxSamples { get; }
    private readonly IReadOnlyList<WikiSiteDescriptor> _sites;
    private readonly Dictionary<WikiCoverageBucket, Dictionary<string, long>> _siteCounts = new();
    private long _matchedSpeciesDirect;
    private long _matchedSpeciesSynonym;
    private long _matchedSubspeciesDirect;
    private long _matchedSubspeciesSynonym;

    public void Record(CoverageMatchMethod method, IucnTaxonomyRow row, bool isSubspecies) {
        switch (method) {
            case CoverageMatchMethod.P627Claim:
                P627Claim++;
                TrackMatch(isSubspecies, viaSynonym: false);
                break;
            case CoverageMatchMethod.P627Reference:
                P627Reference++;
                TrackMatch(isSubspecies, viaSynonym: false);
                break;
            case CoverageMatchMethod.ScientificName:
                ScientificName++;
                TrackMatch(isSubspecies, viaSynonym: false);
                break;
            case CoverageMatchMethod.Synonym:
                SynonymMatches++;
                TrackMatch(isSubspecies, viaSynonym: true);
                break;
            default:
                Unmatched++;
                if (UnmatchedSamples.Count < MaxSamples) {
                    UnmatchedSamples.Add(new CoverageSample(row.InternalTaxonId, row.ScientificNameTaxonomy ?? row.ScientificNameAssessments, row.RedlistVersion ?? "?"));
                }
                break;
        }
    }

    public void RecordSiteLinks(bool isSubspecies, bool viaSynonym, WikiSiteLinkPresence presence) {
        foreach (var site in _sites) {
            if (!presence.HasSite(site.Key)) {
                continue;
            }

            IncrementSiteCount(WikiCoverageBucket.All, site.Key);
            var bucket = DetermineBucket(isSubspecies, viaSynonym);
            IncrementSiteCount(bucket, site.Key);
        }
    }

    public IReadOnlyDictionary<string, long> GetSiteCounts(WikiCoverageBucket bucket) {
        return _siteCounts.TryGetValue(bucket, out var counts)
            ? counts
            : EmptyCounts;
    }

    public string Percent(long count) {
        if (Total == 0) {
            return "0%";
        }

        var pct = (double)count / Total * 100;
        return $"{pct:0.0}%";
    }

    private void TrackMatch(bool isSubspecies, bool viaSynonym) {
        if (isSubspecies) {
            if (viaSynonym) {
                _matchedSubspeciesSynonym++;
            }
            else {
                _matchedSubspeciesDirect++;
            }

            return;
        }

        if (viaSynonym) {
            _matchedSpeciesSynonym++;
        }
        else {
            _matchedSpeciesDirect++;
        }
    }

    private static WikiCoverageBucket DetermineBucket(bool isSubspecies, bool viaSynonym) {
        if (isSubspecies) {
            return viaSynonym ? WikiCoverageBucket.SubspeciesSynonym : WikiCoverageBucket.SubspeciesDirect;
        }

        return viaSynonym ? WikiCoverageBucket.SpeciesSynonym : WikiCoverageBucket.SpeciesDirect;
    }

    private void IncrementSiteCount(WikiCoverageBucket bucket, string siteKey) {
        if (!_siteCounts.TryGetValue(bucket, out var counts)) {
            counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            _siteCounts[bucket] = counts;
        }

        counts.TryGetValue(siteKey, out var current);
        counts[siteKey] = current + 1;
    }
}

internal sealed record CoverageSample(string? TaxonId, string? ScientificName, string RedlistVersion);

internal sealed record WikiSiteDescriptor(string Key, string DisplayName);

internal sealed record WikiCoverageRow(string Label, WikiCoverageBucket Bucket, long Denominator);

internal enum WikiCoverageBucket {
    All,
    SpeciesDirect,
    SpeciesSynonym,
    SubspeciesDirect,
    SubspeciesSynonym
}

internal sealed class WikidataSiteLinkCache {
    private readonly SqliteConnection _connection;
    private readonly IReadOnlyList<string> _siteKeys;
    private readonly Dictionary<string, WikiSiteLinkPresence> _cache = new(StringComparer.OrdinalIgnoreCase);

    public WikidataSiteLinkCache(SqliteConnection connection, IReadOnlyList<string> siteKeys) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _siteKeys = siteKeys ?? throw new ArgumentNullException(nameof(siteKeys));
    }

    public WikiSiteLinkPresence GetPresence(IEnumerable<string>? entityIds) {
        if (entityIds is null) {
            return WikiSiteLinkPresence.Empty;
        }

        var aggregate = WikiSiteLinkPresence.Empty;
        foreach (var entityId in entityIds) {
            if (string.IsNullOrWhiteSpace(entityId)) {
                continue;
            }

            var presence = GetPresenceForEntity(entityId);
            aggregate = aggregate.Combine(presence);
            if (aggregate.HasAllSites(_siteKeys)) {
                break;
            }
        }

        return aggregate;
    }

    private WikiSiteLinkPresence GetPresenceForEntity(string entityId) {
        if (_cache.TryGetValue(entityId, out var cached)) {
            return cached;
        }

        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT json FROM wikidata_entities WHERE entity_id=@id LIMIT 1";
        command.Parameters.AddWithValue("@id", entityId);
        var json = command.ExecuteScalar() as string;
        var presence = WikiSiteLinkPresence.FromJson(json, _siteKeys);
        _cache[entityId] = presence;
        return presence;
    }
}

internal sealed class WikiSiteLinkPresence {
    public static readonly WikiSiteLinkPresence Empty = new(new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase));

    private readonly IReadOnlyDictionary<string, bool> _flags;

    private WikiSiteLinkPresence(IReadOnlyDictionary<string, bool> flags) {
        _flags = flags;
    }

    public bool HasSite(string siteKey) => _flags.TryGetValue(siteKey, out var hasSite) && hasSite;

    public bool HasAllSites(IEnumerable<string> siteKeys) => siteKeys.All(HasSite);

    public WikiSiteLinkPresence Combine(WikiSiteLinkPresence other) {
        if (_flags.Count == 0) {
            return other;
        }

        if (other._flags.Count == 0) {
            return this;
        }

        var merged = new Dictionary<string, bool>(_flags, StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in other._flags) {
            if (kvp.Value) {
                merged[kvp.Key] = true;
            }
        }

        return new WikiSiteLinkPresence(merged);
    }

    public static WikiSiteLinkPresence FromJson(string? json, IReadOnlyList<string> siteKeys) {
        if (string.IsNullOrWhiteSpace(json)) {
            return Empty;
        }

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("entities", out var entities) || entities.ValueKind != JsonValueKind.Object) {
                return Empty;
            }

            foreach (var entity in entities.EnumerateObject()) {
                if (entity.Value.ValueKind != JsonValueKind.Object) {
                    continue;
                }

                if (!entity.Value.TryGetProperty("sitelinks", out var sitelinks) || sitelinks.ValueKind != JsonValueKind.Object) {
                    continue;
                }

                var flags = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var siteKey in siteKeys) {
                    if (sitelinks.TryGetProperty(siteKey, out var site) && site.ValueKind == JsonValueKind.Object) {
                        flags[siteKey] = true;
                    }
                }

                return flags.Count == 0 ? Empty : new WikiSiteLinkPresence(flags);
            }
        }
        catch (JsonException) {
            // Ignore malformed JSON; treat as missing sitelinks.
        }

        return Empty;
    }
}

internal sealed class WikidataIndexBundle {
    private WikidataIndexBundle(WikidataSiteLinkCache siteLinks) {
        SiteLinks = siteLinks ?? throw new ArgumentNullException(nameof(siteLinks));
    }

    public WikidataSiteLinkCache SiteLinks { get; }
    public Dictionary<string, List<string>> P627Claims { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, List<string>> P627References { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, List<string>> ScientificNames { get; } = new(StringComparer.OrdinalIgnoreCase);

    public static WikidataIndexBundle Load(SqliteConnection connection, IReadOnlyList<string> siteKeys) {
        var bundle = new WikidataIndexBundle(new WikidataSiteLinkCache(connection, siteKeys));
        bundle.LoadP627(connection, "claim", bundle.P627Claims);
        bundle.LoadP627(connection, "reference", bundle.P627References);
        bundle.LoadNames(connection);
        return bundle;
    }

    private void LoadP627(SqliteConnection connection, string source, Dictionary<string, List<string>> target) {
        using var command = connection.CreateCommand();
        command.CommandText = @"SELECT v.value, e.entity_id
FROM wikidata_p627_values v
JOIN wikidata_entities e ON e.entity_numeric_id = v.entity_numeric_id
WHERE v.source = @source";
        command.Parameters.AddWithValue("@source", source);

        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            var value = reader.GetString(0).Trim();
            var entityId = reader.GetString(1);
            if (!target.TryGetValue(value, out var list)) {
                list = new List<string>();
                target[value] = list;
            }

            if (!list.Any(existing => string.Equals(existing, entityId, StringComparison.OrdinalIgnoreCase))) {
                list.Add(entityId);
            }
        }
    }

    private void LoadNames(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = @"SELECT LOWER(name), e.entity_id
FROM wikidata_scientific_names n
JOIN wikidata_entities e ON e.entity_numeric_id = n.entity_numeric_id";

        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            var name = reader.IsDBNull(0) ? null : reader.GetString(0)?.Trim();
            if (string.IsNullOrWhiteSpace(name)) {
                continue;
            }

            var entityId = reader.GetString(1);
            if (!ScientificNames.TryGetValue(name!, out var list)) {
                list = new List<string>();
                ScientificNames[name!] = list;
            }

            if (!list.Any(existing => string.Equals(existing, entityId, StringComparison.OrdinalIgnoreCase))) {
                list.Add(entityId);
            }
        }
    }
}
