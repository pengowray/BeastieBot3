using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
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

    [CommandOption("--output-dir <DIR>")]
    [Description("Base directory for coverage detail outputs. Defaults to the current working directory.")]
    public string? OutputDirectory { get; init; }

    [CommandOption("--synonym-output <FILE>")]
    [Description("Optional path for the synonym matches Markdown output file.")]
    public string? SynonymOutputPath { get; init; }

    [CommandOption("--unmatched-output <FILE>")]
    [Description("Optional path for the unmatched taxa Markdown output file.")]
    public string? UnmatchedOutputPath { get; init; }
}

public sealed class WikidataCoverageReportCommand : AsyncCommand<WikidataCoverageReportSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        var exitCode = WikidataCoverageAnalysis.TryExecute(settings, cancellationToken, out var analysisResult);
        if (exitCode != 0 || analysisResult is null) {
            return exitCode;
        }

        RenderReport(analysisResult, settings);
        return 0;
    }

    private static void RenderReport(WikidataCoverageAnalysisResult result, WikidataCoverageReportSettings settings) {
        var stats = result.Stats;
        AnsiConsole.MarkupLine($"[grey]IUCN DB:[/] {Markup.Escape(result.IucnDatabasePath)}");
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(result.WikidataDatabasePath)}");
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
        foreach (var site in WikidataCoverageSites.All) {
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
            foreach (var site in WikidataCoverageSites.All) {
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
    public List<SynonymCoverageItem> SynonymDetails { get; } = new();
    public List<IucnTaxonomyRow> UnmatchedDetails { get; } = new();
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
                UnmatchedDetails.Add(row);
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

    public void RecordSynonymDetail(IucnTaxonomyRow row, TaxonNameCandidate candidate, IEnumerable<string> entityIds) {
        if (row is null) {
            throw new ArgumentNullException(nameof(row));
        }

        if (candidate is null) {
            throw new ArgumentNullException(nameof(candidate));
        }

        var ids = entityIds?
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Select(id => id.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList()
            ?? new List<string>();

        SynonymDetails.Add(new SynonymCoverageItem(row, candidate, ids));
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

internal sealed record SynonymCoverageItem(
    IucnTaxonomyRow Row,
    TaxonNameCandidate Candidate,
    IReadOnlyList<string> EntityIds);

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
        var (hasIndex, isComplete) = GetTaxonNameIndexStatus(connection);
        if (hasIndex) {
            LoadNamesFromQuery(connection,
                @"SELECT normalized_name, e.entity_id
FROM wikidata_taxon_name_index n
JOIN wikidata_entities e ON e.entity_numeric_id = n.entity_numeric_id");

            if (isComplete && ScientificNames.Count > 0) {
                return;
            }
        }

        LoadNamesFromQuery(connection,
            @"SELECT LOWER(name), e.entity_id
FROM wikidata_scientific_names n
JOIN wikidata_entities e ON e.entity_numeric_id = n.entity_numeric_id");
    }

    private void LoadNamesFromQuery(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            var raw = reader.IsDBNull(0) ? null : reader.GetString(0);
            var normalized = ScientificNameHelper.Normalize(raw);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var entityId = reader.GetString(1);
            if (!ScientificNames.TryGetValue(normalized, out var list)) {
                list = new List<string>();
                ScientificNames[normalized] = list;
            }

            if (!list.Any(existing => string.Equals(existing, entityId, StringComparison.OrdinalIgnoreCase))) {
                list.Add(entityId);
            }
        }
    }

    private static (bool HasIndex, bool IsComplete) GetTaxonNameIndexStatus(SqliteConnection connection) {
        using var exists = connection.CreateCommand();
        exists.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='wikidata_taxon_name_index' LIMIT 1";
        var hasIndex = exists.ExecuteScalar() is not null;
        if (!hasIndex) {
            return (false, false);
        }

        var indexCount = GetTableCount(connection, "wikidata_taxon_name_index");
        var sourceCount = GetTableCount(connection, "wikidata_scientific_names");
        var isComplete = sourceCount == 0 || indexCount >= sourceCount;
        return (true, isComplete);
    }

    private static long GetTableCount(SqliteConnection connection, string tableName) {
        using var count = connection.CreateCommand();
        count.CommandText = $"SELECT COUNT(*) FROM {tableName}";
        var result = count.ExecuteScalar();
        return Convert.ToInt64(result ?? 0L);
    }
}
