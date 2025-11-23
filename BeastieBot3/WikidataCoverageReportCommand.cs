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

        var repository = new IucnTaxonomyRepository(iucnConnection);
        if (!repository.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]Missing view view_assessments_html_taxonomy_html in the IUCN database.[/]");
            return -5;
        }

        var indexes = WikidataIndexBundle.Load(wikidataConnection);
        var stats = new CoverageStats(settings.SampleCount);

        foreach (var row in repository.ReadRows(settings.Limit, cancellationToken)) {
            if (!settings.IncludeSubpopulations && IsPopulationOrRegional(row)) {
                continue;
            }

            stats.Total++;
            var taxonId = row.InternalTaxonId?.Trim();
            var normalizedName = NormalizeScientificName(row);
            var isSubspecies = IsSubspecies(row);

            if (!string.IsNullOrEmpty(taxonId) && indexes.P627Claims.TryGetValue(taxonId, out var claimMatches)) {
                stats.Record(CoverageMatchMethod.P627Claim, row);
                RecordWikiPresence(stats, isSubspecies, claimMatches, indexes.SiteLinks);
                continue;
            }

            if (!string.IsNullOrEmpty(taxonId) && indexes.P627References.TryGetValue(taxonId, out var referenceMatches)) {
                stats.Record(CoverageMatchMethod.P627Reference, row);
                RecordWikiPresence(stats, isSubspecies, referenceMatches, indexes.SiteLinks);
                continue;
            }

            if (!string.IsNullOrEmpty(normalizedName) && indexes.ScientificNames.TryGetValue(normalizedName, out var nameMatches)) {
                stats.Record(CoverageMatchMethod.ScientificName, row);
                RecordWikiPresence(stats, isSubspecies, nameMatches, indexes.SiteLinks);
                continue;
            }

            stats.Record(CoverageMatchMethod.None, row);
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
        var presence = stats.WikiPresence;
        if (presence.MatchedTotal == 0) {
            AnsiConsole.MarkupLine("[grey]Wiki coverage stats require at least one matched row.[/]");
            return;
        }

        var wikiTable = new Table().Title("Wiki coverage (matched rows)").Border(TableBorder.Minimal);
        wikiTable.AddColumn("Category");
        wikiTable.AddColumn("enwiki");
        wikiTable.AddColumn("Wikispecies");

        wikiTable.AddRow(
            "All matched",
            FormatCountPercent(presence.EnWikiTotal, presence.MatchedTotal),
            FormatCountPercent(presence.WikispeciesTotal, presence.MatchedTotal)
        );

        wikiTable.AddRow(
            "Species",
            FormatCountPercent(presence.SpeciesEnWiki, presence.MatchedSpecies),
            FormatCountPercent(presence.SpeciesWikispecies, presence.MatchedSpecies)
        );

        wikiTable.AddRow(
            "Subspecies",
            FormatCountPercent(presence.SubspeciesEnWiki, presence.MatchedSubspecies),
            FormatCountPercent(presence.SubspeciesWikispecies, presence.MatchedSubspecies)
        );

        AnsiConsole.Write(wikiTable);
    }

    private static string FormatCountPercent(long count, long total) {
        if (total <= 0) {
            return $"{count} (0.0%)";
        }

        var pct = (double)count / total * 100;
        return $"{count} ({pct:0.0}%)";
    }

    private static void RecordWikiPresence(CoverageStats stats, bool isSubspecies, IEnumerable<string>? entityIds, WikidataSiteLinkCache siteLinks) {
        var presence = entityIds is null ? WikiSiteLinkPresence.Empty : siteLinks.GetPresence(entityIds);
        stats.RecordSiteLinks(isSubspecies, presence.HasEnWiki, presence.HasWikispecies);
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
    ScientificName
}

internal sealed class CoverageStats {
    public CoverageStats(int maxSamples) {
        MaxSamples = maxSamples;
    }

    public long Total { get; set; }
    public long P627Claim { get; private set; }
    public long P627Reference { get; private set; }
    public long ScientificName { get; private set; }
    public long Unmatched { get; private set; }
    public List<CoverageSample> UnmatchedSamples { get; } = new();
    public long Matched => P627Claim + P627Reference + ScientificName;

    private int MaxSamples { get; }
    private long _matchedSpecies;
    private long _matchedSubspecies;
    private long _speciesEnWiki;
    private long _speciesWikispecies;
    private long _subspeciesEnWiki;
    private long _subspeciesWikispecies;

    public void Record(CoverageMatchMethod method, IucnTaxonomyRow row) {
        switch (method) {
            case CoverageMatchMethod.P627Claim:
                P627Claim++;
                break;
            case CoverageMatchMethod.P627Reference:
                P627Reference++;
                break;
            case CoverageMatchMethod.ScientificName:
                ScientificName++;
                break;
            default:
                Unmatched++;
                if (UnmatchedSamples.Count < MaxSamples) {
                    UnmatchedSamples.Add(new CoverageSample(row.InternalTaxonId, row.ScientificNameTaxonomy ?? row.ScientificNameAssessments, row.RedlistVersion ?? "?"));
                }
                break;
        }
    }

    public void RecordSiteLinks(bool isSubspecies, bool hasEnWiki, bool hasWikispecies) {
        if (isSubspecies) {
            _matchedSubspecies++;
            if (hasEnWiki) {
                _subspeciesEnWiki++;
            }

            if (hasWikispecies) {
                _subspeciesWikispecies++;
            }
            return;
        }

        _matchedSpecies++;
        if (hasEnWiki) {
            _speciesEnWiki++;
        }

        if (hasWikispecies) {
            _speciesWikispecies++;
        }
    }

    public CoverageWikiPresence WikiPresence => new(
        _matchedSpecies,
        _matchedSubspecies,
        _speciesEnWiki,
        _speciesWikispecies,
        _subspeciesEnWiki,
        _subspeciesWikispecies
    );

    public string Percent(long count) {
        if (Total == 0) {
            return "0%";
        }

        var pct = (double)count / Total * 100;
        return $"{pct:0.0}%";
    }
}

internal sealed record CoverageSample(string? TaxonId, string? ScientificName, string RedlistVersion);

internal readonly record struct CoverageWikiPresence(
    long MatchedSpecies,
    long MatchedSubspecies,
    long SpeciesEnWiki,
    long SpeciesWikispecies,
    long SubspeciesEnWiki,
    long SubspeciesWikispecies
) {
    public long MatchedTotal => MatchedSpecies + MatchedSubspecies;
    public long EnWikiTotal => SpeciesEnWiki + SubspeciesEnWiki;
    public long WikispeciesTotal => SpeciesWikispecies + SubspeciesWikispecies;
}

internal sealed class WikidataSiteLinkCache {
    private readonly SqliteConnection _connection;
    private readonly Dictionary<string, WikiSiteLinkPresence> _cache = new(StringComparer.OrdinalIgnoreCase);

    public WikidataSiteLinkCache(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
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
            if (aggregate.HasEnWiki && aggregate.HasWikispecies) {
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
        var presence = WikiSiteLinkPresence.FromJson(json);
        _cache[entityId] = presence;
        return presence;
    }
}

internal readonly struct WikiSiteLinkPresence {
    public static readonly WikiSiteLinkPresence Empty = new(false, false);

    public WikiSiteLinkPresence(bool hasEnWiki, bool hasWikispecies) {
        HasEnWiki = hasEnWiki;
        HasWikispecies = hasWikispecies;
    }

    public bool HasEnWiki { get; }
    public bool HasWikispecies { get; }

    public WikiSiteLinkPresence Combine(WikiSiteLinkPresence other) =>
        new(HasEnWiki || other.HasEnWiki, HasWikispecies || other.HasWikispecies);

    public static WikiSiteLinkPresence FromJson(string? json) {
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

                var hasEnWiki = sitelinks.TryGetProperty("enwiki", out var enwiki) && enwiki.ValueKind == JsonValueKind.Object;
                var hasWikispecies = sitelinks.TryGetProperty("specieswiki", out var specieswiki) && specieswiki.ValueKind == JsonValueKind.Object;
                return new WikiSiteLinkPresence(hasEnWiki, hasWikispecies);
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

    public static WikidataIndexBundle Load(SqliteConnection connection) {
        var bundle = new WikidataIndexBundle(new WikidataSiteLinkCache(connection));
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
