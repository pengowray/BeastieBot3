using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3;

internal sealed record WikidataCoverageAnalysisResult(
    CoverageStats Stats,
    string IucnDatabasePath,
    string WikidataDatabasePath,
    string? IucnApiCachePath
);

internal static class WikidataCoverageAnalysis {
    public static int TryExecute(WikidataCoverageReportSettings settings, CancellationToken cancellationToken, out WikidataCoverageAnalysisResult? result) {
        result = null;

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

        var siteKeys = WikidataCoverageSites.All.Select(site => site.Key).ToArray();
        var indexes = WikidataIndexBundle.Load(wikidataConnection, siteKeys);
        var stats = new CoverageStats(settings.SampleCount, WikidataCoverageSites.All);

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

        result = new WikidataCoverageAnalysisResult(stats, iucnDb, wikidataDb, iucnApiCachePath);
        return 0;
    }

    private static string? ResolveOptionalPath(string? overridePath, string? configuredPath, string description) {
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

            stats.RecordSynonymDetail(row, candidate, entityIds);
            stats.Record(CoverageMatchMethod.Synonym, row, isSubspecies);
            RecordWikiPresence(stats, isSubspecies, viaSynonym: true, entityIds, indexes.SiteLinks);
            return true;
        }

        return false;
    }

    private static void RecordWikiPresence(CoverageStats stats, bool isSubspecies, bool viaSynonym, IEnumerable<string>? entityIds, WikidataSiteLinkCache siteLinks) {
        var presence = entityIds is null ? WikiSiteLinkPresence.Empty : siteLinks.GetPresence(entityIds);
        stats.RecordSiteLinks(isSubspecies, viaSynonym, presence);
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
        return ScientificNameHelper.Normalize(raw);
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
