using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataWikipediaMismatchReportSettings : CommonSettings {
    [CommandOption("--iucn-db <PATH>")]
    [Description("Override path to the IUCN taxonomy SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? IucnDatabase { get; init; }

    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }

    [CommandOption("--wikipedia-cache <PATH>")]
    [Description("Override path to the Wikipedia cache SQLite database (defaults to Datastore:enwiki_cache_sqlite).")]
    public string? WikipediaCache { get; init; }

    [CommandOption("--output-dir <DIR>")]
    [Description("Directory for generated reports (defaults to Reports:output_dir or ./reports).")]
    public string? OutputDirectory { get; init; }

    [CommandOption("--markdown-output <FILE>")]
    [Description("Explicit Markdown output path.")]
    public string? MarkdownOutput { get; init; }

    [CommandOption("--csv-output <FILE>")]
    [Description("Explicit CSV output path.")]
    public string? CsvOutput { get; init; }
}

public sealed class WikidataWikipediaMismatchReportCommand : Command<WikidataWikipediaMismatchReportSettings> {
    public override int Execute(CommandContext context, WikidataWikipediaMismatchReportSettings settings, CancellationToken cancellationToken) {
        _ = context;
        _ = cancellationToken;

        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string iucnPath;
        string wikidataCachePath;
        string wikipediaCachePath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            wikidataCachePath = paths.ResolveWikidataCachePath(settings.WikidataCache);
            wikipediaCachePath = paths.ResolveWikipediaCachePath(settings.WikipediaCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }

        if (!File.Exists(iucnPath) || !File.Exists(wikidataCachePath) || !File.Exists(wikipediaCachePath)) {
            if (!File.Exists(iucnPath)) {
                AnsiConsole.MarkupLineInterpolated($"[red]IUCN database not found:[/] {Markup.Escape(iucnPath)}");
            }
            if (!File.Exists(wikidataCachePath)) {
                AnsiConsole.MarkupLineInterpolated($"[red]Wikidata cache not found:[/] {Markup.Escape(wikidataCachePath)}");
            }
            if (!File.Exists(wikipediaCachePath)) {
                AnsiConsole.MarkupLineInterpolated($"[red]Wikipedia cache not found:[/] {Markup.Escape(wikipediaCachePath)}");
            }
            return -2;
        }

        using var iucnConnection = OpenReadOnlyConnection(iucnPath);
        var iucnRepository = new IucnTaxonomyRepository(iucnConnection);
        using var wikidataStore = WikidataCacheStore.Open(wikidataCachePath);
        using var wikipediaStore = WikipediaCacheStore.Open(wikipediaCachePath);

        var mappings = wikidataStore.GetIucnMappings();
        if (mappings.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No Wikidata entities with IUCN IDs were found in the cache.[/]");
            return 0;
        }

        var analyzer = new WikidataWikipediaMismatchAnalyzer(iucnRepository, wikidataStore, wikipediaStore);
        var analysis = analyzer.Analyze(mappings);

        AnsiConsole.MarkupLineInterpolated($"[grey]Evaluated:[/] {analysis.EntitiesEvaluated:n0} entities (issues: {analysis.Issues.Count:n0}).");
        if (analysis.Issues.Count == 0) {
            AnsiConsole.MarkupLine("[green]No Wikipedia mismatches detected for cached entries.[/]");
            return 0;
        }

        var outputDir = DetermineOutputDirectory(settings, paths, iucnPath);
        Directory.CreateDirectory(outputDir);
        var markdownPath = settings.MarkdownOutput ?? Path.Combine(outputDir, "wikidata-wiki-mismatches.md");
        var csvPath = settings.CsvOutput ?? Path.Combine(outputDir, "wikidata-wiki-mismatches.csv");

        WriteMarkdownReport(markdownPath, analysis);
        WriteCsvReport(csvPath, analysis);

        AnsiConsole.MarkupLineInterpolated($"[green]Reports written to[/] {Markup.Escape(markdownPath)} [grey]and[/] {Markup.Escape(csvPath)}.");
        return 0;
    }

    private static string DetermineOutputDirectory(WikidataWikipediaMismatchReportSettings settings, PathsService paths, string iucnPath) {
        if (!string.IsNullOrWhiteSpace(settings.OutputDirectory)) {
            return Path.GetFullPath(settings.OutputDirectory);
        }

        var configured = paths.GetReportOutputDirectory();
        if (!string.IsNullOrWhiteSpace(configured)) {
            return configured!;
        }

        var baseDir = Path.GetDirectoryName(iucnPath);
        if (string.IsNullOrWhiteSpace(baseDir)) {
            baseDir = Directory.GetCurrentDirectory();
        }

        return Path.Combine(baseDir!, "reports");
    }

    private static SqliteConnection OpenReadOnlyConnection(string path) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();
        return connection;
    }

    private static void WriteMarkdownReport(string path, WikidataWikipediaMismatchAnalysis analysis) {
        var summary = BuildIssueSummary(analysis.Issues);
        var builder = new StringBuilder();
        builder.AppendLine("# Wikidata ↔️ Wikipedia mismatch report");
        builder.AppendLine();
        builder.AppendLine($"Generated: {DateTime.UtcNow:O}");
        builder.AppendLine($"Entities evaluated: {analysis.EntitiesEvaluated:n0}");
        builder.AppendLine($"Issues found: {analysis.Issues.Count:n0}");
        builder.AppendLine();
        builder.AppendLine("| Issue | Count |");
        builder.AppendLine("| --- | --- |");
        foreach (var kvp in summary.OrderByDescending(k => k.Value).ThenBy(k => k.Key, StringComparer.OrdinalIgnoreCase)) {
            builder.AppendLine($"| {kvp.Key} | {kvp.Value} |");
        }
        builder.AppendLine();
        builder.AppendLine("| Wikidata | IUCN ID | IUCN Name | Page | Issue | Details |");
        builder.AppendLine("| --- | --- | --- | --- | --- | --- |");
        foreach (var issue in analysis.Issues) {
            var wikiLink = $"[{issue.EntityId}](https://www.wikidata.org/wiki/{issue.EntityId})";
            var pageLink = $"[{EscapeMarkdown(issue.PageTitle)}]({issue.PageUrl})";
            builder.AppendLine($"| {wikiLink} | {issue.IucnTaxonId} | {EscapeMarkdown(issue.IucnName)} | {pageLink} | {issue.IssueType} | {EscapeMarkdown(issue.Details)} |");
        }

        File.WriteAllText(path, builder.ToString());
    }

    private static void WriteCsvReport(string path, WikidataWikipediaMismatchAnalysis analysis) {
        var builder = new StringBuilder();
        builder.AppendLine("entity_id,iucn_taxon_id,iucn_name,page_title,page_url,issue_type,details,expected_rank,actual_rank");
        foreach (var issue in analysis.Issues) {
            builder.AppendLine(string.Join(',',
                Csv(issue.EntityId),
                Csv(issue.IucnTaxonId),
                Csv(issue.IucnName),
                Csv(issue.PageTitle),
                Csv(issue.PageUrl),
                Csv(issue.IssueType),
                Csv(issue.Details),
                Csv(issue.ExpectedRank ?? string.Empty),
                Csv(issue.ActualRank ?? string.Empty)));
        }

        File.WriteAllText(path, builder.ToString());
    }

    private static string Csv(string value) {
        if (value.Contains('"') || value.Contains(',') || value.Contains('\n')) {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }

        return value;
    }

    private static string EscapeMarkdown(string value) => value.Replace("|", "\\|");

    private static Dictionary<string, int> BuildIssueSummary(IEnumerable<WikidataWikiIssue> issues) {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var issue in issues) {
            map.TryGetValue(issue.IssueType, out var count);
            map[issue.IssueType] = count + 1;
        }

        return map;
    }
}

internal sealed class WikidataWikipediaMismatchAnalyzer {
    private readonly IucnTaxonomyRepository _iucnRepository;
    private readonly WikidataCacheStore _wikidataStore;
    private readonly WikipediaCacheStore _wikipediaStore;
    private readonly Dictionary<string, IucnTaxonomyRow> _iucnCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<long, string?> _enwikiCache = new();

    public WikidataWikipediaMismatchAnalyzer(IucnTaxonomyRepository iucnRepository, WikidataCacheStore wikidataStore, WikipediaCacheStore wikipediaStore) {
        _iucnRepository = iucnRepository;
        _wikidataStore = wikidataStore;
        _wikipediaStore = wikipediaStore;
    }

    public WikidataWikipediaMismatchAnalysis Analyze(IReadOnlyList<WikidataIucnMapping> mappings) {
        var issues = new List<WikidataWikiIssue>();
        var evaluated = 0;

        foreach (var mapping in mappings) {
            var row = GetIucnRow(mapping.IucnTaxonId);
            if (row is null) {
                continue;
            }

            var title = GetEnwikiTitle(mapping.EntityNumericId);
            if (string.IsNullOrWhiteSpace(title)) {
                continue;
            }

            var normalized = WikipediaTitleHelper.Normalize(title);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var page = _wikipediaStore.GetPageByNormalizedTitle(normalized);
            if (page is null || !string.Equals(page.DownloadStatus, WikiPageDownloadStatus.Cached, StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            evaluated++;
            var taxobox = _wikipediaStore.GetTaxoboxData(page.PageRowId);
            var context = new WikiIssueContext(mapping, row, page, taxobox);
            issues.AddRange(WikiIssueDetector.FindIssues(context));
        }

        return new WikidataWikipediaMismatchAnalysis(evaluated, issues);
    }

    private IucnTaxonomyRow? GetIucnRow(string iucnTaxonId) {
        if (_iucnCache.TryGetValue(iucnTaxonId, out var cached)) {
            return cached;
        }

        if (!long.TryParse(iucnTaxonId, out var taxonIdLong)) {
            return null;
        }

        var row = _iucnRepository.GetRowByTaxonId(taxonIdLong);
        if (row is not null) {
            _iucnCache[iucnTaxonId] = row;
        }

        return row;
    }

    private string? GetEnwikiTitle(long entityNumericId) {
        if (_enwikiCache.TryGetValue(entityNumericId, out var cached)) {
            return cached;
        }

        var title = _wikidataStore.GetEnwikiTitle(entityNumericId);
        _enwikiCache[entityNumericId] = title;
        return title;
    }
}

internal sealed record WikidataWikipediaMismatchAnalysis(int EntitiesEvaluated, IReadOnlyList<WikidataWikiIssue> Issues);

internal sealed record WikidataWikiIssue(
    string EntityId,
    string IucnTaxonId,
    string IucnName,
    string PageTitle,
    string PageUrl,
    string IssueType,
    string Details,
    string? ExpectedRank,
    string? ActualRank
);

internal sealed record WikiIssueContext(
    WikidataIucnMapping Mapping,
    IucnTaxonomyRow IucnRow,
    WikiPageSummary Page,
    WikiTaxoboxData? Taxobox
);

internal static class WikiIssueDetector {
    public static IReadOnlyList<WikidataWikiIssue> FindIssues(WikiIssueContext context) {
        var issues = new List<WikidataWikiIssue>();
        var expectedRank = DetermineExpectedRank(context.IucnRow);
        var actualRank = DetermineActualRank(context.Taxobox, context.Page, context.IucnRow);
        var iucnName = GetIucnScientificName(context.IucnRow);
        var pageUrl = BuildPageUrl(context.Page.PageTitle);

        void AddIssue(string type, string details) {
            issues.Add(new WikidataWikiIssue(
                context.Mapping.EntityId,
                context.Mapping.IucnTaxonId,
                iucnName,
                context.Page.PageTitle,
                pageUrl,
                type,
                details,
                expectedRank?.ToString(),
                actualRank?.ToString()));
        }

        if (context.Page.IsRedirect) {
            AddIssue("Redirect", context.Page.RedirectTarget is null
                ? "Page is a redirect with unknown target"
                : $"Redirects to {context.Page.RedirectTarget}");
        }

        if (context.Page.IsDisambiguation) {
            AddIssue("Disambiguation", "Page is marked as a disambiguation article");
        }

        if (context.Page.IsSetIndex) {
            AddIssue("SetIndex", "Page is a set index rather than a taxon article");
        }

        if (!context.Page.HasTaxobox) {
            AddIssue("MissingTaxobox", "Page does not include a taxobox template");
        }

        if (expectedRank is null) {
            return issues;
        }

        if (actualRank is null) {
            return issues;
        }

        if (NeedsParentWarning(expectedRank.Value, actualRank.Value, context.Taxobox)) {
            AddIssue("ParentTaxon", "Wikipedia page appears to describe the parent taxon (genus) and the genus is not marked as monotypic");
        }
        else if (expectedRank.Value != actualRank.Value) {
            AddIssue("RankMismatch", $"Expected {expectedRank.Value} but taxobox indicates {actualRank.Value}");
        }

        return issues;
    }

    private static TaxonRank? DetermineExpectedRank(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.InfraName)) {
            return TaxonRank.Subspecies;
        }

        if (!string.IsNullOrWhiteSpace(row.SpeciesName)) {
            return TaxonRank.Species;
        }

        if (!string.IsNullOrWhiteSpace(row.GenusName) && string.IsNullOrWhiteSpace(row.SpeciesName)) {
            return TaxonRank.Genus;
        }

        return null;
    }

    private static TaxonRank? DetermineActualRank(WikiTaxoboxData? taxobox, WikiPageSummary page, IucnTaxonomyRow row) {
        if (taxobox is not null) {
            var normalized = NormalizeRank(taxobox.Rank);
            if (normalized.HasValue) {
                return normalized;
            }

            if (!string.IsNullOrWhiteSpace(taxobox.ScientificName)) {
                var inferred = InferRankFromName(taxobox.ScientificName);
                if (inferred.HasValue) {
                    return inferred;
                }
            }
        }

        var titleRank = InferRankFromName(page.PageTitle);
        if (titleRank == TaxonRank.Genus && !string.IsNullOrWhiteSpace(row.SpeciesName)) {
            return TaxonRank.Genus;
        }

        return titleRank;
    }

    private static TaxonRank? NormalizeRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return null;
        }

        var normalized = rank.Trim().ToLowerInvariant();
        if (normalized.Contains("subspecies", StringComparison.Ordinal) || normalized.Contains("ssp", StringComparison.Ordinal)) {
            return TaxonRank.Subspecies;
        }

        if (normalized.Contains("species", StringComparison.Ordinal)) {
            return TaxonRank.Species;
        }

        if (normalized.Contains("genus", StringComparison.Ordinal)) {
            return TaxonRank.Genus;
        }

        return null;
    }

    private static TaxonRank? InferRankFromName(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }

        var parts = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length switch {
            1 => TaxonRank.Genus,
            2 => TaxonRank.Species,
            >= 3 => TaxonRank.Subspecies,
            _ => null
        };
    }

    private static bool NeedsParentWarning(TaxonRank expected, TaxonRank actual, WikiTaxoboxData? taxobox) {
        if (expected == TaxonRank.Genus) {
            return false;
        }

        if (actual != TaxonRank.Genus) {
            return false;
        }

        return taxobox?.IsMonotypic != true;
    }

    private static string GetIucnScientificName(IucnTaxonomyRow row) {
        return row.ScientificNameTaxonomy
            ?? row.ScientificNameAssessments
            ?? ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName)
            ?? row.GenusName;
    }

    private static string BuildPageUrl(string pageTitle) {
        var slug = WikipediaTitleHelper.ToSlug(pageTitle);
        return $"https://en.wikipedia.org/wiki/{slug}";
    }
}

internal enum TaxonRank {
    Genus,
    Species,
    Subspecies
}
