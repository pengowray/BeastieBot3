using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataIucnFreshnessReportSettings : CommonSettings {
    [CommandOption("--iucn-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs).")]
    public string? IucnDatabase { get; init; }

    [CommandOption("--wikidata-cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite).")]
    public string? WikidataCache { get; init; }

    [CommandOption("--include-subpopulations")]
    [Description("Include IUCN subpopulation/regional assessments in the analysis.")]
    public bool IncludeSubpopulations { get; init; }

    [CommandOption("--output <PATH>")]
    [Description("Write the generated report to this path. Defaults to ./reports/wikidata-iucn-freshness-<timestamp>.md.")]
    public string? OutputPath { get; init; }
}

public sealed class WikidataIucnFreshnessReportCommand : AsyncCommand<WikidataIucnFreshnessReportSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataIucnFreshnessReportSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(WikidataIucnFreshnessReportSettings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile, settings.SettingsDir);

        string iucnPath;
        string wikidataPath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            wikidataPath = paths.ResolveWikidataCachePath(settings.WikidataCache);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN SQLite database not found:[/] {Markup.Escape(iucnPath)}");
            return -2;
        }

        if (!File.Exists(wikidataPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikidata cache SQLite database not found:[/] {Markup.Escape(wikidataPath)}");
            return -3;
        }

        string outputPath;
        try {
            outputPath = ResolveOutputPath(settings.OutputPath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to resolve output path:[/] {Markup.Escape(ex.Message)}");
            return -4;
        }

        var iucnConnectionString = new SqliteConnectionStringBuilder {
            DataSource = iucnPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        var wikidataConnectionString = new SqliteConnectionStringBuilder {
            DataSource = wikidataPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var iucnConnection = new SqliteConnection(iucnConnectionString);
        using var wikidataConnection = new SqliteConnection(wikidataConnectionString);
        iucnConnection.Open();
        wikidataConnection.Open();

        var analyzer = new WikidataIucnFreshnessAnalyzer(iucnConnection, wikidataConnection, settings.IncludeSubpopulations);
        var stats = analyzer.Execute(cancellationToken);

        var context = new WikidataIucnFreshnessReportContext(
            iucnPath,
            wikidataPath,
            outputPath,
            settings.IncludeSubpopulations,
            DateTimeOffset.Now);

        var markdown = WikidataIucnFreshnessReportBuilder.Build(context, stats);

        try {
            var directory = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrWhiteSpace(directory)) {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(outputPath, markdown, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to write report:[/] {Markup.Escape(ex.Message)}");
            return -5;
        }

        AnsiConsole.MarkupLine($"[green]Report saved to:[/] {Markup.Escape(outputPath)}");
        AnsiConsole.MarkupLine($"[grey]IUCN taxa considered:[/] {stats.TotalIucnTaxa:N0}");
        AnsiConsole.MarkupLine($"[grey]P141 references inspected:[/] {stats.TotalP141References:N0}");
        return 0;
    }

    private static string ResolveOutputPath(string? requestedPath) {
        if (!string.IsNullOrWhiteSpace(requestedPath)) {
            return Path.GetFullPath(requestedPath);
        }

        var baseDir = Path.Combine(Environment.CurrentDirectory, "reports");
        var fileName = $"wikidata-iucn-freshness-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.md";
        return Path.GetFullPath(Path.Combine(baseDir, fileName));
    }
}

internal sealed record WikidataIucnFreshnessReportContext(
    string IucnDatabasePath,
    string WikidataDatabasePath,
    string OutputPath,
    bool IncludeSubpopulations,
    DateTimeOffset GeneratedAt
);

internal static class WikidataIucnFreshnessReportBuilder {
    public static string Build(WikidataIucnFreshnessReportContext context, WikidataIucnFreshnessStats stats) {
        var builder = new StringBuilder();
        builder.AppendLine("# Wikidata vs IUCN Freshness Report");
        builder.AppendLine();
        builder.AppendLine($"- Generated: {context.GeneratedAt:O}");
        builder.AppendLine($"- IUCN DB: `{Escape(context.IucnDatabasePath)}`");
        builder.AppendLine($"- Wikidata cache: `{Escape(context.WikidataDatabasePath)}`");
        builder.AppendLine($"- Include subpopulations: {(context.IncludeSubpopulations ? "yes" : "no")}");
        builder.AppendLine($"- IUCN taxa considered: {stats.TotalIucnTaxa:N0}");
        builder.AppendLine($"- P141 references inspected: {stats.TotalP141References:N0}");
        builder.AppendLine();

        WriteP627CoverageSection(builder, stats);
        WriteNameAlignmentSection(builder, stats);
        WriteP141CoverageSection(builder, stats);
        return builder.ToString();
    }

    private static void WriteP627CoverageSection(StringBuilder builder, WikidataIucnFreshnessStats stats) {
        builder.AppendLine("## Direct P627 Coverage");
        builder.AppendLine("Quantifies how many IUCN taxa in the cache are linked to Wikidata via a direct P627 taxon ID claim.");
        builder.AppendLine();

        builder.AppendLine("| Metric | Count | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        builder.AppendLine($"| IUCN taxa with P627 | {stats.IucnTaxaWithDirectP627:N0} | {FormatPercent(stats.IucnTaxaWithDirectP627, stats.TotalIucnTaxa)} |");
        builder.AppendLine($"| IUCN taxa lacking P627 | {stats.TotalIucnTaxa - stats.IucnTaxaWithDirectP627:N0} | {FormatPercent(stats.TotalIucnTaxa - stats.IucnTaxaWithDirectP627, stats.TotalIucnTaxa)} |");
        builder.AppendLine();
    }

    private static void WriteNameAlignmentSection(StringBuilder builder, WikidataIucnFreshnessStats stats) {
        builder.AppendLine("## Name Alignment For P627 Claims");
        builder.AppendLine("Checks whether Wikidata entities that cite an IUCN taxon ID also use the same scientific name as IUCN (ignoring rank tokens such as 'ssp.').");
        builder.AppendLine();

        builder.AppendLine("| Metric | Count | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        builder.AppendLine($"| Wikidata entities with P627 | {stats.WikidataEntitiesWithP627:N0} | 100.0% |");
        builder.AppendLine($"| Entities with exact name match | {stats.WikidataEntitiesWithExactNameMatch:N0} | {FormatPercent(stats.WikidataEntitiesWithExactNameMatch, stats.WikidataEntitiesWithP627)} |");
        builder.AppendLine($"| Entities using alternate/synonym names | {stats.WikidataEntitiesUsingSynonymName:N0} | {FormatPercent(stats.WikidataEntitiesUsingSynonymName, stats.WikidataEntitiesWithP627)} |");
        builder.AppendLine($"| Entities missing a scientific name | {stats.WikidataEntitiesMissingScientificName:N0} | {FormatPercent(stats.WikidataEntitiesMissingScientificName, stats.WikidataEntitiesWithP627)} |");
        builder.AppendLine();

        if (stats.NameMismatchSamples.Count == 0) {
            builder.AppendLine("All sampled entities with P627 either matched the IUCN name or lacked a recorded name.");
        }
        else {
            builder.AppendLine("Sample synonym/alternate-name cases:");
            foreach (var sample in stats.NameMismatchSamples.Take(6)) {
                var iucnNames = string.Join(", ", sample.Taxa.Select(t => $"{Escape(t.TaxonId)} ({Escape(t.Name ?? "unknown")})"));
                var wikiNames = sample.WikidataNames.Count > 0
                    ? string.Join(", ", sample.WikidataNames.Select(Escape))
                    : "(no P225 recorded)";
                var labelPart = string.IsNullOrWhiteSpace(sample.Label) ? string.Empty : $" — {Escape(sample.Label!)}";
                builder.AppendLine($"- {Escape(sample.EntityId)}{labelPart}: IUCN → {iucnNames}; Wikidata → {Escape(wikiNames)}");
            }
        }

        builder.AppendLine();
    }

    private static void WriteP141CoverageSection(StringBuilder builder, WikidataIucnFreshnessStats stats) {
        builder.AppendLine("## P141 Conservation Status Coverage");
        builder.AppendLine("Details how many IUCN taxa are referenced by Wikidata conservation status statements (P141) and whether those statements are in sync with the latest Red List data.");
        builder.AppendLine();

        builder.AppendLine("### 3.1 Status Reach");
        builder.AppendLine("Counts of taxa and entities participating in P141 statements.");
        builder.AppendLine();

        var taxaWithP141 = stats.TaxaWithP141.Count;
        var entitiesWithP141 = stats.EntitiesWithP141.Count;
        var entitiesMissingP627 = stats.EntitiesWithP141ButMissingP627.Count;
        builder.AppendLine("| Metric | Count | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        builder.AppendLine($"| IUCN taxa with P141 reference | {taxaWithP141:N0} | {FormatPercent(taxaWithP141, stats.TotalIucnTaxa)} |");
        builder.AppendLine($"| Wikidata entities with P141 | {entitiesWithP141:N0} | {FormatPercent(entitiesWithP141, entitiesWithP141)} |");
        builder.AppendLine($"| …of which lack a P627 claim | {entitiesMissingP627:N0} | {FormatPercent(entitiesMissingP627, entitiesWithP141)} |");
        builder.AppendLine();

        if (stats.MissingP627Samples.Count > 0) {
            builder.AppendLine("Entities with P141 but no P627 claim (sample):");
            foreach (var sample in stats.MissingP627Samples.Take(6)) {
                var labelPart = string.IsNullOrWhiteSpace(sample.Label) ? string.Empty : $" — {Escape(sample.Label!)}";
                builder.AppendLine($"- {Escape(sample.EntityId)}{labelPart}: taxa {string.Join(", ", sample.TaxonIds.Select(Escape))}");
            }
            builder.AppendLine();
        }

        builder.AppendLine("### 3.2 Reference Editions (P248)");
        builder.AppendLine("Shows which Red List editions are cited inside P141 references. Missing rows indicate statements lacking a source edition.");
        builder.AppendLine();

        var totalReferences = Math.Max(1, stats.TotalP141References);
        builder.AppendLine("| Source | References | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        foreach (var row in stats.SourceCounts.OrderByDescending(kvp => kvp.Value)) {
            var label = stats.SourceLabels.TryGetValue(row.Key, out var friendly)
                ? friendly
                : $"Q{row.Key}";
            builder.AppendLine($"| {Escape(label)} | {row.Value:N0} | {FormatPercent(row.Value, totalReferences)} |");
        }
        if (stats.P141ReferencesMissingSource > 0) {
            builder.AppendLine($"| (missing P248) | {stats.P141ReferencesMissingSource:N0} | {FormatPercent(stats.P141ReferencesMissingSource, totalReferences)} |");
        }
        builder.AppendLine();

        builder.AppendLine("### 3.3 Retrieved Dates (P813)");
        builder.AppendLine("Distribution of 'retrieved' years embedded in P141 references.");
        builder.AppendLine();

        builder.AppendLine("| Year | References | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        foreach (var row in stats.RetrievedYearCounts.OrderByDescending(kvp => kvp.Key)) {
            builder.AppendLine($"| {row.Key} | {row.Value:N0} | {FormatPercent(row.Value, totalReferences)} |");
        }
        if (stats.ReferencesMissingRetrievedYear > 0) {
            builder.AppendLine($"| (missing P813) | {stats.ReferencesMissingRetrievedYear:N0} | {FormatPercent(stats.ReferencesMissingRetrievedYear, totalReferences)} |");
        }
        builder.AppendLine();

        builder.AppendLine("### 3.4 Status Agreement");
        builder.AppendLine("Compares the P141 conservation status against the latest Red List category for the cited taxon.");
        builder.AppendLine();

        var status = stats.StatusAgreement;
        builder.AppendLine("| Outcome | Count | Share |");
        builder.AppendLine("| --- | ---: | ---: |");
        builder.AppendLine($"| Matches current Red List | {status.Matches:N0} | {FormatPercent(status.Matches, status.TotalComparisons)} |");
        builder.AppendLine($"| Mismatches | {status.Mismatches:N0} | {FormatPercent(status.Mismatches, status.TotalComparisons)} |");
        builder.AppendLine($"| Missing IUCN category data | {status.UnknownIucnCategory:N0} | {FormatPercent(status.UnknownIucnCategory, totalReferences)} |");
        builder.AppendLine($"| Wikidata status outside allowed set | {status.UnmappedWikidataStatus:N0} | {FormatPercent(status.UnmappedWikidataStatus, totalReferences)} |");
        builder.AppendLine();

        if (status.MismatchSamples.Count > 0) {
            builder.AppendLine("Sample mismatches:");
            foreach (var sample in status.MismatchSamples.Take(6)) {
                var labelPart = string.IsNullOrWhiteSpace(sample.EntityLabel) ? string.Empty : $" — {Escape(sample.EntityLabel!)}";
                builder.AppendLine($"- {Escape(sample.TaxonId)}: IUCN {Escape(sample.IucnCategory ?? "?")} vs Wikidata {Escape(sample.WikidataCode)} ({Escape(sample.StatusQid)} on {Escape(sample.EntityId)}{labelPart})");
            }
            builder.AppendLine();
        }
    }

    private static string Escape(string value) => value
        .Replace("|", "\\|")
        .Replace("\r", " ")
        .Replace("\n", " ");

    private static string FormatPercent(long part, long total) {
        if (total <= 0) {
            return "0.0%";
        }

        var ratio = (double)part / total;
        return ratio.ToString("0.0%", CultureInfo.InvariantCulture);
    }
}