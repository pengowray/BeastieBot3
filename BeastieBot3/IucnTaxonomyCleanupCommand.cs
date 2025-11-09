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

public sealed class IucnTaxonomyCleanupCommand : Command<IucnTaxonomyCleanupCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Explicit SQLite database path. Overrides paths.ini Datastore:IUCN_sqlite_from_cvs.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of rows to inspect (0 = all)." )]
        public long Limit { get; init; }

        [CommandOption("--max-samples <COUNT>")]
        [Description("Maximum number of issue samples to display per category.")]
        public int MaxSamples { get; init; } = 10;
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        if (settings.MaxSamples <= 0) {
            AnsiConsole.MarkupLine("[red]--max-samples must be greater than zero.[/]");
            return -1;
        }

        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string databasePath;
        try {
            databasePath = paths.ResolveIucnDatabasePath(settings.DatabasePath);
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(databasePath)) {
            AnsiConsole.MarkupLine($"[red]IUCN SQLite database not found at:[/] {Markup.Escape(databasePath)}");
            return -3;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        var repository = new IucnTaxonomyRepository(connection);
        if (!repository.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]view_assessments_html_taxonomy_html not found.[/] Re-run the importer to create the joined view.");
            return -4;
        }

        IEnumerable<IucnTaxonomyRow> rows;
        try {
            rows = repository.ReadRows(settings.Limit, cancellationToken).ToList();
        } catch (OperationCanceledException) {
            throw;
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to read taxonomy data:[/] {Markup.Escape(ex.Message)}");
            return -5;
        }

        var analysis = IucnDataCleanupAnalyzer.Analyze(rows, settings.MaxSamples);
        RenderResults(analysis, databasePath, settings.Limit);

        return 0;
    }

    private static void RenderResults(DataCleanupAnalysisResult analysis, string databasePath, long limit) {
        AnsiConsole.MarkupLine($"[bold]IUCN Taxonomy Cleanup Opportunities[/] ({Markup.Escape(Path.GetFileName(databasePath))})");
        if (limit > 0) {
            AnsiConsole.MarkupLine($"[grey]Row limit:[/] {limit:N0}");
        }
        AnsiConsole.MarkupLine($"[grey]Rows processed:[/] {analysis.TotalRows:N0}");
        AnsiConsole.MarkupLine(string.Empty);

        var totalIssues = analysis.IssueCounts.Values.Sum();
        if (totalIssues == 0) {
            AnsiConsole.MarkupLine("No cleanup suggestions detected.\n");
            return;
        }

        AnsiConsole.MarkupLine("[bold]Issue summary[/]");
        foreach (var (kind, count) in analysis.IssueCounts.Where(kvp => kvp.Value > 0).OrderByDescending(kvp => kvp.Value)) {
            var label = GetIssueLabel(kind);
            AnsiConsole.MarkupLine($"- {label}: {count:N0}");
        }
        AnsiConsole.MarkupLine(string.Empty);

        foreach (var kind in Enum.GetValues<DataCleanupIssueKind>()) {
            var samples = analysis.GetSamples(kind);
            if (samples.Count == 0) {
                continue;
            }

            var label = GetIssueLabel(kind);
            PrintSamples(label, samples);
        }
    }

    private static void PrintSamples(string label, IReadOnlyList<DataCleanupIssueSample> samples) {
        if (samples.Count == 0) {
            return;
        }

        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(label)}[/]");
        foreach (var sample in samples) {
            var builder = new StringBuilder();
            AppendLine(builder, "assessmentId", sample.AssessmentId);
            AppendLine(builder, "internalTaxonId", sample.InternalTaxonId);
            AppendLine(builder, "redlist_version", sample.RedlistVersion);
            AppendLine(builder, "detail", sample.Detail);

            foreach (var field in sample.Fields) {
                builder.Append("[grey]").Append(Markup.Escape(field.FieldName)).AppendLine("[/]");
                AppendValue(builder, "current", field.CurrentValue);
                AppendValue(builder, "suggested", field.SuggestedValue);
            }

            builder.AppendLine();
            AnsiConsole.Markup(builder.ToString());
        }
    }

    private static void AppendLine(StringBuilder builder, string label, string value) {
        builder.Append("[grey]").Append(Markup.Escape(label)).AppendLine("[/]");
        AppendValue(builder, null, value);
    }

    private static void AppendValue(StringBuilder builder, string? label, string? value) {
        if (label is not null) {
            builder.Append("  [grey]").Append(Markup.Escape(label)).AppendLine("[/]");
        }

        if (value is null) {
            builder.AppendLine("    (null)");
            return;
        }

        if (value.Length == 0) {
            builder.AppendLine("    (empty)");
            return;
        }

        foreach (var line in value.Split('\n')) {
            builder.Append("    ").AppendLine(Markup.Escape(line.Length == 0 ? "(empty)" : line));
        }
    }

    private static string GetIssueLabel(DataCleanupIssueKind kind) => kind switch {
        DataCleanupIssueKind.ScientificNameWhitespace => "scientificName whitespace anomalies",
        DataCleanupIssueKind.TaxonomyScientificNameWhitespace => "scientificName:1 whitespace anomalies",
        DataCleanupIssueKind.ScientificNameDisagreement => "scientificName vs scientificName:1 disagreement",
        DataCleanupIssueKind.InfraNameWhitespace => "infraName whitespace anomalies",
        DataCleanupIssueKind.InfraNameMarkerPrefix => "infraName marker prefixes",
        DataCleanupIssueKind.SubpopulationWhitespace => "subpopulationName whitespace anomalies",
        _ => kind.ToString()
    };
}