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

public sealed class IucnTaxonomyConsistencyCommand : Command<IucnTaxonomyConsistencyCommand.Settings> {
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
        [Description("Maximum number of mismatch samples to display per category.")]
        public int MaxSamples { get; init; } = 5;
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

        var result = IucnScientificNameVerifier.Analyze(rows, settings.MaxSamples);
        RenderResults(result, databasePath, settings.Limit);

        var hasFailures = result.FieldMismatchCount > 0
            || result.ReconstructionMismatchCount > 0
            || result.GenusMismatchCount > 0
            || result.SpeciesMismatchCount > 0
            || result.InfraNameMismatchCount > 0
            || result.SubpopulationMismatchCount > 0;

        return hasFailures ? -6 : 0;
    }

    private static void RenderResults(ScientificNameVerificationResult result, string databasePath, long limit) {
        AnsiConsole.MarkupLine($"[bold]IUCN Taxonomy Scientific Name Check[/] ({Markup.Escape(Path.GetFileName(databasePath))})");
        if (limit > 0) {
            AnsiConsole.MarkupLine($"[grey]Row limit:[/] {limit:N0}");
        }
        AnsiConsole.MarkupLine($"[grey]Rows processed:[/] {result.TotalRows:N0}");
        AnsiConsole.MarkupLine(string.Empty);

        AnsiConsole.MarkupLine("[bold]Name categories[/]");
        AnsiConsole.MarkupLine($"- species-or-higher: {result.SpeciesOrHigherCount:N0}");
        AnsiConsole.MarkupLine($"- infraspecific: {result.InfraspecificCount:N0}");
        AnsiConsole.MarkupLine($"- subpopulation: {result.SubpopulationCount:N0}");

        if (result.InfraTypeCounts.Count > 0) {
            var ordered = result.InfraTypeCounts.OrderByDescending(kvp => kvp.Value)
                                               .ThenBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase);
            AnsiConsole.MarkupLine("[bold]Infra types[/]");
            foreach (var (key, value) in ordered) {
                AnsiConsole.MarkupLine($"- {Markup.Escape(key)}: {value:N0}");
            }
        }

        AnsiConsole.MarkupLine("[bold]Field agreement[/]");
        AnsiConsole.MarkupLine($"- scientificName vs scientificName:1 mismatches: {FormatMismatch(result.FieldMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine($"- reconstruction mismatches: {FormatMismatch(result.ReconstructionMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine($"- genus mismatches: {FormatMismatch(result.GenusMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine($"- species mismatches: {FormatMismatch(result.SpeciesMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine($"- infra name mismatches: {FormatMismatch(result.InfraNameMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine($"- subpopulation mismatches: {FormatMismatch(result.SubpopulationMismatchCount, result.TotalRows)}");
        AnsiConsole.MarkupLine(string.Empty);

        if (result.NullScientificNameCount > 0 || result.NullTaxonomyScientificNameCount > 0) {
            AnsiConsole.MarkupLine("[bold]Null counts[/]");
            if (result.NullScientificNameCount > 0) {
                AnsiConsole.MarkupLine($"- scientificName null: {result.NullScientificNameCount:N0}");
            }
            if (result.NullTaxonomyScientificNameCount > 0) {
                AnsiConsole.MarkupLine($"- scientificName:1 null: {result.NullTaxonomyScientificNameCount:N0}");
            }
            AnsiConsole.MarkupLine(string.Empty);
        }

        PrintSamples("Field mismatches", result.GetSamples(ScientificNameMismatchKind.FieldDisagreement));
        PrintSamples("Reconstruction mismatches", result.GetSamples(ScientificNameMismatchKind.ReconstructionFailure));
        PrintSamples("Genus mismatches", result.GetSamples(ScientificNameMismatchKind.GenusMismatch));
        PrintSamples("Species mismatches", result.GetSamples(ScientificNameMismatchKind.SpeciesMismatch));
        PrintSamples("Infra name mismatches", result.GetSamples(ScientificNameMismatchKind.InfraNameMismatch));
        PrintSamples("Subpopulation mismatches", result.GetSamples(ScientificNameMismatchKind.SubpopulationMismatch));
    }

    private static void PrintSamples(string label, IReadOnlyList<ScientificNameMismatchSample> samples) {
        if (samples.Count == 0) {
            return;
        }

        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(label)}[/]");
        foreach (var sample in samples) {
            var builder = new StringBuilder();
            AppendSample(builder, "assessmentId", sample.AssessmentId);
            AppendSample(builder, "internalTaxonId", sample.InternalTaxonId);
            AppendSample(builder, "classification", sample.Classification);
            AppendSample(builder, "kingdom", sample.KingdomName);
            if (!string.IsNullOrEmpty(sample.InfraType)) {
                AppendSample(builder, "infraType", sample.InfraType);
            }
            if (!string.IsNullOrEmpty(sample.SubpopulationName)) {
                AppendSample(builder, "subpopulation", sample.SubpopulationName);
            }
            AppendSample(builder, "scientificName", sample.ScientificNameAssessments);
            AppendSample(builder, "scientificName:1", sample.ScientificNameTaxonomy);
            AppendSample(builder, "reconstructed", sample.ReconstructedName);
            AppendSample(builder, "detail", sample.Detail);
            if (sample.NormalizedAssess is not null || sample.NormalizedTaxonomy is not null || sample.NormalizedReconstructed is not null) {
                AppendSample(builder, "normalized", BuildNormalizedInfo(sample));
            }
            builder.AppendLine();
            AnsiConsole.Markup(builder.ToString());
        }
    }

    private static void AppendSample(StringBuilder builder, string label, string? value) {
        builder.Append("[grey]").Append(Markup.Escape(label)).AppendLine(":[/]");
        if (value is null) {
            builder.AppendLine("  (null)");
            return;
        }
        if (value.Length == 0) {
            builder.AppendLine("  (empty)");
            return;
        }

        foreach (var line in value.Split('\n')) {
            builder.Append("  ").AppendLine(line.Length == 0 ? "(empty)" : Markup.Escape(line));
        }
    }

    private static string BuildNormalizedInfo(ScientificNameMismatchSample sample) {
        var parts = new List<string>();
        if (sample.NormalizedAssess is not null) {
            parts.Add("assess=" + sample.NormalizedAssess);
        }
        if (sample.NormalizedTaxonomy is not null) {
            parts.Add("tax=" + sample.NormalizedTaxonomy);
        }
        if (sample.NormalizedReconstructed is not null) {
            parts.Add("recon=" + sample.NormalizedReconstructed);
        }
        return string.Join(" | ", parts.Select(Markup.Escape));
    }

    private static string FormatMismatch(long mismatches, long total) {
        if (total == 0) {
            return "-";
        }
        if (mismatches == 0) {
            return $"0 (0.000% of {total:N0})";
        }

        var percentage = (double)mismatches / total * 100d;
        return $"{mismatches:N0} ({percentage:F3}% of {total:N0})";
    }
}