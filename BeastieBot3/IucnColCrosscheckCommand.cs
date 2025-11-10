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

public sealed class IucnColCrosscheckCommand : Command<IucnColCrosscheckCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--iucn-database <PATH>")]
        [Description("Explicit IUCN SQLite database path. Overrides paths.ini Datastore:IUCN_sqlite_from_cvs.")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--col-database <PATH>")]
        [Description("Explicit Catalogue of Life SQLite database path. Overrides paths.ini Datastore:COL_sqlite.")]
        public string? ColDatabase { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of IUCN rows to include after filters (0 = all).")]
        public long Limit { get; init; }

        [CommandOption("--output <FILE>")]
        [Description("Optional report output path. Defaults to data-analysis/iucn-col-crosscheck-<timestamp>.txt alongside the IUCN database.")]
        public string? OutputPath { get; init; }

        [CommandOption("--include-subpopulations")]
        [Description("Include IUCN assessments with subpopulation names.")]
        public bool IncludeSubpopulations { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        if (settings.Limit < 0) {
            AnsiConsole.MarkupLine("[red]--limit must be zero or greater.[/]");
            return -1;
        }

        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string iucnPath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLine($"[red]IUCN SQLite database not found at:[/] {Markup.Escape(iucnPath)}");
            return -3;
        }

        var colPath = !string.IsNullOrWhiteSpace(settings.ColDatabase)
            ? settings.ColDatabase.Trim()
            : paths.GetColSqlitePath();

        if (string.IsNullOrWhiteSpace(colPath)) {
            AnsiConsole.MarkupLine("[red]Catalogue of Life database path is not configured. Set Datastore:COL_sqlite or pass --col-database.[/]");
            return -4;
        }

        colPath = Path.GetFullPath(colPath);
        if (!File.Exists(colPath)) {
            AnsiConsole.MarkupLine($"[red]Catalogue of Life SQLite database not found at:[/] {Markup.Escape(colPath)}");
            return -5;
        }

        var iucnConnectionString = new SqliteConnectionStringBuilder {
            DataSource = iucnPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        var colConnectionString = new SqliteConnectionStringBuilder {
            DataSource = colPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var iucnConnection = new SqliteConnection(iucnConnectionString);
        using var colConnection = new SqliteConnection(colConnectionString);

        iucnConnection.Open();
        colConnection.Open();

        var iucnRepository = new IucnTaxonomyRepository(iucnConnection);
        if (!iucnRepository.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]view_assessments_html_taxonomy_html not found in IUCN database. Run the importer to rebuild the view.[/]");
            return -6;
        }

        using (var pragma = colConnection.CreateCommand()) {
            pragma.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'nameusage' LIMIT 1";
            if (pragma.ExecuteScalar() is null) {
                AnsiConsole.MarkupLine("[red]nameusage table not found in Catalogue of Life database.[/]");
                return -7;
            }
        }

        var colRepository = new ColTaxonRepository(colConnection);

        List<IucnTaxonomyRow> allRows;
        try {
            allRows = iucnRepository.ReadRows(0, cancellationToken)
                .Where(row => settings.IncludeSubpopulations || string.IsNullOrWhiteSpace(row.SubpopulationName))
                .ToList();
        } catch (OperationCanceledException) {
            throw;
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to read IUCN taxonomy data:[/] {Markup.Escape(ex.Message)}");
            return -8;
        }

        if (settings.Limit > 0 && allRows.Count > settings.Limit) {
            allRows = allRows.Take((int)Math.Min(settings.Limit, int.MaxValue)).ToList();
        }

        if (allRows.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No IUCN rows matched the filter criteria.[/]");
            return 0;
        }

        var orderedRows = allRows
            .OrderBy(r => SortKey(r.KingdomName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.PhylumName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.ClassName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.OrderName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.FamilyName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.GenusName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.SpeciesName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => SortKey(r.InfraName), StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.AssessmentId, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var reportPath = ResolveReportPath(settings.OutputPath, iucnPath);
        Directory.CreateDirectory(Path.GetDirectoryName(reportPath)!);

        using var stream = new FileStream(reportPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        using var writer = new StreamWriter(stream, new UTF8Encoding(false));

        WriteReportHeader(writer, iucnPath, colPath, orderedRows.Count, settings);

        var stats = new CrosscheckStats();
        var laddersBuffer = new List<TaxonLadder>();

        WriteProgress(orderedRows, colRepository, writer, stats, laddersBuffer, cancellationToken);

        writer.Flush();

        RenderSummary(stats, reportPath);
        return 0;
    }

    private static void WriteProgress(
        IReadOnlyList<IucnTaxonomyRow> rows,
        ColTaxonRepository colRepository,
        StreamWriter writer,
        CrosscheckStats stats,
        List<TaxonLadder> laddersBuffer,
        CancellationToken cancellationToken) {

        var columns = new ProgressColumn[] {
            new TaskDescriptionColumn(),
            new ProgressBarColumn(),
            new PercentageColumn(),
            new RemainingTimeColumn(),
            new SpinnerColumn()
        };

        AnsiConsole.Progress()
            .AutoClear(true)
            .HideCompleted(true)
            .Columns(columns)
            .Start(ctx => {
                var task = ctx.AddTask("Crosschecking IUCN assessments", maxValue: rows.Count);

                foreach (var row in rows) {
                    cancellationToken.ThrowIfCancellationRequested();

                    ProcessRow(row, colRepository, writer, stats, laddersBuffer, cancellationToken);

                    task.Increment(1);
                }
            });
    }

    private static void ProcessRow(
        IucnTaxonomyRow row,
        ColTaxonRepository colRepository,
        StreamWriter writer,
        CrosscheckStats stats,
        List<TaxonLadder> laddersBuffer,
        CancellationToken cancellationToken) {

        var match = FindBestMatch(row, colRepository, cancellationToken);

        stats.Total++;
        if (match.Primary is null) {
            stats.NotFound++;
        } else {
            stats.Matched++;
            if (match.Accepted is not null && !string.Equals(match.Primary.Id, match.Accepted.Id, StringComparison.Ordinal)) {
                stats.Synonyms++;
            }
        }

        var iucnAuthority = GetIucnAuthority(row);
        var colAuthority = match.Primary?.Authorship;
        var acceptedAuthority = match.Accepted?.Authorship;

        var authorityMatches = match.Primary is not null && AuthorityNormalizer.Equivalent(iucnAuthority, colAuthority);
        if (match.Primary is not null) {
            if (authorityMatches) {
                stats.AuthorityMatches++;
            } else {
                stats.AuthorityMismatches++;
            }
        }

        var alignment = BuildAlignment(row, match, laddersBuffer);
        WriteRow(writer, row, match, iucnAuthority, colAuthority, acceptedAuthority, authorityMatches, alignment);
    }

    private static TaxonLadderAlignmentResult BuildAlignment(IucnTaxonomyRow row, ColMatchResult match, List<TaxonLadder> buffer) {
        buffer.Clear();
        buffer.Add(TaxonLadderFactory.FromIucn(row));

        if (match.Primary is not null) {
            buffer.Add(TaxonLadderFactory.FromCol("COL-match", match.Primary));
        }

        if (match.Accepted is not null && (match.Primary is null || !string.Equals(match.Accepted.Id, match.Primary.Id, StringComparison.Ordinal))) {
            buffer.Add(TaxonLadderFactory.FromCol("COL-accepted", match.Accepted));
        }

        return TaxonLadderAlignment.Align(buffer.ToArray());
    }

    private static void WriteReportHeader(StreamWriter writer, string iucnPath, string colPath, int totalRows, Settings settings) {
        writer.WriteLine("IUCN vs Catalogue of Life Crosscheck Report");
        writer.WriteLine($"Generated: {DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}");
        writer.WriteLine($"IUCN database: {iucnPath}");
        writer.WriteLine($"Catalogue of Life database: {colPath}");
        writer.WriteLine($"Include subpopulations: {settings.IncludeSubpopulations}");
        if (settings.Limit > 0) {
            writer.WriteLine($"Limit: {settings.Limit:N0}");
        }
        writer.WriteLine($"Rows processed: {totalRows:N0}");
        writer.WriteLine(new string('=', 80));
        writer.WriteLine();
    }

    private static void WriteRow(
        StreamWriter writer,
        IucnTaxonomyRow row,
        ColMatchResult match,
        string? iucnAuthority,
        string? colAuthority,
        string? acceptedAuthority,
        bool authorityMatches,
        TaxonLadderAlignmentResult alignment) {

        writer.WriteLine($"Assessment: {row.AssessmentId} | InternalTaxonId: {row.InternalTaxonId} | Version: {row.RedlistVersion}");
        writer.WriteLine($"IUCN scientificName: {SafeValue(row.ScientificNameAssessments) }");
        writer.WriteLine($"IUCN scientificName:1: {SafeValue(row.ScientificNameTaxonomy) }");
        writer.WriteLine($"IUCN authority: {SafeValue(iucnAuthority)}");

        if (!string.IsNullOrWhiteSpace(row.InfraType) || !string.IsNullOrWhiteSpace(row.InfraName)) {
            writer.WriteLine($"IUCN infra: {SafeValue(row.InfraType)} {SafeValue(row.InfraName)}");
        }

        if (!string.IsNullOrWhiteSpace(row.SubpopulationName)) {
            writer.WriteLine($"IUCN subpopulation: {row.SubpopulationName.Trim()}");
        }

        writer.WriteLine($"IUCN classification: kingdom={SafeValue(row.KingdomName)}, phylum={SafeValue(row.PhylumName)}, class={SafeValue(row.ClassName)}, order={SafeValue(row.OrderName)}, family={SafeValue(row.FamilyName)}, genus={SafeValue(row.GenusName)}, species={SafeValue(row.SpeciesName)}");

        if (match.Primary is null) {
            writer.WriteLine("COL match: (not found)");
        } else {
            writer.WriteLine($"COL match: id={match.Primary.Id}, status={SafeValue(match.Primary.Status)}, rank={SafeValue(match.Primary.Rank)}, via={match.MatchMethod}");
            writer.WriteLine($"COL name: {match.Primary.ScientificName}");
            writer.WriteLine($"COL authority: {SafeValue(colAuthority)}");
        }

        if (match.Accepted is not null && (match.Primary is null || !string.Equals(match.Accepted.Id, match.Primary.Id, StringComparison.Ordinal))) {
            writer.WriteLine($"COL accepted: id={match.Accepted.Id}, name={match.Accepted.ScientificName}, authority={SafeValue(acceptedAuthority)}");
        }

        if (match.Primary is not null) {
            var normalizedIucn = AuthorityNormalizer.Normalize(iucnAuthority);
            var normalizedCol = AuthorityNormalizer.Normalize(colAuthority);
            writer.WriteLine($"Authority comparison: {(authorityMatches ? "match" : "mismatch")} | IUCN='{normalizedIucn}' | COL='{normalizedCol}'");
        }

        if (match.Candidates.Count > 1) {
            writer.WriteLine($"COL candidates ({match.Candidates.Count}): {string.Join(", ", match.Candidates.Select(c => c.Id))}");
        }

        WriteAlignment(writer, alignment, match.LadderSources);

        writer.WriteLine(new string('-', 80));
        writer.WriteLine();
    }

    private static void WriteAlignment(StreamWriter writer, TaxonLadderAlignmentResult alignment, IReadOnlyList<string> ladders) {
        if (alignment.Rows.Count == 0) {
            writer.WriteLine("Ladder alignment: (no data)");
            return;
        }

        var headers = ladders.Count > 0 ? ladders : new[] { "IUCN" };
        var widths = new int[headers.Count + 1];
        widths[0] = Math.Max("Rank".Length, alignment.Rows.Max(row => row.Rank.Length));

        for (var i = 0; i < headers.Count; i++) {
            var source = headers[i];
            var max = alignment.Rows.Select(row => row.Values.TryGetValue(source, out var val) ? val.Length : 1).DefaultIfEmpty(1).Max();
            widths[i + 1] = Math.Max(headers[i].Length, Math.Min(max, 80));
        }

        static string FormatCell(string value, int width) {
            if (value.Length <= width) {
                return value.PadRight(width);
            }

            if (width <= 3) {
                return new string('.', width);
            }

            var sliceLength = Math.Min(width - 3, value.Length);
            var slice = value.Substring(0, sliceLength);
            return (slice + "...").PadRight(width);
        }

        var headerLine = new StringBuilder();
        headerLine.Append(FormatCell("Rank", widths[0]));
        for (var i = 0; i < headers.Count; i++) {
            headerLine.Append(" | ");
            headerLine.Append(FormatCell(headers[i], widths[i + 1]));
        }
        writer.WriteLine(headerLine.ToString());

        foreach (var row in alignment.Rows) {
            var line = new StringBuilder();
            line.Append(FormatCell(row.Rank, widths[0]));
            for (var i = 0; i < headers.Count; i++) {
                line.Append(" | ");
                var source = headers[i];
                var value = row.Values.TryGetValue(source, out var val) ? val : "-";
                line.Append(FormatCell(value, widths[i + 1]));
            }
            writer.WriteLine(line.ToString());
        }
    }

    private static ColMatchResult FindBestMatch(IucnTaxonomyRow row, ColTaxonRepository repository, CancellationToken cancellationToken) {
        var candidates = new List<ColTaxonRecord>();
        var matchMethod = new List<string>();

        var primaryName = row.ScientificNameTaxonomy;
        if (!string.IsNullOrWhiteSpace(primaryName)) {
            var nameMatches = repository.FindByScientificName(primaryName, cancellationToken);
            if (nameMatches.Count > 0) {
                candidates.AddRange(nameMatches);
                matchMethod.Add("scientificName:1");
            }
        }

        var fallbackName = row.ScientificNameAssessments;
        if (!string.IsNullOrWhiteSpace(fallbackName) && candidates.Count == 0) {
            var fallbackMatches = repository.FindByScientificName(fallbackName, cancellationToken);
            if (fallbackMatches.Count > 0) {
                candidates.AddRange(fallbackMatches);
                matchMethod.Add("scientificName");
            }
        }

        var genus = row.GenusName;
        var species = row.SpeciesName;
        var infra = row.InfraName;
        if (candidates.Count == 0 && !string.IsNullOrWhiteSpace(genus) && !string.IsNullOrWhiteSpace(species)) {
            var componentMatches = repository.FindByComponents(genus, species, infra, cancellationToken);
            if (componentMatches.Count > 0) {
                candidates.AddRange(componentMatches);
                matchMethod.Add("components");
            }
        }

        var uniqueCandidates = candidates
            .GroupBy(c => c.Id, StringComparer.Ordinal)
            .Select(g => g.First())
            .ToList();

        var primary = ChoosePrimary(uniqueCandidates, !string.IsNullOrWhiteSpace(row.InfraName));
        ColTaxonRecord? accepted = null;

        if (primary is not null && !string.IsNullOrWhiteSpace(primary.AcceptedNameUsageId)) {
            accepted = repository.GetById(primary.AcceptedNameUsageId, cancellationToken);
        }

        var ladderSources = new List<string> { "IUCN" };
        if (primary is not null) {
            ladderSources.Add("COL-match");
        }
        if (accepted is not null && (primary is null || !string.Equals(primary.Id, accepted.Id, StringComparison.Ordinal))) {
            ladderSources.Add("COL-accepted");
        }

        var distinctMethods = matchMethod.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        return new ColMatchResult(primary, accepted, uniqueCandidates, distinctMethods.Count == 0 ? "none" : string.Join(",", distinctMethods), ladderSources);
    }

    private static ColTaxonRecord? ChoosePrimary(IReadOnlyList<ColTaxonRecord> candidates, bool expectInfra) {
        if (candidates.Count == 0) {
            return null;
        }

        var acceptedMatches = candidates
            .Where(c => LooksAccepted(c.Status))
            .ToList();

        if (acceptedMatches.Count > 0) {
            var preferredAccepted = PickByInfraExpectation(acceptedMatches, expectInfra);
            if (preferredAccepted is not null) {
                return preferredAccepted;
            }
        }

        var synonymMatches = candidates
            .Where(c => LooksSynonym(c.Status))
            .ToList();

        if (synonymMatches.Count > 0) {
            var preferredSyn = PickByInfraExpectation(synonymMatches, expectInfra);
            if (preferredSyn is not null) {
                return preferredSyn;
            }
        }

        return PickByInfraExpectation(candidates, expectInfra) ?? candidates[0];
    }

    private static ColTaxonRecord? PickByInfraExpectation(IEnumerable<ColTaxonRecord> candidates, bool expectInfra) {
        var list = candidates.ToList();
        if (list.Count == 0) {
            return null;
        }

        if (!expectInfra) {
            var speciesLevel = list.FirstOrDefault(c => !LooksInfraRank(c.Rank));
            if (speciesLevel is not null) {
                return speciesLevel;
            }
        } else {
            var infraLevel = list.FirstOrDefault(c => LooksInfraRank(c.Rank));
            if (infraLevel is not null) {
                return infraLevel;
            }
        }

        return list[0];
    }

    private static bool LooksAccepted(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return false;
        }

        var normalized = status.Trim().ToLowerInvariant();
        return normalized.Contains("accepted", StringComparison.Ordinal);
    }

    private static bool LooksSynonym(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return false;
        }

        var normalized = status.Trim().ToLowerInvariant();
        return normalized.Contains("synonym", StringComparison.Ordinal);
    }

    private static bool LooksInfraRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return false;
        }

        var normalized = rank.Trim().ToLowerInvariant();
        if (normalized.Contains("subspecies", StringComparison.Ordinal) || normalized.Contains("variety", StringComparison.Ordinal)) {
            return true;
        }

        return normalized.Contains("form", StringComparison.Ordinal);
    }

    private static string? GetIucnAuthority(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.InfraName) && !string.IsNullOrWhiteSpace(row.InfraAuthority)) {
            return row.InfraAuthority!.Trim();
        }

        return row.Authority?.Trim();
    }

    private static string SafeValue(string? value) {
        return string.IsNullOrWhiteSpace(value) ? "(none)" : value.Trim();
    }

    private static string SortKey(string? value) {
        return string.IsNullOrWhiteSpace(value) ? "~" : value.Trim();
    }

    private static string ResolveReportPath(string? outputPath, string iucnPath) {
        if (!string.IsNullOrWhiteSpace(outputPath)) {
            var full = Path.GetFullPath(outputPath);
            var directory = Path.GetDirectoryName(full);
            if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory)) {
                Directory.CreateDirectory(directory);
            }
            return full;
        }

        var baseDir = Path.GetDirectoryName(iucnPath) ?? Directory.GetCurrentDirectory();
        var reportDir = Path.Combine(baseDir, "data-analysis");
        Directory.CreateDirectory(reportDir);
        var fileName = $"iucn-col-crosscheck-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.txt";
        return Path.Combine(reportDir, fileName);
    }

    private static void RenderSummary(CrosscheckStats stats, string reportPath) {
        AnsiConsole.MarkupLine("[bold]IUCN vs COL Crosscheck[/]");
        AnsiConsole.MarkupLine($"- rows processed: {stats.Total:N0}");
        AnsiConsole.MarkupLine($"- matches: {stats.Matched:N0}");
        AnsiConsole.MarkupLine($"- not found: {stats.NotFound:N0}");
        AnsiConsole.MarkupLine($"- synonyms: {stats.Synonyms:N0}");
        AnsiConsole.MarkupLine($"- authority matches: {stats.AuthorityMatches:N0}");
        AnsiConsole.MarkupLine($"- authority mismatches: {stats.AuthorityMismatches:N0}");
        AnsiConsole.MarkupLine($"[green]Report written to:[/] {Markup.Escape(reportPath)}");
    }

    private sealed record ColMatchResult(
        ColTaxonRecord? Primary,
        ColTaxonRecord? Accepted,
        IReadOnlyList<ColTaxonRecord> Candidates,
        string MatchMethod,
        IReadOnlyList<string> LadderSources
    );

    private sealed class CrosscheckStats {
        public long Total { get; set; }
        public long Matched { get; set; }
        public long NotFound { get; set; }
        public long Synonyms { get; set; }
        public long AuthorityMatches { get; set; }
        public long AuthorityMismatches { get; set; }
    }
}
