using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Audit.Commentary;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Producers;
using BeastieBot3.Audit.Rendering;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;

// Builds the unofficial "IUCN Red List data observations" static site: runs every audit report
// producer in-process against the locally-imported release and writes a self-contained HTML + CSV
// bundle. Read-only; re-runnable per release.

namespace BeastieBot3.Audit;

[CommandInfo("redlist audit-site", CommandKind.ReadOnly,
    "Build the unofficial IUCN Red List data-observations static site (HTML plus CSV) from the locally-imported release.",
    Rerun = RerunEffect.Rebuilds,
    Examples = new[] {
        "redlist audit-site",
        "redlist audit-site --limit 5000",
        "redlist audit-site --output D:/datasets/beastiebot/reports/redlist-audit-2026",
    })]
internal sealed class RedlistAuditSiteCommand : Command<RedlistAuditSiteCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-o|--output <DIR>")]
        [Description("Output directory for the static bundle. Defaults to <Datastore:reports_dir>/redlist-audit-2026.")]
        public string? OutputDir { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Cap rows scanned per report (for fast test runs). 0 or omitted scans everything.")]
        public long Limit { get; init; }

        [CommandOption("--contact <EMAIL>")]
        [Description("Contact email shown in the footer.")]
        public string? Contact { get; init; }
    }

    // Display order: IUCN-owned first (most actionable first), methodology after.
    private static IReadOnlyList<IAuditReportProducer> Producers() => new IAuditReportProducer[] {
        new FailedAssessmentsProducer(),
        new TaxonomyCleanupProducer(),
        new SynonymWhitespaceProducer(),
        new SynonymOtherFormattingProducer(),
        new OrphanInfraranksProducer(),
        new NoLatestAssessmentProducer(),
        new HtmlConsistencyProducer(),
        new TaxonomyConsistencyProducer(),
        new ColCrosscheckProducer(),
        new FieldHygieneProducer(),
        new NameChangesProducer(),
    };

    public override int Execute(CommandContext context, Settings settings, CancellationToken ct) {
        _ = context;
        var paths = settings.CreatePaths();
        var (release, releaseYear) = ResolveRelease(paths);
        var limit = settings.Limit > 0 ? settings.Limit : (long?)null;

        var commentary = LoadCommentary(paths);
        AnsiConsole.MarkupLineInterpolated($"[grey]Release:[/] {release}    [grey]commentary:[/] {commentary.SourcePath ?? "(none)"}");

        var outputDir = ResolveOutputDir(paths, settings.OutputDir);

        var reports = new List<AuditReport>();
        using (var ctx = new AuditContext(paths, limit is null ? null : (int?)Math.Min(int.MaxValue, limit.Value), release, releaseYear, commentary, ct)) {
            foreach (var producer in Producers()) {
                ct.ThrowIfCancellationRequested();
                try {
                    var report = producer.Produce(ctx);
                    if (report is null) {
                        AnsiConsole.MarkupLineInterpolated($"[yellow]skipped[/] {producer.Id} (data source unavailable)");
                        continue;
                    }
                    reports.Add(report);
                    AnsiConsole.MarkupLineInterpolated($"[green]built[/] {producer.Id}: {report.Count:N0}");
                } catch (OperationCanceledException) {
                    throw;
                } catch (Exception ex) {
                    AnsiConsole.MarkupLineInterpolated($"[red]error[/] {producer.Id}: {Markup.Escape(ex.Message)}");
                }
            }
        }

        if (reports.Count == 0) {
            AnsiConsole.MarkupLine("[red]No reports produced. Check that the IUCN databases are imported and configured.[/]");
            return -1;
        }

        var config = new AuditSiteConfig {
            Contact = string.IsNullOrWhiteSpace(settings.Contact) ? "pengowray@gmail.com" : settings.Contact!,
        };
        var document = new AuditDocument {
            Release = release,
            ReleaseYear = releaseYear,
            GeneratedAt = DateTimeOffset.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DataSources = BuildDataSources(paths, release),
            Reports = reports,
            Config = config,
            CommentarySource = commentary,
        };

        AuditSiteRenderer.Write(document, outputDir, line => AnsiConsole.MarkupLineInterpolated($"[grey]{Markup.Escape(line)}[/]"));

        AnsiConsole.MarkupLineInterpolated($"[green]Audit site written to:[/] {outputDir}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Open:[/] {Path.Combine(outputDir, "index.html")}");
        return 0;
    }

    // Explicit --output wins. Otherwise default to a "redlist-audit-2026" subdirectory of the
    // configured reports directory (Datastore:reports_dir), falling back to ./reports only when no
    // reports directory is configured.
    private static string ResolveOutputDir(PathsService paths, string? explicitDir) {
        if (!string.IsNullOrWhiteSpace(explicitDir)) {
            return Path.GetFullPath(explicitDir);
        }
        var reportsDir = paths.GetReportOutputDirectory();
        var baseDir = !string.IsNullOrWhiteSpace(reportsDir)
            ? reportsDir
            : Path.Combine(Environment.CurrentDirectory, "reports");
        return Path.Combine(Path.GetFullPath(baseDir), "redlist-audit-2026");
    }

    private static (string Release, int? Year) ResolveRelease(PathsService paths) {
        // Prefer import_metadata.redlist_version; fall back to the db filename or the CSV dir.
        try {
            var dbPath = paths.ResolveIucnDatabasePath(null);
            if (File.Exists(dbPath)) {
                using var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = dbPath, Mode = SqliteOpenMode.ReadOnly }.ConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT DISTINCT redlist_version FROM import_metadata LIMIT 1";
                if (cmd.ExecuteScalar() is string v && !string.IsNullOrWhiteSpace(v)) {
                    return (v.Trim(), ParseYear(v));
                }
            }
        } catch { /* fall through */ }

        var fromDir = paths.GetIucnCvsDir();
        var guess = ExtractReleaseToken(fromDir) ?? ExtractReleaseToken(paths.GetIucnDatabasePath()) ?? "unknown";
        return (guess, ParseYear(guess));
    }

    private static string? ExtractReleaseToken(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var match = System.Text.RegularExpressions.Regex.Match(text, @"(\d{4})-(\d)");
        return match.Success ? $"{match.Groups[1].Value}-{match.Groups[2].Value}" : null;
    }

    private static int? ParseYear(string? release) {
        if (string.IsNullOrWhiteSpace(release)) {
            return null;
        }
        var match = System.Text.RegularExpressions.Regex.Match(release, @"(\d{4})");
        return match.Success && int.TryParse(match.Groups[1].Value, out var y) ? y : null;
    }

    private static AuditCommentary LoadCommentary(PathsService paths) {
        try {
            var rules = RulesPaths.Resolve(paths);
            var commentary = AuditCommentary.Load(rules.SourceRulesDir);
            if (commentary.SourcePath is not null) {
                return commentary;
            }
            return AuditCommentary.Load(rules.BuildOutputRulesDir);
        } catch {
            return AuditCommentary.Empty;
        }
    }

    private static IReadOnlyList<AuditDataSource> BuildDataSources(PathsService paths, string release) {
        var sources = new List<AuditDataSource> {
            new("Catalogue of Life reference", "Used as a taxonomic comparison in the crosscheck report"),
        };
        return sources;
    }
}
