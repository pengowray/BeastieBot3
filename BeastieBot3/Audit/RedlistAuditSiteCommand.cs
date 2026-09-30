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
using BeastieBot3.Audit.Producers.ColCrosscheck;
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
        [Description("Output directory for the static bundle. Defaults to <Datastore:reports_dir>/redlist-audit-2026, or <Datastore:reports_dir>/redlist-audit-2026-limited for a run with --limit.")]
        public string? OutputDir { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of database rows most reports check, for a quick test run (0 = all rows, the default). Without --output, a run with a limit is written to <Datastore:reports_dir>/redlist-audit-2026-limited, so the full run's pages are kept. Every page of a limited run has a notice that its counts are partial, and no release-counts.yml is written.")]
        public long Limit { get; init; }

        [CommandOption("--contact <EMAIL>")]
        [Description("Contact email shown in the footer.")]
        public string? Contact { get; init; }
    }

    // Page order on the index is the order below within each index section. Most producers emit one
    // report; ColCrosscheckProducer emits several from one pass, so everything is wrapped to the
    // set-producer contract and iterated uniformly. The CoL set is ordered by FamilyRank wherever it
    // is listed.
    private static IReadOnlyList<IAuditReportSetProducer> Producers() => new IAuditReportSetProducer[] {
        new SingleReportProducer(new EmptyScopeProducer()),
        new SingleReportProducer(new FailedAssessmentsProducer()),
        new SingleReportProducer(new TaxonomyCleanupProducer()),
        new SingleReportProducer(new SynonymWhitespaceProducer()),
        new SingleReportProducer(new SynonymOtherFormattingProducer()),
        new SingleReportProducer(new SynonymNameNotesProducer()),
        new SingleReportProducer(new CommonNameIssuesProducer()),
        new SingleReportProducer(new OrphanInfraranksProducer()),
        new SingleReportProducer(new NoLatestAssessmentProducer()),
        new SingleReportProducer(new ProvisionalNamesProducer()),
        new SingleReportProducer(new HtmlConsistencyProducer()),
        new SingleReportProducer(new TaxonomyConsistencyProducer()),
        new ColCrosscheckProducer(),
        new SingleReportProducer(new NameChangesProducer()),
        new SingleReportProducer(new FieldHygieneProducer()),
    };

    public override int Execute(CommandContext context, Settings settings, CancellationToken ct) {
        _ = context;
        var paths = settings.CreatePaths();
        var (release, releaseYear) = ResolveRelease(paths);
        var limit = settings.Limit > 0 ? settings.Limit : (long?)null;

        var commentary = LoadCommentary(paths);
        var releaseCounts = LoadReleaseCounts(paths);
        AnsiConsole.MarkupLineInterpolated($"[grey]Release:[/] {release}    [grey]commentary:[/] {commentary.SourcePath ?? "(none)"}    [grey]release counts:[/] {releaseCounts.SourcePath ?? "(none)"}");

        var reportsDir = paths.GetReportOutputDirectory();
        var outputDir = ResolveOutputDir(reportsDir, settings.OutputDir, limited: limit is not null, Environment.CurrentDirectory);

        var reports = new List<AuditReport>();
        string? colRelease;
        using (var ctx = new AuditContext(paths, limit is null ? null : (int?)Math.Min(int.MaxValue, limit.Value), release, releaseYear, commentary, ct)) {
            colRelease = ctx.ColReleaseLabel();
            foreach (var producer in Producers()) {
                ct.ThrowIfCancellationRequested();
                try {
                    var produced = producer.Produce(ctx);
                    if (produced.Count == 0) {
                        AnsiConsole.MarkupLineInterpolated($"[yellow]skipped[/] {producer.Id} (data source unavailable, or nothing to report)");
                        continue;
                    }
                    foreach (var report in produced) {
                        reports.Add(report);
                        AnsiConsole.MarkupLineInterpolated($"[green]built[/] {report.Id}: {report.Count:N0}");
                    }
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

        var unknownClasses = TaxonGroups.UnknownClasses(reports.SelectMany(r => r.CsvRows));
        if (unknownClasses.Count > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Taxonomic classes with no group label:[/] {Markup.Escape(string.Join(", ", unknownClasses))}");
            AnsiConsole.MarkupLine("[grey]Their rows are labelled by kingdom alone. Add them to TaxonGroups.ByClass.[/]");
        }

        var config = new AuditSiteConfig {
            Contact = string.IsNullOrWhiteSpace(settings.Contact) ? "feedback@pengowray.com" : settings.Contact!,
        };
        var document = new AuditDocument {
            Release = release,
            ReleaseYear = releaseYear,
            GeneratedAt = DateTimeOffset.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DataSources = BuildDataSources(colRelease),
            Reports = reports,
            Config = config,
            CommentarySource = commentary,
            PreviousRelease = releaseCounts.PreviousRelease(release),
            ReleaseCounts = releaseCounts,
            RowLimit = limit,
        };

        AuditSiteRenderer.Write(document, outputDir, line => AnsiConsole.MarkupLineInterpolated($"[grey]{Markup.Escape(line)}[/]"));

        AnsiConsole.MarkupLineInterpolated($"[green]Audit site written to:[/] {outputDir}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Open:[/] {Path.Combine(outputDir, "index.html")}");

        if (limit is not null) {
            // rules/audit/release-counts.yml records each release's counts; a --limit run's counts
            // are partial, so the renderer saves no block and none is printed here.
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Limited run (--limit {limit.Value}):[/] counts are partial, and every page has a notice saying so. No release-counts.yml was saved.");
            var fullDir = ResolveOutputDir(reportsDir, null, limited: false, Environment.CurrentDirectory);
            if (string.IsNullOrWhiteSpace(settings.OutputDir) && File.Exists(Path.Combine(fullDir, "index.html"))) {
                AnsiConsole.MarkupLineInterpolated($"[grey]The full site in {fullDir} was not changed.[/]");
            }
            return 0;
        }

        var countsFile = Path.Combine(outputDir, AuditSiteRenderer.ReleaseCountsFileName);
        AnsiConsole.MarkupLineInterpolated($"[grey]Row counts for release-counts.yml (also saved as {countsFile}):[/]");
        AnsiConsole.WriteLine(AuditSiteRenderer.ReleaseCountsBlock(document));
        return 0;
    }

    // The default folder name under Datastore:reports_dir, and the suffix a --limit run adds to it.
    // ColArtifacts skips "-limited" folders, so a test run never counts as rebuilding the site.
    public const string FolderName = "redlist-audit-2026";
    public const string LimitedFolderSuffix = "-limited";

    // Explicit --output wins. Otherwise default to a "redlist-audit-2026" subdirectory of the
    // configured reports directory (Datastore:reports_dir), falling back to ./reports only when no
    // reports directory is configured. A --limit run defaults to "redlist-audit-2026-limited" beside
    // it, so a test run never overwrites the pages of a full run.
    internal static string ResolveOutputDir(string? reportsDir, string? explicitDir, bool limited, string currentDir) {
        if (!string.IsNullOrWhiteSpace(explicitDir)) {
            return Path.GetFullPath(explicitDir, currentDir);
        }
        var baseDir = !string.IsNullOrWhiteSpace(reportsDir)
            ? reportsDir
            : Path.Combine(currentDir, "reports");
        return Path.Combine(Path.GetFullPath(baseDir, currentDir), limited ? FolderName + LimitedFolderSuffix : FolderName);
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

    private static AuditReleaseCounts LoadReleaseCounts(PathsService paths) {
        try {
            var rules = RulesPaths.Resolve(paths);
            var counts = AuditReleaseCounts.Load(rules.SourceRulesDir);
            return counts.SourcePath is not null ? counts : AuditReleaseCounts.Load(rules.BuildOutputRulesDir);
        } catch {
            return AuditReleaseCounts.Empty;
        }
    }

    private static IReadOnlyList<AuditDataSource> BuildDataSources(string? colRelease) {
        var detail = colRelease is null
            ? "Used as a taxonomic comparison in the Catalogue of Life reports"
            : $"{colRelease}, used as a taxonomic comparison in the Catalogue of Life reports";
        return new List<AuditDataSource> {
            new("Catalogue of Life reference", detail),
        };
    }
}
