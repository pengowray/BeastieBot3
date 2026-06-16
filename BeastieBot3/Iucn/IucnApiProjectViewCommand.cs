using System;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// Builds a CSV-shaped relational projection of the IUCN API cache so that
// `wikipedia generate-lists`/`generate-charts` can run on the API dataset
// (via --dataset api). Reads the latest downloaded /api/v4/assessment payloads,
// maps them through IucnAssessmentJsonParser + IucnRedlistStatus, and writes
// taxonomy_html + assessments_html + view_assessments_html_taxonomy_html into a
// derived DB (Datastore:IUCN_api_projected_sqlite). See IucnApiProjectionStore.

namespace BeastieBot3.Iucn;

[CommandInfo("iucn api project-view", CommandKind.Mutates,
    "Build a CSV-shaped relational projection of the IUCN API cache so the Wikipedia list and chart commands can run on the API dataset (pass --dataset api). Projects the latest cached assessments only.",
    Reason = "Rewrites the derived projection database (Datastore:IUCN_api_projected_sqlite) from the cached API JSON.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Fully rebuilds the projection from whatever is currently cached. Run after iucn api cache-all / cache-assessments so the latest assessments are downloaded.",
    Examples = new[] {
        "iucn api project-view",
        "iucn api project-view --redlist-version 2025-2",
        "iucn api project-view --limit 1000"
    })]
public sealed class IucnApiProjectViewCommand : AsyncCommand<IucnApiProjectViewCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("Override path to the source IUCN API cache (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CachePath { get; init; }

        [CommandOption("--output <PATH>")]
        [Description("Override path to the derived projection database (defaults to Datastore:IUCN_api_projected_sqlite).")]
        public string? OutputPath { get; init; }

        [CommandOption("--redlist-version <VERSION>")]
        [Description("Label stored as the projection's redlist_version (the API cache is unversioned). Defaults to 'api-cache'.")]
        public string? RedlistVersion { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Process at most N cached assessments (for testing).")]
        public int? Limit { get; init; }

        [CommandOption("--allow-partial")]
        [Description("Build (and exit 0) even when some taxa have a latest assessment whose JSON isn't downloaded yet. Without this, a partial projection still builds but the command exits non-zero.")]
        public bool AllowPartial { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return RunAsync(settings, cancellationToken);
    }

    // Callable entry point so the cache-all wrapper can chain the projection step.
    internal static Task<int> RunAsync(Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CachePath);
        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN API cache not found:[/] {cachePath}");
            AnsiConsole.MarkupLine("Build it first with [yellow]iucn api cache-all[/] (and optionally [yellow]iucn api discover-by-family[/]).");
            return Task.FromResult(1);
        }
        var outputPath = paths.ResolveIucnApiProjectedPath(settings.OutputPath);
        var version = string.IsNullOrWhiteSpace(settings.RedlistVersion) ? "api-cache" : settings.RedlistVersion!.Trim();

        AnsiConsole.MarkupLineInterpolated($"[grey]Source API cache:[/] {cachePath}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Projection output:[/] {outputPath}");

        var ro = new SqliteConnectionStringBuilder { DataSource = cachePath, Mode = SqliteOpenMode.ReadOnly };
        using var source = new SqliteConnection(ro.ConnectionString);
        source.Open();

        long total = ScalarLong(source, "SELECT COUNT(*) FROM assessments");

        // A latest assessment can be "not downloaded" for two very different reasons:
        //   pending     — we simply haven't fetched it yet (keep caching to close the gap)
        //   unservable  — /api/v4/assessment/{id} keeps erroring on it (recorded in
        //                 failed_requests), so it can never be cached. IUCN-side, not ours.
        // Only pending gaps should make a projection "partial"; unservable ones are an
        // accepted, permanent gap (a tiny handful of records the API 500s on). Without this
        // split, those would force --allow-partial on every run forever.
        var hasFailedRequests = ObjectExists(source, "failed_requests");
        const string missingLatest =
            "FROM taxa_assessment_backlog b WHERE b.latest = 1 " +
            "AND NOT EXISTS (SELECT 1 FROM assessments a WHERE a.assessment_id = b.assessment_id)";
        const string errored =
            "EXISTS (SELECT 1 FROM failed_requests f WHERE f.endpoint = 'assessment' " +
            "AND CAST(f.entity_id AS INTEGER) = b.assessment_id)";

        long latestUnservable = hasFailedRequests
            ? ScalarLong(source, $"SELECT COUNT(*) {missingLatest} AND {errored}")
            : 0;
        long latestPending = hasFailedRequests
            ? ScalarLong(source, $"SELECT COUNT(*) {missingLatest} AND NOT {errored}")
            : ScalarLong(source, $"SELECT COUNT(*) {missingLatest}");

        using var store = IucnApiProjectionStore.Open(outputPath);
        store.ResetData();
        var importId = store.InsertImport(Path.GetFileName(cachePath), version);

        long processed = 0, latestRows = 0, skippedNoTaxon = 0, unknownCategory = 0;

        using (var cmd = source.CreateCommand()) {
            // Deterministic order so a re-run projects identical rows and the INSERT OR IGNORE
            // dedupe (taxonomy_html keyed on taxonId) picks a stable winner.
            cmd.CommandText = settings.Limit is > 0
                ? $"SELECT json FROM assessments ORDER BY assessment_id LIMIT {settings.Limit.Value}"
                : "SELECT json FROM assessments ORDER BY assessment_id";

            using var reader = cmd.ExecuteReader();
            using var writer = store.BeginWrite();

            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                processed++;
                if (reader.IsDBNull(0)) continue;

                var parsed = IucnAssessmentJsonParser.Parse(reader.GetString(0));
                if (parsed is null || !parsed.Latest) continue;        // current snapshot only
                if (parsed.TaxonId is null) { skippedNoTaxon++; continue; }

                var (categoryText, known) = ResolveCategory(parsed);
                if (!known) unknownCategory++;

                writer.AddTaxonomy(importId, parsed.TaxonId.Value, parsed.ScientificName,
                    parsed.KingdomName, parsed.PhylumName, parsed.ClassName, parsed.OrderName,
                    parsed.FamilyName, parsed.GenusName, parsed.SpeciesName);
                writer.AddAssessment(importId, parsed, categoryText);
                latestRows++;

                if (processed % 20000 == 0) {
                    AnsiConsole.MarkupLineInterpolated($"[grey]…processed {processed:N0} / {total:N0} assessments ({latestRows:N0} latest)[/]");
                }
            }

            writer.Commit();
        }

        store.BuildView();

        var projectedTaxa = store.CountRows("taxonomy_html");
        var projectedAssessments = store.CountRows("assessments_html");
        // Only genuinely-pending downloads make the projection partial; assessments the API
        // can't serve are an accepted permanent gap and don't block. Persist the pending count
        // as latest_not_downloaded so import_metadata's is_partial matches what we report.
        var isPartial = latestPending > 0;
        store.CompleteImport(importId, projectedTaxa, projectedAssessments, latestPending, isPartial);

        AnsiConsole.MarkupLine(isPartial ? "[yellow]Projection built (partial).[/]" : "[green]Projection built.[/]");
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Metric");
        table.AddColumn(new TableColumn("Value").RightAligned());
        table.AddRow("Assessments scanned", $"{processed:N0}");
        table.AddRow("Latest rows projected", $"{latestRows:N0}");
        table.AddRow("Taxa projected", $"{projectedTaxa:N0}");
        table.AddRow("Latest pending download", $"{latestPending:N0}");
        table.AddRow("Latest unservable (API errors)", $"{latestUnservable:N0}");
        table.AddRow("Skipped (no taxon id)", $"{skippedNoTaxon:N0}");
        table.AddRow("Unknown category codes", $"{unknownCategory:N0}");
        table.AddRow("redlist_version", version);
        table.AddRow("Coverage", isPartial ? "[yellow]partial[/]" : "[green]complete[/]");
        AnsiConsole.Write(table);

        if (latestUnservable > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]Note:[/] {latestUnservable:N0} taxa have a latest assessment the IUCN API keeps erroring on (e.g. HTTP 500) — an accepted permanent gap, so they don't make the projection partial. See [yellow]iucn api report-failed-assessments[/].");
        }
        if (isPartial) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Note:[/] {latestPending:N0} taxa have a latest assessment not downloaded yet — those taxa are missing from the projection. Run [yellow]iucn api cache-assessments[/] for full coverage.");
            if (!settings.AllowPartial) {
                AnsiConsole.MarkupLine("[red]Projection is partial.[/] Re-run after caching, or pass [yellow]--allow-partial[/] to accept it. (The database was still written and is flagged partial in import_metadata.)");
                return Task.FromResult(2);
            }
        }
        return Task.FromResult(0);
    }

    // Map the API category code to the canonical CSV category text the consumers
    // compare against. Falls back to the API english label, then the raw code.
    private static (string text, bool known) ResolveCategory(ProjectedAssessment a) {
        if (!string.IsNullOrWhiteSpace(a.RedlistCategoryCode)
            && IucnRedlistStatus.TryGetDescriptor(a.RedlistCategoryCode, out var descriptor)) {
            return (descriptor!.Category, true);
        }
        var fallback = a.RedlistCategoryEn ?? a.RedlistCategoryCode ?? string.Empty;
        return (fallback, false);
    }

    private static long ScalarLong(SqliteConnection connection, string sql) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        try { return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L); }
        catch { return 0L; }
    }

    private static bool ObjectExists(SqliteConnection connection, string name) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM sqlite_master WHERE name = @name LIMIT 1";
        cmd.Parameters.AddWithValue("@name", name);
        return cmd.ExecuteScalar() is not null;
    }
}
