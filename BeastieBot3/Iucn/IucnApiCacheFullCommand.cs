using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

// Convenience command that runs both API cache steps sequentially:
// 1. IucnApiCacheTaxaCommand - fetches /taxa/sis/{sisId} for all IUCN species
// 2. IucnApiCacheAssessmentsCommand - fetches /assessment/{id} from taxa JSON
// Creates/updates Datastore:IUCN_api_cache_sqlite. Resume-safe; skips existing.
// Run via: iucn api-cache full

namespace BeastieBot3.Iucn;

public sealed class IucnApiCacheFullSettings : CommonSettings {
    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs). Used by the cache-taxa phase to choose which species to fetch.")]
    public string? SourceDatabase { get; init; }

    [CommandOption("--cache <PATH>")]
    [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--taxa-limit <N>")]
    public long? TaxaLimit { get; init; }

    [CommandOption("--assessment-limit <N>")]
    public long? AssessmentLimit { get; init; }

    [CommandOption("--force-taxa")]
    public bool ForceTaxa { get; init; }

    [CommandOption("--force-assessments")]
    public bool ForceAssessments { get; init; }

    [CommandOption("--taxa-max-age-hours <HOURS>")]
    public double? TaxaMaxAgeHours { get; init; }

    [CommandOption("--assessment-max-age-hours <HOURS>")]
    public double? AssessmentMaxAgeHours { get; init; }

    [CommandOption("--refresh-before <DATE>")]
    [Description("Re-download anything fetched before this fixed date (UTC, e.g. 2026-06-16), in every phase. Normally left off: a refresh started with `iucn api refresh-start` supplies the date, so re-runs carry on without you entering it again.")]
    public string? RefreshBefore { get; init; }

    [CommandOption("--skip-tombstones")]
    [Description("Skip the final pass that re-checks taxa and assessments the API previously said were gone. That pass only runs as part of a refresh that asked for it.")]
    public bool SkipTombstones { get; init; }

    [CommandOption("--taxa-failed-only")]
    public bool TaxaFailedOnly { get; init; }

    [CommandOption("--assessment-failed-only")]
    public bool AssessmentFailedOnly { get; init; }

    [CommandOption("--taxa-sleep-ms <MS>")]
    public int TaxaSleepMs { get; init; } = 250;

    [CommandOption("--assessment-sleep-ms <MS>")]
    public int AssessmentSleepMs { get; init; } = 250;

    [CommandOption("--skip-taxa")]
    public bool SkipTaxa { get; init; }

    [CommandOption("--skip-assessments")]
    public bool SkipAssessments { get; init; }

    [CommandOption("--continue-on-taxa-failure")]
    [Description("Proceed to later phases even if the cache-taxa phase returns a non-zero exit code.")]
    public bool ContinueOnTaxaFailure { get; init; }

    // --- Optional extra phases (off by default; --full turns them all on) ---

    [CommandOption("--full")]
    [Description("Build the whole API dataset end to end: cache-taxa -> cache-infraranks (--from-csv) -> cache-assessments -> project-view. Shorthand for --infraranks --infraranks-from-csv --project.")]
    public bool Full { get; init; }

    [CommandOption("--infraranks")]
    [Description("After caching species, also fetch their subspecies/varieties (cache-infraranks) before downloading assessments.")]
    public bool Infraranks { get; init; }

    [CommandOption("--infraranks-from-csv")]
    [Description("Implies --infraranks; also seed infraspecific taxa from the CSV (catches assessed subspecies of unassessed species). Needs the CSV import.")]
    public bool InfraranksFromCsv { get; init; }

    [CommandOption("--project")]
    [Description("After caching, build the CSV-shaped projection (iucn api project-view) so the data is usable via --dataset api.")]
    public bool Project { get; init; }

    [CommandOption("--allow-partial")]
    [Description("Passed to the project-view phase: accept (exit 0) a projection built before every latest assessment is downloaded.")]
    public bool AllowPartial { get; init; }

    [CommandOption("--redlist-version <VERSION>")]
    [Description("Label stored as the projection's redlist_version in the --project phase. Defaults to 'api-cache'.")]
    public string? RedlistVersion { get; init; }

    [CommandOption("--status")]
    [Description("Show what each phase has left to do and what a run would do, then exit without downloading anything.")]
    public bool StatusOnly { get; init; }
}

[CommandInfo("iucn api cache-all", CommandKind.Mutates,
    "Convenience wrapper that chains the API-cache phases in one job: cache-taxa then cache-assessments (the default), and with --full also cache-infraranks (subspecies/varieties, --from-csv) and project-view — i.e. the whole --dataset api build end to end.",
    Reason = "Caches IUCN /api/v4 taxa + assessment payloads into the local API cache (idempotent additive; --force-taxa/--force-assessments re-download already-cached entries). --project also rebuilds the derived projection DB.",
    Rerun = RerunEffect.IdempotentAdd,
    Examples = new[] {
        "iucn api cache-all",
        "iucn api cache-all --full",
        "iucn api cache-all --full --status",
        "iucn api cache-all --full --skip-taxa",
        "iucn api cache-all --taxa-limit 100 --assessment-limit 200"
    })]
public sealed class IucnApiCacheFullCommand : AsyncCommand<IucnApiCacheFullSettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, IucnApiCacheFullSettings settings, CancellationToken cancellationToken) {
        _ = context;

        // --full is shorthand for the extra phases.
        var runInfraranks = settings.Infraranks || settings.InfraranksFromCsv || settings.Full;
        var infraranksFromCsv = settings.InfraranksFromCsv || settings.Full;
        var runProject = settings.Project || settings.Full;

        if (settings.SkipTaxa && settings.SkipAssessments && !runInfraranks && !runProject) {
            AnsiConsole.MarkupLine("[yellow]Nothing to do — every phase is skipped.[/]");
            return 0;
        }

        // Read the refresh in progress once, up front: it decides whether the family sweep and the
        // tombstone pass run at all, and it is what gets closed when everything is downloaded.
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        IucnRefreshSession? session;
        using (var store = IucnApiCacheStore.Open(cachePath)) {
            session = store.GetActiveRefreshSession();
            if (session is not null) {
                var progress = IucnApiRefreshStartCommand.ReadProgress(store, session);
                AnsiConsole.MarkupLineInterpolated(
                    $"[yellow]Refresh in progress:[/] {session.DisplayLabel}, cutoff {IucnRefreshMath.Stamp(session.CutoffUtc)}. Still to re-download: {progress.TaxaRemaining:N0} taxa, {progress.AssessmentsRemaining:N0} assessments.");
            }
        }

        // The family sweep is part of a refresh, not of an ordinary top-up, so it runs when the
        // refresh asked for it and has not done it yet.
        var runDiscovery = session is { IncludeDiscovery: true, DiscoveryDoneAt: null };
        var runTombstones = !settings.SkipTombstones && session is { IncludeTombstones: true, TombstonesDoneAt: null };

        // The plan up front (and nothing but the plan with --status): each phase with what it has
        // left, so "which phase am I up to" is answered before anything downloads.
        var state = IucnApiCacheStateReader.Read(paths);
        PrintPlan(state, settings, runDiscovery, runInfraranks, runTombstones, runProject, settings.StatusOnly);
        if (settings.StatusOnly) {
            return 0;
        }

        // Pipeline order: taxa -> infraranks -> assessments -> project. Assessments runs after
        // infraranks so the single download pass picks up the infra taxa's queued assessments too.
        var taxaResult = 0;
        if (!settings.SkipTaxa) {
            AnsiConsole.MarkupLine("[grey]== Phase: cache-taxa ==[/]");
            taxaResult = await IucnApiCacheTaxaCommand.RunAsync(new IucnApiCacheTaxaSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                SourceDatabase = settings.SourceDatabase,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.TaxaLimit,
                Force = settings.ForceTaxa,
                MaxAgeHours = settings.TaxaMaxAgeHours,
                RefreshBefore = settings.RefreshBefore,
                FailedOnly = settings.TaxaFailedOnly,
                SleepBetweenRequests = settings.TaxaSleepMs
            }, cancellationToken).ConfigureAwait(false);
            if (taxaResult != 0 && !settings.ContinueOnTaxaFailure) {
                return taxaResult;
            }
        }

        // Family paging finds taxa the CSV export omits (removed, reclassified, historical-only).
        // Before infraranks, so their infraspecific taxa are reachable in the same run.
        var discoveryResult = 0;
        if (runDiscovery) {
            AnsiConsole.MarkupLine("[grey]== Phase: discover-by-family ==[/]");
            discoveryResult = await new IucnApiCacheDiscoverByFamilyCommand().ExecuteAsync(context, new IucnApiCacheDiscoverByFamilySettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                RefreshBefore = settings.RefreshBefore,
                SleepBetweenRequests = settings.TaxaSleepMs
            }, cancellationToken).ConfigureAwait(false);

            if (discoveryResult == 0 && session is not null) {
                using var store = IucnApiCacheStore.Open(cachePath);
                store.MarkRefreshPhaseDone(session.Id, "discovery_done_at");
            }
        }

        var infraResult = 0;
        if (runInfraranks) {
            AnsiConsole.MarkupLine("[grey]== Phase: cache-infraranks ==[/]");
            infraResult = await IucnApiCacheInfraranksCommand.RunAsync(new IucnApiCacheInfraranksSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                SourceDatabase = settings.SourceDatabase,
                FromCsv = infraranksFromCsv,
                Limit = settings.TaxaLimit,
                Force = settings.ForceTaxa,
                MaxAgeHours = settings.TaxaMaxAgeHours,
                RefreshBefore = settings.RefreshBefore,
                SleepBetweenRequests = settings.TaxaSleepMs
            }, cancellationToken).ConfigureAwait(false);
        }

        var assessmentResult = 0;
        if (!settings.SkipAssessments) {
            AnsiConsole.MarkupLine("[grey]== Phase: cache-assessments ==[/]");
            assessmentResult = await IucnApiCacheAssessmentsCommand.RunAsync(new IucnApiCacheAssessmentsSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.AssessmentLimit,
                Force = settings.ForceAssessments,
                MaxAgeHours = settings.AssessmentMaxAgeHours,
                RefreshBefore = settings.RefreshBefore,
                FailedOnly = settings.AssessmentFailedOnly,
                SleepBetweenRequests = settings.AssessmentSleepMs
            }, cancellationToken).ConfigureAwait(false);
        }

        // Last, and only once the rest is downloaded: re-check what the API said was gone. A taxon
        // absent from the previous release can exist in the new one, and nothing else ever looks at
        // those ids again. Small enough (a few thousand) that an interrupted pass just re-runs.
        if (runTombstones) {
            AnsiConsole.MarkupLine("[grey]== Phase: re-check taxa and assessments previously reported gone ==[/]");
            await IucnApiCacheTaxaCommand.RunAsync(new IucnApiCacheTaxaSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                SourceDatabase = settings.SourceDatabase,
                CacheDatabase = settings.CacheDatabase,
                RetryTombstones = true,
                RefreshBefore = settings.RefreshBefore,
                SleepBetweenRequests = settings.TaxaSleepMs
            }, cancellationToken).ConfigureAwait(false);

            await IucnApiCacheAssessmentsCommand.RunAsync(new IucnApiCacheAssessmentsSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                RetryTombstones = true,
                RefreshBefore = settings.RefreshBefore,
                SleepBetweenRequests = settings.AssessmentSleepMs
            }, cancellationToken).ConfigureAwait(false);

            // Most of these are expected to fail again — that is the answer, not an error — so the
            // pass is marked done regardless of exit code, or it would repeat on every run forever.
            if (session is not null) {
                using var store = IucnApiCacheStore.Open(cachePath);
                store.MarkRefreshPhaseDone(session.Id, "tombstones_done_at");
            }
            AnsiConsole.MarkupLine("[grey]Ones that failed again are still recorded as gone. The handful of assessments the API answers with a server error are a known fault, not a new problem: see[/] [yellow]iucn api report-failed-assessments[/][grey].[/]");
        }

        // Close the refresh once nothing is left older than its cutoff and every phase has run,
        // so later runs go back to fetching only what is missing.
        if (session is not null) {
            using var store = IucnApiCacheStore.Open(cachePath);
            IucnRefreshRun.CloseIfFinished(store, store.GetActiveRefreshSession());
        }

        var projectResult = 0;
        if (runProject) {
            AnsiConsole.MarkupLine("[grey]== Phase: project-view ==[/]");
            projectResult = await IucnApiProjectViewCommand.RunAsync(new IucnApiProjectViewCommand.Settings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CachePath = settings.CacheDatabase,
                RedlistVersion = settings.RedlistVersion,
                AllowPartial = settings.AllowPartial
            }, cancellationToken).ConfigureAwait(false);
        }

        // Where things stand now, and what a re-run would still find. Without this, a run that
        // hit its limits or was stopped reads as finished.
        AnsiConsole.WriteLine();
        PrintTotals(IucnApiCacheStateReader.Read(paths), finishing: true);

        // Surface the first non-zero result in pipeline order (project-view returns 2 when partial).
        return taxaResult != 0 ? taxaResult
            : discoveryResult != 0 ? discoveryResult
            : infraResult != 0 ? infraResult
            : assessmentResult != 0 ? assessmentResult
            : projectResult;
    }

    private static void PrintPlan(IucnApiCacheState s, IucnApiCacheFullSettings settings,
                                  bool runDiscovery, bool runInfraranks, bool runTombstones, bool runProject,
                                  bool statusOnly) {
        var refresh = s.RefreshProgress;

        var table = new Table().Border(TableBorder.Simple);
        table.AddColumn("#");
        table.AddColumn("Phase");
        table.AddColumn(statusOnly ? "What a run would do" : "This run");

        var n = 0;
        void Row(string phase, bool runs, string text) =>
            table.AddRow((++n).ToString(), phase, runs ? text : $"[grey]skip — {text}[/]");

        Row("Download species records (cache-taxa)",
            runs: !settings.SkipTaxa,
            settings.SkipTaxa ? "--skip-taxa"
                : refresh is { TaxaRemaining: > 0 } ? $"{refresh.TaxaRemaining:N0} taxa to re-download for re-import {refresh.Session.DisplayLabel}"
                : "adds only species not cached yet");

        Row("Family sweep for taxa the CSV omits (discover-by-family)",
            runs: runDiscovery,
            runDiscovery ? "this re-import asked for it and it has not run yet"
                : s.ActiveSession is { IncludeDiscovery: true } ? "already done for this re-import"
                : "only runs as part of a re-import that asks for it");

        Row("Download subspecies and varieties (cache-infraranks)",
            runs: runInfraranks,
            runInfraranks ? "adds only infraspecific taxa not cached yet" : "add --full to include it");

        Row("Download assessments (cache-assessments)",
            runs: !settings.SkipAssessments,
            settings.SkipAssessments ? "--skip-assessments"
                : BuildAssessmentPlanText(s, refresh));

        Row("Re-check taxa the API previously said were gone",
            runs: runTombstones,
            runTombstones ? $"{s.TombstonedTaxa:N0} ids to re-check against the new release"
                : settings.SkipTombstones ? "--skip-tombstones"
                : s.ActiveSession is { IncludeTombstones: true } ? "already done for this re-import"
                : "only runs as part of a re-import that asks for it");

        Row("Rebuild the projection --dataset api reads (project-view)",
            runs: runProject,
            runProject ? ProjectionPlanText(s.Projection) : "add --full to include it");

        AnsiConsole.Write(table);
        PrintTotals(s, finishing: false);
    }

    private static string BuildAssessmentPlanText(IucnApiCacheState s, IucnRefreshProgress? refresh) {
        var backlog = Math.Max(0, s.BacklogOutstanding - s.ServerErrorAssessments);
        var parts = new System.Collections.Generic.List<string>();
        if (backlog > 0) parts.Add($"{backlog:N0} queued assessments to download");
        if (refresh is { AssessmentsRemaining: > 0 }) parts.Add($"{refresh.AssessmentsRemaining:N0} to re-download for the re-import");
        if (s.ServerErrorAssessments > 0) parts.Add($"{s.ServerErrorAssessments:N0} the API answers with a server error are left alone");
        return parts.Count == 0 ? "adds only assessments not cached yet" : string.Join(" · ", parts);
    }

    private static string ProjectionPlanText(IucnProjectionState? p) {
        if (p is null || !p.Exists) return "not built yet";
        if (p.IsPartial) return $"currently incomplete ({p.LatestNotDownloaded:N0} taxa missing their latest assessment); rebuilt at the end of this run";
        return $"rebuilt from whatever this run finishes with (currently {p.ProjectedTaxa:N0} taxa)";
    }

    // The standing counts, so the plan and the finish line both say where the dataset stands
    // overall, not just what one run touched.
    private static void PrintTotals(IucnApiCacheState s, bool finishing) {
        if (!s.CacheExists) {
            AnsiConsole.MarkupLine("[grey]No API cache on disk yet; the first run creates it.[/]");
            return;
        }

        var age = s.OldestTaxaDownloadedAt is { } oldest ? $" · oldest fetched {IucnRefreshMath.Stamp(oldest)}" : "";
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]API cache:[/] {s.TaxaCached:N0} taxa · {s.AssessmentsCached:N0} assessments{age} · {s.TombstonedTaxa:N0} ids recorded as gone");

        var p = s.Projection;
        var projection = p is null || !p.Exists ? "not built yet"
            : p.IsPartial ? $"incomplete ({p.LatestNotDownloaded:N0} taxa missing their latest assessment)"
            : $"complete · {p.ProjectedTaxa:N0} taxa · built {IucnRefreshMath.Stamp(p.BuiltAt ?? DateTime.MinValue)}";
        AnsiConsole.MarkupLineInterpolated($"[grey]Projection:[/] {projection}");

        if (!finishing) return;

        if (s.RefreshProgress is { } refresh && (refresh.TaxaRemaining > 0 || refresh.AssessmentsRemaining > 0)) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Still to do:[/] re-import {refresh.Session.DisplayLabel} is {refresh.PercentDone}% done — {refresh.TaxaRemaining:N0} taxa and {refresh.AssessmentsRemaining:N0} assessments to re-download. Run `iucn api cache-all --full` again to carry on; the cutoff date is remembered.");
            return;
        }
        var backlog = Math.Max(0, s.BacklogOutstanding - s.ServerErrorAssessments);
        if (backlog > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]Still to do:[/] {backlog:N0} queued assessments are not downloaded yet. Run `iucn api cache-all --full` again to fetch them.");
            return;
        }
        AnsiConsole.MarkupLine(s.Projection is { Exists: true, IsPartial: false }
            ? "[green]The API dataset is complete and the projection is current.[/]"
            : "[green]All downloads are done.[/] Rebuild the projection (`iucn api project-view`, or re-run with --full) so --dataset api reads the new data.");
    }
}
