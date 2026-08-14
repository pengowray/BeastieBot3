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
}

[CommandInfo("iucn api cache-all", CommandKind.Mutates,
    "Convenience wrapper that chains the API-cache phases in one job: cache-taxa then cache-assessments (the default), and with --full also cache-infraranks (subspecies/varieties, --from-csv) and project-view — i.e. the whole --dataset api build end to end.",
    Reason = "Caches IUCN /api/v4 taxa + assessment payloads into the local API cache (idempotent additive; --force-taxa/--force-assessments re-download already-cached entries). --project also rebuilds the derived projection DB.",
    Rerun = RerunEffect.IdempotentAdd,
    Examples = new[] {
        "iucn api cache-all",
        "iucn api cache-all --full",
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

        // Surface the first non-zero result in pipeline order (project-view returns 2 when partial).
        return taxaResult != 0 ? taxaResult
            : discoveryResult != 0 ? discoveryResult
            : infraResult != 0 ? infraResult
            : assessmentResult != 0 ? assessmentResult
            : projectResult;
    }
}
