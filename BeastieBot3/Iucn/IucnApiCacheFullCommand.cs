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
                FailedOnly = settings.TaxaFailedOnly,
                SleepBetweenRequests = settings.TaxaSleepMs
            }, cancellationToken).ConfigureAwait(false);
            if (taxaResult != 0 && !settings.ContinueOnTaxaFailure) {
                return taxaResult;
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
                FailedOnly = settings.AssessmentFailedOnly,
                SleepBetweenRequests = settings.AssessmentSleepMs
            }, cancellationToken).ConfigureAwait(false);
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
            : infraResult != 0 ? infraResult
            : assessmentResult != 0 ? assessmentResult
            : projectResult;
    }
}
