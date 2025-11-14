using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class IucnApiCacheFullSettings : CommonSettings {
    [CommandOption("--source-db <PATH>")]
    [Description("Override path to the CSV-derived IUCN SQLite database (defaults to Datastore:IUCN_sqlite_from_cvs). Applied to the taxa step.")]
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
    [Description("Proceed to assessments even if the taxa step returns a non-zero exit code.")]
    public bool ContinueOnTaxaFailure { get; init; }
}

public sealed class IucnApiCacheFullCommand : AsyncCommand<IucnApiCacheFullSettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, IucnApiCacheFullSettings settings, CancellationToken cancellationToken) {
        _ = context;

        if (settings.SkipTaxa && settings.SkipAssessments) {
            AnsiConsole.MarkupLine("[yellow]Both --skip-taxa and --skip-assessments were supplied. Nothing to do.[/]");
            return 0;
        }

        var taxaResult = 0;
        if (!settings.SkipTaxa) {
            var taxaSettings = new IucnApiCacheTaxaSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                SourceDatabase = settings.SourceDatabase,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.TaxaLimit,
                Force = settings.ForceTaxa,
                MaxAgeHours = settings.TaxaMaxAgeHours,
                FailedOnly = settings.TaxaFailedOnly,
                SleepBetweenRequests = settings.TaxaSleepMs
            };

            taxaResult = await IucnApiCacheTaxaCommand.RunAsync(taxaSettings, cancellationToken).ConfigureAwait(false);
            if (taxaResult != 0 && !settings.ContinueOnTaxaFailure) {
                return taxaResult;
            }
        }

        var assessmentResult = 0;
        if (!settings.SkipAssessments) {
            var assessmentSettings = new IucnApiCacheAssessmentsSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.AssessmentLimit,
                Force = settings.ForceAssessments,
                MaxAgeHours = settings.AssessmentMaxAgeHours,
                FailedOnly = settings.AssessmentFailedOnly,
                SleepBetweenRequests = settings.AssessmentSleepMs
            };

            assessmentResult = await IucnApiCacheAssessmentsCommand.RunAsync(assessmentSettings, cancellationToken).ConfigureAwait(false);
        }

        return assessmentResult != 0 ? assessmentResult : taxaResult;
    }
}
