using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataCacheFullSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite). Applied to both steps.")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--seed-limit <N>")]
    public int? SeedLimit { get; init; }

    [CommandOption("--seed-batch-size <N>")]
    public int? SeedBatchSize { get; init; }

    [CommandOption("--seed-cursor <QID>")]
    public string? SeedCursor { get; init; }

    [CommandOption("--seed-reset-cursor")]
    public bool SeedResetCursor { get; init; }

    [CommandOption("--skip-seed")]
    public bool SkipSeed { get; init; }

    [CommandOption("--download-limit <N>")]
    public int? DownloadLimit { get; init; }

    [CommandOption("--download-max-age-hours <HOURS>")]
    public double? DownloadMaxAgeHours { get; init; }

    [CommandOption("--download-force")]
    public bool DownloadForce { get; init; }

    [CommandOption("--download-failed-only")]
    public bool DownloadFailedOnly { get; init; }

    [CommandOption("--skip-download")]
    public bool SkipDownload { get; init; }

    [CommandOption("--continue-on-seed-failure")]
    [Description("Proceed to the download step even if the seed step returned a non-zero exit code.")]
    public bool ContinueOnSeedFailure { get; init; }
}

public sealed class WikidataCacheFullCommand : AsyncCommand<WikidataCacheFullSettings> {
    public override async Task<int> ExecuteAsync(CommandContext context, WikidataCacheFullSettings settings, CancellationToken cancellationToken) {
        _ = context;

        if (settings.SkipSeed && settings.SkipDownload) {
            AnsiConsole.MarkupLine("[yellow]Both --skip-seed and --skip-download were supplied. Nothing to do.[/]");
            return 0;
        }

        var seedResult = 0;
        if (!settings.SkipSeed) {
            var seedSettings = new WikidataSeedSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.SeedLimit,
                BatchSize = settings.SeedBatchSize,
                Cursor = settings.SeedCursor,
                ResetCursor = settings.SeedResetCursor
            };

            seedResult = await WikidataSeedCommand.RunAsync(seedSettings, cancellationToken).ConfigureAwait(false);
            if (seedResult != 0 && !settings.ContinueOnSeedFailure) {
                return seedResult;
            }
        }

        var downloadResult = 0;
        if (!settings.SkipDownload) {
            var downloadSettings = new WikidataCacheItemsSettings {
                IniFile = settings.IniFile,
                SettingsDir = settings.SettingsDir,
                CacheDatabase = settings.CacheDatabase,
                Limit = settings.DownloadLimit,
                MaxAgeHours = settings.DownloadMaxAgeHours,
                Force = settings.DownloadForce,
                FailedOnly = settings.DownloadFailedOnly
            };

            downloadResult = await WikidataCacheItemsCommand.RunAsync(downloadSettings, cancellationToken).ConfigureAwait(false);
        }

        return downloadResult != 0 ? downloadResult : seedResult;
    }
}
