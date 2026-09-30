using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

// Convenience command that runs both Wikidata caching steps sequentially:
// 1. Seed (wikidata seed-taxa): SPARQL sweeps for items with P627 or P141, queued for download
// 2. Cache (wikidata cache-entities): download entity JSON for each queued Q-ID
// Creates/updates Datastore:wikidata_cache_sqlite. Run via: wikidata cache-all

namespace BeastieBot3.Wikidata;

public sealed class WikidataCacheFullSettings : CommonSettings {
    [CommandOption("--cache <PATH>")]
    [Description("Override path to the Wikidata cache SQLite database (defaults to Datastore:wikidata_cache_sqlite). Used by both the seed-taxa and cache-entities phases.")]
    public string? CacheDatabase { get; init; }

    [CommandOption("--seed-limit <N>")]
    [Description("Maximum number of Wikidata items the seed-taxa phase reads in this run, for P627 and P141 combined; 0 or empty means no limit (the default). The next run continues from where this run stopped.")]
    public int? SeedLimit { get; init; }

    [CommandOption("--seed-batch-size <N>")]
    [Description("Number of Wikidata items per Wikidata Query Service request in the seed-taxa phase, from 5 to 2,000 (default: WIKIDATA_SPARQL_BATCH_SIZE in .env, or 500). If a request times out or the query service returns a server error, the command halves the batch size, down to 50, and retries.")]
    public int? SeedBatchSize { get; init; }

    [CommandOption("--seed-cursor <QID>")]
    [Description("Start the P627 and P141 sweeps of the seed-taxa phase after this Q-number (Q12345 or 12345), overriding the saved cursor. Later runs continue from where this run stops.")]
    public string? SeedCursor { get; init; }

    [CommandOption("--seed-reset-cursor")]
    [Description("Start the P627 and P141 sweeps of the seed-taxa phase again from the first Q-number, to find items that gained P627 or P141 after the sweep passed their Q-number. Ignored when --seed-cursor is given.")]
    public bool SeedResetCursor { get; init; }

    [CommandOption("--skip-seed")]
    [Description("Skip the seed-taxa phase, so the job only downloads the items already in the download queue.")]
    public bool SkipSeed { get; init; }

    [CommandOption("--download-limit <N>")]
    [Description("Maximum number of queued Wikidata items to download in the cache-entities phase; 0 or empty means all queued items (the default).")]
    public int? DownloadLimit { get; init; }

    [CommandOption("--download-max-age-hours <HOURS>")]
    [Description("In the cache-entities phase, also re-download cached Wikidata items that were downloaded more than this many hours ago. Empty or 0 means cached items are not re-downloaded (the default).")]
    public double? DownloadMaxAgeHours { get; init; }

    // Passed through as cache-entities --force, so this description has to match that option's
    // behaviour. It describes cache-entities --force from 6de6b52 and 3748738, which re-download
    // cached items, least recently downloaded first; before those commits --force changed nothing.
    [CommandOption("--download-force")]
    [Description("In the cache-entities phase, download the queued Wikidata items, then download every Wikidata item already in the cache again, least recently downloaded first, up to --download-limit in total. --download-max-age-hours is ignored.")]
    public bool DownloadForce { get; init; }

    [CommandOption("--download-failed-only")]
    [Description("In the cache-entities phase, download only the queued Wikidata items that failed to download on an earlier run; items never tried stay in the queue. The seed-taxa phase still runs unless --skip-seed is also given.")]
    public bool DownloadFailedOnly { get; init; }

    [CommandOption("--skip-download")]
    [Description("Skip the cache-entities phase, so the job only finds items with P627 or P141 and adds them to the download queue.")]
    public bool SkipDownload { get; init; }

    [CommandOption("--continue-on-seed-failure")]
    [Description("Run the cache-entities phase even if the seed-taxa phase stops with an error. The job still ends with a non-zero exit code.")]
    public bool ContinueOnSeedFailure { get; init; }
}

[CommandInfo("wikidata cache-all", CommandKind.Mutates,
    "Finds Wikidata items with P627 or P141 and downloads each item not yet in the Wikidata cache (runs wikidata seed-taxa, then wikidata cache-entities, as one job). Uses WIKIDATA_USER_AGENT from .env if it is set.",
    Reason = "Discovers Wikidata Q-ids and downloads their entity JSON into the cache (idempotent additive; --download-force re-downloads already-cached entities).",
    Rerun = RerunEffect.Discovers,
    Examples = new[] {
        "wikidata cache-all",
        "wikidata cache-all --seed-limit 1000 --download-limit 200"
    })]
public sealed class WikidataCacheFullCommand : AsyncCommand<WikidataCacheFullSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataCacheFullSettings settings, CancellationToken cancellationToken) {
        _ = context;

        var seedSettings = new WikidataSeedSettings {
            IniFile = settings.IniFile,
            SettingsDir = settings.SettingsDir,
            CacheDatabase = settings.CacheDatabase,
            Limit = settings.SeedLimit,
            BatchSize = settings.SeedBatchSize,
            Cursor = settings.SeedCursor,
            ResetCursor = settings.SeedResetCursor
        };

        var downloadSettings = new WikidataCacheItemsSettings {
            IniFile = settings.IniFile,
            SettingsDir = settings.SettingsDir,
            CacheDatabase = settings.CacheDatabase,
            Limit = settings.DownloadLimit,
            MaxAgeHours = settings.DownloadMaxAgeHours,
            Force = settings.DownloadForce,
            FailedOnly = settings.DownloadFailedOnly
        };

        return RunPhasesAsync(
            AnsiConsole.Console,
            settings.SkipSeed,
            settings.SkipDownload,
            settings.ContinueOnSeedFailure,
            () => WikidataSeedCommand.RunAsync(seedSettings, cancellationToken),
            () => WikidataCacheItemsCommand.RunAsync(downloadSettings, cancellationToken));
    }

    // seed-taxa returns 0 even when the query service stops responding (it keeps what it fetched
    // and says so), so a seed failure is an exception: a rejected query, an unreadable
    // --seed-cursor, a database error. Without --continue-on-seed-failure it propagates as before.
    // With it, the error is printed, the download phase runs, and the job still exits non-zero.
    // The flag used to test only the seed phase's return code, which is never non-zero, so it
    // changed nothing.
    internal static async Task<int> RunPhasesAsync(
        IAnsiConsole console,
        bool skipSeed,
        bool skipDownload,
        bool continueOnSeedFailure,
        Func<Task<int>> seed,
        Func<Task<int>> download) {
        if (skipSeed && skipDownload) {
            console.MarkupLine("[yellow]Both --skip-seed and --skip-download were supplied. Nothing to do.[/]");
            return 0;
        }

        var seedResult = 0;
        if (!skipSeed) {
            try {
                seedResult = await seed().ConfigureAwait(false);
            } catch (Exception ex) when (continueOnSeedFailure && ex is not OperationCanceledException) {
                console.MarkupLineInterpolated($"[red]The seed-taxa phase failed:[/] {ex.Message}");
                if (!skipDownload) {
                    console.MarkupLine("Running the cache-entities phase anyway, because --continue-on-seed-failure is set.");
                }
                seedResult = -1;
            }

            if (seedResult != 0 && !continueOnSeedFailure) {
                return seedResult;
            }
        }

        var downloadResult = 0;
        if (!skipDownload) {
            downloadResult = await download().ConfigureAwait(false);
        }

        return downloadResult != 0 ? downloadResult : seedResult;
    }
}
