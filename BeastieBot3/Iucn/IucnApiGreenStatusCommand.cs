using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// `iucn api green-status`: downloads every IUCN Green Status of Species assessment
// (/api/v4/green_status/all, one request) into the green_status table of the IUCN API cache,
// which `site build-db` reads. Each run asks for the Red List version first, so that a record
// stored for the first time gets the release it appeared in (IucnApiCacheStore.SaveGreenStatus).
// The run ignores refresh sessions (iucn api refresh-start): it downloads the whole list every time.

namespace BeastieBot3.Iucn;

[CommandInfo("iucn api green-status", CommandKind.Mutates,
    "Download all published IUCN Green Status of Species assessments from the IUCN Red List API into the IUCN API cache.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads all Green Status assessments again and updates the stored ones to match the new download, so assessments that IUCN has replaced or withdrawn are deleted. Each assessment keeps the date and Red List version of the download it first appeared in.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "iucn api green-status",
        "iucn api green-status --status",
    })]
internal sealed class IucnApiGreenStatusCommand : AsyncCommand<IucnApiGreenStatusCommand.Settings> {
    // Option order is the order the web UI shows the fields in.
    public sealed class Settings : CommonSettings {
        [CommandOption("--status")]
        [Description("Print what is stored and exit, without querying the IUCN Red List API.")]
        public bool Status { get; init; }

        [CommandOption("--cache <PATH>")]
        [Description("Override path to the API cache SQLite database (defaults to Datastore:IUCN_api_cache_sqlite).")]
        public string? CacheDatabase { get; init; }
    }

    // The longest list of new or deleted assessments printed after a run.
    private const int MaxListed = 20;

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        AnsiConsole.MarkupLineInterpolated($"[grey]API cache database:[/] {cachePath}");

        if (settings.Status) {
            // Read-only: --status writes nothing, not even the table on a cache that lacks it.
            using var readOnly = IucnApiCacheStore.OpenReadOnly(cachePath);
            PrintStatus(readOnly?.GetGreenStatusSummary());
            return 0;
        }

        var configuration = IucnApiConfiguration.FromEnvironment();
        using var client = new IucnApiClient(configuration);
        using var store = IucnApiCacheStore.Open(cachePath);

        var outcome = await IucnGreenStatusRun.RunAsync(store, client, DateTime.UtcNow, cancellationToken).ConfigureAwait(false);
        if (outcome.RedListVersion is { } version) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Current Red List version:[/] {version}");
        }
        if (outcome.Saved is not { } saved) {
            AnsiConsole.MarkupLine(RefusalMarkup(outcome));
            AnsiConsole.MarkupLine("The IUCN API cache was not changed.");
            return 1;
        }

        PrintRun(outcome, saved);
        return 0;
    }

    // A short red label, then the detail. The caller adds the line saying the cache was not changed.
    internal static string RefusalMarkup(GreenStatusRunOutcome outcome) {
        var detail = outcome.Detail ?? "";
        var (label, text) = outcome.Refusal switch {
            GreenStatusRefusal.VersionRequestFailed => ("Red List version request failed:", detail),
            GreenStatusRefusal.VersionUnreadable => ("Red List version not found in the answer:", detail),
            GreenStatusRefusal.DownloadFailed => ("Download failed:", detail),
            GreenStatusRefusal.NotJson => ("Unreadable download:", $"the answer is not JSON ({detail})."),
            GreenStatusRefusal.NoList => ("Unreadable download:", "the answer has no \"assessments\" list."),
            GreenStatusRefusal.Empty => ("Empty download:", "the IUCN Red List API returned no Green Status assessments."),
            GreenStatusRefusal.NoTaxonId => ("Missing taxon ID:", $"assessment {outcome.Index:N0} of {outcome.Total:N0} in the download."),
            GreenStatusRefusal.NoDate => ("Missing assessment date:", $"assessment {outcome.Index:N0} of {outcome.Total:N0} in the download."),
            _ => ("Stopped:", detail),
        };
        var markup = $"[red]{Markup.Escape(label)}[/]";
        return text.Length > 0 ? markup + " " + Markup.Escape(text) : markup;
    }

    private static void PrintRun(GreenStatusRunOutcome outcome, GreenStatusSaveResult saved) {
        WriteCategoryTable(outcome.Records
            .GroupBy(r => r.SpeciesRecoveryCategory)
            .Select(g => new GreenStatusCount(g.Key, g.Count()))
            .OrderByDescending(c => c.Rows).ThenBy(c => c.Value, StringComparer.Ordinal)
            .ToList());

        AnsiConsole.MarkupLineInterpolated($"Green Status assessments downloaded: {saved.Stored:N0}");
        if (outcome.DuplicateKeys > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{outcome.DuplicateKeys:N0} Green Status assessments in the download have the same taxon ID and assessment date as an earlier one.[/] Only the last of each is stored.");
        }
        if (saved.FirstDownload) {
            AnsiConsole.MarkupLineInterpolated($"First download: all {saved.Stored:N0} Green Status assessments are marked as part of the first download, because it is not known which Red List version each was first published in.");
            return;
        }
        AnsiConsole.MarkupLineInterpolated($"  New since the last download: {saved.Added.Count:N0}");
        AnsiConsole.MarkupLineInterpolated($"  Changed since the last download: {saved.Changed:N0}");
        AnsiConsole.MarkupLineInterpolated($"  Deleted from the cache (not in this download): {saved.Removed.Count:N0}");
        WriteList("New Green Status assessments", saved.Added);
        WriteList("Green Status assessments deleted from the cache", saved.Removed);
    }

    private static void WriteList(string heading, IReadOnlyList<GreenStatusRowInfo> rows) {
        if (rows.Count == 0) return;
        var shown = Math.Min(rows.Count, MaxListed);
        var title = shown < rows.Count ? $"{heading} ({shown:N0} of {rows.Count:N0}):" : $"{heading}:";
        AnsiConsole.MarkupLine($"[bold]{Markup.Escape(title)}[/]");
        var table = new Table().Border(TableBorder.Rounded)
            .AddColumn("Scientific name")
            .AddColumn(new TableColumn("Taxon ID").RightAligned())
            .AddColumn("Assessment date");
        foreach (var row in rows.Take(shown)) {
            table.AddRow(Markup.Escape(row.ScientificName ?? ""), row.SisId.ToString(CultureInfo.InvariantCulture), row.AssessmentDate);
        }
        AnsiConsole.Write(table);
    }

    private static void PrintStatus(GreenStatusSummary? summary) {
        if (summary is null) {
            AnsiConsole.MarkupLine("No Green Status assessments stored. Run [bold]iucn api green-status[/] to download them.");
            return;
        }
        WriteCategoryTable(summary.ByCategory);
        AnsiConsole.MarkupLineInterpolated($"Green Status assessments stored: {summary.Rows:N0}");
        AnsiConsole.MarkupLineInterpolated($"Stored from the first download: {summary.BaselineRows:N0}");
        AnsiConsole.MarkupLineInterpolated($"First download: {Stamp(summary.FirstSeenMin)}");
        AnsiConsole.MarkupLineInterpolated($"Last download: {Stamp(summary.LastSeenMax)}");

        var versions = new Table().Border(TableBorder.Rounded)
            .AddColumn("Red List version when first downloaded")
            .AddColumn(new TableColumn("Assessments").RightAligned());
        foreach (var v in summary.ByFirstSeenVersion) {
            var version = v.Version ?? "(not recorded)";
            versions.AddRow(Markup.Escape(v.Baseline ? version + " (first download)" : version), v.Rows.ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(versions);
    }

    private static void WriteCategoryTable(IReadOnlyList<GreenStatusCount> counts) {
        var table = new Table().Border(TableBorder.Rounded)
            .AddColumn("Species recovery category")
            .AddColumn(new TableColumn("Assessments").RightAligned());
        foreach (var c in counts) {
            table.AddRow(Markup.Escape(c.Value ?? "(none)"), c.Rows.ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(table);
    }

    private static string Stamp(DateTime? utc) => utc is { } at ? IucnRefreshMath.Stamp(at) : "unknown";
}

internal enum GreenStatusRefusal {
    None,
    VersionRequestFailed,
    VersionUnreadable,
    DownloadFailed,
    NotJson,
    NoList,
    Empty,
    NoTaxonId,
    NoDate,
}

/// <summary>
/// What one run did: the version and records it stored (Saved), or why it stored nothing
/// (Refusal, with the request error, or the 1-based position of the record without a key and
/// the number of records in the download).
/// </summary>
internal sealed record GreenStatusRunOutcome {
    public string? RedListVersion { get; init; }
    public IReadOnlyList<IucnGreenStatusRecord> Records { get; init; } = Array.Empty<IucnGreenStatusRecord>();
    public int DuplicateKeys { get; init; }
    public GreenStatusSaveResult? Saved { get; init; }
    public GreenStatusRefusal Refusal { get; init; }
    public string? Detail { get; init; }
    public int Index { get; init; }
    public int Total { get; init; }
}

/// <summary>
/// The run without the console: two requests (each recorded in http_request_log), then one
/// transaction, or nothing written to green_status when either request fails or the answer
/// cannot be used.
/// </summary>
internal static class IucnGreenStatusRun {
    public static async Task<GreenStatusRunOutcome> RunAsync(IucnApiCacheStore store, IucnApiClient client, DateTime nowUtc,
        CancellationToken cancellationToken) {
        var versionAnswer = await RequestAsync(store, IucnApiClient.RedListVersionPath, client.GetRedListVersionAsync, cancellationToken).ConfigureAwait(false);
        if (versionAnswer.Error is { } versionError) {
            return new GreenStatusRunOutcome { Refusal = GreenStatusRefusal.VersionRequestFailed, Detail = versionError };
        }
        if (IucnRedListVersion.Parse(versionAnswer.Body!) is not { } version) {
            return new GreenStatusRunOutcome { Refusal = GreenStatusRefusal.VersionUnreadable, Detail = Shorten(versionAnswer.Body!) };
        }

        var download = await RequestAsync(store, IucnApiClient.GreenStatusAllPath, client.GetGreenStatusAllAsync, cancellationToken).ConfigureAwait(false);
        if (download.Error is { } downloadError) {
            return new GreenStatusRunOutcome { RedListVersion = version, Refusal = GreenStatusRefusal.DownloadFailed, Detail = downloadError };
        }

        var answer = IucnGreenStatusParser.Parse(download.Body!);
        if (!answer.Usable) {
            return new GreenStatusRunOutcome {
                RedListVersion = version,
                Refusal = answer.Problem switch {
                    IucnGreenStatusProblem.NotJson => GreenStatusRefusal.NotJson,
                    IucnGreenStatusProblem.NoList => GreenStatusRefusal.NoList,
                    IucnGreenStatusProblem.Empty => GreenStatusRefusal.Empty,
                    IucnGreenStatusProblem.NoTaxonId => GreenStatusRefusal.NoTaxonId,
                    _ => GreenStatusRefusal.NoDate,
                },
                Detail = answer.Detail,
                Index = answer.Index,
                Total = answer.Total,
            };
        }

        var saved = store.SaveGreenStatus(answer.Records, version, nowUtc);
        return new GreenStatusRunOutcome {
            RedListVersion = version,
            Records = answer.Records,
            DuplicateKeys = answer.DuplicateKeys,
            Saved = saved,
        };
    }

    private sealed record Answer(string? Body, string? Error);

    // One request, logged in http_request_log like the other iucn api downloads. The client has
    // already retried rate limits and server errors; what reaches here is the final failure.
    private static async Task<Answer> RequestAsync(IucnApiCacheStore store, string url,
        Func<CancellationToken, Task<IucnApiResponse>> send, CancellationToken cancellationToken) {
        var importId = store.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();
        try {
            var response = await send(cancellationToken).ConfigureAwait(false);
            store.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return new Answer(response.Body, null);
        } catch (IucnApiException ex) {
            store.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            return new Answer(null, ex.StatusCode is { } status ? $"HTTP {(int)status} {status}" : ex.InnerException?.Message ?? ex.Message);
        } catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested) {
            store.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            return new Answer(null, ex.Message);
        }
    }

    private static string Shorten(string body) {
        var text = body.Trim();
        return text.Length <= 200 ? text : text[..200] + "...";
    }
}
