using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;

namespace BeastieBot3;

internal static class WikidataEntityDownloader {
    public static async Task<bool> DownloadSingleAsync(WikidataApiClient client, WikidataCacheStore store, WikidataEntityWorkItem item, CancellationToken cancellationToken) {
        var url = $"wbgetentities?ids={item.EntityId}";
        var importId = store.BeginImport(url);
        var stopwatch = Stopwatch.StartNew();

        try {
            var response = await client.GetEntityAsync(item.EntityId, cancellationToken).ConfigureAwait(false);
            var record = WikidataEntityParser.Parse(response.Body);
            store.RecordSuccess(record, importId, response.Body, DateTime.UtcNow);
            store.CompleteImportSuccess(importId, (int)response.StatusCode, response.PayloadBytes, stopwatch.Elapsed);
            return true;
        }
        catch (WikidataApiException ex) {
            store.RecordFailure(item.NumericId, ex.Message);
            store.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download {item.EntityId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
        catch (Exception ex) {
            store.RecordFailure(item.NumericId, ex.Message);
            store.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error downloading {item.EntityId}: {Markup.Escape(ex.Message)}[/]");
            return false;
        }
    }
}
