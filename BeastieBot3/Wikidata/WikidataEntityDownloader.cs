using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;

// Downloads single Wikidata entity via WikidataApiClient.GetEntityAsync().
// Records import metadata (timing, status) via ApiImportMetadataStore.
// Called in a loop by WikidataCacheItemsCommand for each pending Q-ID.
// Stores raw JSON response in WikidataCacheStore.wikidata_items table.

namespace BeastieBot3.Wikidata;

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
            AnsiConsole.MarkupLineInterpolated($"[red]Failed to download {item.EntityId}: {ex.Message}[/]");
            return false;
        }
        // Ctrl+C, or Cancel on a web UI job. The catch-all below used to take it, so the item in
        // flight was recorded as a failed download (and joined the --failed-only queue) before
        // the run stopped. The item stays in the queue as it was. The token check keeps an
        // HttpClient timeout, which also arrives as an OperationCanceledException, counted as a
        // failure.
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            throw;
        }
        catch (Exception ex) {
            store.RecordFailure(item.NumericId, ex.Message);
            store.CompleteImportFailure(importId, ex.Message, null, stopwatch.Elapsed);
            AnsiConsole.MarkupLineInterpolated($"[red]Unexpected error downloading {item.EntityId}: {ex.Message}[/]");
            return false;
        }
    }
}
