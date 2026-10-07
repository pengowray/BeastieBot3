using System.Diagnostics;
using BeastieBot3.Infrastructure;
using Spectre.Console;

// Downloads pages through the action API, several titles per request (wikitext, categories,
// redirects followed, no REST HTML), and saves each through WikipediaPageFetcher.SaveQueried like
// any other page. Used by `wikipedia fetch-group-titles` and `wikipedia fetch-species-lists`.

namespace BeastieBot3.Wikipedia;

internal sealed class BatchPageDownloader {
    /// After this many failed requests in a row, the run stops: Wikipedia is not answering.
    public const int MaxFailedInARow = 5;

    private readonly WikipediaCacheStore _cache;
    private readonly WikipediaApiClient _client;
    private readonly WikipediaPageFetcher _fetcher;
    private int _failedInARow;

    public BatchPageDownloader(WikipediaCacheStore cache, WikipediaApiClient client, WikipediaPageFetcher fetcher) {
        _cache = cache;
        _client = client;
        _fetcher = fetcher;
    }

    public long Requests { get; private set; }
    public long PagesSaved { get; private set; }
    public long PagesMissing { get; private set; }
    public long PagesFailed { get; private set; }

    /// Downloads the pages; returns how many titles it asked for.
    public async Task<int> FetchPagesAsync(string description, IReadOnlyList<string> titles, int titlesPerRequest, CancellationToken cancellationToken) {
        if (titles.Count == 0) {
            return 0;
        }
        await ProgressConsole.RunAsync(description, titles.Count, async progress => {
            foreach (var chunk in titles.Chunk(Math.Clamp(titlesPerRequest, 1, WikipediaApiClient.MaxTitlesPerRequest))) {
                cancellationToken.ThrowIfCancellationRequested();
                var now = DateTime.UtcNow;
                var items = chunk.Select(title => {
                    var row = _cache.UpsertPageCandidate(new WikiPageCandidate(title, title, PageId: null, now, now));
                    return new WikiPageWorkItem(row.PageRowId, title, title, WikiPageDownloadStatus.Pending, null, 0);
                }).ToList();
                var importId = _cache.BeginImport($"enwiki:batch of {chunk.Length} from {chunk[0]}");
                var watch = Stopwatch.StartNew();
                IReadOnlyDictionary<string, WikipediaQueryResult> results;
                try {
                    Requests++;
                    results = await _client.QueryPagesAsync(chunk, cancellationToken).ConfigureAwait(false);
                } catch (WikipediaApiException ex) {
                    foreach (var item in items) {
                        _cache.RecordPageFailure(item.PageRowId, ex.Message, DateTime.UtcNow);
                    }
                    _cache.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, watch.Elapsed);
                    PagesFailed += items.Count;
                    progress.Increment(chunk.Length);
                    if (++_failedInARow >= MaxFailedInARow) {
                        throw;
                    }
                    AnsiConsole.MarkupLineInterpolated($"[yellow]A request failed, carrying on: {ex.Message}[/]");
                    continue;
                }
                _failedInARow = 0;
                long bytes = 0;
                foreach (var item in items) {
                    if (!results.TryGetValue(item.PageTitle, out var result)) {
                        _cache.RecordPageFailure(item.PageRowId, "not in the batch answer", DateTime.UtcNow);
                        PagesFailed++;
                        continue;
                    }
                    bytes += result.PayloadBytes;
                    var outcome = _fetcher.SaveQueried(item, result, importId);
                    if (outcome.Success || outcome.Skipped) {
                        PagesSaved++;
                    } else if (outcome.Missing) {
                        PagesMissing++;
                    } else {
                        PagesFailed++;
                    }
                }
                _cache.CompleteImportSuccess(importId, 200, bytes, watch.Elapsed);
                progress.Increment(chunk.Length);
            }
        }, cancellationToken).ConfigureAwait(false);
        return titles.Count;
    }
}
