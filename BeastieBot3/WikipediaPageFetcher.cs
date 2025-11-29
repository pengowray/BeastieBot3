using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BeastieBot3;

internal sealed class WikipediaPageFetcher {
    private readonly WikipediaCacheStore _cache;
    private readonly WikipediaApiClient _client;

    public WikipediaPageFetcher(WikipediaCacheStore cache, WikipediaApiClient client) {
        _cache = cache;
        _client = client;
    }

    public async Task<WikipediaFetchOutcome> FetchAsync(WikiPageWorkItem workItem, CancellationToken cancellationToken) {
        WikipediaQueryResult queryResult;
        try {
            queryResult = await _client.QueryPageAsync(workItem.PageTitle, cancellationToken).ConfigureAwait(false);
        }
        catch (WikipediaApiException ex) {
            _cache.RecordPageFailure(workItem.PageRowId, ex.Message, DateTime.UtcNow);
            return WikipediaFetchOutcome.CreateFailure(workItem.PageTitle, ex.Message);
        }

        if (!queryResult.Exists) {
            var normalized = WikipediaTitleHelper.Normalize(workItem.PageTitle);
            var missing = new WikiMissingTitle(workItem.PageTitle, normalized, queryResult.MissingReason ?? "missing", queryResult.MissingReason, DateTime.UtcNow);
            _cache.RecordMissingTitle(missing);
            _cache.MarkPageMissing(workItem.PageRowId, queryResult.MissingReason ?? "missing", DateTime.UtcNow);
            return WikipediaFetchOutcome.CreateMissing(workItem.PageTitle, queryResult.MissingReason);
        }

        var canonicalTitle = queryResult.CanonicalTitle ?? workItem.PageTitle;
        var importId = _cache.BeginImport($"enwiki:{canonicalTitle}");
        var importStarted = DateTime.UtcNow;
        WikipediaMobileHtmlResult htmlResult;
        try {
            htmlResult = await _client.GetMobileHtmlAsync(canonicalTitle, cancellationToken).ConfigureAwait(false);
        }
        catch (WikipediaApiException ex) {
            _cache.RecordPageFailure(workItem.PageRowId, ex.Message, DateTime.UtcNow);
            _cache.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, DateTime.UtcNow - importStarted);
            return WikipediaFetchOutcome.CreateFailure(workItem.PageTitle, ex.Message);
        }
        try {
            var hash = ComputeSha256(htmlResult.Html);
            var hasTaxobox = HasTaxobox(queryResult.Wikitext);
            var normalizedTitle = WikipediaTitleHelper.Normalize(canonicalTitle);
            var content = new WikiPageContent(
                workItem.PageRowId,
                queryResult.PageId,
                canonicalTitle,
                normalizedTitle,
                queryResult.RevisionId,
                queryResult.Redirects.Count > 0,
                queryResult.Redirects.Count > 0 ? canonicalTitle : null,
                queryResult.IsDisambiguation,
                queryResult.IsSetIndex,
                hasTaxobox,
                htmlResult.Html,
                hash,
                queryResult.Wikitext,
                importId,
                DateTime.UtcNow);

            _cache.SavePageContent(content);
            _cache.ReplaceCategories(workItem.PageRowId, queryResult.Categories);
            _cache.ReplaceRedirectChain(workItem.PageRowId, BuildRedirectEdges(queryResult.Redirects));
            var taxobox = TaxoboxParser.TryParse(workItem.PageRowId, queryResult.Wikitext);
            if (taxobox is not null) {
                _cache.UpsertTaxoboxData(taxobox);
            }
            else {
                _cache.DeleteTaxoboxData(workItem.PageRowId);
            }
            _cache.CompleteImportSuccess(importId, (int)htmlResult.StatusCode, htmlResult.PayloadBytes, DateTime.UtcNow - importStarted);
            return WikipediaFetchOutcome.CreateSuccess(workItem.PageTitle, canonicalTitle);
        }
        catch (Exception ex) {
            _cache.RecordPageFailure(workItem.PageRowId, ex.Message, DateTime.UtcNow);
            _cache.CompleteImportFailure(importId, ex.Message, null, DateTime.UtcNow - importStarted);
            return WikipediaFetchOutcome.CreateFailure(workItem.PageTitle, ex.Message);
        }
    }

    private static string? ComputeSha256(string? payload) {
        if (string.IsNullOrEmpty(payload)) {
            return null;
        }

        var bytes = Encoding.UTF8.GetBytes(payload);
        var hash = SHA256.HashData(bytes);
        var builder = new StringBuilder(hash.Length * 2);
        foreach (var b in hash) {
            builder.Append(b.ToString("x2"));
        }

        return builder.ToString();
    }

    private static bool HasTaxobox(string? wikitext) {
        if (string.IsNullOrWhiteSpace(wikitext)) {
            return false;
        }

        var text = wikitext.AsSpan();
        var templates = new[] { "{{taxobox", "{{speciesbox", "{{automatic taxobox", "{{insectbox", "{{subspeciesbox" };
        foreach (var template in templates) {
            if (text.IndexOf(template, StringComparison.OrdinalIgnoreCase) >= 0) {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<WikiRedirectEdge> BuildRedirectEdges(IReadOnlyList<WikipediaRedirectStep> steps) {
        if (steps.Count == 0) {
            return Array.Empty<WikiRedirectEdge>();
        }

        var edges = new List<WikiRedirectEdge>(steps.Count);
        var hop = 0L;
        foreach (var step in steps) {
            edges.Add(new WikiRedirectEdge(step.ToTitle, hop++, null));
        }

        return edges;
    }
}

internal sealed record WikipediaFetchOutcome(string RequestedTitle, string? FinalTitle, bool Success, bool Missing, string? Message) {
    public static WikipediaFetchOutcome CreateSuccess(string requested, string final) => new(requested, final, true, false, null);
    public static WikipediaFetchOutcome CreateMissing(string requested, string? reason) => new(requested, null, false, true, reason);
    public static WikipediaFetchOutcome CreateFailure(string requested, string? message) => new(requested, null, false, false, message);
}
