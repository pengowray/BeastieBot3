using System;
using System.Collections.Generic;
using System.Globalization;

// The line cache-taxa and cache-assessments print before downloading: how many queued items will
// be downloaded and why the others are skipped. Both commands decide what is due before the
// progress bar starts, so the bar's total and time estimate count downloads only. Their queues
// hold every CSV species or every listed assessment, and outside a refresh almost all of them are
// skipped: a run that made 1,502 requests in 44 minutes once showed "~73:10:06 left".

namespace BeastieBot3.Iucn;

internal static class IucnDownloadQueueSummary {
    // e.g. "Taxa in the queue: 186,627. To download: 1,522. Downloaded after 2026-08-14 22:56 UTC:
    // 185,104. Not found (HTTP 404) on an earlier run: 1."  Zero skip counts are left out.
    // notFoundAfterCutoff: the 404s skipped are only those recorded after the cutoff (the
    // --retry-tombstones re-check asks again about the ones recorded before it).
    public static string Describe(string itemsLabel, int queued, int toDownload, int upToDate, int notFoundEarlier, DateTime? cutoff,
        bool notFoundAfterCutoff = false) {
        var parts = new List<string> {
            $"{itemsLabel} in the queue: {N(queued)}.",
            $"To download: {N(toDownload)}.",
        };
        if (upToDate > 0) {
            parts.Add(cutoff is { } at
                ? $"Downloaded after {IucnRefreshMath.Stamp(at)}: {N(upToDate)}."
                : $"Already cached: {N(upToDate)}.");
        }
        if (notFoundEarlier > 0) {
            parts.Add(notFoundAfterCutoff && cutoff is { } since
                ? $"Not found (HTTP 404) after {IucnRefreshMath.Stamp(since)}: {N(notFoundEarlier)}."
                : $"Not found (HTTP 404) on an earlier run: {N(notFoundEarlier)}.");
        }
        return string.Join(" ", parts);
    }

    private static string N(int value) => value.ToString("N0", CultureInfo.InvariantCulture);
}
