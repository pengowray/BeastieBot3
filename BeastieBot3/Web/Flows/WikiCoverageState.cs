using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.CommonNames;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

// How much Wikidata/Wikipedia work is outstanding right now, in the terms the workflow page
// orders it by: taxa nothing has ever looked for, pages a taxon is waiting on, the rest of the
// queue, failures, and what is merely old.
//
// Run history cannot answer any of that. `fetch-pages` finishing yesterday says nothing about
// whether 190,000 titles are still queued, and after a new IUCN release the interesting number
// is not "did it run" but "how many of the new taxa has nothing looked at yet".
//
// The counts that matter most span two databases (IUCN taxa vs the caches), which takes a second
// or so. The workflow page polls every ten seconds, so this is read from a snapshot refreshed in
// the background: a caller gets the last snapshot immediately, or a state that says it does not
// know yet, and never waits.

namespace BeastieBot3.Web.Flows;

public sealed record WikiCoverageState {
    /// False until the first background read finishes, or when a database is missing/unreadable.
    /// Probes must stay silent rather than report zero gaps they have not measured.
    public bool Known { get; init; }
    public DateTime? ReadAt { get; init; }
    /// Why the counts are missing, when a database was there but the read failed. Callers show it
    /// verbatim: guessing at the cause is how "a cache is missing or brand new" came to be printed
    /// for a year over three caches that were all present.
    public string? UnavailableReason { get; init; }

    public bool IucnExists { get; init; }
    public bool WikidataExists { get; init; }
    public bool WikipediaExists { get; init; }

    /// IUCN taxa excluding subpopulation/regional rows, which neither cache tries to match.
    public long IucnTaxa { get; init; }

    // --- Wikidata ---
    public long WikidataEntitiesCached { get; init; }
    public long WikidataEntitiesQueued { get; init; }     // seeded, JSON never downloaded
    public long WikidataEntitiesFailed { get; init; }
    public long WikidataBackfillMisses { get; init; }     // searched before, nothing found
    /// How far the Wikidata sweep has read, as a Q-number.
    public long WikidataSweepCursor { get; init; }
    /// IUCN taxa with neither a P627 link nor a queued backfill match.
    public long TaxaWithoutWikidata { get; init; }

    // --- Wikipedia ---
    public long PagesKnown { get; init; }                  // every title in the queue, any status
    public long PagesCached { get; init; }
    public long PagesQueued { get; init; }                // never downloaded
    public long PagesFailed { get; init; }
    public long PagesQueuedAwaited { get; init; }         // queued pages a taxon has no article without
    public long PagesMissing { get; init; }                // no English Wikipedia article under that title
    public long MissingTitles { get; init; }
    /// Taxa the matcher has never looked at (no row at all) - a new release's additions.
    public long TaxaNeverMatched { get; init; }
    /// Taxa with a candidate page chosen but not yet downloaded.
    public long TaxaAwaitingPage { get; init; }
    public long TaxaWithArticle { get; init; }
    /// Taxa the matcher looked at and found no article for.
    public long TaxaWithoutArticle { get; init; }
    /// Oldest cached page's download date - what a refresh pass would be working back from.
    public DateTime? OldestCachedPageAt { get; init; }

    // --- the enwiki all-titles dump (a cheap local existence check for queued titles) ---
    public long DumpTitles { get; init; }                  // 0 = no dump imported
    public string? DumpDate { get; init; }                 // the dump's Last-Modified date, yyyy-MM-dd
    /// Queued (pending/failed) titles the dump lists - an article or redirect exists.
    public long PagesQueuedInDump { get; init; }
    /// Queued titles absent from the dump - likely redlinks.
    public long PagesQueuedNotInDump { get; init; }
}

public static class WikiCoverageStateReader {
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);
    private static WikiCoverageState _snapshot = new();
    private static DateTime _snapshotAt = DateTime.MinValue;
    private static int _reading;
    private static bool _warned;

    /// Clears the snapshot so the next read measures again (used by tests and after a repoint).
    public static void Invalidate() {
        _snapshot = new WikiCoverageState();
        _snapshotAt = DateTime.MinValue;
    }

    /// Returns the last snapshot immediately and refreshes it in the background when stale.
    /// Never blocks: the first call returns a state that says it does not know yet.
    public static WikiCoverageState Read(PathsService paths) {
        if (DateTime.UtcNow - _snapshotAt >= Ttl) {
            WarmInBackground(paths);
        }
        return _snapshot;
    }

    /// Measures now, on the calling thread. For tests and one-shot callers.
    public static WikiCoverageState ReadNow(PathsService paths) {
        var state = Measure(paths);
        _snapshot = state;
        _snapshotAt = DateTime.UtcNow;
        return state;
    }

    private static void WarmInBackground(PathsService paths) {
        if (Interlocked.CompareExchange(ref _reading, 1, 0) != 0) return;
        _ = Task.Run(() => {
            try { ReadNow(paths); }
            catch { /* leave the previous snapshot in place */ }
            finally { Interlocked.Exchange(ref _reading, 0); }
        });
    }

    private static WikiCoverageState Measure(PathsService paths) {
        var iucn = TryPath(() => paths.GetIucnDatabasePath());
        var wikidata = TryPath(() => paths.GetWikidataCachePath());
        var wikipedia = TryPath(() => paths.GetWikipediaCachePath());

        var state = new WikiCoverageState {
            ReadAt = DateTime.UtcNow,
            IucnExists = Exists(iucn),
            WikidataExists = Exists(wikidata),
            WikipediaExists = Exists(wikipedia),
        };

        if (!state.IucnExists || !state.WikidataExists || !state.WikipediaExists) {
            return state;
        }

        try {
            // Opened through a file: URI so the two ATTACHed caches can carry ?mode=ro of their
            // own. A plain ATTACH would open them read-write, and this runs on a poll.
            // Pooling off deliberately. A pooled connection goes back to the pool with its ATTACHes
            // still in place, so the next measurement fails on "database wd is already in use" and
            // every count after the first in a process reads as unknown - which is exactly what a
            // long-lived `serve` does. This runs once a minute at most, so a fresh handle is cheap.
            var csb = new SqliteConnectionStringBuilder { DataSource = ReadOnlyUri(iucn!), Pooling = false };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            Attach(conn, "wd", wikidata!);
            Attach(conn, "wp", wikipedia!);

            // One eligibility rule for both caches: subpopulation/regional rows are not taxa
            // either matcher tries to place, so counting them would overstate every gap.
            const string Eligible = """
                SELECT taxonId FROM taxonomy_html
                WHERE subpopulationName IS NULL OR TRIM(subpopulationName) = ''
                """;

            // The all-titles dump tables arrived later than the rest of the schema, so an older
            // cache without them reads as "no dump imported", which is also true.
            var dumpTitles = CountOrZero(conn, "SELECT COUNT(*) FROM wp.enwiki_dump_titles");
            var queuedInDump = dumpTitles == 0 ? 0 : CountOrZero(conn, """
                SELECT COUNT(*) FROM wp.wiki_pages p
                WHERE p.download_status IN ('pending', 'failed')
                  AND EXISTS (SELECT 1 FROM wp.enwiki_dump_titles d WHERE d.title = p.normalized_title)
                """);
            var queuedTotal = dumpTitles == 0 ? 0 : CountOrZero(conn,
                "SELECT COUNT(*) FROM wp.wiki_pages WHERE download_status IN ('pending', 'failed')");

            return state with {
                Known = true,
                DumpTitles = dumpTitles,
                DumpDate = TextOrNull(conn, "SELECT value FROM wp.enwiki_dump_info WHERE key = 'dump_date'"),
                PagesQueuedInDump = queuedInDump,
                PagesQueuedNotInDump = Math.Max(0, queuedTotal - queuedInDump),
                IucnTaxa = Count(conn, $"SELECT COUNT(*) FROM ({Eligible})"),

                WikidataEntitiesCached = Count(conn, "SELECT COUNT(*) FROM wd.wikidata_entities WHERE json_downloaded = 1"),
                WikidataEntitiesQueued = Count(conn, "SELECT COUNT(*) FROM wd.wikidata_entities WHERE json_downloaded = 0"),
                WikidataEntitiesFailed = Count(conn, "SELECT COUNT(*) FROM wd.wikidata_entities WHERE json_downloaded = 0 AND attempt_count > 0 AND last_error IS NOT NULL"),
                // Written by `wikidata backfill-iucn`; absent from a cache last written before
                // it recorded searches, where "none recorded" is the right answer anyway.
                WikidataBackfillMisses = CountOrZero(conn, "SELECT COUNT(*) FROM wd.wikidata_backfill_misses"),
                WikidataSweepCursor = Count(conn, "SELECT CAST(IFNULL((SELECT value FROM wd.wikidata_sync_state WHERE key = 'wikidata_taxa_cursor'), '0') AS INTEGER)"),
                TaxaWithoutWikidata = Count(conn, $"""
                    SELECT COUNT(*) FROM ({Eligible}) t
                    WHERE NOT EXISTS (SELECT 1 FROM wd.wikidata_p627_values p WHERE p.value = CAST(t.taxonId AS TEXT))
                      AND NOT EXISTS (SELECT 1 FROM wd.wikidata_pending_iucn_matches m WHERE m.iucn_taxon_id = CAST(t.taxonId AS TEXT))
                    """),

                PagesKnown = Count(conn, "SELECT COUNT(*) FROM wp.wiki_pages"),
                PagesCached = Count(conn, "SELECT COUNT(*) FROM wp.wiki_pages WHERE download_status = 'cached'"),
                PagesMissing = Count(conn, "SELECT COUNT(*) FROM wp.wiki_pages WHERE download_status = 'missing'"),
                PagesQueued = Count(conn, "SELECT COUNT(*) FROM wp.wiki_pages WHERE download_status = 'pending'"),
                PagesFailed = Count(conn, "SELECT COUNT(*) FROM wp.wiki_pages WHERE download_status = 'failed'"),
                PagesQueuedAwaited = Count(conn, """
                    SELECT COUNT(*) FROM wp.wiki_pages p
                    WHERE p.download_status = 'pending'
                      AND EXISTS (SELECT 1 FROM wp.taxon_wiki_matches m
                                  WHERE m.page_row_id = p.id AND m.match_status = 'pending')
                    """),
                MissingTitles = Count(conn, "SELECT COUNT(*) FROM wp.wiki_missing_titles"),
                TaxaAwaitingPage = Count(conn, "SELECT COUNT(*) FROM wp.taxon_wiki_matches WHERE match_status = 'pending'"),
                TaxaWithArticle = Count(conn, "SELECT COUNT(*) FROM wp.taxon_wiki_matches WHERE match_status = 'matched'"),
                TaxaWithoutArticle = Count(conn, "SELECT COUNT(*) FROM wp.taxon_wiki_matches WHERE match_status = 'missing'"),
                TaxaNeverMatched = Count(conn, $"""
                    SELECT COUNT(*) FROM ({Eligible}) t
                    WHERE NOT EXISTS (SELECT 1 FROM wp.taxon_wiki_matches m
                                      WHERE m.taxon_source = 'iucn' AND m.taxon_identifier = CAST(t.taxonId AS TEXT))
                    """),
                OldestCachedPageAt = Stamp(conn, "SELECT MIN(downloaded_at) FROM wp.wiki_pages WHERE download_status = 'cached'"),
            };
        } catch (Exception ex) {
            // An older cache without one of these tables leaves every step on its usual status
            // rather than claiming there is no work outstanding. Said once, because a probe that
            // silently reports nothing is hard to tell from a probe with nothing to report.
            if (!_warned) {
                _warned = true;
                Console.Error.WriteLine($"Workflow coverage counts unavailable: {ex.Message}");
            }
            return state with { UnavailableReason = ex.Message };
        }
    }

    private static void Attach(SqliteConnection conn, string alias, string path) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"ATTACH DATABASE @path AS {alias}";
        cmd.Parameters.AddWithValue("@path", ReadOnlyUri(path));
        cmd.ExecuteNonQuery();
    }

    private static string ReadOnlyUri(string path) => new Uri(path).AbsoluteUri + "?mode=ro";

    private static string? TryPath(Func<string?> resolve) {
        try {
            var value = resolve();
            return string.IsNullOrWhiteSpace(value) ? null : Path.GetFullPath(value);
        } catch {
            return null;
        }
    }

    private static bool Exists(string? path) => path is not null && File.Exists(path);

    private static long Count(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 30;
        return cmd.ExecuteScalar() is long n ? n : 0;
    }

    private static long CountOrZero(SqliteConnection conn, string sql) {
        try { return Count(conn, sql); } catch { return 0; }
    }

    private static string? TextOrNull(SqliteConnection conn, string sql) {
        try {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 30;
            var value = cmd.ExecuteScalar() as string;
            return string.IsNullOrWhiteSpace(value) ? null : value;
        } catch { return null; }
    }

    private static DateTime? Stamp(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 30;
        return cmd.ExecuteScalar() is string s
            ? CommonNameHubStateReader.ParseStoredUtc(s)
            : null;
    }
}
