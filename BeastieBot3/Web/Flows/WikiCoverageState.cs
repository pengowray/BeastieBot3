using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Wikidata;
using BeastieBot3.Wikipedia;
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

    /// IUCN taxa excluding subpopulation/regional rows and varieties, which neither cache tries to match.
    public long IucnTaxa { get; init; }

    // --- Wikidata ---
    public long WikidataEntitiesCached { get; init; }
    public long WikidataEntitiesQueued { get; init; }     // seeded, JSON never downloaded
    public long WikidataEntitiesFailed { get; init; }
    public long WikidataBackfillMisses { get; init; }     // searched before, nothing found
    /// How far `wikidata seed-taxa` has read items with an IUCN taxon id (P627), as a Q-number.
    /// 0 = not started.
    public long WikidataSweepCursorP627 { get; init; }
    /// How far `wikidata seed-taxa` has read items with an IUCN conservation status (P141).
    public long WikidataSweepCursorP141 { get; init; }
    /// IUCN taxa with neither a P627 link nor a queued backfill match.
    public long TaxaWithoutWikidata { get; init; }
    /// Of those, taxa `wikidata backfill-iucn` has no verdict for yet. Counted directly rather than
    /// as TaxaWithoutWikidata minus the misses table, which also holds taxa a later release dropped.
    public long TaxaNeverSearched { get; init; }

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
    /// Taxa whose only candidate pages were disambiguation or set-index pages.
    public long TaxaRejected { get; init; }
    /// Varieties: in IUCN, but neither matcher nor backfill tries to place them, so they are left
    /// out of IucnTaxa and every gap. Counted so the totals can say where they went.
    public long VarietiesSkipped { get; init; }
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

            // One eligibility rule for both caches: subpopulation/regional rows and varieties are
            // not taxa either matcher tries to place (WikipediaMatchTaxaCommand.ShouldSkip,
            // WikidataIucnBackfillCommand.IsEligible), so counting them overstates every gap. It
            // did: 980 varieties sat in "never checked" for good, and the match step ran on every
            // update to check them. 'variety' is the only such infraType IUCN uses (2026-1).
            const string Eligible = """
                SELECT taxonId FROM taxonomy_html
                WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '')
                  AND IFNULL(infraType, '') <> 'variety'
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

            // Restricted to taxa in this release: rows for taxa a later release dropped are never
            // re-evaluated, so a leftover 'pending' row would hold the settle step open forever.
            var byStatus = CountMatchStatuses(conn, Eligible);
            var pages = CountPageStatuses(conn);

            var withoutWikidata = $"""
                SELECT COUNT(*) FROM ({Eligible}) t
                WHERE NOT EXISTS (SELECT 1 FROM wd.wikidata_p627_values p WHERE p.value = CAST(t.taxonId AS TEXT))
                  AND NOT EXISTS (SELECT 1 FROM wd.wikidata_pending_iucn_matches m WHERE m.iucn_taxon_id = CAST(t.taxonId AS TEXT))
                """;
            var taxaWithoutWikidata = Count(conn, withoutWikidata);
            long taxaNeverSearched;
            try {
                taxaNeverSearched = Count(conn, withoutWikidata +
                    "\n  AND NOT EXISTS (SELECT 1 FROM wd.wikidata_backfill_misses x WHERE x.iucn_taxon_id = CAST(t.taxonId AS TEXT))");
            } catch (SqliteException) {
                // A cache from before backfill-iucn recorded its searches: none recorded.
                taxaNeverSearched = taxaWithoutWikidata;
            }

            return state with {
                Known = true,
                VarietiesSkipped = CountOrZero(conn, """
                    SELECT COUNT(*) FROM taxonomy_html
                    WHERE (subpopulationName IS NULL OR TRIM(subpopulationName) = '') AND infraType = 'variety'
                    """),
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
                WikidataSweepCursorP627 = SweepCursor(conn, WikidataSeedProperty.IucnTaxonId),
                WikidataSweepCursorP141 = SweepCursor(conn, WikidataSeedProperty.ConservationStatus),
                TaxaWithoutWikidata = taxaWithoutWikidata,
                TaxaNeverSearched = taxaNeverSearched,

                PagesKnown = pages.Total,
                PagesCached = pages.Cached,
                PagesMissing = pages.Missing,
                PagesQueued = pages.Pending,
                PagesFailed = pages.Failed,
                PagesQueuedAwaited = Count(conn, $"""
                    SELECT COUNT(*) FROM wp.wiki_pages p
                    WHERE p.download_status = 'pending'
                      AND {WikipediaCacheStore.AwaitedPagePredicate("wp.", "p.id")}
                    """),
                MissingTitles = Count(conn, "SELECT COUNT(*) FROM wp.wiki_missing_titles"),
                TaxaAwaitingPage = byStatus.Pending,
                TaxaWithArticle = byStatus.Matched,
                TaxaWithoutArticle = byStatus.Missing,
                TaxaRejected = byStatus.Rejected,
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

    // The cursor `wikidata seed-taxa` wrote before it swept one property per pass. Private in
    // WikidataSeedCommand, so repeated here.
    private const string LegacySweepCursorKey = "wikidata_taxa_cursor";

    // Same rule as WikidataSeedCommand.ReadCursor: a pass's own cursor once it has one, otherwise
    // the combined cursor it starts from. Reading only the combined key showed a Q-number frozen at
    // the split, and 0 on a cache created after it.
    private static long SweepCursor(SqliteConnection conn, WikidataSeedProperty property) {
        var key = WikidataSeedCommand.Passes.First(p => p.Property == property).CursorKey;
        var own = SyncCursor(conn, key);
        return own > 0 ? own : SyncCursor(conn, LegacySweepCursorKey);
    }

    private static long SyncCursor(SqliteConnection conn, string key) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT value FROM wd.wikidata_sync_state WHERE key = @key";
        cmd.Parameters.AddWithValue("@key", key);
        cmd.CommandTimeout = 30;
        return long.TryParse(cmd.ExecuteScalar() as string, out var n) ? n : 0;
    }

    private static (long Matched, long Missing, long Pending, long Rejected) CountMatchStatuses(SqliteConnection conn, string eligible) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT m.match_status, COUNT(*) FROM ({eligible}) t
            JOIN wp.taxon_wiki_matches m
              ON m.taxon_source = 'iucn' AND m.taxon_identifier = CAST(t.taxonId AS TEXT)
            GROUP BY m.match_status
            """;
        cmd.CommandTimeout = 30;
        long matched = 0, missing = 0, pending = 0, rejected = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var n = reader.GetInt64(1);
            switch (reader.GetString(0)) {
                case "matched": matched = n; break;
                case "missing": missing = n; break;
                case "pending": pending = n; break;
                case "rejected": rejected = n; break;
            }
        }
        return (matched, missing, pending, rejected);
    }

    // One statement, so the parts and the total come from the same moment and add up even while
    // fetch-pages is moving titles from pending to cached. Five separate counts drifted apart then.
    private static (long Total, long Cached, long Pending, long Failed, long Missing) CountPageStatuses(SqliteConnection conn) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT download_status, COUNT(*) FROM wp.wiki_pages GROUP BY download_status";
        cmd.CommandTimeout = 30;
        long total = 0, cached = 0, pending = 0, failed = 0, missing = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var n = reader.GetInt64(1);
            total += n;
            switch (reader.IsDBNull(0) ? null : reader.GetString(0)) {
                case WikiPageDownloadStatus.Cached: cached = n; break;
                case WikiPageDownloadStatus.Pending: pending = n; break;
                case WikiPageDownloadStatus.Failed: failed = n; break;
                case WikiPageDownloadStatus.Missing: missing = n; break;
            }
        }
        return (total, cached, pending, failed, missing);
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
            ? StoredUtc.Parse(s)
            : null;
    }
}
