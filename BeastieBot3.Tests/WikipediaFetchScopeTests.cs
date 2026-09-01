using System;
using System.Linq;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins which queued pages a fetch run picks up, and in what order. The queue holds far more
// titles than any one session can download, so this is the whole point of the priority steps:
// "the pages a taxon is waiting on", "the newest titles", "only the failures", "only old copies".
public class WikipediaFetchScopeTests {
    private static readonly DateTime Now = new(2026, 8, 30, 12, 0, 0, DateTimeKind.Utc);

    private sealed class Fixture : IDisposable {
        public readonly SqliteConnection Connection;
        public readonly WikipediaCacheStore Store;

        public Fixture() {
            Connection = new SqliteConnection("Data Source=:memory:");
            Connection.Open();
            Store = WikipediaCacheStore.OpenFromConnection(Connection);
        }

        public long AddPage(string title, string status, DateTime discovered, DateTime? downloaded = null) {
            using var cmd = Connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO wiki_pages(page_title, normalized_title, discovered_at, last_seen_at,
                                       download_status, downloaded_at)
                VALUES (@t, @t, @d, @d, @s, @dl);
                SELECT last_insert_rowid();
                """;
            cmd.Parameters.AddWithValue("@t", title);
            cmd.Parameters.AddWithValue("@d", discovered.ToString("O"));
            cmd.Parameters.AddWithValue("@s", status);
            cmd.Parameters.AddWithValue("@dl", downloaded?.ToString("O") ?? (object)DBNull.Value);
            return (long)cmd.ExecuteScalar()!;
        }

        public void AwaitPage(long pageRowId, string taxonId, string matchStatus = "pending") {
            using var cmd = Connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO taxon_wiki_matches(taxon_source, taxon_identifier, match_status, page_row_id, matched_at)
                VALUES ('iucn', @id, @st, @page, @at)
                """;
            cmd.Parameters.AddWithValue("@id", taxonId);
            cmd.Parameters.AddWithValue("@st", matchStatus);
            cmd.Parameters.AddWithValue("@page", pageRowId);
            cmd.Parameters.AddWithValue("@at", Now.ToString("O"));
            cmd.ExecuteNonQuery();
        }

        public void Dispose() {
            Store.Dispose();
            Connection.Dispose();
        }
    }

    private static string[] Titles(WikipediaCacheStore store, WikipediaCacheStore.WikiFetchScope scope, int limit = 50) {
        var items = store.GetPendingPages(limit, scope);
        var titles = new string[items.Count];
        for (var i = 0; i < items.Count; i++) titles[i] = items[i].PageTitle;
        return titles;
    }

    [Fact]
    public void Awaited_only_takes_the_pages_a_taxon_has_no_article_without() {
        using var f = new Fixture();
        var wanted = f.AddPage("Ursus maritimus", "pending", Now);
        f.AddPage("Carnivora", "pending", Now);            // a higher taxon nothing is blocked on
        var settled = f.AddPage("Panthera leo", "pending", Now);
        f.AwaitPage(wanted, "1000");
        f.AwaitPage(settled, "1001", matchStatus: "matched");

        Assert.Equal(new[] { "Ursus maritimus" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { AwaitedOnly = true }));
        Assert.Equal(1, f.Store.CountPendingPages(new WikipediaCacheStore.WikiFetchScope { AwaitedOnly = true }));
        Assert.Equal(3, f.Store.CountPendingPages(WikipediaCacheStore.WikiFetchScope.All));
    }

    [Fact]
    public void The_default_order_is_oldest_queued_first() {
        using var f = new Fixture();
        f.AddPage("Queued in June", "pending", Now.AddDays(-60));
        f.AddPage("Queued today", "pending", Now);

        Assert.Equal(new[] { "Queued in June", "Queued today" },
            Titles(f.Store, WikipediaCacheStore.WikiFetchScope.All));
    }

    [Fact]
    public void Newest_first_takes_a_new_releases_titles_before_the_older_backlog() {
        using var f = new Fixture();
        f.AddPage("Queued in June", "pending", Now.AddDays(-60));
        f.AddPage("Queued today", "pending", Now);

        Assert.Equal(new[] { "Queued today", "Queued in June" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { NewestFirst = true }));
    }

    // A page that has just failed is also the newest. If the order did not put never-tried pages
    // first, that page would come back at the front of every batch and be retried without end.
    [Fact]
    public void Newest_first_still_tries_never_tried_pages_before_failures() {
        using var f = new Fixture();
        f.AddPage("Failed just now", "failed", Now);
        f.AddPage("Queued yesterday", "pending", Now.AddDays(-1));

        Assert.Equal(new[] { "Queued yesterday", "Failed just now" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { NewestFirst = true }));
    }

    [Fact]
    public void Failed_only_skips_the_whole_queue_and_takes_the_failures() {
        using var f = new Fixture();
        f.AddPage("Queued", "pending", Now);
        f.AddPage("Failed", "failed", Now);
        f.AddPage("No article", "missing", Now);

        Assert.Equal(new[] { "Failed" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { FailedOnly = true }));
    }

    // prune-queue tells the user a taxon waiting on a removed title keeps its place. That rests on
    // the match row surviving the delete with its page cleared, which only happens with foreign
    // keys on; without it the delete fails and the promise is false.
    [Fact]
    public void Removing_a_queued_title_leaves_the_taxon_waiting_on_it_in_place() {
        using var f = new Fixture();
        var page = f.AddPage("Eumeces schneideri (Daudin, 1802) [orth. error]", "pending", Now);
        f.AwaitPage(page, "1234");

        Assert.Equal(1, f.Store.DeletePages(new[] { page }));

        using var cmd = f.Connection.CreateCommand();
        cmd.CommandText = "SELECT match_status, page_row_id FROM taxon_wiki_matches WHERE taxon_identifier = '1234'";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("pending", reader.GetString(0));
        Assert.True(reader.IsDBNull(1));       // the page is gone, the taxon is still waiting
    }

    [Fact]
    public void Only_queued_titles_are_read_for_pruning() {
        using var f = new Fixture();
        f.AddPage("Queued", "pending", Now);
        f.AddPage("Failed", "failed", Now);
        f.AddPage("Cached", "cached", Now, downloaded: Now);
        f.AddPage("No article", "missing", Now);

        var titles = f.Store.ReadQueuedTitles().Select(t => t.Title).OrderBy(t => t).ToArray();
        Assert.Equal(new[] { "Failed", "Queued" }, titles);
    }

    // Without --refresh-only, a refresh run drags the entire never-fetched queue along with it,
    // which is the opposite of what a "leave the new work alone" pass is for.
    [Fact]
    public void Refresh_only_re_downloads_cached_pages_and_leaves_the_queue_alone() {
        using var f = new Fixture();
        f.AddPage("Queued", "pending", Now);
        f.AddPage("Cached last year", "cached", Now.AddDays(-400), downloaded: Now.AddDays(-400));
        f.AddPage("Cached today", "cached", Now, downloaded: Now);

        var threshold = Now.AddDays(-365);
        Assert.Equal(new[] { "Cached last year" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { RefreshOnly = true, RefreshThreshold = threshold }));

        // The same threshold without --refresh-only picks up the queue as well.
        Assert.Equal(new[] { "Queued", "Cached last year" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { RefreshThreshold = threshold }));
    }

    // --exists-first: titles the all-titles dump lists come before likely redlinks, but never
    // before the status precedence (a failed page in the dump must not jump ahead of a
    // never-tried redlink and loop).
    [Fact]
    public void Exists_first_downloads_dump_listed_titles_before_likely_redlinks() {
        using var f = new Fixture();
        f.AddPage("Fakeus notrealis", "pending", Now.AddDays(-2));      // queued first, but a redlink
        f.AddPage("Ursus maritimus", "pending", Now);
        f.AddPage("Panthera leo", "failed", Now.AddDays(-1));
        f.Store.AddDumpTitles(new[] { "Ursus maritimus", "Panthera leo" });

        Assert.Equal(new[] { "Ursus maritimus", "Fakeus notrealis", "Panthera leo" },
            Titles(f.Store, new WikipediaCacheStore.WikiFetchScope { KnownTitlesFirst = true }));
    }

    [Fact]
    public void Dump_import_is_recorded_and_the_queue_split_counted() {
        using var f = new Fixture();
        f.AddPage("Ursus maritimus", "pending", Now);
        f.AddPage("Fakeus notrealis", "pending", Now);
        f.AddPage("Cached page", "cached", Now, downloaded: Now);       // not queued, not counted

        Assert.Equal(2, f.Store.AddDumpTitles(new[] { "Ursus maritimus", "Cached page" }));
        Assert.Equal(0, f.Store.AddDumpTitles(new[] { "Ursus maritimus" }));   // duplicate ignored
        Assert.Equal(2, f.Store.CountDumpTitles());
        Assert.Equal((1L, 1L), f.Store.CountQueuedAgainstDump());

        f.Store.RecordDumpImport(new EnwikiDumpInfo("2026-08-20", Now, 2, "test", Partial: false));
        var info = f.Store.GetDumpInfo();
        Assert.NotNull(info);
        Assert.Equal("2026-08-20", info!.DumpDate);
        Assert.Equal(2, info.TitleCount);
        Assert.False(info.Partial);

        // A fresh import clears the titles and the record, but keeps download bookkeeping.
        f.Store.SetDumpInfoValue("download_last_modified", "2026-08-20");
        f.Store.ClearDumpTitles();
        Assert.Equal(0, f.Store.CountDumpTitles());
        Assert.Null(f.Store.GetDumpInfo());
        Assert.Equal("2026-08-20", f.Store.GetDumpInfoValue("download_last_modified"));
    }
}
