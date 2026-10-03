using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;

// SQLite store for English Wikipedia pages (Datastore:enwiki_cache_sqlite).
// Schema: pages (title PK, page_id, html, wikitext, content_hash), page_queue
// (titles to fetch), redirects (from_title→to_title), taxobox_data (parsed
// wikitext), taxon_matches (sis_id→page_title). Populated by WikipediaFetchCommand,
// matched by WikipediaMatchTaxaCommand. TaxoboxParser extracts scientific names.

namespace BeastieBot3.Wikipedia;

internal sealed class WikipediaCacheStore : HttpCacheSqliteStore {
    private bool? _hasTitleList;

    private WikipediaCacheStore(SqliteConnection connection) : base(connection) {
    }

    public static WikipediaCacheStore Open(string databasePath) {
        var connection = OpenConnection(databasePath);
        var store = new WikipediaCacheStore(connection);
        store.EnsureImportSchema();
        store.EnsureSchema();
        return store;
    }

    /// <summary>
    /// Opens an existing cache for reading only: no folder is created, no WAL pragma is set and no
    /// schema work is done, so a reader such as `wikipedia generate-lists` cannot change a cache
    /// that a download may be writing. Any write through it fails. Null when the file does not
    /// exist or cannot be opened.
    /// </summary>
    internal static WikipediaCacheStore? OpenReadOnly(string databasePath) {
        if (!File.Exists(databasePath)) {
            return null;
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        try {
            connection.Open();
        } catch (SqliteException) {
            connection.Dispose();
            return null;
        }
        return new WikipediaCacheStore(connection);
    }

    /// Test seam: build the schema on a connection the caller owns (see SqliteStore).
    internal static WikipediaCacheStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new WikipediaCacheStore(connection);
        store.EnsureImportSchema();
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
CREATE TABLE IF NOT EXISTS wiki_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id INTEGER,
    page_title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    download_status TEXT NOT NULL DEFAULT 'pending',
    latest_revision_id INTEGER,
    import_id INTEGER REFERENCES http_request_log(id) ON DELETE SET NULL,
    downloaded_at TEXT,
    html_main TEXT,
    wikitext TEXT,
    html_sha256 TEXT,
    html_bytes INTEGER,
    wikitext_bytes INTEGER,
    is_redirect INTEGER NOT NULL DEFAULT 0,
    redirect_target TEXT,
    is_disambiguation INTEGER NOT NULL DEFAULT 0,
    is_set_index INTEGER NOT NULL DEFAULT 0,
    has_taxobox INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    UNIQUE(normalized_title)
);
CREATE INDEX IF NOT EXISTS idx_wiki_pages_title ON wiki_pages(page_title);
CREATE INDEX IF NOT EXISTS idx_wiki_pages_status ON wiki_pages(download_status, last_seen_at);
CREATE TABLE IF NOT EXISTS wiki_page_categories (
    page_row_id INTEGER NOT NULL REFERENCES wiki_pages(id) ON DELETE CASCADE,
    category_name TEXT NOT NULL,
    PRIMARY KEY(page_row_id, category_name)
);
CREATE INDEX IF NOT EXISTS idx_wiki_page_categories_name ON wiki_page_categories(category_name);
-- Every column that references wiki_pages(id) is indexed: a DELETE on wiki_pages has to find
-- the referencing rows for each cascade / set-null, and without these it scanned the whole
-- categories table (1.1M rows) per page, which turned pruning 70,000 titles into hours.
CREATE INDEX IF NOT EXISTS idx_wiki_page_categories_page ON wiki_page_categories(page_row_id);
CREATE TABLE IF NOT EXISTS wiki_redirect_edges (
    page_row_id INTEGER NOT NULL REFERENCES wiki_pages(id) ON DELETE CASCADE,
    hop INTEGER NOT NULL,
    target_title TEXT NOT NULL,
    PRIMARY KEY(page_row_id, hop)
);
CREATE TABLE IF NOT EXISTS wiki_taxobox_data (
    page_row_id INTEGER PRIMARY KEY REFERENCES wiki_pages(id) ON DELETE CASCADE,
    scientific_name TEXT,
    rank TEXT,
    kingdom TEXT,
    phylum TEXT,
    class_name TEXT,
    order_name TEXT,
    family TEXT,
    subfamily TEXT,
    tribe TEXT,
    genus TEXT,
    species TEXT,
    is_monotypic INTEGER,
    data_json TEXT
);
CREATE TABLE IF NOT EXISTS wiki_missing_titles (
    normalized_title TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    latest_reason TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    last_attempt_at TEXT NOT NULL,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS taxon_wiki_matches (
    taxon_source TEXT NOT NULL,
    taxon_identifier TEXT NOT NULL,
    match_status TEXT NOT NULL,
    page_row_id INTEGER REFERENCES wiki_pages(id) ON DELETE SET NULL,
    candidate_title TEXT,
    normalized_title TEXT,
    synonym_used TEXT,
    redirect_final_title TEXT,
    match_method TEXT,
    notes TEXT,
    matched_at TEXT NOT NULL,
    PRIMARY KEY(taxon_source, taxon_identifier)
);
CREATE INDEX IF NOT EXISTS idx_taxon_wiki_matches_page ON taxon_wiki_matches(page_row_id);
CREATE TABLE IF NOT EXISTS taxon_wiki_match_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxon_source TEXT NOT NULL,
    taxon_identifier TEXT NOT NULL,
    attempt_order INTEGER NOT NULL,
    candidate_title TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    source_hint TEXT NOT NULL,
    outcome TEXT NOT NULL,
    page_row_id INTEGER REFERENCES wiki_pages(id) ON DELETE SET NULL,
    redirect_final_title TEXT,
    notes TEXT,
    attempted_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_taxon_wiki_attempts_taxon ON taxon_wiki_match_attempts(taxon_source, taxon_identifier);
CREATE INDEX IF NOT EXISTS idx_taxon_wiki_attempts_page ON taxon_wiki_match_attempts(page_row_id);
CREATE TABLE IF NOT EXISTS enwiki_dump_titles (
    title TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS enwiki_dump_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
""";
        command.ExecuteNonQuery();
    }

    public WikiCacheStats GetCacheStats() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT
    (SELECT COUNT(*) FROM wiki_pages) AS total_pages,
    (SELECT COUNT(*) FROM wiki_pages WHERE download_status=@cached) AS cached_pages,
    (SELECT COUNT(*) FROM wiki_pages WHERE download_status=@pending) AS pending_pages,
    (SELECT COUNT(*) FROM wiki_pages WHERE download_status=@failed) AS failed_pages,
    (SELECT COUNT(*) FROM wiki_pages WHERE download_status=@missing) AS missing_pages,
    (SELECT COUNT(*) FROM wiki_missing_titles) AS missing_titles,
    (SELECT COUNT(*) FROM taxon_wiki_matches WHERE match_status=@matched) AS matched_taxa
""";
        command.Parameters.AddWithValue("@cached", WikiPageDownloadStatus.Cached);
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@failed", WikiPageDownloadStatus.Failed);
        command.Parameters.AddWithValue("@missing", WikiPageDownloadStatus.Missing);
        command.Parameters.AddWithValue("@matched", TaxonWikiMatchStatus.Matched);

        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return new WikiCacheStats(0, 0, 0, 0, 0, 0, 0);
        }

        long GetValue(int ordinal) => reader.IsDBNull(ordinal) ? 0L : reader.GetInt64(ordinal);

        return new WikiCacheStats(
            GetValue(0),
            GetValue(1),
            GetValue(2),
            GetValue(3),
            GetValue(4),
            GetValue(5),
            GetValue(6));
    }

    // Which queued pages this run is for, and in what order. The queue routinely holds
    // six figures of titles, so "fetch the next N" is only useful if the caller can say
    // which N matter: the pages a taxon is actually waiting on, the ones queued most
    // recently (a new release's taxa), or only re-downloads of pages already cached.
    public sealed record WikiFetchScope {
        public DateTime? RefreshThreshold { get; init; }
        public bool AwaitedOnly { get; init; }   // only pages a taxon with no article yet points at
        public bool RefreshOnly { get; init; }   // skip the never-fetched queue; re-download cached pages only
        public bool FailedOnly { get; init; }    // only titles whose last download attempt failed
        public bool NewestFirst { get; init; }   // most recently queued first, instead of oldest first
        public bool KnownTitlesFirst { get; init; } // titles the enwiki all-titles dump lists before likely redlinks

        // Failures are only retried if they last failed before this moment: the run's start.
        // Without it a batch that fails comes straight back in the next batch (a failure stamps
        // last_seen_at, and nothing else changes), and "--failed-only --limit 2000" spends its
        // whole limit cycling the same few hundred titles.
        public DateTime? FailedBefore { get; init; }

        public static readonly WikiFetchScope All = new();
    }

    // A page "awaited" by a taxon still waiting for an article. The match row names only the
    // taxon's first undownloaded candidate, but every candidate it tried is in the attempt log.
    // Fetching just the named one meant a taxon with 20 candidate titles, each a redlink, needed
    // 20 separate update runs to settle. Shared with WikiCoverageStateReader (schemaPrefix "wp.")
    // so the count the plan prints is the queue the fetch works through.
    internal static string AwaitedPagePredicate(string schemaPrefix, string pageIdExpression) => $"""
        (EXISTS (SELECT 1 FROM {schemaPrefix}taxon_wiki_matches m
                 WHERE m.page_row_id = {pageIdExpression} AND m.match_status = 'pending')
         OR EXISTS (SELECT 1 FROM {schemaPrefix}taxon_wiki_match_attempts a
                    JOIN {schemaPrefix}taxon_wiki_matches m
                      ON m.taxon_source = a.taxon_source AND m.taxon_identifier = a.taxon_identifier
                    WHERE a.page_row_id = {pageIdExpression} AND m.match_status = 'pending'))
        """;

    // WHERE/ORDER BY shared by the queue count and the queue read, so the number the command
    // reports up front is the number of pages it will actually work through.
    private static string PendingPagesSql(WikiFetchScope scope, bool forCount) {
        var select = forCount
            ? "SELECT COUNT(*)"
            : "SELECT id, IFNULL(page_title, normalized_title), normalized_title, download_status, downloaded_at, attempt_count";

        const string failedBeforeRun = "(download_status = @failed AND (@failedBefore IS NULL OR last_seen_at IS NULL OR last_seen_at < @failedBefore))";
        var where = scope switch {
            { FailedOnly: true } => failedBeforeRun,
            { RefreshOnly: true } => "(@refresh IS NOT NULL AND downloaded_at IS NOT NULL AND downloaded_at < @refresh)",
            _ => $"""
                 (download_status = @pending
                     OR {failedBeforeRun}
                     OR (@refresh IS NOT NULL AND downloaded_at IS NOT NULL AND downloaded_at < @refresh))
                 """,
        };

        if (scope.AwaitedOnly) {
            where += "\nAND " + AwaitedPagePredicate("", "wiki_pages.id");
        }

        if (forCount) {
            return $"{select}\nFROM wiki_pages\nWHERE {where}";
        }

        // The status precedence stays first in either order. Without it, --newest-first would put
        // a page that has just failed back at the front of the next batch, and with no attempt
        // cap anywhere that is a retry loop against Wikipedia.
        var order = scope.NewestFirst
            ? """
              ORDER BY CASE download_status WHEN @pending THEN 0 WHEN @failed THEN 1 ELSE 2 END,
                       discovered_at DESC,
                       id DESC
              """
            : """
              ORDER BY CASE download_status WHEN @pending THEN 0 WHEN @failed THEN 1 ELSE 2 END,
                       IFNULL(downloaded_at, '0000-01-01T00:00:00Z'),
                       id
              """;

        // Between the status precedence and the age order: the all-titles dump says which queued
        // titles have an article at all, so a run asking for it downloads pages first and leaves
        // the likely redlinks (each of which costs an API round-trip to learn nothing) for last.
        if (scope.KnownTitlesFirst) {
            var lines = order.Split('\n');
            order = lines[0] + "\n" +
                    "         EXISTS (SELECT 1 FROM enwiki_dump_titles d WHERE d.title = wiki_pages.normalized_title) DESC,\n" +
                    string.Join('\n', lines[1..]);
        }

        return $"{select}\nFROM wiki_pages\nWHERE {where}\n{order}\nLIMIT @limit";
    }

    private static void BindScope(SqliteCommand command, WikiFetchScope scope) {
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@failed", WikiPageDownloadStatus.Failed);
        command.Parameters.AddWithValue("@refresh", scope.RefreshThreshold?.ToString("O") ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@failedBefore", scope.FailedBefore?.ToString("O") ?? (object)DBNull.Value);
    }

    public long CountPendingPages(WikiFetchScope scope) {
        using var command = _connection.CreateCommand();
        command.CommandText = PendingPagesSql(scope, forCount: true);
        BindScope(command, scope);
        return command.ExecuteScalar() is long n ? n : 0;
    }

    public IReadOnlyList<WikiPageWorkItem> GetPendingPages(int limit, WikiFetchScope scope) {
        if (limit <= 0) {
            return Array.Empty<WikiPageWorkItem>();
        }

        using var command = _connection.CreateCommand();
        command.CommandText = PendingPagesSql(scope, forCount: false);
        BindScope(command, scope);
        command.Parameters.AddWithValue("@limit", limit);

        var list = new List<WikiPageWorkItem>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var downloadedValue = reader.IsDBNull(4) ? null : reader.GetString(4);
            var downloadedAt = StoredUtc.Parse(downloadedValue);

            list.Add(new WikiPageWorkItem(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                downloadedAt,
                reader.GetInt32(5)));
        }

        return list;
    }

    /// Every queued title that has not been downloaded, for callers that need to inspect the text
    /// itself rather than filter in SQL.
    public IReadOnlyList<(long Id, string Title)> ReadQueuedTitles() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT id, IFNULL(page_title, normalized_title)
            FROM wiki_pages
            WHERE download_status IN (@pending, @failed)
            ORDER BY id
            """;
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@failed", WikiPageDownloadStatus.Failed);

        var list = new List<(long, string)>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            list.Add((reader.GetInt64(0), reader.GetString(1)));
        }
        return list;
    }

    /// Removes queued titles. A taxon match pointing at one is left in place with its page cleared
    /// (ON DELETE SET NULL), so the next match-taxa run picks the taxon up again.
    public int DeletePages(IReadOnlyList<long> pageRowIds) {
        if (pageRowIds.Count == 0) {
            return 0;
        }

        var deleted = 0;
        using var tx = _connection.BeginTransaction();
        using (var command = _connection.CreateCommand()) {
            command.Transaction = tx;
            command.CommandText = "DELETE FROM wiki_pages WHERE id = @id";
            var id = command.Parameters.Add("@id", SqliteType.Integer);
            foreach (var rowId in pageRowIds) {
                id.Value = rowId;
                deleted += command.ExecuteNonQuery();
            }
        }
        tx.Commit();
        return deleted;
    }

    public WikiPageUpsertResult UpsertPageCandidate(WikiPageCandidate candidate) {
        if (candidate is null) {
            throw new ArgumentNullException(nameof(candidate));
        }

        using var tx = _connection.BeginTransaction();
        long pageRowId;
        var isNew = false;
        using (var select = _connection.CreateCommand()) {
            select.Transaction = tx;
            select.CommandText = "SELECT id FROM wiki_pages WHERE normalized_title=@title LIMIT 1";
            select.Parameters.AddWithValue("@title", candidate.NormalizedTitle);
            var existing = select.ExecuteScalar();
            if (existing is long id) {
                pageRowId = id;
                using var update = _connection.CreateCommand();
                update.Transaction = tx;
                update.CommandText =
                    "UPDATE wiki_pages SET page_title=@titleValue, page_id=COALESCE(@pageId, page_id), last_seen_at=@seen WHERE id=@id";
                update.Parameters.AddWithValue("@titleValue", candidate.Title);
                update.Parameters.AddWithValue("@pageId", candidate.PageId.HasValue ? candidate.PageId.Value : DBNull.Value);
                update.Parameters.AddWithValue("@seen", candidate.LastSeenAt.ToString("O"));
                update.Parameters.AddWithValue("@id", pageRowId);
                update.ExecuteNonQuery();
            }
            else {
                using var insert = _connection.CreateCommand();
                insert.Transaction = tx;
                insert.CommandText =
                    "INSERT INTO wiki_pages(page_id, page_title, normalized_title, discovered_at, last_seen_at) VALUES (@pageId,@title,@normalized,@discovered,@seen); SELECT last_insert_rowid();";
                insert.Parameters.AddWithValue("@pageId", candidate.PageId.HasValue ? candidate.PageId.Value : DBNull.Value);
                insert.Parameters.AddWithValue("@title", candidate.Title);
                insert.Parameters.AddWithValue("@normalized", candidate.NormalizedTitle);
                insert.Parameters.AddWithValue("@discovered", candidate.DiscoveredAt.ToString("O"));
                insert.Parameters.AddWithValue("@seen", candidate.LastSeenAt.ToString("O"));
                pageRowId = (long)(insert.ExecuteScalar() ?? 0L);
                isNew = true;
            }
        }

        tx.Commit();
        return new WikiPageUpsertResult(pageRowId, isNew);
    }

    public WikiPageSummary? GetPageByNormalizedTitle(string normalizedTitle) {
        if (string.IsNullOrWhiteSpace(normalizedTitle)) {
            return null;
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT id, page_title, normalized_title, download_status, is_redirect, redirect_target, is_disambiguation, is_set_index, has_taxobox, last_seen_at
FROM wiki_pages
WHERE normalized_title=@title
LIMIT 1
""";
        command.Parameters.AddWithValue("@title", normalizedTitle);
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return null;
        }

        DateTime? lastSeen = null;
        if (!reader.IsDBNull(9)) {
            var raw = reader.GetString(9);
            if (!string.IsNullOrWhiteSpace(raw) && DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)) {
                lastSeen = parsed;
            }
        }

        return new WikiPageSummary(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? normalizedTitle : reader.GetString(1),
            reader.IsDBNull(2) ? normalizedTitle : reader.GetString(2),
            reader.IsDBNull(3) ? WikiPageDownloadStatus.Pending : reader.GetString(3),
            !reader.IsDBNull(4) && reader.GetInt64(4) != 0,
            reader.IsDBNull(5) ? null : reader.GetString(5),
            !reader.IsDBNull(6) && reader.GetInt64(6) != 0,
            !reader.IsDBNull(7) && reader.GetInt64(7) != 0,
            !reader.IsDBNull(8) && reader.GetInt64(8) != 0,
            lastSeen);
    }

    public void SavePageContent(WikiPageContent content) {
        if (content is null) {
            throw new ArgumentNullException(nameof(content));
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
UPDATE wiki_pages
SET page_id=COALESCE(@pageId, page_id),
    page_title=@title,
    normalized_title=@normalized,
    latest_revision_id=@rev,
    download_status=@status,
    import_id=@importId,
    downloaded_at=@downloaded,
    last_seen_at=@seen,
    html_main=@html,
    wikitext=@wikitext,
    html_sha256=@sha,
    html_bytes=@htmlBytes,
    wikitext_bytes=@wikitextBytes,
    is_redirect=@isRedirect,
    redirect_target=@redirectTarget,
    is_disambiguation=@isDisambig,
    is_set_index=@isSetIndex,
    has_taxobox=@hasTaxobox,
    attempt_count=0,
    last_error=NULL
WHERE id=@id
""";
        command.Parameters.AddWithValue("@pageId", content.PageId.HasValue ? content.PageId.Value : DBNull.Value);
        command.Parameters.AddWithValue("@title", content.CanonicalTitle);
        command.Parameters.AddWithValue("@normalized", content.NormalizedTitle);
        command.Parameters.AddWithValue("@rev", content.LatestRevisionId.HasValue ? content.LatestRevisionId.Value : DBNull.Value);
        command.Parameters.AddWithValue("@status", WikiPageDownloadStatus.Cached);
        command.Parameters.AddWithValue("@importId", content.ImportId);
        command.Parameters.AddWithValue("@downloaded", content.DownloadedAt.ToString("O"));
        command.Parameters.AddWithValue("@seen", content.DownloadedAt.ToString("O"));
        command.Parameters.AddWithValue("@html", (object?)content.HtmlMain ?? DBNull.Value);
        command.Parameters.AddWithValue("@wikitext", (object?)content.Wikitext ?? DBNull.Value);
        command.Parameters.AddWithValue("@sha", (object?)content.HtmlSha256 ?? DBNull.Value);
        command.Parameters.AddWithValue("@htmlBytes", content.HtmlMain is null ? DBNull.Value : Encoding.UTF8.GetByteCount(content.HtmlMain));
        command.Parameters.AddWithValue("@wikitextBytes", content.Wikitext is null ? DBNull.Value : Encoding.UTF8.GetByteCount(content.Wikitext));
        command.Parameters.AddWithValue("@isRedirect", content.IsRedirect ? 1 : 0);
        command.Parameters.AddWithValue("@redirectTarget", (object?)content.RedirectTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("@isDisambig", content.IsDisambiguation ? 1 : 0);
        command.Parameters.AddWithValue("@isSetIndex", content.IsSetIndex ? 1 : 0);
        command.Parameters.AddWithValue("@hasTaxobox", content.HasTaxobox ? 1 : 0);
        command.Parameters.AddWithValue("@id", content.PageRowId);
        command.ExecuteNonQuery();
    }

    public void MarkRedirectStub(long pageRowId, string pageTitle, string normalizedTitle, string redirectTarget, DateTime seenAt) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
UPDATE wiki_pages
SET page_title=@title,
    normalized_title=@normalized,
    download_status=@status,
    downloaded_at=COALESCE(downloaded_at, @seen),
    last_seen_at=@seen,
    is_redirect=1,
    redirect_target=@redirectTarget,
    attempt_count=0,
    last_error=NULL
WHERE id=@id
""";
        command.Parameters.AddWithValue("@title", pageTitle);
        command.Parameters.AddWithValue("@normalized", normalizedTitle);
        command.Parameters.AddWithValue("@status", WikiPageDownloadStatus.Cached);
        command.Parameters.AddWithValue("@seen", seenAt.ToString("O"));
        command.Parameters.AddWithValue("@redirectTarget", redirectTarget);
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    public void ReplaceCategories(long pageRowId, IEnumerable<string> categories) {
        if (categories is null) {
            throw new ArgumentNullException(nameof(categories));
        }

        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM wiki_page_categories WHERE page_row_id=@id";
            delete.Parameters.AddWithValue("@id", pageRowId);
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand()) {
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO wiki_page_categories(page_row_id, category_name) VALUES (@id,@category)";
            var idParam = insert.Parameters.Add("@id", SqliteType.Integer);
            var catParam = insert.Parameters.Add("@category", SqliteType.Text);
            idParam.Value = pageRowId;
            foreach (var category in categories) {
                if (string.IsNullOrWhiteSpace(category)) {
                    continue;
                }
                catParam.Value = category.Trim();
                insert.ExecuteNonQuery();
            }
        }

        tx.Commit();
    }

    public void ReplaceRedirectChain(long pageRowId, IReadOnlyList<WikiRedirectEdge> edges) {
        if (edges is null) {
            throw new ArgumentNullException(nameof(edges));
        }

        using var tx = _connection.BeginTransaction();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM wiki_redirect_edges WHERE page_row_id=@id";
            delete.Parameters.AddWithValue("@id", pageRowId);
            delete.ExecuteNonQuery();
        }

        if (edges.Count > 0) {
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText =
                "INSERT INTO wiki_redirect_edges(page_row_id, hop, target_title) VALUES (@page,@hop,@title)";
            var pageParam = insert.Parameters.Add("@page", SqliteType.Integer);
            var hopParam = insert.Parameters.Add("@hop", SqliteType.Integer);
            var titleParam = insert.Parameters.Add("@title", SqliteType.Text);
            pageParam.Value = pageRowId;
            foreach (var edge in edges) {
                hopParam.Value = edge.Hop;
                titleParam.Value = edge.TargetTitle;
                insert.ExecuteNonQuery();
            }
        }

        tx.Commit();
    }

    public void DeletePage(long pageRowId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM wiki_pages WHERE id=@id";
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Marks an existing page for re-download in place (reset to pending) without deleting the
    /// row, so taxon matches that reference its page_row_id are preserved and the cached
    /// taxobox/categories survive until the re-fetch overwrites them.
    /// </summary>
    public void MarkPageForRefresh(long pageRowId, DateTime now) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE wiki_pages SET download_status=@pending, last_seen_at=@now WHERE id=@id";
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@now", now.ToString("O"));
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    public void MergePageRecords(long sourcePageRowId, long targetPageRowId) {
        if (sourcePageRowId == targetPageRowId) {
            return;
        }

        using var tx = _connection.BeginTransaction();

        void UpdateReference(string sql) {
            using var update = _connection.CreateCommand();
            update.Transaction = tx;
            update.CommandText = sql;
            update.Parameters.AddWithValue("@target", targetPageRowId);
            update.Parameters.AddWithValue("@source", sourcePageRowId);
            update.ExecuteNonQuery();
        }

        UpdateReference("UPDATE taxon_wiki_matches SET page_row_id=@target WHERE page_row_id=@source");
        UpdateReference("UPDATE taxon_wiki_match_attempts SET page_row_id=@target WHERE page_row_id=@source");

        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM wiki_pages WHERE id=@id";
            delete.Parameters.AddWithValue("@id", sourcePageRowId);
            delete.ExecuteNonQuery();
        }

        tx.Commit();
    }

    public void UpsertTaxoboxData(WikiTaxoboxData data) => UpsertTaxoboxData(data, transaction: null);

    private void UpsertTaxoboxData(WikiTaxoboxData data, SqliteTransaction? transaction) {
        if (data is null) {
            throw new ArgumentNullException(nameof(data));
        }

        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
INSERT INTO wiki_taxobox_data(page_row_id, scientific_name, rank, kingdom, phylum, class_name, order_name, family, subfamily, tribe, genus, species, is_monotypic, data_json)
VALUES (@id,@scientific,@rank,@kingdom,@phylum,@class,@order,@family,@subfamily,@tribe,@genus,@species,@mono,@json)
ON CONFLICT(page_row_id) DO UPDATE SET
    scientific_name=excluded.scientific_name,
    rank=excluded.rank,
    kingdom=excluded.kingdom,
    phylum=excluded.phylum,
    class_name=excluded.class_name,
    order_name=excluded.order_name,
    family=excluded.family,
    subfamily=excluded.subfamily,
    tribe=excluded.tribe,
    genus=excluded.genus,
    species=excluded.species,
    is_monotypic=excluded.is_monotypic,
    data_json=excluded.data_json
""";
        command.Parameters.AddWithValue("@id", data.PageRowId);
        command.Parameters.AddWithValue("@scientific", (object?)data.ScientificName ?? DBNull.Value);
        command.Parameters.AddWithValue("@rank", (object?)data.Rank ?? DBNull.Value);
        command.Parameters.AddWithValue("@kingdom", (object?)data.Kingdom ?? DBNull.Value);
        command.Parameters.AddWithValue("@phylum", (object?)data.Phylum ?? DBNull.Value);
        command.Parameters.AddWithValue("@class", (object?)data.Class ?? DBNull.Value);
        command.Parameters.AddWithValue("@order", (object?)data.Order ?? DBNull.Value);
        command.Parameters.AddWithValue("@family", (object?)data.Family ?? DBNull.Value);
        command.Parameters.AddWithValue("@subfamily", (object?)data.Subfamily ?? DBNull.Value);
        command.Parameters.AddWithValue("@tribe", (object?)data.Tribe ?? DBNull.Value);
        command.Parameters.AddWithValue("@genus", (object?)data.Genus ?? DBNull.Value);
        command.Parameters.AddWithValue("@species", (object?)data.Species ?? DBNull.Value);
        command.Parameters.AddWithValue("@mono", data.IsMonotypic.HasValue ? (data.IsMonotypic.Value ? 1 : 0) : DBNull.Value);
        command.Parameters.AddWithValue("@json", (object?)data.DataJson ?? DBNull.Value);
        command.ExecuteNonQuery();
    }

    public void DeleteTaxoboxData(long pageRowId) => DeleteTaxoboxData(pageRowId, transaction: null);

    private void DeleteTaxoboxData(long pageRowId, SqliteTransaction? transaction) {
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "DELETE FROM wiki_taxobox_data WHERE page_row_id=@id";
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Saves the taxobox fields of several pages in one transaction: <paramref name="upserts"/>
    /// replace or add a page's fields, and the pages in <paramref name="deletes"/> lose theirs.
    /// </summary>
    public void SaveTaxoboxChanges(IReadOnlyList<WikiTaxoboxData> upserts, IReadOnlyList<long> deletes) {
        if (upserts.Count == 0 && deletes.Count == 0) {
            return;
        }
        using var tx = _connection.BeginTransaction();
        foreach (var data in upserts) {
            UpsertTaxoboxData(data, tx);
        }
        foreach (var pageRowId in deletes) {
            DeleteTaxoboxData(pageRowId, tx);
        }
        tx.Commit();
    }

    public WikiTaxoboxData? GetTaxoboxData(long pageRowId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"""
SELECT {TaxoboxColumns}
FROM wiki_taxobox_data
WHERE page_row_id=@id
LIMIT 1
""";
        command.Parameters.AddWithValue("@id", pageRowId);
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        return reader.Read() ? ReadTaxobox(reader, 0) : null;
    }

    /// <summary>
    /// What the page with row id <paramref name="pageRowId"/> says about its kingdom: its title, its
    /// taxobox kingdom, genus and taxon parameters, and its "&lt;group&gt; described in &lt;year&gt;"
    /// categories (<see cref="WikiPageKingdom"/>). Null when there is no such row.
    /// </summary>
    public WikiPageKingdomEvidence? GetPageKingdomEvidence(long pageRowId) {
        string title;
        string? kingdom, genus, taxon;
        using (var command = _connection.CreateCommand()) {
            command.CommandText =
                """
                SELECT p.page_title, t.kingdom,
                       CASE WHEN json_valid(t.data_json) THEN json_extract(t.data_json, '$.genus') END,
                       t.genus,
                       CASE WHEN json_valid(t.data_json) THEN json_extract(t.data_json, '$.taxon') END
                FROM wiki_pages p
                LEFT JOIN wiki_taxobox_data t ON t.page_row_id = p.id
                WHERE p.id = @id
                """;
            command.Parameters.AddWithValue("@id", pageRowId);
            using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
            if (!reader.Read()) {
                return null;
            }
            string? Text(int ordinal) => reader.IsDBNull(ordinal) ? null : Convert.ToString(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
            title = reader.GetString(0);
            kingdom = Text(1);
            genus = Text(2) ?? Text(3);
            taxon = Text(4);
        }

        var categories = new List<string>();
        using (var command = _connection.CreateCommand()) {
            command.CommandText =
                "SELECT category_name FROM wiki_page_categories WHERE page_row_id = @id AND category_name LIKE '% described in %'";
            command.Parameters.AddWithValue("@id", pageRowId);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                categories.Add(reader.GetString(0));
            }
        }
        return new WikiPageKingdomEvidence(title, kingdom, genus, taxon, categories);
    }

    /// <summary>
    /// Titles that start with <paramref name="normalizedName"/> followed by " (", such as "Ficus
    /// variegata (plant)": the downloaded pages and redirects with such a title, and, when
    /// <paramref name="includeTitleList"/> is set, the titles in the list of every article title
    /// (enwiki_dump_titles). <see cref="WikiPageKingdom.QualifiedTitlesFor"/> picks the ones for a
    /// taxon's kingdom.
    /// </summary>
    public IReadOnlyList<string> FindTitlesWithQualifier(string normalizedName, bool includeTitleList = true) {
        if (string.IsNullOrWhiteSpace(normalizedName)) {
            return [];
        }
        // Every title that starts with "<name> (" sorts at or after it and before "<name> )".
        var from = normalizedName + " (";
        var to = normalizedName + " )";
        var titles = new SortedSet<string>(StringComparer.Ordinal);
        using (var command = _connection.CreateCommand()) {
            // The status is tested here, not in SQL: with "download_status = ?" in the WHERE clause
            // SQLite reads every downloaded page through the status index instead of the title range.
            command.CommandText =
                "SELECT normalized_title, download_status FROM wiki_pages WHERE normalized_title >= @from AND normalized_title < @to";
            command.Parameters.AddWithValue("@from", from);
            command.Parameters.AddWithValue("@to", to);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (reader.GetString(1) == WikiPageDownloadStatus.Cached) {
                    titles.Add(reader.GetString(0));
                }
            }
        }
        if (includeTitleList && HasTitleList()) {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT title FROM enwiki_dump_titles WHERE title >= @from AND title < @to";
            command.Parameters.AddWithValue("@from", from);
            command.Parameters.AddWithValue("@to", to);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                titles.Add(reader.GetString(0));
            }
        }
        return titles.ToList();
    }

    /// <summary>
    /// Whether the cache has the list of every article title (enwiki_dump_titles). A cache opened
    /// read-only from a file made before the list existed has no such table.
    /// </summary>
    internal bool HasTitleList() {
        if (_hasTitleList is null) {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'enwiki_dump_titles'";
            _hasTitleList = command.ExecuteScalar() is not null;
        }
        return _hasTitleList.Value;
    }

    /// <summary>Whether the list of every article title (enwiki_dump_titles) has <paramref name="normalizedTitle"/>.</summary>
    public bool IsInTitleList(string normalizedTitle) {
        if (string.IsNullOrWhiteSpace(normalizedTitle) || !HasTitleList()) {
            return false;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM enwiki_dump_titles WHERE title = @title";
        command.Parameters.AddWithValue("@title", normalizedTitle);
        return command.ExecuteScalar() is not null;
    }

    /// <summary>
    /// Downloaded pages (download_status cached) in row id order, starting after
    /// <paramref name="afterPageRowId"/>, with their wikitext and their stored taxobox fields.
    /// Wikitext is null for a redirect stored without text. `wikipedia reparse-taxoboxes` reads
    /// the cache a batch at a time with this.
    /// </summary>
    public IReadOnlyList<WikiStoredTaxoboxPage> ReadDownloadedPages(long afterPageRowId, int limit) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"""
SELECT p.id, p.page_title, p.wikitext, {TaxoboxColumnsOf("t")}
FROM wiki_pages p
LEFT JOIN wiki_taxobox_data t ON t.page_row_id = p.id
WHERE p.id > @after AND p.download_status = @cached
ORDER BY p.id
LIMIT @limit
""";
        command.Parameters.AddWithValue("@after", afterPageRowId);
        command.Parameters.AddWithValue("@cached", WikiPageDownloadStatus.Cached);
        command.Parameters.AddWithValue("@limit", limit);
        var pages = new List<WikiStoredTaxoboxPage>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            pages.Add(new WikiStoredTaxoboxPage(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : ReadTaxobox(reader, 3)));
        }
        return pages;
    }

    /// <summary>How many pages are downloaded (download_status cached), redirects included.</summary>
    public long CountDownloadedPages() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM wiki_pages WHERE download_status = @cached";
        command.Parameters.AddWithValue("@cached", WikiPageDownloadStatus.Cached);
        return Convert.ToInt64(command.ExecuteScalar() ?? 0L);
    }

    private const string TaxoboxColumns =
        "page_row_id, scientific_name, rank, kingdom, phylum, class_name, order_name, family, subfamily, tribe, genus, species, is_monotypic, data_json";

    private static string TaxoboxColumnsOf(string alias) =>
        string.Join(", ", TaxoboxColumns.Split(", ").Select(column => $"{alias}.{column}"));

    // The 14 wiki_taxobox_data columns in TaxoboxColumns order, starting at ordinal first.
    private static WikiTaxoboxData ReadTaxobox(SqliteDataReader reader, int first) {
        string? Text(int offset) => reader.IsDBNull(first + offset) ? null : reader.GetString(first + offset);
        bool? Flag(int offset) {
            if (reader.IsDBNull(first + offset)) {
                return null;
            }
            return reader.GetInt64(first + offset) switch {
                0 => false,
                1 => true,
                _ => null
            };
        }

        return new WikiTaxoboxData(
            reader.GetInt64(first),
            Text(1), Text(2), Text(3), Text(4), Text(5), Text(6),
            Text(7), Text(8), Text(9), Text(10), Text(11),
            Flag(12),
            Text(13));
    }

    public void RecordPageFailure(long pageRowId, string errorMessage, DateTime occurredAt) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
UPDATE wiki_pages
SET attempt_count = attempt_count + 1,
    last_error = @error,
    download_status = @status,
    last_seen_at = @seen
WHERE id=@id
""";
        command.Parameters.AddWithValue("@error", errorMessage);
        command.Parameters.AddWithValue("@status", WikiPageDownloadStatus.Failed);
        command.Parameters.AddWithValue("@seen", occurredAt.ToString("O"));
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    public void MarkPageMissing(long pageRowId, string reason, DateTime observedAt) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
UPDATE wiki_pages
SET download_status=@status,
    last_error=@error,
    last_seen_at=@seen
WHERE id=@id
""";
        command.Parameters.AddWithValue("@status", WikiPageDownloadStatus.Missing);
        command.Parameters.AddWithValue("@error", reason);
        command.Parameters.AddWithValue("@seen", observedAt.ToString("O"));
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    public void RecordMissingTitle(WikiMissingTitle missing) {
        if (missing is null) {
            throw new ArgumentNullException(nameof(missing));
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
INSERT INTO wiki_missing_titles(normalized_title, title, latest_reason, attempt_count, last_attempt_at, notes)
VALUES (@normalized,@title,@reason,1,@attempted,@notes)
ON CONFLICT(normalized_title) DO UPDATE SET
    title=excluded.title,
    latest_reason=excluded.latest_reason,
    last_attempt_at=excluded.last_attempt_at,
    notes=excluded.notes,
    attempt_count=wiki_missing_titles.attempt_count + 1
""";
        command.Parameters.AddWithValue("@normalized", missing.NormalizedTitle);
        command.Parameters.AddWithValue("@title", missing.Title);
        command.Parameters.AddWithValue("@reason", missing.ReasonCode);
        command.Parameters.AddWithValue("@attempted", missing.AttemptedAt.ToString("O"));
        command.Parameters.AddWithValue("@notes", (object?)missing.Notes ?? DBNull.Value);
        command.ExecuteNonQuery();
    }

    public void UpsertTaxonMatch(TaxonWikiMatch match) {
        if (match is null) {
            throw new ArgumentNullException(nameof(match));
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
INSERT INTO taxon_wiki_matches(taxon_source, taxon_identifier, match_status, page_row_id, candidate_title, normalized_title, synonym_used, redirect_final_title, match_method, notes, matched_at)
VALUES (@source,@id,@status,@page,@title,@normalized,@synonym,@redirect,@method,@notes,@matched)
ON CONFLICT(taxon_source, taxon_identifier) DO UPDATE SET
    match_status=excluded.match_status,
    page_row_id=excluded.page_row_id,
    candidate_title=excluded.candidate_title,
    normalized_title=excluded.normalized_title,
    synonym_used=excluded.synonym_used,
    redirect_final_title=excluded.redirect_final_title,
    match_method=excluded.match_method,
    notes=excluded.notes,
    matched_at=excluded.matched_at
""";
        command.Parameters.AddWithValue("@source", match.TaxonSource);
        command.Parameters.AddWithValue("@id", match.TaxonIdentifier);
        command.Parameters.AddWithValue("@status", match.MatchStatus);
        command.Parameters.AddWithValue("@page", match.PageRowId.HasValue ? match.PageRowId.Value : DBNull.Value);
        command.Parameters.AddWithValue("@title", (object?)match.CandidateTitle ?? DBNull.Value);
        command.Parameters.AddWithValue("@normalized", (object?)match.NormalizedTitle ?? DBNull.Value);
        command.Parameters.AddWithValue("@synonym", (object?)match.SynonymUsed ?? DBNull.Value);
        command.Parameters.AddWithValue("@redirect", (object?)match.RedirectFinalTitle ?? DBNull.Value);
        command.Parameters.AddWithValue("@method", (object?)match.MatchMethod ?? DBNull.Value);
        command.Parameters.AddWithValue("@notes", (object?)match.Notes ?? DBNull.Value);
        command.Parameters.AddWithValue("@matched", match.MatchedAt.ToString("O"));
        command.ExecuteNonQuery();
    }

    public void RecordTaxonAttempt(TaxonWikiMatchAttempt attempt) {
        if (attempt is null) {
            throw new ArgumentNullException(nameof(attempt));
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
INSERT INTO taxon_wiki_match_attempts(taxon_source, taxon_identifier, attempt_order, candidate_title, normalized_title, source_hint, outcome, page_row_id, redirect_final_title, notes, attempted_at)
VALUES (@source,@id,@order,@title,@normalized,@hint,@outcome,@page,@redirect,@notes,@attempted)
""";
        command.Parameters.AddWithValue("@source", attempt.TaxonSource);
        command.Parameters.AddWithValue("@id", attempt.TaxonIdentifier);
        command.Parameters.AddWithValue("@order", attempt.AttemptOrder);
        command.Parameters.AddWithValue("@title", attempt.CandidateTitle);
        command.Parameters.AddWithValue("@normalized", attempt.NormalizedTitle);
        command.Parameters.AddWithValue("@hint", attempt.SourceHint);
        command.Parameters.AddWithValue("@outcome", attempt.Outcome);
        command.Parameters.AddWithValue("@page", attempt.PageRowId.HasValue ? attempt.PageRowId.Value : DBNull.Value);
        command.Parameters.AddWithValue("@redirect", (object?)attempt.RedirectFinalTitle ?? DBNull.Value);
        command.Parameters.AddWithValue("@notes", (object?)attempt.Notes ?? DBNull.Value);
        command.Parameters.AddWithValue("@attempted", attempt.AttemptedAt.ToString("O"));
        command.ExecuteNonQuery();
    }

    public TaxonWikiMatch? GetTaxonMatch(string taxonSource, string taxonIdentifier) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT taxon_source, taxon_identifier, match_status, page_row_id, candidate_title, normalized_title, synonym_used, redirect_final_title, match_method, notes, matched_at
FROM taxon_wiki_matches
WHERE taxon_source=@source AND taxon_identifier=@id
LIMIT 1
""";
        command.Parameters.AddWithValue("@source", taxonSource);
        command.Parameters.AddWithValue("@id", taxonIdentifier);
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return null;
        }

        var matchedValue = reader.IsDBNull(10) ? null : reader.GetString(10);
        var matchedAt = DateTime.TryParse(matchedValue, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : DateTime.MinValue;

        return new TaxonWikiMatch(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            matchedAt);
    }

    /// <summary>
    /// Removes the prior attempt-log rows for a taxon before it is re-evaluated, so
    /// taxon_wiki_match_attempts holds only the latest run's attempts instead of growing
    /// without bound on every re-run.
    /// </summary>
    public void ClearTaxonAttempts(string taxonSource, string taxonIdentifier) {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM taxon_wiki_match_attempts WHERE taxon_source=@source AND taxon_identifier=@id";
        command.Parameters.AddWithValue("@source", taxonSource);
        command.Parameters.AddWithValue("@id", taxonIdentifier);
        command.ExecuteNonQuery();
    }

    // ---- the enwiki all-titles dump -------------------------------------------------------
    // A twice-monthly list of every ns0 title (articles and redirects, no page ids), kept as a
    // cheap local existence check: a queued title absent from it is almost certainly a redlink.
    // Titles are stored in normalized form (underscores as spaces) so they join directly against
    // wiki_pages.normalized_title.

    public EnwikiDumpInfo? GetDumpInfo() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT key, value FROM enwiki_dump_info";
        string? dumpDate = null, source = null, importedRaw = null;
        long count = 0;
        var partial = false;
        using (var reader = command.ExecuteReader()) {
            while (reader.Read()) {
                var value = reader.GetString(1);
                switch (reader.GetString(0)) {
                    case "dump_date": dumpDate = value; break;
                    case "source": source = value; break;
                    case "imported_at": importedRaw = value; break;
                    case "title_count": long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out count); break;
                    case "partial": partial = value == "1"; break;
                }
            }
        }

        if (importedRaw is null) {
            return null;
        }

        DateTime? importedAt = DateTime.TryParse(importedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            ? parsed
            : null;
        return new EnwikiDumpInfo(dumpDate, importedAt, count, source, partial);
    }

    public string? GetDumpInfoValue(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM enwiki_dump_info WHERE key=@key";
        command.Parameters.AddWithValue("@key", key);
        return command.ExecuteScalar() as string;
    }

    public void SetDumpInfoValue(string key, string? value) {
        using var command = _connection.CreateCommand();
        if (value is null) {
            command.CommandText = "DELETE FROM enwiki_dump_info WHERE key=@key";
            command.Parameters.AddWithValue("@key", key);
        }
        else {
            command.CommandText = "INSERT INTO enwiki_dump_info(key, value) VALUES (@key,@value) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
            command.Parameters.AddWithValue("@key", key);
            command.Parameters.AddWithValue("@value", value);
        }
        command.ExecuteNonQuery();
    }

    /// A fresh import starts by clearing the old dump and its recorded facts (the download
    /// bookkeeping keys are kept), so an interrupted import reads as "no dump" rather than as a
    /// complete older one.
    public void ClearDumpTitles() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            DELETE FROM enwiki_dump_titles;
            DELETE FROM enwiki_dump_info WHERE key IN ('dump_date','imported_at','title_count','partial','source');
            """;
        command.ExecuteNonQuery();
    }

    /// Inserts a batch of already-normalized titles inside one transaction. Returns rows added
    /// (duplicates are ignored, so re-feeding a batch is harmless).
    public long AddDumpTitles(IReadOnlyList<string> normalizedTitles) {
        if (normalizedTitles.Count == 0) {
            return 0;
        }

        long added = 0;
        using var tx = _connection.BeginTransaction();
        using (var command = _connection.CreateCommand()) {
            command.Transaction = tx;
            command.CommandText = "INSERT OR IGNORE INTO enwiki_dump_titles(title) VALUES (@title)";
            var title = command.Parameters.Add("@title", SqliteType.Text);
            foreach (var value in normalizedTitles) {
                title.Value = value;
                added += command.ExecuteNonQuery();
            }
        }
        tx.Commit();
        return added;
    }

    public void RecordDumpImport(EnwikiDumpInfo info) {
        if (info is null) {
            throw new ArgumentNullException(nameof(info));
        }

        SetDumpInfoValue("dump_date", info.DumpDate ?? "");
        SetDumpInfoValue("imported_at", (info.ImportedAt ?? DateTime.UtcNow).ToString("O"));
        SetDumpInfoValue("title_count", info.TitleCount.ToString(CultureInfo.InvariantCulture));
        SetDumpInfoValue("source", info.Source ?? "");
        SetDumpInfoValue("partial", info.Partial ? "1" : "0");
    }

    public long CountDumpTitles() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM enwiki_dump_titles";
        return command.ExecuteScalar() is long n ? n : 0;
    }

    /// How the queue splits against the dump: titles it lists (an article or redirect exists)
    /// versus titles it does not (likely redlinks).
    public (long InDump, long Absent) CountQueuedAgainstDump() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT
                SUM(CASE WHEN EXISTS (SELECT 1 FROM enwiki_dump_titles d WHERE d.title = wiki_pages.normalized_title) THEN 1 ELSE 0 END),
                SUM(CASE WHEN EXISTS (SELECT 1 FROM enwiki_dump_titles d WHERE d.title = wiki_pages.normalized_title) THEN 0 ELSE 1 END)
            FROM wiki_pages
            WHERE download_status IN (@pending, @failed)
            """;
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@failed", WikiPageDownloadStatus.Failed);
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return (0, 0);
        }
        long Get(int ordinal) => reader.IsDBNull(ordinal) ? 0L : reader.GetInt64(ordinal);
        return (Get(0), Get(1));
    }

    public int GetNextAttemptOrder(string taxonSource, string taxonIdentifier) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT IFNULL(MAX(attempt_order), 0) FROM taxon_wiki_match_attempts WHERE taxon_source=@source AND taxon_identifier=@id";
        command.Parameters.AddWithValue("@source", taxonSource);
        command.Parameters.AddWithValue("@id", taxonIdentifier);
        var value = command.ExecuteScalar();
        var current = value is null || value is DBNull ? 0 : Convert.ToInt32(value, CultureInfo.InvariantCulture);
        return current + 1;
    }
}

internal static class WikiPageDownloadStatus {
    public const string Pending = "pending";
    public const string Cached = "cached";
    public const string Failed = "failed";
    public const string Missing = "missing";
}

internal static class TaxonWikiMatchStatus {
    public const string Pending = "pending";
    public const string Matched = "matched";
    public const string Missing = "missing";
    public const string Rejected = "rejected";
}

internal static class TaxonWikiAttemptOutcome {
    public const string Matched = "matched";
    public const string Redirected = "redirected";
    public const string Missing = "missing";
    public const string Failed = "failed";
    public const string PendingFetch = "pending";
}

internal sealed record WikiPageCandidate(string Title, string NormalizedTitle, long? PageId, DateTime DiscoveredAt, DateTime LastSeenAt);

internal sealed record WikiPageSummary(
    long PageRowId,
    string PageTitle,
    string NormalizedTitle,
    string DownloadStatus,
    bool IsRedirect,
    string? RedirectTarget,
    bool IsDisambiguation,
    bool IsSetIndex,
    bool HasTaxobox,
    DateTime? LastSeenAt
);

internal sealed record WikiPageUpsertResult(long PageRowId, bool IsNew);

internal sealed record WikiPageContent(
    long PageRowId,
    long? PageId,
    string CanonicalTitle,
    string NormalizedTitle,
    long? LatestRevisionId,
    bool IsRedirect,
    string? RedirectTarget,
    bool IsDisambiguation,
    bool IsSetIndex,
    bool HasTaxobox,
    string? HtmlMain,
    string? HtmlSha256,
    string? Wikitext,
    long ImportId,
    DateTime DownloadedAt
);

internal sealed record WikiRedirectEdge(string TargetTitle, long Hop);

internal sealed record WikiPageWorkItem(
    long PageRowId,
    string PageTitle,
    string NormalizedTitle,
    string DownloadStatus,
    DateTime? DownloadedAt,
    int AttemptCount
);

internal sealed record WikiTaxoboxData(
    long PageRowId,
    string? ScientificName,
    string? Rank,
    string? Kingdom,
    string? Phylum,
    string? Class,
    string? Order,
    string? Family,
    string? Subfamily,
    string? Tribe,
    string? Genus,
    string? Species,
    bool? IsMonotypic,
    string? DataJson
);

/// <summary>A downloaded page's wikitext (null for a redirect stored without text) and its stored taxobox fields.</summary>
internal sealed record WikiStoredTaxoboxPage(long PageRowId, string PageTitle, string? Wikitext, WikiTaxoboxData? Taxobox);

internal sealed record WikiMissingTitle(string Title, string NormalizedTitle, string ReasonCode, string? Notes, DateTime AttemptedAt);

internal sealed record TaxonWikiMatch(
    string TaxonSource,
    string TaxonIdentifier,
    string MatchStatus,
    long? PageRowId,
    string? CandidateTitle,
    string? NormalizedTitle,
    string? SynonymUsed,
    string? RedirectFinalTitle,
    string? MatchMethod,
    string? Notes,
    DateTime MatchedAt
);

internal sealed record TaxonWikiMatchAttempt(
    string TaxonSource,
    string TaxonIdentifier,
    int AttemptOrder,
    string CandidateTitle,
    string NormalizedTitle,
    string SourceHint,
    string Outcome,
    long? PageRowId,
    string? RedirectFinalTitle,
    string? Notes,
    DateTime AttemptedAt
);

/// What the enwiki all-titles dump import recorded: which dump it was (its Last-Modified date),
/// when it was imported, how many titles went in, and whether the import was cut short (--limit).
internal sealed record EnwikiDumpInfo(
    string? DumpDate,
    DateTime? ImportedAt,
    long TitleCount,
    string? Source,
    bool Partial
);

internal sealed record WikiCacheStats(
    long TotalPages,
    long CachedPages,
    long PendingPages,
    long FailedPages,
    long MissingPages,
    long MissingTitles,
    long MatchedTaxa
);
