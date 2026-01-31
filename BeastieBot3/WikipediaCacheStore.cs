using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class WikipediaCacheStore : IDisposable {
    private readonly SqliteConnection _connection;
    private readonly ApiImportMetadataStore _importStore;

    private WikipediaCacheStore(SqliteConnection connection) {
        _connection = connection;
        _importStore = new ApiImportMetadataStore(connection);
    }

    public static WikipediaCacheStore Open(string databasePath) {
        if (string.IsNullOrWhiteSpace(databasePath)) {
            throw new ArgumentException("Database path must be provided", nameof(databasePath));
        }

        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directory)) {
            Directory.CreateDirectory(directory);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA journal_mode = WAL;";
            pragma.ExecuteNonQuery();
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        var store = new WikipediaCacheStore(connection);
        store._importStore.EnsureSchema();
        store.EnsureSchema();
        return store;
    }

    public void Dispose() => _connection.Dispose();

    public long BeginImport(string url) => _importStore.BeginImport(url);

    public void CompleteImportSuccess(long importId, int httpStatus, long payloadBytes, TimeSpan duration) =>
        _importStore.CompleteImportSuccess(importId, httpStatus, payloadBytes, duration);

    public void CompleteImportFailure(long importId, string errorMessage, int? statusCode, TimeSpan duration) =>
        _importStore.CompleteImportFailure(importId, errorMessage, statusCode, duration);

    private void EnsureSchema() {
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
    import_id INTEGER REFERENCES import_metadata(id) ON DELETE SET NULL,
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
CREATE TABLE IF NOT EXISTS wiki_redirect_edges (
    page_row_id INTEGER NOT NULL REFERENCES wiki_pages(id) ON DELETE CASCADE,
    hop INTEGER NOT NULL,
    target_title TEXT NOT NULL,
    target_page_row_id INTEGER REFERENCES wiki_pages(id) ON DELETE SET NULL,
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

    public IReadOnlyList<WikiPageWorkItem> GetPendingPages(int limit, DateTime? refreshThreshold) {
        if (limit <= 0) {
            return Array.Empty<WikiPageWorkItem>();
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT id, IFNULL(page_title, normalized_title), normalized_title, download_status, downloaded_at, attempt_count
FROM wiki_pages
WHERE download_status IN (@pending, @failed, @missing)
   OR (@refresh IS NOT NULL AND downloaded_at IS NOT NULL AND downloaded_at < @refresh)
ORDER BY CASE download_status WHEN @pending THEN 0 WHEN @failed THEN 1 WHEN @missing THEN 2 ELSE 3 END,
         IFNULL(downloaded_at, '0000-01-01T00:00:00Z'),
         id
LIMIT @limit
""";
        command.Parameters.AddWithValue("@pending", WikiPageDownloadStatus.Pending);
        command.Parameters.AddWithValue("@failed", WikiPageDownloadStatus.Failed);
        command.Parameters.AddWithValue("@missing", WikiPageDownloadStatus.Missing);
        command.Parameters.AddWithValue("@refresh", refreshThreshold?.ToString("O") ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@limit", limit);

        var list = new List<WikiPageWorkItem>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var downloadedValue = reader.IsDBNull(4) ? null : reader.GetString(4);
            DateTime? downloadedAt = null;
            if (!string.IsNullOrEmpty(downloadedValue) && DateTime.TryParse(downloadedValue, out var parsed)) {
                downloadedAt = parsed;
            }

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
                "INSERT INTO wiki_redirect_edges(page_row_id, hop, target_title, target_page_row_id) VALUES (@page,@hop,@title,@target)";
            var pageParam = insert.Parameters.Add("@page", SqliteType.Integer);
            var hopParam = insert.Parameters.Add("@hop", SqliteType.Integer);
            var titleParam = insert.Parameters.Add("@title", SqliteType.Text);
            var targetParam = insert.Parameters.Add("@target", SqliteType.Integer);
            pageParam.Value = pageRowId;
            foreach (var edge in edges) {
                hopParam.Value = edge.Hop;
                titleParam.Value = edge.TargetTitle;
                targetParam.Value = edge.TargetPageRowId.HasValue ? edge.TargetPageRowId.Value : DBNull.Value;
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
        UpdateReference("UPDATE wiki_redirect_edges SET target_page_row_id=@target WHERE target_page_row_id=@source");

        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM wiki_pages WHERE id=@id";
            delete.Parameters.AddWithValue("@id", sourcePageRowId);
            delete.ExecuteNonQuery();
        }

        tx.Commit();
    }

    public void UpsertTaxoboxData(WikiTaxoboxData data) {
        if (data is null) {
            throw new ArgumentNullException(nameof(data));
        }

        using var command = _connection.CreateCommand();
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

    public void DeleteTaxoboxData(long pageRowId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM wiki_taxobox_data WHERE page_row_id=@id";
        command.Parameters.AddWithValue("@id", pageRowId);
        command.ExecuteNonQuery();
    }

    public WikiTaxoboxData? GetTaxoboxData(long pageRowId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
SELECT page_row_id, scientific_name, rank, kingdom, phylum, class_name, order_name, family, subfamily, tribe, genus, species, is_monotypic, data_json
FROM wiki_taxobox_data
WHERE page_row_id=@id
LIMIT 1
""";
        command.Parameters.AddWithValue("@id", pageRowId);
        using var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        if (!reader.Read()) {
            return null;
        }

        bool? ParseBool(int ordinal) {
            if (reader.IsDBNull(ordinal)) {
                return null;
            }

            var value = reader.GetInt64(ordinal);
            return value switch {
                0 => false,
                1 => true,
                _ => null
            };
        }

        return new WikiTaxoboxData(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetString(11),
            ParseBool(12),
            reader.IsDBNull(13) ? null : reader.GetString(13));
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
    public const string Skipped = "skipped";
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

internal sealed record WikiRedirectEdge(string TargetTitle, long Hop, long? TargetPageRowId);

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

internal sealed record WikiCacheStats(
    long TotalPages,
    long CachedPages,
    long PendingPages,
    long FailedPages,
    long MissingPages,
    long MissingTitles,
    long MatchedTaxa
);
