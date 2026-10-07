using System;
using System.Collections.Generic;
using System.Globalization;
using BeastieBot3.Infrastructure;

// The Wikipedia cache's tables for `wikipedia fetch-species-lists` and `wikipedia report-species-lists`:
//   species_list_sources       where pages are found: a template (the pages that use it) or a
//                              category (its "List of" pages and its subcategories, to a depth),
//                              with when each was last read;
//   species_list_pages         the pages found;
//   species_list_page_sources  which sources found each page.
// The pages' wikitext is stored in wiki_pages like any other page.

namespace BeastieBot3.Wikipedia;

/// A place pages are found. Kind: SpeciesListSourceKinds. Depth: 0 for a starting category, one more
/// for each subcategory below it. FoundAt: when it was last read; null when never.
internal sealed record SpeciesListSource(string Name, string Kind, int Depth, DateTime? FoundAt, int? PageCount);

internal static class SpeciesListSourceKinds {
    public const string Template = "template";
    public const string Category = "category";
}

/// A page found, with the sources that found it.
internal sealed record SpeciesListPage(string Title, IReadOnlyList<string> Sources);

/// A downloaded page's text. Title: the article (after any redirect).
internal sealed record StoredPageText(long PageRowId, string Title, string Wikitext, long? RevisionId, DateTime? DownloadedAt);

internal sealed partial class WikipediaCacheStore {
    private void EnsureSpeciesListSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
CREATE TABLE IF NOT EXISTS species_list_sources (
    name TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    depth INTEGER NOT NULL,
    found_at TEXT,
    page_count INTEGER
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS species_list_pages (
    normalized_title TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    first_found_at TEXT NOT NULL,
    last_found_at TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS species_list_page_sources (
    normalized_title TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (normalized_title, source)
) WITHOUT ROWID;
""";
        command.ExecuteNonQuery();
    }

    /// Whether the tables exist (a read-only store does not create them).
    public bool HasSpeciesListTables() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'species_list_pages')";
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) == 1;
    }

    /// Adds a source, keeping the one already there.
    public void AddSpeciesListSource(string name, string kind, int depth) {
        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO species_list_sources (name, kind, depth) VALUES (@name, @kind, @depth)";
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@kind", kind);
        command.Parameters.AddWithValue("@depth", depth);
        command.ExecuteNonQuery();
    }

    public IReadOnlyList<SpeciesListSource> GetSpeciesListSources() {
        var sources = new List<SpeciesListSource>();
        if (!HasSpeciesListTables()) {
            return sources;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT name, kind, depth, found_at, page_count FROM species_list_sources ORDER BY depth, kind DESC, name";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            sources.Add(new SpeciesListSource(reader.GetString(0), reader.GetString(1), reader.GetInt32(2),
                reader.IsDBNull(3) ? null : StoredUtc.Parse(reader.GetString(3)),
                reader.IsDBNull(4) ? null : reader.GetInt32(4)));
        }
        return sources;
    }

    /// Saves what one source found: its pages, and the subcategories to read next (at depth + 1).
    public void SaveSpeciesListFind(SpeciesListSource source, IReadOnlyList<string> pageTitles, IReadOnlyList<string> subcategories, DateTime foundAt) {
        var now = foundAt.ToString("O");
        using var tx = _connection.BeginTransaction();
        using (var page = _connection.CreateCommand()) {
            page.Transaction = tx;
            page.CommandText = """
INSERT INTO species_list_pages (normalized_title, title, first_found_at, last_found_at) VALUES (@normalized, @title, @now, @now)
ON CONFLICT(normalized_title) DO UPDATE SET title = excluded.title, last_found_at = excluded.last_found_at;
INSERT OR IGNORE INTO species_list_page_sources (normalized_title, source) VALUES (@normalized, @source);
""";
            var normalized = page.Parameters.Add("@normalized", Microsoft.Data.Sqlite.SqliteType.Text);
            var title = page.Parameters.Add("@title", Microsoft.Data.Sqlite.SqliteType.Text);
            page.Parameters.AddWithValue("@now", now);
            page.Parameters.AddWithValue("@source", source.Name);
            foreach (var t in pageTitles) {
                normalized.Value = WikipediaTitleHelper.Normalize(t);
                title.Value = t;
                page.ExecuteNonQuery();
            }
        }
        using (var sub = _connection.CreateCommand()) {
            sub.Transaction = tx;
            sub.CommandText = "INSERT OR IGNORE INTO species_list_sources (name, kind, depth) VALUES (@name, @kind, @depth)";
            var name = sub.Parameters.Add("@name", Microsoft.Data.Sqlite.SqliteType.Text);
            sub.Parameters.AddWithValue("@kind", SpeciesListSourceKinds.Category);
            sub.Parameters.AddWithValue("@depth", source.Depth + 1);
            foreach (var category in subcategories) {
                name.Value = category;
                sub.ExecuteNonQuery();
            }
        }
        using (var done = _connection.CreateCommand()) {
            done.Transaction = tx;
            done.CommandText = "UPDATE species_list_sources SET found_at = @now, page_count = @count WHERE name = @name";
            done.Parameters.AddWithValue("@now", now);
            done.Parameters.AddWithValue("@count", pageTitles.Count);
            done.Parameters.AddWithValue("@name", source.Name);
            done.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// Every page found, with the sources that found it, in title order.
    public IReadOnlyList<SpeciesListPage> GetSpeciesListPages() {
        var pages = new List<SpeciesListPage>();
        if (!HasSpeciesListTables()) {
            return pages;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = """
SELECT p.title, group_concat(s.source, char(31))
FROM species_list_pages p
LEFT JOIN species_list_page_sources s ON s.normalized_title = p.normalized_title
GROUP BY p.normalized_title
ORDER BY p.normalized_title
""";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var sources = reader.IsDBNull(1) ? [] : reader.GetString(1).Split('\u001f');
            pages.Add(new SpeciesListPage(reader.GetString(0), sources));
        }
        return pages;
    }

    /// The download state of a title's own row: null when it has none.
    public (string Status, DateTime? DownloadedAt)? GetDownloadState(string title) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT download_status, downloaded_at FROM wiki_pages WHERE normalized_title = @title";
        command.Parameters.AddWithValue("@title", WikipediaTitleHelper.Normalize(title));
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return (reader.GetString(0), reader.IsDBNull(1) ? null : StoredUtc.Parse(reader.GetString(1)));
    }

    /// The text of the downloaded article a title leads to (through redirects); null when it is not downloaded.
    public StoredPageText? ReadArticleText(string title) {
        var article = ResolveDownloadedArticle(title, readTaxobox: false);
        return article is null ? null : ReadPageText(article.PageRowId);
    }

    public StoredPageText? ReadPageText(long pageRowId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT page_title, wikitext, latest_revision_id, downloaded_at FROM wiki_pages WHERE id = @id AND wikitext IS NOT NULL";
        command.Parameters.AddWithValue("@id", pageRowId);
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return new StoredPageText(pageRowId, reader.GetString(0), reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : StoredUtc.Parse(reader.GetString(3)));
    }

    /// The articles of the public site's groups that `wikipedia fetch-group-titles` reached.
    public IReadOnlyList<string> GetGroupArticleTitles() {
        var titles = new List<string>();
        using var exists = _connection.CreateCommand();
        exists.CommandText = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wiki_incoming_redirect_fetches')";
        if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
            return titles;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT resolved_title FROM wiki_incoming_redirect_fetches WHERE resolved_title IS NOT NULL ORDER BY resolved_title";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            titles.Add(reader.GetString(0));
        }
        return titles;
    }
}
