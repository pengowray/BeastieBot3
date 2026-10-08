using System;
using System.Collections.Generic;
using System.Globalization;
using BeastieBot3.Infrastructure;

// The Wikipedia cache's tables for the articles of higher taxa (`wikipedia fetch-group-titles`):
// the redirects that point at an article, which titles have been asked about, and the counts of
// the last run that the web UI's workflow light reads.

namespace BeastieBot3.Wikipedia;

internal sealed partial class WikipediaCacheStore {
    /// <summary>
    /// The downloaded article a title leads to: the title's own page, or the page its redirect points
    /// at (up to three hops). Null when the title, or a page on the way, is not downloaded.
    /// A downloaded article's row can have <c>redirect_target</c> set to its own title (it was first
    /// reached through a redirect), so a redirect is a row whose target is a different title.
    /// </summary>
    public WikiGroupArticle? ResolveDownloadedArticle(string title, bool readTaxobox = true) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        var redirected = false;
        for (var hop = 0; hop < 4 && normalized.Length > 0; hop++) {
            var page = GetPageByNormalizedTitle(normalized);
            if (page is null || page.DownloadStatus != WikiPageDownloadStatus.Cached) {
                return null;
            }
            var target = page.RedirectTarget is { } t ? WikipediaTitleHelper.Normalize(t) : null;
            if (target is not null && !string.Equals(target, page.NormalizedTitle, StringComparison.Ordinal)) {
                normalized = target;
                redirected = true;
                continue;
            }
            return new WikiGroupArticle(page.PageRowId, page.PageTitle, page.NormalizedTitle, page.IsDisambiguation,
                readTaxobox ? TaxoboxName(GetTaxoboxData(page.PageRowId)) : null, redirected);
        }
        return null;
    }

    // The taxon of the taxobox: "genus species" for a speciesbox (the Harpy eagle article gives genus
    // Harpia and species harpyja and no scientific name; the Calabar python article's stored
    // scientific name is its English name), else the stored scientific name.
    private static string? TaxoboxName(WikiTaxoboxData? taxobox) {
        if (taxobox is null) {
            return null;
        }
        if (!string.IsNullOrWhiteSpace(taxobox.Genus) && !string.IsNullOrWhiteSpace(taxobox.Species)) {
            var species = taxobox.Species.Trim();
            var genus = taxobox.Genus.Trim();
            return species.StartsWith(genus + " ", StringComparison.OrdinalIgnoreCase) ? species : $"{genus} {species}";
        }
        return string.IsNullOrWhiteSpace(taxobox.ScientificName) ? null : taxobox.ScientificName.Trim();
    }

    /// Whether the list of every article title (enwiki_dump_titles) is there and has rows.
    public bool HasTitleListRows() {
        if (!HasTitleList()) {
            return false;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT EXISTS (SELECT 1 FROM enwiki_dump_titles)";
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) == 1;
    }

    /// Replaces the redirects stored for <paramref name="title"/>'s page and records that it was asked about.
    public void SaveIncomingRedirects(string title, string? resolvedTitle, IReadOnlyList<WikipediaIncomingRedirect> redirects, DateTime fetchedAt) {
        var normalized = WikipediaTitleHelper.Normalize(title);
        var resolved = resolvedTitle is null ? null : WikipediaTitleHelper.Normalize(resolvedTitle);
        using var tx = _connection.BeginTransaction();
        if (resolved is not null) {
            using (var delete = _connection.CreateCommand()) {
                delete.Transaction = tx;
                delete.CommandText = "DELETE FROM wiki_incoming_redirects WHERE target_title = @target";
                delete.Parameters.AddWithValue("@target", resolved);
                delete.ExecuteNonQuery();
            }
            using var insert = _connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT OR IGNORE INTO wiki_incoming_redirects (target_title, redirect_title, fragment) VALUES (@target, @title, @fragment)";
            var target = insert.Parameters.Add("@target", Microsoft.Data.Sqlite.SqliteType.Text);
            var name = insert.Parameters.Add("@title", Microsoft.Data.Sqlite.SqliteType.Text);
            var fragment = insert.Parameters.Add("@fragment", Microsoft.Data.Sqlite.SqliteType.Text);
            foreach (var redirect in redirects) {
                target.Value = resolved;
                name.Value = redirect.Title;
                fragment.Value = (object?)redirect.Fragment ?? DBNull.Value;
                insert.ExecuteNonQuery();
            }
        }
        using (var mark = _connection.CreateCommand()) {
            mark.Transaction = tx;
            mark.CommandText = """
                INSERT INTO wiki_incoming_redirect_fetches (title, resolved_title, redirect_count, fetched_at)
                VALUES (@title, @resolved, @count, @at)
                ON CONFLICT(title) DO UPDATE SET resolved_title = excluded.resolved_title,
                    redirect_count = excluded.redirect_count, fetched_at = excluded.fetched_at
                """;
            mark.Parameters.AddWithValue("@title", normalized);
            mark.Parameters.AddWithValue("@resolved", (object?)resolved ?? DBNull.Value);
            mark.Parameters.AddWithValue("@count", redirects.Count);
            mark.Parameters.AddWithValue("@at", fetchedAt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
            mark.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// When each title's incoming redirects were last asked for, by normalized title.
    public IReadOnlyDictionary<string, DateTime> ReadIncomingRedirectFetches() {
        var result = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        if (!TableExists("wiki_incoming_redirect_fetches")) {
            return result;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT title, fetched_at FROM wiki_incoming_redirect_fetches";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (StoredUtc.Parse(reader.GetString(1)) is { } at) {
                result[reader.GetString(0)] = at;
            }
        }
        return result;
    }

    /// <summary>
    /// The redirects that point at <paramref name="articleTitle"/>, as last downloaded, followed
    /// through the page the title reached when it was asked about. Null when it has not been asked about.
    /// </summary>
    public IReadOnlyList<WikipediaIncomingRedirect>? ReadIncomingRedirects(string articleTitle) {
        var normalized = WikipediaTitleHelper.Normalize(articleTitle);
        if (!TableExists("wiki_incoming_redirect_fetches")) {
            return null;
        }
        string? resolved;
        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT resolved_title FROM wiki_incoming_redirect_fetches WHERE title = @title";
            command.Parameters.AddWithValue("@title", normalized);
            using var reader = command.ExecuteReader();
            if (!reader.Read()) {
                return null;
            }
            resolved = reader.IsDBNull(0) ? null : reader.GetString(0);
        }
        var list = new List<WikipediaIncomingRedirect>();
        if (resolved is null) {
            return list;
        }
        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT redirect_title, fragment FROM wiki_incoming_redirects WHERE target_title = @target ORDER BY redirect_title";
            command.Parameters.AddWithValue("@target", resolved);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                list.Add(new WikipediaIncomingRedirect(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
            }
        }
        return list;
    }

    public void SetGroupTitleStatus(IReadOnlyDictionary<string, string> values) {
        using var tx = _connection.BeginTransaction();
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "INSERT INTO wiki_group_title_status (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value = excluded.value";
        var key = command.Parameters.Add("@key", Microsoft.Data.Sqlite.SqliteType.Text);
        var value = command.Parameters.Add("@value", Microsoft.Data.Sqlite.SqliteType.Text);
        foreach (var (k, v) in values) {
            key.Value = k;
            value.Value = v;
            command.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// The counts the last run stored; empty when there was none (or the table is not there).
    public IReadOnlyDictionary<string, string> ReadGroupTitleStatus() {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!TableExists("wiki_group_title_status")) {
            return result;
        }
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT key, value FROM wiki_group_title_status";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            result[reader.GetString(0)] = reader.GetString(1);
        }
        return result;
    }

    // A read-only connection can be on a cache made before these tables existed.
    private bool TableExists(string name) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name";
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }
}

/// <summary>
/// A downloaded article that a group's title leads to. TaxoboxName is the scientific name in its
/// taxobox (null when it has none); Redirected is true when the title is a redirect to it.
/// </summary>
internal sealed record WikiGroupArticle(long PageRowId, string Title, string NormalizedTitle, bool IsDisambiguation,
    string? TaxoboxName, bool Redirected) {
    /// Whether the taxobox's scientific name is <paramref name="name"/> (case, italics and a dagger ignored).
    public bool TaxoboxIs(string name) =>
        TaxoboxName is { } taxobox && string.Equals(CleanTaxoboxName(taxobox), CleanTaxoboxName(name), StringComparison.OrdinalIgnoreCase);

    /// Whether the taxobox is a species of genus <paramref name="genus"/> ("Komarekiona eatoni" for "Komarekiona").
    public bool TaxoboxIsSpeciesOf(string genus) =>
        TaxoboxName is { } taxobox && CleanTaxoboxName(taxobox).StartsWith(genus.Trim() + " ", StringComparison.OrdinalIgnoreCase);

    // "†Pteropodidae", "''Pteropus''", "Hylocitrea<ref name=...>", "Ficus (plant)" -> the bare name.
    private static string CleanTaxoboxName(string name) {
        var end = name.IndexOfAny(['<', '{']);
        var text = (end >= 0 ? name[..end] : name).Replace("'", string.Empty).Trim().TrimStart('†', '?');
        if (text.EndsWith(')') && text.LastIndexOf(" (", StringComparison.Ordinal) is > 0 and var bracket) {
            text = text[..bracket];
        }
        return string.Join(' ', text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }
}
