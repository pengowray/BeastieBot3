using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The DOI cache of `iucn resolve-dois` for `site build-db`.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ DOIs from `iucn resolve-dois`

    /// Reads `iucn resolve-dois`'s cache (table doi_check: assessment_id, taxon_id, doi, checked_at,
    /// candidates_tried; doi NULL when no candidate resolved) into dois.Resolved. A file without the
    /// table, or with a table the build cannot read, gives no DOIs and a warning.
    public static void ReadDoiCache(string path, SiteDoiSources dois, SiteBuildStats stats, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'doi_check'";
            if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
                stats.Warnings.Add($"The DOI cache {path} has no doi_check table, so no DOIs from `iucn resolve-dois` were used.");
                return;
            }
        }
        ReadCrossrefTitles(connection, path, dois, stats, cancellationToken);
        try {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT assessment_id, doi, checked_at FROM doi_check";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                stats.DoiChecksRead++;
                if (!reader.IsDBNull(2) && StoredUtc.Parse(reader.GetString(2)) is { } checkedAt
                    && (stats.DoiCheckedTo is null || checkedAt > stats.DoiCheckedTo)) {
                    stats.DoiCheckedTo = checkedAt;
                }
                if (reader.IsDBNull(0) || reader.IsDBNull(1) || SiteBuildRules.NullIfBlank(reader.GetString(1)) is not { } doi) {
                    continue;
                }
                stats.DoiChecksWithDoi++;
                dois.Resolved[reader.GetInt64(0)] = doi;
            }
        } catch (SqliteException ex) {
            dois.Resolved.Clear();
            stats.DoiChecksRead = 0;
            stats.DoiChecksWithDoi = 0;
            stats.DoiCheckedTo = null;
            stats.Warnings.Add($"The DOI cache {path} could not be read, so no DOIs from `iucn resolve-dois` were used: {ex.Message}");
        }
    }

    // The titles Crossref registered for IUCN DOIs (crossref_works.title), which `iucn resolve-dois`
    // stores from October 2026. A cache without them gives none, and a warning.
    private static void ReadCrossrefTitles(SqliteConnection connection, string path, SiteDoiSources dois, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'crossref_works'";
            if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0 || !Iucn.Doi.IucnDoiCacheStore.HasCrossrefTitles(connection)) {
                stats.Warnings.Add($"The DOI cache {path} has no titles from Crossref, so new Wikidata items use IUCN's citation name, which for an older assessment may be newer than the name in the assessment's title. To add the titles, run iucn resolve-dois --refresh-crossref.");
                return;
            }
        }
        // The day each DOI was created tells whether its title has the name the assessment was
        // published under (IucnCitationParts.RegisteredNameIsFromPublication).
        var hasCreated = Iucn.Doi.IucnDoiCacheStore.HasCrossrefCreated(connection);
        if (!hasCreated) {
            stats.Warnings.Add($"The DOI cache {path} has no creation dates from Crossref, so no name in a DOI title is shown as the name an assessment was published under. To add them, run iucn resolve-dois --refresh-crossref --doi-org never.");
        }
        using var command = connection.CreateCommand();
        command.CommandText = hasCreated
            ? "SELECT doi, assessment_id, title, created FROM crossref_works WHERE title IS NOT NULL"
            : "SELECT doi, assessment_id, title, NULL FROM crossref_works WHERE title IS NOT NULL";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            dois.CrossrefTitles[reader.GetString(0)] = (reader.GetInt64(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3));
        }
    }
}
