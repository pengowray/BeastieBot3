using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The DOI cache written by `iucn resolve-dois` (Datastore:IUCN_doi_cache_sqlite, default
// iucn_doi_cache.sqlite). Tables:
//
//   doi_check          one row per assessment checked: the DOI found, or NULL when none was found.
//                      candidates_tried counts the doi.org lookups made for it (0 when Crossref's
//                      list of IUCN DOIs had it). Read by other commands; keep its columns as they are.
//   doi_check_detail   how each doi_check row was decided: found_by ("crossref" or "doi.org"), the
//                      scope it was checked under, its year published and a note.
//   doi_lookup_log     every doi.org handle lookup: the DOI asked for, the HTTP status, the handle
//                      responseCode (1 = exists, 100 = not found), the URL the DOI points to and
//                      what was decided about it.
//   crossref_works     Crossref's list of DOIs under IUCN's prefix 10.2305 that name a Red List
//                      assessment ("...RLTS.T<taxon>A<assessment>..."), with the ids in the DOI and
//                      the ids in the assessment page URL it points to, and the title registered
//                      for it ("Canis mesomelas: Hoffmann, M."; NULL in rows downloaded before the
//                      title column was added). For an errata version published 2015 to 2018 the
//                      ids differ: the DOI keeps the assessment id of the assessment it corrects and
//                      points to the errata version's page.
//   crossref_listings  one row per download of that list: when it started and finished, how many
//                      works Crossref reported and how many were read.

namespace BeastieBot3.Iucn.Doi;

/// How a doi_check row was decided.
internal static class DoiFoundBy {
    public const string Crossref = "crossref";
    public const string DoiOrg = "doi.org";
}

/// One doi_check row.
internal sealed record DoiCheckRow(long AssessmentId, long TaxonId, string? Doi, DateTime CheckedAtUtc, int CandidatesTried);

/// One work from Crossref's list, already parsed. Title: the registered title as Crossref gives it,
/// HTML entities included; null when Crossref gives none. Created: the day the DOI was registered
/// with Crossref (Crossref's "created" date, "2015-09-10"); depositing the record again changes its
/// "deposited" date and keeps its title, so the title has the names current on this day.
internal sealed record CrossrefIucnWork(
    string Doi,
    long TaxonId,
    long AssessmentId,
    string? Release,
    string? Language,
    string? Url,
    long? UrlTaxonId,
    long? UrlAssessmentId,
    string? Title = null,
    string? Created = null);

/// A Crossref list download.
internal sealed record CrossrefListing(long Id, DateTime StartedAtUtc, DateTime? CompletedAtUtc, long? TotalResults, long WorksSeen, int Requests);

/// One doi.org lookup, as logged.
internal sealed record DoiLookupLogRow(long AssessmentId, string Doi, DateTime CheckedAtUtc, int? HttpStatus, int? ResponseCode, string? Url, string Verdict);

internal sealed class IucnDoiCacheStore : SqliteStore {
    private IucnDoiCacheStore(SqliteConnection connection) : base(connection) {
    }

    public static IucnDoiCacheStore Open(string databasePath) {
        var store = new IucnDoiCacheStore(OpenConnection(databasePath));
        store.EnsureSchema();
        return store;
    }

    /// Test seam: a store over a caller-owned connection, such as an in-memory database.
    internal static IucnDoiCacheStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new IucnDoiCacheStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS doi_check (
                assessment_id INTEGER PRIMARY KEY,
                taxon_id INTEGER NOT NULL,
                doi TEXT,
                checked_at TEXT NOT NULL,
                candidates_tried INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS doi_check_detail (
                assessment_id INTEGER PRIMARY KEY REFERENCES doi_check(assessment_id) ON DELETE CASCADE,
                found_by TEXT,
                scope TEXT,
                year_published INTEGER,
                note TEXT
            );
            CREATE TABLE IF NOT EXISTS doi_lookup_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assessment_id INTEGER NOT NULL,
                doi TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                http_status INTEGER,
                response_code INTEGER,
                url TEXT,
                verdict TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_doi_lookup_log_assessment ON doi_lookup_log(assessment_id);
            CREATE TABLE IF NOT EXISTS crossref_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                total_results INTEGER,
                works_seen INTEGER NOT NULL DEFAULT 0,
                requests INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS crossref_works (
                doi TEXT PRIMARY KEY,
                taxon_id INTEGER NOT NULL,
                assessment_id INTEGER NOT NULL,
                release TEXT,
                language TEXT,
                url TEXT,
                url_taxon_id INTEGER,
                url_assessment_id INTEGER,
                listing_id INTEGER NOT NULL,
                title TEXT,
                created TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_crossref_works_assessment ON crossref_works(assessment_id);
            CREATE INDEX IF NOT EXISTS idx_crossref_works_url_assessment ON crossref_works(url_assessment_id);
            """;
        command.ExecuteNonQuery();
        // A cache made before titles were stored gets the column; its rows keep NULL until the
        // next download of Crossref's list.
        if (!HasCrossrefTitles(_connection)) {
            using var alter = _connection.CreateCommand();
            alter.CommandText = "ALTER TABLE crossref_works ADD COLUMN title TEXT";
            alter.ExecuteNonQuery();
        }
        // Likewise the day each DOI was created (October 2026).
        if (!HasCrossrefCreated(_connection)) {
            using var alter = _connection.CreateCommand();
            alter.CommandText = "ALTER TABLE crossref_works ADD COLUMN created TEXT";
            alter.ExecuteNonQuery();
        }
    }

    /// True when crossref_works has the created column (the day each DOI was created).
    public static bool HasCrossrefCreated(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM pragma_table_info('crossref_works') WHERE name = 'created'";
        return command.ExecuteScalar() is not null;
    }

    /// True when crossref_works has the title column. A cache made before it existed and opened
    /// read-only (`site build-db`) has not been migrated.
    public static bool HasCrossrefTitles(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM pragma_table_info('crossref_works') WHERE name = 'title'";
        return command.ExecuteScalar() is not null;
    }

    // ------------------------------------------------------------ doi_check

    /// Every doi_check row, keyed by assessment id.
    public Dictionary<long, DoiCheckRow> ReadChecks() {
        var rows = new Dictionary<long, DoiCheckRow>();
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT assessment_id, taxon_id, doi, checked_at, candidates_tried FROM doi_check";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var row = new DoiCheckRow(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                StoredUtc.Parse(reader.GetString(3)) ?? DateTime.MinValue,
                reader.GetInt32(4));
            rows[row.AssessmentId] = row;
        }
        return rows;
    }

    public DoiCheckRow? GetCheck(long assessmentId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT assessment_id, taxon_id, doi, checked_at, candidates_tried FROM doi_check WHERE assessment_id = @id";
        command.Parameters.AddWithValue("@id", assessmentId);
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return new DoiCheckRow(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            StoredUtc.Parse(reader.GetString(3)) ?? DateTime.MinValue,
            reader.GetInt32(4));
    }

    /// Saves one assessment's result with how it was found and its lookups, in one transaction.
    public void SaveCheck(DoiCheckRow row, string? foundBy, string? scope, int? yearPublished, string? note,
        IReadOnlyList<DoiLookupLogRow> lookups) {
        using var transaction = _connection.BeginTransaction();
        using (var command = _connection.CreateCommand()) {
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO doi_check (assessment_id, taxon_id, doi, checked_at, candidates_tried)
                VALUES (@aid, @tid, @doi, @at, @tried)
                ON CONFLICT(assessment_id) DO UPDATE SET
                    taxon_id = excluded.taxon_id,
                    doi = excluded.doi,
                    checked_at = excluded.checked_at,
                    candidates_tried = excluded.candidates_tried
                """;
            command.Parameters.AddWithValue("@aid", row.AssessmentId);
            command.Parameters.AddWithValue("@tid", row.TaxonId);
            command.Parameters.AddWithValue("@doi", (object?)row.Doi ?? DBNull.Value);
            command.Parameters.AddWithValue("@at", FormatUtc(row.CheckedAtUtc));
            command.Parameters.AddWithValue("@tried", row.CandidatesTried);
            command.ExecuteNonQuery();
        }
        using (var command = _connection.CreateCommand()) {
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO doi_check_detail (assessment_id, found_by, scope, year_published, note)
                VALUES (@aid, @by, @scope, @year, @note)
                ON CONFLICT(assessment_id) DO UPDATE SET
                    found_by = excluded.found_by,
                    scope = excluded.scope,
                    year_published = excluded.year_published,
                    note = excluded.note
                """;
            command.Parameters.AddWithValue("@aid", row.AssessmentId);
            command.Parameters.AddWithValue("@by", (object?)foundBy ?? DBNull.Value);
            command.Parameters.AddWithValue("@scope", (object?)scope ?? DBNull.Value);
            command.Parameters.AddWithValue("@year", (object?)yearPublished ?? DBNull.Value);
            command.Parameters.AddWithValue("@note", (object?)note ?? DBNull.Value);
            command.ExecuteNonQuery();
        }
        AddLookups(lookups, transaction);
        transaction.Commit();
    }

    /// Logs doi.org lookups that did not lead to a saved result (a run stopped by an error).
    public void LogLookups(IReadOnlyList<DoiLookupLogRow> lookups) {
        if (lookups.Count == 0) {
            return;
        }
        using var transaction = _connection.BeginTransaction();
        AddLookups(lookups, transaction);
        transaction.Commit();
    }

    private void AddLookups(IReadOnlyList<DoiLookupLogRow> lookups, SqliteTransaction transaction) {
        if (lookups.Count == 0) {
            return;
        }
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO doi_lookup_log (assessment_id, doi, checked_at, http_status, response_code, url, verdict)
            VALUES (@aid, @doi, @at, @status, @code, @url, @verdict)
            """;
        var aid = command.Parameters.Add("@aid", SqliteType.Integer);
        var doi = command.Parameters.Add("@doi", SqliteType.Text);
        var at = command.Parameters.Add("@at", SqliteType.Text);
        var status = command.Parameters.Add("@status", SqliteType.Integer);
        var code = command.Parameters.Add("@code", SqliteType.Integer);
        var url = command.Parameters.Add("@url", SqliteType.Text);
        var verdict = command.Parameters.Add("@verdict", SqliteType.Text);
        foreach (var lookup in lookups) {
            aid.Value = lookup.AssessmentId;
            doi.Value = lookup.Doi;
            at.Value = FormatUtc(lookup.CheckedAtUtc);
            status.Value = (object?)lookup.HttpStatus ?? DBNull.Value;
            code.Value = (object?)lookup.ResponseCode ?? DBNull.Value;
            url.Value = (object?)lookup.Url ?? DBNull.Value;
            verdict.Value = lookup.Verdict;
            command.ExecuteNonQuery();
        }
    }

    public List<DoiLookupLogRow> ReadLookups(long assessmentId) {
        var rows = new List<DoiLookupLogRow>();
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT assessment_id, doi, checked_at, http_status, response_code, url, verdict
            FROM doi_lookup_log WHERE assessment_id = @id ORDER BY id
            """;
        command.Parameters.AddWithValue("@id", assessmentId);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            rows.Add(new DoiLookupLogRow(
                reader.GetInt64(0),
                reader.GetString(1),
                StoredUtc.Parse(reader.GetString(2)) ?? DateTime.MinValue,
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetString(6)));
        }
        return rows;
    }

    public string? GetFoundBy(long assessmentId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT found_by FROM doi_check_detail WHERE assessment_id = @id";
        command.Parameters.AddWithValue("@id", assessmentId);
        return command.ExecuteScalar() as string;
    }

    // ------------------------------------------------------------ Crossref

    public long StartListing(DateTime startedAtUtc) {
        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT INTO crossref_listings (started_at) VALUES (@at); SELECT last_insert_rowid();";
        command.Parameters.AddWithValue("@at", FormatUtc(startedAtUtc));
        return (long)command.ExecuteScalar()!;
    }

    /// Adds or updates one page of works and the listing's running counts, in one transaction.
    public void AddListingPage(long listingId, IReadOnlyList<CrossrefIucnWork> works, long? totalResults, int itemsOnPage) {
        using var transaction = _connection.BeginTransaction();
        using (var command = _connection.CreateCommand()) {
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO crossref_works (doi, taxon_id, assessment_id, release, language, url, url_taxon_id, url_assessment_id, listing_id, title, created)
                VALUES (@doi, @tid, @aid, @release, @lang, @url, @utid, @uaid, @listing, @title, @created)
                ON CONFLICT(doi) DO UPDATE SET
                    taxon_id = excluded.taxon_id,
                    assessment_id = excluded.assessment_id,
                    release = excluded.release,
                    language = excluded.language,
                    url = excluded.url,
                    url_taxon_id = excluded.url_taxon_id,
                    url_assessment_id = excluded.url_assessment_id,
                    listing_id = excluded.listing_id,
                    title = excluded.title,
                    created = excluded.created
                """;
            var doi = command.Parameters.Add("@doi", SqliteType.Text);
            var tid = command.Parameters.Add("@tid", SqliteType.Integer);
            var aid = command.Parameters.Add("@aid", SqliteType.Integer);
            var release = command.Parameters.Add("@release", SqliteType.Text);
            var lang = command.Parameters.Add("@lang", SqliteType.Text);
            var url = command.Parameters.Add("@url", SqliteType.Text);
            var urlTaxon = command.Parameters.Add("@utid", SqliteType.Integer);
            var urlAssessment = command.Parameters.Add("@uaid", SqliteType.Integer);
            var title = command.Parameters.Add("@title", SqliteType.Text);
            var created = command.Parameters.Add("@created", SqliteType.Text);
            command.Parameters.AddWithValue("@listing", listingId);
            foreach (var work in works) {
                doi.Value = work.Doi;
                tid.Value = work.TaxonId;
                aid.Value = work.AssessmentId;
                release.Value = (object?)work.Release ?? DBNull.Value;
                lang.Value = (object?)work.Language ?? DBNull.Value;
                url.Value = (object?)work.Url ?? DBNull.Value;
                urlTaxon.Value = (object?)work.UrlTaxonId ?? DBNull.Value;
                urlAssessment.Value = (object?)work.UrlAssessmentId ?? DBNull.Value;
                title.Value = (object?)work.Title ?? DBNull.Value;
                created.Value = (object?)work.Created ?? DBNull.Value;
                command.ExecuteNonQuery();
            }
        }
        using (var command = _connection.CreateCommand()) {
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE crossref_listings
                SET works_seen = works_seen + @items, requests = requests + 1, total_results = COALESCE(@total, total_results)
                WHERE id = @id
                """;
            command.Parameters.AddWithValue("@items", itemsOnPage);
            command.Parameters.AddWithValue("@total", (object?)totalResults ?? DBNull.Value);
            command.Parameters.AddWithValue("@id", listingId);
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    public void CompleteListing(long listingId, DateTime completedAtUtc) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE crossref_listings SET completed_at = @at WHERE id = @id";
        command.Parameters.AddWithValue("@at", FormatUtc(completedAtUtc));
        command.Parameters.AddWithValue("@id", listingId);
        command.ExecuteNonQuery();
    }

    /// The newest listing that finished, or null.
    public CrossrefListing? LastCompletedListing() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT id, started_at, completed_at, total_results, works_seen, requests
            FROM crossref_listings WHERE completed_at IS NOT NULL ORDER BY id DESC LIMIT 1
            """;
        using var reader = command.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return new CrossrefListing(
            reader.GetInt64(0),
            StoredUtc.Parse(reader.GetString(1)) ?? DateTime.MinValue,
            StoredUtc.Parse(reader.GetString(2)),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.GetInt64(4),
            reader.GetInt32(5));
    }

    public long CountCrossrefWorks() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM crossref_works";
        return (long)command.ExecuteScalar()!;
    }

    /// Every work whose DOI names the assessment or whose page URL is the assessment's page.
    public List<CrossrefIucnWork> CrossrefWorksFor(long assessmentId) {
        var works = new List<CrossrefIucnWork>();
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT doi, taxon_id, assessment_id, release, language, url, url_taxon_id, url_assessment_id, title
            FROM crossref_works WHERE assessment_id = @id
            UNION
            SELECT doi, taxon_id, assessment_id, release, language, url, url_taxon_id, url_assessment_id, title
            FROM crossref_works WHERE url_assessment_id = @id
            ORDER BY doi
            """;
        command.Parameters.AddWithValue("@id", assessmentId);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            works.Add(new CrossrefIucnWork(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }
        return works;
    }

    internal static string FormatUtc(DateTime value) =>
        DateTime.SpecifyKind(value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : value, DateTimeKind.Utc)
            .ToString("O", CultureInfo.InvariantCulture);
}
