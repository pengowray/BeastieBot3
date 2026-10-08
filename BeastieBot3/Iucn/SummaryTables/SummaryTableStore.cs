using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The rows of IUCN's summary tables 7 and 9 (`iucn summary-tables`), in
// Datastore:IUCN_summary_tables_sqlite. One source_file row per PDF; its rows are replaced together
// when the file or the parser changes. Every cell is kept as printed beside the value read from it,
// so a row that could not be read is still there to look at (anomaly says what was not read).
//
//   source_file          one PDF: where it came from, its SHA-256, what the text above the table says
//   category_change      Table 7 rows: a species, its previous and new category, the reason, the version
//   possibly_extinct     Table 9 rows: a species tagged PE or PEW, the year of its first such
//                        assessment, the date last recorded in the wild

namespace BeastieBot3.Iucn.SummaryTables;

/// One stored file. Release is the release the list gives it; Priority its place in the list.
internal sealed record SummaryTableSource(
    long Id, string FileName, int Table, string Release, string Url, string Sha256, long Bytes, string? DownloadedAt,
    string ParsedAt, int ParserVersion, int Priority, string? Note, string? Title, string? LastUpdated,
    string? PeriodFrom, string? PeriodTo, bool DefinesError, int PageCount, int RowCount, int AnomalyCount, int UnreadCount);

/// A Table 7 row with the file it came from.
internal sealed record StoredCategoryChange(SummaryTableSource Source, CategoryChangeRow Row);

/// A Table 9 row with the file it came from.
internal sealed record StoredPossiblyExtinct(SummaryTableSource Source, PossiblyExtinctRow Row);

internal sealed class SummaryTableStore : SqliteStore {
    private SummaryTableStore(SqliteConnection connection) : base(connection) { }

    public static SummaryTableStore Open(string path) {
        var store = new SummaryTableStore(OpenConnection(path));
        store.EnsureSchema();
        return store;
    }

    /// Null when the file does not exist. Reads only: no schema work, any write fails.
    public static SummaryTableStore? OpenReadOnly(string path) {
        if (!File.Exists(path)) return null;
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        return new SummaryTableStore(connection);
    }

    internal static SummaryTableStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new SummaryTableStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS source_file (
                id INTEGER PRIMARY KEY,
                file_name TEXT NOT NULL UNIQUE,
                table_no INTEGER NOT NULL,
                release TEXT NOT NULL,
                url TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                downloaded_at TEXT,
                parsed_at TEXT NOT NULL,
                parser_version INTEGER NOT NULL,
                priority INTEGER NOT NULL,
                note TEXT,
                title TEXT,
                last_updated TEXT,
                period_from TEXT,
                period_to TEXT,
                defines_error INTEGER NOT NULL DEFAULT 0,
                page_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                anomaly_count INTEGER NOT NULL,
                unread_count INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS category_change (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
                page INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                group_name TEXT,
                section TEXT,
                scientific_name TEXT NOT NULL,
                common_name TEXT,
                old_category_text TEXT,
                new_category_text TEXT,
                old_category TEXT,
                old_tag TEXT,
                new_category TEXT,
                new_tag TEXT,
                reason_text TEXT,
                reason TEXT,
                version_text TEXT,
                version TEXT,
                version_year INTEGER,
                anomaly TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_category_change_source ON category_change(source_id);
            CREATE INDEX IF NOT EXISTS idx_category_change_name ON category_change(scientific_name);
            CREATE TABLE IF NOT EXISTS possibly_extinct (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL REFERENCES source_file(id) ON DELETE CASCADE,
                page INTEGER NOT NULL,
                line_no INTEGER NOT NULL,
                group_name TEXT,
                scientific_name TEXT NOT NULL,
                common_name TEXT,
                category_text TEXT,
                tag TEXT,
                year_assessed_text TEXT,
                year_assessed INTEGER,
                last_recorded TEXT,
                anomaly TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_possibly_extinct_source ON possibly_extinct(source_id);
            CREATE INDEX IF NOT EXISTS idx_possibly_extinct_name ON possibly_extinct(scientific_name);
            """;
        command.ExecuteNonQuery();
    }

    /// The stored files by file name.
    public Dictionary<string, SummaryTableSource> GetSources() {
        var sources = new Dictionary<string, SummaryTableSource>(StringComparer.OrdinalIgnoreCase);
        using var command = _connection.CreateCommand();
        command.CommandText = $"SELECT {SourceColumns} FROM source_file s ORDER BY s.priority, s.file_name";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var source = ReadSource(reader, 0);
            sources[source.FileName] = source;
        }
        return sources;
    }

    /// Keeps a file's place in the list in step with rules/iucn-summary-tables.yml.
    public void SetPriority(string fileName, int priority, string? note) {
        using var command = _connection.CreateCommand();
        command.CommandText = "UPDATE source_file SET priority = @p, note = @n WHERE file_name = @f AND (priority <> @p OR note IS NOT @n)";
        command.Parameters.AddWithValue("@p", priority);
        command.Parameters.AddWithValue("@n", (object?)note ?? DBNull.Value);
        command.Parameters.AddWithValue("@f", fileName);
        command.ExecuteNonQuery();
    }

    /// What is known about a file before its rows are written.
    public sealed record FileFacts(string FileName, int Table, string Release, string Url, string Sha256, long Bytes,
        string? DownloadedAt, int ParserVersion, int Priority, string? Note, int PageCount);

    public void ReplaceTable7(FileFacts file, SummaryTableParse<CategoryChangeRow> parse) {
        using var transaction = _connection.BeginTransaction();
        var sourceId = SaveSource(transaction, file, parse.Info, parse.Rows.Count, Count(parse.Rows, r => r.Anomaly is not null), parse.Unread.Count);
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO category_change (source_id, page, line_no, group_name, section, scientific_name, common_name,
                old_category_text, new_category_text, old_category, old_tag, new_category, new_tag,
                reason_text, reason, version_text, version, version_year, anomaly)
            VALUES (@s, @page, @line, @group, @section, @sci, @common, @oldText, @newText, @old, @oldTag, @new, @newTag,
                @reasonText, @reason, @versionText, @version, @year, @anomaly)
            """;
        var p = AddParameters(command, "@s", "@page", "@line", "@group", "@section", "@sci", "@common", "@oldText", "@newText",
            "@old", "@oldTag", "@new", "@newTag", "@reasonText", "@reason", "@versionText", "@version", "@year", "@anomaly");
        foreach (var row in parse.Rows) {
            var old = row.Old;
            var @new = row.New;
            var version = row.Version;
            Set(p, sourceId, row.Page, row.LineNo, row.Group, row.Section, row.ScientificName, row.CommonName,
                row.OldCategoryText, row.NewCategoryText, old.Category, old.Tag, @new.Category, @new.Tag,
                row.ReasonText, row.Reason, row.VersionText, version, SummaryTableValues.VersionYear(version), row.Anomaly);
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    public void ReplaceTable9(FileFacts file, SummaryTableParse<PossiblyExtinctRow> parse) {
        using var transaction = _connection.BeginTransaction();
        var sourceId = SaveSource(transaction, file, parse.Info, parse.Rows.Count, Count(parse.Rows, r => r.Anomaly is not null), parse.Unread.Count);
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO possibly_extinct (source_id, page, line_no, group_name, scientific_name, common_name,
                category_text, tag, year_assessed_text, year_assessed, last_recorded, anomaly)
            VALUES (@s, @page, @line, @group, @sci, @common, @catText, @tag, @yearText, @year, @last, @anomaly)
            """;
        var p = AddParameters(command, "@s", "@page", "@line", "@group", "@sci", "@common", "@catText", "@tag", "@yearText", "@year", "@last", "@anomaly");
        foreach (var row in parse.Rows) {
            Set(p, sourceId, row.Page, row.LineNo, row.Group, row.ScientificName, row.CommonName,
                row.CategoryText, row.Category.Tag, row.YearText, row.YearAssessed, row.LastRecorded, row.Anomaly);
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    private long SaveSource(SqliteTransaction transaction, FileFacts file, SummaryTableInfo info, int rows, int anomalies, int unread) {
        using (var delete = _connection.CreateCommand()) {
            // ON DELETE CASCADE removes the file's rows.
            delete.Transaction = transaction;
            delete.CommandText = "DELETE FROM source_file WHERE file_name = @f";
            delete.Parameters.AddWithValue("@f", file.FileName);
            delete.ExecuteNonQuery();
        }
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO source_file (file_name, table_no, release, url, sha256, bytes, downloaded_at, parsed_at, parser_version,
                priority, note, title, last_updated, period_from, period_to, defines_error, page_count, row_count, anomaly_count, unread_count)
            VALUES (@f, @t, @r, @u, @sha, @b, @d, @parsed, @pv, @prio, @note, @title, @updated, @from, @to, @err, @pages, @rows, @anom, @unread)
            RETURNING id
            """;
        var p = AddParameters(command, "@f", "@t", "@r", "@u", "@sha", "@b", "@d", "@parsed", "@pv", "@prio", "@note", "@title",
            "@updated", "@from", "@to", "@err", "@pages", "@rows", "@anom", "@unread");
        Set(p, file.FileName, file.Table, file.Release, file.Url, file.Sha256, file.Bytes, file.DownloadedAt, DateTime.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            file.ParserVersion, file.Priority, file.Note, info.Title, info.LastUpdated, info.PeriodFrom, info.PeriodTo,
            info.DefinesError ? 1 : 0, file.PageCount, rows, anomalies, unread);
        return Convert.ToInt64(command.ExecuteScalar());
    }

    /// Every Table 7 row with its file.
    public List<StoredCategoryChange> ReadCategoryChanges() {
        var sources = SourcesById();
        var rows = new List<StoredCategoryChange>();
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT source_id, page, line_no, group_name, section, scientific_name, common_name, old_category_text,
                new_category_text, reason_text, version_text, anomaly
            FROM category_change ORDER BY source_id, line_no
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            rows.Add(new StoredCategoryChange(sources[reader.GetInt64(0)], new CategoryChangeRow(
                reader.GetInt32(1), reader.GetInt32(2), Text(reader, 3), Text(reader, 4), reader.GetString(5), Text(reader, 6),
                Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10), Text(reader, 11))));
        }
        return rows;
    }

    /// Every Table 9 row with its file.
    public List<StoredPossiblyExtinct> ReadPossiblyExtinct() {
        var sources = SourcesById();
        var rows = new List<StoredPossiblyExtinct>();
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT source_id, page, line_no, group_name, scientific_name, common_name, category_text, year_assessed_text,
                last_recorded, anomaly
            FROM possibly_extinct ORDER BY source_id, line_no
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            rows.Add(new StoredPossiblyExtinct(sources[reader.GetInt64(0)], new PossiblyExtinctRow(
                reader.GetInt32(1), reader.GetInt32(2), Text(reader, 3), reader.GetString(4), Text(reader, 5),
                Text(reader, 6), Text(reader, 7), Text(reader, 8), Text(reader, 9))));
        }
        return rows;
    }

    private Dictionary<long, SummaryTableSource> SourcesById() {
        var map = new Dictionary<long, SummaryTableSource>();
        foreach (var source in GetSources().Values) map[source.Id] = source;
        return map;
    }

    // ------------------------------------------------------------ helpers

    private const string SourceColumns = "s.id, s.file_name, s.table_no, s.release, s.url, s.sha256, s.bytes, s.downloaded_at, s.parsed_at, "
        + "s.parser_version, s.priority, s.note, s.title, s.last_updated, s.period_from, s.period_to, s.defines_error, s.page_count, "
        + "s.row_count, s.anomaly_count, s.unread_count";

    private static SummaryTableSource ReadSource(SqliteDataReader r, int o) => new(
        r.GetInt64(o), r.GetString(o + 1), r.GetInt32(o + 2), r.GetString(o + 3), r.GetString(o + 4), r.GetString(o + 5), r.GetInt64(o + 6),
        Text(r, o + 7), r.GetString(o + 8), r.GetInt32(o + 9), r.GetInt32(o + 10), Text(r, o + 11), Text(r, o + 12), Text(r, o + 13),
        Text(r, o + 14), Text(r, o + 15), r.GetInt32(o + 16) != 0, r.GetInt32(o + 17), r.GetInt32(o + 18), r.GetInt32(o + 19), r.GetInt32(o + 20));

    private static string? Text(SqliteDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int Count<T>(IReadOnlyList<T> rows, Func<T, bool> predicate) {
        var n = 0;
        foreach (var row in rows) if (predicate(row)) n++;
        return n;
    }

    private static SqliteParameter[] AddParameters(SqliteCommand command, params string[] names) {
        var parameters = new SqliteParameter[names.Length];
        for (var i = 0; i < names.Length; i++) {
            parameters[i] = command.Parameters.Add(names[i], SqliteType.Text);
        }
        return parameters;
    }

    private static void Set(SqliteParameter[] parameters, params object?[] values) {
        for (var i = 0; i < parameters.Length; i++) {
            var value = values[i];
            parameters[i].SqliteType = value switch {
                int or long => SqliteType.Integer,
                _ => SqliteType.Text,
            };
            parameters[i].Value = value ?? DBNull.Value;
        }
    }
}
