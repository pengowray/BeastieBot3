using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using BeastieBot3.Infrastructure;

// Imports the Australian SPRAT (Species Profile and Threats Database) report CSV into a single flat
// SQLite table (sprat_species). Structurally closest to ColImporter: one wide CSV → one DB file,
// all-TEXT columns, sanitised/de-duplicated header names. Two SPRAT-specific wrinkles handled here:
//   1. The report has a TWO-ROW header — a category-grouping row, then the real column names — so
//      the first data row read is discarded and the second is used as the header.
//   2. The columns the lists rely on are mapped to stable names via SpratColumns.HeaderMap; the
//      eight identically-named "Listed Name" columns and the presence/agreement columns are kept
//      under auto-sanitised, de-duplicated names.
// Used by SpratImportCommand. The bulk DDL + insert is delegated to DelimitedTableImporter.

namespace BeastieBot3.Sprat;

public sealed class SpratImporter {
    private readonly IAnsiConsole _console;
    private readonly SqliteConnection _connection;
    private readonly string _csvPath;
    private readonly string _version;

    public SpratImporter(IAnsiConsole console, SqliteConnection connection, string csvPath, string version) {
        _console = console;
        _connection = connection;
        _csvPath = Path.GetFullPath(csvPath);
        _version = version;
    }

    public void Run(CancellationToken cancellationToken) {
        cancellationToken.ThrowIfCancellationRequested();

        using var reader = new StreamReader(_csvPath, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture) {
            BadDataFound = null,
            MissingFieldFound = null,
            TrimOptions = TrimOptions.None,
            DetectColumnCountChanges = false,
        };
        using var csv = new CsvReader(reader, config);

        // Two-row header: the first row groups columns into categories ("EPBC Act Threatened…",
        // "Taxonomic Data", …); the second row holds the actual column names. Discard the first.
        if (!csv.Read()) {
            _console.MarkupLine("[yellow]SPRAT CSV is empty; nothing imported.[/]");
            return;
        }
        if (!csv.Read()) {
            _console.MarkupLine("[yellow]SPRAT CSV has no column-name header row; nothing imported.[/]");
            return;
        }
        csv.ReadHeader();
        var headers = csv.HeaderRecord ?? Array.Empty<string>();
        if (headers.Length == 0) {
            _console.MarkupLine("[yellow]SPRAT CSV header row is blank; nothing imported.[/]");
            return;
        }

        var columns = BuildColumns(headers);

        var existing = DelimitedTableImporter.EnsureTable(
            _connection, SpratColumns.Table, columns,
            indexImportIdColumn: true, requireImportIdOnExisting: true);

        var importId = InsertImportMetadata(Path.GetFileName(_csvPath), _version, DateTimeOffset.UtcNow);

        long inserted;
        try {
            using var tx = _connection.BeginTransaction();
            (inserted, _) = DelimitedTableImporter.BulkInsert(
                _connection, tx, SpratColumns.Table, columns, importId,
                () => csv.Read(), i => csv.GetField(i),
                insertOrIgnore: false, prepare: true, cancellationToken);
            tx.Commit();
        } catch {
            DeleteImportMetadata(importId);
            throw;
        }

        CreateIndexes();
        UpdateImportCompleted(importId, DateTimeOffset.UtcNow);

        _console.MarkupLine($"[green]Imported SPRAT ->[/] {SpratColumns.Table}: {inserted:N0} rows, {existing.Count} columns.");
    }

    // ==================== Column building ====================

    private static List<DelimitedColumn> BuildColumns(IReadOnlyList<string> headers) {
        var result = new List<DelimitedColumn>(headers.Count);
        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var header in headers) {
            var trimmed = (header ?? string.Empty).Trim();
            // Prefer the stable canonical name for known columns; otherwise sanitise the raw header.
            var name = SpratColumns.HeaderMap.TryGetValue(trimmed, out var canonical)
                ? canonical
                : SanitizeColumnName(trimmed);

            // De-duplicate (the eight per-act "Listed Name" columns all sanitise to the same name).
            if (!used.Add(name)) {
                var n = 2;
                string candidate;
                do {
                    candidate = $"{name}_{n++}";
                } while (!used.Add(candidate));
                name = candidate;
            }

            result.Add(new DelimitedColumn(name, "TEXT"));
        }

        return result;
    }

    private static string SanitizeColumnName(string original) {
        if (string.IsNullOrWhiteSpace(original)) {
            return "column";
        }

        var builder = new StringBuilder(original.Length);
        foreach (var ch in original) {
            builder.Append(char.IsLetterOrDigit(ch) || ch == '_' ? ch : '_');
        }

        var sanitized = builder.ToString().Trim('_');
        if (sanitized.Length == 0) {
            sanitized = "column";
        }
        if (char.IsDigit(sanitized[0])) {
            sanitized = "_" + sanitized;
        }
        return sanitized;
    }

    // ==================== Indexes ====================

    private void CreateIndexes() {
        foreach (var column in SpratColumns.IndexedColumns) {
            using var cmd = _connection.CreateCommand();
            var indexName = $"idx_{SpratColumns.Table}_{column}";
            cmd.CommandText =
                $"CREATE INDEX IF NOT EXISTS {DelimitedTableImporter.QuoteIdentifier(indexName)} " +
                $"ON {DelimitedTableImporter.QuoteIdentifier(SpratColumns.Table)}({DelimitedTableImporter.QuoteIdentifier(column)});";
            cmd.ExecuteNonQuery();
        }
    }

    // ==================== import_metadata bookkeeping ====================

    private long InsertImportMetadata(string filename, string version, DateTimeOffset startedAt) {
        using var insert = _connection.CreateCommand();
        insert.CommandText =
            "INSERT INTO import_metadata (filename, redlist_version, started_at) VALUES (@filename, @version, @started);";
        insert.Parameters.AddWithValue("@filename", filename);
        insert.Parameters.AddWithValue("@version", version);
        insert.Parameters.AddWithValue("@started", startedAt.ToString("O", CultureInfo.InvariantCulture));
        insert.ExecuteNonQuery();

        using var lastId = _connection.CreateCommand();
        lastId.CommandText = "SELECT last_insert_rowid();";
        return Convert.ToInt64(lastId.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private void UpdateImportCompleted(long importId, DateTimeOffset endedAt) {
        using var update = _connection.CreateCommand();
        update.CommandText = "UPDATE import_metadata SET ended_at = @ended WHERE id = @id;";
        update.Parameters.AddWithValue("@ended", endedAt.ToString("O", CultureInfo.InvariantCulture));
        update.Parameters.AddWithValue("@id", importId);
        update.ExecuteNonQuery();
    }

    private void DeleteImportMetadata(long importId) {
        using var delete = _connection.CreateCommand();
        delete.CommandText = "DELETE FROM import_metadata WHERE id = @id;";
        delete.Parameters.AddWithValue("@id", importId);
        delete.ExecuteNonQuery();
    }

    /// <summary>
    /// A pre-existing SPRAT DB counts as "already imported" only if it holds a finished import
    /// (import_metadata.ended_at set). A corrupt file or one left partial by a crashed import returns
    /// false, so the caller rebuilds it from scratch.
    /// </summary>
    public static bool IsImportComplete(string databasePath) {
        try {
            var cs = new SqliteConnectionStringBuilder {
                DataSource = databasePath,
                Mode = SqliteOpenMode.ReadOnly,
            }.ToString();
            using var connection = new SqliteConnection(cs);
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM import_metadata WHERE ended_at IS NOT NULL";
            return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L) > 0;
        } catch {
            return false;
        }
    }
}
