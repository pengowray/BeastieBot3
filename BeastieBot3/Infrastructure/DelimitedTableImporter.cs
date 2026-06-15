using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Infrastructure;

/// <summary>One destination column: its (already-mapped/sanitized) name and SQL type.</summary>
internal readonly record struct DelimitedColumn(string Name, string Type);

/// <summary>
/// Shared "header → create-or-ALTER TEXT table → per-row insert" engine behind the IUCN CSV and
/// CoL TSV importers (audit R3). Each importer keeps its own format quirks — delimiter, header
/// name-mapping/sanitisation, column typing, conflict policy, transaction scope, and post-insert
/// indexing/FTS — and feeds this engine an ordered <see cref="DelimitedColumn"/> list plus a row
/// source. The engine owns only the generic DDL + bulk-insert that both reimplemented byte-for-byte.
/// </summary>
internal static class DelimitedTableImporter {
    /// <summary>
    /// Ensure <paramref name="tableName"/> exists with an <c>import_id</c> column (+ FK to
    /// <c>import_metadata</c>) followed by <paramref name="columns"/> in order, creating it or
    /// ALTERing in any missing columns. Returns the table's current column set.
    /// </summary>
    /// <param name="indexImportIdColumn">Create <c>idx_{table}_import_id</c> on a freshly created table (IUCN does, CoL does not).</param>
    /// <param name="requireImportIdOnExisting">Throw if a pre-existing table lacks <c>import_id</c> (IUCN guard).</param>
    public static HashSet<string> EnsureTable(
        SqliteConnection connection,
        string tableName,
        IReadOnlyList<DelimitedColumn> columns,
        bool indexImportIdColumn,
        bool requireImportIdOnExisting) {

        var existing = GetTableColumns(connection, tableName);
        if (existing is null) {
            CreateTable(connection, tableName, columns, indexImportIdColumn);
            return GetTableColumns(connection, tableName)
                ?? throw new InvalidOperationException($"Table {tableName} was not created as expected.");
        }

        if (requireImportIdOnExisting && !existing.Contains("import_id")) {
            throw new InvalidOperationException(
                $"Existing table {tableName} is missing required import_id column. Please migrate or drop the table.");
        }

        foreach (var col in columns) {
            if (existing.Contains(col.Name)) {
                continue;
            }
            using var alter = connection.CreateCommand();
            alter.CommandText = $"ALTER TABLE {QuoteIdentifier(tableName)} ADD COLUMN {QuoteIdentifier(col.Name)} {col.Type};";
            alter.ExecuteNonQuery();
            existing.Add(col.Name);
        }

        return existing;
    }

    /// <summary>
    /// Bulk-insert rows pulled from <paramref name="readRow"/>/<paramref name="getField"/> (field i of
    /// the current row maps to <c>columns[i]</c>; empty/null becomes <c>NULL</c>). Returns how many rows
    /// were inserted vs. silently skipped (only possible under <paramref name="insertOrIgnore"/> when a
    /// unique constraint rejects them). The insert command joins <paramref name="transaction"/> when given;
    /// the caller owns begin/commit.
    /// </summary>
    public static (long Inserted, long Duplicates) BulkInsert(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string tableName,
        IReadOnlyList<DelimitedColumn> columns,
        long importId,
        Func<bool> readRow,
        Func<int, string?> getField,
        bool insertOrIgnore,
        bool prepare,
        CancellationToken cancellationToken) {

        using var insert = connection.CreateCommand();
        if (transaction is not null) {
            insert.Transaction = transaction;
        }
        insert.CommandText = BuildInsertSql(tableName, columns, insertOrIgnore);

        var importParam = insert.CreateParameter();
        importParam.ParameterName = "@import_id";
        importParam.Value = importId;
        insert.Parameters.Add(importParam);

        var parameters = new SqliteParameter[columns.Count];
        for (var i = 0; i < columns.Count; i++) {
            var param = (SqliteParameter)insert.CreateParameter();
            param.ParameterName = "@c" + i.ToString(CultureInfo.InvariantCulture);
            insert.Parameters.Add(param);
            parameters[i] = param;
        }

        if (prepare) {
            insert.Prepare();
        }

        long inserted = 0;
        long duplicates = 0;
        while (readRow()) {
            cancellationToken.ThrowIfCancellationRequested();
            for (var i = 0; i < columns.Count; i++) {
                var raw = getField(i);
                parameters[i].Value = string.IsNullOrEmpty(raw) ? DBNull.Value : raw;
            }

            var affected = insert.ExecuteNonQuery();
            if (affected > 0) {
                inserted += affected;
            } else {
                duplicates++;
            }
        }

        return (inserted, duplicates);
    }

    /// <summary>Current column names of <paramref name="tableName"/>, or null if it does not exist.</summary>
    public static HashSet<string>? GetTableColumns(SqliteConnection connection, string tableName) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
        using var reader = cmd.ExecuteReader();
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var any = false;
        while (reader.Read()) {
            any = true;
            result.Add(reader.GetString(1));
        }
        return any ? result : null;
    }

    public static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static void CreateTable(
        SqliteConnection connection, string tableName,
        IReadOnlyList<DelimitedColumn> columns, bool indexImportIdColumn) {

        var defs = new List<string> { "\"import_id\" INTEGER NOT NULL" };
        defs.AddRange(columns.Select(c => $"{QuoteIdentifier(c.Name)} {c.Type}"));
        defs.Add("FOREIGN KEY(\"import_id\") REFERENCES import_metadata(\"id\") ON DELETE CASCADE");

        using (var cmd = connection.CreateCommand()) {
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (\n    {string.Join(",\n    ", defs)}\n);";
            cmd.ExecuteNonQuery();
        }

        if (indexImportIdColumn) {
            using var idx = connection.CreateCommand();
            idx.CommandText = $"CREATE INDEX IF NOT EXISTS idx_{tableName}_import_id ON {QuoteIdentifier(tableName)}(import_id);";
            idx.ExecuteNonQuery();
        }
    }

    private static string BuildInsertSql(string tableName, IReadOnlyList<DelimitedColumn> columns, bool insertOrIgnore) {
        var cols = new List<string> { "import_id" };
        cols.AddRange(columns.Select(c => c.Name));
        var identifiers = string.Join(", ", cols.Select(QuoteIdentifier));

        var parameters = new List<string> { "@import_id" };
        for (var i = 0; i < columns.Count; i++) {
            parameters.Add("@c" + i.ToString(CultureInfo.InvariantCulture));
        }

        var orIgnore = insertOrIgnore ? "OR IGNORE " : string.Empty;
        return $"INSERT {orIgnore}INTO {QuoteIdentifier(tableName)} ({identifiers}) VALUES ({string.Join(", ", parameters)});";
    }
}
