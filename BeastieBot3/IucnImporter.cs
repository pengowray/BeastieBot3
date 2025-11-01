using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3;

public sealed class IucnImporter {
    private readonly IAnsiConsole _console;
    private readonly SqliteConnection _connection;
    private readonly string _rootDir;
    private readonly bool _force;

    private readonly ZipEntrySpec[] _expectedEntries = new[] {
        new ZipEntrySpec("assessments.csv", "assessments"),
        new ZipEntrySpec("assessments_with_html.csv", "assessments_html"),
        new ZipEntrySpec("taxonomy.csv", "taxonomy"),
        new ZipEntrySpec("taxonomy_with_html.csv", "taxonomy_html"),
    };

    public IucnImporter(IAnsiConsole console, SqliteConnection connection, string rootDir, bool force) {
        _console = console;
        _connection = connection;
        _rootDir = Path.GetFullPath(rootDir);
        _force = force;
        EnsureImportMetadataTable();
    }

    public void ProcessZip(string zipPath, CancellationToken cancellationToken) {
        cancellationToken.ThrowIfCancellationRequested();

        var fullZipPath = Path.GetFullPath(zipPath);
        var relativeName = ToRelative(fullZipPath);
        var redlistVersion = ExtractRedlistVersion(relativeName);

        if (_force) {
            DeleteExistingImports(relativeName);
        } else {
            var existing = GetLatestImport(relativeName);
            if (existing is { Completed: true }) {
                _console.MarkupLine($"[yellow]Skipping already imported zip:[/] {relativeName}");
                return;
            }
        }

        var startedAt = DateTimeOffset.UtcNow;
        var importId = InsertImportMetadata(relativeName, redlistVersion, startedAt);

        try {
            using var archive = ZipFile.OpenRead(fullZipPath);
            var entryLookup = BuildEntryLookup(archive);
            WriteZipSummary(relativeName, entryLookup);

            using var transaction = _connection.BeginTransaction();
            foreach (var spec in _expectedEntries) {
                cancellationToken.ThrowIfCancellationRequested();
                if (!entryLookup.TryGetValue(spec.EntryName, out var entry)) {
                    continue;
                }

                ImportCsv(importId, redlistVersion, spec, entry!, transaction, cancellationToken);
            }
            transaction.Commit();

            UpdateImportCompleted(importId, DateTimeOffset.UtcNow);
        } catch {
            _console.MarkupLine($"[red]Import aborted for[/] {relativeName}; import_metadata entry will remain open.");
            throw;
        }
    }

    private record struct ZipEntrySpec(string EntryName, string TableName);

    private sealed class ImportInfo {
        public required long Id { get; init; }
        public required bool Completed { get; init; }
    }

    private void EnsureImportMetadataTable() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS import_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    redlist_version TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT
);";
        cmd.ExecuteNonQuery();

        using var idxCmd = _connection.CreateCommand();
        idxCmd.CommandText = "CREATE INDEX IF NOT EXISTS idx_import_metadata_filename ON import_metadata(filename);";
        idxCmd.ExecuteNonQuery();
    }

    private void DeleteExistingImports(string filename) {
        var toDelete = new List<long>();
        using (var select = _connection.CreateCommand()) {
            select.CommandText = "SELECT id FROM import_metadata WHERE filename = @filename;";
            select.Parameters.AddWithValue("@filename", filename);
            using var reader = select.ExecuteReader();
            while (reader.Read()) {
                toDelete.Add(reader.GetInt64(0));
            }
        }

        if (toDelete.Count == 0) {
            return;
        }

        foreach (var id in toDelete) {
            using var delete = _connection.CreateCommand();
            delete.CommandText = "DELETE FROM import_metadata WHERE id = @id;";
            delete.Parameters.AddWithValue("@id", id);
            delete.ExecuteNonQuery();
        }

        if (toDelete.Count > 0) {
            _console.MarkupLine($"[grey]Removed {toDelete.Count} previous import metadata entries for[/] {filename}.");
        }
    }

    private ImportInfo? GetLatestImport(string filename) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"SELECT id, ended_at FROM import_metadata
WHERE filename = @filename
ORDER BY started_at DESC
LIMIT 1;";
        cmd.Parameters.AddWithValue("@filename", filename);
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }

        var endedAtIndex = reader.GetOrdinal("ended_at");
        var isEnded = !reader.IsDBNull(endedAtIndex);
        return new ImportInfo {
            Id = reader.GetInt64(0),
            Completed = isEnded
        };
    }

    private long InsertImportMetadata(string filename, string redlistVersion, DateTimeOffset startedAt) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO import_metadata (filename, redlist_version, started_at)
VALUES (@filename, @version, @started);";
        cmd.Parameters.AddWithValue("@filename", filename);
        cmd.Parameters.AddWithValue("@version", redlistVersion);
        cmd.Parameters.AddWithValue("@started", startedAt.ToString("O", CultureInfo.InvariantCulture));
        cmd.ExecuteNonQuery();

        using var idCmd = _connection.CreateCommand();
        idCmd.CommandText = "SELECT last_insert_rowid();";
        var id = (long)(idCmd.ExecuteScalar() ?? throw new InvalidOperationException("Failed to retrieve import_metadata id."));
        return id;
    }

    private void UpdateImportCompleted(long importId, DateTimeOffset completedAt) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "UPDATE import_metadata SET ended_at = @ended WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", importId);
        cmd.Parameters.AddWithValue("@ended", completedAt.ToString("O", CultureInfo.InvariantCulture));
        cmd.ExecuteNonQuery();
    }

    private Dictionary<string, ZipArchiveEntry?> BuildEntryLookup(ZipArchive archive) {
        var comparer = StringComparer.OrdinalIgnoreCase;
        var dict = new Dictionary<string, ZipArchiveEntry?>(comparer);
        foreach (var entry in archive.Entries) {
            var name = Path.GetFileName(entry.FullName);
            if (string.IsNullOrWhiteSpace(name)) continue;
            if (!_expectedEntries.Any(spec => comparer.Equals(spec.EntryName, name))) continue;
            if (!dict.ContainsKey(name)) {
                dict[name] = entry;
            }
        }
        return dict;
    }

    private void WriteZipSummary(string relativeName, IReadOnlyDictionary<string, ZipArchiveEntry?> entryLookup) {
        var found = _expectedEntries.Count(spec => entryLookup.ContainsKey(spec.EntryName));
        var parts = _expectedEntries.Select(spec => {
            var status = entryLookup.ContainsKey(spec.EntryName) ? "[green]found[/]" : "[red]missing[/]";
            return $"{spec.EntryName}: {status}";
        });
        _console.MarkupLine($"[blue]ZIP[/] {relativeName} -> {found}/{_expectedEntries.Length} csv files ({string.Join(", ", parts)})");
    }

    private void ImportCsv(long importId, string redlistVersion, ZipEntrySpec spec, ZipArchiveEntry entry, SqliteTransaction transaction, CancellationToken cancellationToken) {
        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture) {
            BadDataFound = null,
            MissingFieldFound = null,
            DetectColumnCountChanges = false,
            TrimOptions = TrimOptions.None,
        };

        using var csv = new CsvReader(reader, config);
        if (!csv.Read()) {
            _console.MarkupLine($"[yellow]{spec.EntryName} in zip is empty; skipping.[/]");
            return;
        }

        csv.ReadHeader();
        var headerRecord = csv.HeaderRecord ?? Array.Empty<string>();
        var headers = headerRecord.ToList();

        EnsureDataTable(spec.TableName, headers);

        using var insert = _connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = BuildInsertSql(spec.TableName, headers);

        var importParam = insert.CreateParameter();
        importParam.ParameterName = "@import_id";
        importParam.Value = importId;
        insert.Parameters.Add(importParam);

        var versionParam = insert.CreateParameter();
        versionParam.ParameterName = "@redlist_version";
        versionParam.Value = redlistVersion;
        insert.Parameters.Add(versionParam);

        var parameterMap = new Dictionary<string, SqliteParameter>(headers.Count, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < headers.Count; i++) {
            var param = insert.CreateParameter();
            param.ParameterName = "@c" + i.ToString(CultureInfo.InvariantCulture);
            insert.Parameters.Add(param);
            parameterMap[headers[i]] = param;
        }

        long rowCount = 0;
        while (csv.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var header in headers) {
                var raw = csv.GetField(header);
                parameterMap[header].Value = string.IsNullOrEmpty(raw) ? DBNull.Value : raw;
            }

            insert.ExecuteNonQuery();
            rowCount++;
        }

        _console.MarkupLine($"    {spec.TableName}: inserted {rowCount:N0} rows.");
    }

    private void EnsureDataTable(string tableName, IReadOnlyList<string> csvColumns) {
        var existingColumns = GetTableColumns(tableName);
        if (existingColumns is null) {
            CreateNewDataTable(tableName, csvColumns);
            return;
        }

        if (!existingColumns.Contains("import_id") || !existingColumns.Contains("redlist_version")) {
            throw new InvalidOperationException($"Existing table {tableName} is missing required columns. Please migrate or drop the table.");
        }

        foreach (var column in csvColumns) {
            if (existingColumns.Contains(column)) continue;
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = $"ALTER TABLE {QuoteIdentifier(tableName)} ADD COLUMN {QuoteIdentifier(column)} TEXT;";
            cmd.ExecuteNonQuery();
        }
    }

    private HashSet<string>? GetTableColumns(string tableName) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
        using var reader = cmd.ExecuteReader();
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var any = false;
        while (reader.Read()) {
            any = true;
            var name = reader.GetString(1);
            result.Add(name);
        }
        return any ? result : null;
    }

    private void CreateNewDataTable(string tableName, IReadOnlyList<string> csvColumns) {
        var columns = new List<string> {
            "\"import_id\" INTEGER NOT NULL",
            "\"redlist_version\" TEXT NOT NULL"
        };
        foreach (var column in csvColumns) {
            columns.Add($"{QuoteIdentifier(column)} TEXT");
        }
        columns.Add("FOREIGN KEY(\"import_id\") REFERENCES import_metadata(\"id\") ON DELETE CASCADE");

        using (var cmd = _connection.CreateCommand()) {
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (\n    {string.Join(",\n    ", columns)}\n);";
            cmd.ExecuteNonQuery();
        }

        using (var idx = _connection.CreateCommand()) {
            idx.CommandText = $"CREATE INDEX IF NOT EXISTS idx_{tableName}_import_id ON {QuoteIdentifier(tableName)}(import_id);";
            idx.ExecuteNonQuery();
        }
    }

    private static string BuildInsertSql(string tableName, IReadOnlyList<string> csvColumns) {
        var columns = new List<string> { "import_id", "redlist_version" };
        columns.AddRange(csvColumns);
        var identifiers = string.Join(", ", columns.Select(QuoteIdentifier));

        var parameters = new List<string> { "@import_id", "@redlist_version" };
        for (var i = 0; i < csvColumns.Count; i++) {
            parameters.Add("@c" + i.ToString(CultureInfo.InvariantCulture));
        }

        return $"INSERT INTO {QuoteIdentifier(tableName)} ({identifiers}) VALUES ({string.Join(", ", parameters)});";
    }

    private static string QuoteIdentifier(string identifier) {
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private string ToRelative(string fullPath) {
        try {
            var relative = Path.GetRelativePath(_rootDir, fullPath);
            return string.IsNullOrWhiteSpace(relative) ? Path.GetFileName(fullPath) : relative.Replace('\\', '/');
        } catch {
            return Path.GetFileName(fullPath);
        }
    }

    private static string ExtractRedlistVersion(string relativePath) {
        if (string.IsNullOrWhiteSpace(relativePath)) {
            return "unknown";
        }

        var cleaned = relativePath.Replace('\\', '/');
        var firstSegment = cleaned.Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(firstSegment)) {
            firstSegment = cleaned;
        }

        var trimmed = firstSegment.Trim();
        if (trimmed.Length == 0) {
            return "unknown";
        }

        var spaceIndex = trimmed.IndexOf(' ');
        if (spaceIndex > 0) {
            trimmed = trimmed[..spaceIndex];
        }

        return trimmed;
    }
}
