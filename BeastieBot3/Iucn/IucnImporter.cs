using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using BeastieBot3.Infrastructure;

// Core CSV import logic for IUCN Red List exports. Uses CsvHelper for parsing.
// Creates tables: taxonomy (scientific_name, kingdom, class, order, family, etc.),
// assessments (red_list_category, population_trend, etc.), synonyms. Handles
// UTF-8/BOM encoding and normalizes whitespace. Called by IucnImportCommand.

namespace BeastieBot3.Iucn;

public sealed class IucnImporter {
    private static readonly Regex RedlistVersionRegex = new(@"(?<version>\d{4}-\d+)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
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
        var redlistVersion = ExtractRedlistVersionFromPath(relativeName);

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

                ImportCsv(importId, spec, entry!, transaction, cancellationToken);
            }
            transaction.Commit();

            EnsureViews();

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

    private void ImportCsv(long importId, ZipEntrySpec spec, ZipArchiveEntry entry, SqliteTransaction transaction, CancellationToken cancellationToken) {
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

        // IUCN destination columns: map header names (internalTaxonId -> taxonId) and resolve types
        // (taxonId/assessmentId -> INTEGER NOT NULL, else TEXT), preserving CSV column order.
        var columns = headers
            .Select(h => { var name = MapColumnName(h); return new DelimitedColumn(name, GetColumnType(name)); })
            .ToList();

        var tableColumns = DelimitedTableImporter.EnsureTable(
            _connection, spec.TableName, columns,
            indexImportIdColumn: true, requireImportIdOnExisting: true);
        EnsureIndexes(spec.TableName, tableColumns);

        var (insertedCount, duplicateCount) = DelimitedTableImporter.BulkInsert(
            _connection, transaction, spec.TableName, columns, importId,
            () => csv.Read(), i => csv.GetField(i),
            insertOrIgnore: true, prepare: false, cancellationToken);

        _console.MarkupLine($"    {spec.TableName}: inserted {insertedCount:N0} rows (skipped {duplicateCount:N0} duplicates).");
    }

    private void EnsureIndexes(string tableName, HashSet<string> columns) {
        bool HasColumn(string name) => columns.Contains(name);
        switch (tableName.ToLowerInvariant()) {
            case "assessments":
            case "assessments_html":
                if (HasColumn("taxonId")) {
                    CreateIndex(tableName, "taxonId", "taxonId");
                }
                if (HasColumn("assessmentId")) {
                    CreateIndex(tableName, "assessmentId", "assessmentId");
                    CreateIndex(tableName, "uniq_assessmentId", true, "assessmentId");
                }
                if (HasColumn("scientificName")) {
                    CreateIndex(tableName, "scientificName", "scientificName");
                }
                break;
            case "taxonomy":
            case "taxonomy_html":
                if (HasColumn("taxonId")) {
                    CreateIndex(tableName, "taxonId", "taxonId");
                    CreateIndex(tableName, "uniq_taxonId", true, "taxonId");
                }
                if (HasColumn("scientificName")) {
                    CreateIndex(tableName, "scientificName", "scientificName");
                }

                var hierarchyColumns = new[] { "kingdomName", "phylumName", "className", "orderName", "familyName", "genusName", "speciesName" };
                if (hierarchyColumns.All(HasColumn)) {
                    CreateIndex(tableName, "hierarchy", hierarchyColumns);
                }
                break;
        }
    }

    private void CreateIndex(string tableName, string suffix, params string[] columnNames) =>
        CreateIndex(tableName, suffix, unique: false, columnNames);

    private void CreateIndex(string tableName, string suffix, bool unique, params string[] columnNames) {
        if (columnNames.Length == 0) {
            return;
        }

        var sanitizedSuffix = SanitizeIdentifierPart(suffix);
        var indexName = $"idx_{SanitizeIdentifierPart(tableName)}_{sanitizedSuffix}";
        using var cmd = _connection.CreateCommand();
        var uniqueClause = unique ? "UNIQUE " : string.Empty;
        cmd.CommandText = $"CREATE {uniqueClause}INDEX IF NOT EXISTS {QuoteIdentifier(indexName)} ON {QuoteIdentifier(tableName)}({string.Join(", ", columnNames.Select(QuoteIdentifier))});";
        cmd.ExecuteNonQuery();
    }

    private static string SanitizeIdentifierPart(string value) {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value) {
            builder.Append(char.IsLetterOrDigit(ch) ? ch : '_');
        }
        return builder.Length > 0 ? builder.ToString() : "part";
    }

    private void EnsureViews() {
        // The join view definition (and its collision-aliasing) lives in the shared
        // IucnViewBuilder so the CSV importer and the API-cache projection can never
        // produce divergent column sets.
        if (DelimitedTableImporter.GetTableColumns(_connection, "assessments") is not null && DelimitedTableImporter.GetTableColumns(_connection, "taxonomy") is not null) {
            IucnViewBuilder.RecreateJoinView(_connection, "view_assessments_taxonomy", "assessments", "taxonomy");
        }

        if (DelimitedTableImporter.GetTableColumns(_connection, "assessments_html") is not null && DelimitedTableImporter.GetTableColumns(_connection, "taxonomy_html") is not null) {
            IucnViewBuilder.RecreateJoinView(_connection, "view_assessments_html_taxonomy_html", "assessments_html", "taxonomy_html");
        }
    }

    private static string MapColumnName(string csvColumn) =>
        csvColumn switch {
            "internalTaxonId" => "taxonId",
            _ => csvColumn
        };

    private static string GetColumnType(string columnName) =>
        columnName switch {
            "taxonId" => "INTEGER NOT NULL",
            "assessmentId" => "INTEGER NOT NULL",
            _ => "TEXT"
        };

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

    internal static string ExtractRedlistVersionFromPath(string path) {
        if (string.IsNullOrWhiteSpace(path)) {
            return "unknown";
        }

        var normalized = path.Replace('\\', '/');
        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments) {
            var match = RedlistVersionRegex.Match(segment);
            if (match.Success) {
                return match.Groups["version"].Value;
            }
        }

        var fallback = RedlistVersionRegex.Match(normalized);
        return fallback.Success ? fallback.Groups["version"].Value : "unknown";
    }
}
