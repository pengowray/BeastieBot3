using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace BeastieBot3;

public sealed class ColImporter {
    private readonly IAnsiConsole _console;
    private readonly string _zipPath;
    private readonly string _rootDir;
    private readonly string _datastoreDir;
    private readonly bool _force;
    private readonly Dictionary<string, HashSet<string>> _tableColumnsForIndexing = new(StringComparer.OrdinalIgnoreCase);

    private static readonly Uri DatapackageUri = new("https://api.catalogueoflife.org/datapackage", UriKind.Absolute);

    private sealed record ColumnMapping(string FtsColumn, string[] Candidates, bool IncludeInScientificName, bool IncludeInContext, bool CreateIndex);

    private static readonly ColumnMapping[] NameUsageColumnMappings = new[] {
        new ColumnMapping("scientificname", new[] { "scientificName" }, true, true, true),
        new ColumnMapping("originalspelling", new[] { "originalSpelling" }, true, true, false),
        new ColumnMapping("uninomial", new[] { "uninomial" }, true, true, true),
        new ColumnMapping("genericname", new[] { "genericName" }, true, true, true),
        new ColumnMapping("infragenericepithet", new[] { "infragenericEpithet" }, true, true, true),
        new ColumnMapping("specificepithet", new[] { "specificEpithet" }, true, true, true),
        new ColumnMapping("infraspecificepithet", new[] { "infraspecificEpithet" }, true, true, true),
        new ColumnMapping("cultivarepithet", new[] { "cultivarEpithet" }, true, true, true),
        new ColumnMapping("genus", new[] { "genus" }, true, true, true),
        new ColumnMapping("subtribe", new[] { "subtribe" }, false, true, true),
        new ColumnMapping("tribe", new[] { "tribe" }, false, true, true),
        new ColumnMapping("subfamily", new[] { "subfamily" }, false, true, true),
        new ColumnMapping("family", new[] { "family" }, false, true, true),
        new ColumnMapping("superfamily", new[] { "superfamily" }, false, true, true),
        new ColumnMapping("suborder", new[] { "suborder" }, false, true, true),
        new ColumnMapping("order", new[] { "order" }, false, true, true),
        new ColumnMapping("subclass", new[] { "subclass" }, false, true, true),
        new ColumnMapping("class", new[] { "class" }, false, true, true),
        new ColumnMapping("subphylum", new[] { "subphylum" }, false, true, true),
        new ColumnMapping("phylum", new[] { "phylum" }, false, true, true),
        new ColumnMapping("kingdom", new[] { "kingdom" }, false, true, true)
    };

    private static readonly HashSet<string> IndexedColumnNames = BuildIndexedColumnNames();

    private sealed record ColumnInfo(string Original, string Sanitized);

    public ColImporter(IAnsiConsole console, string zipPath, string rootDir, string datastoreDir, bool force) {
        _console = console;
        _zipPath = Path.GetFullPath(zipPath);
        _rootDir = Path.GetFullPath(rootDir);
        _datastoreDir = Path.GetFullPath(datastoreDir);
        _force = force;
    }

    public void Process(CancellationToken cancellationToken) {
        cancellationToken.ThrowIfCancellationRequested();
        _tableColumnsForIndexing.Clear();

        using var archive = ZipFile.OpenRead(_zipPath);
        var metadataEntry = archive.Entries.FirstOrDefault(e => e.FullName.Equals("metadata.yaml", StringComparison.OrdinalIgnoreCase));
        if (metadataEntry is null) {
            throw new InvalidOperationException("metadata.yaml not found inside the ColDP archive.");
        }

        var metadata = ReadDatasetMetadata(metadataEntry, cancellationToken);
        var datasetLabel = DetermineDatasetLabel(metadata, Path.GetFileNameWithoutExtension(_zipPath));
        var sanitizedStem = SanitizeFileStem(datasetLabel);
        var databaseFileName = $"col_coldp_{sanitizedStem}.sqlite";
        var databasePath = Path.Combine(_datastoreDir, databaseFileName);

        if (File.Exists(databasePath)) {
            if (!_force) {
                _console.MarkupLine($"[yellow]Skipping already processed zip:[/] {_zipPath}");
                return;
            }

            _console.MarkupLine($"[grey]Removing existing database:[/] {databasePath}");
            File.Delete(databasePath);
        }

        EnsureDatapackage(Path.GetDirectoryName(_zipPath)!, cancellationToken);

        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);
        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Default
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        ConfigureConnection(connection);

        EnsureImportMetadataTable(connection);

        var relativeZipName = ToRelative(_zipPath);
        var startedAt = DateTimeOffset.UtcNow;
        var importId = InsertImportMetadata(connection, relativeZipName, datasetLabel, startedAt);

        try {
            InsertDatasetMetadata(connection, importId, datasetLabel, metadata, cancellationToken);

            foreach (var entry in archive.Entries.OrderBy(e => e.FullName, StringComparer.OrdinalIgnoreCase)) {
                cancellationToken.ThrowIfCancellationRequested();

                if (entry.FullName.EndsWith("/", StringComparison.Ordinal)) {
                    continue;
                }

                if (entry.FullName.Equals("metadata.yaml", StringComparison.OrdinalIgnoreCase)) {
                    continue;
                }

                if (entry.FullName.Equals("reference.jsonl", StringComparison.OrdinalIgnoreCase)) {
                    ProcessReferenceJsonl(connection, importId, entry, cancellationToken);
                    continue;
                }

                if (entry.FullName.EndsWith(".tsv", StringComparison.OrdinalIgnoreCase)) {
                    ProcessTsv(connection, importId, entry, cancellationToken);
                    continue;
                }

                if (entry.FullName.StartsWith("source/", StringComparison.OrdinalIgnoreCase) &&
                    entry.FullName.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase)) {
                    ProcessSourceYaml(connection, importId, entry, cancellationToken);
                    continue;
                }
            }

            FinalizeIndexesAndFts(connection);
            UpdateImportCompleted(connection, importId, DateTimeOffset.UtcNow);
            _console.MarkupLine($"[green]Imported ColDP dataset ->[/] {databasePath}");
        } catch {
            _console.MarkupLine($"[red]Import aborted for[/] {relativeZipName}; import_metadata entry will remain open.");
            throw;
        }
    }

    private static DatasetMetadata ReadDatasetMetadata(ZipArchiveEntry entry, CancellationToken cancellationToken) {
        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        var yamlText = reader.ReadToEnd();
        cancellationToken.ThrowIfCancellationRequested();

        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        var map = deserializer.Deserialize<Dictionary<string, object?>>(yamlText) ?? new Dictionary<string, object?>();
        return new DatasetMetadata {
            RawYaml = yamlText,
            Key = ExtractString(map, "key"),
            Title = ExtractString(map, "title"),
            Alias = ExtractString(map, "alias"),
            Description = ExtractString(map, "description"),
            Issued = ExtractString(map, "issued"),
            Version = ExtractString(map, "version")
        };
    }

    private static string? ExtractString(IDictionary<string, object?> map, string key) {
        if (!map.TryGetValue(key, out var value) || value is null) {
            return null;
        }

        return value switch {
            string s => s,
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString()
        };
    }

    private static string DetermineDatasetLabel(DatasetMetadata metadata, string fallbackStem) {
        string? label = null;

        if (!string.IsNullOrWhiteSpace(metadata.Alias)) {
            label = metadata.Alias;
        } else if (!string.IsNullOrWhiteSpace(metadata.Title) && !string.IsNullOrWhiteSpace(metadata.Version)) {
            label = string.Join("_", new[] { metadata.Title, metadata.Version });
        } else if (!string.IsNullOrWhiteSpace(metadata.Title) && !string.IsNullOrWhiteSpace(metadata.Issued)) {
            label = string.Join("_", new[] { metadata.Title, metadata.Issued });
        }

        label ??= fallbackStem;
        label = NormalizeLabel(label);

        if (string.IsNullOrWhiteSpace(label)) {
            label = NormalizeLabel(fallbackStem);
        }

        return string.IsNullOrWhiteSpace(label) ? "dataset" : label;
    }

    private static string NormalizeLabel(string value) {
        var replaced = string.Join("_", value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        var builder = new StringBuilder(replaced.Length);
        foreach (var ch in replaced) {
            if (char.IsLetterOrDigit(ch)) {
                builder.Append(ch);
            } else if (ch is '_' or '-' or '.') {
                builder.Append(ch);
            } else {
                builder.Append('_');
            }
        }

        var result = builder.ToString().Trim('_');
        if (result.Length > 128) {
            result = result[..128].Trim('_');
        }
        return result;
    }

    private void EnsureDatapackage(string directory, CancellationToken cancellationToken) {
        var datapackagePath = Path.Combine(directory, "datapackage.json");
        if (File.Exists(datapackagePath)) {
            return;
        }

        _console.MarkupLine("[grey]datapackage.json missing; downloading from Catalogue of Life API...[/]");

        using var client = new HttpClient();
        using var response = client.GetAsync(DatapackageUri, HttpCompletionOption.ResponseHeadersRead, cancellationToken).GetAwaiter().GetResult();
        response.EnsureSuccessStatusCode();

        using var contentStream = response.Content.ReadAsStream(cancellationToken);
        using var fileStream = new FileStream(datapackagePath, FileMode.Create, FileAccess.Write, FileShare.None);
        contentStream.CopyToAsync(fileStream, cancellationToken).GetAwaiter().GetResult();

        _console.MarkupLine($"[green]Downloaded datapackage.json ->[/] {datapackagePath}");
    }

    private static void EnsureImportMetadataTable(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS import_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    redlist_version TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT
);";
        cmd.ExecuteNonQuery();

        using var idx = connection.CreateCommand();
        idx.CommandText = "CREATE INDEX IF NOT EXISTS idx_import_metadata_filename ON import_metadata(filename);";
        idx.ExecuteNonQuery();
    }

    private static long InsertImportMetadata(SqliteConnection connection, string filename, string datasetLabel, DateTimeOffset startedAt) {
        using var insert = connection.CreateCommand();
        insert.CommandText = @"INSERT INTO import_metadata (filename, redlist_version, started_at)
VALUES (@filename, @version, @started);";
        insert.Parameters.AddWithValue("@filename", filename);
        insert.Parameters.AddWithValue("@version", datasetLabel);
        insert.Parameters.AddWithValue("@started", startedAt.ToString("O", CultureInfo.InvariantCulture));
        insert.ExecuteNonQuery();

        using var lastId = connection.CreateCommand();
        lastId.CommandText = "SELECT last_insert_rowid();";
        var idObj = lastId.ExecuteScalar() ?? throw new InvalidOperationException("Failed to retrieve import_metadata id.");
        return Convert.ToInt64(idObj, CultureInfo.InvariantCulture);
    }

    private static void UpdateImportCompleted(SqliteConnection connection, long importId, DateTimeOffset endedAt) {
        using var update = connection.CreateCommand();
        update.CommandText = "UPDATE import_metadata SET ended_at = @ended WHERE id = @id;";
        update.Parameters.AddWithValue("@ended", endedAt.ToString("O", CultureInfo.InvariantCulture));
        update.Parameters.AddWithValue("@id", importId);
        update.ExecuteNonQuery();
    }

    private void InsertDatasetMetadata(SqliteConnection connection, long importId, string datasetLabel, DatasetMetadata metadata, CancellationToken cancellationToken) {
        EnsureDatasetMetadataTable(connection);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO dataset_metadata (import_id, dataset_label, key, title, alias, description, issued, version, raw_yaml)
VALUES (@import_id, @label, @key, @title, @alias, @description, @issued, @version, @raw_yaml);";
        cmd.Parameters.AddWithValue("@import_id", importId);
        cmd.Parameters.AddWithValue("@label", datasetLabel);
        cmd.Parameters.AddWithValue("@key", metadata.Key ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@title", metadata.Title ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@alias", metadata.Alias ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@description", metadata.Description ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@issued", metadata.Issued ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@version", metadata.Version ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@raw_yaml", metadata.RawYaml ?? (object)DBNull.Value);
        cmd.ExecuteNonQuery();

        cancellationToken.ThrowIfCancellationRequested();
    }

    private void EnsureDatasetMetadataTable(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS dataset_metadata (
    import_id INTEGER NOT NULL,
    dataset_label TEXT NOT NULL,
    key TEXT,
    title TEXT,
    alias TEXT,
    description TEXT,
    issued TEXT,
    version TEXT,
    raw_yaml TEXT,
    FOREIGN KEY(import_id) REFERENCES import_metadata(id) ON DELETE CASCADE
);";
        cmd.ExecuteNonQuery();
        RegisterTableColumns(connection, "dataset_metadata");
    }

    private void ProcessTsv(SqliteConnection connection, long importId, ZipArchiveEntry entry, CancellationToken cancellationToken) {
        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8, true);

        var config = new CsvConfiguration(CultureInfo.InvariantCulture) {
            Delimiter = "\t",
            BadDataFound = null,
            MissingFieldFound = null,
            TrimOptions = TrimOptions.None,
            DetectColumnCountChanges = false
        };

        using var csv = new CsvReader(reader, config);
        if (!csv.Read()) {
            _console.MarkupLine($"[yellow]{entry.FullName} is empty; skipping.[/]");
            return;
        }

        csv.ReadHeader();
        var headerRecord = csv.HeaderRecord ?? Array.Empty<string>();
        var headers = headerRecord.ToList();

        if (headers.Count == 0) {
            _console.MarkupLine($"[yellow]{entry.FullName} does not provide headers; skipping.[/]");
            return;
        }

        var columnInfos = BuildColumnInfos(headers);
        var sanitizedHeaders = columnInfos.Select(ci => ci.Sanitized).ToList();

        var tableName = SanitizeTableName(Path.GetFileNameWithoutExtension(entry.Name));
        var existingColumns = EnsureDataTable(connection, tableName, sanitizedHeaders);

        using var transaction = connection.BeginTransaction();
        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = BuildInsertSql(tableName, sanitizedHeaders);

        var importParam = insert.CreateParameter();
        importParam.ParameterName = "@import_id";
        importParam.Value = importId;
        insert.Parameters.Add(importParam);

        var parameters = new SqliteParameter[sanitizedHeaders.Count];
        for (var i = 0; i < sanitizedHeaders.Count; i++) {
            var param = insert.CreateParameter();
            param.ParameterName = "@c" + i.ToString(CultureInfo.InvariantCulture);
            insert.Parameters.Add(param);
            parameters[i] = param;
        }

        insert.Prepare();

        long inserted = 0;
        while (csv.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            for (var i = 0; i < columnInfos.Count; i++) {
                var raw = csv.GetField(i);
                parameters[i].Value = string.IsNullOrEmpty(raw) ? DBNull.Value : raw;
            }

            inserted += insert.ExecuteNonQuery();
        }

        transaction.Commit();
        RegisterTableColumns(tableName, existingColumns);
        _console.MarkupLine($"    {tableName}: inserted {inserted:N0} rows (columns: {existingColumns.Count}).");
    }

    private HashSet<string> EnsureDataTable(SqliteConnection connection, string tableName, IReadOnlyList<string> csvColumns) {
        var existing = GetTableColumns(connection, tableName);
        if (existing is null) {
            CreateNewDataTable(connection, tableName, csvColumns);
            existing = GetTableColumns(connection, tableName) ?? throw new InvalidOperationException($"Failed to create table {tableName}.");
            return existing;
        }

        foreach (var column in csvColumns) {
            if (existing.Contains(column)) {
                continue;
            }

            using var alter = connection.CreateCommand();
            alter.CommandText = $"ALTER TABLE {QuoteIdentifier(tableName)} ADD COLUMN {QuoteIdentifier(column)} TEXT;";
            alter.ExecuteNonQuery();
            existing.Add(column);
        }

        return existing;
    }

    private static List<ColumnInfo> BuildColumnInfos(IReadOnlyList<string> headers) {
        var result = new List<ColumnInfo>(headers.Count);
        var occurrences = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var header in headers) {
            var sanitized = SanitizeColumnName(header);
            var baseName = sanitized;

            if (occurrences.TryGetValue(baseName, out var count)) {
                count++;
                var candidate = $"{baseName}_{count}";
                while (occurrences.ContainsKey(candidate)) {
                    count++;
                    candidate = $"{baseName}_{count}";
                }

                occurrences[baseName] = count;
                occurrences[candidate] = 0;
                sanitized = candidate;
            } else {
                occurrences[baseName] = 0;
            }

            result.Add(new ColumnInfo(header, sanitized));
        }

        return result;
    }

    private static string SanitizeColumnName(string original) {
        if (string.IsNullOrWhiteSpace(original)) {
            return "column";
        }

        var trimmed = original.Trim();
        var colonIndex = trimmed.IndexOf(':');
        if (colonIndex >= 0) {
            trimmed = trimmed[(colonIndex + 1)..];
        }

        if (string.IsNullOrWhiteSpace(trimmed)) {
            trimmed = "column";
        }

        var builder = new StringBuilder(trimmed.Length);
        foreach (var ch in trimmed) {
            if (char.IsLetterOrDigit(ch) || ch == '_') {
                builder.Append(ch);
            } else {
                builder.Append('_');
            }
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

    private static HashSet<string>? GetTableColumns(SqliteConnection connection, string tableName) {
        using var pragma = connection.CreateCommand();
        pragma.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";
        using var reader = pragma.ExecuteReader();
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var any = false;
        while (reader.Read()) {
            any = true;
            result.Add(reader.GetString(1));
        }

        return any ? result : null;
    }

    private static void CreateNewDataTable(SqliteConnection connection, string tableName, IReadOnlyList<string> csvColumns) {
        var columns = new List<string> {
            "\"import_id\" INTEGER NOT NULL"
        };

        foreach (var column in csvColumns) {
            columns.Add($"{QuoteIdentifier(column)} TEXT");
        }

        columns.Add("FOREIGN KEY(\"import_id\") REFERENCES import_metadata(\"id\") ON DELETE CASCADE");

        using var create = connection.CreateCommand();
        create.CommandText = $"CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (\n    {string.Join(",\n    ", columns)}\n);";
        create.ExecuteNonQuery();

    }

    private static string BuildInsertSql(string tableName, IReadOnlyList<string> csvColumns) {
        var columns = new List<string> { "import_id" };
        columns.AddRange(csvColumns);
        var identifiers = string.Join(", ", columns.Select(QuoteIdentifier));

        var parameters = new List<string> { "@import_id" };
        for (var i = 0; i < csvColumns.Count; i++) {
            parameters.Add("@c" + i.ToString(CultureInfo.InvariantCulture));
        }

        return $"INSERT INTO {QuoteIdentifier(tableName)} ({identifiers}) VALUES ({string.Join(", ", parameters)});";
    }

    private void ProcessReferenceJsonl(SqliteConnection connection, long importId, ZipArchiveEntry entry, CancellationToken cancellationToken) {
        EnsureReferenceJsonTable(connection);

        using var transaction = connection.BeginTransaction();
        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = @"INSERT INTO reference_json (import_id, reference_id, json)
VALUES (@import_id, @reference_id, @json);";

        var importParam = insert.CreateParameter();
        importParam.ParameterName = "@import_id";
        importParam.Value = importId;
        insert.Parameters.Add(importParam);

        var idParam = insert.CreateParameter();
        idParam.ParameterName = "@reference_id";
        insert.Parameters.Add(idParam);

        var jsonParam = insert.CreateParameter();
        jsonParam.ParameterName = "@json";
        insert.Parameters.Add(jsonParam);

        insert.Prepare();

        long inserted = 0;
        string? line;
        while ((line = reader.ReadLine()) is not null) {
            cancellationToken.ThrowIfCancellationRequested();
            var trimmed = line.Trim();
            if (trimmed.Length == 0) {
                continue;
            }

            string? referenceId = null;
            try {
                using var doc = JsonDocument.Parse(trimmed);
                if (doc.RootElement.TryGetProperty("id", out var idElement) && idElement.ValueKind == JsonValueKind.String) {
                    referenceId = idElement.GetString();
                }
            } catch (JsonException) {
                // keep referenceId as null and store raw json line
            }

            idParam.Value = referenceId ?? (object)DBNull.Value;
            jsonParam.Value = trimmed;
            inserted += insert.ExecuteNonQuery();
        }

        transaction.Commit();
        _console.MarkupLine($"    reference_json: inserted {inserted:N0} rows.");
    }

    private void EnsureReferenceJsonTable(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS reference_json (
    import_id INTEGER NOT NULL,
    reference_id TEXT,
    json TEXT NOT NULL,
    FOREIGN KEY(import_id) REFERENCES import_metadata(id) ON DELETE CASCADE
);";
        cmd.ExecuteNonQuery();
        RegisterTableColumns(connection, "reference_json");
    }

    private void ProcessSourceYaml(SqliteConnection connection, long importId, ZipArchiveEntry entry, CancellationToken cancellationToken) {
        EnsureSourceMetadataTable(connection);

        using var stream = entry.Open();
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        var yamlText = reader.ReadToEnd();
        cancellationToken.ThrowIfCancellationRequested();

        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        var map = deserializer.Deserialize<Dictionary<string, object?>>(yamlText) ?? new Dictionary<string, object?>();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO source_metadata (import_id, source_key, title, alias, description, issued, version, raw_yaml, filename)
VALUES (@import_id, @key, @title, @alias, @description, @issued, @version, @raw_yaml, @filename);";
        cmd.Parameters.AddWithValue("@import_id", importId);
        cmd.Parameters.AddWithValue("@key", ExtractString(map, "key") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@title", ExtractString(map, "title") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@alias", ExtractString(map, "alias") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@description", ExtractString(map, "description") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@issued", ExtractString(map, "issued") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@version", ExtractString(map, "version") ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@raw_yaml", yamlText);
        cmd.Parameters.AddWithValue("@filename", entry.FullName);
        cmd.ExecuteNonQuery();
    }

    private void EnsureSourceMetadataTable(SqliteConnection connection) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS source_metadata (
    import_id INTEGER NOT NULL,
    source_key TEXT,
    title TEXT,
    alias TEXT,
    description TEXT,
    issued TEXT,
    version TEXT,
    raw_yaml TEXT,
    filename TEXT,
    FOREIGN KEY(import_id) REFERENCES import_metadata(id) ON DELETE CASCADE
);";
        cmd.ExecuteNonQuery();

        RegisterTableColumns(connection, "source_metadata");
    }

    private void EnsureColumnIndexes(SqliteConnection connection, string tableName, HashSet<string> columns) {
        if (columns.Count == 0) {
            return;
        }

        foreach (var column in columns) {
            if (!ShouldIndexColumn(tableName, column)) {
                continue;
            }

            CreateColumnIndex(connection, tableName, column);
        }

        if (string.Equals(tableName, "source_metadata", StringComparison.OrdinalIgnoreCase) && columns.Contains("alias")) {
            CreateColumnIndex(connection, tableName, "alias");
        }
    }

    private static bool ShouldIndexColumn(string tableName, string columnName) {
        if (string.IsNullOrWhiteSpace(columnName)) {
            return false;
        }

        var lower = columnName.ToLowerInvariant();
        if (lower is "import_id") {
            return false;
        }

        if (IndexedColumnNames.Contains(columnName)) {
            return true;
        }

        if (columnName.IndexOf("authorship", StringComparison.OrdinalIgnoreCase) >= 0 &&
            !columnName.EndsWith("id", StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        if (lower.EndsWith("id", StringComparison.Ordinal)) {
            return true;
        }

        if (lower.EndsWith("key", StringComparison.Ordinal)) {
            return true;
        }

        return false;
    }

    private void CreateColumnIndex(SqliteConnection connection, string tableName, string columnName) {
        using var cmd = connection.CreateCommand();
        var indexName = $"idx_{SanitizeIdentifierPart(tableName)}_{SanitizeIdentifierPart(columnName)}";
        _console.MarkupLine($"[grey]Ensuring index {indexName} on {tableName}({columnName}).[/]");
        cmd.CommandText = $"CREATE INDEX IF NOT EXISTS {QuoteIdentifier(indexName)} ON {QuoteIdentifier(tableName)}({QuoteIdentifier(columnName)});";
        cmd.ExecuteNonQuery();
    }

    private void RebuildFtsIfNeeded(SqliteConnection connection, string tableName, HashSet<string> columns) {
        if (columns.Count == 0) {
            return;
        }

        if (string.Equals(tableName, "vernacularname", StringComparison.OrdinalIgnoreCase)) {
            var hasName = columns.Contains("name");
            var hasTrans = columns.Contains("transliteration");
            if (hasName || hasTrans) {
                RebuildVernacularFts(connection, hasName, hasTrans);
            }
            return;
        }

        if (string.Equals(tableName, "nameusage", StringComparison.OrdinalIgnoreCase)) {
            RebuildNameUsageScientificNameFts(connection, columns);
            RebuildNameUsageScientificContextFts(connection, columns);
            RebuildNameUsageAuthorshipFts(connection, columns);
            RebuildNameUsageNotesFts(connection, columns);
        }

        if (string.Equals(tableName, "distribution", StringComparison.OrdinalIgnoreCase) && columns.Contains("area")) {
            RebuildDistributionAreaFts(connection);
        }
    }

    private void RebuildVernacularFts(SqliteConnection connection, bool hasName, bool hasTransliteration) {
        var columns = new List<(string FtsColumn, string DatasetColumn)>();
        if (hasName) {
            columns.Add(("name", "name"));
        }

        if (hasTransliteration) {
            columns.Add(("transliteration", "transliteration"));
        }

        RebuildFtsTable(connection, "vernacularname_fts", "vernacularname", columns);
    }

    private void RebuildDistributionAreaFts(SqliteConnection connection) {
        var columns = new List<(string FtsColumn, string DatasetColumn)> {
            ("area", "area")
        };
        RebuildFtsTable(connection, "distribution_area_fts", "distribution", columns);
    }

    private void RebuildNameUsageScientificNameFts(SqliteConnection connection, HashSet<string> columns) {
        var active = GetActiveColumns(NameUsageColumnMappings.Where(m => m.IncludeInScientificName), columns);
        if (active.Count == 0) {
            return;
        }

        RebuildFtsTable(connection, "nameusage_scientific_name_fts", "nameusage", active);
    }

    private void RebuildNameUsageScientificContextFts(SqliteConnection connection, HashSet<string> columns) {
        var active = GetActiveColumns(NameUsageColumnMappings.Where(m => m.IncludeInContext), columns);
        if (active.Count == 0) {
            return;
        }

        RebuildFtsTable(connection, "nameusage_taxon_context_fts", "nameusage", active);
    }

    private void RebuildNameUsageAuthorshipFts(SqliteConnection connection, HashSet<string> columns) {
        var selected = new List<(string FtsColumn, string DatasetColumn)>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var column in columns.OrderBy(name => name, StringComparer.OrdinalIgnoreCase)) {
            if (!column.Contains("authorship", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            if (column.EndsWith("id", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var ftsColumn = ToFtsColumnName(column);
            if (!seen.Add(ftsColumn)) {
                continue;
            }

            selected.Add((ftsColumn, column));
        }

        if (selected.Count == 0) {
            return;
        }

        RebuildFtsTable(connection, "nameusage_authorship_fts", "nameusage", selected);
    }

    private void RebuildNameUsageNotesFts(SqliteConnection connection, HashSet<string> columns) {
        var candidates = new (string FtsColumn, string[] SourceColumns)[] {
            ("etymology", new[] { "etymology" }),
            ("nameremarks", new[] { "nameRemarks" }),
            ("remarks", new[] { "remarks" })
        };

        var active = new List<(string FtsColumn, string DatasetColumn)>();
        foreach (var (ftsColumn, sourceColumns) in candidates) {
            var match = sourceColumns.FirstOrDefault(columns.Contains);
            if (match is null) {
                continue;
            }
            active.Add((ftsColumn, match));
        }

        if (active.Count == 0) {
            return;
        }

        RebuildFtsTable(connection, "nameusage_notes_fts", "nameusage", active);
    }

    private static List<(string FtsColumn, string DatasetColumn)> GetActiveColumns(IEnumerable<ColumnMapping> mappings, HashSet<string> columns) {
        var result = new List<(string FtsColumn, string DatasetColumn)>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var mapping in mappings) {
            if (!seen.Add(mapping.FtsColumn)) {
                continue;
            }

            var datasetColumn = mapping.Candidates.FirstOrDefault(columns.Contains);
            if (datasetColumn is null) {
                continue;
            }

            result.Add((mapping.FtsColumn, datasetColumn));
        }

        return result;
    }

    private void RebuildFtsTable(SqliteConnection connection, string ftsTableName, string sourceTable, List<(string FtsColumn, string DatasetColumn)> activeColumns, string tokenizer = "unicode61") {
        if (activeColumns.Count == 0) {
            return;
        }

        _console.MarkupLine($"[grey]Rebuilding full-text index {ftsTableName} from {sourceTable} ({activeColumns.Count} columns).[/]");
        var ftsColumns = string.Join(", ", activeColumns.Select(c => c.FtsColumn));

        using (var create = connection.CreateCommand()) {
            create.CommandText = $"CREATE VIRTUAL TABLE IF NOT EXISTS {QuoteIdentifier(ftsTableName)} USING fts5({ftsColumns}, tokenize='{tokenizer}');";
            create.ExecuteNonQuery();
        }

        using var transaction = connection.BeginTransaction();

        using (var clear = connection.CreateCommand()) {
            clear.Transaction = transaction;
            clear.CommandText = $"DELETE FROM {QuoteIdentifier(ftsTableName)};";
            clear.ExecuteNonQuery();
        }

        var insertColumns = "rowid, " + ftsColumns;
        var selectColumns = "rowid, " + string.Join(", ", activeColumns.Select(c => QuoteIdentifier(c.DatasetColumn)));
        var nonNullFilters = activeColumns.Select(c => $"{QuoteIdentifier(c.DatasetColumn)} IS NOT NULL").ToList();
        var whereClause = nonNullFilters.Count > 0 ? " WHERE " + string.Join(" OR ", nonNullFilters) : string.Empty;

        using (var populate = connection.CreateCommand()) {
            populate.Transaction = transaction;
            populate.CommandText = $"INSERT INTO {QuoteIdentifier(ftsTableName)}({insertColumns}) SELECT {selectColumns} FROM {QuoteIdentifier(sourceTable)}{whereClause};";
            populate.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private static string ToFtsColumnName(string sourceName) {
        var builder = new StringBuilder(sourceName.Length);
        foreach (var ch in sourceName) {
            if (char.IsLetterOrDigit(ch)) {
                builder.Append(char.ToLowerInvariant(ch));
            } else {
                builder.Append('_');
            }
        }

        var result = builder.ToString().Trim('_');
        if (string.IsNullOrEmpty(result)) {
            result = "col";
        }

        if (char.IsDigit(result[0])) {
            result = "_" + result;
        }

        return result;
    }

    private static HashSet<string> BuildIndexedColumnNames() {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in NameUsageColumnMappings) {
            if (!mapping.CreateIndex) {
                continue;
            }

            foreach (var candidate in mapping.Candidates) {
                if (string.IsNullOrWhiteSpace(candidate)) {
                    continue;
                }

                set.Add(candidate);
            }
        }

        return set;
    }

    private static string QuoteIdentifier(string identifier) {
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private static string SanitizeTableName(string baseName) {
        if (string.IsNullOrWhiteSpace(baseName)) {
            return "table";
        }

        var lower = baseName.ToLowerInvariant();
        var builder = new StringBuilder(lower.Length + 4);
        if (!char.IsLetter(lower[0]) && lower[0] != '_') {
            builder.Append('_');
        }

        foreach (var ch in lower) {
            if (char.IsLetterOrDigit(ch) || ch == '_') {
                builder.Append(ch);
            } else {
                builder.Append('_');
            }
        }

        return builder.ToString();
    }

    private static string SanitizeFileStem(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return "dataset";
        }

        var builder = new StringBuilder(value.Length);
        foreach (var ch in value) {
            if (char.IsLetterOrDigit(ch) || ch is '.' or '_' or '-') {
                builder.Append(ch);
            }
        }

        if (builder.Length == 0) {
            return "dataset";
        }

        var sanitized = builder.ToString().Trim('_');
        if (sanitized.Length == 0) {
            sanitized = builder.ToString();
        }

        if (sanitized.Length > 255) {
            sanitized = sanitized[..255];
        }

        return sanitized;
    }

    private static string SanitizeIdentifierPart(string value) {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value) {
            builder.Append(char.IsLetterOrDigit(ch) ? ch : '_');
        }
        return builder.Length > 0 ? builder.ToString() : "part";
    }

    private void ConfigureConnection(SqliteConnection connection) {
        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        ConfigureConnectionPerformance(connection);
    }

    private void ConfigureConnectionPerformance(SqliteConnection connection) {
        _console.MarkupLine("[grey]Applying SQLite pragmas for faster bulk load (journal_mode=MEMORY, synchronous=OFF, temp_store=MEMORY, cache_size=-200000).[/]");

        using (var journal = connection.CreateCommand()) {
            journal.CommandText = "PRAGMA journal_mode = MEMORY;";
            journal.ExecuteScalar();
        }

        using (var synchronous = connection.CreateCommand()) {
            synchronous.CommandText = "PRAGMA synchronous = OFF;";
            synchronous.ExecuteNonQuery();
        }

        using (var tempStore = connection.CreateCommand()) {
            tempStore.CommandText = "PRAGMA temp_store = MEMORY;";
            tempStore.ExecuteNonQuery();
        }

        using (var cacheSize = connection.CreateCommand()) {
            cacheSize.CommandText = "PRAGMA cache_size = -200000;";
            cacheSize.ExecuteNonQuery();
        }
    }

    private void FinalizeIndexesAndFts(SqliteConnection connection) {
        if (_tableColumnsForIndexing.Count == 0) {
            return;
        }

        foreach (var tableName in _tableColumnsForIndexing.Keys.OrderBy(t => t, StringComparer.OrdinalIgnoreCase)) {
            if (!_tableColumnsForIndexing.TryGetValue(tableName, out var columns) || columns.Count == 0) {
                continue;
            }

            _console.MarkupLine($"[grey]Finalizing indexes and FTS for {tableName}...[/]");
            EnsureColumnIndexes(connection, tableName, columns);
            RebuildFtsIfNeeded(connection, tableName, columns);
        }

        _tableColumnsForIndexing.Clear();
    }

    private void RegisterTableColumns(SqliteConnection connection, string tableName) {
        var columns = GetTableColumns(connection, tableName);
        if (columns is null) {
            return;
        }

        RegisterTableColumns(tableName, columns);
    }

    private void RegisterTableColumns(string tableName, HashSet<string> columns) {
        if (!_tableColumnsForIndexing.TryGetValue(tableName, out var existing)) {
            _tableColumnsForIndexing[tableName] = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
            return;
        }

        existing.UnionWith(columns);
    }

    private string ToRelative(string fullPath) {
        try {
            var relative = Path.GetRelativePath(_rootDir, fullPath);
            return string.IsNullOrWhiteSpace(relative) ? Path.GetFileName(fullPath) : relative.Replace('\\', '/');
        } catch {
            return Path.GetFileName(fullPath);
        }
    }

    private sealed class DatasetMetadata {
        public string? RawYaml { get; init; }
        public string? Key { get; init; }
        public string? Title { get; init; }
        public string? Alias { get; init; }
        public string? Description { get; init; }
        public string? Issued { get; init; }
        public string? Version { get; init; }
    }
}
