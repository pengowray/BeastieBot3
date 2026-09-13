using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

// Read-only access to cached taxon items in the Wikidata cache (Datastore:wikidata_cache_sqlite).
// Opens the file with Mode=ReadOnly and does no schema work, so it never touches a cache another
// process is writing to; WikidataCacheStore.Open is not used for that reason.
//
// ReadAll pages by entity_numeric_id instead of holding one statement open for the whole run, so
// a concurrent writer's WAL checkpoints are not held back for the minute or so a full pass takes.

namespace BeastieBot3.WikidataEdits;

internal sealed class WdTaxonItemReader : IDisposable {
    private const int LookupBatchSize = 500;
    private const int PageSize = 1000;

    private readonly SqliteConnection _connection;

    private WdTaxonItemReader(SqliteConnection connection) {
        _connection = connection;
    }

    /// Rows read so far whose JSON failed to parse or held no usable item.
    public int SkippedRows { get; private set; }

    public static WdTaxonItemReader Open(string databasePath) => new(OpenReadOnly(databasePath));

    internal static SqliteConnection OpenReadOnly(string databasePath) {
        if (string.IsNullOrWhiteSpace(databasePath)) {
            throw new ArgumentException("Wikidata cache path is empty.", nameof(databasePath));
        }

        if (!File.Exists(databasePath)) {
            throw new FileNotFoundException("Wikidata cache database not found.", databasePath);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false,
        };
        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();
        return connection;
    }

    /// Items with downloaded JSON, for sizing a progress bar before ReadAll.
    public int CountDownloaded() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM wikidata_entities WHERE json_downloaded = 1";
        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    public WdTaxonItem? Get(string qid) {
        if (!TryParseQid(qid, out var numericId)) {
            return null;
        }

        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT entity_numeric_id, downloaded_at, json
            FROM wikidata_entities
            WHERE entity_numeric_id = @id AND json_downloaded = 1 AND json IS NOT NULL
            """;
        command.Parameters.AddWithValue("@id", numericId);
        using var reader = command.ExecuteReader();
        return reader.Read() ? ParseRow(reader) : null;
    }

    /// Items keyed by Qid. Ids that are malformed, not cached, or not downloaded are left out.
    public IReadOnlyDictionary<string, WdTaxonItem> GetMany(IEnumerable<string> qids) {
        var ids = new List<long>();
        var seen = new HashSet<long>();
        foreach (var qid in qids) {
            if (TryParseQid(qid, out var numericId) && seen.Add(numericId)) {
                ids.Add(numericId);
            }
        }

        var result = new Dictionary<string, WdTaxonItem>(ids.Count, StringComparer.OrdinalIgnoreCase);
        for (var start = 0; start < ids.Count; start += LookupBatchSize) {
            var count = Math.Min(LookupBatchSize, ids.Count - start);
            using var command = _connection.CreateCommand();
            var sql = new StringBuilder("""
                SELECT entity_numeric_id, downloaded_at, json
                FROM wikidata_entities
                WHERE json_downloaded = 1 AND json IS NOT NULL AND entity_numeric_id IN (
                """);
            for (var i = 0; i < count; i++) {
                var name = "@p" + i.ToString(CultureInfo.InvariantCulture);
                sql.Append(i == 0 ? name : "," + name);
                command.Parameters.AddWithValue(name, ids[start + i]);
            }

            sql.Append(')');
            command.CommandText = sql.ToString();
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var item = ParseRow(reader);
                if (item is not null) {
                    result[item.Qid] = item;
                }
            }
        }

        return result;
    }

    /// Every downloaded item, in entity_numeric_id order, parsed one at a time.
    public IEnumerable<WdTaxonItem> ReadAll(CancellationToken cancellationToken = default) {
        var after = long.MinValue;
        while (true) {
            cancellationToken.ThrowIfCancellationRequested();
            using var command = _connection.CreateCommand();
            command.CommandText = """
                SELECT entity_numeric_id, downloaded_at, json
                FROM wikidata_entities
                WHERE json_downloaded = 1 AND json IS NOT NULL AND entity_numeric_id > @after
                ORDER BY entity_numeric_id
                LIMIT @limit
                """;
            command.Parameters.AddWithValue("@after", after);
            command.Parameters.AddWithValue("@limit", PageSize);

            var rows = 0;
            using (var reader = command.ExecuteReader()) {
                while (reader.Read()) {
                    rows++;
                    after = reader.GetInt64(0);
                    var item = ParseRow(reader);
                    if (item is not null) {
                        yield return item;
                    }
                }
            }

            if (rows < PageSize) {
                yield break;
            }
        }
    }

    private WdTaxonItem? ParseRow(SqliteDataReader reader) {
        var downloadedAt = reader.IsDBNull(1) ? null : IucnApiCacheStore.ParseStoredUtc(reader.GetString(1));
        try {
            var item = WdTaxonItemParser.Parse(reader.GetString(2), downloadedAt);
            if (item is null) {
                SkippedRows++;
            }

            return item;
        }
        catch (JsonException) {
            SkippedRows++;
            return null;
        }
    }

    /// Accepts "Q24024" or "q24024" (surrounding whitespace ignored).
    internal static bool TryParseQid(string? qid, out long numericId) {
        numericId = 0;
        var span = qid.AsSpan().Trim();
        return span.Length >= 2
            && (span[0] == 'Q' || span[0] == 'q')
            && long.TryParse(span[1..], NumberStyles.None, CultureInfo.InvariantCulture, out numericId)
            && numericId > 0;
    }

    public void Dispose() => _connection.Dispose();
}
