using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class WikidataCacheStore : IDisposable {
    private readonly SqliteConnection _connection;
    private readonly ApiImportMetadataStore _importStore;

    private WikidataCacheStore(SqliteConnection connection) {
        _connection = connection;
        _importStore = new ApiImportMetadataStore(connection);
    }

    public static WikidataCacheStore Open(string databasePath) {
        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directory)) {
            Directory.CreateDirectory(directory);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA journal_mode = WAL;";
            pragma.ExecuteNonQuery();
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        var store = new WikidataCacheStore(connection);
        store._importStore.EnsureSchema();
        store.EnsureSchema();
        return store;
    }

    public void Dispose() => _connection.Dispose();

    public long BeginImport(string url) => _importStore.BeginImport(url);

    public void CompleteImportSuccess(long importId, int httpStatus, long payloadBytes, TimeSpan duration) =>
        _importStore.CompleteImportSuccess(importId, httpStatus, payloadBytes, duration);

    public void CompleteImportFailure(long importId, string errorMessage, int? statusCode, TimeSpan duration) =>
        _importStore.CompleteImportFailure(importId, errorMessage, statusCode, duration);

    public void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
    CREATE TABLE IF NOT EXISTS wikidata_entities (
    entity_numeric_id INTEGER PRIMARY KEY,
    entity_id TEXT NOT NULL UNIQUE,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    has_p141 INTEGER NOT NULL DEFAULT 0,
    has_p627 INTEGER NOT NULL DEFAULT 0,
    json_downloaded INTEGER NOT NULL DEFAULT 0,
    downloaded_at TEXT,
    import_id INTEGER REFERENCES import_metadata(id) ON DELETE SET NULL,
    label_en TEXT,
    description_en TEXT,
    last_error TEXT,
    last_attempt_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    json TEXT
);

internal sealed record WikidataEnwikiSitelink(long EntityNumericId, string EntityId, string Title);

internal sealed record WikidataPendingIucnMatchRow(
    string IucnTaxonId,
    long NumericId,
    string EntityId,
    string? MatchedName,
    string MatchMethod,
    bool IsSynonym,
    DateTime DiscoveredAt,
    DateTime LastSeenAt);
CREATE INDEX IF NOT EXISTS idx_wikidata_entities_downloaded ON wikidata_entities(json_downloaded, entity_numeric_id);
CREATE INDEX IF NOT EXISTS idx_wikidata_entities_last_seen ON wikidata_entities(last_seen_at);
CREATE TABLE IF NOT EXISTS wikidata_sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wikidata_p627_values (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(entity_numeric_id, source, value)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_p627_value ON wikidata_p627_values(value);
CREATE TABLE IF NOT EXISTS wikidata_p141_statements (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    statement_id TEXT NOT NULL,
    status_qid INTEGER NOT NULL,
    status_entity_id TEXT NOT NULL,
    rank TEXT NOT NULL,
    PRIMARY KEY(entity_numeric_id, statement_id)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_p141_status ON wikidata_p141_statements(status_qid);
CREATE TABLE IF NOT EXISTS wikidata_p141_references (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    statement_id TEXT NOT NULL,
    reference_hash TEXT NOT NULL,
    source_qid INTEGER,
    iucn_taxon_id TEXT NOT NULL,
    PRIMARY KEY(entity_numeric_id, statement_id, reference_hash, iucn_taxon_id)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_p141_ref_iucn ON wikidata_p141_references(iucn_taxon_id);
CREATE TABLE IF NOT EXISTS wikidata_scientific_names (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    language TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY(entity_numeric_id, language)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_scientific_name ON wikidata_scientific_names(name COLLATE NOCASE);
CREATE TABLE IF NOT EXISTS wikidata_taxon_name_index (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    normalized_name TEXT NOT NULL,
    PRIMARY KEY(entity_numeric_id, normalized_name)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_taxon_name_value ON wikidata_taxon_name_index(normalized_name);
CREATE INDEX IF NOT EXISTS idx_wikidata_taxon_name_entity ON wikidata_taxon_name_index(entity_numeric_id);
CREATE TABLE IF NOT EXISTS wikidata_taxon_rank (
    entity_numeric_id INTEGER PRIMARY KEY REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    rank_qid INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wikidata_taxon_rank ON wikidata_taxon_rank(rank_qid);
CREATE TABLE IF NOT EXISTS wikidata_parent_taxa (
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    parent_qid INTEGER NOT NULL,
    PRIMARY KEY(entity_numeric_id, parent_qid)
);
CREATE INDEX IF NOT EXISTS idx_wikidata_parent_taxa_parent ON wikidata_parent_taxa(parent_qid);
CREATE TABLE IF NOT EXISTS wikidata_pending_iucn_matches (
    iucn_taxon_id TEXT PRIMARY KEY,
    entity_numeric_id INTEGER NOT NULL REFERENCES wikidata_entities(entity_numeric_id) ON DELETE CASCADE,
    entity_id TEXT NOT NULL,
    matched_name TEXT,
    match_method TEXT NOT NULL,
    is_synonym INTEGER NOT NULL,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pending_iucn_entity ON wikidata_pending_iucn_matches(entity_numeric_id);
""";
        command.ExecuteNonQuery();
    BackfillTaxonNameIndex();
    }

    public SeedUpsertResult UpsertSeeds(IReadOnlyList<WikidataSeedRow> seeds) {
        if (seeds.Count == 0) {
            return new SeedUpsertResult(0, 0);
        }

        var now = DateTime.UtcNow.ToString("O");
        var newCount = 0;
        var updatedCount = 0;
        using var tx = _connection.BeginTransaction();

        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = @"INSERT OR IGNORE INTO wikidata_entities(entity_numeric_id, entity_id, discovered_at, last_seen_at, has_p141, has_p627) VALUES (@id,@entity,@discovered,@seen,@p141,@p627)";
        var idParam = insert.Parameters.Add("@id", SqliteType.Integer);
        var entityParam = insert.Parameters.Add("@entity", SqliteType.Text);
        var discoveredParam = insert.Parameters.Add("@discovered", SqliteType.Text);
        var seenParam = insert.Parameters.Add("@seen", SqliteType.Text);
        var p141Param = insert.Parameters.Add("@p141", SqliteType.Integer);
        var p627Param = insert.Parameters.Add("@p627", SqliteType.Integer);

        using var update = _connection.CreateCommand();
        update.Transaction = tx;
        update.CommandText = @"UPDATE wikidata_entities SET entity_id=@entity, last_seen_at=@seen, has_p141=CASE WHEN has_p141=1 THEN 1 ELSE @p141 END, has_p627=CASE WHEN has_p627=1 THEN 1 ELSE @p627 END WHERE entity_numeric_id=@id";
        var updateId = update.Parameters.Add("@id", SqliteType.Integer);
        var updateEntity = update.Parameters.Add("@entity", SqliteType.Text);
        var updateSeen = update.Parameters.Add("@seen", SqliteType.Text);
        var updateP141 = update.Parameters.Add("@p141", SqliteType.Integer);
        var updateP627 = update.Parameters.Add("@p627", SqliteType.Integer);

        foreach (var seed in seeds) {
            idParam.Value = seed.NumericId;
            entityParam.Value = seed.EntityId;
            discoveredParam.Value = now;
            seenParam.Value = now;
            p141Param.Value = seed.HasP141 ? 1 : 0;
            p627Param.Value = seed.HasP627 ? 1 : 0;

            var inserted = insert.ExecuteNonQuery();
            if (inserted == 1) {
                newCount++;
                continue;
            }

            updateId.Value = seed.NumericId;
            updateEntity.Value = seed.EntityId;
            updateSeen.Value = now;
            updateP141.Value = seed.HasP141 ? 1 : 0;
            updateP627.Value = seed.HasP627 ? 1 : 0;
            update.ExecuteNonQuery();
            updatedCount++;
        }

        tx.Commit();
        return new SeedUpsertResult(newCount, updatedCount);
    }

    public IReadOnlyList<WikidataEnwikiSitelink> GetEnwikiSitelinks(long? resumeAfterNumericId, int limit) {
        if (limit <= 0) {
            return Array.Empty<WikidataEnwikiSitelink>();
        }

        using var command = _connection.CreateCommand();
        var builder = new System.Text.StringBuilder();
        builder.Append("SELECT entity_numeric_id, entity_id, json FROM wikidata_entities WHERE json_downloaded = 1 AND json IS NOT NULL AND TRIM(json) <> ''");
        if (resumeAfterNumericId.HasValue) {
            builder.Append(" AND entity_numeric_id > @resume");
            command.Parameters.AddWithValue("@resume", resumeAfterNumericId.Value);
        }

        builder.Append(" ORDER BY entity_numeric_id LIMIT @limit");
        command.CommandText = builder.ToString();
        command.Parameters.AddWithValue("@limit", limit);

        var list = new List<WikidataEnwikiSitelink>();
        using var reader = command.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            if (reader.IsDBNull(2)) {
                continue;
            }

            var json = reader.GetString(2);
            if (!WikidataSitelinkExtractor.TryGetEnwikiTitle(json, out var title) || string.IsNullOrWhiteSpace(title)) {
                continue;
            }

            list.Add(new WikidataEnwikiSitelink(reader.GetInt64(0), reader.GetString(1), title));
        }

        return list;
    }

    public void UpsertPendingIucnMatches(IReadOnlyList<WikidataPendingIucnMatchRow> matches) {
        if (matches.Count == 0) {
            return;
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
INSERT INTO wikidata_pending_iucn_matches(iucn_taxon_id, entity_numeric_id, entity_id, matched_name, match_method, is_synonym, discovered_at, last_seen_at)
VALUES (@iucn, @id, @entity, @name, @method, @syn, @discovered, @seen)
ON CONFLICT(iucn_taxon_id) DO UPDATE SET
    entity_numeric_id=excluded.entity_numeric_id,
    entity_id=excluded.entity_id,
    matched_name=excluded.matched_name,
    match_method=excluded.match_method,
    is_synonym=excluded.is_synonym,
    last_seen_at=excluded.last_seen_at
""";
        var iucnParam = command.Parameters.Add("@iucn", SqliteType.Text);
        var idParam = command.Parameters.Add("@id", SqliteType.Integer);
        var entityParam = command.Parameters.Add("@entity", SqliteType.Text);
        var nameParam = command.Parameters.Add("@name", SqliteType.Text);
        var methodParam = command.Parameters.Add("@method", SqliteType.Text);
        var synParam = command.Parameters.Add("@syn", SqliteType.Integer);
        var discoveredParam = command.Parameters.Add("@discovered", SqliteType.Text);
        var seenParam = command.Parameters.Add("@seen", SqliteType.Text);

        foreach (var match in matches) {
            iucnParam.Value = match.IucnTaxonId;
            idParam.Value = match.NumericId;
            entityParam.Value = match.EntityId;
            nameParam.Value = (object?)match.MatchedName ?? DBNull.Value;
            methodParam.Value = match.MatchMethod;
            synParam.Value = match.IsSynonym ? 1 : 0;
            discoveredParam.Value = match.DiscoveredAt.ToString("O");
            seenParam.Value = match.LastSeenAt.ToString("O");
            command.ExecuteNonQuery();
        }
    }

    public long GetSyncCursor(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM wikidata_sync_state WHERE key=@key LIMIT 1";
        command.Parameters.AddWithValue("@key", key);
        var result = command.ExecuteScalar() as string;
        return long.TryParse(result, out var value) ? value : 0L;
    }

    public void SetSyncCursor(string key, long value) {
        using var command = _connection.CreateCommand();
        command.CommandText = @"INSERT INTO wikidata_sync_state(key, value) VALUES (@key, @value)
ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", value.ToString());
        command.ExecuteNonQuery();
    }

    public IReadOnlyList<WikidataEntityWorkItem> GetPendingEntities(int limit, DateTime? refreshThreshold) {
        if (limit <= 0) {
            return Array.Empty<WikidataEntityWorkItem>();
        }

        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT entity_numeric_id, entity_id, downloaded_at, attempt_count
FROM wikidata_entities
WHERE json_downloaded = 0
   OR (@refresh IS NOT NULL AND downloaded_at IS NOT NULL AND downloaded_at < @refresh)
ORDER BY json_downloaded ASC, entity_numeric_id
LIMIT @limit";
        command.Parameters.AddWithValue("@refresh", refreshThreshold?.ToString("O") ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@limit", limit);

        var list = new List<WikidataEntityWorkItem>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var numericId = reader.GetInt64(0);
            var entityId = reader.GetString(1);
            var downloadedAtValue = reader.IsDBNull(2) ? null : reader.GetString(2);
            DateTime? downloadedAt = null;
            if (!string.IsNullOrEmpty(downloadedAtValue) && DateTime.TryParse(downloadedAtValue, out var parsed)) {
                downloadedAt = parsed;
            }

            var attemptCount = reader.GetInt32(3);
            list.Add(new WikidataEntityWorkItem(numericId, entityId, downloadedAt, attemptCount));
        }

        return list;
    }

    public IReadOnlyList<WikidataEntityWorkItem> GetFailedEntities(int limit) {
        if (limit <= 0) {
            return Array.Empty<WikidataEntityWorkItem>();
        }

        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT entity_numeric_id, entity_id, downloaded_at, attempt_count
FROM wikidata_entities
WHERE json_downloaded = 0 AND attempt_count > 0 AND last_error IS NOT NULL
ORDER BY IFNULL(last_attempt_at, last_seen_at) DESC
LIMIT @limit";
        command.Parameters.AddWithValue("@limit", limit);

        var list = new List<WikidataEntityWorkItem>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var numericId = reader.GetInt64(0);
            var entityId = reader.GetString(1);
            var downloadedAtValue = reader.IsDBNull(2) ? null : reader.GetString(2);
            DateTime? downloadedAt = null;
            if (!string.IsNullOrEmpty(downloadedAtValue) && DateTime.TryParse(downloadedAtValue, out var parsed)) {
                downloadedAt = parsed;
            }

            var attemptCount = reader.GetInt32(3);
            list.Add(new WikidataEntityWorkItem(numericId, entityId, downloadedAt, attemptCount));
        }

        return list;
    }

    public int CountPendingEntities(DateTime? refreshThreshold) {
        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT COUNT(*)
FROM wikidata_entities
WHERE json_downloaded = 0
   OR (@refresh IS NOT NULL AND downloaded_at IS NOT NULL AND downloaded_at < @refresh)";
        command.Parameters.AddWithValue("@refresh", refreshThreshold?.ToString("O") ?? (object)DBNull.Value);
        var result = command.ExecuteScalar();
        return Convert.ToInt32(result ?? 0);
    }

    public int CountFailedEntities() {
        using var command = _connection.CreateCommand();
        command.CommandText = @"SELECT COUNT(*)
FROM wikidata_entities
WHERE json_downloaded = 0 AND attempt_count > 0 AND last_error IS NOT NULL";
        var result = command.ExecuteScalar();
        return Convert.ToInt32(result ?? 0);
    }

    public int ResetCachedPayloads() {
        using var tx = _connection.BeginTransaction();

        using (var clear = _connection.CreateCommand()) {
            clear.Transaction = tx;
            clear.CommandText =
                """
DELETE FROM wikidata_p627_values;
DELETE FROM wikidata_p141_references;
DELETE FROM wikidata_p141_statements;
DELETE FROM wikidata_scientific_names;
DELETE FROM wikidata_taxon_name_index;
DELETE FROM wikidata_taxon_rank;
DELETE FROM wikidata_parent_taxa;
""";
            clear.ExecuteNonQuery();
        }

        using var reset = _connection.CreateCommand();
        reset.Transaction = tx;
        reset.CommandText =
            """
UPDATE wikidata_entities
SET json_downloaded = 0,
    downloaded_at = NULL,
    import_id = NULL,
    label_en = NULL,
    description_en = NULL,
    json = NULL,
    attempt_count = 0,
    last_error = NULL,
    last_attempt_at = NULL
""";
        var affected = reset.ExecuteNonQuery();

        tx.Commit();
        return affected;
    }

    public void RecordFailure(long numericId, string errorMessage) {
        using var command = _connection.CreateCommand();
        command.CommandText = @"UPDATE wikidata_entities
SET attempt_count = attempt_count + 1,
    last_error = @error,
    last_attempt_at = @attempted
WHERE entity_numeric_id = @id";
        command.Parameters.AddWithValue("@error", errorMessage);
        command.Parameters.AddWithValue("@attempted", DateTime.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("@id", numericId);
        command.ExecuteNonQuery();
    }

    public void RecordSuccess(WikidataEntityRecord record, long importId, string json, DateTime downloadedAt) {
        using var tx = _connection.BeginTransaction();

        using (var update = _connection.CreateCommand()) {
            update.Transaction = tx;
            update.CommandText = @"UPDATE wikidata_entities
SET json_downloaded=1,
    downloaded_at=@downloaded,
    import_id=@import,
    label_en=@label,
    description_en=@description,
    has_p141=@p141,
    has_p627=@p627,
    json=@json,
    attempt_count=0,
    last_error=NULL,
    last_attempt_at=@attempted
WHERE entity_numeric_id=@id";
            update.Parameters.AddWithValue("@downloaded", downloadedAt.ToString("O"));
            update.Parameters.AddWithValue("@import", importId);
            update.Parameters.AddWithValue("@label", (object?)record.LabelEn ?? DBNull.Value);
            update.Parameters.AddWithValue("@description", (object?)record.DescriptionEn ?? DBNull.Value);
            update.Parameters.AddWithValue("@p141", record.HasP141 ? 1 : 0);
            update.Parameters.AddWithValue("@p627", record.HasP627 ? 1 : 0);
            update.Parameters.AddWithValue("@json", json);
            update.Parameters.AddWithValue("@attempted", DateTime.UtcNow.ToString("O"));
            update.Parameters.AddWithValue("@id", record.NumericId);
            update.ExecuteNonQuery();
        }

        ClearIndexes(record.NumericId, tx);
        InsertP627Values(record, tx);
        _ = InsertP141Statements(record, tx);
        InsertScientificNames(record, tx);
        InsertTaxonNameIndex(record, tx);
        InsertRank(record, tx);
        InsertParentTaxa(record, tx);

        tx.Commit();
    }

    private void ClearIndexes(long numericId, SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = @"DELETE FROM wikidata_p627_values WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_p141_references WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_p141_statements WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_scientific_names WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_taxon_name_index WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_taxon_rank WHERE entity_numeric_id=@id;
    DELETE FROM wikidata_parent_taxa WHERE entity_numeric_id=@id;";
        command.Parameters.AddWithValue("@id", numericId);
        command.ExecuteNonQuery();
    }

    private void InsertP627Values(WikidataEntityRecord record, SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "INSERT INTO wikidata_p627_values(entity_numeric_id, source, value) VALUES (@id, @source, @value)";
        var idParam = command.Parameters.Add("@id", SqliteType.Integer);
        var sourceParam = command.Parameters.Add("@source", SqliteType.Text);
        var valueParam = command.Parameters.Add("@value", SqliteType.Text);
        idParam.Value = record.NumericId;

        var claimValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in record.P627Claims) {
            if (!claimValues.Add(value)) {
                continue;
            }

            sourceParam.Value = "claim";
            valueParam.Value = value;
            command.ExecuteNonQuery();
        }

        var referenceValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in record.P627References) {
            if (!referenceValues.Add(value)) {
                continue;
            }

            sourceParam.Value = "reference";
            valueParam.Value = value;
            command.ExecuteNonQuery();
        }
    }

    private P141InsertResult InsertP141Statements(WikidataEntityRecord record, SqliteTransaction tx) {
        using var statementCommand = _connection.CreateCommand();
        statementCommand.Transaction = tx;
        statementCommand.CommandText = "INSERT INTO wikidata_p141_statements(entity_numeric_id, statement_id, status_qid, status_entity_id, rank) VALUES (@id,@statement,@status,@statusId,@rank)";
        var idParam = statementCommand.Parameters.Add("@id", SqliteType.Integer);
        var statementParam = statementCommand.Parameters.Add("@statement", SqliteType.Text);
        var statusParam = statementCommand.Parameters.Add("@status", SqliteType.Integer);
        var statusIdParam = statementCommand.Parameters.Add("@statusId", SqliteType.Text);
        var rankParam = statementCommand.Parameters.Add("@rank", SqliteType.Text);
        idParam.Value = record.NumericId;

        using var referenceCommand = _connection.CreateCommand();
        referenceCommand.Transaction = tx;
        referenceCommand.CommandText = "INSERT INTO wikidata_p141_references(entity_numeric_id, statement_id, reference_hash, source_qid, iucn_taxon_id) VALUES (@id,@statement,@hash,@source,@iucn)";
        var refIdParam = referenceCommand.Parameters.Add("@id", SqliteType.Integer);
        var refStatementParam = referenceCommand.Parameters.Add("@statement", SqliteType.Text);
        var hashParam = referenceCommand.Parameters.Add("@hash", SqliteType.Text);
        var sourceParam = referenceCommand.Parameters.Add("@source", SqliteType.Integer);
        var iucnParam = referenceCommand.Parameters.Add("@iucn", SqliteType.Text);
        refIdParam.Value = record.NumericId;

        long statementCount = 0;
        long referenceCount = 0;
        foreach (var statement in record.P141Statements) {
            statementParam.Value = statement.StatementId;
            statusParam.Value = statement.StatusNumericId;
            statusIdParam.Value = statement.StatusEntityId;
            rankParam.Value = statement.Rank;
            statementCommand.ExecuteNonQuery();
            statementCount++;

            foreach (var reference in statement.References) {
                if (reference.IucnTaxonIds.Count == 0) {
                    continue;
                }

                refStatementParam.Value = statement.StatementId;
                hashParam.Value = reference.ReferenceHash;
                sourceParam.Value = reference.SourceQid.HasValue ? reference.SourceQid.Value : DBNull.Value;
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var value in reference.IucnTaxonIds) {
                    if (!seen.Add(value)) {
                        continue;
                    }

                    iucnParam.Value = value;
                    referenceCommand.ExecuteNonQuery();
                    referenceCount++;
                }
            }
        }

        return new P141InsertResult(statementCount, referenceCount);
    }

    private void ClearP141Entries(long numericId, SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = @"DELETE FROM wikidata_p141_references WHERE entity_numeric_id=@id;
DELETE FROM wikidata_p141_statements WHERE entity_numeric_id=@id;";
        command.Parameters.AddWithValue("@id", numericId);
        command.ExecuteNonQuery();
    }

    private void ClearAllP141Entries(SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "DELETE FROM wikidata_p141_references;DELETE FROM wikidata_p141_statements;";
        command.ExecuteNonQuery();
    }

    private void InsertScientificNames(WikidataEntityRecord record, SqliteTransaction tx) {
        if (record.ScientificNames.Count == 0) {
            return;
        }

        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "INSERT INTO wikidata_scientific_names(entity_numeric_id, language, name) VALUES (@id,@lang,@name)";
        var idParam = command.Parameters.Add("@id", SqliteType.Integer);
        var langParam = command.Parameters.Add("@lang", SqliteType.Text);
        var nameParam = command.Parameters.Add("@name", SqliteType.Text);
        idParam.Value = record.NumericId;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in record.ScientificNames) {
            if (!seen.Add(name.Language)) {
                continue;
            }

            langParam.Value = name.Language;
            nameParam.Value = name.Value;
            command.ExecuteNonQuery();
        }
    }

    public long RebuildTaxonNameIndex(bool forceRebuild = false, CancellationToken cancellationToken = default) {
        var sourceCount = CountDistinctScientificNameEntities();
        if (sourceCount == 0) {
            BackfillScientificNamesFromEntities(cancellationToken);
            sourceCount = CountDistinctScientificNameEntities();
            if (sourceCount == 0) {
                if (forceRebuild) {
                    ClearTaxonNameIndex();
                }

                return 0;
            }
        }

        if (forceRebuild) {
            ClearTaxonNameIndex();
        }

        var existingCount = CountDistinctTaxonNameIndexEntities();
        if (!forceRebuild && existingCount >= sourceCount) {
            return 0;
        }

        using var select = _connection.CreateCommand();
        select.CommandText = "SELECT entity_numeric_id, name FROM wikidata_scientific_names";
        using var reader = select.ExecuteReader(CommandBehavior.SequentialAccess);
        using var tx = _connection.BeginTransaction();
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = "INSERT OR IGNORE INTO wikidata_taxon_name_index(entity_numeric_id, normalized_name) VALUES (@id,@name)";
        var idParam = insert.Parameters.Add("@id", SqliteType.Integer);
        var nameParam = insert.Parameters.Add("@name", SqliteType.Text);

        long inserted = 0;
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            var normalized = ScientificNameHelper.Normalize(reader.IsDBNull(1) ? null : reader.GetString(1));
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            idParam.Value = reader.GetInt64(0);
            nameParam.Value = normalized;
            if (insert.ExecuteNonQuery() == 1) {
                inserted++;
            }
        }

        tx.Commit();
        return inserted;
    }

    public P141RebuildResult RebuildP141Tables(bool forceRebuild = false, CancellationToken cancellationToken = default) {
        var existingStatements = GetTableRowCount("wikidata_p141_statements");
        if (!forceRebuild && existingStatements > 0) {
            return new P141RebuildResult(0, 0, 0, 0, WasSkipped: true);
        }

        using var select = _connection.CreateCommand();
        select.CommandText = "SELECT entity_numeric_id, json FROM wikidata_entities WHERE json IS NOT NULL AND TRIM(json) <> ''";
        using var reader = select.ExecuteReader(CommandBehavior.SequentialAccess);
        using var tx = _connection.BeginTransaction();
        if (forceRebuild) {
            ClearAllP141Entries(tx);
        }

        long processedEntities = 0;
        long statementsInserted = 0;
        long referencesInserted = 0;
        long jsonFailures = 0;

        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.IsDBNull(1)) {
                continue;
            }

            var entityId = reader.GetInt64(0);
            var json = reader.GetString(1);
            if (string.IsNullOrWhiteSpace(json)) {
                continue;
            }

            WikidataEntityRecord record;
            try {
                record = WikidataEntityParser.Parse(json);
            }
            catch (Exception) {
                jsonFailures++;
                continue;
            }

            if (record.P141Statements.Count == 0) {
                continue;
            }

            processedEntities++;
            ClearP141Entries(entityId, tx);
            var insertResult = InsertP141Statements(record, tx);
            statementsInserted += insertResult.StatementCount;
            referencesInserted += insertResult.ReferenceCount;
        }

        tx.Commit();
        return new P141RebuildResult(processedEntities, statementsInserted, referencesInserted, jsonFailures, WasSkipped: false);
    }

    private long BackfillScientificNamesFromEntities(CancellationToken cancellationToken) {
        using var select = _connection.CreateCommand();
        select.CommandText = "SELECT entity_numeric_id, json FROM wikidata_entities WHERE json IS NOT NULL AND TRIM(json) <> ''";
        using var reader = select.ExecuteReader(CommandBehavior.SequentialAccess);
        using var tx = _connection.BeginTransaction();
        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = "INSERT OR IGNORE INTO wikidata_scientific_names(entity_numeric_id, language, name) VALUES (@id,@lang,@name)";
        var idParam = insert.Parameters.Add("@id", SqliteType.Integer);
        var langParam = insert.Parameters.Add("@lang", SqliteType.Text);
        var nameParam = insert.Parameters.Add("@name", SqliteType.Text);

        long entityCount = 0;
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.IsDBNull(1)) {
                continue;
            }

            var json = reader.GetString(1);
            if (string.IsNullOrWhiteSpace(json)) {
                continue;
            }

            var record = WikidataEntityParser.Parse(json);
            if (record.ScientificNames.Count == 0) {
                continue;
            }

            idParam.Value = reader.GetInt64(0);
            var insertedForEntity = false;
            foreach (var name in record.ScientificNames) {
                langParam.Value = name.Language;
                nameParam.Value = name.Value;
                if (insert.ExecuteNonQuery() == 1) {
                    insertedForEntity = true;
                }
            }

            if (insertedForEntity) {
                entityCount++;
            }
        }

        tx.Commit();
        return entityCount;
    }

    private void InsertTaxonNameIndex(WikidataEntityRecord record, SqliteTransaction tx) {
        if (record.ScientificNames.Count == 0) {
            return;
        }

        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "INSERT OR IGNORE INTO wikidata_taxon_name_index(entity_numeric_id, normalized_name) VALUES (@id,@name)";
        var idParam = command.Parameters.Add("@id", SqliteType.Integer);
        var nameParam = command.Parameters.Add("@name", SqliteType.Text);
        idParam.Value = record.NumericId;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in record.ScientificNames) {
            var normalized = ScientificNameHelper.Normalize(name.Value);
            if (string.IsNullOrWhiteSpace(normalized) || !seen.Add(normalized)) {
                continue;
            }

            nameParam.Value = normalized;
            command.ExecuteNonQuery();
        }
    }

    private void InsertRank(WikidataEntityRecord record, SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "DELETE FROM wikidata_taxon_rank WHERE entity_numeric_id=@id";
        command.Parameters.AddWithValue("@id", record.NumericId);
        command.ExecuteNonQuery();

        if (record.RankQid is null) {
            return;
        }

        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = "INSERT INTO wikidata_taxon_rank(entity_numeric_id, rank_qid) VALUES (@id,@rank)";
        insert.Parameters.AddWithValue("@id", record.NumericId);
        insert.Parameters.AddWithValue("@rank", record.RankQid.Value);
        insert.ExecuteNonQuery();
    }

    private void InsertParentTaxa(WikidataEntityRecord record, SqliteTransaction tx) {
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = "DELETE FROM wikidata_parent_taxa WHERE entity_numeric_id=@id";
        command.Parameters.AddWithValue("@id", record.NumericId);
        command.ExecuteNonQuery();

        if (record.ParentTaxaQids.Count == 0) {
            return;
        }

        using var insert = _connection.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = "INSERT INTO wikidata_parent_taxa(entity_numeric_id, parent_qid) VALUES (@id,@parent)";
        var idParam = insert.Parameters.Add("@id", SqliteType.Integer);
        var parentParam = insert.Parameters.Add("@parent", SqliteType.Integer);
        idParam.Value = record.NumericId;

        var seen = new HashSet<long>();
        foreach (var parent in record.ParentTaxaQids) {
            if (!seen.Add(parent)) {
                continue;
            }

            parentParam.Value = parent;
            insert.ExecuteNonQuery();
        }
    }

    private void BackfillTaxonNameIndex() => RebuildTaxonNameIndex(false, CancellationToken.None);

    private long CountDistinctScientificNameEntities() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(DISTINCT entity_numeric_id) FROM wikidata_scientific_names";
        var result = command.ExecuteScalar();
        return Convert.ToInt64(result ?? 0L);
    }

    private long CountDistinctTaxonNameIndexEntities() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(DISTINCT entity_numeric_id) FROM wikidata_taxon_name_index";
        var result = command.ExecuteScalar();
        return Convert.ToInt64(result ?? 0L);
    }

    private void ClearTaxonNameIndex() {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM wikidata_taxon_name_index";
        command.ExecuteNonQuery();
    }

    private long GetTableRowCount(string tableName) {
        using var command = _connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {tableName}";
        var result = command.ExecuteScalar();
        return Convert.ToInt64(result ?? 0L);
    }
}

internal sealed record SeedUpsertResult(int NewCount, int UpdatedCount);

internal sealed record WikidataEntityWorkItem(long NumericId, string EntityId, DateTime? DownloadedAt, int AttemptCount);

internal sealed record WikidataPendingIucnMatchRow(
    string IucnTaxonId,
    long NumericId,
    string EntityId,
    string? MatchedName,
    string MatchMethod,
    bool IsSynonym,
    DateTime DiscoveredAt,
    DateTime LastSeenAt);

internal sealed record P141InsertResult(long StatementCount, long ReferenceCount);

internal sealed record P141RebuildResult(
    long EntitiesProcessed,
    long StatementsInserted,
    long ReferencesInserted,
    long JsonFailures,
    bool WasSkipped);

internal sealed record WikidataEnwikiSitelink(long EntityNumericId, string EntityId, string Title);
