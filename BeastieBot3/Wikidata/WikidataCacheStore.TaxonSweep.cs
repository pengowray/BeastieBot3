using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

// wikidata_taxon_sweep: one short row per Wikidata item with a taxon name (P225), written by
// `wikidata sweep-taxa`. Unlike wikidata_entities it keeps no JSON, so all 4 million taxon items fit
// in a few hundred MB. site build-db reads it for species that are in Wikidata but not in IUCN.
//
// A pass reads every item from Q1 up, a page at a time; the cursor (taxon_sweep_cursor in
// wikidata_sync_state) is the last item stored. Each row stores when a pass last saw it (seen_at).
// When a pass reaches the end, rows it did not see (items deleted, merged or no longer with a taxon
// name) are deleted, the finish time is recorded and the cursor goes back to 0.

namespace BeastieBot3.Wikidata;

internal sealed record WikidataTaxonSweepState(
    long Cursor,
    DateTime? PassStartedUtc,
    DateTime? LastCompletedUtc,
    long Rows,
    long RowsSeenThisPass,
    long Species,
    long? LastTotal);

internal sealed partial class WikidataCacheStore {
    internal const string TaxonSweepCursorKey = "taxon_sweep_cursor";
    internal const string TaxonSweepStartedKey = "taxon_sweep_pass_started";
    internal const string TaxonSweepCompletedKey = "taxon_sweep_completed";
    internal const string TaxonSweepTotalKey = "taxon_sweep_total";

    private void EnsureTaxonSweepSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS wikidata_taxon_sweep (
                qid            INTEGER PRIMARY KEY,   -- numeric part of the Q-number
                taxon_name     TEXT NOT NULL,         -- taxon name (P225); the first in ordinal order when there are several
                other_names    TEXT,                  -- the item's other taxon names, '|' between them
                rank_qid       INTEGER,               -- taxon rank (P105), numeric: 7432 species, 34740 genus
                parent_qids    TEXT,                  -- parent taxon (P171) Q-numbers, numeric, space between them
                col_ids        TEXT,                  -- Catalogue of Life ID (P10585) values, space between them
                iucn_taxon_ids TEXT,                  -- IUCN taxon ID (P627) values, space between them
                enwiki_title   TEXT,                  -- English Wikipedia article (sitelink)
                label_en       TEXT,                  -- English label, only when it is not one of the taxon names
                instance_of    TEXT,                  -- instance of (P31) values other than taxon (Q16521), numeric, space between them:
                                                      -- 23038290 fossil taxon, 1040689 synonym, 98961713 extinct taxon ...
                synonym_of     TEXT,                  -- the items that name this one as taxon synonym (P1420), numeric, space between them
                seen_at        TEXT NOT NULL          -- UTC "O": when a sweep pass last read the item
            );
            CREATE INDEX IF NOT EXISTS idx_wikidata_taxon_sweep_rank ON wikidata_taxon_sweep(rank_qid);
            """;
        command.ExecuteNonQuery();
        // instance_of and synonym_of were added after the first sweeps.
        foreach (var column in new[] { "instance_of", "synonym_of" }) {
            using var columns = _connection.CreateCommand();
            columns.CommandText = "SELECT COUNT(*) FROM pragma_table_info('wikidata_taxon_sweep') WHERE name = @name";
            columns.Parameters.AddWithValue("@name", column);
            if (Convert.ToInt64(columns.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
                using var alter = _connection.CreateCommand();
                alter.CommandText = $"ALTER TABLE wikidata_taxon_sweep ADD COLUMN {column} TEXT";
                alter.ExecuteNonQuery();
            }
        }
    }

    public string? GetSyncText(string key) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT value FROM wikidata_sync_state WHERE key=@key LIMIT 1";
        command.Parameters.AddWithValue("@key", key);
        return command.ExecuteScalar() as string;
    }

    public void SetSyncText(string key, string? value) {
        using var command = _connection.CreateCommand();
        if (value is null) {
            command.CommandText = "DELETE FROM wikidata_sync_state WHERE key=@key";
        } else {
            command.CommandText = @"INSERT INTO wikidata_sync_state(key, value) VALUES (@key, @value)
ON CONFLICT(key) DO UPDATE SET value=excluded.value";
            command.Parameters.AddWithValue("@value", value);
        }
        command.Parameters.AddWithValue("@key", key);
        command.ExecuteNonQuery();
    }

    /// Stores one page and moves the cursor to its last item, in one transaction, so a stopped run
    /// never leaves the cursor ahead of the rows.
    public void StoreTaxonSweepPage(IReadOnlyList<WikidataSweptTaxon> items, DateTime seenAtUtc) {
        if (items.Count == 0) {
            return;
        }
        using var tx = _connection.BeginTransaction();
        using var command = _connection.CreateCommand();
        command.Transaction = tx;
        command.CommandText = """
            INSERT INTO wikidata_taxon_sweep(qid, taxon_name, other_names, rank_qid, parent_qids, col_ids, iucn_taxon_ids, enwiki_title, label_en, instance_of, synonym_of, seen_at)
            VALUES (@qid, @name, @other, @rank, @parents, @col, @iucn, @enwiki, @label, @instance, @synonym_of, @seen)
            ON CONFLICT(qid) DO UPDATE SET taxon_name=excluded.taxon_name, other_names=excluded.other_names,
                rank_qid=excluded.rank_qid, parent_qids=excluded.parent_qids, col_ids=excluded.col_ids,
                iucn_taxon_ids=excluded.iucn_taxon_ids, enwiki_title=excluded.enwiki_title,
                label_en=excluded.label_en, instance_of=excluded.instance_of, synonym_of=excluded.synonym_of, seen_at=excluded.seen_at
            """;
        var qid = command.Parameters.Add("@qid", SqliteType.Integer);
        var name = command.Parameters.Add("@name", SqliteType.Text);
        var other = command.Parameters.Add("@other", SqliteType.Text);
        var rank = command.Parameters.Add("@rank", SqliteType.Integer);
        var parents = command.Parameters.Add("@parents", SqliteType.Text);
        var col = command.Parameters.Add("@col", SqliteType.Text);
        var iucn = command.Parameters.Add("@iucn", SqliteType.Text);
        var enwiki = command.Parameters.Add("@enwiki", SqliteType.Text);
        var label = command.Parameters.Add("@label", SqliteType.Text);
        var instance = command.Parameters.Add("@instance", SqliteType.Text);
        var synonymOf = command.Parameters.Add("@synonym_of", SqliteType.Text);
        command.Parameters.AddWithValue("@seen", seenAtUtc.ToString("O", CultureInfo.InvariantCulture));
        foreach (var item in items) {
            qid.Value = item.Qid;
            name.Value = item.TaxonName;
            other.Value = Joined(item.OtherNames, "|");
            rank.Value = item.RankQid is { } r ? r : DBNull.Value;
            parents.Value = item.ParentQids.Count == 0 ? DBNull.Value : string.Join(' ', item.ParentQids);
            col.Value = Joined(item.ColIds, " ");
            iucn.Value = Joined(item.IucnTaxonIds, " ");
            enwiki.Value = (object?)item.EnwikiTitle ?? DBNull.Value;
            label.Value = (object?)item.LabelEn ?? DBNull.Value;
            instance.Value = item.InstanceOf.Count == 0 ? DBNull.Value : string.Join(' ', item.InstanceOf);
            synonymOf.Value = item.SynonymOf.Count == 0 ? DBNull.Value : string.Join(' ', item.SynonymOf);
            command.ExecuteNonQuery();
        }
        using (var cursor = _connection.CreateCommand()) {
            cursor.Transaction = tx;
            cursor.CommandText = @"INSERT INTO wikidata_sync_state(key, value) VALUES (@key, @value)
ON CONFLICT(key) DO UPDATE SET value=excluded.value";
            cursor.Parameters.AddWithValue("@key", TaxonSweepCursorKey);
            cursor.Parameters.AddWithValue("@value", items[^1].Qid.ToString(CultureInfo.InvariantCulture));
            cursor.ExecuteNonQuery();
        }
        tx.Commit();
    }

    /// Ends a pass that reached the last item: deletes the rows the pass did not see, records the
    /// finish time and puts the cursor back to 0. Returns how many rows it deleted.
    public int CompleteTaxonSweepPass(DateTime passStartedUtc, DateTime finishedUtc) {
        using var tx = _connection.BeginTransaction();
        int deleted;
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM wikidata_taxon_sweep WHERE seen_at < @started";
            delete.Parameters.AddWithValue("@started", passStartedUtc.ToString("O", CultureInfo.InvariantCulture));
            deleted = delete.ExecuteNonQuery();
        }
        foreach (var (key, value) in new (string, string?)[] {
                     (TaxonSweepCursorKey, "0"),
                     (TaxonSweepStartedKey, null),
                     (TaxonSweepCompletedKey, finishedUtc.ToString("O", CultureInfo.InvariantCulture)),
                 }) {
            using var command = _connection.CreateCommand();
            command.Transaction = tx;
            if (value is null) {
                command.CommandText = "DELETE FROM wikidata_sync_state WHERE key=@key";
            } else {
                command.CommandText = @"INSERT INTO wikidata_sync_state(key, value) VALUES (@key, @value)
ON CONFLICT(key) DO UPDATE SET value=excluded.value";
                command.Parameters.AddWithValue("@value", value);
            }
            command.Parameters.AddWithValue("@key", key);
            command.ExecuteNonQuery();
        }
        tx.Commit();
        return deleted;
    }

    public WikidataTaxonSweepState GetTaxonSweepState() {
        var started = BeastieBot3.Infrastructure.StoredUtc.Parse(GetSyncText(TaxonSweepStartedKey));
        long Count(string sql, DateTime? since = null) {
            using var command = _connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = 0;
            if (since is { } s) {
                command.Parameters.AddWithValue("@since", s.ToString("O", CultureInfo.InvariantCulture));
            }
            return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        }
        return new WikidataTaxonSweepState(
            GetSyncCursor(TaxonSweepCursorKey),
            started,
            BeastieBot3.Infrastructure.StoredUtc.Parse(GetSyncText(TaxonSweepCompletedKey)),
            Count("SELECT COUNT(*) FROM wikidata_taxon_sweep"),
            started is { } passStart ? Count("SELECT COUNT(*) FROM wikidata_taxon_sweep WHERE seen_at >= @since", passStart) : 0,
            Count($"SELECT COUNT(*) FROM wikidata_taxon_sweep WHERE rank_qid = {WikidataTaxonSweep.SpeciesRank}"),
            long.TryParse(GetSyncText(TaxonSweepTotalKey), out var total) ? total : null);
    }

    private static object Joined(IReadOnlyList<string> values, string separator) =>
        values.Count == 0 ? DBNull.Value : string.Join(separator, values);
}
