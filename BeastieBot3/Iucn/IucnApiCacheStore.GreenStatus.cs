using System;
using System.Collections.Generic;
using System.Globalization;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The IUCN Green Status of Species assessments in the API cache, written by `iucn api green-status`
// and read by `site build-db`. The site build reads this table's columns, so the DDL is a contract
// with it: change both together.
//
// Each run stores the whole list the API gives (SaveGreenStatus): rows already there keep when they
// were first stored, the Red List version of that time and their baseline flag; rows the download
// does not have are deleted. A row's first_seen_version says which release a Green Status appeared
// in, except for the rows of the first download (baseline = 1), whose release is not known.

namespace BeastieBot3.Iucn;

internal sealed partial class IucnApiCacheStore {
    internal const string GreenStatusDdl = """
        CREATE TABLE IF NOT EXISTS green_status (
            sis_id                 INTEGER NOT NULL,   -- the taxon's SIS id (taxon.sis_id)
            assessment_date        TEXT NOT NULL,      -- yyyy-MM-dd, the Green Status assessment's date
            red_list_assessment_id INTEGER,            -- the assessment id at the end of url: the Red List page the Green Status is shown on
            json                   TEXT NOT NULL,      -- the API's record as given (justification included; the site build leaves it out)
            first_seen_at          TEXT NOT NULL,      -- UTC "O": when the record was first stored
            first_seen_version     TEXT,               -- the Red List version (/api/v4/information/red_list_version) when the record was first stored
            baseline               INTEGER NOT NULL,   -- 1 when the record was in the first download, so the release it was published in is not known
            last_seen_at           TEXT NOT NULL,      -- UTC "O": the last download that had it
            PRIMARY KEY (sis_id, assessment_date)
        ) WITHOUT ROWID;
        """;

    private void EnsureGreenStatusSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = GreenStatusDdl;
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Stores one full download in one transaction: upserts every record by (sis_id,
    /// assessment_date), then deletes the stored rows the download does not have. A new row gets
    /// first_seen_at = <paramref name="nowUtc"/>, first_seen_version = <paramref name="redListVersion"/>,
    /// and baseline = 1 only when the table was empty before this call. Refuses an empty download.
    /// </summary>
    public GreenStatusSaveResult SaveGreenStatus(IReadOnlyList<IucnGreenStatusRecord> records, string redListVersion, DateTime nowUtc) {
        if (records.Count == 0) {
            throw new ArgumentException("An empty Green Status download would delete every stored row.", nameof(records));
        }
        var now = nowUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

        using var tx = _connection.BeginTransaction();

        var stored = new Dictionary<(long, string), (string Json, string? Name)>();
        using (var read = _connection.CreateCommand()) {
            read.Transaction = tx;
            read.CommandText = "SELECT sis_id, assessment_date, json, json_extract(json, '$.taxon.scientific_name') FROM green_status";
            using var reader = read.ExecuteReader();
            while (reader.Read()) {
                stored[(reader.GetInt64(0), reader.GetString(1))] =
                    (reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetValue(3) as string);
            }
        }
        var firstDownload = stored.Count == 0;

        var added = new List<GreenStatusRowInfo>();
        var changed = 0;
        var seen = new HashSet<(long, string)>();
        using (var upsert = _connection.CreateCommand()) {
            upsert.Transaction = tx;
            upsert.CommandText = """
                INSERT INTO green_status (sis_id, assessment_date, red_list_assessment_id, json,
                                          first_seen_at, first_seen_version, baseline, last_seen_at)
                VALUES (@sis, @date, @rlId, @json, @now, @version, @baseline, @now)
                ON CONFLICT (sis_id, assessment_date) DO UPDATE SET
                    red_list_assessment_id = excluded.red_list_assessment_id,
                    json = excluded.json,
                    last_seen_at = excluded.last_seen_at
                """;
            var sis = upsert.Parameters.Add("@sis", SqliteType.Integer);
            var date = upsert.Parameters.Add("@date", SqliteType.Text);
            var rlId = upsert.Parameters.Add("@rlId", SqliteType.Integer);
            var json = upsert.Parameters.Add("@json", SqliteType.Text);
            upsert.Parameters.AddWithValue("@now", now);
            upsert.Parameters.AddWithValue("@version", redListVersion);
            upsert.Parameters.AddWithValue("@baseline", firstDownload ? 1 : 0);

            foreach (var record in records) {
                var key = (record.SisId, record.AssessmentDate);
                if (!seen.Add(key)) continue;
                if (stored.TryGetValue(key, out var old)) {
                    if (!string.Equals(old.Json, record.Json, StringComparison.Ordinal)) changed++;
                } else {
                    added.Add(new GreenStatusRowInfo(record.SisId, record.AssessmentDate, record.ScientificName));
                }
                sis.Value = record.SisId;
                date.Value = record.AssessmentDate;
                rlId.Value = record.RedListAssessmentId is { } id ? id : DBNull.Value;
                json.Value = record.Json;
                upsert.ExecuteNonQuery();
            }
        }

        var removed = new List<GreenStatusRowInfo>();
        using (var delete = _connection.CreateCommand()) {
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM green_status WHERE sis_id = @sis AND assessment_date = @date";
            var sis = delete.Parameters.Add("@sis", SqliteType.Integer);
            var date = delete.Parameters.Add("@date", SqliteType.Text);
            foreach (var (key, row) in stored) {
                if (seen.Contains(key)) continue;
                sis.Value = key.Item1;
                date.Value = key.Item2;
                delete.ExecuteNonQuery();
                removed.Add(new GreenStatusRowInfo(key.Item1, key.Item2, row.Name));
            }
        }

        tx.Commit();
        return new GreenStatusSaveResult(firstDownload, seen.Count, added, changed, removed);
    }

    /// <summary>
    /// What the table holds, for `iucn api green-status --status`. Null when the table is empty or
    /// missing (a cache opened with OpenReadOnly that no newer command has opened for writing).
    /// </summary>
    public GreenStatusSummary? GetGreenStatusSummary() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'green_status'";
        if (Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) return null;

        command.CommandText = """
            SELECT COUNT(*), COALESCE(SUM(baseline), 0), MIN(first_seen_at), MAX(first_seen_at), MAX(last_seen_at)
            FROM green_status
            """;
        long rows, baselineRows;
        DateTime? firstSeenMin, firstSeenMax, lastSeenMax;
        using (var reader = command.ExecuteReader()) {
            reader.Read();
            rows = reader.GetInt64(0);
            if (rows == 0) return null;
            baselineRows = reader.GetInt64(1);
            firstSeenMin = StoredUtc.Parse(reader.IsDBNull(2) ? null : reader.GetString(2));
            firstSeenMax = StoredUtc.Parse(reader.IsDBNull(3) ? null : reader.GetString(3));
            lastSeenMax = StoredUtc.Parse(reader.IsDBNull(4) ? null : reader.GetString(4));
        }

        var versions = new List<GreenStatusVersionCount>();
        command.CommandText = """
            SELECT first_seen_version, baseline, COUNT(*) FROM green_status
            GROUP BY first_seen_version, baseline ORDER BY MIN(first_seen_at), baseline DESC
            """;
        using (var reader = command.ExecuteReader()) {
            while (reader.Read()) {
                versions.Add(new GreenStatusVersionCount(reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetInt64(1) == 1, reader.GetInt64(2)));
            }
        }

        return new GreenStatusSummary(rows, baselineRows, firstSeenMin, firstSeenMax, lastSeenMax, versions,
            Counts("SELECT json_extract(json, '$.species_recovery_category') AS c, COUNT(*) FROM green_status GROUP BY c ORDER BY COUNT(*) DESC, c"));
    }

    private IReadOnlyList<GreenStatusCount> Counts(string sql) {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        var counts = new List<GreenStatusCount>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            counts.Add(new GreenStatusCount(reader.IsDBNull(0) ? null : Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetInt64(1)));
        }
        return counts;
    }
}

/// <summary>A stored Green Status assessment, named for the console.</summary>
internal sealed record GreenStatusRowInfo(long SisId, string AssessmentDate, string? ScientificName);

/// <summary>
/// What one SaveGreenStatus call did. Stored is the number of rows the download has (each key
/// once); Changed counts rows already stored whose JSON differs from the download's.
/// </summary>
internal sealed record GreenStatusSaveResult(
    bool FirstDownload,
    int Stored,
    IReadOnlyList<GreenStatusRowInfo> Added,
    int Changed,
    IReadOnlyList<GreenStatusRowInfo> Removed);

internal sealed record GreenStatusCount(string? Value, long Rows);

/// <summary>Stored rows by the Red List version of their first download; Baseline rows came in the first download.</summary>
internal sealed record GreenStatusVersionCount(string? Version, bool Baseline, long Rows);

internal sealed record GreenStatusSummary(
    long Rows,
    long BaselineRows,
    DateTime? FirstSeenMin,
    DateTime? FirstSeenMax,
    DateTime? LastSeenMax,
    IReadOnlyList<GreenStatusVersionCount> ByFirstSeenVersion,
    IReadOnlyList<GreenStatusCount> ByCategory);
