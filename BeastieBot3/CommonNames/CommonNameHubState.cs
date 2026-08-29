using System;
using System.IO;
using BeastieBot3.Configuration;
using Microsoft.Data.Sqlite;

// What the common-name hub looks like on disk right now, for the workflow page's lights.
//
// The one question a step's run history cannot answer: the ambiguous-name list is derived from
// the names in the hub, so it goes stale the moment names are aggregated again â€” and nothing
// downstream notices. A list built before the last aggregate silently un-flags names that have
// become ambiguous since, and Wikipedia list generation then uses them as if they were unique.
//
// Read-only and offline: opened read-only, no schema work, and every query is O(log n) or over
// the tiny bookkeeping tables, because this is polled every ten seconds.

namespace BeastieBot3.CommonNames;

public sealed record CommonNameHubState {
    public string? HubPath { get; init; }
    public bool HubExists { get; init; }
    public bool Readable { get; init; }
    public long ConflictCount { get; init; }
    /// When the ambiguous-name list was last built, from the detect-conflicts run record, or
    /// failing that the newest conflict row (lists built before runs were recorded).
    public DateTime? ConflictsBuiltAt { get; init; }
    /// When names in the hub last changed: the newest finished aggregate/init import, or the
    /// newest source purge (`aggregate --source X --replace`), whichever is later.
    public DateTime? NamesChangedAt { get; init; }

    public string FileName => HubPath is null ? "the common-name hub" : Path.GetFileName(HubPath);
}

public static class CommonNameHubStateReader {
    public static CommonNameHubState Read(PathsService paths) {
        string? hubPath = null;
        try { hubPath = paths.ResolveCommonNameStorePath(null); } catch { /* unset â€” reported as no hub */ }

        var full = string.IsNullOrWhiteSpace(hubPath) ? null : Path.GetFullPath(hubPath);
        var state = new CommonNameHubState {
            HubPath = full,
            HubExists = full is not null && File.Exists(full),
        };
        if (!state.HubExists) return state;

        try {
            var csb = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();

            return state with {
                Readable = true,
                ConflictCount = Scalar(conn, "SELECT COUNT(*) FROM common_name_conflicts") is long c ? c : 0,
                ConflictsBuiltAt = ReadConflictsBuiltAt(conn),
                NamesChangedAt = Later(
                    Stamp(conn, """
                        SELECT MAX(ended_at) FROM import_runs
                        WHERE status = 'completed'
                          AND (import_type LIKE 'common_names_%' OR import_type LIKE 'synonyms_%' OR import_type LIKE 'taxa_%')
                        """),
                    Stamp(conn, "SELECT MAX(replaced_at) FROM source_replacements")),
            };
        } catch {
            // An unreadable or older hub leaves the step on its usual status rather than
            // asserting anything about it.
            return state;
        }
    }

    // Prefer the recorded run. Falling back to the highest-id conflict row keeps a hub built
    // before detect-conflicts recorded its runs from reporting "never built" forever; ids only
    // ever increase, so the newest row is the last thing the detection pass wrote.
    private static DateTime? ReadConflictsBuiltAt(SqliteConnection conn) =>
        Stamp(conn, "SELECT MAX(ended_at) FROM import_runs WHERE import_type = 'detect_conflicts' AND status = 'completed'")
        ?? Stamp(conn, "SELECT detected_at FROM common_name_conflicts ORDER BY id DESC LIMIT 1");

    private static DateTime? Later(DateTime? a, DateTime? b) =>
        a is null ? b : b is null ? a : (a > b ? a : b);

    private static object? Scalar(SqliteConnection conn, string sql) {
        try {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 5;
            var value = cmd.ExecuteScalar();
            return value is DBNull ? null : value;
        } catch {
            return null;
        }
    }

    private static DateTime? Stamp(SqliteConnection conn, string sql) =>
        Scalar(conn, sql) is string s ? ParseStoredUtc(s) : null;

    // Timestamps are written as UTC "O" strings. Plain DateTime.Parse turns the trailing Z into
    // local time, which would shift every comparison here by the machine's offset.
    internal static DateTime? ParseStoredUtc(string? value) {
        if (!DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind, out var parsed)) {
            return null;
        }
        return parsed.Kind switch {
            DateTimeKind.Utc => parsed,
            DateTimeKind.Local => parsed.ToUniversalTime(),
            _ => DateTime.SpecifyKind(parsed, DateTimeKind.Utc),
        };
    }
}
