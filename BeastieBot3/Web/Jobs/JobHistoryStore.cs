using System.Text.Json;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;

namespace BeastieBot3.Web.Jobs;

// SQLite-backed persistence for the web job runner so the "Recent jobs" list
// (and each job's captured output) survives server restarts.
//
// Schema: one row per job. Output is stored as TEXT and capped at 256 KB so a
// single runaway import doesn't bloat the DB. Past that, we append a
// "[output truncated]" marker.
//
// Lifecycle matches every other store in the project: private ctor +
// static Open() factory + IDisposable. WAL mode so dashboard reads do not
// block job inserts.

// Web-layer type, kept public for the minimal-API DI graph (JobRegistry/FlowEvaluator), so it
// reuses SqliteStore.OpenConnection as a helper rather than inheriting the internal base.
public sealed class JobHistoryStore : IDisposable {
    private const int MaxStoredOutputBytes = 256 * 1024;
    private const string TruncationMarker = "\n\x1b[2m[output truncated; persisted up to 256 KB]\x1b[0m\n";

    private readonly SqliteConnection _connection;
    private readonly object _writeLock = new();

    private JobHistoryStore(SqliteConnection connection) { _connection = connection; }

    public static JobHistoryStore Open(string databasePath) {
        var connection = SqliteStore.OpenConnection(databasePath);
        var store = new JobHistoryStore(connection);
        store.EnsureSchema();
        store.MarkOrphanedRunningJobs();
        return store;
    }

    public void Dispose() => _connection.Dispose();

    private void EnsureSchema() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                args_json TEXT NOT NULL,
                status TEXT NOT NULL,
                exit_code INTEGER,
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                output TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS ix_jobs_created_at ON jobs (created_at DESC);
            """;
        cmd.ExecuteNonQuery();
    }

    // Any job that was Running when the server stopped is orphaned — its
    // process never reached the completion path. Surface that explicitly so
    // the UI doesn't show it as still in-flight.
    private void MarkOrphanedRunningJobs() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            UPDATE jobs
            SET status = 'failed',
                error = COALESCE(error, '') || '[interrupted by server restart]',
                completed_at = COALESCE(completed_at, @now)
            WHERE status IN ('pending', 'running');
            """;
        cmd.Parameters.AddWithValue("@now", DateTimeOffset.UtcNow.ToString("O"));
        cmd.ExecuteNonQuery();
    }

    public void Insert(Job job) {
        lock (_writeLock) {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO jobs (id, command, args_json, status, created_at)
                VALUES (@id, @command, @args, @status, @created_at);
                """;
            cmd.Parameters.AddWithValue("@id", job.Id);
            cmd.Parameters.AddWithValue("@command", job.Command);
            cmd.Parameters.AddWithValue("@args", JsonSerializer.Serialize(job.Args));
            cmd.Parameters.AddWithValue("@status", StatusText(job.Status));
            cmd.Parameters.AddWithValue("@created_at", job.CreatedAt.ToString("O"));
            cmd.ExecuteNonQuery();
        }
    }

    public void RecordStarted(Job job) {
        lock (_writeLock) {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "UPDATE jobs SET status = @status, started_at = @started_at WHERE id = @id;";
            cmd.Parameters.AddWithValue("@id", job.Id);
            cmd.Parameters.AddWithValue("@status", StatusText(job.Status));
            cmd.Parameters.AddWithValue("@started_at", (job.StartedAt ?? DateTimeOffset.UtcNow).ToString("O"));
            cmd.ExecuteNonQuery();
        }
    }

    public void RecordCompleted(Job job, string output) {
        lock (_writeLock) {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                UPDATE jobs
                SET status = @status,
                    exit_code = @exit_code,
                    error = @error,
                    completed_at = @completed_at,
                    output = @output
                WHERE id = @id;
                """;
            cmd.Parameters.AddWithValue("@id", job.Id);
            cmd.Parameters.AddWithValue("@status", StatusText(job.Status));
            cmd.Parameters.AddWithValue("@exit_code", (object?)job.ExitCode ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@error", (object?)job.Error ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@completed_at", (job.CompletedAt ?? DateTimeOffset.UtcNow).ToString("O"));
            cmd.Parameters.AddWithValue("@output", TruncateOutput(output));
            cmd.ExecuteNonQuery();
        }
    }

    // Pull recent jobs into memory so the dashboard can render them after a
    // restart. Returns most-recent-first; limit caps memory + UI clutter.
    public IReadOnlyList<PersistedJob> LoadRecent(int limit = 100) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, command, args_json, status, exit_code, error,
                   created_at, started_at, completed_at, output
            FROM jobs
            ORDER BY created_at DESC
            LIMIT @limit;
            """;
        cmd.Parameters.AddWithValue("@limit", limit);

        var result = new List<PersistedJob>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var argsJson = reader.GetString(2);
            string[] args;
            try {
                args = JsonSerializer.Deserialize<string[]>(argsJson) ?? Array.Empty<string>();
            } catch {
                args = Array.Empty<string>();
            }
            result.Add(new PersistedJob {
                Id = reader.GetString(0),
                Command = reader.GetString(1),
                Args = args,
                Status = ParseStatus(reader.GetString(3)),
                ExitCode = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                Error = reader.IsDBNull(5) ? null : reader.GetString(5),
                CreatedAt = DateTimeOffset.Parse(reader.GetString(6)),
                StartedAt = reader.IsDBNull(7) ? null : DateTimeOffset.Parse(reader.GetString(7)),
                CompletedAt = reader.IsDBNull(8) ? null : DateTimeOffset.Parse(reader.GetString(8)),
                Output = reader.GetString(9),
            });
        }
        return result;
    }

    // Most recent successful completion of `commandPath`. Used by the Workflows
    // page to show "when was this step last refreshed?" alongside each step.
    public DateTimeOffset? GetLastSuccessfulRun(string commandPath) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            SELECT completed_at FROM jobs
            WHERE command = @cmd AND status = 'succeeded' AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1;
            """;
        cmd.Parameters.AddWithValue("@cmd", commandPath);
        var raw = cmd.ExecuteScalar();
        if (raw is null || raw is DBNull) return null;
        return DateTimeOffset.Parse((string)raw);
    }

    private static string TruncateOutput(string output) {
        if (output.Length <= MaxStoredOutputBytes) return output;
        return output.Substring(0, MaxStoredOutputBytes) + TruncationMarker;
    }

    private static string StatusText(JobStatus s) => s switch {
        JobStatus.Pending   => "pending",
        JobStatus.Running   => "running",
        JobStatus.Succeeded => "succeeded",
        JobStatus.Failed    => "failed",
        JobStatus.Cancelled => "cancelled",
        _ => "unknown",
    };

    private static JobStatus ParseStatus(string s) => s switch {
        "pending"   => JobStatus.Pending,
        "running"   => JobStatus.Running,
        "succeeded" => JobStatus.Succeeded,
        "failed"    => JobStatus.Failed,
        "cancelled" => JobStatus.Cancelled,
        _ => JobStatus.Failed,
    };
}

// Plain DTO returned from LoadRecent. Distinct from Job because Job carries
// an active broadcaster; PersistedJob is a frozen snapshot used to seed the
// registry at startup.
public sealed class PersistedJob {
    public required string Id { get; init; }
    public required string Command { get; init; }
    public required IReadOnlyList<string> Args { get; init; }
    public required JobStatus Status { get; init; }
    public int? ExitCode { get; init; }
    public string? Error { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public string Output { get; init; } = "";
}
