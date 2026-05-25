using System;

namespace BeastieBot3.Web.Jobs;

// Record describing a CLI job execution scheduled or executed via the web UI.
// Jobs are held in JobRegistry (in-memory) and mirrored to JobHistoryStore
// (SQLite) so the "Recent jobs" list and each job's captured output survive
// server restarts.

public enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

public sealed class Job {
    public required string Id { get; init; }
    public required string Command { get; init; }
    public required IReadOnlyList<string> Args { get; init; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public JobStatus Status { get; set; } = JobStatus.Pending;
    public int? ExitCode { get; set; }
    public string? Error { get; set; }
    public JobOutputBroadcaster Output { get; init; } = new();

    // Live job cancellation: set when the job starts, populated only for jobs
    // owned by this process. Rehydrated past jobs have a null source.
    public CancellationTokenSource? CancellationSource { get; set; }

    public string CommandLine =>
        Args.Count == 0 ? Command : Command + " " + string.Join(' ', Args);
}
