using BeastieBot3.Configuration;
using BeastieBot3.Web.Jobs;
using BeastieBot3.Web.Status;

namespace BeastieBot3.Web.Flows;

// Resolves a FlowDefinition into a runtime snapshot the UI can render:
//   - each step gets a status (ready / ok / blocked) derived from data-source
//     presence, plus a "last run" timestamp from the job history store
//   - each step's OutputPatterns are matched against the safe-root dirs; the
//     newest matching file per pattern is surfaced as a "View latest" link
//   - any currently-running job whose command matches the step's commands is
//     attached so the UI can show an in-flight indicator
//   - each FlowResource gets its on-disk absolute path so the file viewer
//     can list/read it via safe-rooted FilesEndpoints

public sealed class FlowEvaluator {
    private readonly StatusService _status;
    private readonly JobHistoryStore? _history;
    private readonly JobRegistry? _registry;
    private readonly PathsService _paths;

    public FlowEvaluator(StatusService status, JobHistoryStore? history, JobRegistry? registry = null) {
        _status = status;
        _history = history;
        _registry = registry;
        _paths = new PathsService();
    }

    public FlowSnapshot Snapshot(FlowDefinition flow) {
        var sourceStatusById = _status.Collect().ToDictionary(s => s.Id);

        // Capture currently-running jobs once per snapshot so each step looks them
        // up in memory rather than hitting JobRegistry per step.
        var runningJobsByCommand = _registry?.All()
            .Where(j => j.Status is JobStatus.Pending or JobStatus.Running)
            .GroupBy(j => j.Command)
            .ToDictionary(g => g.Key, g => g.ToList())
            ?? new Dictionary<string, List<Job>>();

        var steps = flow.Steps.Select(s => Evaluate(s, sourceStatusById, runningJobsByCommand)).ToList();

        // Collect the subset of data sources actually referenced by this flow,
        // so the UI can render input/output chips with their existence and
        // primary row count without a second /api/status fetch.
        var referencedIds = flow.Steps
            .SelectMany(s => s.InputSourceIds.Concat(s.OutputSourceIds))
            .Distinct(StringComparer.Ordinal);
        var sources = new Dictionary<string, FlowSourceInfo>();
        foreach (var id in referencedIds) {
            if (!sourceStatusById.TryGetValue(id, out var s)) continue;
            sources[id] = new FlowSourceInfo {
                Id = s.Id,
                Name = s.Name,
                Kind = s.Kind,
                Exists = s.Exists,
                Headline = SummariseHeadline(s),
            };
        }

        return new FlowSnapshot {
            Id = flow.Id,
            Title = flow.Title,
            Description = flow.Description,
            Steps = steps,
            Sources = sources,
            Templates = flow.Templates,
            Outputs = flow.Outputs,
        };
    }

    // Pick the most informative single metric from a DataSourceStatus to show
    // as a one-line headline on a step's source chip. Prefers the first non-
    // null, non-zero metric so a brand-new database that has only "0 rows"
    // metrics still shows "0 taxa" rather than blank.
    private static string? SummariseHeadline(DataSourceStatus s) {
        if (!s.Exists) return "missing";
        if (s.Metrics.Count == 0) return null;
        var first = s.Metrics.FirstOrDefault(m => m.Value is > 0)
                    ?? s.Metrics.First();
        if (first.Value is null) return first.Label + ": n/a";
        return string.Format("{0:N0} {1}", first.Value, first.Label);
    }

    private FlowStepSnapshot Evaluate(FlowStep step,
                                      IReadOnlyDictionary<string, DataSourceStatus> sources,
                                      IReadOnlyDictionary<string, List<Job>> runningJobsByCommand) {
        // Block status: any required input data source missing.
        // (Optional steps still report block info; the UI styles them differently.)
        var missingInputs = step.InputSourceIds
            .Where(id => !sources.TryGetValue(id, out var s) || !s.Exists)
            .ToList();

        // Most recent successful completion across any of the step's commands.
        DateTimeOffset? lastRun = null;
        string? lastRunCommand = null;
        if (_history is not null) {
            foreach (var cmd in step.Commands) {
                var t = _history.GetLastSuccessfulRun(cmd);
                if (t is null) continue;
                if (lastRun is null || t > lastRun) {
                    lastRun = t;
                    lastRunCommand = cmd;
                }
            }
        }

        // Active job(s) for this step.
        var running = step.Commands
            .SelectMany(c => runningJobsByCommand.TryGetValue(c, out var jobs) ? jobs : Enumerable.Empty<Job>())
            .Select(j => new FlowRunningJob {
                JobId = j.Id,
                Command = j.Command,
                Status = j.Status.ToString().ToLowerInvariant(),
                StartedAt = j.StartedAt,
            })
            .ToList();

        // Latest matching file per output pattern.
        var latestOutputs = new List<FlowOutputFile>();
        foreach (var p in step.OutputPatterns) {
            var match = FindLatestMatch(p);
            if (match is not null) latestOutputs.Add(match);
        }

        string status;
        if (missingInputs.Count > 0) {
            status = "blocked";
        } else if (running.Count > 0) {
            status = "running";
        } else if (lastRun is null) {
            status = "never-run";
        } else {
            status = "ok";
        }

        return new FlowStepSnapshot {
            Id = step.Id,
            Title = step.Title,
            Description = step.Description,
            Commands = step.Commands,
            InputSourceIds = step.InputSourceIds,
            OutputSourceIds = step.OutputSourceIds,
            Optional = step.Optional,
            Section = step.Section.ToString().ToLowerInvariant(),
            Note = step.Note,
            Status = status,
            MissingInputs = missingInputs,
            LastRunAt = lastRun,
            LastRunCommand = lastRunCommand,
            RunningJobs = running,
            LatestOutputs = latestOutputs,
        };
    }

    // Resolves a FlowOutputPattern against the matching safe-root directory
    // and returns metadata for the most recently modified file (or null).
    private FlowOutputFile? FindLatestMatch(FlowOutputPattern pattern) {
        var rootPath = ResolveRootPath(pattern.Root);
        if (rootPath is null || !Directory.Exists(rootPath)) return null;
        try {
            var newest = new DirectoryInfo(rootPath)
                .EnumerateFiles(pattern.Pattern, SearchOption.TopDirectoryOnly)
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .FirstOrDefault();
            if (newest is null) return null;
            return new FlowOutputFile {
                Root = pattern.Root,
                Path = newest.Name,
                Label = pattern.Label ?? pattern.Pattern,
                Modified = newest.LastWriteTimeUtc,
                Size = newest.Length,
            };
        } catch {
            return null;
        }
    }

    private string? ResolveRootPath(string root) => root switch {
        "rules"            => Path.Combine(AppContext.BaseDirectory, "rules"),
        "reports"          => _paths.GetReportOutputDirectory() is { Length: > 0 } r ? Path.GetFullPath(r) : null,
        "wikipedia-output" => _paths.GetWikipediaOutputDirectory() is { Length: > 0 } w ? Path.GetFullPath(w) : null,
        _ => null,
    };
}

public sealed record FlowSnapshot {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public required IReadOnlyList<FlowStepSnapshot> Steps { get; init; }
    public required IReadOnlyDictionary<string, FlowSourceInfo> Sources { get; init; }
    public required IReadOnlyList<FlowResource> Templates { get; init; }
    public required IReadOnlyList<FlowResource> Outputs { get; init; }
}

public sealed record FlowSourceInfo {
    public required string Id { get; init; }
    public required string Name { get; init; }
    public required string Kind { get; init; }     // "sqlite" | "directory"
    public required bool Exists { get; init; }
    public string? Headline { get; init; }          // e.g. "191,472 assessments"
}

public sealed record FlowStepSnapshot {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public required IReadOnlyList<string> Commands { get; init; }
    public required IReadOnlyList<string> InputSourceIds { get; init; }
    public required IReadOnlyList<string> OutputSourceIds { get; init; }
    public required bool Optional { get; init; }
    public required string Section { get; init; }            // "pipeline" | "maintenance"
    public string? Note { get; init; }
    public required string Status { get; init; }              // "blocked" | "running" | "never-run" | "ok"
    public required IReadOnlyList<string> MissingInputs { get; init; }
    public DateTimeOffset? LastRunAt { get; init; }
    public string? LastRunCommand { get; init; }
    public IReadOnlyList<FlowRunningJob> RunningJobs { get; init; } = Array.Empty<FlowRunningJob>();
    public IReadOnlyList<FlowOutputFile> LatestOutputs { get; init; } = Array.Empty<FlowOutputFile>();
}

public sealed record FlowRunningJob {
    public required string JobId { get; init; }
    public required string Command { get; init; }
    public required string Status { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
}

public sealed record FlowOutputFile {
    public required string Root { get; init; }
    public required string Path { get; init; }
    public required string Label { get; init; }
    public required DateTimeOffset Modified { get; init; }
    public required long Size { get; init; }
}
