using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Jobs;
using BeastieBot3.Web.Status;

namespace BeastieBot3.Web.Flows;

// Resolves a FlowDefinition into a runtime snapshot the UI can render:
//   - each step gets a status (ready / ok / blocked) derived from data-source
//     presence, plus a "last run" timestamp from the job history store
//   - a step carrying a Probe gets its status from the actual on-disk state
//     instead (see FlowStepProbes), which is the only way a step done by hand
//     can report anything, and the only way a step with a command can say
//     "this release went in" rather than "the command ran at some point"
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

    public FlowEvaluator(StatusService status, JobHistoryStore? history, JobRegistry? registry = null, PathsService? paths = null) {
        _status = status;
        _history = history;
        _registry = registry;
        _paths = paths ?? new PathsService();
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

        // Read once per snapshot, and only for a flow that asks: small SQLite reads plus a zip
        // listing, shared by every probed step in the flow.
        var iucnState = new Lazy<IucnReleaseState>(() => IucnReleaseStateReader.Read(_paths));
        var apiState = new Lazy<IucnApiCacheState>(() => IucnApiCacheStateReader.Read(_paths));

        var steps = flow.Steps.Select(s => Evaluate(s, sourceStatusById, runningJobsByCommand, iucnState, apiState)).ToList();

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
                Path = s.Path,
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
                                      IReadOnlyDictionary<string, List<Job>> runningJobsByCommand,
                                      Lazy<IucnReleaseState> iucnState,
                                      Lazy<IucnApiCacheState> apiState) {
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

        // What the on-disk state says about this step, if it carries a probe. Kept separate from
        // the status so its explanation still shows while the step is blocked or running.
        var probe = RunProbe(step, iucnState, apiState);

        string status;
        if (missingInputs.Count > 0) {
            status = "blocked";
        } else if (running.Count > 0) {
            status = "running";
        } else if (probe is not null) {
            // The on-disk state outranks run history: `iucn import` succeeding last month says
            // nothing about the release sitting in the folder today.
            status = probe.Status;
        } else if (step.Commands.Count == 0) {
            // Nothing to launch, so "not run" would be wrong: the user does this one by hand.
            status = "manual";
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
            Group = step.Group,
            Note = step.Note,
            GuideTitle = step.GuideTitle,
            GuideSteps = step.GuideSteps,
            Status = status,
            Detail = probe?.Detail,
            MissingInputs = missingInputs,
            LastRunAt = lastRun,
            LastRunCommand = lastRunCommand,
            RunningJobs = running,
            LatestOutputs = latestOutputs,
        };
    }

    // A probe reads real files, so anything unexpected there must not take the whole page down:
    // an unreadable folder or database just leaves the step on its usual status.
    private static FlowProbeResult? RunProbe(FlowStep step, Lazy<IucnReleaseState> iucnState, Lazy<IucnApiCacheState> apiState) {
        if (step.Probe is null) return null;
        try {
            if (FlowStepProbes.IsIucnCsvProbe(step.Probe)) return FlowStepProbes.Evaluate(step.Probe, iucnState.Value);
            if (FlowStepProbes.IsIucnApiProbe(step.Probe)) return FlowStepProbes.EvaluateApi(step.Probe, apiState.Value);
            return null;
        } catch {
            return null;
        }
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
    public string? Path { get; init; }              // resolved on-disk path, shown as the chip tooltip
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
    public string? Group { get; init; }                       // optional sub-section heading within the pipeline
    public string? Note { get; init; }
    public string? GuideTitle { get; init; }                  // heading for the collapsible manual walkthrough
    public IReadOnlyList<string> GuideSteps { get; init; } = Array.Empty<string>();
    public required string Status { get; init; }              // "blocked" | "running" | "todo" | "manual" | "never-run" | "ok"
    public string? Detail { get; init; }                      // one line of on-disk state from the step's probe
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
