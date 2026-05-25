using BeastieBot3.Configuration;
using BeastieBot3.Web.Jobs;
using BeastieBot3.Web.Status;

namespace BeastieBot3.Web.Flows;

// Resolves a FlowDefinition into a runtime snapshot the UI can render:
//   - each step gets a status (ready / ok / blocked) derived from data-source
//     presence, plus a "last run" timestamp from the job history store
//   - each FlowResource gets its on-disk absolute path so the file viewer
//     can list/read it via safe-rooted FilesEndpoints

public sealed class FlowEvaluator {
    private readonly StatusService _status;
    private readonly JobHistoryStore? _history;

    public FlowEvaluator(StatusService status, JobHistoryStore? history) {
        _status = status;
        _history = history;
    }

    public FlowSnapshot Snapshot(FlowDefinition flow) {
        var sourceStatusById = _status.Collect().ToDictionary(s => s.Id);
        var steps = flow.Steps.Select(s => Evaluate(s, sourceStatusById)).ToList();
        return new FlowSnapshot {
            Id = flow.Id,
            Title = flow.Title,
            Description = flow.Description,
            Steps = steps,
            Templates = flow.Templates,
            Outputs = flow.Outputs,
        };
    }

    private FlowStepSnapshot Evaluate(FlowStep step, IReadOnlyDictionary<string, DataSourceStatus> sources) {
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

        string status;
        if (missingInputs.Count > 0) {
            status = "blocked";
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
            Note = step.Note,
            Status = status,
            MissingInputs = missingInputs,
            LastRunAt = lastRun,
            LastRunCommand = lastRunCommand,
        };
    }
}

public sealed record FlowSnapshot {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public required IReadOnlyList<FlowStepSnapshot> Steps { get; init; }
    public required IReadOnlyList<FlowResource> Templates { get; init; }
    public required IReadOnlyList<FlowResource> Outputs { get; init; }
}

public sealed record FlowStepSnapshot {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public required IReadOnlyList<string> Commands { get; init; }
    public required IReadOnlyList<string> InputSourceIds { get; init; }
    public required IReadOnlyList<string> OutputSourceIds { get; init; }
    public required bool Optional { get; init; }
    public string? Note { get; init; }
    public required string Status { get; init; }              // "blocked" | "never-run" | "ok"
    public required IReadOnlyList<string> MissingInputs { get; init; }
    public DateTimeOffset? LastRunAt { get; init; }
    public string? LastRunCommand { get; init; }
}
