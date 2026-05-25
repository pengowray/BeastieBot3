using System.Collections.Concurrent;

namespace BeastieBot3.Web.Jobs;

// Job index. In-memory map keyed by short opaque id, mirrored to an optional
// JobHistoryStore so the list survives server restarts. New jobs are inserted
// here; the JobRunner is responsible for persisting state transitions.

public sealed class JobRegistry {
    private readonly ConcurrentDictionary<string, Job> _jobs = new();
    private readonly JobHistoryStore? _store;
    private const int MaxRetained = 200;

    public JobRegistry() : this(null) {}

    public JobRegistry(JobHistoryStore? store) {
        _store = store;
        if (_store is not null) RehydrateFromStore();
    }

    public JobHistoryStore? Store => _store;

    public Job Create(string command, IReadOnlyList<string> args) {
        var id = Guid.NewGuid().ToString("N").Substring(0, 12);
        var job = new Job { Id = id, Command = command, Args = args };
        _jobs[id] = job;
        _store?.Insert(job);
        EvictIfNeeded();
        return job;
    }

    public Job? Get(string id) => _jobs.TryGetValue(id, out var j) ? j : null;

    public IReadOnlyCollection<Job> All() =>
        _jobs.Values.OrderByDescending(j => j.CreatedAt).ToList();

    private void RehydrateFromStore() {
        if (_store is null) return;
        foreach (var p in _store.LoadRecent(MaxRetained)) {
            var job = new Job {
                Id = p.Id,
                Command = p.Command,
                Args = p.Args,
                CreatedAt = p.CreatedAt,
                StartedAt = p.StartedAt,
                CompletedAt = p.CompletedAt,
                Status = p.Status,
                ExitCode = p.ExitCode,
                Error = p.Error,
                // Pre-completed broadcaster so SSE subscribers receive the
                // stored output once and disconnect.
                Output = new JobOutputBroadcaster(p.Output, completed: true),
            };
            _jobs[p.Id] = job;
        }
    }

    private void EvictIfNeeded() {
        if (_jobs.Count <= MaxRetained) return;
        var stale = _jobs.Values
            .Where(j => j.Status is JobStatus.Succeeded or JobStatus.Failed or JobStatus.Cancelled)
            .OrderBy(j => j.CompletedAt ?? j.CreatedAt)
            .Take(_jobs.Count - MaxRetained)
            .Select(j => j.Id)
            .ToList();
        foreach (var id in stale) _jobs.TryRemove(id, out _);
    }
}
