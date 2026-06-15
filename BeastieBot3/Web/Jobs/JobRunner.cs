using Spectre.Console;

namespace BeastieBot3.Web.Jobs;

// Runs jobs concurrently. AsyncLocal-aware console proxies (installed by
// ServeCommand at startup) give each job its own logical Console.Out,
// Console.Error and AnsiConsole.Console — so two jobs running at the same
// time do not interleave each other's output.
//
// A small semaphore caps the worst case: without it, a user could enqueue
// 50 long-running fetches at once and saturate the IUCN/Wikidata APIs.

public sealed class JobRunner {
    private readonly JobRegistry _registry;
    private readonly ConsoleRoutingSuite _console;
    private readonly SemaphoreSlim _gate;

    public int MaxConcurrent { get; }

    public JobRunner(JobRegistry registry, ConsoleRoutingSuite console)
        : this(registry, console, maxConcurrent: 4) {}

    public JobRunner(JobRegistry registry, ConsoleRoutingSuite console, int maxConcurrent) {
        if (maxConcurrent < 1) {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrent), "must be at least 1");
        }
        _registry = registry;
        _console = console;
        MaxConcurrent = maxConcurrent;
        _gate = new SemaphoreSlim(maxConcurrent, maxConcurrent);
    }

    public Job Enqueue(string command, IReadOnlyList<string> args) {
        var job = _registry.Create(command, args);
        _ = Task.Run(() => RunAsync(job));
        return job;
    }

    private async Task RunAsync(Job job) {
        var cts = new CancellationTokenSource();
        job.CancellationSource = cts;
        bool gateAcquired = false;
        try {
            // Honor cancellation while waiting for a concurrency slot — otherwise
            // queued jobs would be uncancellable until the gate opens.
            await _gate.WaitAsync(cts.Token).ConfigureAwait(false);
            gateAcquired = true;
        } catch (OperationCanceledException) when (cts.IsCancellationRequested) {
            job.Status = JobStatus.Cancelled;
            job.ExitCode = -2;
            job.CompletedAt = DateTimeOffset.UtcNow;
            job.Output.Append($"\x1b[2m$ beastiebot3 {job.CommandLine}\x1b[0m\n\x1b[33m[cancelled before start]\x1b[0m\n");
            job.Output.Complete();
            _registry.Store?.RecordCompleted(job, job.Output.GetHistory());
            cts.Dispose();
            job.CancellationSource = null;
            return;
        }
        try {
            job.Status = JobStatus.Running;
            job.StartedAt = DateTimeOffset.UtcNow;
            _registry.Store?.RecordStarted(job);

            // Build per-job writers. Each tee writes to both the original
            // terminal stream (so the operator sees output) AND to this job's
            // broadcaster (so SSE subscribers see it). Crucially the tee's
            // primary must be the *base* writer behind the proxy — using the
            // proxy itself would recurse forever through the AsyncLocal slot.
            var teeOut = new TeeTextWriter(_console.StdOut.Base, chunk => job.Output.Append(chunk));
            var teeErr = new TeeTextWriter(_console.StdErr.Base, chunk => job.Output.Append(chunk));
            var teeAnsi = AnsiConsole.Create(new AnsiConsoleSettings {
                Ansi = AnsiSupport.Yes,
                ColorSystem = ColorSystemSupport.Standard,
                // The output is captured to a buffer, not a live terminal: mark it
                // non-interactive so Spectre live displays don't animate frame-by-frame
                // into the log (ProgressConsole emits throttled text counts instead).
                Interactive = InteractionSupport.No,
                Out = new AnsiConsoleOutput(teeOut),
            });

            int exitCode;
            bool cancelled = false;
            using (_console.PushAll(teeOut, teeErr, teeAnsi)) {
                job.Output.Append($"\x1b[2m$ beastiebot3 {job.CommandLine}\x1b[0m\n");

                // Multi-segment path ("iucn api cache-taxa") becomes separate argv tokens.
                var argv = new List<string>();
                argv.AddRange(job.Command.Split(' ', StringSplitOptions.RemoveEmptyEntries));
                argv.AddRange(job.Args);

                try {
                    var app = Program.BuildApp();
                    exitCode = await app.RunAsync(argv, cts.Token).ConfigureAwait(false);
                } catch (Exception ex) when (!cts.IsCancellationRequested) {
                    exitCode = 1;
                    job.Error = ex.Message;
                    job.Output.Append($"\n\x1b[31m[unhandled exception] {ex.GetType().Name}: {ex.Message}\x1b[0m\n");
                    if (ex.StackTrace is { } st) {
                        job.Output.Append($"\x1b[2m{st}\x1b[0m\n");
                    }
                } catch (OperationCanceledException) {
                    // Cancellation path. The OCE may have been caught by Spectre's
                    // exception handler instead — we also detect that case below.
                    exitCode = -2;
                }

                // Whether RunAsync returned normally (Spectre handler swallowed
                // the OCE and returned a sentinel) or it bubbled up, detect
                // cancellation via the CTS itself so we always book the right
                // status.
                if (cts.IsCancellationRequested) {
                    cancelled = true;
                    job.Output.Append("\n\x1b[33m[cancelled by request]\x1b[0m\n");
                }
            }

            job.ExitCode = exitCode;
            job.Status = cancelled
                ? JobStatus.Cancelled
                : exitCode == 0 ? JobStatus.Succeeded : JobStatus.Failed;
            job.CompletedAt = DateTimeOffset.UtcNow;
            string label = job.Status switch {
                JobStatus.Succeeded => "\x1b[32mdone\x1b[0m",
                JobStatus.Cancelled => "\x1b[33mcancelled\x1b[0m",
                _                   => "\x1b[31mexit " + exitCode + "\x1b[0m",
            };
            var elapsed = (job.CompletedAt.Value - (job.StartedAt ?? job.CreatedAt)).TotalSeconds;
            job.Output.Append($"\n\x1b[2m[{label}\x1b[2m in {elapsed:F1}s]\x1b[0m\n");
        } finally {
            job.Output.Complete();
            _registry.Store?.RecordCompleted(job, job.Output.GetHistory());
            cts.Dispose();
            job.CancellationSource = null;
            if (gateAcquired) _gate.Release();
        }
    }
}
