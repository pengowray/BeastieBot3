using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace BeastieBot3.Infrastructure;

// Progress reporting that adapts to the console it's writing to:
//
//   • Interactive terminal  → a Spectre.Console live Progress bar (description, bar, %,
//     N/total count, ETA, spinner) that redraws in place.
//   • Non-interactive sink   → a throttled plain-text line (at most one every ~2s, plus a
//     final) such as "Downloading taxa: 1,024/4,853 (21%)  ~03:12 left".
//
// The web job runner captures console output to an unbounded buffer (and persists it), so a
// live bar's per-frame redraws would balloon memory and the stored log with thousands of
// frames. Routing long item loops through this helper keeps the captured log small AND adds
// the running item count. Use it instead of calling AnsiConsole.Progress() directly.
//
// Pass total <= 0 for an indeterminate loop (unknown count): the text sink then emits just
// "description: N" and the bar shows an indeterminate spinner. Call Total= later once known.

internal interface IProgressHandle {
    double Value { get; }
    void Increment(double amount = 1);
    /// <summary>Plain-text label shown for the current work (e.g. "Scanning Felidae").</summary>
    string Description { set; }
    /// <summary>Set/refine the total once known; &lt;= 0 marks the task indeterminate.</summary>
    double Total { set; }
}

internal static class ProgressConsole {
    public static Task RunAsync(string description, double total, Func<IProgressHandle, Task> work, CancellationToken cancellationToken = default)
        => RunAsync(AnsiConsole.Console, description, total, work, cancellationToken);

    public static async Task RunAsync(IAnsiConsole console, string description, double total, Func<IProgressHandle, Task> work, CancellationToken cancellationToken = default) {
        _ = cancellationToken; // cancellation is observed by the caller's loop; kept for call-site symmetry
        if (console.Profile.Capabilities.Interactive) {
            await console.Progress()
                .Columns(Columns())
                .StartAsync(async ctx => {
                    var task = CreateTask(ctx, description, total);
                    await work(new SpectreHandle(task)).ConfigureAwait(false);
                    Finish(task);
                }).ConfigureAwait(false);
        } else {
            var handle = new TextHandle(console, description, total);
            try { await work(handle).ConfigureAwait(false); }
            finally { handle.WriteFinal(); }
        }
    }

    public static void Run(string description, double total, Action<IProgressHandle> work)
        => Run(AnsiConsole.Console, description, total, work);

    public static void Run(IAnsiConsole console, string description, double total, Action<IProgressHandle> work) {
        if (console.Profile.Capabilities.Interactive) {
            console.Progress()
                .Columns(Columns())
                .Start(ctx => {
                    var task = CreateTask(ctx, description, total);
                    work(new SpectreHandle(task));
                    Finish(task);
                });
        } else {
            var handle = new TextHandle(console, description, total);
            try { work(handle); }
            finally { handle.WriteFinal(); }
        }
    }

    private static ProgressColumn[] Columns() => new ProgressColumn[] {
        new TaskDescriptionColumn(),
        new ProgressBarColumn(),
        new PercentageColumn(),
        new ProgressCountColumn(),
        new RemainingTimeColumn(),
        new SpinnerColumn(),
    };

    private static ProgressTask CreateTask(ProgressContext ctx, string description, double total) {
        // Descriptions follow Spectre's convention: they MAY contain markup ("[green]…[/]").
        var task = ctx.AddTask(description, autoStart: true);
        if (total > 0) { task.MaxValue = total; } else { task.IsIndeterminate = true; }
        return task;
    }

    // Strip Spectre markup so the plain-text sink shows "IUCN names" not "[green]IUCN names[/]".
    internal static string StripMarkup(string text) {
        if (string.IsNullOrEmpty(text) || text.IndexOf('[') < 0) return text;
        try { return Markup.Remove(text); } catch { return text; }
    }

    private static void Finish(ProgressTask task) {
        if (!task.IsIndeterminate) task.Value = task.MaxValue;
    }

    private sealed class SpectreHandle : IProgressHandle {
        private readonly ProgressTask _task;
        public SpectreHandle(ProgressTask task) => _task = task;
        public double Value => _task.Value;
        public void Increment(double amount = 1) => _task.Increment(amount);
        public string Description { set => _task.Description = value; }
        public double Total {
            set {
                if (value > 0) { _task.MaxValue = value; _task.IsIndeterminate = false; }
                else { _task.IsIndeterminate = true; }
            }
        }
    }

    // Throttled text reporter for non-interactive sinks (web capture / piped output).
    private sealed class TextHandle : IProgressHandle {
        private static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);
        private readonly IAnsiConsole _console;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private double _total;
        private string _description;
        private double _value;
        private TimeSpan _lastEmit = TimeSpan.FromSeconds(-100);
        private bool _emittedAny;

        public TextHandle(IAnsiConsole console, string description, double total) {
            _console = console;
            _description = description;
            _total = total;
        }

        public double Value => _value;
        public string Description { set => _description = value; }
        public double Total { set => _total = value; }

        public void Increment(double amount = 1) {
            _value += amount;
            var now = _stopwatch.Elapsed;
            if (now - _lastEmit >= Interval) {
                _lastEmit = now;
                Emit();
            }
        }

        // Always print a closing line so the final count is recorded even if the loop
        // finished within the throttle window (and so a job log shows the totals).
        public void WriteFinal() {
            if (_emittedAny && _value <= 0) return;
            Emit();
        }

        private void Emit() {
            _emittedAny = true;
            var n = (long)Math.Round(_value);
            string line;
            if (_total > 0) {
                var total = (long)Math.Round(_total);
                var pct = Math.Clamp(_value / _total, 0, 1);
                line = $"{_description}: {n:N0}/{total:N0} ({pct:P0}){FormatEta(pct)}";
            } else {
                line = $"{_description}: {n:N0}";
            }
            // Strip any markup from the (caller-supplied) description so the log shows plain text.
            _console.WriteLine(StripMarkup(line));
        }

        private string FormatEta(double pct) {
            if (pct <= 0 || pct >= 1) return string.Empty;
            var elapsed = _stopwatch.Elapsed.TotalSeconds;
            var remaining = elapsed / pct - elapsed;
            if (remaining <= 0 || double.IsNaN(remaining) || double.IsInfinity(remaining)) return string.Empty;
            var ts = TimeSpan.FromSeconds(remaining);
            return ts.TotalHours >= 1
                ? $"  ~{(int)ts.TotalHours}:{ts.Minutes:D2}:{ts.Seconds:D2} left"
                : $"  ~{ts.Minutes:D2}:{ts.Seconds:D2} left";
        }
    }
}

// Spectre column rendering the running "N/total" count (just "N" while indeterminate).
internal sealed class ProgressCountColumn : ProgressColumn {
    public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime) {
        _ = options; _ = deltaTime;
        var value = (long)Math.Round(task.Value);
        if (task.IsIndeterminate) {
            return new Markup($"[grey]{value:N0}[/]");
        }
        var total = (long)Math.Round(task.MaxValue);
        return new Markup($"[grey]{value:N0}/{total:N0}[/]");
    }
}
