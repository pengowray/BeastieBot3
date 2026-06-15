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

internal interface IProgressHandle {
        double Value { get; }
        void Increment(double amount = 1);
        /// <summary>Plain-text label shown for the current work (e.g. "Scanning Felidae").</summary>
        string Description { set; }
    }

    internal static class ProgressConsole {
        public static Task RunAsync(string description, double total, Func<IProgressHandle, Task> work, CancellationToken cancellationToken = default)
            => RunAsync(AnsiConsole.Console, description, total, work, cancellationToken);

        public static async Task RunAsync(IAnsiConsole console, string description, double total, Func<IProgressHandle, Task> work, CancellationToken cancellationToken = default) {
            _ = cancellationToken; // cancellation is observed by the caller's loop; kept for call-site symmetry
            if (console.Profile.Capabilities.Interactive) {
                await console.Progress()
                    .Columns(
                        new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new PercentageColumn(),
                        new ProgressCountColumn(),
                        new RemainingTimeColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx => {
                        var task = ctx.AddTask(Markup.Escape(description), maxValue: total <= 0 ? 1 : total);
                        await work(new SpectreHandle(task)).ConfigureAwait(false);
                        task.Value = task.MaxValue;
                    }).ConfigureAwait(false);
            } else {
                var handle = new TextHandle(console, description, total);
                try {
                    await work(handle).ConfigureAwait(false);
                } finally {
                    handle.WriteFinal();
                }
            }
        }

        private sealed class SpectreHandle : IProgressHandle {
            private readonly ProgressTask _task;
            public SpectreHandle(ProgressTask task) => _task = task;
            public double Value => _task.Value;
            public void Increment(double amount = 1) => _task.Increment(amount);
            public string Description { set => _task.Description = Markup.Escape(value); }
        }

        // Throttled text reporter for non-interactive sinks (web capture / piped output).
        private sealed class TextHandle : IProgressHandle {
            private static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);
            private readonly IAnsiConsole _console;
            private readonly double _total;
            private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
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
                var total = (long)Math.Round(_total);
                string line;
                if (total > 0) {
                    var pct = Math.Clamp(_value / _total, 0, 1);
                    line = $"{_description}: {n:N0}/{total:N0} ({pct:P0}){FormatEta(pct)}";
                } else {
                    line = $"{_description}: {n:N0}";
                }
                // Plain text (not markup) so brackets in descriptions are safe.
                _console.WriteLine(line);
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

    // Spectre column rendering the running "N/total" count next to the bar.
    internal sealed class ProgressCountColumn : ProgressColumn {
        public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime) {
            _ = options; _ = deltaTime;
            var total = (long)Math.Round(task.MaxValue);
            var value = (long)Math.Round(task.Value);
            return new Markup($"[grey]{value:N0}/{total:N0}[/]");
        }
    }
