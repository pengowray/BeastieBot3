using System.Text;
using System.Threading.Channels;

namespace BeastieBot3.Web.Jobs;

// Captures one job's console output and fans it out to any number of live SSE
// subscribers. Late subscribers receive a replay of the history buffer followed
// by the live stream until the job completes.

public sealed class JobOutputBroadcaster {
    // A multi-hour download emits far more text than anyone reads back. Keep a
    // bounded tail: the end of a log is where the errors and the exit status
    // are, and an unbounded buffer both grew the serve process without limit
    // and handed a reconnecting browser a replay it could not render.
    private const int MaxHistoryChars = 1_000_000;
    private const int HistoryTrimSliceChars = 256_000;
    private const string TrimMarker = "\x1b[2m[earlier output trimmed]\x1b[0m\n";

    private readonly object _lock = new();
    private readonly StringBuilder _history = new();
    private readonly List<Channel<string>> _subscribers = new();
    private bool _completed;

    public JobOutputBroadcaster() {}

    // Used when rehydrating a persisted past job: the broadcaster carries the
    // stored output as its frozen history and reports as already-completed,
    // so SSE subscribers replay the text once and disconnect.
    public JobOutputBroadcaster(string initialHistory, bool completed) {
        if (!string.IsNullOrEmpty(initialHistory)) _history.Append(initialHistory);
        _completed = completed;
    }

    public bool IsCompleted {
        get { lock (_lock) return _completed; }
    }

    public string GetHistory() {
        lock (_lock) return _history.ToString();
    }

    public void Append(string chunk) {
        if (string.IsNullOrEmpty(chunk)) return;
        List<Channel<string>> snapshot;
        lock (_lock) {
            if (_completed) return;
            _history.Append(chunk);
            TrimHistoryLocked();
            snapshot = _subscribers.ToList();
        }
        foreach (var ch in snapshot) {
            ch.Writer.TryWrite(chunk);
        }
    }

    // Drops whole lines off the front once the buffer passes its cap. Trimming a
    // large slice at a time keeps this O(n) per slice rather than per append.
    private void TrimHistoryLocked() {
        if (_history.Length <= MaxHistoryChars) return;
        var text = _history.ToString();
        var target = text.Length - (MaxHistoryChars - HistoryTrimSliceChars);
        var newline = text.IndexOf('\n', target);
        var cut = newline >= 0 ? newline + 1 : target;
        _history.Clear();
        _history.Append(TrimMarker).Append(text, cut, text.Length - cut);
    }

    public void Complete() {
        List<Channel<string>> snapshot;
        lock (_lock) {
            if (_completed) return;
            _completed = true;
            snapshot = _subscribers.ToList();
            _subscribers.Clear();
        }
        foreach (var ch in snapshot) ch.Writer.TryComplete();
    }

    // Returns the history seen so far, plus a reader for any subsequent output.
    // The reader will be null if the broadcaster has already completed.
    public (string History, ChannelReader<string>? Reader) Subscribe() {
        lock (_lock) {
            if (_completed) {
                return (_history.ToString(), null);
            }
            var ch = Channel.CreateUnbounded<string>(new UnboundedChannelOptions {
                SingleReader = true,
                SingleWriter = false,
            });
            _subscribers.Add(ch);
            return (_history.ToString(), ch.Reader);
        }
    }
}
