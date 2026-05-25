using System.Text;
using System.Threading.Channels;

namespace BeastieBot3.Web.Jobs;

// Captures one job's console output and fans it out to any number of live SSE
// subscribers. Late subscribers receive a replay of the history buffer followed
// by the live stream until the job completes.

public sealed class JobOutputBroadcaster {
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
            snapshot = _subscribers.ToList();
        }
        foreach (var ch in snapshot) {
            ch.Writer.TryWrite(chunk);
        }
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
