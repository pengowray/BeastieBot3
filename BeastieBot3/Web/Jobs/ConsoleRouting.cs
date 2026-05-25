using System.Text;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace BeastieBot3.Web.Jobs;

// Per-async-context console routing.
//
// Spectre.Console.Cli commands resolve `AnsiConsole.Console` (a process-wide
// static) and `Console.Out/Error` (also static). With a single serve process
// running many concurrent jobs, that means everyone's output funnels through
// the same writer — and concurrent jobs would interleave each other's text.
//
// Instead we install proxies at startup. Each proxy holds an AsyncLocal<T?>
// "current" override that flows through async/await, plus a fallback writer
// used when nothing's been pushed (server boot messages, dashboard logs).
//
// When the JobRunner starts a job, it calls Suite.PushAll(teeOut, teeErr,
// teeAnsi). The disposable returned from PushAll restores the previous
// AsyncLocal values on the job's logical context — which is independent of
// any other job's context, so jobs can run concurrently without seeing each
// other's writes.

public sealed class AsyncLocalTextWriterProxy : TextWriter {
    private readonly AsyncLocal<TextWriter?> _current = new();
    private readonly TextWriter _baseWriter;

    public AsyncLocalTextWriterProxy(TextWriter baseWriter) {
        _baseWriter = baseWriter;
    }

    // Exposed so callers (the job runner) can target the underlying terminal
    // writer directly. Wrapping the proxy itself in a tee whose "primary" is
    // the proxy would recurse infinitely — the tee writes to its primary,
    // the primary routes back through AsyncLocal to the tee, and so on.
    public TextWriter Base => _baseWriter;

    public IDisposable Push(TextWriter writer) {
        var prev = _current.Value;
        _current.Value = writer;
        return new Pop(this, prev);
    }

    private TextWriter Active => _current.Value ?? _baseWriter;

    public override Encoding Encoding => Active.Encoding;
    public override IFormatProvider FormatProvider => Active.FormatProvider;

    public override void Write(char value) => Active.Write(value);
    public override void Write(string? value) => Active.Write(value);
    public override void Write(char[] buffer, int index, int count) => Active.Write(buffer, index, count);
    public override void Write(ReadOnlySpan<char> buffer) => Active.Write(buffer);
    public override void Flush() => Active.Flush();

    protected override void Dispose(bool disposing) {
        // Do not dispose the base writer — it is the original Console.Out.
        base.Dispose(disposing);
    }

    private sealed class Pop : IDisposable {
        private readonly AsyncLocalTextWriterProxy _owner;
        private readonly TextWriter? _previous;
        public Pop(AsyncLocalTextWriterProxy owner, TextWriter? prev) { _owner = owner; _previous = prev; }
        public void Dispose() => _owner._current.Value = _previous;
    }
}

public sealed class AsyncLocalAnsiConsoleProxy : IAnsiConsole {
    private readonly AsyncLocal<IAnsiConsole?> _current = new();
    private readonly IAnsiConsole _base;

    public AsyncLocalAnsiConsoleProxy(IAnsiConsole baseConsole) {
        _base = baseConsole;
    }

    public IAnsiConsole Base => _base;

    public IDisposable Push(IAnsiConsole console) {
        var prev = _current.Value;
        _current.Value = console;
        return new Pop(this, prev);
    }

    private IAnsiConsole Active => _current.Value ?? _base;

    public Profile Profile => Active.Profile;
    public IAnsiConsoleCursor Cursor => Active.Cursor;
    public IAnsiConsoleInput Input => Active.Input;
    public IExclusivityMode ExclusivityMode => Active.ExclusivityMode;
    public RenderPipeline Pipeline => Active.Pipeline;
    public void Clear(bool home) => Active.Clear(home);
    public void Write(IRenderable renderable) => Active.Write(renderable);

    private sealed class Pop : IDisposable {
        private readonly AsyncLocalAnsiConsoleProxy _owner;
        private readonly IAnsiConsole? _previous;
        public Pop(AsyncLocalAnsiConsoleProxy owner, IAnsiConsole? prev) { _owner = owner; _previous = prev; }
        public void Dispose() => _owner._current.Value = _previous;
    }
}

// Holds all three proxies together so a job runner can push the whole
// console environment in one call and restore it as one disposable.
public sealed class ConsoleRoutingSuite {
    public AsyncLocalTextWriterProxy StdOut { get; }
    public AsyncLocalTextWriterProxy StdErr { get; }
    public AsyncLocalAnsiConsoleProxy Ansi { get; }

    public ConsoleRoutingSuite(TextWriter originalOut, TextWriter originalErr, IAnsiConsole originalAnsi) {
        StdOut = new AsyncLocalTextWriterProxy(originalOut);
        StdErr = new AsyncLocalTextWriterProxy(originalErr);
        Ansi = new AsyncLocalAnsiConsoleProxy(originalAnsi);
    }

    public IDisposable PushAll(TextWriter teeOut, TextWriter teeErr, IAnsiConsole teeAnsi) {
        var a = StdOut.Push(teeOut);
        var b = StdErr.Push(teeErr);
        var c = Ansi.Push(teeAnsi);
        return new Multi(a, b, c);
    }

    private sealed class Multi : IDisposable {
        private readonly IDisposable[] _items;
        public Multi(params IDisposable[] items) { _items = items; }
        public void Dispose() {
            // Pop in reverse to mirror nested using semantics.
            for (var i = _items.Length - 1; i >= 0; i--) _items[i].Dispose();
        }
    }
}
