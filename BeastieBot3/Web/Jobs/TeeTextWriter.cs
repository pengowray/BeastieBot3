using System.Text;

namespace BeastieBot3.Web.Jobs;

// TextWriter that mirrors every write to two destinations: the original
// terminal Console.Out plus a captured-output sink (the per-job broadcaster).
// Spectre.Console writes through whichever TextWriter we hand to its
// AnsiConsoleOutput, so this tee lets the web UI and the terminal see the
// same ANSI-coloured stream simultaneously.

public sealed class TeeTextWriter : TextWriter {
    private readonly TextWriter _primary;
    private readonly Action<string> _capture;

    public TeeTextWriter(TextWriter primary, Action<string> capture) {
        _primary = primary;
        _capture = capture;
    }

    public override Encoding Encoding => _primary.Encoding;
    public override IFormatProvider FormatProvider => _primary.FormatProvider;

    public override void Write(char value) {
        _primary.Write(value);
        _capture(value.ToString());
    }

    public override void Write(string? value) {
        _primary.Write(value);
        if (!string.IsNullOrEmpty(value)) _capture(value);
    }

    public override void Write(char[] buffer, int index, int count) {
        _primary.Write(buffer, index, count);
        if (count > 0) _capture(new string(buffer, index, count));
    }

    public override void Write(ReadOnlySpan<char> buffer) {
        _primary.Write(buffer);
        if (!buffer.IsEmpty) _capture(new string(buffer));
    }

    public override void Flush() => _primary.Flush();

    protected override void Dispose(bool disposing) {
        // Never dispose the primary writer (it is Console.Out).
        base.Dispose(disposing);
    }
}
