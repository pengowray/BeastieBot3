using Spectre.Console;

namespace BeastieBot3.Infrastructure;

// Spectre sizes every console from System.Console, even one writing to a StringWriter or a
// pipe. On Windows, a redirected console throws there and Spectre falls back to 80 columns.
// On Linux with no terminal attached, System.Console reports a width of -1 instead, and Spectre
// then renders nothing: plain lines vanish and tables collapse to "…". Pin a default size when
// that happens. A console with a real size is left alone, so it still follows terminal resizes.
internal static class ConsoleSize {
    public const int DefaultWidth = 80;
    public const int DefaultHeight = 24;

    public static IAnsiConsole EnsureUsable(IAnsiConsole console) {
        if (console.Profile.Width <= 0) console.Profile.Width = DefaultWidth;
        if (console.Profile.Height <= 0) console.Profile.Height = DefaultHeight;
        return console;
    }

    // Writes a line the reader will copy (a paths.ini line, a file path) straight to the
    // console's writer, so Spectre does not break it at the console width. Without a terminal
    // (web jobs, pipes) that width is the 80-column default above, which split long paths.
    public static void WriteLineUnwrapped(IAnsiConsole console, string line) {
        console.Profile.Out.Writer.WriteLine(line);
    }
}
