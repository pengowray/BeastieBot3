using System.IO;
using BeastieBot3.Infrastructure;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins the non-interactive (web-capture / piped) path of ProgressConsole: it must emit a plain
// text line with the item count — "desc: N/total (pct)" when the total is known, "desc: N" when
// indeterminate — and strip Spectre markup from the description. This is the path that replaced
// the live progress bar's per-frame redraws in captured job logs.
public class ProgressConsoleTests {
    private static (IAnsiConsole Console, StringWriter Out) NonInteractive() {
        var sw = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
            Out = new AnsiConsoleOutput(sw),
        });
        return (console, sw);
    }

    [Fact]
    public void NonInteractive_Determinate_EmitsCountTotalAndPercent() {
        var (console, sw) = NonInteractive();
        ProgressConsole.Run(console, "Downloading taxa", 3, p => {
            for (var i = 0; i < 3; i++) p.Increment(1);
        });
        var output = sw.ToString();
        Assert.Contains("Downloading taxa", output);
        Assert.Contains("3/3", output);
        Assert.Contains("100%", output);
    }

    [Fact]
    public void NonInteractive_Indeterminate_EmitsCountOnly() {
        var (console, sw) = NonInteractive();
        ProgressConsole.Run(console, "Importing taxa", 0, p => {
            for (var i = 0; i < 5; i++) p.Increment(1);
        });
        var output = sw.ToString();
        Assert.Contains("Importing taxa: 5", output);
        Assert.DoesNotContain("%", output); // no percentage when the total is unknown
    }

    [Fact]
    public void NonInteractive_StripsMarkupFromDescription() {
        var (console, sw) = NonInteractive();
        ProgressConsole.Run(console, "[green]IUCN names[/]", 2, p => {
            p.Increment(1);
            p.Increment(1);
        });
        var output = sw.ToString();
        Assert.Contains("IUCN names", output);
        Assert.DoesNotContain("[green]", output);
    }

    [Fact]
    public void NonInteractive_TotalSetLater_SwitchesToCountTotal() {
        var (console, sw) = NonInteractive();
        ProgressConsole.Run(console, "Caching", 0, p => {
            p.Total = 4; // discovered after starting
            for (var i = 0; i < 4; i++) p.Increment(1);
        });
        var output = sw.ToString();
        Assert.Contains("4/4", output);
    }
}
