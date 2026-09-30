using System;
using System.IO;
using System.Threading.Tasks;
using BeastieBot3.Infrastructure;
using BeastieBot3.Wikidata;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins the phase order of wikidata cache-all and what --continue-on-seed-failure does. seed-taxa
// reports a failure by throwing (it returns 0 even after a query-service outage), so the flag
// used to change nothing: it only looked at a return code that was never non-zero.
public class WikidataCacheAllPhaseTests {
    private static IAnsiConsole MakeConsole(StringWriter sw) =>
        ConsoleSize.EnsureUsable(AnsiConsole.Create(new AnsiConsoleSettings {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
            Out = new AnsiConsoleOutput(sw),
        }));

    private sealed class Phases {
        public int SeedCalls;
        public int DownloadCalls;
        public Func<Task<int>> Seed(Func<int> body) => () => { SeedCalls++; return Task.FromResult(body()); };
        public Func<Task<int>> Download(int result) => () => { DownloadCalls++; return Task.FromResult(result); };
    }

    [Fact]
    public async Task RunsSeedThenDownload() {
        var sw = new StringWriter();
        var p = new Phases();
        var result = await WikidataCacheFullCommand.RunPhasesAsync(MakeConsole(sw), false, false, false,
            p.Seed(() => 0), p.Download(0));
        Assert.Equal(0, result);
        Assert.Equal(1, p.SeedCalls);
        Assert.Equal(1, p.DownloadCalls);
    }

    [Fact]
    public async Task BothSkipped_DoesNothing() {
        var sw = new StringWriter();
        var p = new Phases();
        var result = await WikidataCacheFullCommand.RunPhasesAsync(MakeConsole(sw), true, true, false,
            p.Seed(() => 0), p.Download(0));
        Assert.Equal(0, result);
        Assert.Equal(0, p.SeedCalls);
        Assert.Equal(0, p.DownloadCalls);
        Assert.Contains("Nothing to do", sw.ToString());
    }

    [Fact]
    public async Task SeedThrows_WithoutFlag_PropagatesAndSkipsDownload() {
        var sw = new StringWriter();
        var p = new Phases();
        await Assert.ThrowsAsync<InvalidOperationException>(() => WikidataCacheFullCommand.RunPhasesAsync(
            MakeConsole(sw), false, false, false,
            p.Seed(() => throw new InvalidOperationException("bad cursor")), p.Download(0)));
        Assert.Equal(0, p.DownloadCalls);
    }

    [Fact]
    public async Task SeedThrows_WithFlag_RunsDownloadAndStillFails() {
        var sw = new StringWriter();
        var p = new Phases();
        var result = await WikidataCacheFullCommand.RunPhasesAsync(MakeConsole(sw), false, false, true,
            p.Seed(() => throw new InvalidOperationException("bad cursor")), p.Download(0));
        Assert.NotEqual(0, result);
        Assert.Equal(1, p.DownloadCalls);
        Assert.Contains("bad cursor", sw.ToString());
    }

    [Fact]
    public async Task Cancellation_IsNotSwallowedByTheFlag() {
        var sw = new StringWriter();
        var p = new Phases();
        await Assert.ThrowsAsync<OperationCanceledException>(() => WikidataCacheFullCommand.RunPhasesAsync(
            MakeConsole(sw), false, false, true,
            p.Seed(() => throw new OperationCanceledException()), p.Download(0)));
        Assert.Equal(0, p.DownloadCalls);
    }

    [Fact]
    public async Task DownloadFailure_IsTheJobResult() {
        var sw = new StringWriter();
        var p = new Phases();
        var result = await WikidataCacheFullCommand.RunPhasesAsync(MakeConsole(sw), false, false, false,
            p.Seed(() => 0), p.Download(-1));
        Assert.Equal(-1, result);
    }
}
