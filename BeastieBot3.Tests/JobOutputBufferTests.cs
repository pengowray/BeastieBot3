using System;
using System.Linq;
using BeastieBot3.Web.Jobs;

namespace BeastieBot3.Tests;

// Pins the two output caps that keep a long-running job from taking the browser
// (and the serve process) down with it: the live broadcaster's in-memory history
// and the persisted copy both keep a bounded *tail*, marked at the head.
public class JobOutputBufferTests {
    private const string TrimMark = "[earlier output trimmed]";

    [Fact]
    public void ShortOutputIsKeptVerbatim() {
        var b = new JobOutputBroadcaster();
        b.Append("hello\nworld\n");
        Assert.Equal("hello\nworld\n", b.GetHistory());
    }

    [Fact]
    public void LongOutputIsCappedAndMarked() {
        var b = new JobOutputBroadcaster();
        for (var i = 0; i < 200_000; i++) b.Append($"line {i}\n");

        var history = b.GetHistory();
        Assert.True(history.Length <= 1_100_000, $"history was {history.Length} chars");
        Assert.Contains(TrimMark, history);
        // The tail survives: the last line written is still the last line held.
        Assert.EndsWith("line 199999\n", history);
        // The head is gone.
        Assert.DoesNotContain("\nline 0\n", history);
    }

    [Fact]
    public void TrimCutsAtALineBoundary() {
        var b = new JobOutputBroadcaster();
        for (var i = 0; i < 200_000; i++) b.Append($"line {i}\n");

        var lines = b.GetHistory().Split('\n').Skip(1).Where(l => l.Length > 0).ToList();
        Assert.All(lines, l => Assert.StartsWith("line ", l));
    }

    [Fact]
    public void CompletedBroadcasterKeepsItsFrozenHistory() {
        var b = new JobOutputBroadcaster("stored output\n", completed: true);
        b.Append("ignored\n");
        Assert.Equal("stored output\n", b.GetHistory());
    }
}
