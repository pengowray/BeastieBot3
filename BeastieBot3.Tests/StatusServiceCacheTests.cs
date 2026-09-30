using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Status;

namespace BeastieBot3.Tests;

// Pins StatusService.Collect's reuse window. The dashboard requests /api/status and every
// /api/flows/{id} at once, and each of those calls Collect(); they must share one result rather
// than each running every metric query. The folder has no paths.ini, so every source reports
// "Not configured" and no database is opened.
public class StatusServiceCacheTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-status-" + Guid.NewGuid().ToString("N"));

    private sealed class ManualClock : TimeProvider {
        public DateTimeOffset Now { get; set; } = new(2026, 9, 30, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    public StatusServiceCacheTests() {
        Directory.CreateDirectory(_dir);
    }

    private PathsService EmptyPaths() => new(Path.Combine(_dir, "paths.ini"), _dir);

    [Fact]
    public void Collect_ReusesResultInsideWindow() {
        var clock = new ManualClock();
        var svc = new StatusService(EmptyPaths(), clock);

        var first = svc.Collect();
        clock.Now += StatusService.ReuseFor - TimeSpan.FromMilliseconds(1);
        Assert.Same(first, svc.Collect());
        Assert.Equal(DataSourceCatalogue.All.Count, first.Count);
    }

    [Fact]
    public void Collect_RecomputesAfterWindow() {
        var clock = new ManualClock();
        var svc = new StatusService(EmptyPaths(), clock);

        var first = svc.Collect();
        clock.Now += StatusService.ReuseFor;
        Assert.NotSame(first, svc.Collect());
    }

    [Fact]
    public void Collect_RecomputesWhenClockGoesBack() {
        // A wall-clock change must not pin an old result for however far the clock moved back.
        var clock = new ManualClock();
        var svc = new StatusService(EmptyPaths(), clock);

        var first = svc.Collect();
        clock.Now -= TimeSpan.FromHours(1);
        Assert.NotSame(first, svc.Collect());
    }

    [Fact]
    public void Collect_ParallelCallsShareOneResult() {
        var clock = new ManualClock();
        var svc = new StatusService(EmptyPaths(), clock);

        var results = new IReadOnlyList<DataSourceStatus>[9];
        Parallel.For(0, results.Length, i => results[i] = svc.Collect());
        Assert.All(results, r => Assert.Same(results[0], r));
    }

    public void Dispose() {
        try { Directory.Delete(_dir, recursive: true); } catch { /* a locked temp dir is not a test failure */ }
    }
}
