using System;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// Pins the API workflow step lights. The question these answer is "how far in am I" when nothing
// is running — a 37-hour download that can only report progress while it happens is not much use
// to someone coming back the next morning.
public class FlowApiProbeTests {
    private static readonly DateTime Cutoff = new(2026, 6, 16, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Now = new(2026, 8, 14, 12, 0, 0, DateTimeKind.Utc);

    private static IucnRefreshSession Session(bool discovery = false, DateTime? discoveryDone = null) => new() {
        Id = 1,
        CutoffUtc = Cutoff,
        StartedAt = Now,
        Label = "2026-1",
        IncludeDiscovery = discovery,
        DiscoveryDoneAt = discoveryDone,
        StartTaxaRemaining = 100_000,
        StartAssessmentsRemaining = 200_000,
    };

    private static IucnApiCacheState State(
        long taxa = 186_236,
        long assessments = 352_384,
        long backlogOutstanding = 0,
        long serverErrors = 0,
        IucnRefreshSession? session = null,
        long taxaRemaining = 0,
        long assessmentsRemaining = 0,
        IucnProjectionState? projection = null,
        bool cacheExists = true) => new() {
            CachePath = @"D:\datasets\beastiebot\iucn_api_cache.sqlite",
            CacheExists = cacheExists,
            TaxaCached = taxa,
            AssessmentsCached = assessments,
            BacklogOutstanding = backlogOutstanding,
            ServerErrorAssessments = serverErrors,
            OldestTaxaDownloadedAt = new DateTime(2025, 11, 14, 5, 37, 0, DateTimeKind.Utc),
            ActiveSession = session,
            RefreshTaxaRemaining = taxaRemaining,
            RefreshAssessmentsRemaining = assessmentsRemaining,
            Projection = projection,
        };

    private static IucnProjectionState Projection(bool exists = true, bool partial = false, long missing = 0) => new() {
        Path = @"D:\datasets\beastiebot\iucn_api_projected.sqlite",
        Exists = exists,
        BuiltAt = new DateTime(2026, 6, 16, 6, 1, 0, DateTimeKind.Utc),
        IsPartial = partial,
        LatestNotDownloaded = missing,
        ProjectedTaxa = 181_338,
    };

    // ---- the re-import step ----

    [Fact]
    public void NoCacheYet_SaysNothingAboutReimporting() =>
        Assert.Null(FlowStepProbes.ApiRefresh(State(cacheExists: false)));

    [Fact]
    public void NoSession_SaysTopUpOnlyAndHowOldTheDataIs() {
        var r = FlowStepProbes.ApiRefresh(State())!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("only fetch what is missing", r.Detail);
        Assert.Contains("2025-11-14", r.Detail);
    }

    // The whole point of the stored cutoff: the step says the date is already known.
    [Fact]
    public void SessionRunning_SaysTheDateIsCarriedForYou() {
        var r = FlowStepProbes.ApiRefresh(State(session: Session(), taxaRemaining: 50_000, assessmentsRemaining: 100_000))!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("2026-06-16", r.Detail);
        Assert.Contains("50% done", r.Detail);
        Assert.Contains("use this date on their own", r.Detail);
    }

    // ---- the download step ----

    [Fact]
    public void NoCache_IsTodo() {
        var r = FlowStepProbes.ApiTaxa(State(cacheExists: false))!;
        Assert.Equal("todo", r.Status);
    }

    [Fact]
    public void NoSession_ReportsCoverageAndAge() {
        var r = FlowStepProbes.ApiTaxa(State())!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("186,236 taxa", r.Detail);
        Assert.Contains("352,384 assessments", r.Detail);
    }

    // Mid-refresh the step is not done, however much is in the cache — that was the old lie.
    [Fact]
    public void RefreshPartWayThrough_IsTodoWithBothCountsLeft() {
        var r = FlowStepProbes.ApiTaxa(State(session: Session(), taxaRemaining: 40_000, assessmentsRemaining: 90_000))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("40,000 taxa", r.Detail);
        Assert.Contains("90,000 assessments", r.Detail);
        Assert.Contains("cutoff date is remembered", r.Detail);
    }

    [Fact]
    public void RefreshWithNothingLeft_IsOk() {
        var r = FlowStepProbes.ApiTaxa(State(session: Session()))!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("everything re-downloaded", r.Detail);
    }

    // ---- the family sweep ----

    [Fact]
    public void FamilySweep_SaysNothingOutsideARefresh() {
        Assert.Null(FlowStepProbes.ApiDiscovery(State()));
        Assert.Null(FlowStepProbes.ApiDiscovery(State(session: Session(discovery: false))));
    }

    [Fact]
    public void FamilySweep_IsTodoUntilItHasRun() {
        var r = FlowStepProbes.ApiDiscovery(State(session: Session(discovery: true)))!;
        Assert.Equal("todo", r.Status);

        var done = FlowStepProbes.ApiDiscovery(State(session: Session(discovery: true, discoveryDone: Now)))!;
        Assert.Equal("ok", done.Status);
    }

    // ---- subspecies / varieties ----

    [Fact]
    public void Infraranks_IsOkWhenTheBacklogIsClear() {
        var r = FlowStepProbes.ApiInfraranks(State())!;
        Assert.Equal("ok", r.Status);
    }

    [Fact]
    public void Infraranks_IsTodoWhileQueuedAssessmentsAreMissing() {
        var r = FlowStepProbes.ApiInfraranks(State(backlogOutstanding: 4_000))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("4,000", r.Detail);
    }

    // The handful the API answers with a server error never come down, so counting them as work
    // left would leave this step amber forever.
    [Fact]
    public void Infraranks_IgnoresTheOnesTheApiCannotServe() {
        var r = FlowStepProbes.ApiInfraranks(State(backlogOutstanding: 23, serverErrors: 23))!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("server error", r.Detail);
    }

    // ---- the projection ----

    [Fact]
    public void Projection_NotBuilt_IsTodo() {
        var r = FlowStepProbes.ApiProjection(State(projection: Projection(exists: false)))!;
        Assert.Equal("todo", r.Status);
    }

    [Fact]
    public void Projection_Complete_IsOk() {
        var r = FlowStepProbes.ApiProjection(State(projection: Projection()))!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("181,338 taxa", r.Detail);
    }

    [Fact]
    public void Projection_Partial_IsTodoWithTheMissingCount() {
        var r = FlowStepProbes.ApiProjection(State(projection: Projection(partial: true, missing: 512)))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("512", r.Detail);
    }

    // A projection built before the re-import finished still holds the old download, however
    // healthy it looks on its own.
    [Fact]
    public void Projection_BuiltBeforeTheRefreshFinished_IsTodo() {
        var r = FlowStepProbes.ApiProjection(State(session: Session(), projection: Projection()))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("still holds the old download", r.Detail);
    }

    // ---- the one-button light (cache-all --full) ----
    // One decisive fact at a time, in run order: nothing yet, re-import mid-flight, assessment
    // backlog, stale projection, done.

    [Fact]
    public void UpdateLight_NothingCached_SaysTheFirstBuildIsLongButResumable() {
        var r = FlowStepProbes.ApiUpdate(State(cacheExists: false));
        Assert.Equal("todo", r.Status);
        Assert.Contains("Nothing cached from the API yet", r.Detail);
        Assert.Contains("continues where it stopped", r.Detail);
    }

    [Fact]
    public void UpdateLight_MidReimport_ShowsPercentAndRemainingCounts() {
        var r = FlowStepProbes.ApiUpdate(State(session: Session(), taxaRemaining: 50_000, assessmentsRemaining: 100_000));
        Assert.Equal("todo", r.Status);
        Assert.Contains("50% done", r.Detail);
        Assert.Contains("50,000 taxa and 100,000 assessments", r.Detail);
    }

    [Fact]
    public void UpdateLight_BacklogBeyondServerErrors_IsTodo() {
        var r = FlowStepProbes.ApiUpdate(State(backlogOutstanding: 900, serverErrors: 100));
        Assert.Equal("todo", r.Status);
        Assert.Contains("800 queued assessments", r.Detail);
    }

    [Fact]
    public void UpdateLight_PartialProjection_IsTodo() {
        var r = FlowStepProbes.ApiUpdate(State(projection: Projection(partial: true, missing: 512)));
        Assert.Equal("todo", r.Status);
        Assert.Contains("512", r.Detail);
    }

    [Fact]
    public void UpdateLight_Complete_IsOkAndPointsANewReleaseAtTheReimportStep() {
        var r = FlowStepProbes.ApiUpdate(State(projection: Projection()));
        Assert.Equal("ok", r.Status);
        Assert.Contains("186,236 taxa", r.Detail);
        Assert.Contains("start a re-import first", r.Detail);
    }
}
