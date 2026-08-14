using System;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the refresh session: the piece that makes re-importing the API cache for a new release
// possible at all. The API cache carries no release version, so "re-import" means "re-download
// everything fetched before a date" — and that date has to be fixed and stored, or a refresh
// spanning several days keeps re-fetching its own work and never finishes.
public class IucnRefreshSessionTests {
    private static readonly DateTime Cutoff = new(2026, 6, 16, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Now = new(2026, 8, 14, 12, 0, 0, DateTimeKind.Utc);

    private static IucnRefreshSession Session(
        bool tombstones = false, bool discovery = false,
        DateTime? tombstonesDone = null, DateTime? discoveryDone = null,
        DateTime? completed = null, long startTaxa = 100, long startAssessments = 200) => new() {
            Id = 1,
            CutoffUtc = Cutoff,
            StartedAt = Now,
            Label = "2026-1",
            IncludeTombstones = tombstones,
            IncludeDiscovery = discovery,
            StartTaxaRemaining = startTaxa,
            StartAssessmentsRemaining = startAssessments,
            TombstonesDoneAt = tombstonesDone,
            DiscoveryDoneAt = discoveryDone,
            CompletedAt = completed,
        };

    // ---- cutoff parsing ----

    // A bare date typed by the operator must be read as UTC: downloaded_at is stored in UTC, so
    // treating it as local time would move the refresh boundary by the machine's offset.
    [Fact]
    public void BareDate_ReadsAsUtcMidnight() {
        Assert.True(IucnRefreshMath.TryParseCutoffUtc("2026-06-16", out var utc));
        Assert.Equal(new DateTime(2026, 6, 16, 0, 0, 0, DateTimeKind.Utc), utc);
        Assert.Equal(DateTimeKind.Utc, utc.Kind);
    }

    [Fact]
    public void TimestampWithZone_ConvertsToUtc() {
        Assert.True(IucnRefreshMath.TryParseCutoffUtc("2026-06-16T10:00:00+10:00", out var utc));
        Assert.Equal(new DateTime(2026, 6, 16, 0, 0, 0, DateTimeKind.Utc), utc);
    }

    [Fact]
    public void NowIsAccepted() => Assert.True(IucnRefreshMath.TryParseCutoffUtc("now", out _));

    [Fact]
    public void Rubbish_IsRejected() {
        Assert.False(IucnRefreshMath.TryParseCutoffUtc("last tuesday", out _));
        Assert.False(IucnRefreshMath.TryParseCutoffUtc("", out _));
        Assert.False(IucnRefreshMath.TryParseCutoffUtc(null, out _));
    }

    // ---- which cutoff a run uses ----

    // The point of the whole design: a plain re-run picks the session's date back up, so resuming
    // never means retyping it.
    [Fact]
    public void NoFlags_TakesTheActiveSessionsCutoff() {
        var (threshold, source) = IucnRefreshMath.ResolveThreshold(null, null, Session(), Now);
        Assert.Equal(Cutoff, threshold);
        Assert.Equal(RefreshThresholdSource.Session, source);
    }

    [Fact]
    public void NoFlagsAndNoSession_RefreshesNothing() {
        var (threshold, source) = IucnRefreshMath.ResolveThreshold(null, null, null, Now);
        Assert.Null(threshold);
        Assert.Equal(RefreshThresholdSource.None, source);
    }

    [Fact]
    public void CompletedSession_NoLongerApplies() {
        var (threshold, _) = IucnRefreshMath.ResolveThreshold(null, null, Session(completed: Now), Now);
        Assert.Null(threshold);
    }

    [Fact]
    public void ExplicitDate_BeatsTheSession() {
        var explicitCutoff = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var (threshold, source) = IucnRefreshMath.ResolveThreshold(explicitCutoff, 24, Session(), Now);
        Assert.Equal(explicitCutoff, threshold);
        Assert.Equal(RefreshThresholdSource.Explicit, source);
    }

    [Fact]
    public void MaxAgeHours_BeatsTheSessionButIsARollingWindow() {
        var (threshold, source) = IucnRefreshMath.ResolveThreshold(null, 24, Session(), Now);
        Assert.Equal(Now.AddHours(-24), threshold);
        Assert.Equal(RefreshThresholdSource.MaxAgeHours, source);
    }

    // ---- when a refresh is finished ----

    [Fact]
    public void NotFinishedWhileAnythingIsLeft() {
        Assert.False(IucnRefreshMath.IsComplete(Session(), taxaRemaining: 5, assessmentsRemaining: 0));
        Assert.False(IucnRefreshMath.IsComplete(Session(), taxaRemaining: 0, assessmentsRemaining: 5));
    }

    [Fact]
    public void FinishedWhenDownloadsAreDoneAndNoExtraPhasesWereAskedFor() =>
        Assert.True(IucnRefreshMath.IsComplete(Session(), 0, 0));

    // Downloads being done is not enough: the sweep and the re-check are part of the refresh, and
    // closing early would leave them permanently unrun.
    [Fact]
    public void NotFinishedUntilTheRequestedPhasesHaveRun() {
        Assert.False(IucnRefreshMath.IsComplete(Session(tombstones: true), 0, 0));
        Assert.False(IucnRefreshMath.IsComplete(Session(discovery: true), 0, 0));
        Assert.True(IucnRefreshMath.IsComplete(Session(tombstones: true, tombstonesDone: Now), 0, 0));
        Assert.True(IucnRefreshMath.IsComplete(
            Session(tombstones: true, discovery: true, tombstonesDone: Now, discoveryDone: Now), 0, 0));
    }

    // ---- progress ----

    // The denominator is snapshotted at the start, so new SIS ids arriving from a fresh CSV import
    // mid-refresh can't make progress jump around.
    [Fact]
    public void ProgressCountsDownFromTheStartingTotals() {
        var progress = new IucnRefreshProgress {
            Session = Session(startTaxa: 100, startAssessments: 200),
            TaxaRemaining = 40,
            AssessmentsRemaining = 60,
        };
        Assert.Equal(60, progress.TaxaDone);
        Assert.Equal(140, progress.AssessmentsDone);
        Assert.Equal(66, progress.PercentDone);   // 200 of 300
    }

    [Fact]
    public void ProgressNeverGoesNegativeOrPastComplete() {
        Assert.Equal(0, IucnRefreshMath.Done(100, 150));
        Assert.Equal(100, IucnRefreshMath.Percent(0, 0));
        Assert.Equal(0, IucnRefreshMath.Percent(100, 500));
    }

    // ---- the store, over :memory: ----

    private static SqliteConnection OpenMemory() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return connection;
    }

    [Fact]
    public void SessionSurvivesAReopen_SoTheDateOutlivesTheProcess() {
        using var connection = OpenMemory();
        using var store = IucnApiCacheStore.OpenFromConnection(connection);

        var started = store.StartRefreshSession(Cutoff, "2026-1", includeTombstones: true, includeDiscovery: true);
        var read = store.GetActiveRefreshSession();

        Assert.NotNull(read);
        Assert.Equal(started.Id, read!.Id);
        Assert.Equal(Cutoff, read.CutoffUtc);
        Assert.Equal(DateTimeKind.Utc, read.CutoffUtc.Kind);
        Assert.Equal("2026-1", read.Label);
        Assert.True(read.IncludeTombstones);
        Assert.True(read.IncludeDiscovery);
        Assert.Null(read.CompletedAt);
    }

    [Fact]
    public void ClosingASession_LeavesNoActiveOne() {
        using var connection = OpenMemory();
        using var store = IucnApiCacheStore.OpenFromConnection(connection);

        var session = store.StartRefreshSession(Cutoff, "2026-1", false, false);
        store.CloseRefreshSession(session.Id);

        Assert.Null(store.GetActiveRefreshSession());
        Assert.NotNull(store.GetLastRefreshSession()!.CompletedAt);
    }

    [Fact]
    public void PhaseMarkersArePersisted() {
        using var connection = OpenMemory();
        using var store = IucnApiCacheStore.OpenFromConnection(connection);

        var session = store.StartRefreshSession(Cutoff, "2026-1", true, true);
        store.MarkRefreshPhaseDone(session.Id, "discovery_done_at");

        var read = store.GetActiveRefreshSession()!;
        Assert.NotNull(read.DiscoveryDoneAt);
        Assert.Null(read.TombstonesDoneAt);
    }

    [Fact]
    public void AnUnknownPhaseColumnIsRefused() {
        using var connection = OpenMemory();
        using var store = IucnApiCacheStore.OpenFromConnection(connection);
        var session = store.StartRefreshSession(Cutoff, null, false, false);
        Assert.Throws<ArgumentException>(() => store.MarkRefreshPhaseDone(session.Id, "completed_at"));
    }
}
