using System;
using System.Globalization;

// A refresh session: "re-download everything in the API cache that was fetched before this date".
//
// The API cache carries no release version — a payload downloaded during 2025-2 looks exactly like
// one downloaded today — so the only way to re-import a release is by download date. Two things
// make that usable across days:
//
//   • The cutoff is FIXED and stored, not a rolling "older than N hours" window. A window
//     recomputed from "now" on every run re-fetches work the previous run already did, which on a
//     ~37-hour refresh means never finishing.
//   • The cache commands fall back to the active session's cutoff when none is given, so resuming
//     is a plain re-run. Nobody retypes the date.
//
// The session also carries the starting counts, so progress has a stable denominator: new SIS ids
// arriving from a fresh CSV import would otherwise inflate "done" as the run went on.

namespace BeastieBot3.Iucn;

public sealed record IucnRefreshSession {
    public required long Id { get; init; }
    public required DateTime CutoffUtc { get; init; }
    public required DateTime StartedAt { get; init; }
    public string? Label { get; init; }                    // e.g. "2026-1" — what the operator is refreshing to
    public bool IncludeTombstones { get; init; }
    public bool IncludeDiscovery { get; init; }
    public long StartTaxaRemaining { get; init; }
    public long StartAssessmentsRemaining { get; init; }
    public DateTime? TombstonesDoneAt { get; init; }
    public DateTime? DiscoveryDoneAt { get; init; }
    public DateTime? CompletedAt { get; init; }

    public string DisplayLabel => string.IsNullOrWhiteSpace(Label) ? $"session {Id}" : Label!;
}

// A session plus the live counts, so callers report progress from one place.
public sealed record IucnRefreshProgress {
    public required IucnRefreshSession Session { get; init; }
    public required long TaxaRemaining { get; init; }
    public required long AssessmentsRemaining { get; init; }

    public long TaxaDone => IucnRefreshMath.Done(Session.StartTaxaRemaining, TaxaRemaining);
    public long AssessmentsDone => IucnRefreshMath.Done(Session.StartAssessmentsRemaining, AssessmentsRemaining);
    public int PercentDone => IucnRefreshMath.Percent(
        Session.StartTaxaRemaining + Session.StartAssessmentsRemaining,
        TaxaRemaining + AssessmentsRemaining);

    public bool IsFinished => IucnRefreshMath.IsComplete(Session, TaxaRemaining, AssessmentsRemaining);
}

// Where a run's refresh cutoff came from. Reported on screen so a run that quietly picked up a
// session's date says so rather than looking like an ordinary top-up.
public enum RefreshThresholdSource {
    None,           // no refresh — only entries never downloaded are fetched
    Explicit,       // --refresh-before on this run
    MaxAgeHours,    // --max-age-hours on this run (rolling window; not resumable)
    Session,        // the active refresh session's stored cutoff
}

public static class IucnRefreshMath {
    public static long Done(long start, long remaining) => Math.Max(0, start - remaining);

    public static int Percent(long start, long remaining) {
        if (start <= 0) return 100;
        var done = Done(start, remaining);
        return (int)Math.Clamp(done * 100 / start, 0, 100);
    }

    // A session is finished when nothing is left older than the cutoff and every phase it asked
    // for has run. Without this the fallback cutoff would apply forever and the step would offer
    // to resume a refresh that finished months ago.
    public static bool IsComplete(IucnRefreshSession session, long taxaRemaining, long assessmentsRemaining) =>
        taxaRemaining == 0
        && assessmentsRemaining == 0
        && (!session.IncludeTombstones || session.TombstonesDoneAt is not null)
        && (!session.IncludeDiscovery || session.DiscoveryDoneAt is not null);

    // Which cutoff a run uses. An explicit flag always wins, so a one-off top-up can ignore the
    // session; otherwise the session's stored cutoff carries across runs by itself.
    public static (DateTime? Threshold, RefreshThresholdSource Source) ResolveThreshold(
        DateTime? refreshBeforeUtc,
        double? maxAgeHours,
        IucnRefreshSession? activeSession,
        DateTime utcNow) {
        if (refreshBeforeUtc is { } explicitCutoff) {
            return (explicitCutoff, RefreshThresholdSource.Explicit);
        }
        if (maxAgeHours is { } hours && hours > 0) {
            return (utcNow - TimeSpan.FromHours(hours), RefreshThresholdSource.MaxAgeHours);
        }
        if (activeSession is { CompletedAt: null } session) {
            return (session.CutoffUtc, RefreshThresholdSource.Session);
        }
        return (null, RefreshThresholdSource.None);
    }

    // Cutoffs are compared against downloaded_at values stored as UTC "O" strings, so a bare date
    // typed by the operator must be read as UTC. DateTime.Parse would call it local time and shift
    // the boundary by the timezone offset.
    public static bool TryParseCutoffUtc(string? text, out DateTime utc) {
        utc = default;
        if (string.IsNullOrWhiteSpace(text)) return false;

        var trimmed = text.Trim();
        if (string.Equals(trimmed, "now", StringComparison.OrdinalIgnoreCase)) {
            utc = DateTime.UtcNow;
            return true;
        }

        if (!DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)) {
            return false;
        }

        utc = parsed.UtcDateTime;
        return true;
    }

    // How the run describes the cutoff it is using, in one line.
    public static string DescribeThreshold(DateTime threshold, RefreshThresholdSource source, IucnRefreshSession? session) => source switch {
        RefreshThresholdSource.Explicit =>
            $"Re-downloading anything fetched before {Stamp(threshold)}.",
        RefreshThresholdSource.MaxAgeHours =>
            $"Re-downloading anything fetched before {Stamp(threshold)} (a rolling window: the next run moves it, so a long refresh will repeat work).",
        RefreshThresholdSource.Session =>
            $"Refresh {session?.DisplayLabel ?? "session"} is running: re-downloading anything fetched before {Stamp(threshold)}.",
        _ => "",
    };

    public static string Stamp(DateTime utc) => utc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC";
}
