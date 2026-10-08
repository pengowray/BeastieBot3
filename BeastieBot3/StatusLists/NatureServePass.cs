using System.Globalization;
using BeastieBot3.Infrastructure;

// The progress of `statuses natureserve-fetch`, kept in status_sync_state, and what a run does with it.
//
// A pass is either full (every record) or a refresh (records modified since the last pass started).
// Both page through scientific name prefixes (natureserve_partition). When a full pass finishes and
// stored as many records as NatureServe said it has, it deletes the records it did not see. A
// refresh cannot see deletions that way, so it asks NatureServe for the records unpublished since
// the last pass started and deletes those.

namespace BeastieBot3.StatusLists;

internal static class NatureServePassKeys {
    public const string Started = "natureserve_pass_started";
    public const string Kind = "natureserve_pass_kind";                       // full | refresh
    public const string ModifiedSince = "natureserve_pass_modified_since";
    public const string Total = "natureserve_pass_total";
    public const string Completed = "natureserve_completed";
    public const string CompletedPassStarted = "natureserve_completed_pass_started";
    public const string CompletedKind = "natureserve_completed_kind";
    public const string CompletedSeen = "natureserve_completed_seen";
    public const string CompletedTotal = "natureserve_completed_total";
    public const string FullCompleted = "natureserve_full_completed";

    public const string Full = "full";
    public const string Refresh = "refresh";

    /// The keys of the pass under way, cleared when a pass starts or finishes.
    public static readonly string[] PassKeys = { Started, Kind, ModifiedSince, Total };
}

internal sealed record NatureServePassState(
    DateTime? PassStartedUtc,
    bool PassIsRefresh,
    DateTime? PassModifiedSinceUtc,
    long? PassTotal,
    DateTime? CompletedUtc,
    DateTime? CompletedPassStartedUtc,
    string? CompletedKind,
    long? CompletedSeen,
    long? CompletedTotal,
    DateTime? FullCompletedUtc) {

    public static NatureServePassState Read(Func<string, string?> get) => new(
        StoredUtc.Parse(get(NatureServePassKeys.Started)),
        get(NatureServePassKeys.Kind) == NatureServePassKeys.Refresh,
        StoredUtc.Parse(get(NatureServePassKeys.ModifiedSince)),
        Long(get(NatureServePassKeys.Total)),
        StoredUtc.Parse(get(NatureServePassKeys.Completed)),
        StoredUtc.Parse(get(NatureServePassKeys.CompletedPassStarted)),
        get(NatureServePassKeys.CompletedKind),
        Long(get(NatureServePassKeys.CompletedSeen)),
        Long(get(NatureServePassKeys.CompletedTotal)),
        StoredUtc.Parse(get(NatureServePassKeys.FullCompleted)));

    private static long? Long(string? value) =>
        long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
}

internal static class NatureServePlan {
    public enum Action { Continue, StartFull, StartRefresh, UpToDate }

    /// A refresh asks for records modified since the last pass started, less this margin for the
    /// difference between NatureServe's clock and this machine's.
    public static readonly TimeSpan RefreshMargin = TimeSpan.FromHours(1);

    public static Action Decide(NatureServePassState state, bool restart, int? refreshDays, DateTime nowUtc) {
        if (restart) {
            return Action.StartFull;
        }
        if (state.PassStartedUtc is not null) {
            return Action.Continue;
        }
        if (state.CompletedUtc is not { } completed || state.FullCompletedUtc is null) {
            return Action.StartFull;
        }
        return refreshDays is { } days && nowUtc - completed > TimeSpan.FromDays(Math.Max(0, days))
            ? Action.StartRefresh
            : Action.UpToDate;
    }

    /// The modifiedSince time of a refresh pass.
    public static DateTime RefreshSince(NatureServePassState state) =>
        (state.CompletedPassStartedUtc ?? state.CompletedUtc ?? DateTime.UtcNow) - RefreshMargin;

    /// Whether a finished full pass may delete the records it did not see: only when it stored at
    /// least as many records as NatureServe said it has, so a record missed by paging is never
    /// deleted.
    public static bool MayDeleteUnseen(long seen, long? total) => total is { } t && t > 0 && seen >= t;

    /// The NatureServe Explorer citation form, with the access date.
    public static string Citation(DateTime accessedUtc) =>
        string.Create(CultureInfo.InvariantCulture,
            $"NatureServe. {accessedUtc:yyyy}. NatureServe Explorer [web application]. NatureServe, Arlington, Virginia. Available https://explorer.natureserve.org/. (Accessed: {accessedUtc.ToString("MMMM d, yyyy", CultureInfo.InvariantCulture)}).");
}
