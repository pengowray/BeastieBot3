using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Doi;
using BeastieBot3.Iucn.Gbif;
using Microsoft.Data.Sqlite;

// The count behind the light of the public site workflow's DOI step: of the latest global
// assessments with no DOI from IUCN's citation, the GBIF checklist or Wikidata (the assessments
// `iucn resolve-dois` works on with its default --scope), how many the DOI cache has checked, and
// what it found. These are the numbers `iucn resolve-dois --status` prints.
//
// The count has two parts with very different costs:
//   1. Which assessments have no DOI from those sources. IucnDoiScopeReader decides that, the same
//      way the command does, by reading the CSV export, every taxon record in the IUCN API cache,
//      the GBIF checklist and some cached payloads: about 17 seconds on release 2026-1. It runs on
//      a background task and the poll reads its last result.
//   2. Which of those the DOI cache has checked: one primary-key lookup per assessment on a
//      read-only connection, done again whenever the DOI cache changes, so the light follows a
//      `resolve-dois` run straight away.
//
// Part 1 is counted again as soon as the IUCN Red List database or the GBIF checklist changes,
// because both are steps of this workflow. When only the IUCN API cache or the Wikidata cache
// changed, it is counted again at most every 30 minutes: both caches change for hours on end during
// an API refresh or `wikipedia update`, and what those changes do to this count (a citation that
// gains a DOI, a new Wikidata DOI) is rare.
//
// The previous release's CSV export is not read: it decides an assessment's DOI language and the
// release it was new in, which the command needs and this count does not.

namespace BeastieBot3.Web.Flows;

/// <summary>The DOI cache's progress on the latest global assessments that need it.</summary>
public sealed record SiteDoiCount {
    /// The Red List release of the CSV export the count was made from.
    public required string Release { get; init; }
    /// Latest global assessments in the CSV export.
    public int InScope { get; init; }
    /// Of those, the assessments with no DOI from IUCN's citation, the GBIF checklist or Wikidata.
    public int WithoutSourceDoi { get; init; }
    /// Of those, the assessments the DOI cache found a DOI for.
    public int Found { get; init; }
    /// Of those, the assessments the DOI cache checked and found no DOI for.
    public int NotFound { get; init; }
    /// Of those, the assessments the DOI cache has no result for.
    public int NotChecked { get; init; }
    /// When part 1 of the count started.
    public DateTime CountedAtUtc { get; init; }
}

public static class SiteDoiCountReader {
    internal static readonly TimeSpan SlowInputInterval = TimeSpan.FromMinutes(30);

    /// <summary>The result of part 1, with the input keys it was counted for.</summary>
    internal sealed record TargetCount(
        string FastKey,
        string SlowKey,
        DateTime CountedAtUtc,
        string? Release,
        int InScope,
        IReadOnlyList<DoiTarget> Targets,
        string? Error);

    private sealed record CheckCount(TargetCount Targets, string DoiKey, SiteDoiCount? Count);

    private static TargetCount? _targets;
    private static CheckCount? _checks;
    private static int _counting;
    private static bool _warned;

    /// Forgets both parts, so the next read counts again (tests, and after paths.ini changes).
    public static void Invalidate() {
        _targets = null;
        _checks = null;
    }

    /// <summary>
    /// The count for the poll. Never waits for part 1: the first call starts it in the background
    /// and returns null, as does every call while the last count is for another IUCN release or
    /// GBIF checklist, or failed. Never throws.
    /// </summary>
    public static SiteDoiCount? Read(PublicSitePaths paths, PublicSiteState state) {
        try {
            var (fast, slow) = Keys(paths, state);
            var last = _targets;
            if (ShouldRecount(last, fast, slow, DateTime.UtcNow, SlowInputInterval)) {
                CountInBackground(paths, fast, slow);
            }
            if (last is null || last.Error is not null || last.FastKey != fast) {
                return null;
            }

            var doiKey = DoiCacheKey(paths.DoiCache);
            var checks = _checks;
            if (checks is not null && ReferenceEquals(checks.Targets, last) && checks.DoiKey == doiKey) {
                return checks.Count;
            }
            var count = CountChecks(last, paths.DoiCache, DateTime.UtcNow);
            _checks = new CheckCount(last, doiKey, count);
            return count;
        } catch {
            return null;
        }
    }

    /// <summary>
    /// Pure: whether part 1 must be counted again. Always when there is no count yet or the IUCN
    /// Red List database or GBIF checklist changed (the fast key); when only the API cache or the
    /// Wikidata cache changed (the slow key), or the last count failed, once the last count is at
    /// least <paramref name="slowInterval"/> old.
    /// </summary>
    internal static bool ShouldRecount(TargetCount? last, string fastKey, string slowKey, DateTime nowUtc, TimeSpan slowInterval) {
        if (last is null || last.FastKey != fastKey) return true;
        if (last.SlowKey == slowKey && last.Error is null) return false;
        return nowUtc - last.CountedAtUtc >= slowInterval;
    }

    /// <summary>
    /// The input keys of part 1, from the paths and the change times the public site state already
    /// holds: the fast key changes with the IUCN Red List database or the newest GBIF checklist,
    /// the slow key with the newest download in the API cache or the Wikidata cache.
    /// </summary>
    internal static (string Fast, string Slow) Keys(PublicSitePaths paths, PublicSiteState state) {
        string Time(string input) =>
            state.Inputs.FirstOrDefault(i => i.Name == input)?.ChangedAtUtc.Ticks.ToString() ?? "-";
        var fast = string.Join("|",
            paths.IucnDatabase, paths.ApiCache, paths.GbifDir, paths.WikidataCache,
            Time(PublicSiteStateReader.IucnInput), state.GbifZipName, Time(PublicSiteStateReader.GbifInput));
        var slow = string.Join("|", Time(PublicSiteStateReader.ApiCacheInput), Time(PublicSiteStateReader.WikidataInput));
        return (fast, slow);
    }

    private static string DoiCacheKey(string? path) =>
        $"{path}|{PublicSiteStateReader.SqliteChangedAt(path)?.Ticks}";

    private static void CountInBackground(PublicSitePaths paths, string fast, string slow) {
        if (Interlocked.CompareExchange(ref _counting, 1, 0) != 0) return;
        _ = Task.Run(() => {
            try {
                _targets = CountTargets(paths, fast, slow, DateTime.UtcNow);
            } finally {
                Interlocked.Exchange(ref _counting, 0);
            }
        });
    }

    /// <summary>
    /// Part 1, on the calling thread: the latest global assessments with no DOI from IUCN's
    /// citation, the GBIF checklist or Wikidata. A failure is kept in the result, never thrown.
    /// </summary>
    internal static TargetCount CountTargets(PublicSitePaths paths, string fastKey, string slowKey, DateTime nowUtc) {
        try {
            if (!File.Exists(paths.IucnDatabase) || !File.Exists(paths.ApiCache)) {
                return Failed("the IUCN Red List database or the IUCN API cache does not exist.");
            }
            var sources = new DoiScopeSources(
                paths.IucnDatabase!,
                PreviousIucnDatabase: null,
                paths.ApiCache!,
                GbifIucnChecklistFiles.FindNewest(paths.GbifDir),
                paths.WikidataCache);
            var result = IucnDoiScopeReader.Read(sources, DoiScope.LatestGlobal, _ => { }, CancellationToken.None);
            return new TargetCount(fastKey, slowKey, nowUtc, result.Release, result.Counts.InScope, result.Targets, null);
        } catch (Exception ex) {
            if (!_warned) {
                _warned = true;
                Console.Error.WriteLine($"DOI step count unavailable: {ex.Message}");
            }
            return Failed(ex.Message);
        }

        TargetCount Failed(string error) =>
            new(fastKey, slowKey, nowUtc, null, 0, Array.Empty<DoiTarget>(), error);
    }

    /// <summary>
    /// Part 2: how many of the targets the DOI cache has checked, the way `resolve-dois --status`
    /// counts them (DoiRunPlan). A missing DOI cache has checked none. Null when the DOI cache
    /// could not be read.
    /// </summary>
    internal static SiteDoiCount? CountChecks(TargetCount targets, string? doiCachePath, DateTime nowUtc) {
        Dictionary<long, DoiCheckRow> rows;
        try {
            rows = ReadChecks(doiCachePath, targets.Targets);
        } catch (Exception) {
            return null;
        }
        var plan = DoiRunPlan.Make(targets.Targets, rows, recheck: false, recheckMissingAfter: null, nowUtc);
        return new SiteDoiCount {
            Release = targets.Release ?? "",
            InScope = targets.InScope,
            WithoutSourceDoi = targets.Targets.Count,
            Found = plan.CheckedFound,
            NotFound = plan.CheckedNotFound,
            NotChecked = plan.ToCheck.Count,
            CountedAtUtc = targets.CountedAtUtc,
        };
    }

    // The doi_check rows of the targets only: the cache also holds the regional and history scopes
    // (about 148,000 rows in October 2026). Read-only, no pooling, no schema work.
    private static Dictionary<long, DoiCheckRow> ReadChecks(string? path, IReadOnlyList<DoiTarget> targets) {
        var rows = new Dictionary<long, DoiCheckRow>();
        if (targets.Count == 0 || string.IsNullOrWhiteSpace(path) || !File.Exists(path)) return rows;

        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        conn.Open();
        using (var exists = conn.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'doi_check'";
            if (exists.ExecuteScalar() is not long n || n == 0) return rows;
        }
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT assessment_id, taxon_id, doi, checked_at, candidates_tried FROM doi_check
            WHERE assessment_id IN (SELECT value FROM json_each(@ids))
            """;
        cmd.Parameters.AddWithValue("@ids", JsonSerializer.Serialize(targets.Select(t => t.AssessmentId)));
        cmd.CommandTimeout = 5;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var row = new DoiCheckRow(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? DateTime.MinValue : StoredUtc.Parse(reader.GetString(3)) ?? DateTime.MinValue,
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4));
            rows[row.AssessmentId] = row;
        }
        return rows;
    }
}
