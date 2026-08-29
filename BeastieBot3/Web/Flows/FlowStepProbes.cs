using System;
using BeastieBot3.Col;
using BeastieBot3.CommonNames;
using BeastieBot3.Iucn;

namespace BeastieBot3.Web.Flows;

// Turns the on-disk state of a route into a step status the workflow page can show.
//
// Without this, a step done by hand can only ever say "you do this one yourself", and a step
// with a command can only say when that command last ran — which is not the same question.
// `iucn import` running three minutes ago says nothing about whether the release downloaded
// since then went in, and a database holding the previous release looks perfectly healthy.
//
// Pure functions over a state record so the decisions can be pinned by tests.

public sealed record FlowProbeResult(string Status, string? Detail);

public static class FlowStepProbes {
    // Probe keys, referenced from FlowCatalogue steps.
    public const string IucnCsvDownload = "iucn-csv-download";
    public const string IucnCsvImport = "iucn-csv-import";
    public const string IucnCsvRepoint = "iucn-csv-repoint";
    public const string IucnApiRefresh = "iucn-api-refresh";
    public const string IucnApiTaxa = "iucn-api-taxa";
    public const string IucnApiDiscovery = "iucn-api-discovery";
    public const string IucnApiInfraranks = "iucn-api-infraranks";
    public const string IucnApiProjection = "iucn-api-projection";

    public static bool IsIucnCsvProbe(string probe) =>
        probe is IucnCsvDownload or IucnCsvImport or IucnCsvRepoint;

    public const string ColImport = "col-import";
    public const string ColRepoint = "col-repoint";
    public const string ColCleanup = "col-cleanup";
    public const string ColRebuildNames = "col-rebuild-names";
    public const string ColRebuildAudit = "col-rebuild-audit";
    public const string ColRebuildLists = "col-rebuild-lists";

    public const string CommonNameConflicts = "common-name-conflicts";

    public static bool IsCommonNameProbe(string probe) => probe is CommonNameConflicts;

    public static bool IsIucnApiProbe(string probe) =>
        probe is IucnApiRefresh or IucnApiTaxa or IucnApiDiscovery or IucnApiInfraranks or IucnApiProjection;

    public static bool IsColProbe(string probe) =>
        probe is ColImport or ColRepoint or ColCleanup
            or ColRebuildNames or ColRebuildAudit or ColRebuildLists;

    public static FlowProbeResult? EvaluateCommonNames(string probe, CommonNameHubState state) => probe switch {
        CommonNameConflicts => Conflicts(state),
        _ => null,
    };

    public static FlowProbeResult? EvaluateCol(string probe, ColUpdateState state, ColArtifacts artifacts) => probe switch {
        ColImport => ColImportStep(state),
        ColRepoint => ColRepointStep(state),
        ColCleanup => ColCleanupStep(state),
        ColRebuildNames => ColRebuild(state, artifacts.CommonNamesModified),
        ColRebuildAudit => ColRebuild(state, artifacts.AuditSiteModified),
        ColRebuildLists => ColRebuild(state, artifacts.WikipediaListsModified),
        _ => null,
    };

    // Null = this probe has nothing to say; the caller falls back to its usual status.
    public static FlowProbeResult? Evaluate(string probe, IucnReleaseState state) => probe switch {
        IucnCsvDownload => Download(state),
        IucnCsvImport => Import(state),
        IucnCsvRepoint => Repoint(state),
        _ => null,
    };

    public static FlowProbeResult? EvaluateApi(string probe, IucnApiCacheState state) => probe switch {
        IucnApiRefresh => ApiRefresh(state),
        IucnApiTaxa => ApiTaxa(state),
        IucnApiDiscovery => ApiDiscovery(state),
        IucnApiInfraranks => ApiInfraranks(state),
        IucnApiProjection => ApiProjection(state),
        _ => null,
    };

    // Are the release's zip files where the import will look for them?
    internal static FlowProbeResult Download(IucnReleaseState s) {
        if (s.InputDir is null) {
            return new FlowProbeResult("todo", "No download folder set: add [Datasets] IUCN_CVS_dir to paths.ini.");
        }
        if (!s.InputDirExists) {
            return new FlowProbeResult("todo", $"Download folder not found: {s.InputDir}");
        }
        if (s.Zips.Count == 0) {
            return new FlowProbeResult("todo", $"No zip files in {s.InputDir}");
        }
        if (s.InputRelease is null) {
            return new FlowProbeResult("todo",
                $"Nothing in the folder name says which release these {Zips(s.Zips.Count)} are. "
                + $"Rename {s.InputDir} to something like IUCN_CVS_2026-1.");
        }
        if (s.MisreadZips.Count > 0) {
            var n = s.MisreadZips.Count;
            return new FlowProbeResult("todo",
                $"The import will stop: {(n == 1 ? "1 zip file here reads" : $"{n} zip files here read")} "
                + $"as release {s.MisreadRelease}, not {s.InputRelease}. "
                + $"Each download needs its own subfolder whose name starts with {s.InputRelease}.");
        }
        return new FlowProbeResult("ok", $"Release {s.InputRelease} · {Zips(s.Zips.Count)} in {s.InputDir}");
    }

    // Is this release in a database — and all of it, not just the first of the two downloads?
    internal static FlowProbeResult? Import(IucnReleaseState s) {
        // Nothing to measure against: no release to name, or nothing downloaded yet.
        if (s.InputRelease is null || s.Zips.Count == 0) return null;

        var holding = s.HoldingDb;
        if (holding is null) {
            var blocker = s.ConfiguredDb?.HeldRelease;
            return new FlowProbeResult("todo", blocker is null
                ? $"Release {s.InputRelease} is not imported yet."
                : $"Release {s.InputRelease} is not imported. {s.ConfiguredDb!.FileName} holds release {blocker}.");
        }

        var imported = s.ImportedZipCount(holding);
        if (imported < s.Zips.Count) {
            return new FlowProbeResult("todo",
                $"Only part of release {s.InputRelease} is imported: {holding.FileName} has {imported} of its "
                + $"{s.Zips.Count} zip files. Re-run to add the rest.");
        }

        return new FlowProbeResult("ok",
            $"Release {s.InputRelease} is in {holding.FileName} · {imported} of {s.Zips.Count} zip files imported");
    }

    // Does everything else actually read the database this release went into? True only once
    // paths.ini has been edited AND serve restarted, because paths.ini is read at startup only.
    internal static FlowProbeResult? Repoint(IucnReleaseState s) {
        if (s.InputRelease is null) return null;

        var holding = s.HoldingDb;
        if (holding is null) return null;   // nothing imported to point at yet

        if (s.ConfiguredDb is not null && s.ConfiguredDb.Holds(s.InputRelease)) {
            return new FlowProbeResult("ok",
                $"paths.ini already points at {holding.FileName}, which holds release {s.InputRelease}.");
        }

        var stale = s.ConfiguredDb?.HeldRelease;
        var staleName = s.ConfiguredDb?.FileName ?? "the old database";
        return new FlowProbeResult("todo",
            $"Release {s.InputRelease} went into {holding.FileName}, but everything still reads {staleName}"
            + (stale is null ? "." : $" (release {stale}).")
            + " Point [Datastore] IUCN_sqlite_from_cvs at the new file and restart serve.");
    }

    // ---- the API route --------------------------------------------------------------------
    // A refresh is one job across several steps, so while one is running every download step
    // reports against it. With no refresh in progress each step reports its own coverage.

    // Is a re-import under way, and does it still need starting?
    internal static FlowProbeResult? ApiRefresh(IucnApiCacheState s) {
        if (!s.CacheExists) return null;   // nothing cached yet, so there is nothing to re-import

        if (s.ActiveSession is not { } session) {
            var age = s.OldestTaxaDownloadedAt is { } oldest
                ? $" The oldest payload was fetched {IucnRefreshMath.Stamp(oldest)}."
                : "";
            return new FlowProbeResult("ok",
                $"No re-import in progress, so the steps below only fetch what is missing.{age}");
        }

        var progress = s.RefreshProgress!;
        return new FlowProbeResult("ok",
            $"Re-import {session.DisplayLabel} is running: everything fetched before {IucnRefreshMath.Stamp(session.CutoffUtc)}, {progress.PercentDone}% done. The steps below use this date on their own.");
    }

    // Species and their assessments: the two long download phases.
    internal static FlowProbeResult? ApiTaxa(IucnApiCacheState s) {
        if (!s.CacheExists) {
            return new FlowProbeResult("todo", "No API cache yet. This step creates it.");
        }

        if (s.RefreshProgress is { } refresh) {
            var session = refresh.Session;
            if (refresh.TaxaRemaining == 0 && refresh.AssessmentsRemaining == 0) {
                return new FlowProbeResult("ok",
                    $"Refresh {session.DisplayLabel}: everything re-downloaded ({s.TaxaCached:N0} taxa, {s.AssessmentsCached:N0} assessments).");
            }
            return new FlowProbeResult("todo",
                $"Refresh {session.DisplayLabel} is {refresh.PercentDone}% done: "
                + $"{refresh.TaxaRemaining:N0} taxa and {refresh.AssessmentsRemaining:N0} assessments still to re-download. "
                + "Re-run to carry on; the cutoff date is remembered.");
        }

        if (s.TaxaCached == 0) {
            return new FlowProbeResult("todo", "Nothing cached from the API yet.");
        }

        var age = s.OldestTaxaDownloadedAt is { } oldest
            ? $" · oldest fetched {IucnRefreshMath.Stamp(oldest)}"
            : "";
        return new FlowProbeResult("ok",
            $"{s.TaxaCached:N0} taxa and {s.AssessmentsCached:N0} assessments cached{age}");
    }

    // Family paging: only meaningful as part of a refresh, since on its own it is a discovery
    // sweep you run when you feel like it.
    internal static FlowProbeResult? ApiDiscovery(IucnApiCacheState s) {
        if (s.ActiveSession is not { IncludeDiscovery: true } session) return null;

        return session.DiscoveryDoneAt is null
            ? new FlowProbeResult("todo",
                $"Refresh {session.DisplayLabel} includes the family sweep and it has not run yet. It runs itself as part of the next full API run.")
            : new FlowProbeResult("ok",
                $"Refresh {session.DisplayLabel}: family sweep done {IucnRefreshMath.Stamp(session.DiscoveryDoneAt.Value)}.");
    }

    // Subspecies and varieties queue assessments of their own, so the honest signal for these
    // steps is how much of the assessment backlog is still undownloaded.
    internal static FlowProbeResult? ApiInfraranks(IucnApiCacheState s) {
        if (!s.CacheExists || s.TaxaCached == 0) return null;

        if (s.BacklogOutstanding == 0) {
            return new FlowProbeResult("ok", $"Every queued assessment is downloaded ({s.AssessmentsCached:N0} in the cache).");
        }

        // The handful the API answers with a server error never come down; they are not work left.
        if (s.BacklogOutstanding <= s.ServerErrorAssessments) {
            return new FlowProbeResult("ok",
                $"{s.BacklogOutstanding:N0} queued assessments are still missing, and all of them are ones the API answers with a server error. Nothing left to fetch.");
        }

        return new FlowProbeResult("todo",
            $"{s.BacklogOutstanding:N0} queued assessments are not downloaded yet.");
    }

    // The projection is what --dataset api actually reads, so "is it built from what is in the
    // cache now" is the question, not "did the command run".
    internal static FlowProbeResult? ApiProjection(IucnApiCacheState s) {
        var projection = s.Projection;
        if (projection is null) return null;

        if (!projection.Exists) {
            return new FlowProbeResult("todo", "Not built yet, so --dataset api has nothing to read.");
        }

        if (s.ActiveSession is { } session) {
            return new FlowProbeResult("todo",
                $"Built {Stamp(projection.BuiltAt)} — before refresh {session.DisplayLabel} finished, so it still holds the old download. Re-build it at the end.");
        }

        if (projection.IsPartial) {
            return new FlowProbeResult("todo",
                $"Built {Stamp(projection.BuiltAt)} but incomplete: {projection.LatestNotDownloaded:N0} taxa have a current assessment that was not downloaded. Download them, then build it again.");
        }

        return new FlowProbeResult("ok",
            $"Built {Stamp(projection.BuiltAt)} · {projection.ProjectedTaxa:N0} taxa, complete");
    }

    // ---- Catalogue of Life ------------------------------------------------------------------
    // Nothing else anywhere checks the CoL version: a database from the previous release looks
    // perfectly healthy, and its consumers degrade quietly rather than failing, so their output
    // just stays frozen on the old release. These steps say which.

    internal static FlowProbeResult? ColImportStep(ColUpdateState s) => s.Status switch {
        "not-imported" => new FlowProbeResult("todo", s.Message),
        "incomplete" => new FlowProbeResult("todo", s.Message),
        "update-available" => new FlowProbeResult("todo",
            $"The input folder has a newer release ({Release(s.Input?.Label, s.Input?.Issued)}) than the imported database ({Release(s.Loaded?.Label, s.Loaded?.Issued)})."),
        "fresh" => new FlowProbeResult("ok",
            $"{Release(s.Loaded?.Label, s.Loaded?.Issued)} is imported into {s.Loaded?.FileName}."),
        "no-input" => new FlowProbeResult("ok",
            $"{Release(s.Loaded?.Label, s.Loaded?.Issued)} is imported. No ColDP zip in the input folder, so a newer release can't be spotted."),
        // Reading the input archive for the first time takes a while; say what is imported and
        // leave the comparison to the next poll rather than claiming there is nothing newer.
        "input-pending" => new FlowProbeResult("ok",
            $"{Release(s.Loaded?.Label, s.Loaded?.Issued)} is imported into {s.Loaded?.FileName}. Still checking the input folder for anything newer."),
        _ => null,
    };

    internal static FlowProbeResult? ColRepointStep(ColUpdateState s) {
        if (s.Loaded is not { Exists: true } loaded) {
            return new FlowProbeResult("todo", "Datastore:COL_sqlite does not point at a file that exists.");
        }

        // Order matters: while a newer release is still waiting to be imported there is nothing
        // to repoint at yet, so say that rather than "set both keys" — which is the right advice
        // only once the import has run.
        if (s.Status == "update-available") {
            return new FlowProbeResult("todo",
                $"Still reading {loaded.FileName} ({Release(loaded.Label, loaded.Issued)}). Import the newer release first, then point COL_sqlite and COL_dir at it and restart serve.");
        }

        // Both keys have to move together and nothing else notices when only one does.
        if (s.ConfigDisagrees) {
            return new FlowProbeResult("todo",
                $"paths.ini disagrees with itself: COL_sqlite reads {loaded.FileName} ({loaded.Label}) while COL_dir holds {s.Input?.Label}. Set both to the same release and restart serve.");
        }

        return new FlowProbeResult("ok",
            $"paths.ini already points at {loaded.FileName}, which holds {Release(loaded.Label, loaded.Issued)}.");
    }

    internal static FlowProbeResult? ColCleanupStep(ColUpdateState s) {
        if (s.Leftovers.Count == 0) return null;

        var biggest = s.Leftovers[0];
        return new FlowProbeResult("todo",
            s.Leftovers.Count == 1
                ? $"{biggest.FileName} ({Bytes(biggest.Bytes)}) is left over from an earlier release and is never read again."
                : $"{s.Leftovers.Count} files from earlier releases are left on disk and never read again, {Bytes(s.LeftoverBytes)} in total (largest: {biggest.FileName}).");
    }

    // Was this output written since the release now being read was imported? Nothing else notices
    // when it wasn't: CoL consumers degrade quietly, so the output simply stays on the old release.
    internal static FlowProbeResult? ColRebuild(ColUpdateState s, DateTime? artifactModified) {
        if (s.Loaded is not { Exists: true }) return null;
        if (s.CurrentSince is not { } since) return null;

        if (artifactModified is null) {
            return new FlowProbeResult("todo", "Not built yet.");
        }
        if (artifactModified.Value < since) {
            return new FlowProbeResult("todo",
                $"Last built {Stamp(artifactModified)}, before Catalogue of Life {s.Loaded.Label} was imported ({Stamp(since)}), so it still reflects the previous release.");
        }
        return new FlowProbeResult("ok",
            $"Last built {Stamp(artifactModified)}, after Catalogue of Life {s.Loaded.Label} was imported.");
    }

    // ---- the common-name hub ----------------------------------------------------------------
    // The ambiguous-name list is derived from the names in the hub, so aggregating again leaves
    // it out of date, and nothing downstream complains: generation just treats a name that has
    // since become ambiguous as if it were unique.
    internal static FlowProbeResult? Conflicts(CommonNameHubState s) {
        if (!s.HubExists || !s.Readable) return null;

        if (s.ConflictsBuiltAt is null) {
            return s.ConflictCount == 0
                ? new FlowProbeResult("todo", "Not built yet, so no common name is treated as ambiguous.")
                : null;   // rows but no date to judge them by — leave the step on its run history
        }

        var built = s.ConflictsBuiltAt.Value;
        if (s.NamesChangedAt is { } changed && changed > built) {
            return new FlowProbeResult("todo",
                $"Last built {Stamp(built)}, before the names were last aggregated ({Stamp(changed)}), so names that have become ambiguous since are not flagged.");
        }

        return new FlowProbeResult("ok",
            $"Last built {Stamp(built)} · {s.ConflictCount:N0} ambiguous names");
    }

    private static string Release(string? label, string? issued) =>
        ColUpdateStateReader.Describe(label, issued);

    private static string Bytes(long bytes) {
        string[] units = { "bytes", "KB", "MB", "GB", "TB" };
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1) { value /= 1024; unit++; }
        return unit == 0 ? $"{bytes:N0} bytes" : $"{value:0.#} {units[unit]}";
    }

    private static string Stamp(DateTime? utc) => utc is null ? "at some point" : IucnRefreshMath.Stamp(utc.Value);

    private static string Zips(int count) => count == 1 ? "1 zip file" : $"{count} zip files";
}
