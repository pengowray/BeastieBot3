using System;
using System.Collections.Generic;
using System.Linq;
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
    public const string IucnApiUpdateAll = "iucn-api-update-all";

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

    // The Wikidata/Wikipedia ladder: how much work each priority step has left.
    public const string WikidataSweep = "wiki-wd-sweep";
    public const string WikidataSearch = "wiki-wd-search";
    public const string WikidataDownload = "wiki-wd-download";
    public const string WikipediaQueue = "wiki-wp-queue";
    public const string WikipediaMatch = "wiki-wp-match";
    public const string WikipediaFetchAwaited = "wiki-wp-fetch-awaited";
    public const string WikipediaFetchRest = "wiki-wp-fetch-rest";
    public const string WikipediaTitlesDump = "wiki-wp-titles-dump";
    public const string WikiRetryFailed = "wiki-retry-failed";
    public const string WikiRefresh = "wiki-refresh";
    public const string WikiUpdateAll = "wiki-update-all";

    public static bool IsWikiProbe(string probe) =>
        probe is WikidataSweep or WikidataSearch or WikidataDownload or WikipediaQueue or WikipediaMatch
            or WikipediaFetchAwaited or WikipediaFetchRest or WikipediaTitlesDump or WikiRetryFailed or WikiRefresh
            or WikiUpdateAll;

    public static FlowProbeResult? EvaluateWiki(string probe, WikiCoverageState s) {
        // Nothing measured yet (first poll after startup, or a cache missing): say nothing
        // rather than report gaps of zero that were never counted.
        if (!s.Known) return null;
        return probe switch {
            WikidataSweep => WikiWikidataSweep(s),
            WikidataSearch => WikiWikidataSearch(s),
            WikidataDownload => WikiWikidataDownload(s),
            WikipediaQueue => WikiWikipediaQueue(s),
            WikipediaMatch => WikiWikipediaMatch(s),
            WikipediaFetchAwaited => WikiFetchAwaited(s),
            WikipediaFetchRest => WikiFetchRest(s),
            WikipediaTitlesDump => WikiTitlesDump(s),
            WikiRetryFailed => WikiFailures(s),
            WikiRefresh => WikiRefreshAge(s),
            WikiUpdateAll => WikiUpdate(s),
            _ => null,
        };
    }

    // The one light for the "Update" button: everything the individual steps below would find,
    // in one line, most valuable work first. "backlog" not "todo" when only the standing download
    // queues remain: never finished in one sitting is not the same as overdue.
    internal static FlowProbeResult WikiUpdate(WikiCoverageState s) {
        var unsearched = Math.Max(0, s.TaxaWithoutWikidata - s.WikidataBackfillMisses);
        var parts = new List<string>();
        if (unsearched > 0) parts.Add($"{unsearched:n0} taxa never searched for on Wikidata");
        if (s.TaxaNeverMatched > 0) parts.Add($"{s.TaxaNeverMatched:n0} taxa never checked for an article");
        if (s.WikidataEntitiesQueued > 0) parts.Add($"{s.WikidataEntitiesQueued:n0} Wikidata items to download");
        if (s.PagesQueuedAwaited > 0) parts.Add($"{s.PagesQueuedAwaited:n0} pages to download for taxa with no article yet");

        if (parts.Count > 0) {
            var status = unsearched > 0 || s.TaxaNeverMatched > 0 ? "todo" : "backlog";
            return new FlowProbeResult(status, $"To do: {string.Join(" · ", parts)}.");
        }

        var rest = Math.Max(0, s.PagesQueued - s.PagesQueuedAwaited);
        var failed = s.WikidataEntitiesFailed + s.PagesFailed;
        if (rest == 0 && failed == 0) {
            return new FlowProbeResult("ok", $"Caught up. {s.TaxaWithArticle:n0} taxa have an article; nothing is queued.");
        }
        return new FlowProbeResult("ok",
            $"Caught up on everything the lists need ({s.TaxaWithArticle:n0} taxa have an article). Low priority: {rest:n0} other titles queued, {failed:n0} failed downloads — --include-rest works through those.");
    }

    internal static FlowProbeResult WikiWikidataSweep(WikiCoverageState s) {
        var known = s.WikidataEntitiesCached + s.WikidataEntitiesQueued;
        if (known == 0) {
            return new FlowProbeResult("todo", "No Wikidata items found yet.");
        }
        return new FlowProbeResult("ok", s.WikidataSweepCursor > 0
            ? $"{known:n0} Wikidata items found. The last sweep read as far as Q{s.WikidataSweepCursor:n0}."
            : $"{known:n0} Wikidata items found.");
    }

    internal static FlowProbeResult WikiWikipediaQueue(WikiCoverageState s) {
        if (s.PagesKnown == 0) {
            return new FlowProbeResult("todo", "No titles queued yet.");
        }
        return new FlowProbeResult("ok",
            $"{s.PagesKnown:n0} titles: {s.PagesCached:n0} downloaded, {s.PagesQueued:n0} to download, {s.PagesMissing:n0} with no article.");
    }

    // "backlog" is a queue worked down over time, not something overdue: it shows the count
    // without the amber a genuinely-unfinished step gets.
    internal static FlowProbeResult WikiWikidataSearch(WikiCoverageState s) {
        if (s.TaxaWithoutWikidata == 0) {
            return new FlowProbeResult("ok", "Every IUCN taxon has a Wikidata item.");
        }

        var unsearched = Math.Max(0, s.TaxaWithoutWikidata - s.WikidataBackfillMisses);
        if (unsearched == 0) {
            return new FlowProbeResult("ok",
                $"{s.TaxaWithoutWikidata:n0} IUCN taxa have no Wikidata item, all searched for already without a match.");
        }

        return new FlowProbeResult("todo", s.WikidataBackfillMisses == 0
            ? $"{unsearched:n0} IUCN taxa have no Wikidata item and have not been searched for."
            : $"{s.TaxaWithoutWikidata:n0} IUCN taxa have no Wikidata item: {unsearched:n0} not searched for yet, {s.WikidataBackfillMisses:n0} searched before with no match.");
    }

    internal static FlowProbeResult WikiWikidataDownload(WikiCoverageState s) =>
        s.WikidataEntitiesQueued == 0
            ? new FlowProbeResult("ok", $"All {s.WikidataEntitiesCached:n0} Wikidata items downloaded.")
            : new FlowProbeResult("backlog", $"{s.WikidataEntitiesQueued:n0} Wikidata items to download. {s.WikidataEntitiesCached:n0} already downloaded.");

    internal static FlowProbeResult WikiWikipediaMatch(WikiCoverageState s) =>
        s.TaxaNeverMatched == 0
            ? new FlowProbeResult("ok", $"All {s.IucnTaxa:n0} IUCN taxa have been checked for an article.")
            : new FlowProbeResult("todo", $"{s.TaxaNeverMatched:n0} IUCN taxa have never been checked for an article.");

    internal static FlowProbeResult WikiFetchAwaited(WikiCoverageState s) =>
        s.PagesQueuedAwaited == 0
            ? new FlowProbeResult("ok", $"No taxon is waiting on a page. {s.TaxaWithArticle:n0} taxa have an article.")
            : new FlowProbeResult("backlog", $"{s.PagesQueuedAwaited:n0} pages to download for taxa with no article yet.");

    internal static FlowProbeResult WikiFetchRest(WikiCoverageState s) {
        var rest = Math.Max(0, s.PagesQueued - s.PagesQueuedAwaited);
        return rest == 0
            ? new FlowProbeResult("ok", "Nothing else queued.")
            : new FlowProbeResult("backlog", $"{rest:n0} other pages queued: higher taxa, synonyms and redirects no taxon is waiting on.");
    }

    // The all-titles dump is optional but cheap: without it every likely redlink in the queue
    // costs an API round-trip to learn nothing.
    internal static FlowProbeResult WikiTitlesDump(WikiCoverageState s) {
        if (s.DumpTitles == 0) {
            return new FlowProbeResult("todo",
                "No all-titles dump imported, so the fetch queue cannot tell likely redlinks from real pages.");
        }
        var dump = s.DumpDate is null ? "All-titles dump" : $"All-titles dump of {s.DumpDate}";
        var queued = s.PagesQueuedInDump + s.PagesQueuedNotInDump;
        return new FlowProbeResult("ok", queued == 0
            ? $"{dump}: {s.DumpTitles:n0} titles imported."
            : $"{dump}: {s.DumpTitles:n0} titles imported. {s.PagesQueuedInDump:n0} queued titles are in it; {s.PagesQueuedNotInDump:n0} are not (likely redlinks).");
    }

    internal static FlowProbeResult WikiFailures(WikiCoverageState s) {
        if (s.PagesFailed == 0 && s.WikidataEntitiesFailed == 0) {
            return new FlowProbeResult("ok", "No downloads failed.");
        }
        var parts = new List<string>();
        if (s.PagesFailed > 0) parts.Add($"{s.PagesFailed:n0} Wikipedia pages");
        if (s.WikidataEntitiesFailed > 0) parts.Add($"{s.WikidataEntitiesFailed:n0} Wikidata items");
        return new FlowProbeResult("todo", $"{string.Join(" and ", parts)} failed to download.");
    }

    internal static FlowProbeResult WikiRefreshAge(WikiCoverageState s) =>
        s.PagesCached == 0
            ? new FlowProbeResult("ok", "No pages cached yet.")
            : new FlowProbeResult("ok", s.OldestCachedPageAt is { } oldest
                ? $"{s.PagesCached:n0} pages cached, oldest downloaded {oldest:d MMM yyyy}."
                : $"{s.PagesCached:n0} pages cached.");

    public static bool IsIucnApiProbe(string probe) =>
        probe is IucnApiRefresh or IucnApiTaxa or IucnApiDiscovery or IucnApiInfraranks or IucnApiProjection
            or IucnApiUpdateAll;

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
        IucnApiUpdateAll => ApiUpdate(state),
        _ => null,
    };

    // The one light for the "cache-all --full" button: the single most decisive fact about the
    // API dataset, in the order a run works through them — nothing yet, a re-import mid-flight,
    // an assessment backlog, a stale projection, or done.
    internal static FlowProbeResult ApiUpdate(IucnApiCacheState s) {
        if (!s.CacheExists || s.TaxaCached == 0) {
            return new FlowProbeResult("todo",
                "Nothing cached from the API yet. The first build downloads everything (roughly 37 hours); stop and re-run whenever — it continues where it stopped.");
        }

        if (s.RefreshProgress is { } refresh) {
            var session = refresh.Session;
            if (refresh.TaxaRemaining > 0 || refresh.AssessmentsRemaining > 0) {
                return new FlowProbeResult("todo",
                    $"Re-import {session.DisplayLabel} is {refresh.PercentDone}% done: {refresh.TaxaRemaining:N0} taxa and {refresh.AssessmentsRemaining:N0} assessments still to re-download. Re-run to carry on; the cutoff date is remembered.");
            }
            return new FlowProbeResult("todo",
                $"Re-import {session.DisplayLabel}: everything is re-downloaded. One more run finishes the remaining checks and closes it.");
        }

        var backlog = Math.Max(0, s.BacklogOutstanding - s.ServerErrorAssessments);
        if (backlog > 0) {
            return new FlowProbeResult("todo", $"{backlog:N0} queued assessments are not downloaded yet.");
        }

        if (s.Projection is { } p) {
            if (!p.Exists) {
                return new FlowProbeResult("todo",
                    $"{s.TaxaCached:N0} taxa and {s.AssessmentsCached:N0} assessments cached, but the projection --dataset api reads is not built yet.");
            }
            if (p.IsPartial) {
                return new FlowProbeResult("todo",
                    $"The projection is incomplete: {p.LatestNotDownloaded:N0} taxa have a current assessment that was not downloaded. Re-run to fetch them and rebuild it.");
            }
        }

        var age = s.OldestTaxaDownloadedAt is { } oldest ? $" · oldest fetched {IucnRefreshMath.Stamp(oldest)}" : "";
        return new FlowProbeResult("ok",
            $"{s.TaxaCached:N0} taxa and {s.AssessmentsCached:N0} assessments cached{age}. For a new release, start a re-import first (the step above).");
    }

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
