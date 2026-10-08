using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Iucn;
using BeastieBot3.Shared.SiteData;

// Step lights for the public species site workflow. Pure over PublicSiteState, pinned by
// PublicSiteProbeTests.
//
// The DOI step's light counts the latest global assessments (the command's default --scope) that
// have no DOI from another source and no result in the DOI cache (SiteDoiCountReader).
// Checking the citations and deploying have no light: the report is read by a person, and the
// deployed database is on the server.

namespace BeastieBot3.Web.Flows;

public static class PublicSiteProbes {
    public const string Gbif = "site-gbif";
    public const string Dois = "site-dois";
    public const string Build = "site-build";
    public const string WikidataSweep = "site-wikidata-sweep";
    public const string GroupTitles = "site-group-titles";
    public const string NatureServe = "site-natureserve";
    public const string Ecos = "site-ecos";
    public const string Nztcs = "site-nztcs";
    public const string Salve = "site-salve";
    public const string GreenStatus = "site-green-status";

    /// The age after which the workflow asks for a new pass of `wikidata sweep-taxa`; the step's
    /// command passes the same number as --refresh-days.
    public const int SweepRefreshDays = 30;

    /// The age after which the workflow asks for the NatureServe and ECOS statuses again; the
    /// NatureServe step's command passes the same number as --refresh-days.
    public const int StatusListsRefreshDays = 30;

    /// The age after which the workflow asks for the IUCN Green Status assessments again.
    public const int GreenStatusRefreshDays = 30;

    public static bool IsProbe(string probe) => probe is Gbif or Dois or Build or WikidataSweep or GroupTitles or NatureServe or Ecos or Nztcs or Salve or GreenStatus;

    public static FlowProbeResult? Evaluate(string probe, PublicSiteState s) => probe switch {
        Gbif => GbifStep(s),
        Dois => DoiStep(s),
        Build => BuildStep(s),
        WikidataSweep => SweepStep(s),
        GroupTitles => GroupTitlesStep(s),
        NatureServe => NatureServeStep(s),
        Ecos => EcosStep(s),
        Nztcs => NztcsStep(s),
        Salve => SalveStep(s),
        GreenStatus => GreenStatusStep(s),
        _ => null,
    };

    // ---- GBIF's checklist ----

    // `site build-db` takes DOIs for latest global assessments from the newest checklist zip, and
    // accepts a DOI only when the assessment id inside it is the assessment's own. A checklist from
    // an earlier release than the IUCN Red List database has the previous assessment's DOI for
    // every taxon reassessed since, so those taxa get no DOI from it.
    internal static FlowProbeResult GbifStep(PublicSiteState s) {
        if (s.GbifDir is null) {
            return new FlowProbeResult("todo", "No folder set for the GBIF checklist: add [Datasets] GBIF_IUCN_dir to paths.ini.");
        }
        if (s.GbifZipName is null) {
            return new FlowProbeResult("todo", $"Not downloaded yet: no checklist zip in {s.GbifDir}.");
        }
        if (s.GbifReadError is { } error) {
            return new FlowProbeResult("todo", $"Could not read {s.GbifZipName}: {Sentence(error)} Download it again.");
        }

        var file = s.GbifPublished is null ? s.GbifZipName : $"{s.GbifZipName}, published {s.GbifPublished}";
        if (s.GbifRelease is not { } gbif) {
            return new FlowProbeResult("ok", $"{file}. Its metadata does not say which Red List release it is from.");
        }
        if (s.IucnRelease is not { } iucn) {
            return new FlowProbeResult("ok", $"Release {gbif} ({file}).");
        }
        if (string.Equals(gbif, iucn, StringComparison.OrdinalIgnoreCase)) {
            return new FlowProbeResult("ok", $"Release {gbif}, the same release as the IUCN Red List database ({file}).");
        }

        return CompareReleases(gbif, iucn) switch {
            < 0 => new FlowProbeResult("todo",
                $"The newest checklist is from release {gbif}, but the IUCN Red List database holds release {iucn}, so assessments new in release {iucn} get no DOI from the checklist. Download it again. If the new download is still from release {gbif}, GBIF does not have release {iucn} yet."),
            > 0 => new FlowProbeResult("todo",
                $"The newest checklist is from release {gbif}, but the IUCN Red List database holds the older release {iucn}. Import release {gbif} first (Import IUCN data workflow)."),
            _ => new FlowProbeResult("todo",
                $"The newest checklist is from release {gbif}, but the IUCN Red List database holds release {iucn}."),
        };
    }

    /// <summary>
    /// Orders two Red List release names such as "2025-2" and "2026-1" (a bare year counts as
    /// part 0 of that year). Null when either is not in that form.
    /// </summary>
    internal static int? CompareReleases(string a, string b) {
        if (ParseRelease(a) is not { } x || ParseRelease(b) is not { } y) return null;
        var byYear = x.Year.CompareTo(y.Year);
        return byYear != 0 ? byYear : x.Part.CompareTo(y.Part);
    }

    private static (int Year, int Part)? ParseRelease(string release) {
        var parts = release.Trim().Split('-');
        if (parts.Length is < 1 or > 2 || !int.TryParse(parts[0], out var year) || year < 1900) return null;
        if (parts.Length == 1) return (year, 0);
        return int.TryParse(parts[1], out var part) ? (year, part) : null;
    }

    // ---- DOIs ----

    private const string OtherSources = "IUCN's citation, the GBIF checklist or Wikidata";

    // "todo" without a DOI cache, "backlog" while some of the assessments that need a DOI have no
    // result in it, "ok" once every one has. Null (the step shows its last run) until the background
    // count has finished: a count not made yet must not read as nothing left to check.
    internal static FlowProbeResult? DoiStep(PublicSiteState s) {
        if (s.DoiCachePath is null) return null;
        var c = s.DoiCount;
        if (!s.DoiCacheExists) {
            var work = c is null ? "" : $" {c.WithoutSourceDoi:n0} latest global assessments have no DOI from {OtherSources}.";
            return new FlowProbeResult("todo", $"Not run yet: there is no DOI cache at {s.DoiCachePath}.{work}");
        }
        if (c is null) return null;
        if (c.WithoutSourceDoi == 0) {
            return new FlowProbeResult("ok", $"Every latest global assessment has a DOI from {OtherSources}.");
        }
        if (c.NotChecked > 0) {
            return new FlowProbeResult("backlog",
                $"Not checked yet: {c.NotChecked:n0} of the {c.WithoutSourceDoi:n0} latest global assessments that have no DOI from {OtherSources}.");
        }
        return new FlowProbeResult("ok",
            $"Checked the {c.WithoutSourceDoi:n0} latest global assessments that have no DOI from {OtherSources}: found a DOI for {c.Found:n0} and none for {c.NotFound:n0}.");
    }

    // ---- the pages and redirects of the groups' titles ----

    // "todo" when it has never run, or when the IUCN Red List database (where the groups come from)
    // changed after the last run; "backlog" while the last run left pages or redirect lists to download.
    internal static FlowProbeResult GroupTitlesStep(PublicSiteState s) {
        if (s.WikipediaCachePath is null) {
            return new FlowProbeResult("todo", "No path set for the Wikipedia cache: add [Datastore] enwiki_cache_sqlite to paths.ini.");
        }
        if (s.GroupTitles is not { } run) {
            return new FlowProbeResult("todo", "Not run yet.");
        }
        var stamp = IucnRefreshMath.Stamp(run.FinishedAtUtc);
        if (s.IucnFileChangedAtUtc is { } changed && (run.IucnChangedAtUtc is not { } seen || changed > seen)) {
            return new FlowProbeResult("todo",
                $"The IUCN Red List database changed after the last run ({stamp}), so the list of groups may be different. Run it again.");
        }
        if (run.PagesToDownload > 0 || run.RedirectListsToDownload > 0) {
            return new FlowProbeResult("backlog",
                $"Last run {stamp}: {run.PagesToDownload:n0} pages and {run.RedirectListsToDownload:n0} redirect lists still to download.");
        }
        return new FlowProbeResult("ok", $"Last run {stamp}: {run.Articles:n0} articles of groups, all with their redirects downloaded.");
    }

    // ---- the site database ----

    // ---- `wikidata sweep-taxa` ----

    internal static FlowProbeResult SweepStep(PublicSiteState s) {
        if (!s.WikidataCacheExists) {
            return new FlowProbeResult("todo", "No Wikidata cache yet.");
        }
        if (s.SweepPassStartedUtc is { } started) {
            return new FlowProbeResult("backlog",
                $"A pass started {started:yyyy-MM-dd} and has reached Q{s.SweepCursor}. Run the step again to finish it.");
        }
        if (s.SweepCompletedUtc is not { } completed) {
            return new FlowProbeResult("todo", "Never run, so the site lists no species from Wikidata that IUCN does not have.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - completed).TotalDays);
        return days > SweepRefreshDays
            ? new FlowProbeResult("todo", $"The last pass finished {completed:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"The last pass finished {completed:yyyy-MM-dd}.");
    }

    // ---- `statuses natureserve-fetch`, `ecos-import`, `nztcs-import` and `salve-import` ----

    private const string NoStatusListsPath = "No path set for the status lists store: add datastore_dir or status_lists_sqlite under [Datastore] in paths.ini.";

    internal static FlowProbeResult NatureServeStep(PublicSiteState s) {
        if (s.StatusListsPath is null) {
            return new FlowProbeResult("todo", NoStatusListsPath);
        }
        if (s.NatureServePassStartedUtc is { } started) {
            var of = s.NatureServePassTotal is { } total ? $" of {total:n0}" : "";
            return new FlowProbeResult("backlog",
                $"A download started {started:yyyy-MM-dd} and has stored {s.NatureServePassStored:n0}{of} records. Run the step again to finish it.");
        }
        if (s.NatureServe is not { } done) {
            return new FlowProbeResult("todo", "Not downloaded yet.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - done.FetchedAtUtc).TotalDays);
        return days > StatusListsRefreshDays
            ? new FlowProbeResult("todo", $"{done.Rows:n0} records. The last download finished {done.FetchedAtUtc:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"{done.Rows:n0} records. The last download finished {done.FetchedAtUtc:yyyy-MM-dd}.");
    }

    internal static FlowProbeResult EcosStep(PublicSiteState s) {
        if (s.StatusListsPath is null) {
            return new FlowProbeResult("todo", NoStatusListsPath);
        }
        if (s.Ecos is not { } done) {
            return new FlowProbeResult("todo", "Not downloaded yet.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - done.FetchedAtUtc).TotalDays);
        return days > StatusListsRefreshDays
            ? new FlowProbeResult("todo", $"{done.Rows:n0} listings, downloaded {done.FetchedAtUtc:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"{done.Rows:n0} listings, downloaded {done.FetchedAtUtc:yyyy-MM-dd}.");
    }

    internal static FlowProbeResult NztcsStep(PublicSiteState s) {
        if (s.StatusListsPath is null) {
            return new FlowProbeResult("todo", NoStatusListsPath);
        }
        if (s.Nztcs is not { } done) {
            return new FlowProbeResult("todo", "Not downloaded yet.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - done.FetchedAtUtc).TotalDays);
        return days > StatusListsRefreshDays
            ? new FlowProbeResult("todo", $"{done.Rows:n0} assessments, downloaded {done.FetchedAtUtc:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"{done.Rows:n0} assessments, downloaded {done.FetchedAtUtc:yyyy-MM-dd}.");
    }

    internal static FlowProbeResult SalveStep(PublicSiteState s) {
        if (s.StatusListsPath is null) {
            return new FlowProbeResult("todo", NoStatusListsPath);
        }
        if (s.Salve is not { } done) {
            return new FlowProbeResult("todo", "Not downloaded yet.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - done.FetchedAtUtc).TotalDays);
        return days > StatusListsRefreshDays
            ? new FlowProbeResult("todo", $"{done.Rows:n0} assessments, downloaded {done.FetchedAtUtc:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"{done.Rows:n0} assessments, downloaded {done.FetchedAtUtc:yyyy-MM-dd}.");
    }

    // ---- `iucn api green-status` ----

    internal static FlowProbeResult GreenStatusStep(PublicSiteState s) {
        if (s.ApiCachePath is null) {
            return new FlowProbeResult("todo", "No path set for the IUCN API cache: add IUCN_api_cache_sqlite under [Datastore] in paths.ini.");
        }
        if (s.GreenStatus is not { } done) {
            return new FlowProbeResult("todo", "Not downloaded yet.");
        }
        var days = (int)Math.Floor((s.ReadAtUtc - done.LastDownloadUtc).TotalDays);
        return days > GreenStatusRefreshDays
            ? new FlowProbeResult("todo", $"{done.Rows:n0} assessments, downloaded {done.LastDownloadUtc:yyyy-MM-dd}, {days} days ago.")
            : new FlowProbeResult("ok", $"{done.Rows:n0} assessments, downloaded {done.LastDownloadUtc:yyyy-MM-dd}.");
    }

    internal static FlowProbeResult BuildStep(PublicSiteState s) {
        if (s.SitePath is null) {
            return new FlowProbeResult("todo", "No path set for the site database: add [Datastore] site_sqlite to paths.ini.");
        }
        if (!s.SiteExists) {
            return new FlowProbeResult("todo", $"Not built yet: no file at {s.SitePath}.");
        }
        if (s.SiteReadError is { } error) {
            return new FlowProbeResult("todo", $"Could not read the site database: {Sentence(error)}");
        }
        if (s.SiteSchemaVersion != SiteDbSchema.Version) {
            var built = s.SiteSchemaVersion is { } v ? $"Built with schema version {v}." : "The site database does not record its schema version.";
            return new FlowProbeResult("todo",
                $"{built} The site in this checkout needs schema version {SiteDbSchema.Version} and refuses any other version.");
        }
        if (s.SiteBuiltAtUtc is not { } builtAt) {
            return new FlowProbeResult("todo", "The site database does not record when it was built.");
        }
        var stamp = IucnRefreshMath.Stamp(builtAt);
        if (s.IucnRelease is { } current && s.SiteIucnRelease is { } builtFrom
            && !string.Equals(current, builtFrom, StringComparison.OrdinalIgnoreCase)) {
            return new FlowProbeResult("todo",
                $"Built {stamp} from release {builtFrom}, but the IUCN Red List database now holds release {current}.");
        }

        var changed = ChangedSince(s.Inputs, builtAt);
        if (changed.Count > 0) {
            return new FlowProbeResult("todo",
                $"Out of date: {JoinNames(changed.Select(c => "the " + c.Name).ToList())} changed after the site database was built on {stamp}.");
        }

        var release = s.SiteIucnRelease is null ? "" : $" from release {s.SiteIucnRelease}";
        var taxa = s.SiteTaxonCount is { } n ? $", with {n:n0} taxa" : "";
        return new FlowProbeResult("ok", $"Built {stamp}{release}{taxa}.");
    }

    /// The inputs that changed after the build, in the order the build reads them.
    internal static IReadOnlyList<SiteInputChange> ChangedSince(IReadOnlyList<SiteInputChange> inputs, DateTime builtAtUtc) =>
        inputs.Where(i => i.ChangedAtUtc > builtAtUtc).ToList();

    // An exception message as one sentence ending in a full stop, whether or not it had one.
    private static string Sentence(string message) => message.Trim().TrimEnd('.') + ".";

    private static string JoinNames(IReadOnlyList<string> names) => names.Count switch {
        0 => "",
        1 => names[0],
        2 => $"{names[0]} and {names[1]}",
        _ => $"{string.Join(", ", names.Take(names.Count - 1))}, and {names[^1]}",
    };
}
