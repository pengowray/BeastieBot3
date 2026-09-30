namespace BeastieBot3.Web.Flows;

// Hand-maintained catalogue of "flows" — vertical pipelines that walk users
// from inputs through processing steps to outputs. Flows (in display order):
//
//   iucn-import  — get the IUCN dataset in (CSV release vs API), the prerequisite
//                  for everything else; grouped into CSV / API / Compare routes.
//   col-update   — bring in a new Catalogue of Life release and refresh everything
//                  downstream that reads it (a step-by-step guide for non-experts).
//   wiki-reports — the full Wikipedia list/chart generation pipeline.
//   wiki-quality — coverage and freshness reports on Wikipedia/Wikidata caches.
//   iucn-quality — consistency and cleanup reports on the IUCN dataset.
//
// Each step references data source IDs from `DataSourceCatalogue` (so the
// flow UI can re-use the existing status pills) and command paths from
// `CommandRegistry` (so it can fire jobs through the existing runner).

public sealed record FlowDefinition {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public IReadOnlyList<FlowStep> Steps { get; init; } = Array.Empty<FlowStep>();
    public IReadOnlyList<FlowResource> Templates { get; init; } = Array.Empty<FlowResource>();
    public IReadOnlyList<FlowResource> Outputs { get; init; } = Array.Empty<FlowResource>();
}

public enum FlowSection {
    Pipeline,     // core path through the flow; rendered as a vertical timeline
    StepByStep,   // the same work as a one-button pipeline step, one command at a time; collapsed panel
    Maintenance,  // repair/coverage steps not normally needed; rendered in a separate panel
}

public sealed record FlowStep {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public IReadOnlyList<string> Commands { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> InputSourceIds { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> OutputSourceIds { get; init; } = Array.Empty<string>();
    public bool Optional { get; init; } = false;
    public string? Note { get; init; }
    public FlowSection Section { get; init; } = FlowSection.Pipeline;

    // Optional numbered walkthrough for steps the user performs by hand (downloading a
    // release, editing config). Rendered collapsed under GuideTitle so the long procedure
    // doesn't crowd the timeline. Plain text, one instruction per entry.
    public string? GuideTitle { get; init; }
    public IReadOnlyList<string> GuideSteps { get; init; } = Array.Empty<string>();

    // Optional on-disk state check (a FlowStepProbes key). Without one, a step can only report
    // when its command last ran — or, with no command at all, nothing. A probe answers whether
    // the step's result is actually in place right now: the release downloaded, imported, and
    // being the one everything reads. See FlowStepProbes.
    public string? Probe { get; init; }

    // Optional sub-section heading within the pipeline. Consecutive steps sharing a Group are
    // rendered under one header (e.g. "1 · From the CSV release"), letting a single flow present
    // several clearly-separated routes. Null = no heading (the default flat timeline).
    public string? Group { get; init; }

    // Glob patterns (under a named safe root) that match the step's output
    // files. The evaluator picks the most-recent matching file per pattern
    // and surfaces it in the snapshot so the UI can link "View latest" per
    // step. Empty = no specific output file (the step writes only to the
    // sqlite stores referenced by OutputSourceIds).
    public IReadOnlyList<FlowOutputPattern> OutputPatterns { get; init; } = Array.Empty<FlowOutputPattern>();
}

public sealed record FlowOutputPattern {
    public required string Root { get; init; }       // "reports" | "wikipedia-output"
    public required string Pattern { get; init; }    // e.g. "iucn-name-changes-*.md"
    public string? Label { get; init; }              // optional human label; defaults to pattern
}

// A file or directory the flow points users at — templates the commands
// consume, or outputs they produce. Keyed by short root id to keep paths
// out of the API surface.
public sealed record FlowResource {
    public required string Label { get; init; }
    public required string Root { get; init; }     // "rules" | "reports" | "wikipedia-output"
    public required string Path { get; init; }     // path under root
    public required string Kind { get; init; }     // "template" | "yaml" | "markdown" | "wikitext" | "directory"
    public string? Description { get; init; }
}

public static class FlowCatalogue {
    public static readonly IReadOnlyList<FlowDefinition> All = new[] {

        // ---------------------------------------------------------------
        // Import IUCN: the prerequisite for everything else. Two routes to the
        // IUCN dataset (CSV release vs the live API), plus an optional compare.
        // Grouped so the choice and the API sub-steps are clearly separated.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "iucn-import",
            Title = "Import IUCN data",
            Description = "Every other workflow reads the IUCN data imported here. Most workflows need only the CSV release (1 · From the CSV release). The API dataset (2 · From the IUCN API) is optional: it adds synonyms, past assessments and taxa missing from the CSV release, and it needs the CSV release imported first. List and chart commands read the CSV release unless run with `--dataset api`.",
            Steps = new[] {
                // ===== 1 · From the CSV release =====
                new FlowStep {
                    Id = "csv-download",
                    Title = "Download the release from iucnredlist.org (manual)",
                    Description = "Two searches on the Red List website, each result downloaded as a zip and saved into the IUCN CSV input folder. There is no command for this: the software never downloads the release for you.",
                    Commands = Array.Empty<string>(),
                    OutputSourceIds = new[] { "iucn-csv-input" },
                    Probe = FlowStepProbes.IucnCsvDownload,
                    Group = "1 · From the CSV release",
                    Note = "You need a free iucnredlist.org account to download search results. It takes two downloads because the site limits how much bird data one download may contain (ask for the lot in one go and it refuses, saying the search exceeded a limit on bird data), so perching birds (Passeriformes) are downloaded separately from everything else. The website changes from time to time; if the filters no longer match the guide below, aim for the same end result: all four kingdoms, and nothing ticked under Geographical Scope or Include.",
                    GuideTitle = "How to download the two zips",
                    GuideSteps = new[] {
                        "Create a free account on iucnredlist.org and sign in. Only a signed-in user can download search results, and the finished file appears on your account page.",
                        "Open iucnredlist.org/search and clear the two default filters: under Geographical Scope remove Global, under Include remove Species. Leave both empty. Do not tick everything instead: an empty filter places no restriction, but ticking every geographic scope quietly drops the assessments that have no scope recorded (28 of them in the 2026-1 release).",
                        "Under Taxonomy, tick all four kingdoms: Animalia, Plantae, Fungi and Chromista.",
                        "Still under Taxonomy, open Animalia > Chordata > Aves and untick Passeriformes. This first download is everything except perching birds.",
                        "Press Download at the top right of the search page and choose Search Results.",
                        "Work through the three-page form: describe what you intend to use the data for, answer whether the use is academic, research or educational, and agree to the terms of use.",
                        "The download appears on your account page, marked Preparing at first. Once that clears, download the zip from there.",
                        "Run the second search: leave Geographical Scope and Include empty as before, but under Taxonomy tick only Animalia > Chordata > Aves > Passeriformes. Download it the same way.",
                        "Save both zips, still zipped, in a release folder whose name includes the release version, for example D:\\datasets\\IUCN_CVS_2026-1. You can also give each download its own subfolder, such as D:\\datasets\\IUCN_CVS_2026-1\\passerines. If a subfolder's name and the release folder's name include different release versions, `iucn import` stops before importing anything.",
                        "Keep one release per folder. The import picks up every zip anywhere below the folder, and refuses to mix two releases in one database.",
                        "Last, set [Datasets] IUCN_CVS_dir in paths.ini to that release folder. If you launch commands from these web pages rather than the command line, restart serve afterwards, because paths.ini is only read at startup.",
                    },
                },
                new FlowStep {
                    Id = "csv-import",
                    Title = "Import the IUCN CSV release",
                    Description = "Load the downloaded zips into the release's SQLite database. This is the quick route: for most workflows the CSV database is all you need.",
                    Commands = new[] { "iucn import" },
                    InputSourceIds = new[] { "iucn-csv-input" },
                    OutputSourceIds = new[] { "iucn-main" },
                    Probe = FlowStepProbes.IucnCsvImport,
                    Group = "1 · From the CSV release",
                    Note = "One run imports every zip below [Datasets] IUCN_CVS_dir, so both downloads go in together. When those zips are a newer release than the configured database holds, the import creates a new file for them (IUCN_2026-1.sqlite, alongside the old one) and prints the paths.ini line to change; the previous release is never overwritten unless you ask for that with --force --replace-release. Zips already imported are skipped on a re-run. Afterwards the Data sources tab shows the row counts, which you can compare against the result counts iucnredlist.org showed for each search.",
                },
                new FlowStep {
                    Id = "csv-repoint",
                    Title = "Point paths.ini at the new database & restart serve (manual)",
                    Description = "This step is needed when `iucn import` wrote the new release to a new file, such as IUCN_2026-1.sqlite. Open paths.ini in a text editor, set [Datastore] IUCN_sqlite_from_cvs to that file, and restart serve. Until then, every other command and every page of this web UI read the old database.",
                    Commands = Array.Empty<string>(),
                    InputSourceIds = new[] { "iucn-main" },
                    Optional = true,
                    Probe = FlowStepProbes.IucnCsvRepoint,
                    Group = "1 · From the CSV release",
                    Note = "Until serve restarts with the new setting, this step's status is amber, and the line under the step title shows the database file serve is still using and the release that file contains. To check the setting: `show-paths` prints the path of the IUCN Red List database, and the Data sources page shows which release the IUCN Red List database contains. After the restart, the Wikipedia workflows, such as \"Wikipedia reports pipeline\", read the new release.",
                },

                // ===== 2 · From the IUCN API =====
                new FlowStep {
                    Id = "api-refresh-start",
                    Title = "Start a re-import of the API data (only for a new release)",
                    Description = "Mark everything downloaded before a date to be fetched again. Skip this the first time you build the API dataset: without it, the steps below only fetch what is missing.",
                    Commands = new[] { "iucn api refresh-start" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.IucnApiRefresh,
                    Group = "2 · From the IUCN API",
                    Note = "Open Options and set --cutoff to a date just before the new release was published (UTC, for example 2026-06-16). With the default cutoff (now), the whole IUCN API cache is downloaded again. This step only stores the date: on every run, `iucn api cache-all --full` in \"Build or update the API dataset\" re-downloads everything downloaded before the stored date, so you enter the date once. A full API re-import is about 37 hours of downloading, and you can stop `iucn api cache-all --full` at any time; the next run continues with what is left. By default the API re-import also runs \"Discover extra taxa by family\" once and re-checks taxa and assessments for which the API returned HTTP 404 or 410, because a taxon missing from the previous release can be in the new one. --no-discovery turns off \"Discover extra taxa by family\", and --no-tombstones turns off the re-check.",
                },
                new FlowStep {
                    Id = "api-update",
                    Title = "Build or update the API dataset",
                    Description = "`iucn api cache-all --full` downloads species, subspecies, varieties and their assessments into the IUCN API cache, then rebuilds the IUCN API projection that `--dataset api` reads. During an API re-import it also runs \"Discover extra taxa by family\" and re-checks taxa and assessments that the API reported as gone (HTTP 404 or 410).",
                    Commands = new[] { "iucn api cache-all --full", "iucn api cache-all --full --status" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache", "iucn-api-projected" },
                    Probe = FlowStepProbes.IucnApiUpdateAll,
                    Group = "2 · From the IUCN API",
                    Note = "You can stop `iucn api cache-all --full` at any time: each phase downloads only what is missing from the IUCN API cache (during an API re-import, also what was downloaded before the cutoff date), so the next run continues where the last one stopped. Before and after downloading, the command prints a table of its phases and the number of taxa and assessments in the IUCN API cache. `iucn api cache-all --full --status` prints only that and downloads nothing. The \"Step by step\" panel below has the same work as separate commands.",
                },
                new FlowStep {
                    Id = "api-cache-species",
                    Title = "Download species listed in the CSV release",
                    Description = "Download /api/v4 taxa + assessment payloads for the species present in the imported CSV. The quickest way to seed the API cache once the CSV is imported.",
                    Commands = new[] { "iucn api cache-all" },
                    Probe = FlowStepProbes.IucnApiTaxa,
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Section = FlowSection.StepByStep,
                    Group = "From the IUCN API",
                    Note = "`iucn api cache-all` runs cache-taxa and then cache-assessments. It reads the species list from the IUCN Red List database, so do \"Import the IUCN CSV release\" first. Re-running downloads only what is not cached yet; --force-taxa and --force-assessments download everything again.",
                },
                new FlowStep {
                    Id = "api-discover-by-family",
                    Title = "Discover extra taxa by family (no CSV needed)",
                    Description = "Download every taxon listed under any family on the IUCN API that is not yet in the IUCN API cache. This finds taxa missing from the CSV release: removed or delisted taxa, reclassified taxa, and taxa with only historical assessments. `iucn api cache-all --full` runs this step only during an API re-import.",
                    Commands = new[] { "iucn api discover-by-family" },
                    Probe = FlowStepProbes.IucnApiDiscovery,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Section = FlowSection.StepByStep,
                    Group = "From the IUCN API",
                    Note = "Slower (pages ~800–1000 families on the live API). Use --dry-run to preview, --family Felidae,Canidae to target. Newly-discovered taxa still need their assessments downloaded — the next step's cache-assessments covers them.",
                },
                new FlowStep {
                    Id = "api-infraranks-cached",
                    Title = "Add subspecies & varieties of cached species",
                    Description = "This step downloads the subspecies and varieties listed in the species records in the IUCN API cache, then downloads their assessments. It works from the IUCN API cache alone and does not need the CSV import.",
                    Commands = new[] { "iucn api cache-infraranks", "iucn api cache-assessments" },
                    Probe = FlowStepProbes.IucnApiInfraranks,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Section = FlowSection.StepByStep,
                    Group = "From the IUCN API",
                    Note = "If \"Import the IUCN CSV release\" is done, run \"Add subspecies & varieties, including CSV-listed ones\" instead: it does everything this step does, and also downloads subspecies and varieties whose species is not assessed. cache-infraranks queues the assessments of each subspecies and variety it downloads, and cache-assessments downloads the queue. Re-running skips what is already cached, and subspecies and varieties the API answered \"not found\" (HTTP 404) for.",
                },
                new FlowStep {
                    Id = "api-infraranks-csv",
                    Title = "Add subspecies & varieties, including CSV-listed ones",
                    Description = "Download the subspecies and varieties listed in cached species records and in the IUCN Red List database, then their assessments. Assessed subspecies and varieties whose species is not assessed are listed only in the IUCN Red List database, so only this step downloads them.",
                    Commands = new[] { "iucn api cache-infraranks --from-csv", "iucn api cache-assessments" },
                    Probe = FlowStepProbes.IucnApiInfraranks,
                    InputSourceIds = new[] { "iucn-api-cache", "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Section = FlowSection.StepByStep,
                    Group = "From the IUCN API",
                    Note = "Run this step instead of \"Add subspecies & varieties of cached species\" once \"Import the IUCN CSV release\" is done. `iucn api cache-all --full` runs the same command, cache-infraranks --from-csv. Re-running skips subspecies and varieties already cached, and those the API answered \"not found\" (HTTP 404) for.",
                },
                new FlowStep {
                    Id = "api-project-view",
                    Title = "Project the API cache for list/chart generation",
                    Description = "Re-shape the latest cached assessments into the same CSV-compatible relational view the CSV import produces, so list/chart generation can read the API dataset via --dataset api.",
                    Commands = new[] { "iucn api project-view" },
                    Probe = FlowStepProbes.IucnApiProjection,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-projected" },
                    Section = FlowSection.StepByStep,
                    Group = "From the IUCN API",
                    Note = "Run last — after cache-all, discover-by-family, cache-infraranks and cache-assessments — so it isn't partial: project-view exits non-zero and flags the projection partial if any taxon's latest assessment isn't downloaded yet (pass --allow-partial to accept). Then generate with --dataset api.",
                },

                // ===== 3 · Compare CSV vs API =====
                new FlowStep {
                    Id = "compare-datasets",
                    Title = "Compare CSV vs API",
                    Description = "Check that the two datasets agree before choosing which to generate from. The Data sources page shows a side-by-side card (version, totals, per-category, coverage); the count-scopes audit diffs them on the command line.",
                    Commands = new[] { "iucn count-scopes --compare" },
                    InputSourceIds = new[] { "iucn-main", "iucn-api-projected" },
                    Optional = true,
                    Group = "3 · Compare the two datasets",
                    Note = "Runs `iucn count-scopes --compare` to diff the CSV and API datasets side by side — global-species (canonical) and subspecies/variety (infra) counts per taxa group. Small deltas are expected: the API omits taxa with no current assessment, and can't enumerate orphan infrataxa (assessed subspecies of unassessed species). Open the Data sources tab for the visual comparison.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-count-scopes-compare-*.md", Label = "Count-scope compare" },
                    },
                },
            },
            // No Outputs links: this flow produces databases, which the steps' Outputs lists and the
            // Data sources page already show. A link here could only open the reports folder.
        },

        // ---------------------------------------------------------------
        // Update Catalogue of Life: import a new CoL release, repoint config,
        // then refresh every local store/output that reads CoL. Written as a
        // guide for someone with little background: each step says why it's
        // needed, whether it downloads pages, and what is left stale if you
        // skip it. CoL is enrichment, not a hard dependency — most steps
        // degrade gracefully, but their *outputs* freeze on the old release
        // until re-run. Grouped: import & repoint → refresh derived data →
        // discover new matches (downloads) → regenerate outputs, plus a
        // Maintenance panel for cleanup and a full from-scratch rebuild.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "col-update",
            Title = "Update Catalogue of Life",
            Description = "Import a new Catalogue of Life (CoL) release, then rebuild the outputs that use CoL data: the common-name hub, the Red List audit site and the Wikipedia lists. Optional steps also search Wikidata and Wikipedia, using the new release's synonyms, for IUCN taxa that have no Wikidata item or Wikipedia article. One step is done by hand: edit paths.ini to point at the new CoL database, then restart serve. Each output is based on the previous CoL release until the step that builds it is run again.",
            Steps = new[] {
                // ===== 1 · Import & repoint =====
                new FlowStep {
                    Id = "col-import",
                    Title = "Import the new CoL release",
                    Description = "Load the new ColDP zip from the CoL input folder into a version-named SQLite database. The starting point — everything below reads this file.",
                    Commands = new[] { "col import" },
                    InputSourceIds = new[] { "col-input" },
                    OutputSourceIds = new[] { "col-sqlite" },
                    Group = "1 · Import & repoint",
                    Probe = FlowStepProbes.ColImport,
                    Note = "`col import` builds col_coldp_<label>.sqlite for each ColDP zip in Datasets:COL_dir, where <label> is the release alias from the zip's metadata.yaml with spaces replaced by underscores (alias \"COL26.5 XR\" gives col_coldp_COL26.5_XR.sqlite). A new release therefore gets a new database file, and the previous one stays on disk until you delete it ('Delete old CoL leftovers (manual)' under Maintenance). Each database is over 10 GB and the import takes tens of minutes. The button always asks for confirmation because --force deletes and rebuilds even a complete database; without --force, a finished database is skipped and an incomplete one is imported again.",
                },
                new FlowStep {
                    Id = "repoint-paths",
                    Title = "Repoint paths.ini & restart serve (manual)",
                    Description = "Point the config at the new file, then restart the web server so it picks up the change. No command — you edit paths.ini by hand.",
                    Commands = Array.Empty<string>(),
                    InputSourceIds = new[] { "col-sqlite" },
                    OutputSourceIds = new[] { "col-sqlite" },
                    Group = "1 · Import & repoint",
                    Probe = FlowStepProbes.ColRepoint,
                    Note = "Set [Datastore] COL_sqlite to the new col_coldp_<label>.sqlite file and [Datasets] COL_dir to the folder with the new release's zip, then restart serve. serve reads paths.ini only when it starts, so until the restart the web UI still reads the old CoL database; `show-paths` on the Run command page shows the edited values straight away. After the restart, the 'Loaded:' line at the top of this page and the 'Catalogue of Life version' card on the Data sources page show the new release, and this step's status line warns if COL_sqlite and COL_dir hold different releases.",
                },
                new FlowStep {
                    Id = "verify-col",
                    Title = "Verify the new database",
                    Description = "Confirm the new DB opens, is fully populated, and is the release you expect.",
                    Commands = new[] { "col report-nameusage-fields", "col report-subgenus-homonyms" },
                    InputSourceIds = new[] { "col-sqlite" },
                    OutputSourceIds = new[] { "reports" },
                    Optional = true,
                    Group = "1 · Import & repoint",
                    Note = "Check the 'Database:' line in the output of `col report-nameusage-fields`: if it names the new file, COL_sqlite points at the new release, and the 'Rows in table:' line should show millions of rows. `col report-subgenus-homonyms` runs a heavier query to check that the new database answers queries; its output ends with 'Potential homonyms found:' and a count. If the import did not finish, the 'Loaded:' line at the top of this page says 'import incomplete'.",
                },

                // ===== 2 · Refresh derived data =====
                new FlowStep {
                    Id = "common-names",
                    Title = "Re-aggregate common names",
                    Description = "Pull the new release's English vernacular names and synonyms into the common-name hub.",
                    Commands = new[] { "common-names aggregate", "common-names aggregate --source col --replace" },
                    InputSourceIds = new[] { "col-sqlite", "iucn-main" },
                    OutputSourceIds = new[] { "common-names" },
                    Group = "2 · Refresh derived data",
                    Probe = FlowStepProbes.ColRebuildNames,
                    Note = "Imports the new release's English vernacular names and scientific-name synonyms from Catalogue of Life into the common-name hub. It downloads nothing and is safe to run twice. By default it only adds and updates: names this release dropped stay in the hub, and the old release's Catalogue of Life ids stay attached to their species. Catalogue of Life reissues those ids between releases, so a leftover id can attach one of the new names to the wrong species. To avoid that, run it with --replace (the second command button below): that deletes the Catalogue of Life names, synonyms and ids already in the hub before importing, leaving the hub matching this release exactly. Names from IUCN, Wikidata and Wikipedia are kept either way. `common-names init` is only needed when the hub database does not exist yet; it seeds species from IUCN and caps.txt and never reads Catalogue of Life.",
                },
                new FlowStep {
                    Id = "detect-conflicts",
                    Title = "Rebuild the ambiguous-name list",
                    Description = "Record each English common name used for more than one species in the same kingdom. `wikipedia generate-lists` works out ambiguous names itself when it runs, so the lists do not depend on this step.",
                    Commands = new[] { "common-names detect-conflicts", "common-names detect-conflicts --clear-existing" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Group = "2 · Refresh derived data",
                    Probe = FlowStepProbes.CommonNameConflicts,
                    Note = "Run `common-names detect-conflicts --clear-existing` after 'Re-aggregate common names'. With --clear-existing, the command empties the ambiguous-name list, then rebuilds it from the common names now in the hub. Plain `common-names detect-conflicts` only adds entries, so after a plain `common-names aggregate` the list keeps entries for names that are no longer ambiguous. After `common-names aggregate --source col --replace`, which empties the list, both commands give the same result.",
                },
                new FlowStep {
                    Id = "redlist-audit",
                    Title = "Rebuild the Red List audit site",
                    Description = "Regenerate the audit site so its Catalogue-of-Life crosscheck page reflects the new release.",
                    Commands = new[] { "redlist audit-site" },
                    InputSourceIds = new[] { "col-sqlite", "iucn-main" },
                    OutputSourceIds = new[] { "reports" },
                    Group = "2 · Refresh derived data",
                    Probe = FlowStepProbes.ColRebuildAudit,
                    Note = "`redlist audit-site` builds its 'Catalogue of Life crosscheck' pages from the CoL database that COL_sqlite points at, so run it after the repoint; `iucn report-col-crosscheck` is not needed first. The site is written to <reports_dir>/redlist-audit-2026/ (open index.html) and names the CoL release it used under 'Catalogue of Life reference'. A run with --limit is written to <reports_dir>/redlist-audit-2026-limited/ instead, and does not mark this step as done. If COL_sqlite points at a missing file or a database with no nameusage table, the site is built without those pages, the job still succeeds, and its output says 'skipped col-crosscheck'. The 'Catalogue of Life crosscheck' pages from the earlier run are then removed from the folder.",
                },
                new FlowStep {
                    Id = "iucn-crosscheck",
                    Title = "IUCN ↔ CoL crosscheck report",
                    Description = "Optional standalone crosscheck text report (the audit site above already covers this).",
                    Commands = new[] { "iucn report-col-crosscheck" },
                    InputSourceIds = new[] { "iucn-main", "col-sqlite" },
                    OutputSourceIds = new[] { "reports" },
                    Optional = true,
                    Group = "2 · Refresh derived data",
                    Note = "`iucn report-col-crosscheck` writes iucn-col-crosscheck-*.txt to the reports folder, one file per run. For each IUCN taxon, the file reports whether the name is in CoL, whether CoL treats the name as accepted or as a synonym, and whether the authority and the classification (kingdom to genus) match. The header gives the path of the CoL database used. A jump in authority mismatches after a CoL update can mean the release renamed the authorship column: the tool then finds no authorship column and treats every CoL authority as empty.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-col-crosscheck-*.txt", Label = "Crosscheck report" },
                    },
                },

                // ===== 3 · Discover new matches (downloads) =====
                new FlowStep {
                    Id = "wikidata-discover",
                    Title = "Search Wikidata for the taxa still without an item",
                    Description = "Search Wikidata for each IUCN taxon that still has no Wikidata item, using its scientific name and now the new release's synonyms. Existing matches are never changed.",
                    Commands = new[] { "wikidata backfill-iucn", "wikidata cache-entities" },
                    InputSourceIds = new[] { "col-sqlite", "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikidataSearch,
                    Group = "3 \u00b7 Search for new matches (downloads)",
                    Note = "After a Catalogue of Life update, open Options next to `wikidata backfill-iucn`, tick --retry-missing and click Run, so taxa that had no match last time are searched again with the new CoL synonyms. Without --retry-missing, `wikidata backfill-iucn` searches only taxa never searched before. Then run `wikidata cache-entities` to download the matched Wikidata items. Each match listed in the job output links a taxon that had no Wikidata item. This step downloads from Wikidata.",
                },
                new FlowStep {
                    Id = "wikipedia-discover",
                    Title = "Look for articles for the taxa that have none",
                    Description = "Run the three commands in order: the first queues possible article titles (now including the new CoL release's synonyms), the second downloads those pages, and the third matches taxa to the downloaded pages.",
                    Commands = new[] { "wikipedia match-taxa", "wikipedia fetch-pages --awaited-only --newest-first", "wikipedia match-taxa" },
                    InputSourceIds = new[] { "col-sqlite", "iucn-main", "wikidata-cache", "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikipediaFetchAwaited,
                    Group = "3 \u00b7 Search for new matches (downloads)",
                    Note = "Run 'Search Wikidata for the taxa still without an item' first, because `wikipedia match-taxa` also finds article titles through Wikidata sitelinks. The first `wikipedia match-taxa` matches taxa to pages already downloaded and queues other possible article titles. `wikipedia fetch-pages --awaited-only --newest-first` downloads the queued titles that could be the article of a taxon with no article yet. The second `wikipedia match-taxa` records each match, or that the taxon has no article. Taxa that already have a matched article are skipped unless you add --reprocess-matched.",
                },

                // ===== 4 · Regenerate outputs =====
                new FlowStep {
                    Id = "generate-lists",
                    Title = "Regenerate Wikipedia lists",
                    Description = "Rebuild the .wikitext lists with the re-aggregated common names, the article links found in group 3, and the new CoL release's suborders, superfamilies, subfamilies, tribes and subgenera.",
                    Commands = new[] { "wikipedia generate-lists" },
                    InputSourceIds = new[] { "iucn-main", "col-sqlite", "common-names", "wikipedia-cache" },
                    OutputSourceIds = Array.Empty<string>(),
                    Group = "4 · Regenerate outputs",
                    Probe = FlowStepProbes.ColRebuildLists,
                    Note = "Run 'Re-aggregate common names' and the steps under '3 · Search for new matches (downloads)' first, so the lists get the new common names and article links. Then run `wikipedia generate-lists` for every list, without --list, --status or --taxa-group: this step's status line checks only the newest .wikitext file, so it shows done even when some lists still use the previous CoL release. The first run after a CoL update is slower while it builds a new cache file beside the CoL database. `wikipedia generate-charts` does not use CoL, so charts need no re-run.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.wikitext", Label = "Lists" },
                    },
                },

                // -------- Maintenance (only when needed) --------
                new FlowStep {
                    Id = "coverage-check",
                    Title = "Check Wikidata coverage",
                    Description = "Read-only summary of how many IUCN taxa now have a matching Wikidata entity (reads CoL live).",
                    Commands = new[] { "wikidata report-coverage" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Note = "`wikidata report-coverage` counts the IUCN taxa matched to a cached Wikidata item, by match method: P627, scientific name, or synonym (IUCN synonyms and the CoL synonyms in the database that COL_sqlite points at). To see what the new CoL release added, run it before and after the group 3 search steps and compare the counts. This step downloads nothing.",
                },
                new FlowStep {
                    Id = "cleanup-orphans",
                    Probe = FlowStepProbes.ColCleanup,
                    Title = "Delete old CoL leftovers (manual)",
                    Description = "Remove the previous release's database and its orphaned enrich-cache to reclaim disk.",
                    Commands = Array.Empty<string>(),
                    InputSourceIds = new[] { "col-sqlite" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Note = "Manual — no command. After the repoint, the old col_coldp_<oldlabel>.sqlite and its sidecar col_coldp_<oldlabel>.sqlite.enrich-cache.sqlite (plus any -wal/-shm companions) are left on disk and never read again. Each CoL DB is multi-GB, so deleting the previous release reclaims significant space. The new enrich-cache rebuilds itself automatically on the next generate-lists — there is no cache to clear by hand.",
                },
                new FlowStep {
                    Id = "full-rebuild-names",
                    Title = "Rebuild the common-name hub from scratch (manual + command)",
                    Description = "Delete the common-name database and build it again from every source.",
                    Commands = new[] { "common-names init", "common-names aggregate" },
                    InputSourceIds = new[] { "iucn-main", "col-sqlite" },
                    OutputSourceIds = new[] { "common-names" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Note = "Not part of a normal release update: `common-names aggregate --source col --replace` in the Re-aggregate step above already makes the hub match the new Catalogue of Life release. Use this only when the hub itself looks wrong (for example after an interrupted import, or when several sources need clearing at once): delete the common-names SQLite file by hand, then run init, then aggregate. Rebuilding takes far longer than a re-aggregate, and any hand edits made to the hub are lost."
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "Red List audit site", Root = "reports", Path = "redlist-audit-2026", Kind = "directory",
                    Description = "The rebuilt static audit site (open index.html); its CoL-crosscheck page reflects the new release." },
                new FlowResource { Label = "Wikipedia lists", Root = "wikipedia-output", Path = "", Kind = "directory",
                    Description = ".wikitext list files from `wikipedia generate-lists`. After 'Regenerate Wikipedia lists', sections by suborder, superfamily, subfamily, tribe or subgenus use the new CoL release." },
                new FlowResource { Label = "Reports output", Root = "reports", Path = "", Kind = "directory",
                    Description = "Text files from `iucn report-col-crosscheck` and `col report-nameusage-fields`." },
            },
        },

        // ---------------------------------------------------------------
        // Wiki Reports: the full pipeline that produces Wikipedia output.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-reports",
            Title = "Wikipedia reports pipeline",
            Description = "Import the source data (group 1), run `wikipedia update` (group 2), build the common names (group 3), then generate the Wikipedia lists and charts (group 4). `wikipedia update` runs the Wikidata and Wikipedia download, search, queue and match commands in priority order, skipping any step with nothing to do, and can be stopped and re-run at any time without losing finished work. To run its commands one at a time, open \"Step by step\"; the lowest-priority work, \"Re-download old copies\", is in \"Maintenance\".",
            Steps = new[] {
                // -------- Pipeline (core path) --------
                new FlowStep {
                    Id = "iucn-import",
                    Title = "Import IUCN Red List CSVs",
                    Description = "Load the IUCN CSV release into the local SQLite store. The base dataset every other step joins against.",
                    Commands = new[] { "iucn import" },
                    InputSourceIds = new[] { "iucn-csv-input" },
                    OutputSourceIds = new[] { "iucn-main" },
                    Probe = FlowStepProbes.IucnCsvImport,
                    Group = "1 \u00b7 Source data",
                    Note = "Each IUCN release goes in its own database file, IUCN_<version>.sqlite. When the database in paths.ini holds a different release, `iucn import` creates the new file beside it and prints the paths.ini line to change; restart serve after changing it. The \"Import IUCN data\" workflow (first tab) covers the import in more detail, and the IUCN API route.",
                },
                new FlowStep {
                    Id = "col-import",
                    Title = "Import Catalogue of Life",
                    Description = "Import COL ColDP archives for cross-check and taxonomy enrichment. Optional but improves list quality.",
                    Commands = new[] { "col import" },
                    InputSourceIds = new[] { "col-input" },
                    OutputSourceIds = new[] { "col-sqlite" },
                    Optional = true,
                    Probe = FlowStepProbes.ColImport,
                    Group = "1 \u00b7 Source data",
                },
                new FlowStep {
                    Id = "wiki-update",
                    Title = "Update the Wikidata and Wikipedia caches",
                    Description = "`wikipedia update` finds Wikidata items with P627 or P141, downloads them, searches Wikidata for taxa still without an item, queues Wikipedia titles, matches taxa to articles, downloads the candidate pages of taxa with no matched article, and matches those taxa again. It skips steps with nothing to do.",
                    Commands = new[] { "wikipedia update", "wikipedia update --status", "wikipedia update --include-rest" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "wikidata-cache", "wikipedia-cache" },
                    Probe = FlowStepProbes.WikiUpdateAll,
                    Group = "2 · Update the caches",
                    Note = "Re-running is safe: finished downloads are kept, and the next run continues where the last one stopped. The first button, `wikipedia update`, has no download cap unless you set --limit <N> under Options; --until-done repeats the steps until nothing is left or a round makes no progress. The second, `wikipedia update --status`, shows what each step would do and downloads nothing. The third, `wikipedia update --include-rest`, also retries failed downloads and downloads the low-priority titles (higher taxa, synonyms and redirect targets). \"Step by step\" has the same steps as separate commands.",
                },
                new FlowStep {
                    Id = "wikidata-seed",
                    Title = "Find Wikidata items for IUCN taxa",
                    Description = "Add every Wikidata item with P627 (IUCN taxon ID) or P141 (IUCN conservation status) to the Wikidata download queue. The next step, \"Download the queued Wikidata items\", downloads the item data.",
                    Commands = new[] { "wikidata seed-taxa", "wikidata cache-all" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = FlowStepProbes.WikidataSweep,
                    Section = FlowSection.StepByStep,
                    Group = "Wikidata",
                    Note = "The first button, `wikidata seed-taxa`, reads the Wikidata items with P627 or P141 in Q-number order and queues the ones not yet cached. It is much faster than the one-taxon-at-a-time search in \"Search for the taxa still without an item\". Each run continues from where the last one stopped, so it misses items that gained P627 or P141 after the sweep passed their Q-number; `wikidata seed-taxa --reset-cursor` (under \"Re-download old copies\" in Maintenance) starts again from the first Q-number. The second button, `wikidata cache-all`, runs the sweep and then downloads the queue.",
                },
                new FlowStep {
                    Id = "wikidata-download",
                    Title = "Download the queued Wikidata items",
                    Description = "Fetch the entity data for every queued id, and index the scientific names inside it.",
                    Commands = new[] { "wikidata cache-entities" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = FlowStepProbes.WikidataDownload,
                    Section = FlowSection.StepByStep,
                    Group = "Wikidata",
                    Note = "Re-running is safe: items already downloaded are skipped. As it downloads each item, `wikidata cache-entities` adds the item's scientific names to the name index and extracts its P141 statements, so \"Rebuild Wikidata lookup indexes\" (Maintenance) is normally not needed. --failed-only retries only failed downloads; --refresh-only with --max-age-hours <N> re-downloads items older than N hours and leaves never-downloaded items in the queue.",
                },
                new FlowStep {
                    Id = "wikidata-search",
                    Title = "Search for the taxa still without an item",
                    Description = "For IUCN taxa the sweep did not cover, search Wikidata by scientific name and synonyms, then download what it finds.",
                    Commands = new[] { "wikidata backfill-iucn", "wikidata cache-entities" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikidataSearch,
                    Section = FlowSection.StepByStep,
                    Group = "Wikidata",
                    Note = "This search checks one taxon at a time, so it is much slower than \"Find Wikidata items for IUCN taxa\"; run that step first. It searches only taxa with no linked Wikidata item, by scientific name and then by synonyms from the IUCN API cache and the Catalogue of Life database, so every match it reports is a new link. Taxa searched before without a match are skipped: --retry-missing searches them again (worth doing after a Catalogue of Life update adds synonyms), and --retry-missing-after <DAYS> only those last searched more than DAYS days ago. Items found go into the download queue; the second button, `wikidata cache-entities`, downloads them.",
                },
                new FlowStep {
                    Id = "wikipedia-queue",
                    Title = "Queue Wikipedia titles",
                    Description = "Add candidate page titles to the download queue: the English Wikipedia links on cached Wikidata items, plus class, order and family names from IUCN.",
                    Commands = new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Probe = FlowStepProbes.WikipediaQueue,
                    Section = FlowSection.StepByStep,
                    Group = "Wikipedia",
                    Note = "Both commands only add titles; nothing is downloaded here. Re-running skips titles already queued unless you pass --force-refresh or --refresh-days, and a refreshed title keeps its existing match and cached page until the new copy arrives. Run the Wikidata steps first, because the article titles come from the cached items.",
                },
                new FlowStep {
                    Id = "wikipedia-match",
                    Title = "Match taxa to articles",
                    Description = "Work out which article belongs to each IUCN taxon, and queue candidate titles for the taxa that have none.",
                    Commands = new[] { "wikipedia match-taxa" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache", "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Probe = FlowStepProbes.WikipediaMatch,
                    Section = FlowSection.StepByStep,
                    Group = "Wikipedia",
                    Note = "Match, download, match again. The first run picks candidate titles and queues them without making any network calls; the download step below fetches them; the second run reads the fetched pages and settles the matches. Taxa already matched are left alone unless you pass --reprocess-matched. Candidates come from Wikidata site links, IUCN and Catalogue of Life synonyms, scientific names in cached taxoboxes, and redirects.",
                },
                new FlowStep {
                    Id = "wikipedia-titles-dump",
                    Title = "Import the all-titles dump",
                    Description = "Download English Wikipedia's list of every article and redirect title (published about twice a month) as a local check for which queued titles exist at all.",
                    Commands = new[] { "wikipedia titles-dump" },
                    InputSourceIds = Array.Empty<string>(),
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikipediaTitlesDump,
                    Section = FlowSection.StepByStep,
                    Group = "Wikipedia",
                    Note = "One download (about 90 MB) instead of one API call per queued title. A queued title absent from the list is almost certainly a redlink, so the download steps below can take the real pages first: add --exists-first to fetch-pages. The list is titles only — it says a page exists, not what it holds, and it includes redirects — so it changes no matches and downloads no pages. Re-running checks for a newer dump and re-imports only when there is one; an interrupted download resumes where it stopped.",
                },
                new FlowStep {
                    Id = "wikipedia-fetch-awaited",
                    Title = "Download the pages taxa are waiting on",
                    Description = "Download the queued Wikipedia pages for every candidate title of each IUCN taxon with no matched article yet. The most recently queued titles are downloaded first.",
                    Commands = new[] { "wikipedia fetch-pages --awaited-only --newest-first", "wikipedia match-taxa" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Probe = FlowStepProbes.WikipediaFetchAwaited,
                    Section = FlowSection.StepByStep,
                    Group = "Wikipedia",
                    Note = "The queue holds far more titles than the taxa themselves need, because higher taxa, synonyms and redirect targets are queued too. --awaited-only narrows it to pages a taxon is waiting on, and --newest-first takes the titles queued most recently, which after a release update are the new taxa. It downloads only what has not been downloaded, so it can be stopped and resumed; add --limit to stop after a set number. Run match-taxa again afterwards (the second button) to settle the matches for the pages that arrived.",
                },
                new FlowStep {
                    Id = "wikipedia-fetch-rest",
                    Title = "Download the rest of the queue",
                    Description = "Fetch the remaining queued titles: higher taxa, synonyms and redirect targets.",
                    Commands = new[] { "wikipedia fetch-pages", "wikipedia fetch-pages --exists-first" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikipediaFetchRest,
                    Section = FlowSection.StepByStep,
                    Group = "Wikipedia",
                    Note = "The titles left in the Wikipedia titles queue after \"Download the pages taxa are waiting on\" are higher taxa, synonyms and redirect targets. They help resolve redirects and synonyms, but the lists do not need them. There can be hundreds of thousands, which take days to download, so use --limit <N> to spread the work over several sessions; each run continues where the last one stopped. With the all-titles dump imported, the second button, `wikipedia fetch-pages --exists-first`, downloads titles that exist on Wikipedia first and likely redlinks last. If you run this step's buttons directly, run \"Remove titles that cannot be articles\" (Maintenance) first; `wikipedia update` does that itself.",
                },
                new FlowStep {
                    Id = "common-names",
                    Title = "Aggregate common names",
                    Description = "Build the unified common-name store across IUCN, Wikidata, Wikipedia, and COL.",
                    Commands = new[] { "common-names init", "common-names aggregate" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache", "wikipedia-cache", "col-sqlite" },
                    OutputSourceIds = new[] { "common-names" },
                    Group = "3 \u00b7 Common names",
                    Note = "`common-names init` (first button) adds every taxon in the IUCN Red List database to the common names store and imports the capitalization rules from rules/caps.txt. `common-names aggregate` (second button) then reads common names from the IUCN API cache, the Wikidata cache, the Wikipedia cache and the Catalogue of Life database. Neither command downloads anything. A re-run adds and updates names but never removes one; after a source's cache or database is refreshed, `common-names aggregate --source wikidata --replace` (or wikipedia, col or iucn) replaces that source's names and deletes every recorded ambiguous name, so run \"Find ambiguous common names\" afterwards. `common-names sources` lists when each source was last aggregated and replaced.",
                },
                new FlowStep {
                    Id = "detect-conflicts",
                    Title = "Find ambiguous common names",
                    Description = "Find English common names used for more than one valid taxon in the same kingdom, and record them in the common names store.",
                    Commands = new[] { "common-names detect-conflicts", "common-names detect-conflicts --clear-existing" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Probe = FlowStepProbes.CommonNameConflicts,
                    Group = "3 \u00b7 Common names",
                    Note = "Run this step after \"Aggregate common names\": aggregating can add ambiguous names, and --replace deletes the recorded ones. The recorded names are counted in this step's status and as \"conflicts\" for the Common names store on the Data sources page. `wikipedia generate-lists` works out ambiguous names itself: it uses the taxon's next-best common name, or its scientific name when every common name is ambiguous. Without --clear-existing (second button), a name recorded in an earlier run stays recorded even if it is no longer ambiguous.",
                },
                new FlowStep {
                    Id = "refresh-caps",
                    Title = "Refresh capitalization rules (after editing caps.txt)",
                    Description = "Re-import rules/caps.txt into the common-names store so capitalization edits (including multi-word phrase rules like \"guinea pig\") take effect — without rebuilding the whole store.",
                    Commands = new[] { "common-names init --skip-taxa" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Optional = true,
                    Group = "3 \u00b7 Common names",
                    Note = "Only needed when you've edited rules/caps.txt since the last full \"Aggregate common names\" run — `--skip-taxa` reimports just the caps rules (fast, idempotent), because the generator reads them from the common-names DB, not the file. The other rule files — taxon-rules.yml, rules/rules-list.txt, and the list/preset/group YAML — are read directly at generation time, so they need no import: edit them and re-run Generate.",
                },
                new FlowStep {
                    Id = "generate",
                    Title = "Generate Wikipedia lists + charts",
                    Description = "Apply YAML rules and Mustache templates to produce final wikitext output.",
                    Commands = new[] { "wikipedia generate-lists", "wikipedia generate-charts" },
                    InputSourceIds = new[] { "iucn-main", "wikipedia-cache", "common-names", "col-sqlite" },
                    OutputSourceIds = Array.Empty<string>(),
                    Group = "4 \u00b7 Generate",
                    Note = "Uses rules/wikipedia-lists.yml, rules/chart-groups.yml, rules/rules-list.txt, and templates under rules/wikipedia/templates/. These (and taxon-rules.yml) are read fresh each run — no import step. Edited caps.txt? Run \"Refresh capitalization rules\" above first.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.wikitext", Label = "Lists" },
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.tab",      Label = "Chart data" },
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.chart",    Label = "Chart def" },
                    },
                },

                // -------- Maintenance (only when needed) --------
                new FlowStep {
                    Id = "wiki-retry-failed",
                    Title = "Retry failed downloads",
                    Description = "Try again to download the Wikipedia pages and Wikidata items whose last download failed.",
                    Commands = new[] { "wikipedia fetch-pages --failed-only", "wikidata cache-entities --failed-only" },
                    InputSourceIds = new[] { "wikipedia-cache", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache", "wikidata-cache" },
                    Section = FlowSection.Maintenance,
                    Probe = FlowStepProbes.WikiRetryFailed,
                    Note = "A failed download is retried after everything never tried, so with a queue this size an ordinary fetch would not reach one for days. --failed-only goes straight to them, in both caches. A page recorded as missing is a different thing: English Wikipedia has no article under that title, and re-running will not change that.",
                },
                new FlowStep {
                    Id = "wikipedia-prune-queue",
                    Title = "Remove titles that cannot be articles",
                    Description = "Remove queued Wikipedia titles that include a taxonomic authority or a nomenclatural note, such as `Eumeces schneideri (Daudin, 1802) [orth. error]`. No Wikipedia article has a title like that.",
                    Commands = new[] { "wikipedia prune-queue", "wikipedia prune-queue --apply" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Note = "`wikipedia update` already runs `wikipedia prune-queue --apply` before it matches taxa to articles. Run this step on its own before running `wikipedia fetch-pages` directly: the first button reports how many titles it would remove, with examples, and the second removes them. Older versions of `wikipedia match-taxa` queued IUCN synonyms with the authority and note attached (73,144 of 190,212 queued titles in August 2026). Downloaded pages and existing matches are unchanged, and the next `wikipedia match-taxa` run tries again for any taxon that was waiting on a removed title.",
                },
                new FlowStep {
                    Id = "wiki-refresh",
                    Title = "Re-download old copies",
                    Description = "Re-download Wikipedia pages and Wikidata items that were downloaded long ago. This is the lowest-priority step: lists can be generated from the old copies.",
                    Commands = new[] {
                        "wikipedia fetch-pages --refresh-only --refresh-days 365",
                        "wikidata cache-entities --refresh-only --max-age-hours 8760",
                        "wikidata seed-taxa --reset-cursor",
                    },
                    InputSourceIds = new[] { "wikipedia-cache", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache", "wikidata-cache" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Probe = FlowStepProbes.WikiRefresh,
                    Note = "Re-downloading every cached Wikipedia page and Wikidata item takes days, so run this step only when `wikipedia update --include-rest` has nothing left to do. The first two buttons re-download only cached copies older than the age in their options (--refresh-days, --max-age-hours); --refresh-only keeps them from also downloading queued titles and items that were never downloaded. The third button, `wikidata seed-taxa --reset-cursor`, restarts the sweep in \"Find Wikidata items for IUCN taxa\" from the first Q-number, to find items that gained P627 or P141 after the sweep had passed them.",
                },
                new FlowStep {
                    Id = "wikidata-rebuild-indexes",
                    Title = "Rebuild Wikidata lookup indexes",
                    Description = "Recompute the normalised taxon-name index from cached entity JSON. The cache-entities command builds this index automatically during download, so only run this when the index is suspected stale.",
                    Commands = new[] { "wikidata rebuild-indexes" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Section = FlowSection.Maintenance,
                    Note = "Without --force, `wikidata rebuild-indexes` adds missing entries to the name index; with --force, it deletes the name index and rebuilds it from the downloaded items. --include-p141 also extracts P141 statements and their references, but only if none have been extracted yet; `wikidata cache-entities` already extracts them as it downloads, so on most caches --include-p141 needs --force, which re-extracts them all.",
                },
                new FlowStep {
                    Id = "wikidata-reset",
                    Title = "Reset Wikidata cache",
                    Description = "Delete every downloaded Wikidata entity payload while keeping the seed queue intact. Use only if you want to redo entity downloads from scratch.",
                    Commands = new[] { "wikidata reset-cache" },
                    InputSourceIds = Array.Empty<string>(),
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Section = FlowSection.Maintenance,
                },
            },
            Templates = new[] {
                new FlowResource { Label = "Lists config",   Root = "rules", Path = "wikipedia-lists.yml",  Kind = "yaml" },
                new FlowResource { Label = "List presets",   Root = "rules", Path = "list-presets.yml",      Kind = "yaml" },
                new FlowResource { Label = "Taxa groups",    Root = "rules", Path = "taxa-groups.yml",       Kind = "yaml" },
                new FlowResource { Label = "Chart groups",   Root = "rules", Path = "chart-groups.yml",      Kind = "yaml" },
                new FlowResource { Label = "Taxon rules",    Root = "rules", Path = "taxon-rules.yml",       Kind = "yaml" },
                new FlowResource { Label = "Rule list (legacy)", Root = "rules", Path = "rules-list.txt",   Kind = "template" },
                new FlowResource { Label = "Caps rules",     Root = "rules", Path = "caps.txt",              Kind = "template" },
                new FlowResource { Label = "Templates dir",  Root = "rules", Path = "wikipedia/templates",   Kind = "directory" },
            },
            Outputs = new[] {
                new FlowResource { Label = "Wikipedia output", Root = "wikipedia-output", Path = "",  Kind = "directory",
                    Description = "Generated wikitext lists and chart files." },
            },
        },

        // ---------------------------------------------------------------
        // Australia (SPRAT): a self-contained pipeline — import the EPBC
        // report CSV, then generate the "rare and threatened <group> of Australia"
        // lists. Independent of the IUCN dataset (SPRAT carries its own IUCN
        // status column); the common-names hub is an optional enrichment.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "sprat-australia",
            Title = "Australian threatened-species lists (SPRAT)",
            Description = "Import the Australian Government's SPRAT (Species Profile and Threats Database) report and generate \"List of rare and threatened <group> of Australia\" wikitext pages. Membership spans the EPBC Act, the IUCN Red List, and the eight state/territory acts, and every entry shows its status under each system. A self-contained pipeline — it does not need the IUCN import.",
            Steps = new[] {
                new FlowStep {
                    Id = "sprat-import",
                    Title = "Import the SPRAT report CSV",
                    Description = "Load the SPRAT \"Select All\" report CSV (EPBC + state/territory + IUCN statuses, taxonomy, presence) into a local SQLite database.",
                    Commands = new[] { "sprat import" },
                    InputSourceIds = new[] { "sprat-input" },
                    OutputSourceIds = new[] { "sprat-sqlite" },
                    Note = "Download the SPRAT report CSV by hand from environment.gov.au/sprat-public (choose Select All for every field) and set Datasets:SPRAT_csv in paths.ini to its path. `sprat import` skips a database that already holds a finished import; to import a new report, tick --force under Options, which rebuilds the SPRAT (EPBC) database from the new CSV.",
                },
                new FlowStep {
                    Id = "sprat-generate",
                    Title = "Generate the Australia lists",
                    Description = "Produce one \"List of rare and threatened <group> of Australia\" wikitext page per major taxonomic group (mammals, birds, reptiles, amphibians, fish, invertebrates; dicots, monocots, ferns/conifers/allies).",
                    Commands = new[] { "sprat generate-lists" },
                    InputSourceIds = new[] { "sprat-sqlite", "common-names", "wikipedia-cache" },
                    OutputSourceIds = Array.Empty<string>(),
                    Note = "Only the SPRAT (EPBC) database is required. With the Common names store, entries use the store's common names, capitalized by the rules in rules/caps.txt, and link to the taxon's Wikipedia article when the store has its title; other entries link to the scientific name, which may be a redlink. With the IUCN Red List database, each taxon found in it gets an {{IUCN status}} template that cites its Global assessment; other taxa show only the IUCN category listed in SPRAT. The lists are written to australia/ inside Datastore:wikipedia_output_dir.",
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "Australia lists", Root = "wikipedia-output", Path = "australia", Kind = "directory",
                    Description = "Generated \"List of rare and threatened <group> of Australia\" wikitext files." },
            },
        },

        // ---------------------------------------------------------------
        // Wikidata IUCN statuses: bring P141 on Wikidata taxon items up to the
        // latest IUCN release, citing the release and each assessment. Dry run
        // only for now; the Wikidata-side steps (coordination, the release item,
        // proposals, the bot request) are done by hand on Wikidata.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wikidata-iucn-status",
            Title = "Update IUCN statuses on Wikidata",
            Description = "Plan updates to the IUCN conservation status (P141) of Wikidata species and subspecies items from the latest Red List release, each status cited to the release and to its own assessment. Dry run only: nothing on this page edits Wikidata, and the steps on Wikidata itself are done by hand.",
            Steps = new[] {
                // ===== 1 · Local data =====
                new FlowStep {
                    Id = "iucn-api-dataset",
                    Title = "Build or update the IUCN API dataset",
                    Description = "The dry run reads each taxon's latest global assessment, with its citation and assessors, from the IUCN API cache. The same step as in the Import IUCN data workflow.",
                    Commands = new[] { "iucn api cache-all --full", "iucn api cache-all --full --status" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Probe = FlowStepProbes.IucnApiUpdateAll,
                    Group = "1 · Local data",
                },
                new FlowStep {
                    Id = "wikidata-links",
                    Title = "Update the Wikidata and Wikipedia caches",
                    Description = "Finds the Wikidata item for each IUCN taxon: by the IUCN taxon id (P627) already on the item, or by searching for its name. The same step as in the Wikipedia reports pipeline.",
                    Commands = new[] { "wikipedia update", "wikipedia update --status" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = FlowStepProbes.WikiUpdateAll,
                    Group = "1 · Local data",
                },
                new FlowStep {
                    Id = "wikidata-refresh-items",
                    Title = "Download fresh copies of the linked items",
                    Description = "Re-download the Wikidata items last downloaded more than 30 days ago, so the plan is built on each item's current revision.",
                    Commands = new[] { "wikidata cache-entities --refresh-only --max-age-hours 720" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = WikidataIucnProbes.ItemsFresh,
                    Group = "1 · Local data",
                    Note = "This step's status line comes from the last dry run (3 · Dry run), because only a dry run reads the linked items, and a dry run with --limit reads only some of them. The line stays empty until the first dry run. Each edit is sent with the item revision it was planned on, and Wikidata refuses the edit if the item has changed since.",
                },
                new FlowStep {
                    Id = "wikidata-assessment-items",
                    Title = "Look up assessment items already on Wikidata",
                    Description = "Finds the items Wikidata already has for individual IUCN assessments, so the plan cites them instead of proposing duplicates.",
                    Commands = new[] { "wikidata iucn-assessment-items", "wikidata iucn-assessment-items --status" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    Probe = WikidataIucnProbes.AssessmentItems,
                    Group = "1 · Local data",
                },

                // ===== 2 · On Wikidata, by hand =====
                new FlowStep {
                    Id = "wikidata-coordinate",
                    Title = "Contact the IUCN Updater Bot operator (manual)",
                    Description = "Someone else is building a bot for the same job. Talk to them before asking for approval of a second one.",
                    Group = "2 · On Wikidata, by hand",
                    Note = "Nikola Tulechki announced IUCN Updater Bot on Property talk:P141 on 20 July 2026; the account was registered on 4 August 2026 and had no bot approval when checked on 13 September 2026.",
                    GuideTitle = "What to raise",
                    GuideSteps = new[] {
                        "Post on Property talk:P141 or on the operator's talk page.",
                        "Offer the dry run's report and its confidence tiers.",
                        "Ask which rank convention they plan for a changed status: a new preferred-rank statement with the old one kept at normal rank (MatSuBot, 2025), or the value replaced in place (SuccuBot, 2013 to 2023).",
                        "Ask whether they plan to cite each individual assessment as well as the release.",
                    },
                },
                new FlowStep {
                    Id = "wikidata-edition-item",
                    Title = "Create the item for the 2026.1 release (manual)",
                    Description = "Every status edit cites the release through its item. Wikidata has release items up to 2025.2 (Q136547248) and none for 2026.1 yet.",
                    Probe = WikidataIucnProbes.EditionItem,
                    Group = "2 · On Wikidata, by hand",
                    Note = "Once the item exists, put its Q-id in edition_item in rules/wikidata/iucn-status.yml and run the dry run again.",
                    GuideTitle = "Statements to copy from the 2025.2 item (Q136547248)",
                    GuideSteps = new[] {
                        "label and title (P1476): The IUCN Red List of Threatened Species 2026.1",
                        "instance of (P31): version, edition or translation (Q3331189)",
                        "edition or translation of (P629): IUCN Red List (Q32059)",
                        "publication date (P577): the release date",
                        "follows (P155): Q136547248, and add followed by (P156) = the new item to Q136547248",
                        "language of work or name (P407): English (Q1860)",
                    },
                },
                new FlowStep {
                    Id = "wikidata-assessment-model",
                    Title = "Propose how assessment items are modelled (manual)",
                    Description = "Citing each assessment means creating about 170,000 items. Agree their class, label and statements with WikiProject Taxonomy first.",
                    Group = "2 · On Wikidata, by hand",
                    Note = "The ~6,600 assessment items already on Wikidata are mostly scholarly-article items labelled \"Species: Authors\", with the authors only in the label. The model this dry run proposes is assessment_item in rules/wikidata/iucn-status.yml; the dry run writes sample items built from it (the Sample assessment items file).",
                },
                new FlowStep {
                    Id = "wikidata-assessment-id-property",
                    Title = "Propose a property for the IUCN assessment id (manual)",
                    Description = "Wikidata has a property for the IUCN taxon id (P627) but none for the assessment id. Until there is one, each planned P141 statement cites the release with a reference that also gives the assessment page URL as reference URL (P854), and that URL contains the assessment id.",
                    Optional = true,
                    Group = "2 · On Wikidata, by hand",
                },

                // ===== 3 · Dry run =====
                new FlowStep {
                    Id = "wikidata-iucn-plan",
                    Title = "Plan the status updates (dry run)",
                    Description = "Match each assessed taxon to its item, rate how sure the match is (tiers A to D), and plan the changes in both rank variants. Writes a report, a CSV of every pair, sample edits and sample assessment items.",
                    Commands = new[] { "wikidata iucn-status-plan", "wikidata iucn-status-plan --limit 2000" },
                    InputSourceIds = new[] { "iucn-api-cache", "wikidata-cache" },
                    Probe = WikidataIucnProbes.Plan,
                    Group = "3 · Dry run",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-iucn-status-*.md", Label = "Report" },
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-iucn-status-*.csv", Label = "Every pair (CSV)" },
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-iucn-status-*-sample-edits.jsonl", Label = "Sample edits" },
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-iucn-status-*-sample-assessment-items.jsonl", Label = "Sample assessment items" },
                    },
                    Note = "Tiers A and B would be edited. Tiers C and D need a person to confirm the match first. The second button stops after 2,000 linked items for a quick look; the stored plan and report are then partial.",
                },
                new FlowStep {
                    Id = "wikidata-iucn-review",
                    Title = "Review the uncertain matches (manual)",
                    Description = "Check the tier C and D pairs in the CSV: items found by name search, synonym or label, and conflicting or duplicated IUCN ids. There is no review tool yet, and nothing records review decisions yet.",
                    Group = "3 · Dry run",
                },

                // ===== 4 · Editing (not built) =====
                new FlowStep {
                    Id = "wikidata-bot-request",
                    Title = "Request bot approval (manual)",
                    Description = "Open a request on Wikidata:Requests for permissions/Bot before any test edits.",
                    Group = "4 · Editing (not built yet)",
                    Note = "Raise the IUCN Red List terms of use in the bot request. It has been an open question since 2013 whether those terms allow Red List data to be added to Wikidata under CC0.",
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "Dry run settings", Root = "rules", Path = "wikidata/iucn-status.yml", Kind = "yaml",
                    Description = "The release item, the rank variants to plan, and how a new assessment item is modelled." },
            },
        },

        // ---------------------------------------------------------------
        // Wiki/Wikidata Quality: curated grouping of report commands that
        // surface coverage gaps, freshness, sitelink mismatches.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-quality",
            Title = "Wikipedia / Wikidata quality",
            Description = "These read-only reports show which IUCN taxa have a Wikidata item or a Wikipedia article, whether P141 statements on Wikidata match the current Red List category, and which enwiki sitelinks point to a redirect, a disambiguation page or an article about a different taxon.",
            Steps = new[] {
                new FlowStep {
                    Id = "cache-status",
                    Title = "Inspect Wikipedia cache",
                    Description = "High-level row counts, queue depth and failed pages from the local Wikipedia cache.",
                    Commands = new[] { "wikipedia cache-status" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                },
                new FlowStep {
                    Id = "coverage",
                    Title = "Wikidata coverage summary",
                    Description = "How many IUCN taxa have a matching cached Wikidata entity.",
                    Commands = new[] { "wikidata report-coverage" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                },
                new FlowStep {
                    Id = "coverage-details",
                    Title = "Wikidata coverage details",
                    Description = "Per-taxon list of synonym-only matches and unmatched taxa grouped by taxonomy.",
                    Commands = new[] { "wikidata report-coverage-details" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-coverage-synonyms-*.md",  Label = "Synonym-only matches" },
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-coverage-unmatched-*.md", Label = "Unmatched taxa" },
                    },
                },
                new FlowStep {
                    Id = "freshness",
                    Title = "IUCN freshness in Wikidata",
                    Description = "Checks whether the P141 statements on items in the Wikidata cache match the current Red List category, which Red List edition (P248) and retrieved date (P813) their references cite, and how many IUCN taxa have an item with P627 and with IUCN's scientific name as its taxon name (P225).",
                    Commands = new[] { "wikidata report-iucn-freshness" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-iucn-freshness-*.md" },
                    },
                },
                new FlowStep {
                    Id = "wiki-mismatches",
                    Title = "Wikipedia sitelink mismatches",
                    Description = "Wikidata entries whose enwiki sitelinks resolve to redirects, disambiguations, or mismatched taxa.",
                    Commands = new[] { "wikidata report-wiki-mismatches" },
                    InputSourceIds = new[] { "wikidata-cache", "wikipedia-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-wiki-mismatches*.md", Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "wikidata-wiki-mismatches*.csv", Label = "CSV" },
                    },
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "Reports output", Root = "reports", Path = "", Kind = "directory",
                    Description = "Reports whose file names start with wikidata-. \"Inspect Wikipedia cache\" and \"Wikidata coverage summary\" write no file; their results are in the job output." },
            },
        },

        // ---------------------------------------------------------------
        // IUCN Quality: reports specifically on the IUCN dataset itself.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "iucn-quality",
            Title = "IUCN data quality",
            Description = "These reports list errors and inconsistencies in scientific names, synonyms and narrative text fields, differences from the Catalogue of Life, taxa with no current assessment, subspecies and varieties whose species is not assessed, and failed assessment downloads. Some steps need the IUCN Red List database and others need the IUCN API cache; the \"Import IUCN data\" workflow builds both.",
            Steps = new[] {
                new FlowStep {
                    Id = "html-consistency",
                    Title = "HTML vs plain-text consistency",
                    Description = "Compares the narrative text fields of each assessment in assessments_with_html.csv, with the HTML markup removed, against the same fields in assessments.csv. Counts the assessments that differ in each field: rationale, habitat, threats, population, range, and use and trade.",
                    Commands = new[] { "iucn report-html-consistency" },
                    InputSourceIds = new[] { "iucn-main" },
                },
                new FlowStep {
                    Id = "taxonomy-consistency",
                    Title = "Taxonomy consistency",
                    Description = "Compares each assessment's scientific name with a name built from its genus, species, infraspecific rank and name, and subpopulation name fields. Also checks that the scientific name is the same in the assessments and taxonomy CSV files.",
                    Commands = new[] { "iucn report-taxonomy-consistency" },
                    InputSourceIds = new[] { "iucn-main" },
                },
                new FlowStep {
                    Id = "taxonomy-cleanup",
                    Title = "Taxonomy cleanup candidates",
                    Description = "Lists taxonomy values to clean up: extra spaces, tabs and non-breaking spaces in name and authority fields; scientific names that differ between the assessments and taxonomy CSV files; and infraspecific names that start with \"ssp.\", \"var.\" or another rank marker.",
                    Commands = new[] { "iucn report-taxonomy-cleanup" },
                    InputSourceIds = new[] { "iucn-main" },
                },
                new FlowStep {
                    Id = "col-crosscheck",
                    Title = "Crosscheck against Catalogue of Life",
                    Description = "Compare IUCN species against COL for presence, synonymy, and authority alignment.",
                    Commands = new[] { "iucn report-col-crosscheck" },
                    InputSourceIds = new[] { "iucn-main", "col-sqlite" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-col-crosscheck-*.txt" },
                    },
                },
                new FlowStep {
                    Id = "name-changes",
                    Title = "Taxon name changes",
                    Description = "Report assessments where taxon_scientific_name changes while sharing the same SIS taxon id.",
                    Commands = new[] { "iucn report-name-changes" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-name-changes-*.md" },
                    },
                },
                new FlowStep {
                    Id = "synonym-formatting",
                    Title = "Synonym formatting anomalies",
                    Description = "List IUCN synonyms with double spaces, stray punctuation, or other formatting issues.",
                    Commands = new[] { "iucn report-synonym-formatting" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-synonym-formatting-*.md",  Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-synonym-formatting-*.csv", Label = "CSV" },
                    },
                },
                new FlowStep {
                    Id = "no-latest",
                    Title = "Cached taxa without current assessment",
                    Description = "Lists taxa in the IUCN API cache with no assessment marked \"latest\", grouped by taxonomy. These taxa may have been removed from the Red List, merged into another taxon or taxonomically reclassified.",
                    Commands = new[] { "iucn api report-no-latest" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-no-latest-assessment-*.md",  Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-no-latest-assessment-*.csv", Label = "CSV" },
                    },
                },
                new FlowStep {
                    Id = "orphan-infraranks",
                    Title = "Orphan subspecies & varieties (not API-discoverable)",
                    Description = "Lists assessed subspecies and varieties whose species has no species-level assessment, grouped by taxonomy. `iucn api cache-infraranks --from-csv` or `iucn api cache-all --full` downloads them into the IUCN API cache by their SIS ids.",
                    Commands = new[] { "iucn report-orphan-infraranks" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-orphan-infraranks-*.md",  Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-orphan-infraranks-*.csv", Label = "CSV" },
                    },
                },
                new FlowStep {
                    Id = "failed-assessments",
                    Title = "Assessment downloads that keep failing",
                    Description = "Lists assessments whose most recent download attempt from the IUCN API failed, with the HTTP status, the number of failed attempts, and whether each is the taxon's latest assessment. HTTP 404 means the taxon's list of assessments includes the assessment, but requesting it returns \"not found\".",
                    Commands = new[] { "iucn api report-failed-assessments" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-failed-assessments-*.md",  Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-failed-assessments-*.csv", Label = "CSV" },
                    },
                },
                // Building & projecting the IUCN API cache now lives in the "Import IUCN data"
                // workflow (the first tab) — that's where the CSV vs API routes are laid out.
            },
            Outputs = new[] {
                new FlowResource { Label = "Reports output", Root = "reports", Path = "", Kind = "directory",
                    Description = "Reports whose file names start with iucn-. \"HTML vs plain-text consistency\", \"Taxonomy consistency\" and \"Taxonomy cleanup candidates\" write no file; their results are in the job output." },
            },
        },
    };

    public static FlowDefinition? Find(string id) => All.FirstOrDefault(f => f.Id == id);
}
