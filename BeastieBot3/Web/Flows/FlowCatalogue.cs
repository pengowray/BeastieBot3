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
            Description = "Get the IUCN Red List into the local store — the base dataset every other workflow builds on. Pick one route: the CSV release (fast, the current published snapshot) or the live API (more complete: historical/delisted taxa, subspecies, synonyms). Optionally compare the two before generating lists/charts with --dataset csv|api.",
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
                        "Save both zips, still zipped, under a folder for the release, for example D:\\datasets\\IUCN_CVS_2026-1. Give each download its own subfolder, and start that subfolder's name with the release version: \"2026-1 non-passerines\" and \"2026-1 passerines\".",
                        "Starting the subfolder name with the version is not decoration. It is where the import reads the release from, and IUCN names the zips with a random id that contains number pairs like 1373-414. Miss the version out and the import mistakes one of those for the release, filing the two downloads as different releases.",
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
                    Description = "Tell everything else to read the database the import just created, then restart the web server. No command: you edit paths.ini by hand. Skip this when the import went into the file paths.ini already names.",
                    Commands = Array.Empty<string>(),
                    InputSourceIds = new[] { "iucn-main" },
                    Optional = true,
                    Probe = FlowStepProbes.IucnCsvRepoint,
                    Group = "1 · From the CSV release",
                    Note = "Set [Datastore] IUCN_sqlite_from_cvs to the file the import reported, then restart serve. Until you do, every other command and every page here still reads the previous release, and nothing warns you: a database from the old release looks perfectly healthy. paths.ini is only read at startup, so the restart is what makes the change take effect. Confirm with `show-paths` or the Data sources tab, which shows the imported version. That is all you need for the CSV dataset: skip to the Wikipedia workflows, or build the API dataset below as an alternative.",
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
                    Note = "The API data carries no release version — a payload downloaded during the last release looks exactly like one downloaded today — so a re-import means \"fetch everything again that is older than this date\". The date is stored, so the steps below pick it up on their own and you never type it twice. Stop the download whenever you like: re-running carries on from where it stopped rather than starting over. The whole re-import is roughly 37 hours of downloading, and it also re-checks the taxa the API previously said were gone, because that answer was only ever true of the release it was recorded against. Set the date to just before the new release was published, or leave it at now to re-fetch the lot.",
                },
                new FlowStep {
                    Id = "api-cache-species",
                    Title = "Cache species from the API (CSV-sourced)",
                    Description = "Download /api/v4 taxa + assessment payloads for the species present in the imported CSV. The quickest way to seed the API cache once the CSV is imported.",
                    Commands = new[] { "iucn api cache-all" },
                    Probe = FlowStepProbes.IucnApiTaxa,
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Group = "2 · From the IUCN API",
                    Note = "Reads the SIS ids from the CSV database (so run step 1 first). cache-all = cache-taxa then cache-assessments in one job. Idempotent — re-running only fetches what's missing unless you pass --force-taxa / --force-assessments. Shortcut: `iucn api cache-all --full` chains ALL the API steps below in one command — cache-taxa → cache-infraranks (--from-csv) → cache-assessments → project-view.",
                },
                new FlowStep {
                    Id = "api-discover-by-family",
                    Title = "Discover extra taxa by family (no CSV needed)",
                    Description = "Page every family on the live API to also pick up taxa the CSV omits — removed/delisted, reclassified, or historical-only. API-native: doesn't rely on the CSV at all.",
                    Commands = new[] { "iucn api discover-by-family" },
                    Probe = FlowStepProbes.IucnApiDiscovery,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Group = "2 · From the IUCN API",
                    Note = "Slower (pages ~800–1000 families on the live API). Use --dry-run to preview, --family Felidae,Canidae to target. Newly-discovered taxa still need their assessments downloaded — the next step's cache-assessments covers them.",
                },
                new FlowStep {
                    Id = "api-infraranks-cached",
                    Title = "Add subspecies & varieties (cached API-sourced)",
                    Description = "Fetch the infraspecific taxa (subspecies/varieties) listed under the species already in the cache (their taxon.infrarank_taxa) and download their assessments. API-native — no CSV needed.",
                    Commands = new[] { "iucn api cache-infraranks", "iucn api cache-assessments" },
                    Probe = FlowStepProbes.IucnApiInfraranks,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Group = "2 · From the IUCN API",
                    Note = "Subspecies' assessments aren't in the parent payload. cache-infraranks fetches each infrarank taxon (queuing its assessments); the following cache-assessments downloads them — and any queued by discover-by-family. Idempotent; ids previously 404'd (no standalone record) are skipped. Reaches subspecies of assessed species only — for the rest, run the CSV-sourced step.",
                },
                new FlowStep {
                    Id = "api-infraranks-csv",
                    Title = "Add subspecies & varieties (CSV-sourced)",
                    Description = "Also seed infraspecific taxa from the imported CSV, catching assessed subspecies/varieties whose PARENT species is unassessed (~0.2% of taxa). Those appear on no family page and in no cached species, so they're reachable only by their CSV-listed sis_id.",
                    Commands = new[] { "iucn api cache-infraranks --from-csv", "iucn api cache-assessments" },
                    Probe = FlowStepProbes.IucnApiInfraranks,
                    InputSourceIds = new[] { "iucn-api-cache", "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Group = "2 · From the IUCN API",
                    Note = "Needs the CSV import (iucn-main). --from-csv unions the CSV's infraspecific taxonIds with the API discovery; the superset is filtered the same way (skips already-cached + 404-tombstoned). This is the only way to reach orphan subspecies of unassessed species. Supersedes the cached-API-sourced step, so you can run just this one if you want full coverage.",
                },
                new FlowStep {
                    Id = "api-project-view",
                    Title = "Project the API cache for list/chart generation",
                    Description = "Re-shape the latest cached assessments into the same CSV-compatible relational view the CSV import produces, so list/chart generation can read the API dataset via --dataset api.",
                    Commands = new[] { "iucn api project-view" },
                    Probe = FlowStepProbes.IucnApiProjection,
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-projected" },
                    Group = "2 · From the IUCN API",
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
            Outputs = new[] {
                new FlowResource { Label = "IUCN (CSV) database",     Root = "reports", Path = "", Kind = "directory",
                    Description = "The CSV-imported and API-projected SQLite databases live under Datastore paths (see Data sources / show-paths)." },
            },
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
            Description = "Bring in a new Catalogue of Life (CoL) release and refresh everything that reads it: the common-name hub, the Red List audit site, Wikidata/Wikipedia synonym discovery, and the Wikipedia lists' sub-rank grouping. The flow can't change paths.ini for you, so one step is a manual config edit + serve restart. CoL is taxonomy enrichment, not a hard dependency — most consumers keep working without it, but their generated output stays frozen on the previous release until you re-run the step that produces it.",
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
                    Note = "Reads the ColDP zip(s) from Datasets:COL_dir and builds col_coldp_<label>.sqlite, where <label> is the alias inside the zip's metadata.yaml (e.g. \"COL26.5 XR\" -> col_coldp_COL26.5_XR.sqlite) — NOT the zip filename. A new release gets a new filename, so it imports ALONGSIDE the old DB (the old one is left on disk; remove it in Maintenance below). Listed as Destructive only because --force wipes and rebuilds; without --force a finished DB is skipped and a half-written/corrupt one is rebuilt. The import is multi-GB and slow. It downloads datapackage.json from the CoL API once, only if the input folder doesn't already contain one (a provenance snapshot that is never parsed).",
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
                    Note = "The importer never edits paths.ini. Set [Datastore] COL_sqlite to the new col_coldp_<label>.sqlite AND [Datasets] COL_dir to the new folder — set BOTH, they can drift independently and nothing warns you if they disagree. Then RESTART serve: it loads paths.ini once into a singleton with no hot-reload, so until you restart, every page (including this flow) keeps resolving the OLD CoL file. Confirm with `show-paths` or the Data sources tab. Important: there is no CoL version/freshness check anywhere — a database from the previous release looks perfectly healthy — so confirming the dataset label in the next step is your only safeguard against silently running on the old release.",
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
                    Note = "`col report-nameusage-fields` opens the new COL_sqlite, confirms the nameusage table is populated, and prints the dataset label + row counts — proving the repoint reached the intended release. `col report-subgenus-homonyms` is a heavier query that exercises the indexes and confirms the DB is fully queryable. (`col check` only checks that the source folder is mounted; it never opens the database, so it can't confirm the import. Also note nothing detects a half-written import — the success signal is internal — so a clean profile is reassuring but not a guarantee.)",
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
                    Description = "Work out which common names now point at more than one species, so the Wikipedia lists know not to use them on their own.",
                    Commands = new[] { "common-names detect-conflicts", "common-names detect-conflicts --clear-existing" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Group = "2 · Refresh derived data",
                    Probe = FlowStepProbes.CommonNameConflicts,
                    Note = "Run this after the re-aggregate above: a name shared by two species is only found by comparing the names now in the hub, and aggregating does not redo that comparison. `--replace` empties the list first, so after the second aggregate button the plain command rebuilds it. After a plain aggregate, use `--clear-existing` (the second button) instead: without it, old rows stay behind for pairs that are no longer ambiguous. Wikipedia list generation reads the result: it passes over an ambiguous name in favour of the taxon's next-best common name, and falls back to the scientific name when every candidate is ambiguous.",
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
                    Note = "The audit site's CoL-crosscheck page reads CoL live, so re-running reflects the new release. It internally replicates the `iucn report-col-crosscheck` logic, so you do NOT need to run that command first. Output: <reports_dir>/redlist-audit-2026/ (open index.html). If COL_sqlite is missing or has no nameusage table the CoL page is silently skipped (exit 0) — so verify the repoint first. The rendered site records no CoL version label; it always shows whatever COL_sqlite currently points at.",
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
                    Note = "Produces a timestamped iucn-col-crosscheck-*.txt (presence, accepted-vs-synonym status, authority alignment, and rank-ladder alignment) whose header records the exact CoL path used. Redundant with the audit site for the web view — run it for the standalone file. Tip: glance at the authority-match counts; if the new release renamed a CoL column outside the reader's known names it is silently read as NULL, which can inflate apparent authority mismatches.",
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
                    Note = "`backfill-iucn` goes through the IUCN taxa that have no linked Wikidata item and searches Wikidata for each one: first its scientific name, then its IUCN and Catalogue of Life synonyms. A hit links the taxon to that item and queues the item for download; `cache-entities` then downloads the queued items and updates the name index as it goes (no separate rebuild). This step only adds links. Taxa that already have an item are not searched, and no existing match is changed or re-checked, so every match it reports is new. Taxa searched before with no result are skipped too, so a plain run covers only never-searched taxa. After a Catalogue of Life update the point is to try the new synonyms on the old no-result taxa, so add --retry-missing. Downloads from Wikidata (needs WIKIDATA_USER_AGENT in .env). Safe to skip: you only miss new matches; nothing already cached breaks. The full picture is in the Wikipedia reports pipeline.",
                },
                new FlowStep {
                    Id = "wikipedia-discover",
                    Title = "Look for articles for the taxa that have none",
                    Description = "The new release's synonyms give the matcher extra candidate article titles. Queue them, download them, then match again.",
                    Commands = new[] { "wikipedia match-taxa", "wikipedia fetch-pages --awaited-only --newest-first", "wikipedia match-taxa" },
                    InputSourceIds = new[] { "col-sqlite", "iucn-main", "wikidata-cache", "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikipediaFetchAwaited,
                    Group = "3 \u00b7 Search for new matches (downloads)",
                    Note = "Match, download, match again. The first `match-taxa` makes no network calls and only queues candidate titles; `fetch-pages` downloads them, narrowed to pages a taxon is waiting on and taking the newest titles first; the second `match-taxa` settles the matches for what arrived. Taxa that already have a matched article are left alone (pass --reprocess-matched to revisit them), so this only finds articles for taxa that had none. Run the Wikidata step above first, because match-taxa also resolves through Wikidata site links. Downloads from Wikipedia.",
                },

                // ===== 4 · Regenerate outputs =====
                new FlowStep {
                    Id = "generate-lists",
                    Title = "Regenerate Wikipedia lists",
                    Description = "Bake the new CoL sub-rank grouping, refreshed common names, and new article links into the wikitext lists.",
                    Commands = new[] { "wikipedia generate-lists" },
                    InputSourceIds = new[] { "iucn-main", "col-sqlite", "common-names", "wikipedia-cache" },
                    OutputSourceIds = Array.Empty<string>(),
                    Group = "4 · Regenerate outputs",
                    Probe = FlowStepProbes.ColRebuildLists,
                    Note = "Regenerate the FULL set — do NOT pass --list/--status/--taxa-group for a CoL update. CoL only changes the section grouping of lists that split on CoL-only ranks (suborder/superfamily/subfamily/tribe/subgenus), virtual groups, or auto-split; but structure-metrics.json is keyed to the IUCN release only, so a partial regenerate leaves stale CoL-grouped metrics behind (visible in `wikipedia preview-impact`). Run the common-names and discovery steps first so new vernaculars and article links are included. The first run is slower: the new CoL enrich-cache sidecar (col_coldp_<label>.sqlite.enrich-cache.sqlite, created next to the CoL DB) starts empty and rebuilds once. Charts (`wikipedia generate-charts`) don't read CoL — skip them for a CoL-only update.",
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
                    Note = "Reads CoL live, so it always reflects the current COL_sqlite. Run after the discovery steps to confirm the new release improved coverage. No downloads.",
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
                    Description = "Regenerated wikitext lists with the new CoL sub-rank grouping." },
                new FlowResource { Label = "Reports output", Root = "reports", Path = "", Kind = "directory",
                    Description = "Crosscheck and CoL profile reports land here as text/CSV." },
            },
        },

        // ---------------------------------------------------------------
        // Wiki Reports: the full pipeline that produces Wikipedia output.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-reports",
            Title = "Wikipedia reports pipeline",
            Description = "The steps are in priority order: catch up on a new release first, then work down the standing download queues, which are never finished in one sitting. Every download step only adds, so it can be stopped and picked up later, and each shows how much work it has left. Re-downloading copies you already have is the lowest priority and sits under Maintenance.",
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
                    Note = "The starting point of this pipeline — see the dedicated \"Import IUCN data\" workflow (first tab) for the full picture: the CSV route shown here, the IUCN API route (--dataset api), and comparing the two. " +
                           "A new IUCN release belongs in a fresh database file (IUCN_<version>.sqlite) — importing into an existing DB double-counts.",
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
                    Id = "wikidata-seed",
                    Title = "Find Wikidata items for IUCN taxa",
                    Description = "One sweep of Wikidata for every item that carries an IUCN Red List id, adding each to the download queue. Downloads no entity data itself.",
                    Commands = new[] { "wikidata seed-taxa", "wikidata cache-all" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = FlowStepProbes.WikidataSweep,
                    Group = "2 \u00b7 Wikidata",
                    Note = "The cheap way to find items: one query returns everything Wikidata already tags with an IUCN id, instead of searching for taxa one at a time. It queues ids and stops there; the next step downloads them. Safe to re-run, and it continues from where the last sweep stopped, so it only adds ids it has not seen. It never goes back over items it has already passed, which means an item that gained an IUCN id since your last sweep is only picked up by starting the sweep again with --reset-cursor (see Re-download old copies, under Maintenance). `wikidata cache-all` runs this sweep and the download step below as one job. Needs WIKIDATA_USER_AGENT in .env.",
                },
                new FlowStep {
                    Id = "wikidata-download",
                    Title = "Download the queued Wikidata items",
                    Description = "Fetch the entity data for every queued id, and index the scientific names inside it.",
                    Commands = new[] { "wikidata cache-entities" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Probe = FlowStepProbes.WikidataDownload,
                    Group = "2 \u00b7 Wikidata",
                    Note = "Adds only: an item already downloaded is skipped, so stopping and re-running loses nothing. The name index the Wikipedia matcher reads is built during the download, so there is no separate rebuild step to run. --failed-only retries just the failures; --refresh-only with --max-age-hours re-downloads old copies without pulling in anything never downloaded.",
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
                    Group = "2 \u00b7 Wikidata",
                    Note = "Much slower than the sweep because it searches one taxon at a time, so run it after the sweep rather than instead of it. It only adds links: taxa that already have an item are not searched again, and no existing match is changed, so every match it reports is new. Taxa searched before with no match are skipped, so a run after a new release spends its time on taxa never searched; --retry-missing searches for those again, and --retry-missing-after <DAYS> only for the ones searched longest ago. Synonyms come from the IUCN API cache and Catalogue of Life, so importing those first finds more matches. Ids it finds are queued, not downloaded, so run the download step after it.",
                },
                new FlowStep {
                    Id = "wikipedia-queue",
                    Title = "Queue Wikipedia titles",
                    Description = "Add candidate page titles to the download queue: the English Wikipedia links on cached Wikidata items, plus class, order and family names from IUCN.",
                    Commands = new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Probe = FlowStepProbes.WikipediaQueue,
                    Group = "3 \u00b7 Wikipedia",
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
                    Group = "3 \u00b7 Wikipedia",
                    Note = "Match, download, match again. The first run picks candidate titles and queues them without making any network calls; the download step below fetches them; the second run reads the fetched pages and settles the matches. Taxa already matched are left alone unless you pass --reprocess-matched. Candidates come from Wikidata site links, IUCN and Catalogue of Life synonyms, scientific names in cached taxoboxes, and redirects.",
                },
                new FlowStep {
                    Id = "wikipedia-fetch-awaited",
                    Title = "Download the pages taxa are waiting on",
                    Description = "Fetch the queued pages that an IUCN taxon has no article without, newest titles first.",
                    Commands = new[] { "wikipedia fetch-pages --awaited-only --newest-first", "wikipedia match-taxa" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Probe = FlowStepProbes.WikipediaFetchAwaited,
                    Group = "3 \u00b7 Wikipedia",
                    Note = "The queue holds far more titles than the taxa themselves need, because higher taxa, synonyms and redirect targets are queued too. --awaited-only narrows it to pages a taxon is waiting on, and --newest-first takes the titles queued most recently, which after a release update are the new taxa. It downloads only what has not been downloaded, so it can be stopped and resumed; add --limit to stop after a set number. Run match-taxa again afterwards (the second button) to settle the matches for the pages that arrived.",
                },
                new FlowStep {
                    Id = "wikipedia-fetch-rest",
                    Title = "Download the rest of the queue",
                    Description = "Fetch the remaining queued titles: higher taxa, synonyms and redirect targets.",
                    Commands = new[] { "wikipedia fetch-pages" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Probe = FlowStepProbes.WikipediaFetchRest,
                    Group = "3 \u00b7 Wikipedia",
                    Note = "The lists do not need these pages, but they improve redirect and synonym resolution for the taxa that do. There can be hundreds of thousands of them, so this is a job of days: use --limit to work through it in sessions. It downloads only what is missing, so stopping and re-running continues where it stopped. Run \"Remove titles that cannot be articles\" under Maintenance first: in August 2026, 38% of the queue was titles carrying an authority, which no article has.",
                },
                new FlowStep {
                    Id = "common-names",
                    Title = "Aggregate common names",
                    Description = "Build the unified common-name store across IUCN, Wikidata, Wikipedia, and COL.",
                    Commands = new[] { "common-names init", "common-names aggregate" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache", "wikipedia-cache", "col-sqlite" },
                    OutputSourceIds = new[] { "common-names" },
                    Group = "4 \u00b7 Common names",
                    Note = "`init` seeds the store's species from IUCN and rules/caps.txt; `aggregate` then reads the IUCN, Wikidata, Wikipedia and Catalogue of Life caches and fills in the names. Both are safe to re-run and download nothing. Re-running only adds and updates, so a name a source has since dropped or renamed stays in the store. After a cache has been refreshed or rebuilt, re-import that one source from scratch instead: `common-names aggregate --source wikidata --replace` (or wikipedia, col, iucn), which clears what that source contributed before importing it again and leaves the others alone. `common-names sources` lists when each source was last aggregated and last replaced.",
                },
                new FlowStep {
                    Id = "detect-conflicts",
                    Title = "Find ambiguous common names",
                    Description = "Work out which common names point at more than one species, so the lists know not to use them on their own.",
                    Commands = new[] { "common-names detect-conflicts", "common-names detect-conflicts --clear-existing" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Probe = FlowStepProbes.CommonNameConflicts,
                    Group = "4 \u00b7 Common names",
                    Note = "Run after aggregating: a name shared by two species is only found by comparing the names now in the hub, and aggregating does not redo that comparison. Generation reads the result: it passes over an ambiguous name in favour of the taxon's next-best common name, and falls back to the scientific name when every candidate is ambiguous. Use `--clear-existing` (the second button) to rebuild the list from scratch; without it, old rows stay behind for pairs that are no longer ambiguous.",
                },
                new FlowStep {
                    Id = "refresh-caps",
                    Title = "Refresh capitalization rules (after editing caps.txt)",
                    Description = "Re-import rules/caps.txt into the common-names store so capitalization edits (including multi-word phrase rules like \"guinea pig\") take effect — without rebuilding the whole store.",
                    Commands = new[] { "common-names init --skip-taxa" },
                    InputSourceIds = new[] { "common-names" },
                    OutputSourceIds = new[] { "common-names" },
                    Optional = true,
                    Group = "4 \u00b7 Common names",
                    Note = "Only needed when you've edited rules/caps.txt since the last full \"Aggregate common names\" run — `--skip-taxa` reimports just the caps rules (fast, idempotent), because the generator reads them from the common-names DB, not the file. The other rule files — taxon-rules.yml, rules/rules-list.txt, and the list/preset/group YAML — are read directly at generation time, so they need no import: edit them and re-run Generate.",
                },
                new FlowStep {
                    Id = "generate",
                    Title = "Generate Wikipedia lists + charts",
                    Description = "Apply YAML rules and Mustache templates to produce final wikitext output.",
                    Commands = new[] { "wikipedia generate-lists", "wikipedia generate-charts" },
                    InputSourceIds = new[] { "iucn-main", "wikipedia-cache", "common-names", "col-sqlite" },
                    OutputSourceIds = Array.Empty<string>(),
                    Group = "5 \u00b7 Generate",
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
                    Description = "Try the Wikipedia pages and Wikidata items that failed to download again.",
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
                    Description = "Take the queued titles that carry a taxonomic authority or a nomenclatural note out of the queue. No Wikipedia article is titled that way.",
                    Commands = new[] { "wikipedia prune-queue", "wikipedia prune-queue --apply" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Optional = true,
                    Section = FlowSection.Maintenance,
                    Note = "IUCN stores a synonym complete with its authority and any note, for example `Eumeces schneideri (Daudin, 1802) [orth. error]`, and earlier runs queued those strings as article titles. In the 2026-1 cache in August 2026 that was 73,144 of the 190,212 titles waiting to be downloaded, and not one of them can exist. The matcher no longer makes them, so this is a one-off tidy of what is already queued. The first button reports what it would remove and changes nothing; the second removes them. Cached pages and settled matches are left alone, and a taxon that was waiting on a removed title is picked up again by the next `wikipedia match-taxa` run.",
                },
                new FlowStep {
                    Id = "wiki-refresh",
                    Title = "Re-download old copies",
                    Description = "Update pages and items downloaded a long time ago. The lowest-priority work here: nothing is missing without it.",
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
                    Note = "These already have a cached copy, and both caches are large enough that refreshing everything takes days, so leave this until the steps above are done. --refresh-only re-downloads cached copies and leaves the never-downloaded queue alone; without it, a refresh turns into a fetch of the whole queue as well. `seed-taxa --reset-cursor` starts the Wikidata sweep from the beginning, which is the only way to pick up items that gained an IUCN id after your last sweep. Adjust the day and hour thresholds on the buttons to suit how old is too old.",
                },
                new FlowStep {
                    Id = "wikidata-rebuild-indexes",
                    Title = "Rebuild Wikidata lookup indexes",
                    Description = "Recompute the normalised taxon-name index from cached entity JSON. The cache-entities command builds this index automatically during download, so only run this when the index is suspected stale.",
                    Commands = new[] { "wikidata rebuild-indexes" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Section = FlowSection.Maintenance,
                    Note = "--force drops and rebuilds; --include-p141 also rebuilds the P141 statement cache.",
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
                    Note = "The CSV is downloaded manually from environment.gov.au/sprat-public (Select All for every field). A completed database is skipped unless --force; after a fresh download, point Datasets:SPRAT_csv at it and re-run.",
                },
                new FlowStep {
                    Id = "sprat-generate",
                    Title = "Generate the Australia lists",
                    Description = "Produce one \"List of rare and threatened <group> of Australia\" wikitext page per major taxonomic group (mammals, birds, reptiles, amphibians, fish, invertebrates; dicots, monocots, ferns/conifers/allies).",
                    Commands = new[] { "sprat generate-lists" },
                    InputSourceIds = new[] { "sprat-sqlite", "common-names", "wikipedia-cache" },
                    OutputSourceIds = Array.Empty<string>(),
                    Note = "SPRAT is the data source — no IUCN import required. The common-names hub and Wikipedia cache are optional: when present they supply real Wikipedia article links and conventionally-cased common names; otherwise SPRAT's own vernaculars are used. Output lands in the wikipedia output dir's australia/ subfolder.",
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "Australia lists", Root = "wikipedia-output", Path = "australia", Kind = "directory",
                    Description = "Generated \"List of rare and threatened <group> of Australia\" wikitext files." },
            },
        },

        // ---------------------------------------------------------------
        // Wiki/Wikidata Quality: curated grouping of report commands that
        // surface coverage gaps, freshness, sitelink mismatches.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-quality",
            Title = "Wikipedia / Wikidata quality",
            Description = "Reports that surface coverage gaps, stale matches, and sitelink mismatches across the Wikipedia and Wikidata caches.",
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
                    Description = "Compare IUCN data against the IUCN claims stored in cached Wikidata entities; surface stale rows.",
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
                    Description = "All quality reports land here as Markdown (and sometimes CSV)." },
            },
        },

        // ---------------------------------------------------------------
        // IUCN Quality: reports specifically on the IUCN dataset itself.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "iucn-quality",
            Title = "IUCN data quality",
            Description = "Reports that surface formatting inconsistencies, name changes, synonym anomalies and missing assessments in the IUCN dataset. Build the dataset first via the Import IUCN data workflow (CSV or API).",
            Steps = new[] {
                new FlowStep {
                    Id = "html-consistency",
                    Title = "HTML vs plain-text consistency",
                    Description = "Strip HTML from `_html` fields and compare against the plain-text versions for normalization drift.",
                    Commands = new[] { "iucn report-html-consistency" },
                    InputSourceIds = new[] { "iucn-main" },
                },
                new FlowStep {
                    Id = "taxonomy-consistency",
                    Title = "Taxonomy consistency",
                    Description = "Rebuild scientific names from taxonomy components and verify field alignment.",
                    Commands = new[] { "iucn report-taxonomy-consistency" },
                    InputSourceIds = new[] { "iucn-main" },
                },
                new FlowStep {
                    Id = "taxonomy-cleanup",
                    Title = "Taxonomy cleanup candidates",
                    Description = "Identify per-record taxonomy fields needing whitespace normalisation or marker cleanup.",
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
                    Description = "Cached taxa whose `latest_assessment` is missing, grouped phylogenetically.",
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
                    Description = "Assessed subspecies/varieties whose parent species is unassessed — reachable from the API only by their CSV sis_id, so they explain the small API-vs-CSV coverage gap. Grouped by taxonomy.",
                    Commands = new[] { "iucn report-orphan-infraranks" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-orphan-infraranks-*.md",  Label = "Markdown" },
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-orphan-infraranks-*.csv", Label = "CSV" },
                    },
                },
                new FlowStep {
                    Id = "failed-assessments",
                    Title = "Assessments the API can't serve (HTTP 500)",
                    Description = "Assessment downloads the IUCN API keeps 500ing on. Root cause: each has an empty geographic scope (no region), and the API errors on exactly those. They're phantom scope-less duplicates of a taxon's real per-scope assessments, so they create no projection gap.",
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
                    Description = "All IUCN quality reports land here as Markdown (and sometimes CSV)." },
            },
        },
    };

    public static FlowDefinition? Find(string id) => All.FirstOrDefault(f => f.Id == id);
}
