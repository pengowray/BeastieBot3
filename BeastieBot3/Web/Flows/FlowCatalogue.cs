namespace BeastieBot3.Web.Flows;

// Hand-maintained catalogue of "flows" — vertical pipelines that walk users
// from inputs through processing steps to outputs. Flows (in display order):
//
//   iucn-import  — get the IUCN dataset in (CSV release vs API), the prerequisite
//                  for everything else; grouped into CSV / API / Compare routes.
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
                    Id = "csv-import",
                    Title = "Import the IUCN CSV release",
                    Description = "Load a downloaded IUCN CSV export (zip) into a local SQLite database. The fast path and the current published snapshot — most pipelines only need this.",
                    Commands = new[] { "iucn import" },
                    InputSourceIds = new[] { "iucn-csv-input" },
                    OutputSourceIds = new[] { "iucn-main" },
                    Group = "1 · From the CSV release",
                    Note = "The zip is downloaded manually from iucnredlist.org. A new release belongs in a fresh database file (IUCN_<version>.sqlite) — `iucn import` refuses a zip whose release differs from what the DB already holds; use --force to wipe and rebuild as the new release. That's all you need for the CSV dataset — skip to the Wikipedia workflows, or build the API dataset below as an alternative.",
                },

                // ===== 2 · From the IUCN API =====
                new FlowStep {
                    Id = "api-cache-species",
                    Title = "Cache species from the API (CSV-sourced)",
                    Description = "Download /api/v4 taxa + assessment payloads for the species present in the imported CSV. The quickest way to seed the API cache once the CSV is imported.",
                    Commands = new[] { "iucn api cache-all" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Group = "2 · From the IUCN API",
                    Note = "Reads the SIS ids from the CSV database (so run step 1 first). cache-all = cache-taxa then cache-assessments in one job. Idempotent — re-running only fetches what's missing unless you pass --force-taxa / --force-assessments.",
                },
                new FlowStep {
                    Id = "api-discover-by-family",
                    Title = "Discover extra taxa by family (no CSV needed)",
                    Description = "Page every family on the live API to also pick up taxa the CSV omits — removed/delisted, reclassified, or historical-only. API-native: doesn't rely on the CSV at all.",
                    Commands = new[] { "iucn api discover-by-family" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Group = "2 · From the IUCN API",
                    Note = "Slower (pages ~800–1000 families on the live API). Use --dry-run to preview, --family Felidae,Canidae to target. Newly-discovered taxa still need their assessments downloaded — the next step's cache-assessments covers them.",
                },
                new FlowStep {
                    Id = "api-infraranks",
                    Title = "Add subspecies & varieties",
                    Description = "Fetch the infraspecific taxa (subspecies/varieties) listed under each cached species and download their assessments. Without this the API dataset is species-only and under-counts versus the CSV.",
                    Commands = new[] { "iucn api cache-infraranks", "iucn api cache-assessments" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-cache" },
                    Optional = true,
                    Group = "2 · From the IUCN API",
                    Note = "Their assessments aren't in the parent species payload. cache-infraranks fetches each infrarank taxon (queuing its assessments); the following cache-assessments downloads them — and also any assessments queued by discover-by-family. API-native and idempotent.",
                },
                new FlowStep {
                    Id = "api-project-view",
                    Title = "Project the API cache for list/chart generation",
                    Description = "Re-shape the latest cached assessments into the same CSV-compatible relational view the CSV import produces, so list/chart generation can read the API dataset via --dataset api.",
                    Commands = new[] { "iucn api project-view" },
                    InputSourceIds = new[] { "iucn-api-cache" },
                    OutputSourceIds = new[] { "iucn-api-projected" },
                    Group = "2 · From the IUCN API",
                    Note = "Run last — after cache-all, discover-by-family, cache-infraranks and cache-assessments — so it isn't partial: project-view exits non-zero and flags the projection partial if any taxon's latest assessment isn't downloaded yet (pass --allow-partial to accept). Then generate with --dataset api.",
                },

                // ===== 3 · Compare CSV vs API =====
                new FlowStep {
                    Id = "compare-datasets",
                    Title = "Compare CSV vs API (optional)",
                    Description = "Check that the two datasets agree before choosing which to generate from. The Data sources page shows a side-by-side card (version, totals, per-category, coverage); the count-scopes audit diffs them on the command line.",
                    Commands = new[] { "iucn count-scopes" },
                    InputSourceIds = new[] { "iucn-main", "iucn-api-projected" },
                    Optional = true,
                    Group = "3 · Compare the two datasets",
                    Note = "Run `iucn count-scopes --compare` to diff the CSV and API global-species counts (the button runs the single-dataset audit; add --compare via the command form). Small deltas are expected — the API has no taxa lacking a latest assessment, and the projection flags partial coverage. Open the Data sources tab for the visual comparison.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "reports", Pattern = "iucn-count-scopes-*.md", Label = "Count-scope audit" },
                    },
                },
            },
            Outputs = new[] {
                new FlowResource { Label = "IUCN (CSV) database",     Root = "reports", Path = "", Kind = "directory",
                    Description = "The CSV-imported and API-projected SQLite databases live under Datastore paths (see Data sources / show-paths)." },
            },
        },

        // ---------------------------------------------------------------
        // Wiki Reports: the full pipeline that produces Wikipedia output.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-reports",
            Title = "Wikipedia reports pipeline",
            Description = "Generate wikitext lists and IUCN charts for Wikipedia. Each step caches data locally so re-runs only download new material. Maintenance steps (below) are not normally needed — they exist for coverage gaps and cache repair.",
            Steps = new[] {
                // -------- Pipeline (core path) --------
                new FlowStep {
                    Id = "iucn-import",
                    Title = "Import IUCN Red List CSVs",
                    Description = "Load the IUCN CSV release into the local SQLite store. The base dataset every other step joins against.",
                    Commands = new[] { "iucn import" },
                    InputSourceIds = new[] { "iucn-csv-input" },
                    OutputSourceIds = new[] { "iucn-main" },
                    Note = "The starting point of this pipeline — see the dedicated \"Import IUCN data\" workflow (first tab) for the full picture: the CSV route shown here, the IUCN API route (--dataset api), and comparing the two. " +
                           "A new IUCN release belongs in a fresh database file (IUCN_<version>.sqlite) — importing into an existing DB double-counts.",
                },
                new FlowStep {
                    Id = "col-import",
                    Title = "Import Catalogue of Life (optional)",
                    Description = "Import COL ColDP archives for cross-check and taxonomy enrichment. Optional but improves list quality.",
                    Commands = new[] { "col import" },
                    InputSourceIds = new[] { "col-input" },
                    OutputSourceIds = new[] { "col-sqlite" },
                    Optional = true,
                },
                new FlowStep {
                    Id = "wikidata-cache",
                    Title = "Seed and cache Wikidata entities",
                    Description = "Discover Wikidata Q-ids for IUCN-linked taxa (SPARQL on P627) and download their entity JSON, populating the normalised taxon-name lookup index inline.",
                    Commands = new[] { "wikidata cache-all", "wikidata seed-taxa", "wikidata cache-entities" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Note = "wikidata cache-all bundles the seed-taxa (Q-id discovery) and cache-entities (JSON download) passes into a single job. Requires WIKIDATA_USER_AGENT in .env.",
                },
                new FlowStep {
                    Id = "wikipedia-enqueue-fetch",
                    Title = "Enqueue and fetch Wikipedia pages",
                    Description = "Seed the Wikipedia page queue from Wikidata enwiki sitelinks and higher-taxon names, then download HTML + wikitext for every queued title.",
                    Commands = new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa", "wikipedia fetch-pages" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                    Note = "By default this runs three commands — enqueue-wikidata, enqueue-taxa, then fetch-pages. fetch-pages can also enqueue+fetch a single page inline via --title \"Ursus maritimus\".",
                },
                new FlowStep {
                    Id = "wikipedia-match",
                    Title = "Match taxa to Wikipedia pages",
                    Description = "Resolve each IUCN taxon to the best Wikipedia page (via Wikidata sitelinks, synonyms, redirects).",
                    Commands = new[] { "wikipedia match-taxa" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache", "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                },
                new FlowStep {
                    Id = "common-names",
                    Title = "Aggregate common names",
                    Description = "Build the unified common-name store across IUCN, Wikidata, Wikipedia, and COL.",
                    Commands = new[] { "common-names init", "common-names aggregate" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache", "wikipedia-cache", "col-sqlite" },
                    OutputSourceIds = new[] { "common-names" },
                },
                new FlowStep {
                    Id = "generate",
                    Title = "Generate Wikipedia lists + charts",
                    Description = "Apply YAML rules and Mustache templates to produce final wikitext output.",
                    Commands = new[] { "wikipedia generate-lists", "wikipedia generate-charts" },
                    InputSourceIds = new[] { "iucn-main", "wikipedia-cache", "common-names", "col-sqlite" },
                    OutputSourceIds = Array.Empty<string>(),
                    Note = "Uses rules/wikipedia-lists.yml, rules/chart-groups.yml, and templates under rules/wikipedia/templates/.",
                    OutputPatterns = new[] {
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.wikitext", Label = "Lists" },
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.tab",      Label = "Chart data" },
                        new FlowOutputPattern { Root = "wikipedia-output", Pattern = "*.chart",    Label = "Chart def" },
                    },
                },

                // -------- Maintenance (only when needed) --------
                new FlowStep {
                    Id = "wikidata-backfill",
                    Title = "Backfill: discover missing Wikidata Q-ids",
                    Description = "For IUCN taxa not linked to a Wikidata entity (i.e. Wikidata never declared P627 for them), search by scientific name and synonyms to find a likely match. Run when Wikidata coverage drops.",
                    Commands = new[] { "wikidata backfill-iucn" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Section = FlowSection.Maintenance,
                    Note = "Only needed if `wikidata report-coverage` shows many unmatched taxa. The main cache-all / seed-taxa pass only finds Q-ids that Wikidata already tags as IUCN species (via P627); backfill searches by scientific name and synonyms for the rest.",
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
                new FlowResource { Label = "Caps rules",     Root = "rules", Path = "caps.txt",              Kind = "template" },
                new FlowResource { Label = "Templates dir",  Root = "rules", Path = "wikipedia/templates",   Kind = "directory" },
            },
            Outputs = new[] {
                new FlowResource { Label = "Wikipedia output", Root = "wikipedia-output", Path = "",  Kind = "directory",
                    Description = "Generated wikitext lists and chart files." },
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
