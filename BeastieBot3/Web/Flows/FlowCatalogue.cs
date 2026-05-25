namespace BeastieBot3.Web.Flows;

// Hand-maintained catalogue of "flows" — vertical pipelines that walk users
// from inputs through processing steps to outputs. Three flows:
//
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

public sealed record FlowStep {
    public required string Id { get; init; }
    public required string Title { get; init; }
    public required string Description { get; init; }
    public IReadOnlyList<string> Commands { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> InputSourceIds { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> OutputSourceIds { get; init; } = Array.Empty<string>();
    public bool Optional { get; init; } = false;
    public string? Note { get; init; }

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
        // Wiki Reports: the full pipeline that produces Wikipedia output.
        // ---------------------------------------------------------------
        new FlowDefinition {
            Id = "wiki-reports",
            Title = "Wikipedia reports pipeline",
            Description = "Generate wikitext lists and IUCN charts for Wikipedia. Each step caches data locally so re-runs only download new material.",
            Steps = new[] {
                new FlowStep {
                    Id = "iucn-import",
                    Title = "Import IUCN Red List CSVs",
                    Description = "Load the IUCN CSV release into the local SQLite store. The base dataset every other step joins against.",
                    Commands = new[] { "iucn import" },
                    InputSourceIds = new[] { "iucn-csv-input" },
                    OutputSourceIds = new[] { "iucn-main" },
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
                    Id = "wikidata-seed",
                    Title = "Seed Wikidata Q-ids",
                    Description = "Find Wikidata entities for taxa carrying IUCN identifiers and queue them for download.",
                    Commands = new[] { "wikidata seed-taxa" },
                    InputSourceIds = new[] { "iucn-main" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Note = "Requires WIKIDATA_USER_AGENT in .env.",
                },
                new FlowStep {
                    Id = "wikidata-cache",
                    Title = "Cache Wikidata entities",
                    Description = "Download queued Wikidata JSON payloads into the local cache.",
                    Commands = new[] { "wikidata cache-entities" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                },
                new FlowStep {
                    Id = "wikidata-backfill",
                    Title = "Backfill missing Wikidata matches",
                    Description = "For IUCN taxa not yet linked to a Wikidata entity, search by scientific name and synonyms.",
                    Commands = new[] { "wikidata backfill-iucn" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                    Optional = true,
                },
                new FlowStep {
                    Id = "wikidata-rebuild-indexes",
                    Title = "Rebuild Wikidata lookup indexes",
                    Description = "Build the normalised taxon-name index used downstream for matching. Re-run after backfills.",
                    Commands = new[] { "wikidata rebuild-indexes" },
                    InputSourceIds = new[] { "wikidata-cache" },
                    OutputSourceIds = new[] { "wikidata-cache" },
                },
                new FlowStep {
                    Id = "wikipedia-enqueue",
                    Title = "Enqueue Wikipedia pages",
                    Description = "Seed the Wikipedia cache with page titles to fetch — both Wikidata sitelinks and higher-taxon names.",
                    Commands = new[] { "wikipedia enqueue-wikidata", "wikipedia enqueue-taxa" },
                    InputSourceIds = new[] { "iucn-main", "wikidata-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
                },
                new FlowStep {
                    Id = "wikipedia-fetch",
                    Title = "Fetch Wikipedia pages",
                    Description = "Download HTML and wikitext for every queued title. Respects MediaWiki rate limits.",
                    Commands = new[] { "wikipedia fetch-pages" },
                    InputSourceIds = new[] { "wikipedia-cache" },
                    OutputSourceIds = new[] { "wikipedia-cache" },
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
            Description = "Reports that surface formatting inconsistencies, name changes, synonym anomalies and missing assessments in the IUCN dataset.",
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
            },
            Outputs = new[] {
                new FlowResource { Label = "Reports output", Root = "reports", Path = "", Kind = "directory",
                    Description = "All IUCN quality reports land here as Markdown (and sometimes CSV)." },
            },
        },
    };

    public static FlowDefinition? Find(string id) => All.FirstOrDefault(f => f.Id == id);
}
