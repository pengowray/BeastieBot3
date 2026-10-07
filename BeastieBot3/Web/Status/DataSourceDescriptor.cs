using BeastieBot3.Configuration;
using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Web.Status;

// Static catalogue describing every data source the status dashboard
// introspects. Each descriptor declares how to resolve its path, what kind
// it is (sqlite file or directory), and which row counts are interesting.
//
// Queries are run against a read-only SQLite open so they can never contend
// with a concurrent import. Missing tables are tolerated (a freshly-cloned
// install legitimately has none of these yet).

public sealed record DataSourceDescriptor {
    public required string Id { get; init; }
    public required string Name { get; init; }
    public required string Kind { get; init; }                       // "sqlite" | "directory"
    public required Func<PathsService, string?> ResolvePath { get; init; }
    public IReadOnlyList<MetricSpec> Metrics { get; init; } = Array.Empty<MetricSpec>();
    public string? Description { get; init; }
}

public sealed record MetricSpec {
    public required string Label { get; init; }
    public required string Sql { get; init; }                        // expected to return a single scalar (long)
    // If true, a missing table just records "n/a" instead of raising an error.
    public bool TolerateMissing { get; init; } = true;
}

public static class DataSourceCatalogue {
    public static readonly IReadOnlyList<DataSourceDescriptor> All = new[] {
        new DataSourceDescriptor {
            Id = "iucn-csv-input",
            Name = "IUCN CSV input",
            Kind = "directory",
            Description = "Folder containing the IUCN Red List CSV zip(s) that `iucn import` ingests.",
            ResolvePath = p => p.GetIucnCvsDir(),
        },
        new DataSourceDescriptor {
            Id = "col-input",
            Name = "Catalogue of Life input",
            Kind = "directory",
            Description = "Folder containing the COL ColDP zip archive(s) for `col import`.",
            ResolvePath = p => p.GetColDir(),
        },
        new DataSourceDescriptor {
            Id = "sprat-input",
            Name = "SPRAT (EPBC) input",
            Kind = "directory",
            Description = "Folder holding the Australian SPRAT report CSV that `sprat import` ingests.",
            // SPRAT is configured as a single CSV file; show its containing folder (the status
            // dashboard renders directories, not individual files).
            ResolvePath = p => p.GetSpratCsvPath() is { Length: > 0 } csv
                ? Path.GetDirectoryName(Path.GetFullPath(csv))
                : null,
        },
        new DataSourceDescriptor {
            Id = "gbif-checklist",
            Name = "GBIF checklist",
            Kind = "directory",
            Description = "Folder containing the IUCN Red List checklist zips that IUCN publishes on GBIF, downloaded by `iucn gbif-download`. `iucn resolve-dois` and `site build-db` read the newest zip.",
            ResolvePath = p => p.GetGbifIucnDir(),
        },
        new DataSourceDescriptor {
            Id = "iucn-main",
            Name = "IUCN Red List database",
            Kind = "sqlite",
            Description = "Imported from IUCN CSV via `iucn import`.",
            ResolvePath = p => p.GetIucnDatabasePath(),
            Metrics = new[] {
                new MetricSpec { Label = "assessments",   Sql = "SELECT COUNT(*) FROM assessments_html" },
                new MetricSpec { Label = "taxonomy rows", Sql = "SELECT COUNT(*) FROM taxonomy_html" },
            },
        },
        new DataSourceDescriptor {
            Id = "iucn-api-cache",
            Name = "IUCN API cache",
            Kind = "sqlite",
            Description = "Local cache of /api/v4 taxa and assessment payloads.",
            ResolvePath = p => p.GetIucnApiCachePath(),
            Metrics = new[] {
                new MetricSpec { Label = "taxa cached",         Sql = "SELECT COUNT(*) FROM taxa" },
                new MetricSpec { Label = "assessments cached",  Sql = "SELECT COUNT(*) FROM assessments" },
                // Assessment ids listed by the cached taxa that are neither downloaded nor failed.
                // taxa_assessment_backlog keeps every listed id after its download, so a bare
                // COUNT(*) of it never drops to 0. Failed ids are left out because a 404 tombstone
                // stays in the backlog for good and is already counted under "failed requests".
                // Both lookups are index searches: about 0.15s on a 366k-row backlog.
                new MetricSpec {
                    Label = "assessments to download",
                    Sql = """
                        SELECT COUNT(*) FROM taxa_assessment_backlog b
                        WHERE NOT EXISTS (SELECT 1 FROM assessments a WHERE a.assessment_id = b.assessment_id)
                          AND NOT EXISTS (SELECT 1 FROM failed_requests f
                                          WHERE f.endpoint = 'assessment' AND f.entity_id = CAST(b.assessment_id AS TEXT))
                        """,
                },
                new MetricSpec { Label = "failed requests",     Sql = "SELECT COUNT(*) FROM failed_requests" },
            },
        },
        new DataSourceDescriptor {
            Id = "iucn-api-projected",
            Name = "IUCN API projection",
            Kind = "sqlite",
            Description = "CSV-shaped projection of the API cache for list/chart generation, built by `iucn api project-view`.",
            ResolvePath = p => p.GetIucnApiProjectedPath(),
            Metrics = new[] {
                new MetricSpec { Label = "assessments (latest)", Sql = "SELECT COUNT(*) FROM assessments_html" },
                new MetricSpec { Label = "taxonomy rows",        Sql = "SELECT COUNT(*) FROM taxonomy_html" },
            },
        },
        new DataSourceDescriptor {
            Id = "iucn-doi-cache",
            Name = "DOI cache",
            Kind = "sqlite",
            Description = "DOIs found by `iucn resolve-dois` for assessments that have no DOI from IUCN's citation, the GBIF checklist or Wikidata. `site build-db` reads its DOIs from this cache.",
            ResolvePath = p => p.GetIucnDoiCachePath(),
            // One row per assessment checked, with a NULL doi when none was found. About 0.1s cold
            // on 148k rows; doi is not indexed, so the two splits read the table too.
            Metrics = new[] {
                new MetricSpec { Label = "assessments checked", Sql = "SELECT COUNT(*) FROM doi_check" },
                new MetricSpec { Label = "DOI found",           Sql = "SELECT COUNT(*) FROM doi_check WHERE doi IS NOT NULL" },
                new MetricSpec { Label = "no DOI found",        Sql = "SELECT COUNT(*) FROM doi_check WHERE doi IS NULL" },
            },
        },
        new DataSourceDescriptor {
            Id = "wikidata-cache",
            Name = "Wikidata cache",
            Kind = "sqlite",
            Description = "Wikidata entity payloads + lookup indexes for IUCN taxa.",
            ResolvePath = p => p.GetWikidataCachePath(),
            Metrics = new[] {
                new MetricSpec { Label = "entities cached",   Sql = "SELECT COUNT(*) FROM wikidata_entities WHERE json_downloaded = 1" },
                new MetricSpec { Label = "pending download",  Sql = "SELECT COUNT(*) FROM wikidata_entities WHERE json_downloaded = 0" },
                new MetricSpec { Label = "taxa linked by name or synonym", Sql = "SELECT COUNT(*) FROM wikidata_pending_iucn_matches" },
            },
        },
        new DataSourceDescriptor {
            Id = "wikipedia-cache",
            Name = "Wikipedia cache",
            Kind = "sqlite",
            Description = "Wikipedia HTML+wikitext pages and IUCN-to-page matches.",
            ResolvePath = p => p.GetWikipediaCachePath(),
            Metrics = new[] {
                new MetricSpec { Label = "pages cached",   Sql = "SELECT COUNT(*) FROM wiki_pages" },
                // Only rows the matcher settled on an article; the table also holds pending,
                // missing and rejected rows. Same count as WikipediaCacheStore.GetCacheStats.
                new MetricSpec { Label = "matched taxa",   Sql = "SELECT COUNT(*) FROM taxon_wiki_matches WHERE match_status = 'matched'" },
                new MetricSpec { Label = "titles with no article", Sql = "SELECT COUNT(*) FROM wiki_missing_titles" },
            },
        },
        new DataSourceDescriptor {
            Id = "wikispecies-cache",
            Name = "Wikispecies cache",
            Kind = "sqlite",
            Description = "Wikispecies pages of IUCN taxa and the taxonavigation templates above them, for the species site's comparison of ranks (`wikispecies fetch`).",
            ResolvePath = p => p.GetWikispeciesCachePath(),
            Metrics = new[] {
                new MetricSpec { Label = "pages cached",   Sql = "SELECT COUNT(*) FROM wiki_pages WHERE download_status = 'cached'" },
                new MetricSpec { Label = "titles with no page", Sql = "SELECT COUNT(*) FROM wiki_missing_titles" },
            },
        },
        new DataSourceDescriptor {
            Id = "common-names",
            Name = "Common names store",
            Kind = "sqlite",
            Description = "Common names for IUCN taxa from IUCN, Wikidata, Wikipedia and Catalogue of Life, combined by `common-names aggregate`. `wikipedia generate-lists` reads its common names from this store.",
            ResolvePath = p => p.GetCommonNameStorePath(),
            // No count of shared names: generate-lists works them out by reading every English
            // common name (about 1s on the full store), too slow for a card polled every 10s.
            // `common-names report --report ambiguous` lists them.
            Metrics = new[] {
                new MetricSpec { Label = "taxa",         Sql = "SELECT COUNT(*) FROM taxa" },
                new MetricSpec { Label = "common names", Sql = "SELECT COUNT(*) FROM common_names" },
            },
        },
        new DataSourceDescriptor {
            Id = "col-sqlite",
            Name = "Catalogue of Life database",
            Kind = "sqlite",
            Description = "Imported from a COL ColDP archive via `col import`.",
            ResolvePath = p => p.GetColSqlitePath(),
            Metrics = new[] {
                new MetricSpec { Label = "name usages",      Sql = "SELECT COUNT(*) FROM nameusage" },
                new MetricSpec { Label = "vernacular names", Sql = "SELECT COUNT(*) FROM vernacularname" },
            },
        },
        new DataSourceDescriptor {
            Id = "sprat-sqlite",
            Name = "SPRAT (EPBC) database",
            Kind = "sqlite",
            Description = "Australian EPBC + state/territory + IUCN species statuses, imported from the SPRAT report CSV via `sprat import`.",
            ResolvePath = p => p.GetSpratDatabasePath(),
            Metrics = new[] {
                new MetricSpec { Label = "species", Sql = "SELECT COUNT(*) FROM sprat_species" },
                new MetricSpec {
                    Label = "EPBC threatened",
                    Sql = "SELECT COUNT(*) FROM sprat_species WHERE epbc_status IN ('Critically Endangered','Endangered','Vulnerable')",
                },
            },
        },
        new DataSourceDescriptor {
            Id = "site-sqlite",
            Name = "Site database",
            Kind = "sqlite",
            Description = "Database of the public species site, built by `site build-db`. `deploy/oracle/deploy-db.sh` uploads it to the server.",
            ResolvePath = p => p.GetSiteDatabasePath(),
            // `site build-db` replaces this file with a rename, which is why StatusService opens
            // every database without pooling.
            Metrics = new[] {
                new MetricSpec { Label = "taxa",           Sql = "SELECT COUNT(*) FROM taxon" },
                new MetricSpec { Label = "assessments",    Sql = "SELECT COUNT(*) FROM assessment" },
                new MetricSpec {
                    Label = "schema version",
                    Sql = $"SELECT CAST(value AS INTEGER) FROM meta WHERE key = '{SiteDbSchema.MetaKeys.SchemaVersion}'",
                },
            },
        },
        new DataSourceDescriptor {
            Id = "reports",
            Name = "Reports output",
            Kind = "directory",
            Description = "Output folder for generated Markdown/CSV reports.",
            ResolvePath = p => p.GetReportOutputDirectory(),
        },
    };
}
