using BeastieBot3.Configuration;

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
                new MetricSpec { Label = "backlog (pending)",   Sql = "SELECT COUNT(*) FROM taxa_assessment_backlog" },
                new MetricSpec { Label = "failed requests",     Sql = "SELECT COUNT(*) FROM failed_requests" },
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
                new MetricSpec { Label = "pending matches",   Sql = "SELECT COUNT(*) FROM wikidata_pending_iucn_matches" },
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
                new MetricSpec { Label = "matched taxa",   Sql = "SELECT COUNT(*) FROM taxon_wiki_matches" },
                new MetricSpec { Label = "missing titles", Sql = "SELECT COUNT(*) FROM wiki_missing_titles" },
            },
        },
        new DataSourceDescriptor {
            Id = "common-names",
            Name = "Common names store",
            Kind = "sqlite",
            Description = "Aggregated common-name dictionary with conflict detection.",
            ResolvePath = p => p.GetCommonNameStorePath(),
            Metrics = new[] {
                new MetricSpec { Label = "taxa",         Sql = "SELECT COUNT(*) FROM taxa" },
                new MetricSpec { Label = "common names", Sql = "SELECT COUNT(*) FROM common_names" },
                new MetricSpec { Label = "conflicts",    Sql = "SELECT COUNT(*) FROM common_name_conflicts" },
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
            Id = "reports",
            Name = "Reports output",
            Kind = "directory",
            Description = "Output folder for generated Markdown/CSV reports.",
            ResolvePath = p => p.GetReportOutputDirectory(),
        },
    };
}
