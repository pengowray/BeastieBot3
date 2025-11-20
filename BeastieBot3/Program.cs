using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;
using System.Threading;

namespace BeastieBot3 {
    // Common CLI settings shared by all commands
    public class CommonSettings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }
    }

    internal class Program {
        static int Main(string[] args) {
            var app = new CommandApp();
            app.Configure(config => {
                config.SetApplicationName("beastiebot3");
                config.ValidateExamples();

                // show-paths (list settings)
                config.AddCommand<ShowPathsCommand>("show-paths")
                    .WithDescription("Show all key/value pairs from the settings INI file.")
                    .WithExample(new[] { "show-paths" })
                    .WithExample(new[] { "show-paths", "--settings-dir", "/config" });

                // col check
                config.AddBranch("col", col => {
                    col.SetDescription("Catalogue of Life related commands");
                    col.AddCommand<CheckColCommand>("check")
                        .WithDescription("Detect the mounted COL dataset inside the container.")
                        .WithExample(new[] { "col", "check" });
                    col.AddCommand<ColImportCommand>("import")
                        .WithDescription("Import Catalogue of Life ColDP zip archives into individual SQLite databases.")
                        .WithExample(new[] { "col", "import" })
                        .WithExample(new[] { "col", "import", "--force" });
                    col.AddCommand<ColSubgenusHomonymReportCommand>("report-subgenus-homonyms")
                        .WithDescription("Report subgenus entries whose names collide with genus names in the COL database.")
                        .WithExample(new[] { "col", "report-subgenus-homonyms" });
                    col.AddCommand<ColNameUsageFieldProfileCommand>("report-nameusage-fields")
                        .WithDescription("Profile COL nameusage fields for whitespace, ASCII coverage, and other text anomalies.")
                        .WithExample(new[] { "col", "report-nameusage-fields" })
                        .WithExample(new[] { "col", "report-nameusage-fields", "--columns", "scientificName,authorship" })
                        .WithExample(new[] { "col", "report-nameusage-fields", "--limit", "100000" });
                });

                config.AddBranch("iucn", iucn => {
                    iucn.SetDescription("IUCN Red List dataset commands");
                    iucn.AddCommand<IucnImportCommand>("import")
                        .WithDescription("Import IUCN CSV data from zip archives into the SQLite datastore.")
                        .WithExample(new[] { "iucn", "import" })
                        .WithExample(new[] { "iucn", "import", "--force" });
                    iucn.AddCommand<IucnHtmlConsistencyCommand>("report-html-consistency")
                        .WithDescription("Compare HTML and plain-text assessment fields for normalization inconsistencies.")
                        .WithExample(new[] { "iucn", "report-html-consistency" })
                        .WithExample(new[] { "iucn", "report-html-consistency", "--limit", "1000" });
                    iucn.AddCommand<IucnTaxonomyConsistencyCommand>("report-taxonomy-consistency")
                        .WithDescription("Rebuild scientific names from taxonomy components and verify field alignment.")
                        .WithExample(new[] { "iucn", "report-taxonomy-consistency" })
                        .WithExample(new[] { "iucn", "report-taxonomy-consistency", "--limit", "5000" });
                    iucn.AddCommand<IucnTaxonomyCleanupCommand>("report-taxonomy-cleanup")
                        .WithDescription("Identify per-record taxonomy fields that need whitespace normalization or marker cleanup.")
                        .WithExample(new[] { "iucn", "report-taxonomy-cleanup" })
                        .WithExample(new[] { "iucn", "report-taxonomy-cleanup", "--limit", "10000" });
                    iucn.AddCommand<IucnColCrosscheckCommand>("report-col-crosscheck")
                        .WithDescription("Crosscheck IUCN species against Catalogue of Life for presence, synonymy, and authority alignment.")
                        .WithExample(new[] { "iucn", "report-col-crosscheck" })
                        .WithExample(new[] { "iucn", "report-col-crosscheck", "--limit", "5000" });

                    iucn.AddBranch("api", api => {
                        api.SetDescription("Commands that cache data from the live IUCN API");
                        api.AddCommand<IucnApiCacheTaxaCommand>("cache-taxa")
                            .WithDescription("Download /api/v4/taxa/sis/{sis_id} payloads into the local API cache.")
                            .WithExample(new[] { "iucn", "api", "cache-taxa" })
                            .WithExample(new[] { "iucn", "api", "cache-taxa", "--limit", "100" })
                            .WithExample(new[] { "iucn", "api", "cache-taxa", "--failed-only" });

                        api.AddCommand<IucnApiCacheAssessmentsCommand>("cache-assessments")
                            .WithDescription("Download /api/v4/assessment/{assessment_id} payloads based on the cached taxa backlog.")
                            .WithExample(new[] { "iucn", "api", "cache-assessments" })
                            .WithExample(new[] { "iucn", "api", "cache-assessments", "--limit", "100" })
                            .WithExample(new[] { "iucn", "api", "cache-assessments", "--failed-only" });

                        api.AddCommand<IucnApiCacheFullCommand>("cache-all")
                            .WithDescription("Run both cache-taxa and cache-assessments sequentially with a single command.")
                            .WithExample(new[] { "iucn", "api", "cache-all" })
                            .WithExample(new[] { "iucn", "api", "cache-all", "--taxa-limit", "100", "--assessment-limit", "200" });
                    });

                    config.AddBranch("wikidata", wikidata => {
                        wikidata.SetDescription("Wikidata caching and reporting commands");
                        wikidata.AddCommand<WikidataSeedCommand>("seed-taxa")
                            .WithDescription("Fetch Wikidata Q-ids for taxa carrying IUCN identifiers and enqueue them for caching.")
                            .WithExample(new[] { "wikidata", "seed-taxa" })
                            .WithExample(new[] { "wikidata", "seed-taxa", "--limit", "1000" });
                        wikidata.AddCommand<WikidataCacheItemsCommand>("cache-entities")
                            .WithDescription("Download Wikidata JSON for queued taxa and populate the local cache, including lookup indexes.")
                            .WithExample(new[] { "wikidata", "cache-entities" })
                            .WithExample(new[] { "wikidata", "cache-entities", "--failed-only" });
                        wikidata.AddCommand<WikidataCacheFullCommand>("cache-all")
                            .WithDescription("Run both the seed and cache steps sequentially.")
                            .WithExample(new[] { "wikidata", "cache-all" })
                            .WithExample(new[] { "wikidata", "cache-all", "--seed-limit", "1000", "--download-limit", "200" });
                        wikidata.AddCommand<WikidataCoverageReportCommand>("report-coverage")
                            .WithDescription("Report how many IUCN taxa currently map to cached Wikidata entities using P627 and scientific-name matches.")
                            .WithExample(new[] { "wikidata", "report-coverage" })
                            .WithExample(new[] { "wikidata", "report-coverage", "--include-subpopulations" });
                    });
                });
            });

            return app.Run(args);
        }
    }
}
