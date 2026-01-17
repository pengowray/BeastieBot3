using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

/// <summary>
/// Initializes the common name store, importing taxa from IUCN and caps rules from caps.txt.
/// </summary>
internal sealed class CommonNameInitCommand : AsyncCommand<CommonNameInitCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("-d|--database <PATH>")]
        [Description("Path to the common names SQLite database. Defaults to paths.ini value.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--caps-file <PATH>")]
        [Description("Path to caps.txt file. Defaults to rules/caps.txt.")]
        public string? CapsFilePath { get; init; }

        [CommandOption("--iucn-database <PATH>")]
        [Description("Path to the IUCN SQLite database. Defaults to paths.ini value.")]
        public string? IucnDatabasePath { get; init; }

        [CommandOption("--skip-taxa")]
        [Description("Skip importing taxa (only import caps rules).")]
        public bool SkipTaxa { get; init; }

        [CommandOption("--skip-caps")]
        [Description("Skip importing caps rules (only import taxa).")]
        public bool SkipCaps { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Limit number of taxa to import (for testing).")]
        public int? Limit { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.IniFile);

        // Resolve database paths
        var commonNameDbPath = paths.ResolveCommonNameStorePath(settings.DatabasePath);
        AnsiConsole.MarkupLine($"[blue]Common name store:[/] {commonNameDbPath}");

        using var store = CommonNameStore.Open(commonNameDbPath);

        // Import caps rules
        if (!settings.SkipCaps) {
            await ImportCapsRulesAsync(store, settings.CapsFilePath);
        }

        // Import taxa from IUCN
        if (!settings.SkipTaxa) {
            var iucnDbPath = paths.ResolveIucnDatabasePath(settings.IucnDatabasePath);
            await ImportIucnTaxaAsync(store, iucnDbPath, settings.Limit);
        }

        // Show statistics
        var stats = store.GetStatistics();
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[green]Common name store initialized:[/]");
        var table = new Table();
        table.AddColumn("Metric");
        table.AddColumn(new TableColumn("Count").RightAligned());
        table.AddRow("Taxa", stats.TaxaCount.ToString("N0"));
        table.AddRow("Synonyms", stats.SynonymCount.ToString("N0"));
        table.AddRow("Common Names", stats.CommonNameCount.ToString("N0"));
        table.AddRow("Conflicts", stats.ConflictCount.ToString("N0"));
        table.AddRow("Caps Rules", store.GetCapsRuleCount().ToString("N0"));
        AnsiConsole.Write(table);

        return 0;
    }

    private static Task ImportCapsRulesAsync(CommonNameStore store, string? capsFilePath) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Importing caps rules...[/]");

            string path;
            try {
                path = capsFilePath ?? CapsFileParser.GetDefaultCapsFilePath();
            }
            catch (FileNotFoundException ex) {
                AnsiConsole.MarkupLine($"[red]Warning:[/] {ex.Message}");
                return;
            }

            AnsiConsole.MarkupLine($"[blue]Caps file:[/] {path}");

            var rules = CapsFileParser.ParseFile(path);
            var imported = 0;

            AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .Start("Loading caps rules...", ctx => {
                    foreach (var rule in rules) {
                        store.InsertCapsRule(rule.LowercaseWord, rule.CorrectForm, rule.Examples);
                        imported++;
                    }
                });

            AnsiConsole.MarkupLine($"[green]Imported {imported:N0} caps rules[/]");
        });
    }

    private static Task ImportIucnTaxaAsync(CommonNameStore store, string iucnDbPath, int? limit) {
        return Task.Run(() => {
            AnsiConsole.MarkupLine("[yellow]Importing IUCN taxa...[/]");
            AnsiConsole.MarkupLine($"[blue]IUCN database:[/] {iucnDbPath}");

            if (!File.Exists(iucnDbPath)) {
                AnsiConsole.MarkupLine($"[red]Error:[/] IUCN database not found: {iucnDbPath}");
                return;
            }

            var runId = store.BeginImportRun("taxa_iucn");
            var processed = 0;
            var added = 0;
            var errors = 0;

            // Open connection to IUCN database
            using var iucnConnection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={iucnDbPath};Mode=ReadOnly");
            iucnConnection.Open();
            var iucnRepo = new IucnTaxonomyRepository(iucnConnection);

            // Get unique taxa from IUCN
            var seenTaxonIds = new HashSet<long>();

            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                    new TaskDescriptionColumn(),
                    new ProgressBarColumn(),
                    new PercentageColumn(),
                    new SpinnerColumn())
                .Start(ctx => {
                    // We don't know total count upfront, use indeterminate progress
                    var task = ctx.AddTask("[green]Importing taxa[/]", autoStart: true);
                    task.IsIndeterminate = !limit.HasValue;

                    if (limit.HasValue) {
                        task.MaxValue = limit.Value;
                        task.IsIndeterminate = false;
                    }

                    foreach (var row in iucnRepo.ReadRows(0, System.Threading.CancellationToken.None)) {
                        if (limit.HasValue && added >= limit.Value) {
                            break;
                        }

                        // Skip if we've already seen this taxon (we want unique taxa, not assessments)
                        if (!seenTaxonIds.Add(row.TaxonId)) {
                            continue;
                        }

                        try {
                            processed++;
                            if (limit.HasValue) {
                                task.Increment(1);
                            }

                            // Determine the canonical name and rank
                            var scientificName = row.ScientificNameTaxonomy ?? row.ScientificNameAssessments;
                            if (string.IsNullOrWhiteSpace(scientificName)) {
                                scientificName = ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName);
                            }

                            if (string.IsNullOrWhiteSpace(scientificName)) {
                                errors++;
                                continue;
                            }

                            var canonicalName = ScientificNameNormalizer.Normalize(scientificName);
                            if (canonicalName == null) {
                                errors++;
                                continue;
                            }

                            // Determine rank
                            var rank = DetermineRank(row);

                            // Insert or update the taxon
                            var taxonId = store.InsertOrUpdateTaxon(
                                canonicalName: canonicalName,
                                originalName: scientificName,
                                rank: rank,
                                kingdom: row.KingdomName,
                                isExtinct: false, // IUCN doesn't have this directly; would need to check category
                                isFossil: false,
                                validityStatus: "valid",
                                primarySource: "iucn",
                                primarySourceId: row.TaxonId.ToString()
                            );

                            // Generate and insert synonym variants
                            InsertSynonymVariants(store, taxonId, row);

                            added++;
                        }
                        catch (Exception ex) {
                            errors++;
                            if (errors <= 5) {
                                AnsiConsole.MarkupLine($"[red]Error processing taxon {row.TaxonId}:[/] {ex.Message}");
                            }
                        }
                    }
                });

            store.CompleteImportRun(runId, processed, added, 0, errors);
            AnsiConsole.MarkupLine($"[green]Imported {added:N0} taxa ({errors:N0} errors)[/]");
        });
    }

    private static void InsertSynonymVariants(CommonNameStore store, long taxonId, IucnTaxonomyRow row) {
        // Generate variants from the taxonomy parts
        var variants = ScientificNameNormalizer.GenerateVariants(
            row.GenusName,
            null, // IUCN doesn't have subgenus in the main taxonomy table
            row.SpeciesName,
            row.InfraName,
            row.InfraType
        );

        foreach (var variant in variants) {
            if (variant.VariantType != "canonical") {
                store.InsertSynonym(
                    taxonId,
                    variant.NormalizedForm,
                    variant.OriginalForm,
                    "constructed",
                    variant.VariantType
                );
            }
        }
    }

    private static string DetermineRank(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.InfraName)) {
            return string.IsNullOrWhiteSpace(row.InfraType)
                ? "subspecies"
                : row.InfraType.ToLowerInvariant() switch {
                    "var." or "variety" => "variety",
                    "f." or "forma" => "form",
                    _ => "subspecies"
                };
        }
        if (!string.IsNullOrWhiteSpace(row.SpeciesName)) {
            return "species";
        }
        return "genus";
    }
}
