using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Collections.Generic;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.CommonNames;
using BeastieBot3.Configuration;
using BeastieBot3.Wikipedia;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

// CLI entry point for the Australia threatened-species lists. Generates one
// "List of rare and threatened <group> of Australia" wikitext page per major taxonomic group from the
// imported SPRAT database, combining EPBC Act and IUCN threatened status (Phase 1) with the state /
// territory statuses shown inline. Reuses the Wikipedia-list renderer via SpratListGenerator.

namespace BeastieBot3.Sprat;

[CommandInfo("sprat generate-lists", CommandKind.ReadOnly,
    "Generate 'rare and threatened <group> of Australia' wikitext lists from the SPRAT (EPBC) + IUCN data.",
    Reason = "Generates wikitext list output files only.",
    Examples = new[] {
        "sprat generate-lists",
        "sprat generate-lists --group mammals",
        "sprat generate-lists --group dicots --limit 100",
    })]
public sealed class SpratGenerateListsCommand : Command<SpratGenerateListsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--database <PATH>")]
        [System.ComponentModel.Description("Override the SPRAT SQLite path (default: Datastore:SPRAT_sqlite).")]
        public string? DatabasePath { get; init; }

        [CommandOption("--group <ID>")]
        [System.ComponentModel.Description("Filter to specific group ids (repeatable): mammals, birds, reptiles, amphibians, fish, invertebrates, dicots, monocots, ferns-conifers-allies.")]
        public string[]? Groups { get; init; }

        [CommandOption("--output-dir <DIR>")]
        public string? OutputDirectory { get; init; }

        [CommandOption("--rules <FILE>")]
        public string? RulesPath { get; init; }

        [CommandOption("--modernization-rules <FILE>")]
        [System.ComponentModel.Description("Override the taxon-modernization YAML (default: rules/taxon-modernization.yml).")]
        public string? ModernizationRulesPath { get; init; }

        [CommandOption("--limit <N>")]
        [System.ComponentModel.Description("Cap the number of taxa per list (testing).")]
        public int? Limit { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();

        var dbPath = paths.ResolveSpratDatabasePath(settings.DatabasePath);
        if (!File.Exists(dbPath)) {
            AnsiConsole.MarkupLine($"[red]SPRAT database not found:[/] {dbPath}\n[grey]Run[/] [white]sprat import[/] [grey]first.[/]");
            return -1;
        }

        var outputDir = ResolveOutputDir(paths, settings.OutputDirectory);
        var rulesPath = ResolveRulesPath(paths, settings.RulesPath);
        var legacyRules = new LegacyTaxaRuleList(rulesPath);

        var groups = SpratListGroups.All;
        if (settings.Groups is { Length: > 0 }) {
            var wanted = new HashSet<string>(settings.Groups.Select(g => g.Trim()), StringComparer.OrdinalIgnoreCase);
            groups = groups.Where(g => wanted.Contains(g.Id)).ToList();
            if (groups.Count == 0) {
                AnsiConsole.MarkupLine($"[yellow]No SPRAT list groups matched[/] {Markup.Escape(string.Join(", ", settings.Groups))}.");
                AnsiConsole.MarkupLine($"[grey]Available:[/] {string.Join(", ", SpratListGroups.All.Select(g => g.Id))}");
                return 0;
            }
        }

        using var query = new SpratListQueryService(dbPath);

        // Optional aggregated-names hub: real Wikipedia article links (#3) + conventionally-cased
        // names for the taxa it knows; SPRAT vernaculars (sentence-cased via the hub's caps rules, #1)
        // fill the rest. Degrades gracefully when the hub isn't built.
        var commonNamesPath = paths.ResolveCommonNameStorePath(null);
        using var hub = File.Exists(commonNamesPath) ? CommonNameStore.Open(commonNamesPath) : null;
        using var wikiCache = ResolveWikiCache(paths, hub);
        using var provider = hub is not null ? new StoreBackedCommonNameProvider(hub, wikiCache) : null;
        IReadOnlyDictionary<string, string> capsRules = hub?.GetAllCapsRules() ?? new Dictionary<string, string>();
        AnsiConsole.MarkupLine(hub is not null
            ? $"[grey]Aggregated-names hub:[/] {commonNamesPath}"
            : "[yellow]Common-names hub not found; using SPRAT vernaculars only (some links may redlink).[/]");

        var modernizer = TaxonModernizer.Load(ResolveModernizationPath(paths, settings.ModernizationRulesPath));
        var generator = new SpratListGenerator(query, legacyRules, provider, capsRules, modernizer);
        var datasetVersion = query.GetDatasetVersion();
        AnsiConsole.MarkupLine($"[grey]SPRAT dataset:[/] {datasetVersion}");

        var results = new List<SpratListResult>();
        foreach (var group in groups) {
            cancellationToken.ThrowIfCancellationRequested();
            AnsiConsole.MarkupLine($"[grey]Generating[/] [white]{group.Title}[/]...");
            var result = generator.Generate(group, outputDir, settings.Limit);
            results.Add(result);
            AnsiConsole.MarkupLine($"  [green]saved[/] {result.OutputPath} ([cyan]{result.TotalEntries}[/] taxa).");
        }

        AnsiConsole.MarkupLine($"[green]Generated {results.Count} Australia list(s),[/] {results.Sum(r => r.TotalEntries)} taxa total.");

        var modernizations = generator.ModernizationLog.Changes.Count;
        if (modernizations > 0) {
            AnsiConsole.MarkupLine($"[grey]Applied[/] [cyan]{modernizations}[/] [grey]taxonomy modernization(s).[/]");
        }
        return 0;
    }

    private static string ResolveModernizationPath(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }
        return Path.Combine(paths.BaseDirectory, "rules", "taxon-modernization.yml");
    }

    private static WikipediaCacheStore? ResolveWikiCache(PathsService paths, CommonNameStore? hub) {
        if (hub is null) {
            return null;
        }
        var path = paths.GetWikipediaCachePath();
        return !string.IsNullOrWhiteSpace(path) && File.Exists(path) ? WikipediaCacheStore.Open(path) : null;
    }

    private static string ResolveOutputDir(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }
        var configured = paths.GetWikipediaOutputDirectory();
        if (!string.IsNullOrWhiteSpace(configured)) {
            return Path.Combine(Path.GetFullPath(configured), "australia");
        }
        var datastore = paths.GetDatastoreDir();
        if (!string.IsNullOrWhiteSpace(datastore)) {
            return Path.Combine(Path.GetFullPath(datastore), "wikipedia-lists", "australia");
        }
        return Path.Combine(paths.BaseDirectory, "output", "wikipedia", "australia");
    }

    private static string ResolveRulesPath(PathsService paths, string? overridePath) {
        if (!string.IsNullOrWhiteSpace(overridePath)) {
            return Path.GetFullPath(overridePath);
        }
        return Path.Combine(paths.BaseDirectory, "rules", "rules-list.txt");
    }
}
