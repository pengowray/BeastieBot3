using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Command to generate YAML configuration for marine mammals virtual groups.
/// </summary>
/// <remarks>
/// <para>
/// This command queries the IUCN database for taxa tagged with the "Marine" system
/// and analyzes which mammalian families are fully marine vs partially marine.
/// </para>
/// <para>
/// <b>IMPORTANT:</b> Unlike Squamata (snakes/lizards) or Artiodactyla (cetaceans/ungulates),
/// the marine mammals grouping is only relevant for a dedicated "List of marine mammals" article.
/// General mammal lists should NOT use virtual groups to separate pinnipeds from other carnivores.
/// The output from this command is for reference when creating marine-specific list configurations.
/// </para>
/// <para>
/// <b>Usage:</b> <c>wikipedia marine-mammals-config [--output path]</c>
/// </para>
/// <para>
/// <b>Output:</b>
/// <list type="bullet">
/// <item>Analysis of which families are fully marine vs partially marine</item>
/// <item>Family lists for each marine mammal group</item>
/// <item>Summary table of marine mammals by order</item>
/// </list>
/// </para>
/// <para>
/// <b>Marine mammal groups identified:</b>
/// <list type="bullet">
/// <item><b>Cetaceans</b> (Artiodactyla): whales, dolphins, porpoises - 100% marine families</item>
/// <item><b>Pinnipeds</b> (Carnivora): seals, sea lions, walrus - Odobenidae, Otariidae, Phocidae</item>
/// <item><b>Sirenians</b> (Sirenia): manatees, dugongs - entire order is marine</item>
/// </list>
/// </para>
/// <para>
/// <b>Partially marine families</b> (individual species, not family-level):
/// <list type="bullet">
/// <item>Mustelidae: sea otters and some river otters</item>
/// <item>Ursidae: polar bear</item>
/// <item>Canidae: arctic fox (coastal populations)</item>
/// </list>
/// </para>
/// </remarks>
internal sealed class MarineMammalsConfigCommand : Command<MarineMammalsConfigCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("--database <PATH>")]
        public string? DatabasePath { get; init; }

        [CommandOption("--output <PATH>")]
        public string? OutputPath { get; init; }

        [CommandOption("-s|--settings-dir <DIR>")]
        public string? SettingsDirectory { get; init; }

        [CommandOption("--ini-file <FILE>")]
        public string? IniFile { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = new PathsService(settings.SettingsDirectory, settings.IniFile);
        var dbPath = paths.ResolveIucnDatabasePath(settings.DatabasePath);

        if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath)) {
            AnsiConsole.MarkupLine("[red]IUCN database not found.[/]");
            return 1;
        }

        using var connection = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly");
        connection.Open();

        // Get all marine mammal data grouped by order and family
        var marineMammals = GetMarineMammalData(connection);

        // Analyze which families are fully marine vs partially marine
        var familyAnalysis = AnalyzeFamilies(connection, marineMammals);

        // Generate YAML output
        var yaml = GenerateYaml(marineMammals, familyAnalysis);

        if (!string.IsNullOrWhiteSpace(settings.OutputPath)) {
            File.WriteAllText(settings.OutputPath, yaml);
            AnsiConsole.MarkupLine($"[green]Configuration written to {settings.OutputPath}[/]");
        } else {
            AnsiConsole.WriteLine(yaml);
        }

        // Print summary
        PrintSummary(marineMammals, familyAnalysis);

        return 0;
    }

    private record MarineMammalRecord(
        string OrderName,
        string FamilyName,
        string ScientificName,
        long TaxonId,
        string Systems,
        string? RedlistCategory);

    private List<MarineMammalRecord> GetMarineMammalData(SqliteConnection connection) {
        var results = new List<MarineMammalRecord>();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT DISTINCT 
                orderName, 
                familyName, 
                scientificName,
                taxonId,
                systems,
                redlistCategory
            FROM view_assessments_taxonomy 
            WHERE className = 'MAMMALIA' 
              AND systems LIKE '%Marine%'
            ORDER BY orderName, familyName, scientificName";

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            results.Add(new MarineMammalRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt64(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5)));
        }

        return results;
    }

    private record FamilyAnalysis(
        string FamilyName,
        string OrderName,
        int TotalSpecies,
        int MarineSpecies,
        bool IsFullyMarine,
        bool IsPinniped);

    private Dictionary<string, FamilyAnalysis> AnalyzeFamilies(
        SqliteConnection connection, 
        List<MarineMammalRecord> marineMammals) {
        
        var results = new Dictionary<string, FamilyAnalysis>(StringComparer.OrdinalIgnoreCase);

        // Get total species count per family
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT familyName, orderName, COUNT(DISTINCT taxonId) as total
            FROM view_assessments_taxonomy 
            WHERE className = 'MAMMALIA'
            GROUP BY familyName, orderName";

        var totalCounts = new Dictionary<string, (string Order, int Total)>(StringComparer.OrdinalIgnoreCase);
        using (var reader = cmd.ExecuteReader()) {
            while (reader.Read()) {
                totalCounts[reader.GetString(0)] = (reader.GetString(1), reader.GetInt32(2));
            }
        }

        // Count marine species per family
        var marineCounts = marineMammals
            .GroupBy(m => m.FamilyName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

        // Pinniped families (seals, sea lions, walruses)
        var pinnipedFamilies = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            "ODOBENIDAE", "OTARIIDAE", "PHOCIDAE"
        };

        foreach (var family in marineCounts.Keys) {
            if (!totalCounts.TryGetValue(family, out var total)) continue;
            
            var marineCount = marineCounts[family];
            var isFullyMarine = marineCount >= total.Total * 0.9; // 90%+ marine = fully marine
            
            results[family] = new FamilyAnalysis(
                family,
                total.Order,
                total.Total,
                marineCount,
                isFullyMarine,
                pinnipedFamilies.Contains(family));
        }

        return results;
    }

    private string GenerateYaml(
        List<MarineMammalRecord> marineMammals,
        Dictionary<string, FamilyAnalysis> familyAnalysis) {
        
        var sb = new StringBuilder();
        sb.AppendLine("# Marine mammals configuration for taxon-rules.yml");
        sb.AppendLine("# Generated from IUCN data - taxa tagged with 'Marine' system");
        sb.AppendLine($"# Generated: {DateTime.UtcNow:yyyy-MM-dd}");
        sb.AppendLine();
        sb.AppendLine("# Add this to the virtual_groups section of taxon-rules.yml");
        sb.AppendLine();

        // Group by order
        var byOrder = marineMammals
            .GroupBy(m => m.OrderName, StringComparer.OrdinalIgnoreCase)
            .OrderBy(g => g.Key)
            .ToList();

        foreach (var orderGroup in byOrder) {
            var orderName = ToTitleCase(orderGroup.Key);
            
            // Skip Artiodactyla - cetaceans are already handled
            if (orderName.Equals("Artiodactyla", StringComparison.OrdinalIgnoreCase)) {
                sb.AppendLine($"  # {orderName}: Cetaceans already handled in existing virtual_groups");
                sb.AppendLine();
                continue;
            }

            // Sirenia is all marine
            if (orderName.Equals("Sirenia", StringComparison.OrdinalIgnoreCase)) {
                sb.AppendLine($"  # {orderName}: All marine (dugongs and manatees)");
                sb.AppendLine($"  # No virtual_groups needed - entire order is marine");
                sb.AppendLine();
                continue;
            }

            // Carnivora needs special handling
            if (orderName.Equals("Carnivora", StringComparison.OrdinalIgnoreCase)) {
                GenerateCarnivoraYaml(sb, orderGroup.ToList(), familyAnalysis);
            }
        }

        sb.AppendLine();
        sb.AppendLine("# Fully marine families (for reference):");
        foreach (var fa in familyAnalysis.Values.Where(f => f.IsFullyMarine).OrderBy(f => f.OrderName).ThenBy(f => f.FamilyName)) {
            sb.AppendLine($"#   {fa.FamilyName} ({fa.OrderName}): {fa.MarineSpecies}/{fa.TotalSpecies} marine");
        }

        sb.AppendLine();
        sb.AppendLine("# Partially marine families (for reference):");
        foreach (var fa in familyAnalysis.Values.Where(f => !f.IsFullyMarine).OrderBy(f => f.OrderName).ThenBy(f => f.FamilyName)) {
            sb.AppendLine($"#   {fa.FamilyName} ({fa.OrderName}): {fa.MarineSpecies}/{fa.TotalSpecies} marine");
        }

        return sb.ToString();
    }

    private void GenerateCarnivoraYaml(
        StringBuilder sb, 
        List<MarineMammalRecord> carnivora,
        Dictionary<string, FamilyAnalysis> familyAnalysis) {
        
        sb.AppendLine("  # Carnivora: separate marine carnivores (pinnipeds, sea otters, polar bears)");
        sb.AppendLine("  Carnivora:");
        sb.AppendLine("    groups:");
        sb.AppendLine("      - name: Pinnipeds");
        sb.AppendLine("        common_name: pinniped");
        sb.AppendLine("        common_plural: pinnipeds");
        sb.AppendLine("        main_article: Pinniped");
        sb.AppendLine("        families:");

        // Pinniped families (fully marine)
        var pinnipedFamilies = new[] { "ODOBENIDAE", "OTARIIDAE", "PHOCIDAE" };
        foreach (var family in pinnipedFamilies) {
            if (familyAnalysis.ContainsKey(family)) {
                sb.AppendLine($"          - {ToTitleCase(family)}");
            }
        }

        sb.AppendLine();
        sb.AppendLine("      - name: Other carnivores");
        sb.AppendLine("        common_name: carnivore");
        sb.AppendLine("        common_plural: carnivores");
        sb.AppendLine("        main_article: Carnivora");
        sb.AppendLine("        default: true");
        sb.AppendLine();

        // List individual marine species from partially marine families
        var partiallyMarineFamilies = carnivora
            .Where(c => !pinnipedFamilies.Contains(c.FamilyName, StringComparer.OrdinalIgnoreCase))
            .GroupBy(c => c.FamilyName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (partiallyMarineFamilies.Count > 0) {
            sb.AppendLine("  # Note: These families have some marine species but are not fully marine:");
            foreach (var family in partiallyMarineFamilies) {
                var fa = familyAnalysis.GetValueOrDefault(family.Key);
                sb.AppendLine($"  # {family.Key}: {fa?.MarineSpecies ?? family.Count()}/{fa?.TotalSpecies ?? 0} species are marine");
                foreach (var species in family.OrderBy(s => s.ScientificName)) {
                    sb.AppendLine($"  #   - {species.ScientificName}");
                }
            }
        }
    }

    private void PrintSummary(
        List<MarineMammalRecord> marineMammals,
        Dictionary<string, FamilyAnalysis> familyAnalysis) {
        
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Marine Mammals Summary[/]");
        AnsiConsole.WriteLine();

        var table = new Table();
        table.AddColumn("Order");
        table.AddColumn("Families");
        table.AddColumn("Marine Taxa");

        var byOrder = marineMammals
            .GroupBy(m => m.OrderName)
            .OrderBy(g => g.Key);

        foreach (var orderGroup in byOrder) {
            var families = orderGroup
                .Select(m => m.FamilyName)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Count();
            
            table.AddRow(
                ToTitleCase(orderGroup.Key),
                families.ToString(),
                orderGroup.Count().ToString());
        }

        AnsiConsole.Write(table);

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine($"[bold]Total marine mammal taxa:[/] {marineMammals.Count}");
        AnsiConsole.MarkupLine($"[bold]Fully marine families:[/] {familyAnalysis.Values.Count(f => f.IsFullyMarine)}");
        AnsiConsole.MarkupLine($"[bold]Partially marine families:[/] {familyAnalysis.Values.Count(f => !f.IsFullyMarine)}");
    }

    private static string ToTitleCase(string value) {
        if (string.IsNullOrWhiteSpace(value)) return value;
        return char.ToUpperInvariant(value[0]) + value[1..].ToLowerInvariant();
    }
}
