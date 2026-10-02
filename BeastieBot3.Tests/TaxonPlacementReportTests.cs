using BeastieBot3.Col;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// The placement report lists the CoL groups placed per IUCN parent with species counts, and the
// groups left out with the reason.
public class TaxonPlacementReportTests {
    private static IReadOnlyList<ColLineageNode> Lineage(string spec) =>
        spec.Split('|').Select(s => s.Split(':')).Select(p => new ColLineageNode(p[1], p[1], p[0])).ToList();

    private static IEnumerable<PlacementSample> Species(int count, string order, string family, string genus, string lineage) =>
        Enumerable.Range(0, count).Select(_ => new PlacementSample("ANIMALIA", "ACTINOPTERYGII", order, family, genus, Lineage(lineage)));

    private static string Report() {
        const string fish = "kingdom:Animalia|gigaclass:Actinopterygii|class:Teleostei";
        var samples = Species(60, "PERCIFORMES", "PERCIDAE", "Perca", $"{fish}|order:Perciformes|suborder:Percoidei|family:Percidae|genus:Perca")
            .Concat(Species(20, "SCORPAENIFORMES", "SCORPAENIDAE", "Scorpaena", $"{fish}|order:Perciformes|suborder:Scorpaenoidei|family:Scorpaenidae|genus:Scorpaena"))
            .Concat(Species(6, "PERCIFORMES", "MIXIDAE", "Mixus", $"{fish}|order:Perciformes|suborder:Percoidei|family:Mixidae|genus:Mixus"))
            .Concat(Species(4, "PERCIFORMES", "MIXIDAE", "Mixus", $"{fish}|order:Perciformes|suborder:Blennioidei|family:Mixidae|genus:Mixus"))
            .ToList();
        var output = TaxonPlacementBuilder.Build(samples);
        var matching = new PlacementMatchStats(samples.Count, samples.Count, samples.Count, 0,
            new Dictionary<ColMatchKind, int> { [ColMatchKind.Accepted] = samples.Count }, 0, 0, 10, true);
        var status = new TaxonPlacementStatus(PlacementState.NoFile, "/col.placement.sqlite", "k", null);
        var result = new PlacementRunResult(output.Index, true, output, matching, status, null, TimeSpan.FromSeconds(3));
        return TaxonPlacementReport.Build(result, "/iucn.sqlite", "/col.sqlite");
    }

    [Fact]
    public void Report_ShowsGroupsWithSpeciesCounts() {
        var report = Report();
        Assert.Contains("### Perciformes (Actinopterygii)", report);
        Assert.Contains("- **Percoidei** (suborder): 60 species. Families: Percidae 60", report);
        Assert.Contains("### Scorpaeniformes (Actinopterygii)", report);
        Assert.Contains("- **Scorpaenoidei** (suborder): 20 species. Families: Scorpaenidae 20", report);
        Assert.Contains("- **Teleostei** (CoL rank: class, not shown in headings): 90 species", report);
    }

    [Fact]
    public void Report_ListsGroupsLeftOut() {
        var report = Report();
        Assert.Contains("| order and family | Scorpaeniformes | Perciformes (order) | 1 | 20 | 22% of 90 | Perciformes (70 of 90) |", report);
        Assert.Contains("| order and family | Mixidae | Perciformes | 10 | Percoidei (suborder) 60%, Blennioidei (suborder) 40% |", report);
    }
}
