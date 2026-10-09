using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

public sealed class ClassificationComparisonTests {
    private static LadderStep S(string? rank, string name) => new(rank, name);

    [Fact]
    public void EachGroupIsARowAndTheSameGroupLinesUp() {
        var iucn = new LadderColumn("IUCN", null, [S("kingdom", "Animalia"), S("phylum", "Chordata"), S("class", "Mammalia"), S("order", "Carnivora"),
            S("family", "Felidae"), S("genus", "Panthera"), S("species", "Panthera leo")], Backbone: true);
        var col = new LadderColumn("CoL", null, [S("kingdom", "Animalia"), S("phylum", "Chordata"), S("class", "Mammalia"), S("order", "Carnivora"),
            S("suborder", "Feliformia"), S("family", "Felidae"), S("subfamily", "Pantherinae"), S("genus", "Panthera"), S("species", "Panthera leo (Linnaeus, 1758)")],
            Backbone: true);
        var wikidata = new LadderColumn("Wikidata", null, [S("domain", "Eukaryota"), S("kingdom", "Animalia"), S("phylum", "Chordata"),
            S("class", "Mammalia"), S("subclass", "Theria"), S(null, "Placentalia"), S("order", "Carnivora"), S("suborder", "Feliformia"),
            S("family", "Felidae"), S("subfamily", "Pantherinae"), S("genus", "Panthera"), S("species", "Panthera leo")]);
        var rows = ClassificationComparison.Build([iucn, col, wikidata]);
        Assert.Equal(["domain", "kingdom", "phylum", "class", "subclass", "no rank", "order", "suborder", "family", "subfamily", "genus", "species"],
            rows.Select(r => r.RankLabel));
        // Feliformia: one row for both sources that have it.
        var suborder = rows.Single(r => r.RankLabel == "suborder");
        Assert.Equal(["CoL", "Wikidata"], suborder.Cells.Select((c, i) => c.Step is null ? null : new[] { "IUCN", "CoL", "Wikidata" }[i]).OfType<string>());
        // The rows above order other than kingdom, phylum and class are hidden at first; every row from order down is shown.
        Assert.Equal(["domain", "subclass", "no rank"], rows.Where(r => r.AboveCut).Select(r => r.RankLabel));
        Assert.DoesNotContain(rows, r => r.Cells.Any(c => c.Differs));
    }

    [Fact]
    public void WithNoOrderTheRowsAboveFamilyAreHidden() {
        var a = new LadderColumn("A", null, [S("kingdom", "Plantae"), S("class", "Magnoliopsida"), S("superfamily", "X"), S("family", "Poaceae"),
            S("genus", "Bromus")], Backbone: true);
        var rows = ClassificationComparison.Build([a, a with { Title = "B" }]);
        Assert.Equal(["superfamily"], rows.Where(r => r.AboveCut).Select(r => r.RankLabel));
    }

    [Fact]
    public void AGroupWithAnotherRankInOneSourceShowsItsRank() {
        var a = new LadderColumn("A", null, [S("kingdom", "Animalia"), S("superorder", "Laurasiatheria"), S("order", "Carnivora")]);
        var b = new LadderColumn("B", null, [S("kingdom", "Animalia"), S("magnorder", "Laurasiatheria"), S("order", "Carnivora")]);
        var c = new LadderColumn("C", null, [S("kingdom", "Animalia"), S("superorder", "Laurasiatheria"), S("order", "Carnivora")]);
        var row = ClassificationComparison.Build([a, b, c]).Single(r => r.RankLabel == "superorder");
        Assert.Equal([false, true, false], row.Cells.Select(cell => cell.OtherRank));
    }

    [Fact]
    public void SourcesThatDisagreeOnTheOrderStillGiveEveryGroupOnce() {
        var order = ClassificationComparison.MergeOrder([["a", "x", "y", "b"], ["a", "y", "x", "b"]]);
        Assert.Equal(4, order.Distinct().Count());
        Assert.Equal("a", order[0]);
        Assert.Equal("b", order[^1]);
    }

    [Fact]
    public void AMainRankWithAnotherNameIsMarked() {
        var iucn = new LadderColumn("IUCN", null, [S("kingdom", "Animalia"), S("order", "Cetartiodactyla"), S("family", "Balaenidae"),
            S("species", "Balaena mysticetus")]);
        var col = new LadderColumn("CoL", null, [S("kingdom", "Animalia"), S("order", "Artiodactyla"), S("family", "Balaenidae"),
            S("species", "Balaena mysticetus Linnaeus, 1758")]);
        var rows = ClassificationComparison.Build([iucn, col]);
        Assert.True(rows.Single(r => r.RankLabel == "order").Cells[1].Differs);
        Assert.False(rows.Single(r => r.RankLabel == "species").Cells[1].Differs);
        Assert.False(rows.Single(r => r.RankLabel == "family").Cells[1].Differs);
    }

    [Theory]
    [InlineData("Panthera leo (Linnaeus, 1758)", "panthera leo")]
    [InlineData("PANTHERA LEO", "panthera leo")]
    [InlineData("FELIDAE", "felidae")]
    [InlineData("Felis silvestris ssp. lybica", "felis silvestris ssp. lybica")]
    public void NamesCompareWithoutAuthorityOrCase(string name, string clean) => Assert.Equal(clean, ClassificationComparison.Clean(name));

}
