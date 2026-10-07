using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

public sealed class ClassificationComparisonTests {
    private static LadderStep S(string? rank, string name) => new(rank, name);

    [Fact]
    public void LaddersLineUpOnTheMainRanksWithTheGroupsBetweenUnderThem() {
        var iucn = new LadderColumn("IUCN", null, [S("kingdom", "Animalia"), S("phylum", "Chordata"), S("class", "Mammalia"), S("order", "Carnivora"),
            S("family", "Felidae"), S("genus", "Panthera"), S("species", "Panthera leo")]);
        var wikidata = new LadderColumn("Wikidata", null, [S("domain", "Eukaryota"), S("kingdom", "Animalia"), S("phylum", "Chordata"),
            S("class", "Mammalia"), S("subclass", "Theria"), S(null, "Placentalia"), S("order", "Carnivora"), S("suborder", "Feliformia"),
            S("family", "Felidae"), S("subfamily", "Pantherinae"), S("genus", "Panthera"), S("species", "Panthera leo")]);
        var rows = ClassificationComparison.Build([iucn, wikidata]);
        Assert.Equal([ClassificationComparison.AboveKingdom, "kingdom", "phylum", "class", "order", "family", "genus", "species"], rows.Select(r => r.RankLabel));
        var classRow = rows.Single(r => r.RankLabel == "class");
        Assert.Equal(["Theria", "Placentalia"], classRow.Cells[1].Between.Select(s => s.Name));
        Assert.Empty(classRow.Cells[0].Between);
        Assert.Equal("Eukaryota", Assert.Single(rows[0].Cells[1].Between).Name);
        Assert.DoesNotContain(rows, r => r.Cells.Any(c => c.Differs));
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
