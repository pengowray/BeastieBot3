using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// Pins the CoL rank ladder that decides which lineage nodes sit between two IUCN ranks.
public class ColRankOrderTests {
    [Fact]
    public void Ladder_RunsBroadToNarrow() {
        var ranks = new[] {
            "domain", "realm", "superkingdom", "kingdom", "subkingdom", "infrakingdom",
            "superphylum", "phylum", "subphylum", "infraphylum", "parvphylum",
            "gigaclass", "megaclass", "superclass", "class", "subclass", "infraclass", "subterclass", "parvclass",
            "superorder", "order", "suborder", "infraorder", "parvorder", "nanorder",
            "section zoology", "subsection zoology", "series zoology",
            "superfamily", "epifamily", "family", "subfamily", "infrafamily",
            "supertribe", "tribe", "subtribe", "infratribe",
            "genus", "subgenus", "section botany", "subsection botany", "series botany",
            "species aggregate", "species", "subspecies", "variety", "form",
        };
        var ordinals = ranks.Select(r => ColRankOrder.Of(r) ?? throw new Xunit.Sdk.XunitException("No ordinal for " + r)).ToList();
        for (var i = 1; i < ordinals.Count; i++) {
            Assert.True(ordinals[i] > ordinals[i - 1], $"{ranks[i]} should be narrower than {ranks[i - 1]}");
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("unranked")]
    [InlineData("clade")]
    [InlineData("other")]
    [InlineData("no such rank")]
    public void UnknownRanks_HaveNoOrdinal(string? rank) => Assert.Null(ColRankOrder.Of(rank));

    [Theory]
    [InlineData("Suborder", "suborder")]
    [InlineData(" SUPERFAMILY ", "superfamily")]
    [InlineData("Section_Zoology", "section zoology")]
    [InlineData("series  zoology", "series zoology")]
    [InlineData("division", "phylum")]
    [InlineData("subdivision", "subphylum")]
    public void Ranks_AreMatchedLoosely(string input, string canonical) =>
        Assert.Equal(ColRankOrder.Of(canonical), ColRankOrder.Of(input));

    [Fact]
    public void ZoologicalSectionsAndSeries_SitBetweenParvorderAndSuperfamily() {
        foreach (var rank in new[] { "section zoology", "subsection zoology", "series zoology" }) {
            Assert.True(ColRankOrder.Of(rank) > ColRankOrder.Of("parvorder"), rank);
            Assert.True(ColRankOrder.Of(rank) < ColRankOrder.Of("superfamily"), rank);
        }
    }

    [Theory]
    [InlineData("species", true)]
    [InlineData("subspecies", true)]
    [InlineData("variety", true)]
    [InlineData("subvariety", true)]
    [InlineData("form", true)]
    [InlineData("subform", true)]
    [InlineData("proles", true)]
    [InlineData("infraspecific name", true)]
    [InlineData("infrasubspecific name", true)]
    [InlineData("forma specialis", true)]
    [InlineData("natio", true)]
    [InlineData("morph", true)]
    [InlineData("lusus", true)]
    [InlineData("aberration", true)]
    [InlineData("mutatio", true)]
    [InlineData("species aggregate", false)]
    [InlineData("genus", false)]
    [InlineData("subgenus", false)]
    [InlineData("infrageneric name", false)]
    [InlineData("unranked", false)]
    public void IsSpeciesOrBelow(string rank, bool expected) =>
        Assert.Equal(expected, ColRankOrder.IsSpeciesOrBelow(rank));

    [Fact]
    public void Clean_LowerCasesAndNamesBlankUnranked() {
        Assert.Equal("suborder", ColRankOrder.Clean("Suborder"));
        Assert.Equal("unranked", ColRankOrder.Clean(null));
        Assert.Equal("unranked", ColRankOrder.Clean(" "));
        Assert.Equal("clade", ColRankOrder.Clean("Clade"));
    }
}
