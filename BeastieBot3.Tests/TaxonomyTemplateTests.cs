using System.Collections.Generic;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

public class TaxonomyTemplateTests {
    [Fact]
    public void ATemplateGivesItsRankParentAndName() {
        var text = "{{Don't edit this line {{{machine code|}}}\n|rank=subfamilia\n|link=Pantherinae\n|parent=Felidae\n|refs=<!--Shown on this page only-->\n}}";
        Assert.Equal(new TaxonomyTemplate("Pantherinae", "subfamily", "Felidae", "Pantherinae"), TaxonomyTemplates.Parse("Pantherinae", text));
    }

    [Fact]
    public void ALinkWithALabelShowsTheLabel() {
        var text = "{{Don't edit this line {{{machine code|}}}\n|rank=genus\n|link=Panthera (genus)|''Panthera''\n|parent=Pantherinae\n}}";
        Assert.Equal("Panthera", TaxonomyTemplates.Parse("Panthera (genus)", text)!.Display);
    }

    [Theory]
    [InlineData("regnum", "kingdom")]
    [InlineData("ordo", "order")]
    [InlineData("superfamilia", "superfamily")]
    [InlineData("infraordo", "infraorder")]
    [InlineData("clade", "clade")]
    [InlineData("unranked", null)]
    [InlineData("zoodivisio", "zoodivisio")]
    public void LatinRanksReadInEnglish(string latin, string? english) => Assert.Equal(english, TaxonomyTemplates.EnglishRank(latin));

    [Fact]
    public void ATaxoboxStartsFromItsTaxonParentOrGenus() {
        Assert.Equal("Felidae", TaxonomyTemplates.StartOf(new Dictionary<string, string> { ["taxon"] = "Felidae" }));
        Assert.Equal("Panthera", TaxonomyTemplates.StartOf(new Dictionary<string, string> { ["taxon"] = "Panthera leo" }));
        Assert.Equal("Felis (Felis)", TaxonomyTemplates.StartOf(new Dictionary<string, string> { ["genus"] = "Felis", ["parent"] = "Felis (Felis)" }));
        Assert.Equal("Felis", TaxonomyTemplates.StartOf(new Dictionary<string, string> { ["genus"] = "Felis", ["species"] = "silvestris" }));
        Assert.Null(TaxonomyTemplates.StartOf(new Dictionary<string, string> { ["image"] = "x.jpg" }));
    }
}
