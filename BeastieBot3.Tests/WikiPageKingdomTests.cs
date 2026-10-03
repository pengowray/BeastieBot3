using System;
using BeastieBot3.Wikipedia;

namespace BeastieBot3.Tests;

// Pins how a Wikipedia page's kingdom is read (WikiPageKingdom). The matcher matched the plant
// Ficus variegata to "Ficus variegata (gastropod)" and the palm Gaussia princeps to "Gaussia
// princeps (crustacean)" (October 2026); both automatic taxoboxes name their genus with the
// bracketed word ("Ficus (gastropod)") and have no kingdom parameter.
public sealed class WikiPageKingdomTests {
    private const string Plantae = WikiPageKingdom.Plantae;
    private const string Animalia = WikiPageKingdom.Animalia;
    private const string Fungi = WikiPageKingdom.Fungi;
    private const string Chromista = WikiPageKingdom.Chromista;

    private static WikiPageKingdomEvidence Page(string title, string? genus = null, string? kingdom = null, string[]? categories = null) =>
        new(title, kingdom, genus, null, categories ?? []);

    [Theory]
    [InlineData("Ficus (gastropod)", Animalia)]
    [InlineData("Gaussia (crustacean)", Animalia)]
    [InlineData("Ficus variegata (gastropod)", Animalia)]
    [InlineData("Orestias elegans (Fish)", Animalia)]
    [InlineData("Platycleis (bush cricket)", Animalia)]
    [InlineData("Gaussia princeps (plant)", Plantae)]
    [InlineData("Gaussia princeps (palm)", Plantae)]
    [InlineData("Ficus variegata (tree)", Plantae)]
    [InlineData("Ramaria (fungus)", Fungi)]
    public void ABracketedWordForAGroup_NamesItsKingdom(string text, string kingdom) =>
        Assert.Contains(kingdom, WikiPageKingdom.FromQualifier(text)!);

    [Theory]
    [InlineData("Salix fragilis (disambiguation)")]
    [InlineData("Bombus (Pyrobombus)")]
    [InlineData("Turdus (genus)")]
    [InlineData("Hebe (New Zealand)")]
    [InlineData("Leucoraja wallacei")]
    [InlineData(null)]
    public void OtherBracketedWords_NameNoKingdom(string? text) => Assert.Null(WikiPageKingdom.FromQualifier(text));

    [Theory]
    [InlineData("[[Animal]]ia", Animalia)]
    [InlineData("[[Animalia]]", Animalia)]
    [InlineData("[[Fungus|Fungi]]", Fungi)]
    [InlineData("[[Plantae]]", Plantae)]
    public void ATaxoboxKingdomValue_NamesItsKingdom(string value, string kingdom) =>
        Assert.Contains(kingdom, WikiPageKingdom.FromTaxoboxKingdom(value)!);

    [Theory]
    [InlineData("Gastropods described in 1798", Animalia)]
    [InlineData("Crustaceans described in 1894", Animalia)]
    [InlineData("Birds described in 1789", Animalia)]
    [InlineData("Plants described in 1753", Plantae)]
    [InlineData("Lichens described in 1810", Fungi)]
    public void ADescribedInCategory_NamesItsKingdom(string category, string kingdom) =>
        Assert.Contains(kingdom, WikiPageKingdom.FromCategory(category)!);

    [Theory]
    [InlineData("Taxa described in 1800")]
    [InlineData("Taxa named by Johann Friedrich Gmelin")]
    [InlineData("Malagasy warblers")]
    public void OtherCategories_NameNoKingdom(string category) => Assert.Null(WikiPageKingdom.FromCategory(category));

    [Fact]
    public void TheGastropodPage_IsAboutAnotherKingdom_ForThePlantFicusVariegata() {
        var gastropod = Page("Ficus variegata (gastropod)", genus: "Ficus (gastropod)", categories: ["Gastropods described in 1798"]);

        var conflict = WikiPageKingdom.Conflict("PLANTAE", gastropod);

        Assert.NotNull(conflict);
        Assert.Contains("ANIMALIA", conflict);
        Assert.Contains("Ficus (gastropod)", conflict);
        Assert.Null(WikiPageKingdom.Conflict("ANIMALIA", gastropod));
    }

    [Fact]
    public void TheCrustaceanPage_IsAboutAnotherKingdom_ForThePalmGaussiaPrinceps() {
        var crustacean = Page("Gaussia princeps (crustacean)", genus: "Gaussia (crustacean)", categories: ["Crustaceans described in 1894"]);

        Assert.NotNull(WikiPageKingdom.Conflict("PLANTAE", crustacean));
        Assert.Null(WikiPageKingdom.Conflict("ANIMALIA", crustacean));
    }

    [Fact]
    public void ACategoryAlone_IsEnough() {
        // The plant Beilschmiedia madagascariensis, matched through its IUCN synonym Bernieria
        // madagascariensis to the article on the bird.
        var bird = Page("Long-billed bernieria", genus: "Bernieria", categories: ["Birds described in 1789", "Malagasy warblers"]);

        Assert.NotNull(WikiPageKingdom.Conflict("PLANTAE", bird));
    }

    [Fact]
    public void APageThatNamesNoKingdom_IsNotAboutAnotherKingdom() {
        Assert.Null(WikiPageKingdom.Conflict("PLANTAE", Page("Leucoraja wallacei", genus: "Leucoraja")));
        Assert.Null(WikiPageKingdom.Conflict(null, Page("Ficus variegata (gastropod)")));
    }

    [Fact]
    public void APageThatAlsoNamesTheTaxonsKingdom_IsNotAboutAnotherKingdom() {
        var mixed = Page("Ramaria (plant)", categories: ["Fungi described in 1821"]);

        Assert.Null(WikiPageKingdom.Conflict("FUNGI", mixed));
    }

    [Fact]
    public void PlantWords_AllowBrownAlgae() =>
        Assert.Null(WikiPageKingdom.Conflict(Chromista, Page("Ascophyllum (plant)", categories: ["Plants described in 1809"])));

    [Fact]
    public void TheRedirectsTitle_IsReadToo() =>
        Assert.NotNull(WikiPageKingdom.Conflict("PLANTAE", Page("Ficus variegata"), otherTitle: "Ficus variegata (gastropod)"));

    [Fact]
    public void QualifiedTitles_ForThePlant_PutPlantFirst_AndKeepOneTitlePerWord() {
        string[] titles = [
            "Ficus variegata (Tree)", "Ficus variegata (disambiguation)", "Ficus variegata (gastropod)",
            "Ficus variegata (plant)", "Ficus variegata (tree)", "Ficus variegatus (plant)",
        ];

        Assert.Equal(["Ficus variegata (plant)", "Ficus variegata (tree)"],
            WikiPageKingdom.QualifiedTitlesFor("Ficus variegata", "PLANTAE", titles));
        Assert.Equal(["Ficus variegata (gastropod)"], WikiPageKingdom.QualifiedTitlesFor("Ficus variegata", "ANIMALIA", titles));
        Assert.Empty(WikiPageKingdom.QualifiedTitlesFor("Ficus variegata", null, titles));
    }

    [Fact]
    public void QualifiedTitles_ForThePalm() {
        string[] titles = [
            "Gaussia princeps (copepod)", "Gaussia princeps (crustacean)", "Gaussia princeps (disambiguation)",
            "Gaussia princeps (palm)", "Gaussia princeps (plant)", "Gaussia princeps (tree)",
        ];

        Assert.Equal(["Gaussia princeps (plant)", "Gaussia princeps (tree)", "Gaussia princeps (palm)"],
            WikiPageKingdom.QualifiedTitlesFor("Gaussia princeps", "PLANTAE", titles));
    }
}
