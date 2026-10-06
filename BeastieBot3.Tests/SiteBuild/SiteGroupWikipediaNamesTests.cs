using BeastieBot3.SiteBuild;
using BeastieBot3.Wikipedia;

namespace BeastieBot3.Tests.SiteBuild;

// Which English Wikipedia titles a group takes as names (SiteGroupWikipediaNames).
public class SiteGroupWikipediaNamesTests {
    private static WikiGroupArticle Article(string title, string? taxobox, bool redirected = false, bool disambiguation = false) =>
        new(1, title, title, disambiguation, taxobox, redirected);

    [Fact]
    public void ArticleWhoseTaxoboxIsTheGroupIsTheGroups() {
        Assert.True(SiteGroupWikipediaNames.IsAboutGroup("Pteropodidae", Article("Megabat", "Pteropodidae", redirected: true), false, false));
        Assert.True(SiteGroupWikipediaNames.IsAboutGroup("Chiroptera", Article("Bat", "Chiroptera"), false, false));
        Assert.True(SiteGroupWikipediaNames.IsAboutGroup("Pteropodidae", Article("Megabat", "''Pteropodidae''"), false, false));
    }

    [Fact]
    public void MonotypicGenusDoesNotTakeTheSpeciesArticle() {
        // Orycteropus redirects to Aardvark, whose taxobox is the species.
        Assert.False(SiteGroupWikipediaNames.IsAboutGroup("Orycteropus", Article("Aardvark", "Orycteropus afer", redirected: true), false, false));
    }

    [Fact]
    public void ArticleWithNoTaxoboxNeedsARedirectAndNoTaxonMatchedToIt() {
        Assert.True(SiteGroupWikipediaNames.IsAboutGroup("Araneae", Article("Spider", null, redirected: true), false, false));
        Assert.False(SiteGroupWikipediaNames.IsAboutGroup("Araneae", Article("Araneae", null), false, false));
        Assert.False(SiteGroupWikipediaNames.IsAboutGroup("Araneae", Article("Spider", null, redirected: true), false, ownedByTaxon: true));
    }

    [Fact]
    public void DisambiguationPagesAndOtherKingdomsAreNotTheGroups() {
        Assert.False(SiteGroupWikipediaNames.IsAboutGroup("Morus", Article("Morus", null, disambiguation: true), false, false));
        Assert.False(SiteGroupWikipediaNames.IsAboutGroup("Ficus", Article("Ficus", "Ficus"), kingdomConflict: true, false));
    }

    [Fact]
    public void NamesKeepEnglishNamesAndScientificSynonymsAndDropJunk() {
        var redirects = new[] {
            "Fruit bat", "Fruit bats", "Fruit-bat", "Megabats", "Megachiroptera", "Pteropodidae", "Pteropus",
            "Fox Bat's", "MEGABAT", "Megabat (animal)", "Megabbat", "Chiropterology", "Old World fruit bat",
        }.Select(t => new WikipediaIncomingRedirect(t, null)).ToList();
        redirects.Add(new WikipediaIncomingRedirect("Harpyionycterinae", "List of genera"));

        var names = SiteGroupWikipediaNames.Names("Pteropodidae", "Megabat", redirects, key => key == "pteropus");

        Assert.Equal(new[] { "Megabat", "Fruit bat", "Fruit bats", "Fruit-bat", "Megabats", "Megachiroptera", "Old World fruit bat" }, names);
    }

    [Theory]
    [InlineData("chiroptra", "chiroptera", true)]
    [InlineData("megabbat", "megabat", true)]
    [InlineData("megabats", "megabat", false)]
    [InlineData("chiropteran", "chiroptera", false)]
    [InlineData("rat", "bat", false)]
    public void Misspellings(string candidate, string anchor, bool expected) =>
        Assert.Equal(expected, SiteGroupWikipediaNames.LooksLikeMisspelling(candidate, anchor));
}
