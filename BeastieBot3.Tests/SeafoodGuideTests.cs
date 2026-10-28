using System.Text.Json;
using BeastieBot3.StatusLists;

namespace BeastieBot3.Tests;

// `seafood goodfish-import` and `seafood seafoodwatch-import`: the names read from a guide's list, a
// GoodFish page and a Seafood Watch recommendation. Names and fields are from the guides (October 2026).
public sealed class SeafoodGuideTests {
    private static readonly IReadOnlyDictionary<string, string> NoFixes = new Dictionary<string, string>();

    // The scientific names read, joined by " | ", with "-" for a name that has none.
    private static string Scientific(string written, IReadOnlyDictionary<string, string>? fixes = null) =>
        string.Join(" | ", SeafoodNames.Read(SeafoodNames.Split(written), fixes ?? NoFixes).Select(n => n.ScientificName ?? "-"));

    [Fact]
    public void AbbreviatedGenusTakesTheFullNameOnTheSameRating() =>
        Assert.Equal("Haliotis conicopora | Haliotis laevigata | Haliotis roei | Haliotis rubra",
            Scientific("H. conicopora, H. laevigata, H. roei, Haliotis rubra"));

    [Fact]
    public void AbbreviationWithTwoPossibleGeneraIsNotExpanded() =>
        Assert.Equal("- | Sepia rosella | Sepioteuthis australis", Scientific("S. apama, Sepia rosella, Sepioteuthis australis"));

    [Fact]
    public void EpithetsArePutInLowerCase() =>
        Assert.Equal("Scortum barcoo | Katelysia peronei | Katelysia scalarina", Scientific("Scortum Barcoo, K. Peronei, Katelysia scalarina"));

    [Fact]
    public void GeneraAndGroupsHaveNoScientificName() =>
        Assert.Equal("- | - | - | -", Scientific("Sepia spp., Various species, Various Chaerodon species, Oreochromis niloticus x Oreochromis aureus"));

    [Fact]
    public void BracketedSubgenusAndOtherNameAreLeftOut() =>
        Assert.Equal("Penaeus indicus | Octopus americanus", Scientific("Penaeus (Fenneropenaeus) indicus, Octopus americanus (O. vulgaris)"));

    [Fact]
    public void SlashedEpithetTakesTheGenusBeforeIt() =>
        Assert.Equal("Sebastes melanostictus | Sebastes aleutianus | Sebastes crocotulus | Sebastes miniatus",
            Scientific("Sebastes melanostictus/aleutianus, Sebastes crocotulus/S. miniatus"));

    [Fact]
    public void FixReplacesTheNameAsRead() {
        var fixes = SeafoodNames.FixesFromYaml("""
            fixes:
              - guide: goodfish
                name: Katelysia peronei
                means: Katelysia peronii
              - guide: seafoodwatch
                name: Sardinops sagax
                means: Wrong guide
            """, SeafoodGuideKeys.GoodFish);
        Assert.Equal("Katelysia peronii | Katelysia scalarina | Sardinops sagax", Scientific("K. Peronei, Katelysia scalarina, Sardinops sagax", fixes));
    }

    [Fact]
    public void AbbreviationAloneTakesTheNameFromOtherRatings() {
        SeafoodRating Rating(string id, string names) => new(SeafoodGuideKeys.GoodFish, id, "Australian salmon", null, ["Eat Less"], null, null,
            null, null, [], null, [], null, "https://goodfish.org.au/", null, null, SeafoodNames.Read(SeafoodNames.Split(names), NoFixes));
        var ratings = SeafoodNames.ExpandAcrossRatings([Rating("1", "A. truttaceus, Arripis trutta"), Rating("2", "A. truttaceus"), Rating("3", "K. peronii")]);
        Assert.Equal("Arripis truttaceus", ratings[1].Names[0].ScientificName);
        Assert.Null(ratings[2].Names[0].ScientificName);
    }

    // The part of the Bluespotted emperor page that GoodFishGuide keeps, shortened.
    private const string BluespottedEmperor = """
        <div class="species-meta"><div class="classifications"><ul>
          <li data-link="better-choice" class="classification-link classification classification--better-choice sm ls">Better Choice</li>
          <li data-link="say-no" class="classification-link classification classification--say-no sm ls">Say No</li>
        </ul></div>
        <div class="flex-item page-title"><h1>Bluespotted emperor</h1>
        <h3 class="blue">Latin name: Lethrinus punctulatus              <h3><br></h3>
        <h3 class="blue">Common name: Norwest snapper            </div>
        <div class="species-information__content"><div class="classifications"><ul>
          <li class="classification classification--better-choice sm ls">Better Choice</li>
          <li class="classification classification--say-no sm ls">Say No</li></ul></div>
        <h3 class="species-block__title grey">Wild Caught</h3> <h3 class="species-block__title">Region:<br>WA</h3>
        <div class="note"> <span class="green"></span> <p><strong>Note:</strong> Choose line and trap caught blue spotted emperor if available.</p> </div>
        <div class="species-block__more-information toggle-block"> <p class="toggle-button"><span class="toggle-icon"></span><strong>More information</strong></p>
        <div class="toggle-content text-rules text-black"><ul> <li>WA Northern Demersal Scalefish Fishery and Pilbara Demersal Scalefish Fishery (284t in 2021)</li> </ul>
        <p>Bluespotted emperor is a tropical reef fish.</p>
        """;

    [Fact]
    public void GoodFishPageWithTwoRatings() {
        var rating = GoodFishGuide.ReadPage(1658, "https://goodfish.org.au/species/bluespotted-emperor/", null, "2026-08-06T07:24:06.0000000Z",
            BluespottedEmperor, NoFixes);
        Assert.NotNull(rating);
        Assert.Equal("Bluespotted emperor", rating.CommonName);
        Assert.Equal(["Better Choice", "Say No"], rating.Ratings);
        Assert.Equal("WA", rating.Region);
        Assert.Equal(["Wild Caught"], rating.Methods);
        Assert.Equal("wild", rating.Production);
        Assert.Equal(["WA Northern Demersal Scalefish Fishery and Pilbara Demersal Scalefish Fishery (284t in 2021)"], rating.Fisheries);
        Assert.Equal("Choose line and trap caught blue spotted emperor if available.", rating.Note);
        Assert.Equal("Lethrinus punctulatus", Assert.Single(rating.Names).ScientificName);
    }

    [Fact]
    public void GoodFishPageWithNoRatingIsLeftOut() =>
        Assert.Null(GoodFishGuide.ReadPage(345, "https://goodfish.org.au/species/mackerel-icefish/", null, null,
            "<div class=\"species-meta\"><div class=\"classifications\"><ul></ul></div><h1>Mackerel icefish</h1>", NoFixes));

    [Theory]
    [InlineData("Farmed", "farmed")]
    [InlineData("Australian Farmed", "farmed")]
    [InlineData("Wild Caught", "wild")]
    [InlineData(null, null)]
    public void GoodFishProduction(string? method, string? production) => Assert.Equal(production, GoodFishGuide.Production(method));

    [Fact]
    public void SeafoodWatchMethodsAndProduction() {
        var (methods, production) = SeafoodWatchGuide.Methods("Bottom trawls#Wild|Longlines#Wild|Bottom trawls#Wild");
        Assert.Equal(["Bottom trawls", "Longlines"], methods);
        Assert.Equal("wild", production);
        Assert.Equal("wild|farmed", SeafoodWatchGuide.Methods("Net pens#Farmed|Gillnets#Wild").Production);
    }

    [Fact]
    public void SeafoodWatchRecommendation() {
        using var document = JsonDocument.Parse("""
            {"groupId":"64","sName":"Oncorhynchus kisutch","country":"United States,Domestic","method":"Gillnets and entangling nets#Wild",
             "bow":"Columbia River","detail":"Coho fishery below Bonneville Dam|Zones 1-5","color":"Yellow","buyText":"Good Alternative",
             "link":"/recommendation/salmon/coho-salmon-1436?species=218","score":"2.674","groupName":"Salmon","formatedC":"United States",
             "id":40072,"name":"Coho salmon","summary":"<p>Not stored.</p>"}
            """);
        var rating = SeafoodWatchGuide.ReadRecommendation(document.RootElement, "Salmon", NoFixes);
        Assert.NotNull(rating);
        Assert.Equal("40072", rating.RatingId);
        Assert.Equal("Coho salmon", rating.CommonName);
        Assert.Equal(["Good Alternative"], rating.Ratings);
        Assert.Equal("Yellow", rating.Color);
        Assert.Equal("United States", rating.Region);
        Assert.Equal("Columbia River", rating.WaterBody);
        Assert.Equal(["Coho fishery below Bonneville Dam", "Zones 1-5"], rating.Fisheries);
        Assert.Equal("https://www.seafoodwatch.org/recommendation/salmon/coho-salmon-1436?species=218", rating.Url);
        Assert.Equal("Oncorhynchus kisutch", Assert.Single(rating.Names).ScientificName);
    }

    [Fact]
    public void SeafoodWatchNotApplicableIsNoValue() {
        using var document = JsonDocument.Parse("""
            {"sName":"Thunnus albacares","country":"N/A","formatedC":"N/A","method":"Floating object purse seine (FAD)#Wild",
             "bow":"Eastern Indian Ocean,Western Indian Ocean","detail":"N/A","color":"Red","buyText":"Avoid","id":75259,"name":"Yellowfin tuna"}
            """);
        var rating = SeafoodWatchGuide.ReadRecommendation(document.RootElement, null, NoFixes)!;
        Assert.Null(rating.Region);
        Assert.Empty(rating.Fisheries);
        Assert.Equal("Eastern Indian Ocean, Western Indian Ocean", rating.WaterBody);
    }
}
