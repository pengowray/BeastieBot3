using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Tests;

public sealed class AreaNamesTests {
    private static readonly AreaNames Names = new([
        new("BR", "Brazil", null),
        new("US", "United States", null),
        new("TZ", "Tanzania, United Republic of", null),
        new("VN", "Viet Nam", null),
        new("CD", "Congo, The Democratic Republic of the", null),
        new("CG", "Congo", null),
        new("GE", "Georgia", null),
        new("GEO-OO", "Georgia", "US"),
        new("HAW-HI", "Hawaiian Is.", "US"),
        new("GB", "United Kingdom", null),
        new("IM", "Isle of Man", null),
        new("WAU-WA", "Western Australia", "AU"),
        new("AU", "Australia", null),
        new("CI", "Côte d'Ivoire", null),
        new("TT", "Trinidad and Tobago", null),
    ]);

    [Theory]
    [InlineData("List of birds of Brazil", "BR")]
    [InlineData("List of mammals of the United States", "US")]
    [InlineData("List of birds of Tanzania", "TZ")]
    [InlineData("List of birds of Vietnam", "VN")]
    [InlineData("List of birds of Viet Nam", "VN")]
    [InlineData("List of birds of the Democratic Republic of the Congo", "CD")]
    [InlineData("List of birds of the Republic of the Congo", "CG")]
    [InlineData("List of birds of Georgia (country)", "GE")]
    [InlineData("List of birds of Hawaii", "HAW-HI")]
    [InlineData("List of birds of Great Britain", "GB")]
    [InlineData("List of birds of the Isle of Man", "IM")]
    [InlineData("List of reptiles of Western Australia", "WAU-WA")]
    [InlineData("List of mammals of Australia", "AU")]
    [InlineData("List of birds of Ivory Coast", "CI")]
    [InlineData("List of birds of Côte d'Ivoire", "CI")]
    [InlineData("List of birds of Trinidad and Tobago", "TT")]
    [InlineData("List of amphibians in Brazil", "BR")]
    [InlineData("List of endemic birds of Brazil", "BR")]
    public void FindsTheAreaATitleIsOf(string title, string code) => Assert.Equal(code, Names.FromTitle(title)?.Code);

    [Theory]
    [InlineData("List of birds of Europe")]
    [InlineData("List of birds")]
    [InlineData("List of parrots")]
    [InlineData("List of birds of the world")]
    [InlineData("List of mammals of South America")]
    public void ATitleOfNoAreaFindsNone(string title) => Assert.Null(Names.FromTitle(title));

    [Theory]
    [InlineData("Tanzania, United Republic of", "Tanzania", "Tanzania")]
    [InlineData("United States", "United States", "the United States")]
    [InlineData("Hawaiian Is.", "Hawaiian Islands", "the Hawaiian Islands")]
    [InlineData("Johnston I.", "Johnston Island", "Johnston Island")]
    [InlineData("Congo, The Democratic Republic of the", "Democratic Republic of the Congo", "the Democratic Republic of the Congo")]
    [InlineData("Viet Nam", "Vietnam", "Vietnam")]
    [InlineData("Netherlands", "Netherlands", "the Netherlands")]
    [InlineData("Brazil", "Brazil", "Brazil")]
    public void NamesReadAsEnglishWikipediaWritesThem(string iucn, string display, string sentence) {
        var area = new AreaName("XX", iucn, null);
        Assert.Equal(display, area.DisplayName);
        Assert.Equal(sentence, area.SentenceName);
    }

    [Fact]
    public void EndemicInTheTitleMeansEndemicSpecies() {
        Assert.Equal(AreaMode.Endemic, AreaNames.ModeFromTitle("List of endemic birds of Brazil"));
        Assert.Equal(AreaMode.Native, AreaNames.ModeFromTitle("List of birds of Brazil"));
    }
}
