using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

public sealed class NameTransliterationTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    [Theory]
    [InlineData("ホッキョクグマ", "ja", "hokkyokuguma")]
    [InlineData("老虎", "zh", "lǎo hǔ")]
    [InlineData("Белый медведь", "ru", "Belyj medvedʹ")]
    [InlineData("Πολική αρκούδα", "el", "Polikḗ arkoúda")]
    [InlineData("ध्रुवीय भालू", "hi", "dhruvīya bhālū")]
    public void ANameInAnotherScriptGetsLatinLetters(string name, string lang, string latin) =>
        Assert.Equal(latin, NameTransliteration.For(name, lang));

    [Theory]
    [InlineData("Ours blanc", "fr")]
    [InlineData("Éléphant d'Afrique", "fr")]
    // Written without most vowels, or only with diacritics few readers know.
    [InlineData("دب قطبي", "ar")]
    [InlineData("דוב קוטב", "he")]
    [InlineData("หมีขั้วโลก", "th")]
    // Chinese characters are read as Mandarin, so only Chinese names get them.
    [InlineData("北極熊", "ja")]
    [InlineData("老虎", "yue")]
    [InlineData("老虎", null)]
    public void OtherNamesGetNone(string name, string? lang) =>
        Assert.Null(NameTransliteration.For(name, lang));

    [Fact]
    public async Task TheTaxonPageShowsTheTransliterationUnderTheName() {
        var html = await factory.Client().GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains("<td lang=\"ja\">トラ <span class=\"transliteration\" lang=\"ja-Latn\" title=\"Transliteration into Latin letters\">tora</span></td>", html);
        Assert.Contains("<td lang=\"de\">Tiger</td>", html);
    }
}
