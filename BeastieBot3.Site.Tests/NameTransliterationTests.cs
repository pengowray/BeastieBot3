using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

public sealed class NameTransliterationTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    [Theory]
    [InlineData("ホッキョクグマ", "ja", "hokkyokuguma")]
    [InlineData("老虎", "zh", "lǎo hǔ")]
    [InlineData("Белый медведь", "ru", "Belyj medvedʹ")]
    [InlineData("Πολική αρκούδα", "el", "Polikḗ arkoúda")]
    [InlineData("ध्रुवीय भालू", "hi", "dhruvīya bhālū")]
    // Malayalam with chillu letters, which ICU4N's data predates.
    [InlineData("\u0D2A\u0D41\u0D32\u0D3F\u0D2F\u0D7B", "ml", "puliyan")]
    // Kazakh: ICU's Cyrillic-Latin has no \u04AF or \u04E9; AnyAscii gives those letters.
    [InlineData("\u049A\u0430\u0440 \u0431\u0430\u0440\u044B\u0441\u044B", "kk", "K\u0326ar barysy")]
    [InlineData("\u0410\u049B \u0430\u044E \u04AF\u04E9", "kk", "Ak\u0326 a\u00FB uo")]
    // Scripts with no ICU4N transform: AnyAscii.
    [InlineData("\u1290\u1265\u122D", "am", "nebr")]
    [InlineData("\u13A0\u13AA\u13D5\u13AF", "chr", "agodehi")]
    // Thaana writes its vowels, so it is kept.
    [InlineData("\u0789\u07AA\u0788\u07A6", "dv", "muva")]
    public void ANameInAnotherScriptGetsLatinLetters(string name, string lang, string latin) =>
        Assert.Equal(latin, NameTransliteration.For(name, lang));

    [Theory]
    [InlineData("Ours blanc", "fr")]
    [InlineData("Éléphant d'Afrique", "fr")]
    // Written without most vowels, or only with diacritics few readers know.
    [InlineData("دب قطبي", "ar")]
    [InlineData("דוב קוטב", "he")]
    [InlineData("หมีขั้วโลก", "th")]
    // Myanmar, Khmer, Sinhala and Tibetan: neither library keeps the inherent vowels.
    [InlineData("\u101C\u1004\u103A\u1038\u1015\u102D\u102F\u1004\u103A", "my")]
    [InlineData("\u0F66\u0F9F\u0F42", "bo")]
    // Chinese characters are read as Mandarin, so only Chinese names get them.
    [InlineData("北極熊", "ja")]
    [InlineData("老虎", "yue")]
    [InlineData("老虎", null)]
    public void OtherNamesGetNone(string name, string? lang) =>
        Assert.Null(NameTransliteration.For(name, lang));

    [Fact]
    public async Task TheTaxonPageShowsTheTransliterationUnderTheName() {
        var html = await factory.Client().GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains("<td lang=\"ja\">トラ <span class=\"transliteration\" lang=\"ja-Latn\" title=\"Automatic transliteration into Latin letters\">tora</span></td>", html);
        Assert.Contains("<td lang=\"de\">Tiger</td>", html);
    }
}
