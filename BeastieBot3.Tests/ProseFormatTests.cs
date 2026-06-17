using System.Globalization;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins the small number/text prose helpers extracted in the R2 carve-up. Number/percentage
// formatting is culture-sensitive, so these tests fix the culture to en-US for determinism.
public class ProseFormatTests {
    private static void WithEnUs(System.Action body) {
        var prior = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
        try { body(); } finally { CultureInfo.CurrentCulture = prior; }
    }

    [Theory]
    [InlineData(0, "zero")]
    [InlineData(1, "one")]
    [InlineData(10, "ten")]
    [InlineData(11, "11")]
    [InlineData(999, "999")]
    [InlineData(6445, "6,445")]
    [InlineData(9999, "9,999")]
    public void NewspaperNumber_SmallSpelledLargeGrouped(int n, string expected) {
        WithEnUs(() => Assert.Equal(expected, ProseFormat.NewspaperNumber(n)));
    }

    [Fact]
    public void NewspaperNumber_TenThousandPlus_HasThousandsSeparator() {
        WithEnUs(() => Assert.Equal("12,345", ProseFormat.NewspaperNumber(12345)));
    }

    [Fact]
    public void FormatPercentage_AdaptivePrecision() {
        WithEnUs(() => {
            Assert.Equal("50.0%", ProseFormat.FormatPercentage(1, 2));     // 1-100% -> P1
            Assert.Equal("10.0%", ProseFormat.FormatPercentage(10, 100));  // boundary stays one decimal
            Assert.Equal("5.0%", ProseFormat.FormatPercentage(5, 100));    // 1-10% -> P1
            Assert.Equal("0.03%", ProseFormat.FormatPercentage(3, 10000)); // < 1% -> P2
        });
    }

    [Theory]
    [InlineData("ARTIODACTYLA", "Artiodactyla")]
    [InlineData("felidae", "Felidae")]
    public void ToTitleCase_FirstUpperRestLower(string input, string expected) {
        Assert.Equal(expected, ProseFormat.ToTitleCase(input));
    }

    [Theory]
    [InlineData("bald eagle", "Bald eagle")]
    [InlineData("a", "A")]
    public void Uppercase_OnlyFirstChar(string input, string expected) {
        Assert.Equal(expected, ProseFormat.Uppercase(input));
    }
}
