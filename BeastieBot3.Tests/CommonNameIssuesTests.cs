using BeastieBot3.Audit.Producers;
using Xunit;

namespace BeastieBot3.Tests;

// Pins the common-name issue classifier and its tidied suggestion, focusing on the Hawaiian ʻokina
// rule: a backtick before a vowel is the ʻokina (U+02BB), a backtick before a consonant is a
// possessive apostrophe. Cases are taken from the real IUCN English common names.
public class CommonNameIssuesTests {
    private static string? Suggest(string name) =>
        CommonNameIssuesProducer.CleanedSuggestion(name, CommonNameIssuesProducer.Classify(name));

    [Theory]
    [InlineData("Hawai`i `Elepaio", "Hawaiʻi ʻElepaio")]
    [InlineData("Kaua`i `Elepaio", "Kauaʻi ʻElepaio")]
    [InlineData("Ko`oko`olau", "Koʻokoʻolau")]
    [InlineData("Koko`olau", "Kokoʻolau")]
    [InlineData("Molokai Koki`o", "Molokai Kokiʻo")]
    [InlineData("Ko`olau Spurwing Long-legged Fly", "Koʻolau Spurwing Long-legged Fly")]
    public void HawaiianBacktickBecomesOkina(string name, string expected) {
        Assert.Contains(CommonNameIssue.HawaiianOkina, CommonNameIssuesProducer.Classify(name));
        Assert.Equal(expected, Suggest(name));
    }

    [Theory]
    [InlineData("Law`s Persian Violet", "Law's Persian Violet")]
    [InlineData("Olrog`s Four-eyed Opossum", "Olrog's Four-eyed Opossum")]
    [InlineData("Pontoh`s Pygmy Seahorse", "Pontoh's Pygmy Seahorse")]
    [InlineData("Sickle-leaved Hare`s-ear", "Sickle-leaved Hare's-ear")]
    public void PossessiveBacktickBecomesApostrophe(string name, string expected) {
        var issues = CommonNameIssuesProducer.Classify(name);
        Assert.Contains(CommonNameIssue.AcuteApostrophe, issues);
        Assert.DoesNotContain(CommonNameIssue.HawaiianOkina, issues);
        Assert.Equal(expected, Suggest(name));
    }

    [Theory]
    [InlineData("Castelnau´s piranha", "Castelnau's piranha")]
    [InlineData("Graham´s Nipple Cactus", "Graham's Nipple Cactus")]
    public void AcuteAccentBecomesApostrophe(string name, string expected) {
        var issues = CommonNameIssuesProducer.Classify(name);
        Assert.Contains(CommonNameIssue.AcuteApostrophe, issues);
        Assert.DoesNotContain(CommonNameIssue.HawaiianOkina, issues);
        Assert.Equal(expected, Suggest(name));
    }

    [Fact]
    public void CorrectOkinaIsNotFlagged() {
        // Already uses the proper ʻokina (U+02BB); nothing to fix.
        Assert.Empty(CommonNameIssuesProducer.Classify("Koʻolau Mountain Melicope"));
        Assert.Null(Suggest("Koʻolau Mountain Melicope"));
    }

    [Fact]
    public void OrdinaryNamesAreNotFlagged() {
        Assert.Empty(CommonNameIssuesProducer.Classify("Asian Elephant"));
    }
}
