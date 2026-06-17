using System;
using System.Collections.Generic;
using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// Pins the shared common-name display normalizer: caps.txt-driven casing (single-word and
// multi-word phrase rules), the proper-noun safety net, and typography cleanup. No DB needed —
// the caps rules are passed in as a plain dictionary (keys lowercased, as the store stores them).
public class CommonNameNormalizerTests {
    private static readonly Dictionary<string, string> NoRules = new(StringComparer.OrdinalIgnoreCase);

    [Theory]
    // IUCN house-style mid-name capitals get lowercased (the high-volume defect).
    [InlineData("Taiwanese Humpback dolphin", "Taiwanese humpback dolphin")]
    [InlineData("African banded Barb", "African banded barb")]
    // Possessive proper nouns keep their capitalization even without an explicit rule.
    [InlineData("De Winton's golden Mole", "De Winton's golden mole")]
    // Internal capitals are preserved.
    [InlineData("Forest McGregor frog", "Forest McGregor frog")]
    // Curly apostrophe is straightened; trailing common noun lowercased.
    [InlineData("Nordmann’s birch Mouse", "Nordmann's birch mouse")]
    // Stray double-space collapses.
    [InlineData("Manipur giant zebra  fish", "Manipur giant zebra fish")]
    public void ApplyCapitalization_NoRules(string input, string expected) {
        Assert.Equal(expected, CommonNameNormalizer.ApplyCapitalization(input, NoRules));
    }

    [Fact]
    public void SingleWordRule_CapitalizesGeographicAdjective() {
        var rules = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["african"] = "African" };
        Assert.Equal("West African hawkfish", CommonNameNormalizer.ApplyCapitalization("west african Hawkfish", rules));
    }

    [Fact]
    public void PhraseRule_OverridesConstituentSingleWordRule() {
        var rules = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
            ["guinea"] = "Guinea",
            ["guinea pig"] = "guinea pig",
        };
        // "guinea pig" phrase wins over the single-word "Guinea" rule.
        Assert.Equal("Santa Catarina's guinea pig",
            CommonNameNormalizer.ApplyCapitalization("Santa Catarina's Guinea pig", rules));
    }

    [Fact]
    public void PhraseRule_DoesNotFireWhenNotMatched() {
        var rules = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
            ["guinea"] = "Guinea",
            ["guinea pig"] = "guinea pig",
        };
        // "Guinea" not followed by "pig" still uses the single-word rule.
        Assert.Equal("New Guinea giant rat",
            CommonNameNormalizer.ApplyCapitalization("new guinea giant rat", rules));
    }

    [Fact]
    public void NormalizeDisplayTypography_StraightensQuotesAndCollapsesSpaces() {
        Assert.Equal("Abbott's \"booby\" form", CommonNameNormalizer.NormalizeDisplayTypography("Abbott’s  “booby” form"));
    }
}
