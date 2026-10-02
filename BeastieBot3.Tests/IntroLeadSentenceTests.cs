using System.Collections.Generic;
using System.Linq;
using BeastieBot3.WikipediaLists;
using Xunit;

namespace BeastieBot3.Tests;

public sealed class IntroLeadSentenceTests {
    private static WikipediaSectionDefinition Section(params string[] codes) => new() {
        Statuses = codes.Select(c => new SectionStatusDefinition { Code = c }).ToList(),
    };

    [Fact]
    public void ExtinctInTheWild_WithPossiblyExtinctInTheWild_NamesBothCategories() =>
        Assert.Equal(
            "[[Extinct in the wild|extinct in the wild]], or as [[Critically endangered species|critically endangered]] and possibly extinct in the wild",
            IntroProseBuilder.StatusPhrase(new[] { Section("EW", "CR(PEW)") }));

    [Fact]
    public void TaggedCodesUnderAListedCategory_AddNothing() =>
        Assert.Equal(
            "[[Critically endangered species|critically endangered]]",
            IntroProseBuilder.StatusPhrase(new[] { Section("CR", "CR(PE)", "CR(PEW)") }));

    [Fact]
    public void SeveralCategories_AreJoinedWithOr() =>
        Assert.Equal(
            "[[Critically endangered species|critically endangered]], [[Endangered species|endangered]] or [[Vulnerable species|vulnerable]]",
            IntroProseBuilder.StatusPhrase(new[] { Section("CR"), Section("EN"), Section("VU") }));

    [Theory]
    [InlineData(850, "conifer", "850 conifer [[taxon|taxa]]")]
    [InlineData(1357, "fungus", "1,357 fungus [[taxon|taxa]]")]
    [InlineData(1, "cycad", "one cycad [[taxon]]")]
    [InlineData(53, null, "53 [[taxon|taxa]]")]
    public void CountTaxaPhrase_FormatsNumberAdjectiveAndNoun(int count, string? adjective, string expected) =>
        Assert.Equal(expected, IntroProseBuilder.CountTaxaPhrase(count, adjective));

    [Fact]
    public void KingdomAdjective_OnlyForOneIncludedKingdom() {
        Assert.Equal("animal", IntroProseBuilder.KingdomAdjective(new List<TaxonFilterDefinition> {
            new() { Rank = "kingdom", Value = "Animalia" },
        }));
        Assert.Null(IntroProseBuilder.KingdomAdjective(new List<TaxonFilterDefinition> {
            new() { Rank = "kingdom", Values = new List<string> { "Animalia", "Plantae" } },
        }));
        Assert.Null(IntroProseBuilder.KingdomAdjective(new List<TaxonFilterDefinition>()));
    }
}
