using System;
using System.IO;
using System.Linq;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

namespace BeastieBot3.Tests;

// Pins the "Members of the [[X]] family are called Y." line under a heading: it takes the plural from
// rules-list.txt ("X = y ! ys" or "X plural ys"), and is left out when the rule's name is a scientific
// name. Also checks the shipped rules-list.txt, so a new higher-taxon line with only a singular
// ("Hyperoliidae = African reed frog") fails here instead of reading "are called African reed frog".
public sealed class HeadingCommonNameSentenceTests : IDisposable {
    private readonly string _rulesPath = Path.GetTempFileName();

    public void Dispose() => File.Delete(_rulesPath);

    private HeadingInfo Heading(string raw, string rank, params string[] rulesLines) {
        File.WriteAllLines(_rulesPath, rulesLines);
        var headings = new HeadingFormatter(new LegacyTaxaRuleList(_rulesPath), taxonRules: null, storeBackedProvider: null);
        return headings.FormatHeading(raw, rank);
    }

    [Fact]
    public void UsesThePluralAfterTheExclamationMark() {
        var heading = Heading("HYPEROLIIDAE", "family", "Hyperoliidae = African reed frog ! African reed frogs // comment");

        Assert.Equal("Members of the [[Hyperoliidae]] family are called African reed frogs.", heading.CommonNameSentence);
    }

    [Fact]
    public void UsesAPluralLine() {
        var heading = Heading("HIPPOSIDERIDAE", "family",
            "Hipposideridae = Old World leaf-nosed bat",
            "Hipposideridae plural Old World leaf-nosed bats");

        Assert.Equal("Members of the [[Hipposideridae]] family are called Old World leaf-nosed bats.", heading.CommonNameSentence);
    }

    [Fact]
    public void AFamilyNameIsSaidToBeAnotherNameOfTheFamily() {
        var heading = Heading("FABACEAE", "family", "Fabaceae = legume family");

        Assert.Equal("[[Fabaceae]] is also known as the legume family.", heading.CommonNameSentence);
    }

    [Fact]
    public void KeepsALowercaseNameThatMatchesTheTaxon() {
        var heading = Heading("PRIMATES", "order", "Primates = primate ! primates");

        Assert.Equal("Members of the [[Primates]] order are called primates.", heading.CommonNameSentence);
    }

    [Theory]
    [InlineData("DAUBENTONIIDAE", "family", "Daubentoniidae = Daubentoniidae")]
    [InlineData("ANTHOCEROTALES", "order", "Anthocerotales = Dendrocerotales")]
    public void NoSentenceWhenTheNameIsAScientificName(string raw, string rank, string rule) {
        var heading = Heading(raw, rank, rule);

        Assert.Null(heading.CommonNameSentence);
    }

    [Theory]
    [InlineData("Daubentoniidae", "Daubentoniidae", true)]
    [InlineData("DAUBENTONIIDAE", "Daubentoniidae", true)]
    [InlineData("Anthocerotaceae", "Dendrocerotaceae", true)]
    [InlineData("Cyprinodontiformes", "Atheriniformes", true)]
    [InlineData("Primates", "primates", false)]
    [InlineData("Cheirogaleidae", "cheirogaleids", false)]
    [InlineData("Hominidae", "great apes", false)]
    [InlineData("Equidae", "Equids of the Old World", false)]
    public void IsScientificName(string raw, string name, bool expected) {
        Assert.Equal(expected, HeadingFormatter.IsScientificName(raw, name));
    }

    // A one-word key is a genus or a higher taxon, which can be a heading. Species lines ("Genus
    // species = ...") are names for list lines and need no plural.
    [Fact]
    public void ShippedRulesGiveAPluralForEveryHigherTaxonName() {
        var path = Path.Combine(AppContext.BaseDirectory, "rules", "rules-list.txt");
        var rules = new LegacyTaxaRuleList(path);
        var missing = File.ReadLines(path)
            .Select(line => line.Split("//", 2)[0].Trim())
            .Where(line => line.Contains(" = ", StringComparison.Ordinal))
            .Select(line => line.Split(" = ", 2)[0].Trim())
            .Where(taxon => !taxon.Contains(' '))
            .Distinct()
            .Where(taxon => rules.Get(taxon) is { } rule
                && rule.CommonPlural is null
                && !HeadingFormatter.IsScientificName(taxon, rule.CommonName)
                && !HeadingFormatter.IsFamilyName(rule.CommonName!))
            .Select(taxon => $"{taxon} = {rules.Get(taxon)!.CommonName}")
            .ToList();

        Assert.Empty(missing);
    }
}
