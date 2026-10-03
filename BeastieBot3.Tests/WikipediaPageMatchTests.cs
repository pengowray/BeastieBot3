using System.Collections.Generic;
using BeastieBot3.CommonNames;

namespace BeastieBot3.Tests;

// `common-names aggregate --source wikipedia`: a page matched to several taxa gives its title and
// taxobox name to its own taxon only. The pages and matches are from the Wikipedia cache
// (September 2026).
public class WikipediaPageMatchTests {
    private static MatchedTaxon Taxon(string id, string? method, string canonical, params string[] synonyms) =>
        new(id, method, new TaxonScientificNames(canonical, synonyms));

    [Theory]
    [InlineData("iucn-synonym", true)]
    [InlineData("col-accepted", true)]
    [InlineData("col-synonym", true)]
    [InlineData("col-accepted-via-synonym", true)]
    [InlineData("iucn-taxonomy", false)]
    [InlineData("iucn-constructed", false)]
    [InlineData("iucn-infra-rank", false)]
    [InlineData("TaxonName", false)]
    [InlineData("Label", false)]
    [InlineData(null, false)]
    public void IsThroughAnotherName(string? method, bool expected) {
        Assert.Equal(expected, WikipediaPageMatch.IsThroughAnotherName(method));
    }

    [Fact]
    public void SplitSpecies_TheTaxonTheTaxoboxNamesGetsTheNames() {
        // "Channel-billed toucan": every taxon is matched by its own name, through redirects.
        var taxa = new[] {
            Taxon("22726222", "iucn-taxonomy", "ramphastos vitellinus"),
            Taxon("22726239", "iucn-taxonomy", "ramphastos culminatus"),
            Taxon("22726233", "iucn-taxonomy", "ramphastos ariel"),
            Taxon("62220237", "iucn-taxonomy", "ramphastos citreolaemus"),
        };
        Assert.Equal(["22726222"], WikipediaPageMatch.TaxaGivenNames(taxa, "Ramphastos vitellinus"));
    }

    [Fact]
    public void ASubspeciesboxName_MatchesTheStoresNameWithItsRankMarker() {
        // "Grant's gazelle" is Nanger granti's page; on a page for the subspecies the taxobox gives
        // "Nanger granti granti" and the store "nanger granti ssp. granti".
        var taxa = new[] {
            Taxon("8971", "iucn-taxonomy", "nanger granti"),
            Taxon("51187222", "col-accepted", "nanger granti ssp. granti"),
        };
        Assert.Equal(["8971"], WikipediaPageMatch.TaxaGivenNames(taxa, "Nanger granti"));
        Assert.Equal(["51187222"], WikipediaPageMatch.TaxaGivenNames(taxa, "Nanger granti granti"));
    }

    [Fact]
    public void TheTaxoboxNameAsASynonym_GivesTheNamesToThatTaxon() {
        var taxa = new[] {
            Taxon("1", "iucn-taxonomy", "pholidoscelis polops", "ameiva polops"),
            Taxon("2", "iucn-taxonomy", "pholidoscelis exsul"),
        };
        Assert.Equal(["1"], WikipediaPageMatch.TaxaGivenNames(taxa, "Ameiva polops"));
    }

    [Fact]
    public void WithoutATaxoboxName_TaxaMatchedByOwnNameGetTheNames() {
        // "Hector's dolphin": the subspecies is matched through the synonym "Cephalorhynchus hectori".
        var taxa = new[] {
            Taxon("4162", "iucn-taxonomy", "cephalorhynchus hectori"),
            Taxon("39427", "iucn-synonym", "cephalorhynchus hectori maui"),
        };
        Assert.Equal(["4162"], WikipediaPageMatch.TaxaGivenNames(taxa, null));
    }

    [Fact]
    public void WhenNothingDecides_EveryTaxonGetsTheNames() {
        var taxa = new[] {
            Taxon("1", "iucn-synonym", "aus bus"),
            Taxon("2", "col-accepted", "aus cus"),
        };
        Assert.Equal(new HashSet<string> { "1", "2" }, WikipediaPageMatch.TaxaGivenNames(taxa, "Dus eus"));
    }

    [Fact]
    public void OneTaxon_GetsTheNames() {
        Assert.Equal(["1"], WikipediaPageMatch.TaxaGivenNames([Taxon("1", "iucn-synonym", "aus bus")], "Cus dus"));
    }

    [Fact]
    public void SubjectName_FromSpeciesboxGenusAndSpecies() {
        Assert.Equal("Ramphastos vitellinus", WikipediaPageMatch.SubjectName(new Dictionary<string, string> {
            ["genus"] = "Ramphastos", ["species"] = "vitellinus", ["name"] = "Channel-billed toucan",
        }));
    }

    [Fact]
    public void SubjectName_FromTaxon_WithAReference() {
        Assert.Equal("Panthera leo", WikipediaPageMatch.SubjectName(new Dictionary<string, string> {
            ["taxon"] = "Panthera leo<ref name=MSW3>{MSW3 Carnivora |id=14000228 |page=546 |heading=Species ''Panthera leo''}</ref>",
        }));
    }

    [Fact]
    public void SubjectName_FromTaxoboxBinomial_NotFromTheAbbreviatedSpecies() {
        Assert.Equal("Panthera leo", WikipediaPageMatch.SubjectName(new Dictionary<string, string> {
            ["genus"] = "''[[Panthera]]''", ["species"] = "'''''P. leo'''''", ["binomial"] = "''Panthera leo''",
        }));
    }

    [Fact]
    public void SubjectName_FromSubspeciesbox() {
        Assert.Equal("Giraffa camelopardalis antiquorum", WikipediaPageMatch.SubjectName(new Dictionary<string, string> {
            ["genus"] = "Giraffa", ["species"] = "camelopardalis", ["subspecies"] = "antiquorum",
        }));
    }

    [Fact]
    public void SubjectName_NullWhenTheTaxoboxHasNoPlainName() {
        Assert.Null(WikipediaPageMatch.SubjectName(new Dictionary<string, string> { ["name"] = "Hector's dolphin" }));
        Assert.Null(WikipediaPageMatch.SubjectName(new Dictionary<string, string> { ["genus"] = "Ramphastos", ["species"] = "''R. ariel''" }));
    }
}
