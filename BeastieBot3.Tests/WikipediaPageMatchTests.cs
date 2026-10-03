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

    [Fact]
    public void SubjectName_WithoutTheHybridSign() {
        // The page "Yucca × schottii".
        Assert.Equal("Yucca schottii", WikipediaPageMatch.SubjectName(new Dictionary<string, string> {
            ["name"] = "Schott's yucca", ["genus"] = "Yucca", ["species"] = "× schottii",
        }));
    }

    // A page's title or taxobox name that is the scientific name its own taxobox gives. The store
    // knew neither the genus nor the epithet of the first two, so ScientificNameCheck took them for
    // common names and the lists showed them in place of the IUCN name ("Yucatán rustrump").

    [Theory]
    [InlineData("Tliltocatl epicureanus", "taxon", "Tliltocatl epicureanus")]
    [InlineData("Philhelius citrofasciatus", "taxon", "Philhelius citrofasciatus")]
    [InlineData("Panthera leo", "binomial", "''Panthera leo''")]
    public void ATitleThatIsTheTaxoboxsOwnTaxon_IsTheSubject(string title, string parameter, string value) {
        var keys = WikipediaPageMatch.SubjectKeys(new Dictionary<string, string> { [parameter] = value });
        Assert.True(WikipediaPageMatch.IsTaxoboxSubject(title, keys));
    }

    [Theory]
    [InlineData("Ilybius discicolle", "Ilybius", "discicolle")]
    [InlineData("Yucca × schottii", "Yucca", "× schottii")]
    [InlineData("Malus × zumi", "Malus", "× zumi")]
    [InlineData("Cyanea st.-johnii", "Cyanea", "st.-johnii")]
    public void ATitleThatIsTheTaxoboxsGenusAndSpecies_IsTheSubject(string title, string genus, string species) {
        var keys = WikipediaPageMatch.SubjectKeys(new Dictionary<string, string> { ["genus"] = genus, ["species"] = species });
        Assert.True(WikipediaPageMatch.IsTaxoboxSubject(title, keys));
    }

    [Theory]
    // Common names that ScientificNameCheck cannot tell from scientific names by their words.
    [InlineData("Leka keppe", "taxon", "Sarotherodon lohbergeri")]
    [InlineData("Shuttles hoppfish", "taxon", "Periophthalmus modestus")]
    [InlineData("Plainchin dreamarm", "taxon", "Leptacanthichthys gracilispinis")]
    [InlineData("Lion", "binomial", "Panthera leo")]
    public void ATitleThatIsNotTheTaxoboxsTaxon_IsNotTheSubject(string title, string parameter, string value) {
        var keys = WikipediaPageMatch.SubjectKeys(new Dictionary<string, string> { [parameter] = value });
        Assert.False(WikipediaPageMatch.IsTaxoboxSubject(title, keys));
    }

    [Fact]
    public void TheTaxoboxNameField_IsNotASubject() {
        var keys = WikipediaPageMatch.SubjectKeys(new Dictionary<string, string> {
            ["name"] = "Primordial tapecua<ref name=msw3>{{MSW3 Muroidea| id = 13000930| page = 1178}}</ref>",
            ["genus"] = "Tapecomys", ["species"] = "primus",
        });
        Assert.False(WikipediaPageMatch.IsTaxoboxSubject("Primordial tapecua", keys));
        Assert.True(WikipediaPageMatch.IsTaxoboxSubject("Tapecomys primus", keys));
    }

    // Pages about a genus or a higher taxon, matched to one IUCN species (taxobox parameters as the
    // Wikipedia cache has them, October 2026).

    private static readonly Dictionary<string, string> Trachycephalus = new() {
        ["name"] = "Casque-headed tree frogs", ["taxon"] = "Trachycephalus",
    };

    private static readonly Dictionary<string, string> Maple = new() {
        ["taxon"] = "Acer", ["subdivision_ranks"] = "Species",
    };

    private static readonly Dictionary<string, string> Capraiuscola = new() { ["taxon"] = "Capraiuscola" };

    [Fact]
    public void PageGenus_IsTheOneWordTaxonOfAGenusPage() {
        Assert.Equal("Trachycephalus", WikipediaPageMatch.PageGenus(Trachycephalus));
        Assert.Equal("Acer", WikipediaPageMatch.PageGenus(Maple));
        Assert.Equal("Capraiuscola", WikipediaPageMatch.PageGenus(Capraiuscola));
        // An old-style Taxobox for a genus has a genus and no species.
        Assert.Equal("Leucoraja", WikipediaPageMatch.PageGenus(new Dictionary<string, string> {
            ["genus"] = "''[[Leucoraja]]''", ["type_species"] = "'''''Raja fullonica'''''",
        }));
    }

    [Fact]
    public void PageGenus_IsNullForASpeciesPage() {
        // The Raiatea starling is a species whose genus is uncertain; the cache stores its rank as genus.
        Assert.Null(WikipediaPageMatch.PageGenus(new Dictionary<string, string> {
            ["name"] = "Raiatea starling", ["taxon"] = "Aplonis/?/?",
            ["species_text"] = "{{extinct}}'''''A.''? ''ulietensis'''''",
            ["binomial_text"] = "{{extinct}}''Aplonis''? ''ulietensis''",
        }));
        Assert.Null(WikipediaPageMatch.PageGenus(new Dictionary<string, string> { ["taxon"] = "Tliltocatl epicureanus" }));
        Assert.Null(WikipediaPageMatch.PageGenus(new Dictionary<string, string> { ["genus"] = "Ilybius", ["species"] = "discicolle" }));
        Assert.Null(WikipediaPageMatch.PageGenus(new Dictionary<string, string> { ["name"] = "Hector's dolphin" }));
    }

    [Fact]
    public void AGenusPage_GivesNoNamesToASpeciesMatchedToIt() {
        // Store counts: Trachycephalus 18 species, Acer 159, Capraiuscola none (the store has
        // Miramella ebneri, which CoL places in Capraiuscola).
        var speciesInGenus = new Dictionary<string, int> { ["Trachycephalus"] = 18, ["Acer"] = 159, ["Capraiuscola"] = 0 };
        int Count(string genus) => speciesInGenus[genus];

        Assert.True(WikipediaPageMatch.IsGenusPageOfSpecies(Trachycephalus, "trachycephalus vermiculatus", false, Count));
        Assert.True(WikipediaPageMatch.IsGenusPageOfSpecies(Maple, "acer kwangnanense", false, Count));
        Assert.True(WikipediaPageMatch.IsGenusPageOfSpecies(Capraiuscola, "miramella ebneri", false, Count));
        Assert.True(WikipediaPageMatch.IsGenusPageOfSpecies(Trachycephalus, "trachycephalus vermiculatus ssp. x", false, Count));
    }

    [Fact]
    public void CountSpeciesInGenus_CountsBinomials_NotSubspeciesOrOtherGenera() {
        var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = CommonNameStore.OpenFromConnection(connection);
        var id = 0;
        void Add(string canonical, string validity = "valid") =>
            store.InsertOrUpdateTaxon(canonical, canonical, "species", "ANIMALIA", false, false, validity, "iucn", (++id).ToString());
        Add("allophryne ruthveni");
        Add("allophryne ruthveni ssp. x");
        Add("allophryne relicta");
        Add("allophryne resplendens", validity: "synonym");
        Add("allophrynella aus");
        Add("pristoceuthophilus sp. nov.");

        Assert.Equal(2, store.CountSpeciesInGenus("Allophryne"));
        Assert.Equal(1, store.CountSpeciesInGenus("Pristoceuthophilus"));
        Assert.Equal(0, store.CountSpeciesInGenus("Capraiuscola"));
    }

    [Fact]
    public void AGenusPage_GivesNamesToItsOnlySpecies_AndToTheGenus() {
        int One(string genus) => 1;
        int Many(string genus) => 18;

        Assert.False(WikipediaPageMatch.IsGenusPageOfSpecies(Trachycephalus, "trachycephalus vermiculatus", false, One));
        Assert.False(WikipediaPageMatch.IsGenusPageOfSpecies(Trachycephalus, "trachycephalus vermiculatus", true, Many));
        Assert.False(WikipediaPageMatch.IsGenusPageOfSpecies(Trachycephalus, "trachycephalus", false, Many));
        Assert.False(WikipediaPageMatch.IsGenusPageOfSpecies(
            new Dictionary<string, string> { ["taxon"] = "Tliltocatl epicureanus" }, "brachypelma epicureanum", false, Many));
    }
}
