using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

public sealed class SiteTaxoboxSynonymsTests {
    private static SiteTaxon Taxon(long id, string name) => new() {
        TaxonId = id, ScientificName = name, Kind = SiteTaxonKind.Species, Kingdom = "ANIMALIA", InRelease = true,
    };

    private const string Serotine = """
        {"genus":"Nycticeinops","species":"crassulus","authority":"([[Oldfield Thomas|Thomas]], 1904)",
         "synonyms":"''Pipistrellus crassulus'' <small>Thomas, 1904</small><br> ''Hypsugo crassulus''"}
        """;

    [Fact]
    public void ATaxonTakesTheTaxoboxNameAndItsSynonyms() {
        var taxon = Taxon(44853, "Pipistrellus crassulus");
        var stats = new SiteBuildStats();
        SiteLinkReaders.AddTaxoboxSynonyms([taxon], Serotine, stats);
        Assert.Equal(new[] {
            new SiteSynonym("Nycticeinops crassulus", "(Thomas, 1904)"),
            new SiteSynonym("Pipistrellus crassulus", "Thomas, 1904"),
            new SiteSynonym("Hypsugo crassulus"),
        }, taxon.WikipediaSynonyms);
        Assert.Equal(3, stats.WikipediaTaxoboxSynonyms);
    }

    [Fact]
    public void APageOfSeveralTaxaGivesItsNamesOnlyToTheTaxonItNames() {
        var named = Taxon(1, "Nycticeinops crassulus");
        var other = Taxon(2, "Pipistrellus bellieri");
        SiteLinkReaders.AddTaxoboxSynonyms([named, other], Serotine, new SiteBuildStats());
        Assert.NotEmpty(named.WikipediaSynonyms);
        Assert.Empty(other.WikipediaSynonyms);

        var a = Taxon(3, "Pipistrellus crassulus");
        var b = Taxon(4, "Pipistrellus bellieri");
        var stats = new SiteBuildStats();
        SiteLinkReaders.AddTaxoboxSynonyms([a, b], Serotine, stats);
        Assert.Empty(a.WikipediaSynonyms);
        Assert.Equal(1, stats.WikipediaTaxoboxPagesShared);
    }

    [Fact]
    public void AGenusPageGivesNothing() {
        var taxon = Taxon(5, "Acer kwangnanense");
        SiteLinkReaders.AddTaxoboxSynonyms([taxon], """{"taxon":"Acer","synonyms":"''Negundo'' Boehm."}""", new SiteBuildStats());
        Assert.Empty(taxon.WikipediaSynonyms);
    }
}
