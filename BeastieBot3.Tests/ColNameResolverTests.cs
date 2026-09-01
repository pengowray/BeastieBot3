using System;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Col;

namespace BeastieBot3.Tests;

// Pins ColNameResolver: the per-taxon CoL resolution shared by the Wikipedia list pipeline. It must
// resolve an IUCN name that CoL treats as a synonym to the accepted name (via parentID), offer a
// formatting-equivalent near match as a corrected spelling, offer the way CoL writes the same name
// when only Latin gender agreement separates them, leave genuine spelling variants alone, and never
// cross kingdoms.
public class ColNameResolverTests {
    [Fact]
    public void Synonym_ResolvesAcceptedName() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        var r = resolver.Resolve("Felis", "leo", null, "Felis leo", "Animalia", CancellationToken.None);
        Assert.Equal("Panthera leo", r.AcceptedName);
        Assert.Null(r.CorrectedSpelling);
    }

    [Fact]
    public void AcceptedExactMatch_OffersNoCorrection() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        var r = resolver.Resolve("Panthera", "leo", null, "Panthera leo", "Animalia", CancellationToken.None);
        Assert.False(r.HasAcceptedName);
        Assert.False(r.HasCorrectedSpelling);
        // CoL knows the name; callers must not mistake "nothing to offer" for "never heard of it"
        // and go looking through the taxon's other names.
        Assert.False(r.NameIsUnknownToCol);
    }

    [Theory]
    [InlineData("Panthera", "leo", false)]        // accepted in CoL
    [InlineData("Felis", "leo", false)]           // a CoL synonym
    [InlineData("Schistura", "striatus", false)]  // a Latin variant is in CoL
    [InlineData("Naja", "hajé", false)]           // a formatting-equivalent spelling is in CoL
    [InlineData("Idiopoma", "javanica", true)]    // CoL has no record of this name at all
    [InlineData("Panthera", "leoo", true)]        // one edit from an accepted name, but not that name
    public void NameIsUnknownToCol_SeparatesAgreementFromAbsence(string genus, string species, bool unknown) {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        var r = resolver.Resolve(genus, species, null, $"{genus} {species}", "Animalia", CancellationToken.None);
        Assert.Equal(unknown, r.NameIsUnknownToCol);
    }

    [Fact]
    public void FormattingEquivalentSlip_OffersCorrectedSpelling() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // Diacritic slip in the epithet: no exact match, but the same name up to a diacritic.
        var r = resolver.Resolve("Naja", "hajé", null, "Naja hajé", "Animalia", CancellationToken.None);
        Assert.Equal("Naja haje", r.CorrectedSpelling);
        Assert.Null(r.AcceptedName);
    }

    [Fact]
    public void GenuineSpellingVariant_IsNotOffered() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // A real one-letter difference could be a different taxon, so it is left to IUCN.
        var r = resolver.Resolve("Panthera", "leoo", null, "Panthera leoo", "Animalia", CancellationToken.None);
        Assert.False(r.HasCorrectedSpelling);
        Assert.False(r.HasAcceptedName);
    }

    [Fact]
    public void SynonymPointingToOtherKingdom_IsNotResolved() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // CoL has "Crosskingdom testus" only as a (blank-kingdom) synonym whose accepted taxon is a
        // Plantae name; an Animalia taxon of that name must not be resolved to the plant.
        var r = resolver.Resolve("Crosskingdom", "testus", null, "Crosskingdom testus", "Animalia", CancellationToken.None);
        Assert.False(r.HasAcceptedName);
        Assert.False(r.HasCorrectedSpelling);
    }

    [Fact]
    public void CrossKingdomHomonym_IsNotMatched() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // CoL has "Homonymus testus" only in Plantae; an Animalia taxon of the same name must not
        // be corrected to it.
        var r = resolver.Resolve("Homonymus", "testus", null, "Homonymus testus", "Animalia", CancellationToken.None);
        Assert.False(r.HasAcceptedName);
        Assert.False(r.HasCorrectedSpelling);
    }

    [Fact]
    public void LatinVariant_OffersTheWayColWritesIt() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // CoL made the epithet agree with the genus; IUCN kept the masculine form of the old genus.
        var r = resolver.Resolve("Schistura", "striatus", null, "Schistura striatus", "Animalia", CancellationToken.None);
        Assert.Equal("Schistura striata", r.VariantName);
        Assert.False(r.HasCorrectedSpelling);
    }

    [Fact]
    public void LatinVariantThatIsAColSynonym_AlsoOffersTheAcceptedName() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // "Blechnum socialis" is not in CoL; "Blechnum sociale" is, as a synonym of another name.
        // Both are worth offering: the article may be filed under either.
        var r = resolver.Resolve("Blechnum", "socialis", null, "Blechnum socialis", "Plantae", CancellationToken.None);
        Assert.Equal("Blechnum sociale", r.VariantName);
        Assert.Equal("Blechnum occidentale", r.AcceptedName);
    }

    [Fact]
    public void DifferentSpeciesInTheSameGenus_IsNotOfferedAsAVariant() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // One edit apart and in the same genus, but two different beetles. Offering this would put
        // a wrong link on a published list.
        var r = resolver.Resolve("Elater", "turcicus", null, "Elater turcicus", "Animalia", CancellationToken.None);
        Assert.False(r.HasVariantName);
        Assert.False(r.HasCorrectedSpelling);
        Assert.False(r.HasAcceptedName);
    }

    [Fact]
    public void SecondHop_ResolvesAnotherIucnNameToTheColAcceptedName() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // The IUCN name itself is nowhere in CoL, not even as a near spelling. Starting from one of
        // the taxon's other IUCN names is what reaches CoL when the two disagree about the genus.
        var direct = resolver.Resolve("Idiopoma", "javanica", null, "Idiopoma javanica", "Animalia", CancellationToken.None);
        Assert.False(direct.HasAcceptedName);
        Assert.False(direct.HasVariantName);

        var viaSynonym = resolver.Resolve(null, null, null, "Vivipara javanica", "Animalia", CancellationToken.None);
        Assert.Equal("Filopaludina javanica", viaSynonym.AcceptedName);
    }

    private static SqliteConnection BuildCol() {
        var conn = new SqliteConnection("Data Source=:memory:;Pooling=False");
        conn.Open();
        using (var create = conn.CreateCommand()) {
            create.CommandText = @"CREATE TABLE nameusage (
                ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, authorship TEXT, rank TEXT,
                kingdom TEXT, genericName TEXT, specificEpithet TEXT);";
            create.ExecuteNonQuery();
        }
        Add(conn, "P_LEO", "species", "accepted", "Panthera leo", "Animalia", "Panthera", "leo");
        Add(conn, "FELIS_LEO", "species", "synonym", "Felis leo", "Animalia", "Felis", "leo", parentId: "P_LEO");
        Add(conn, "NAJA", "species", "accepted", "Naja haje", "Animalia", "Naja", "haje");
        Add(conn, "HOMO_PLANT", "species", "accepted", "Homonymus testus", "Plantae", "Homonymus", "testus");
        // A synonym with a blank kingdom (as real CoL synonyms have) whose accepted taxon is a plant.
        Add(conn, "XK_ACC", "species", "accepted", "Crosskingdom acceptus", "Plantae", "Crosskingdom", "acceptus");
        Add(conn, "XK_SYN", "species", "synonym", "Crosskingdom testus", "", "Crosskingdom", "testus", parentId: "XK_ACC");
        // Gender agreement: CoL has the feminine form, IUCN the masculine one.
        Add(conn, "SCHIST", "species", "accepted", "Schistura striata", "Animalia", "Schistura", "striata");
        // A Latin variant that is itself a CoL synonym, so both names are worth offering.
        Add(conn, "BLECH_ACC", "species", "accepted", "Blechnum occidentale", "Plantae", "Blechnum", "occidentale");
        Add(conn, "BLECH_SYN", "species", "synonym", "Blechnum sociale", "", "Blechnum", "sociale", parentId: "BLECH_ACC");
        // Same genus, one edit apart, different species. Must never be offered.
        Add(conn, "ELATER", "species", "accepted", "Elater suecicus", "Animalia", "Elater", "suecicus");
        // The second hop: IUCN also calls this taxon "Vivipara javanica", which CoL holds as a
        // synonym of the accepted name. Nothing under the genus IUCN publishes is in CoL at all.
        Add(conn, "FILO_ACC", "species", "accepted", "Filopaludina javanica", "Animalia", "Filopaludina", "javanica");
        Add(conn, "VIVI_SYN", "species", "synonym", "Vivipara javanica", "", "Vivipara", "javanica", parentId: "FILO_ACC");
        return conn;
    }

    private static void Add(SqliteConnection conn, string id, string rank, string status, string scientificName,
        string kingdom, string genericName, string specificEpithet, string? parentId = null) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO nameusage (ID, parentID, status, scientificName, rank, kingdom, genericName, specificEpithet)
            VALUES (@id, @parent, @status, @sci, @rank, @kingdom, @genus, @sp);";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@parent", (object?)parentId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@status", status);
        cmd.Parameters.AddWithValue("@sci", scientificName);
        cmd.Parameters.AddWithValue("@rank", rank);
        cmd.Parameters.AddWithValue("@kingdom", kingdom);
        cmd.Parameters.AddWithValue("@genus", genericName);
        cmd.Parameters.AddWithValue("@sp", specificEpithet);
        cmd.ExecuteNonQuery();
    }
}
