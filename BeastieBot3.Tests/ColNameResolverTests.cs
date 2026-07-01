using System;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Col;

namespace BeastieBot3.Tests;

// Pins ColNameResolver: the per-taxon CoL resolution shared by the Wikipedia list pipeline. It must
// resolve an IUCN name that CoL treats as a synonym to the accepted name (via parentID), offer a
// formatting-equivalent near match as a corrected spelling, leave genuine spelling variants alone,
// and never cross kingdoms.
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
    public void CrossKingdomHomonym_IsNotMatched() {
        using var col = BuildCol();
        var resolver = new ColNameResolver(new ColTaxonRepository(col));
        // CoL has "Homonymus testus" only in Plantae; an Animalia taxon of the same name must not
        // be corrected to it.
        var r = resolver.Resolve("Homonymus", "testus", null, "Homonymus testus", "Animalia", CancellationToken.None);
        Assert.False(r.HasAcceptedName);
        Assert.False(r.HasCorrectedSpelling);
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
