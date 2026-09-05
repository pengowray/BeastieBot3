using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Producers.ColCrosscheck;
using BeastieBot3.Col;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// Pins the CoL crosscheck engine: the synonym-via-parentID fix (there is no acceptedNameUsageID
// column in this ColDP schema), homonym suppression for higher-rank synonymy, the not-found vs
// close-match split, and the typo vs genuine-reorganisation split for higher-rank placement.
public class ColCrosscheckTests {
    // ---- ColDifference buckets ----

    [Fact]
    public void ColDifference_TypoKindsBucketAsTypo() {
        foreach (var kind in new[] {
            ScientificNameDifference.Kind.Punctuation, ScientificNameDifference.Kind.Unicode,
            ScientificNameDifference.Kind.Diacritic, ScientificNameDifference.Kind.Fuzzy }) {
            Assert.Equal(ColDifference.Bucket.Typo, ColDifference.Classify(new ScientificNameDifference.Result(kind, 1, "x")));
        }
    }

    [Fact]
    public void ColDifference_UnrelatedIsGenuine_CaseAndWhitespaceDropped() {
        Assert.Equal(ColDifference.Bucket.Genuine, ColDifference.Classify(new ScientificNameDifference.Result(ScientificNameDifference.Kind.Unrelated, 9, "x")));
        Assert.Equal(ColDifference.Bucket.Drop, ColDifference.Classify(new ScientificNameDifference.Result(ScientificNameDifference.Kind.Case, 0, "x")));
        Assert.Equal(ColDifference.Bucket.Drop, ColDifference.Classify(new ScientificNameDifference.Result(ScientificNameDifference.Kind.Whitespace, 0, "x")));
        Assert.Equal(ColDifference.Bucket.Drop, ColDifference.Classify(new ScientificNameDifference.Result(ScientificNameDifference.Kind.Exact, 0, "x")));
    }

    // ---- Engine end-to-end over an in-memory Catalogue of Life ----

    [Fact]
    public void Engine_SortsFindingsIntoTheRightBuckets() {
        using var col = BuildCol();
        var repo = new ColTaxonRepository(col);
        // IUCN records "Panthera leo" as a synonym of taxon 103 (the Felis leo assessment), so the
        // two catalogues are reversed for that pair.
        var iucnSynonyms = IucnSynonymIndex.FromEntries(new[] {
            ("Panthera leo", 103L), ("Yearclash novus", 114L), ("Authordiff novus", 115L),
        });
        var data = new ColCrosscheckEngine(repo, iucnSynonyms).Run(IucnRows(), CancellationToken.None);

        // Synonym at species level, resolved through parentID (this used to return zero). IUCN also
        // records the CoL accepted name among its own synonyms, so the pair is joinable and belongs
        // on the accepted-name-differs report rather than in the synonym list.
        var reversed = Assert.Single(data.AcceptedDiffers, f => f.ScientificName == "Felis leo");
        Assert.Equal("Panthera leo", reversed.SuggestedValue);
        Assert.Equal("(Linnaeus, 1758)", reversed.Get("colAuthority")); // CoL accepted name's authority + year
        Assert.Equal("1998", reversed.YearPublished);                   // IUCN assessment year
        Assert.DoesNotContain(data.Synonym, f => f.ScientificName == "Felis leo");
        Assert.Null(reversed.Get("authorityYears"));                    // no IUCN authority to compare

        // Same author on both sides but a different year: flagged, and lifted above the plain rows.
        var clash = Assert.Single(data.AcceptedDiffers, f => f.ScientificName == "Yearclash oldus");
        Assert.Equal("IUCN 1881 vs CoL 1891", clash.Get("authorityYears"));
        Assert.Equal("Herzenstein, 1881", clash.Get("iucnAuthority"));
        Assert.Contains("Both credit Herzenstein", clash.Detail);
        Assert.True(clash.SeverityTier > reversed.SeverityTier);

        // Different author and a different year is a different name, not a date to check: not flagged.
        var authordiff = Assert.Single(data.AcceptedDiffers, f => f.ScientificName == "Authordiff oldus");
        Assert.Null(authordiff.Get("authorityYears"));

        // Higher-rank synonym: a genus CoL records only as a synonym, with the accepted spelling and its authority.
        var higherSyn = Assert.Single(data.SynonymHigher, f => f.ScientificName == "Bofonaria");
        Assert.Equal("genus", higherSyn.Rank);
        Assert.Equal("Bufonaria", higherSyn.SuggestedValue);
        Assert.Equal("(Schumacher, 1817)", higherSyn.Get("colAuthority"));

        // Homonym suppression: Anemone is a synonym AND an accepted genus, so it is not reported.
        Assert.DoesNotContain(data.SynonymHigher, f => f.ScientificName == "Anemone");

        // No exact match, but a near candidate in the same genus.
        var close = Assert.Single(data.CloseMatch, f => f.ScientificName == "Panthera leoo");
        Assert.Equal("Panthera leo", close.SuggestedValue);
        // IUCN files the closest name as a synonym of taxon 103, not of this row, and the cell names
        // that taxon (by id here, since the test index carries no accepted names).
        Assert.Equal("IUCN taxon 103", close.Get("iucnSynonym"));

        // No exact match and no near candidate.
        Assert.Contains(data.NotFound, f => f.ScientificName == "Zzzus nonexistus");

        // Higher-rank placement: a spelling-variant parent (typo) versus a genuinely different one.
        // Higher-rank names are shown exactly as IUCN records them (upper case).
        var typo = Assert.Single(data.Classification, f => f.ScientificName == "MURICIDAE");
        Assert.Equal("order", typo.Field);
        Assert.Equal("Neogastropoda", typo.SuggestedValue);
        var reorg = Assert.Single(data.Reorg, f => f.ScientificName == "REORGIDAE");
        Assert.Equal("order", reorg.Field);
        Assert.Equal("Omega", reorg.SuggestedValue);

        // Author-name difference (diacritic) is kept, with the CoL year; the exact-match control is
        // silent, and an authority differing only by the year is dropped.
        var authOnca = Assert.Single(data.Authority, f => f.ScientificName == "Panthera onca");
        Assert.Equal("1776", authOnca.Get("colYear"));
        Assert.DoesNotContain(data.Authority, f => f.ScientificName == "Panthera leo");
        Assert.DoesNotContain(data.Authority, f => f.ScientificName == "Yearus changeus");
        Assert.DoesNotContain(data.Authority, f => f.ScientificName == "Punctus namus"); // punctuation-only
        Assert.DoesNotContain(data.Synonym, f => f.ScientificName == "Panthera leo");

        // Class-rank placement surfaces: the same-phylum gate steps up to kingdom for class rank,
        // so a class placed under a different phylum is not silently gated away.
        var classReorg = Assert.Single(data.Reorg, f => f.ScientificName == "TESTACLASS");
        Assert.Equal("class", classReorg.Rank);
        Assert.Equal("phylum", classReorg.Field);
        Assert.Equal("Alpha", classReorg.SuggestedValue);

        // A name whose only CoL usage is "misapplied" is neither accepted nor a synonym: it is not
        // reported as a clean match, a synonym, not-found, or a spurious authority typo.
        Assert.DoesNotContain(data.Authority, f => f.ScientificName == "Misapplia namus");
        Assert.DoesNotContain(data.Synonym, f => f.ScientificName == "Misapplia namus");
        Assert.DoesNotContain(data.NotFound, f => f.ScientificName == "Misapplia namus");
    }

    // ---- fixtures ----

    private static IReadOnlyList<IucnTaxonomyRow> IucnRows() => new[] {
        Row(1, 101, "Panthera leo", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera", "leo", "(Linnaeus, 1758)"),
        Row(2, 102, "Panthera onca", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera", "onca", "Müller, 1776"),
        Row(3, 103, "Felis leo", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Felis", "leo", yearPublished: "1998"),
        Row(4, 104, "Zzzus nonexistus", "ANIMALIA", "ARTHROPODA", "INSECTA", "DIPTERA", "ZZZIDAE", "Zzzus", "nonexistus"),
        Row(5, 105, "Panthera leoo", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Panthera", "leoo"),
        Row(6, 106, "Muricidus testus", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "NEOGASTROPODAA", "MURICIDAE", "Muricidus", "testus"),
        Row(7, 107, "Reorgus testus", "ANIMALIA", "ZETA", "GAMMA", "ALPHA", "REORGIDAE", "Reorgus", "testus"),
        Row(8, 108, "Bofonaria testus", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "NEOGASTROPODA", "BURSIDAE", "Bofonaria", "testus"),
        Row(9, 109, "Anemone testus", "PLANTAE", "TRACHEOPHYTA", "MAGNOLIOPSIDA", "RANUNCULALES", "RANUNCULACEAE", "Anemone", "testus"),
        // Class placed under a different phylum than CoL (BETA vs Alpha); genus/family/order are
        // absent from CoL so only the class rank is compared.
        Row(10, 110, "Testagenus testsp", "KDOM", "BETA", "TESTACLASS", "TESTAORD", "TESTAFAM", "Testagenus", "testsp"),
        // A name CoL records only as "misapplied", with an authority that would otherwise look like a typo.
        Row(11, 111, "Misapplia namus", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Misapplia", "namus", "Foo, 1901"),
        // Exact match whose authority differs only by the year -> dropped from the authority report.
        Row(12, 112, "Yearus changeus", "ANIMALIA", "ARTHROPODA", "INSECTA", "COLEOPTERA", "YEARIDAE", "Yearus", "changeus", "(Smith, 1900)"),
        // Exact match whose authority differs only by punctuation (an apostrophe) -> also dropped.
        Row(13, 113, "Punctus namus", "ANIMALIA", "MOLLUSCA", "GASTROPODA", "LITTORINIMORPHA", "PUNCTIDAE", "Punctus", "namus", "(dOrbigny, 1835)"),
        // Reversed pair whose two authorities credit the same author but a different year.
        Row(14, 114, "Yearclash oldus", "ANIMALIA", "CHORDATA", "ACTINOPTERYGII", "CYPRINIFORMES", "CYPRINIDAE", "Yearclash", "oldus", "Herzenstein, 1881"),
        // Reversed pair whose two authorities are different authors: the year gap is not a date to check.
        Row(15, 115, "Authordiff oldus", "ANIMALIA", "CHORDATA", "AVES", "COLUMBIFORMES", "COLUMBIDAE", "Authordiff", "oldus", "Layard, 1875"),
    };

    private static IucnTaxonomyRow Row(long assessmentId, long taxonId, string name, string kingdom, string phylum,
        string klass, string order, string family, string genus, string species, string? authority = null,
        string? yearPublished = null) =>
        new(assessmentId, taxonId, name, name, kingdom, phylum, klass, order, family, genus, species,
            null, null, null, authority, null, "Least Concern", yearPublished);

    private static SqliteConnection BuildCol() {
        // Pooling off so each test gets a private in-memory database (a pooled :memory: connection
        // would keep its table and rows across the using, colliding with the next build).
        var conn = new SqliteConnection("Data Source=:memory:;Pooling=False");
        conn.Open();
        using (var create = conn.CreateCommand()) {
            create.CommandText = @"CREATE TABLE nameusage (
                ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, authorship TEXT, rank TEXT,
                kingdom TEXT, phylum TEXT, ""class"" TEXT, ""order"" TEXT, family TEXT,
                genericName TEXT, specificEpithet TEXT);";
            create.ExecuteNonQuery();
        }

        // Accepted species.
        AddCol(conn, "P_LEO", "species", "accepted", "Panthera leo", authorship: "(Linnaeus, 1758)",
            kingdom: "Animalia", phylum: "Chordata", klass: "Mammalia", order: "Carnivora", family: "Felidae",
            genericName: "Panthera", specificEpithet: "leo");
        AddCol(conn, "P_ONCA", "species", "accepted", "Panthera onca", authorship: "Muller, 1776",
            kingdom: "Animalia", phylum: "Chordata", klass: "Mammalia", order: "Carnivora", family: "Felidae",
            genericName: "Panthera", specificEpithet: "onca");

        // Species synonym reached through parentID.
        AddCol(conn, "FELIS_LEO", "species", "synonym", "Felis leo", parentId: "P_LEO",
            genericName: "Felis", specificEpithet: "leo");

        // Higher taxa (accepted) carrying inline ancestors.
        AddCol(conn, "PANTHERA_G", "genus", "accepted", "Panthera",
            kingdom: "Animalia", phylum: "Chordata", klass: "Mammalia", order: "Carnivora", family: "Felidae");
        AddCol(conn, "FELIDAE", "family", "accepted", "Felidae",
            kingdom: "Animalia", phylum: "Chordata", klass: "Mammalia", order: "Carnivora");
        AddCol(conn, "CARNIVORA_O", "order", "accepted", "Carnivora",
            kingdom: "Animalia", phylum: "Chordata", klass: "Mammalia");
        AddCol(conn, "MURICIDAE", "family", "accepted", "Muricidae",
            kingdom: "Animalia", phylum: "Mollusca", klass: "Gastropoda", order: "Neogastropoda");
        AddCol(conn, "REORGIDAE", "family", "accepted", "Reorgidae",
            kingdom: "Animalia", phylum: "Zeta", klass: "Gamma", order: "Omega");

        // Higher-rank synonym (no accepted usage anywhere) -> reported.
        AddCol(conn, "BUFONARIA", "genus", "accepted", "Bufonaria", authorship: "(Schumacher, 1817)",
            kingdom: "Animalia", phylum: "Mollusca", klass: "Gastropoda", family: "Bursidae");
        AddCol(conn, "BOFONARIA", "genus", "synonym", "Bofonaria", parentId: "BUFONARIA");

        // Homonym: Anemone is both an accepted genus and a synonym -> suppressed.
        AddCol(conn, "ANEMONE_ACC", "genus", "accepted", "Anemone",
            kingdom: "Plantae", phylum: "Tracheophyta", klass: "Magnoliopsida", order: "Ranunculales", family: "Ranunculaceae");
        AddCol(conn, "ANEMONE_SYN", "genus", "synonym", "Anemone", parentId: "BUFONARIA");

        // Accepted class in a different phylum (Alpha) than IUCN records (BETA); same kingdom.
        AddCol(conn, "TESTACLASS_C", "class", "accepted", "Testaclass", kingdom: "Kdom", phylum: "Alpha");

        // A name CoL keeps only as a misapplied usage (neither accepted nor synonym).
        AddCol(conn, "MISAPP", "species", "misapplied", "Misapplia namus", authorship: "Foo, 1900", parentId: "P_LEO",
            genericName: "Misapplia", specificEpithet: "namus");

        // Exact match whose authority differs only by the year -> dropped from the authority report.
        AddCol(conn, "YEARUS", "species", "accepted", "Yearus changeus", authorship: "(Smith, 1902)",
            kingdom: "Animalia", phylum: "Arthropoda", klass: "Insecta", order: "Coleoptera",
            genericName: "Yearus", specificEpithet: "changeus");

        // Reversed pair, same author, different year -> the authority-years flag.
        AddCol(conn, "YC_NEW", "species", "accepted", "Yearclash novus", authorship: "(Herzenstein, 1891)",
            kingdom: "Animalia", phylum: "Chordata", klass: "Actinopterygii", order: "Cypriniformes", family: "Cyprinidae",
            genericName: "Yearclash", specificEpithet: "novus");
        AddCol(conn, "YC_OLD", "species", "synonym", "Yearclash oldus", authorship: "Herzenstein, 1881", parentId: "YC_NEW",
            genericName: "Yearclash", specificEpithet: "oldus");

        // Reversed pair, different authors -> no flag.
        AddCol(conn, "AD_NEW", "species", "accepted", "Authordiff novus", authorship: "D. G. Elliot, 1878",
            kingdom: "Animalia", phylum: "Chordata", klass: "Aves", order: "Columbiformes", family: "Columbidae",
            genericName: "Authordiff", specificEpithet: "novus");
        AddCol(conn, "AD_OLD", "species", "synonym", "Authordiff oldus", authorship: "Layard, 1875", parentId: "AD_NEW",
            genericName: "Authordiff", specificEpithet: "oldus");

        // Exact match whose authority differs only by punctuation (apostrophe) -> dropped.
        AddCol(conn, "PUNCTUS", "species", "accepted", "Punctus namus", authorship: "(d'Orbigny, 1835)",
            kingdom: "Animalia", phylum: "Mollusca", klass: "Gastropoda", order: "Littorinimorpha",
            genericName: "Punctus", specificEpithet: "namus");

        return conn;
    }

    private static void AddCol(SqliteConnection conn, string id, string rank, string status, string scientificName,
        string? authorship = null, string? parentId = null, string? kingdom = null, string? phylum = null,
        string? klass = null, string? order = null, string? family = null, string? genericName = null,
        string? specificEpithet = null) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO nameusage
            (ID, parentID, status, scientificName, authorship, rank, kingdom, phylum, ""class"", ""order"", family, genericName, specificEpithet)
            VALUES (@id, @parent, @status, @sci, @auth, @rank, @kingdom, @phylum, @class, @order, @family, @genus, @sp);";
        P(cmd, "@id", id);
        P(cmd, "@parent", parentId);
        P(cmd, "@status", status);
        P(cmd, "@sci", scientificName);
        P(cmd, "@auth", authorship);
        P(cmd, "@rank", rank);
        P(cmd, "@kingdom", kingdom);
        P(cmd, "@phylum", phylum);
        P(cmd, "@class", klass);
        P(cmd, "@order", order);
        P(cmd, "@family", family);
        P(cmd, "@genus", genericName);
        P(cmd, "@sp", specificEpithet);
        cmd.ExecuteNonQuery();
    }

    private static void P(SqliteCommand cmd, string name, string? value) =>
        cmd.Parameters.AddWithValue(name, (object?)value ?? DBNull.Value);
}
