using System;
using System.Linq;
using System.Threading;
using BeastieBot3.Audit.Producers.ColCrosscheck;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// Pins the Wikidata/Wikipedia lookup behind the three extra columns on "Names not found in the
// Catalogue of Life". Both caches are optional and sit outside the Red List, so the cases that
// matter are the missing ones: an absent cache must read as "did not check", never as "found
// nothing", and a bare genus must never be offered as another name for a species.
public class OtherSourceIndexTests {
    private const long Taxon = 188716;

    [Fact]
    public void ReportsTheItemTheArticleAndTheOtherNames() {
        using var wd = Wikidata();
        using var wp = Wikipedia();
        var index = OtherSourceIndex.Build(wd, wp)!;

        var hit = index.Lookup(Taxon, "Idiopoma javanica", CancellationToken.None);

        Assert.Equal("Q42", hit.WikidataId);
        Assert.Equal("Filopaludina javanica", hit.WikipediaTitle);
        Assert.Contains("Filopaludina javanica", hit.OtherNames);
        // The IUCN name is already known to be absent from CoL, so offering it back is noise.
        Assert.DoesNotContain("Idiopoma javanica", hit.OtherNames);
        // A redirect target is another name for the taxon and is worth following.
        Assert.Contains("Bellamya javanica", hit.OtherNames);
    }

    [Fact]
    public void ABareGenusIsNotAnotherNameForASpecies() {
        using var wd = Wikidata(scientificName: "Launaea");
        var index = OtherSourceIndex.Build(wd, null)!;

        var hit = index.Lookup(Taxon, "Launaea sp. nov. A", CancellationToken.None);

        Assert.Equal("Q42", hit.WikidataId);
        Assert.Empty(hit.OtherNames);
    }

    [Fact]
    public void NoCachesMeansNoIndexAtAll() {
        // The caller leaves the columns out of the table entirely, rather than rendering them blank.
        Assert.Null(OtherSourceIndex.Build(null, null));
    }

    [Fact]
    public void OneCacheIsEnoughAndTheOtherStaysSilent() {
        using var wp = Wikipedia();
        var index = OtherSourceIndex.Build(null, wp)!;

        Assert.False(index.HasWikidata);
        Assert.True(index.HasWikipedia);
        var hit = index.Lookup(Taxon, "Idiopoma javanica", CancellationToken.None);
        Assert.Null(hit.WikidataId);
        Assert.Equal("Filopaludina javanica", hit.WikipediaTitle);
    }

    [Fact]
    public void ATaxonNothingElseRecordsComesBackEmpty() {
        using var wd = Wikidata();
        using var wp = Wikipedia();
        var index = OtherSourceIndex.Build(wd, wp)!;

        var hit = index.Lookup(999_999, "Gnathophis tritos", CancellationToken.None);

        Assert.Null(hit.WikidataId);
        Assert.Null(hit.WikipediaTitle);
        Assert.Empty(hit.OtherNames);
    }

    [Theory]
    [InlineData("Q42", "https://www.wikidata.org/wiki/Q42")]
    public void WikidataUrlsPointAtTheItem(string id, string expected) =>
        Assert.Equal(expected, OtherSourceIndex.WikidataUrl(id));

    [Theory]
    [InlineData("Filopaludina javanica", "https://en.wikipedia.org/wiki/Filopaludina_javanica")]
    public void WikipediaUrlsUseUnderscores(string title, string expected) =>
        Assert.Equal(expected, OtherSourceIndex.WikipediaUrl(title));

    private static SqliteConnection Wikidata(string scientificName = "Filopaludina javanica") {
        var conn = Open();
        Exec(conn, """
            CREATE TABLE wikidata_entities (entity_numeric_id INTEGER, entity_id TEXT);
            CREATE TABLE wikidata_p627_values (entity_numeric_id INTEGER, value TEXT);
            CREATE TABLE wikidata_scientific_names (entity_numeric_id INTEGER, name TEXT);
            INSERT INTO wikidata_entities VALUES (42, 'Q42');
            INSERT INTO wikidata_p627_values VALUES (42, '188716');
            """);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO wikidata_scientific_names VALUES (42, @n)";
        cmd.Parameters.AddWithValue("@n", scientificName);
        cmd.ExecuteNonQuery();
        return conn;
    }

    private static SqliteConnection Wikipedia() {
        var conn = Open();
        Exec(conn, """
            CREATE TABLE wiki_pages (id INTEGER PRIMARY KEY, page_title TEXT, redirect_target TEXT);
            CREATE TABLE taxon_wiki_matches (taxon_source TEXT, taxon_identifier TEXT, match_status TEXT,
                                             page_row_id INTEGER, candidate_title TEXT);
            INSERT INTO wiki_pages VALUES (1, 'Filopaludina javanica', 'Bellamya_javanica');
            INSERT INTO taxon_wiki_matches VALUES ('iucn', '188716', 'matched', 1, 'Filopaludina javanica');
            """);
        return conn;
    }

    private static SqliteConnection Open() {
        var conn = new SqliteConnection("Data Source=:memory:;Pooling=False");
        conn.Open();
        return conn;
    }

    private static void Exec(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
