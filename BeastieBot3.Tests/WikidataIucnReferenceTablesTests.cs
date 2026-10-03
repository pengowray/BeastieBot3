using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the two lists `wikidata iucn-assessment-items` stores for `site build-db`: the Red List's
// editions and the IUCN taxon IDs at deprecated rank (WikidataIucnReferenceTables).
public class WikidataIucnReferenceTablesTests {
    [Fact]
    public void ParseDeprecatedTaxonIds_ItemAndId() {
        // Shape of the query service's answer on 3 October 2026 (two of its 11 rows, one repeated).
        const string json = """
            {"head":{"vars":["item","id"]},"results":{"bindings":[
              {"id":{"type":"literal","value":"96251644"},"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q3008560"}},
              {"id":{"type":"literal","value":"136526"},"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q2684561"}},
              {"id":{"type":"literal","value":"136526"},"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q2684561"}},
              {"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q1"}}
            ]}}
            """;
        Assert.Equal(new[] { (3008560L, "96251644"), (2684561L, "136526") }, WikidataIucnReferenceTables.ParseDeprecatedTaxonIds(json));
    }

    [Fact]
    public void Query_AsksForDeprecatedRankOnly_AndLeavesOutAnIdAlsoStatedAtAnotherRank() {
        var query = WikidataIucnReferenceTables.DeprecatedTaxonIdsQuery();
        Assert.Contains("wikibase:rank wikibase:DeprecatedRank", query);
        Assert.Contains("FILTER NOT EXISTS", query);
    }

    [Fact]
    public void Store_ReplacesTheLists_AndAMissingTableReadsAsNull() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        Assert.Null(WikidataIucnReferenceTables.ReadEditions(connection));
        Assert.Null(WikidataIucnReferenceTables.ReadDeprecatedTaxonIds(connection));

        using var store = WikidataCacheStore.OpenFromConnection(connection);
        Assert.Empty(WikidataIucnReferenceTables.ReadEditions(connection)!);
        var at = new DateTime(2026, 10, 3, 1, 0, 0, DateTimeKind.Utc);
        store.ReplaceRedListEditions(["Q115962546", "Q136547248"], at);
        store.ReplaceDeprecatedIucnTaxonIds([(3008560, "96251644"), (2684561, "136526")], at);
        store.ReplaceDeprecatedIucnTaxonIds([(3008560, "96251644")], at);

        Assert.Equal(new HashSet<string> { "Q115962546", "Q136547248" }, WikidataIucnReferenceTables.ReadEditions(connection));
        Assert.Equal(new HashSet<(long, string)> { (3008560, "96251644") }, WikidataIucnReferenceTables.ReadDeprecatedTaxonIds(connection));
    }
}
