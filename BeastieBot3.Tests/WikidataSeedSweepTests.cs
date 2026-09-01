using BeastieBot3.Wikidata;
using Xunit;

namespace BeastieBot3.Tests;

// The sweep query is the fix, so its shape is the test. It used to UNION P141 with P627, join
// `instance of: taxon`, then GROUP BY and ORDER BY, which put the whole cost before the LIMIT: the
// query service timed out identically at batch 250, 125, 62 and 50, and `wikipedia update` died at
// its first step. One property per query, and nothing else in the WHERE clause, answers in seconds.
public class WikidataSeedSweepTests {
    [Theory]
    [InlineData(WikidataSeedProperty.IucnTaxonId, "P627")]
    [InlineData(WikidataSeedProperty.ConservationStatus, "P141")]
    internal void EachPassAsksAboutOnePropertyOnly(WikidataSeedProperty property, string expected) {
        var query = WikidataApiClient.BuildTaxonQuery(property, 136591620, 50);

        Assert.Contains($"wdt:{expected} ?value", query);
        Assert.DoesNotContain("UNION", query);
        Assert.DoesNotContain("GROUP BY", query);
        // The taxon-type join cost 6x the time and excluded 852 items that carry an IUCN taxon id
        // but are typed as something other than a plain taxon.
        Assert.DoesNotContain("wd:Q16521", query);
    }

    [Fact]
    public void TheCursorAndLimitReachTheQuery() {
        var query = WikidataApiClient.BuildTaxonQuery(WikidataSeedProperty.IucnTaxonId, 136591620, 250);

        Assert.Contains("FILTER(?qid > 136591620)", query);
        Assert.Contains("LIMIT 250", query);
        Assert.Contains("ORDER BY ?qid", query);
    }

    [Fact]
    public void TheTwoPassesKeepSeparateCursors() {
        var keys = new HashSet<string>();
        foreach (var pass in WikidataSeedCommand.Passes) {
            Assert.True(keys.Add(pass.CursorKey), $"{pass.Label} shares a cursor with another pass");
        }
        Assert.Equal(2, keys.Count);
    }

    [Fact]
    public void AnOlderCacheCarriesOnFromTheCombinedCursor() {
        using var store = WikidataCacheStore.Open(":memory:");
        // What a cache written before the split holds: one cursor, covering both properties.
        store.SetSyncCursor("wikidata_taxa_cursor", 136591620);

        foreach (var pass in WikidataSeedCommand.Passes) {
            Assert.Equal(136591620, WikidataSeedCommand.ReadCursor(store, pass.CursorKey));
        }

        // Once a pass records its own position, that one wins.
        store.SetSyncCursor(WikidataSeedCommand.Passes[0].CursorKey, 141142115);
        Assert.Equal(141142115, WikidataSeedCommand.ReadCursor(store, WikidataSeedCommand.Passes[0].CursorKey));
        Assert.Equal(136591620, WikidataSeedCommand.ReadCursor(store, WikidataSeedCommand.Passes[1].CursorKey));
    }
}
