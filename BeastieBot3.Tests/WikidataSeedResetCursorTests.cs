using BeastieBot3.Wikidata;
using Xunit;

namespace BeastieBot3.Tests;

// `wikidata seed-taxa --reset-cursor` used to write 0 to each pass's cursor as that pass started.
// ReadCursor reads a pass cursor of 0 as "no cursor of its own" and falls back to the combined
// cursor from before the sweep was split, so a reset run that stopped early (--limit, an outage,
// Ctrl+C) resumed on the next run from the old combined position instead of the first Q-number.
// A pass the run never reached (P141, after an outage in the P627 pass) was not reset at all.
public class WikidataSeedResetCursorTests {
    private const string CombinedCursorKey = "wikidata_taxa_cursor";
    private static string P627 => WikidataSeedCommand.Passes[0].CursorKey;
    private static string P141 => WikidataSeedCommand.Passes[1].CursorKey;

    [Fact]
    public void Reset_StartsBothPassesFromTheFirstQNumber_EvenWithACombinedCursor() {
        using var store = WikidataCacheStore.Open(":memory:");
        store.SetSyncCursor(CombinedCursorKey, 136591620);
        store.SetSyncCursor(P627, 141142115);
        // P141 has no cursor of its own, so before the reset it reads the combined one.
        Assert.Equal(136591620, WikidataSeedCommand.ReadCursor(store, P141));

        Assert.True(WikidataSeedCommand.ApplyResetCursor(new WikidataSeedSettings { ResetCursor = true }, store));

        Assert.Equal(0, WikidataSeedCommand.ReadCursor(store, P627));
        Assert.Equal(0, WikidataSeedCommand.ReadCursor(store, P141));
    }

    [Fact]
    public void AfterAReset_APassThatHasNotMovedStaysAtTheStart() {
        using var store = WikidataCacheStore.Open(":memory:");
        store.SetSyncCursor(CombinedCursorKey, 136591620);
        WikidataSeedCommand.ApplyResetCursor(new WikidataSeedSettings { ResetCursor = true }, store);

        // The reset run reads one batch in the P627 pass, then stops at --limit before P141 moves.
        store.SetSyncCursor(P627, 500);

        Assert.Equal(500, WikidataSeedCommand.ReadCursor(store, P627));
        Assert.Equal(0, WikidataSeedCommand.ReadCursor(store, P141));
    }

    [Fact]
    public void Reset_IsIgnoredWhenACursorIsGiven() {
        using var store = WikidataCacheStore.Open(":memory:");
        store.SetSyncCursor(CombinedCursorKey, 136591620);
        store.SetSyncCursor(P627, 141142115);

        var settings = new WikidataSeedSettings { ResetCursor = true, Cursor = "Q5" };
        Assert.False(WikidataSeedCommand.ApplyResetCursor(settings, store));

        Assert.Equal(141142115, WikidataSeedCommand.ReadCursor(store, P627));
        Assert.Equal(136591620, WikidataSeedCommand.ReadCursor(store, P141));
    }

    [Fact]
    public void WithoutReset_NothingIsWritten() {
        using var store = WikidataCacheStore.Open(":memory:");
        store.SetSyncCursor(CombinedCursorKey, 136591620);

        Assert.False(WikidataSeedCommand.ApplyResetCursor(new WikidataSeedSettings(), store));

        // An older cache still carries on from the combined cursor.
        foreach (var pass in WikidataSeedCommand.Passes) {
            Assert.Equal(136591620, WikidataSeedCommand.ReadCursor(store, pass.CursorKey));
        }
    }
}
