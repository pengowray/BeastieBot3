using System;
using System.IO;
using System.Linq;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `wikipedia reparse-taxoboxes`: what counts as a change when a page's stored taxobox fields are
// compared with what the parser reads now, and the cache reads and writes it uses. The stored
// fields come from the parser of the day the page was downloaded, so they only change when this
// command runs or the page is downloaded again.
public sealed class TaxoboxReparseTests {
    private const string SlowLoris = """
        {{Speciesbox
        | name = Sunda slow loris{{sfn|Groves|2005|p=122}}
        | status = VU
        | genus = Nycticebus
        | species = coucang
        }}
        """;

    // The fields the parser before October 2026 stored for SlowLoris: the nested template kept one
    // brace.
    private static WikiTaxoboxData OldSlowLoris(long pageRowId) =>
        TaxoboxParser.TryParse(pageRowId, SlowLoris)! with {
            DataJson = """{"name":"Sunda slow loris{sfn|Groves|2005|p=122}","status":"VU","genus":"Nycticebus","species":"coucang"}""",
        };

    private static WikiTaxoboxData Parse(string wikitext) => TaxoboxParser.TryParse(1, wikitext)!;

    [Fact]
    public void SameFields_AreUnchanged() {
        var result = TaxoboxReparse.Compare(Parse(SlowLoris), Parse(SlowLoris));

        Assert.Equal(TaxoboxReparseOutcome.Unchanged, result.Outcome);
        Assert.Empty(result.Columns);
        Assert.Empty(result.Parameters);
    }

    [Fact]
    public void ParametersInAnotherOrder_AreUnchanged() {
        var stored = Parse(SlowLoris) with {
            DataJson = """{"species":"coucang","genus":"Nycticebus","status":"VU","name":"Sunda slow loris{{sfn|Groves|2005|p=122}}"}""",
        };

        Assert.Equal(TaxoboxReparseOutcome.Unchanged, TaxoboxReparse.Compare(stored, Parse(SlowLoris)).Outcome);
    }

    [Fact]
    public void NestedTemplateKeepingBothBraces_ChangesTheNameParameter() {
        var result = TaxoboxReparse.Compare(OldSlowLoris(1), Parse(SlowLoris));

        Assert.Equal(TaxoboxReparseOutcome.Changed, result.Outcome);
        Assert.Equal(new[] { "name" }, result.Parameters);
        Assert.Empty(result.Columns);
        Assert.True(result.ParameterChanged("Name"));
    }

    [Fact]
    public void ChangedColumn_IsNamed() {
        var stored = Parse(SlowLoris) with { ScientificName = "Sunda slow loris{sfn|Groves|2005|p=122}", Genus = null };

        var result = TaxoboxReparse.Compare(stored, Parse(SlowLoris));

        Assert.Equal(TaxoboxReparseOutcome.Changed, result.Outcome);
        Assert.Equal(new[] { "scientific_name", "genus" }, result.Columns);
    }

    [Fact]
    public void ParameterAddedOrRemoved_IsAChange() {
        Assert.Equal(new[] { "status" }, TaxoboxReparse.ChangedParameters("""{"name":"A","status":"VU"}""", """{"name":"A"}"""));
        Assert.Equal(new[] { "image" }, TaxoboxReparse.ChangedParameters("""{"name":"A"}""", """{"name":"A","image":"a.jpg"}"""));
    }

    [Fact]
    public void TaxoboxFoundOrLost_IsAddedOrRemoved() {
        Assert.Equal(TaxoboxReparseOutcome.Added, TaxoboxReparse.Compare(null, Parse(SlowLoris)).Outcome);
        Assert.Equal(TaxoboxReparseOutcome.Removed, TaxoboxReparse.Compare(Parse(SlowLoris), null).Outcome);
        Assert.Equal(TaxoboxReparseOutcome.Unchanged, TaxoboxReparse.Compare(null, null).Outcome);
    }

    [Fact]
    public void Tally_CountsEachOutcomeAndTheCommonNamesThatChange() {
        // The name field changes inside the citation only, so the common name read from it is
        // "Sunda slow loris" before and after; a taxobox found or lost changes the common name.
        var tally = new TaxoboxReparseTally();
        var changed = Parse(SlowLoris);
        tally.Add("Nycticebus coucang", TaxoboxReparse.Compare(OldSlowLoris(1), changed), OldSlowLoris(1), changed);
        tally.Add("Same", TaxoboxReparse.Compare(changed, changed), changed, changed);
        tally.Add("New", TaxoboxReparse.Compare(null, changed), null, changed);
        tally.Add("Gone", TaxoboxReparse.Compare(changed, null), changed, null);

        Assert.Equal((4, 1, 1, 1, 1, 1, 0, 0, 2), (tally.PagesRead, tally.Unchanged, tally.Changed, tally.Added, tally.Removed,
            tally.NameChanged, tally.ScientificNameChanged, tally.OtherColumnsChanged, tally.CommonNameChanged));
        Assert.Equal(3, tally.ToSave);
        Assert.Equal(new (string, string?, string?)[] { ("New", null, "Sunda slow loris"), ("Gone", "Sunda slow loris", null) },
            tally.CommonNameExamples);
    }

    private static long AddDownloadedPage(WikipediaCacheStore store, string title, string? wikitext) {
        var now = new DateTime(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc);
        var row = store.UpsertPageCandidate(new WikiPageCandidate(title, title.ToLowerInvariant(), null, now, now)).PageRowId;
        store.SavePageContent(new WikiPageContent(row, null, title, title.ToLowerInvariant(), null, false, null, false, false,
            wikitext is not null, null, null, wikitext, store.BeginImport("test"), now));
        return row;
    }

    [Fact]
    public void Store_ReadsDownloadedPagesWithTheirTaxoboxes_AndSavesChangesTogether() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = WikipediaCacheStore.OpenFromConnection(connection);
        var loris = AddDownloadedPage(store, "Sunda slow loris", SlowLoris);
        store.UpsertTaxoboxData(OldSlowLoris(loris));
        var noBox = AddDownloadedPage(store, "Slow loris (disambiguation)", "A list of lorises.");
        store.UpsertTaxoboxData(OldSlowLoris(noBox));
        var now = new DateTime(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc);
        store.UpsertPageCandidate(new WikiPageCandidate("Queued", "queued", null, now, now));

        var pages = store.ReadDownloadedPages(0, 10);

        Assert.Equal(new[] { "Sunda slow loris", "Slow loris (disambiguation)" }, pages.Select(p => p.PageTitle));
        Assert.Equal(2, store.CountDownloadedPages());
        Assert.Equal(OldSlowLoris(loris), pages[0].Taxobox);
        Assert.Single(store.ReadDownloadedPages(loris, 10));

        var parsed = TaxoboxParser.TryParse(loris, pages[0].Wikitext)!;
        store.SaveTaxoboxChanges(new[] { parsed }, new[] { noBox });

        Assert.Equal(parsed, store.GetTaxoboxData(loris));
        Assert.Null(store.GetTaxoboxData(noBox));
    }

    [Fact]
    public void OpenReadOnly_ReadsAndRefusesWrites() {
        var path = Path.Combine(Path.GetTempPath(), $"enwiki-readonly-{Guid.NewGuid():N}.sqlite");
        try {
            long loris;
            using (var store = WikipediaCacheStore.Open(path)) {
                loris = AddDownloadedPage(store, "Sunda slow loris", SlowLoris);
                store.UpsertTaxoboxData(OldSlowLoris(loris));
            }
            SqliteConnection.ClearAllPools();

            Assert.Null(WikipediaCacheStore.OpenReadOnly(path + ".missing"));
            using (var readOnly = WikipediaCacheStore.OpenReadOnly(path)) {
                Assert.NotNull(readOnly);
                Assert.Equal(OldSlowLoris(loris), readOnly!.GetTaxoboxData(loris));
                Assert.Throws<SqliteException>(() => readOnly.DeleteTaxoboxData(loris));
            }
        } finally {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-wal", "-shm" }) {
                try { File.Delete(path + suffix); } catch (IOException) { }
            }
        }
    }
}
