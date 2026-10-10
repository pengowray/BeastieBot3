using BeastieBot3.Iucn.Doi;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests.Doi;

// Pins reading Crossref's list of IUCN DOIs, the DOI cache's round trip, and which assessments a
// run checks.
public class CrossrefAndStoreTests {
    private static IucnDoiCacheStore MemoryStore(out SqliteConnection connection) {
        connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return IucnDoiCacheStore.OpenFromConnection(connection);
    }

    private const string Page1 = """
        {"status":"ok","message-type":"work-list","message":{"total-results":4,"next-cursor":"abc+/=","items":[
          {"DOI":"10.2305\/iucn.ch.2005.3.en","resource":{"primary":{"URL":"http:\/\/www.iucn.org\/bookstore\/cover.html"}}},
          {"DOI":"10.2305\/iucn.uk.2015-4.rlts.t22823a14871490.en","resource":{"primary":{"URL":"https:\/\/www.iucnredlist.org\/species\/22823\/14871490"}},"title":["Ursus maritimus: Wiig, \u00d8., Amstrup, S. &amp; Atwood, T."],"created":{"date-parts":[[2015,11,19]],"date-time":"2015-11-19T10:00:00Z","timestamp":1447927200000}},
          {"DOI":"10.2305\/iucn.uk.2016-2.rlts.t712a45033386.en","resource":{"primary":{"URL":"https:\/\/www.iucnredlist.org\/species\/712\/121745669"}}}
        ]}}
        """;

    private const string Page2 = """
        {"status":"ok","message":{"total-results":4,"next-cursor":"def","items":[
          {"DOI":"10.2305\/iucn.uk.2020-3.rlts.e.t1a2.en","resource":{"primary":{"URL":"https:\/\/www.iucnredlist.org\/species\/1\/2"}}}
        ]}}
        """;

    private const string EmptyPage = """{"status":"ok","message":{"total-results":4,"next-cursor":"ghi","items":[]}}""";

    [Fact]
    public void ParsePage_KeepsAssessmentDois_InCanonicalForm_WithTheirPages() {
        var page = CrossrefIucnWorks.ParsePage(Page1);

        Assert.Equal(4, page.TotalResults);
        Assert.Equal("abc+/=", page.NextCursor);
        Assert.Equal(3, page.ItemCount);
        Assert.Equal(2, page.Works.Count);
        var bear = page.Works[0];
        Assert.Equal(new CrossrefIucnWork("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", 22823, 14871490, "2015-4", "en",
            "https://www.iucnredlist.org/species/22823/14871490", 22823, 14871490,
            "Ursus maritimus: Wiig, \u00d8., Amstrup, S. &amp; Atwood, T.", "2015-11-19"), bear);
        var panda = page.Works[1];
        Assert.Equal(45033386, panda.AssessmentId);
        Assert.Equal(121745669, panda.UrlAssessmentId);
        Assert.Null(panda.Title);
        Assert.Null(panda.Created);
        Assert.Empty(page.UnreadRlts);
    }

    [Fact]
    public void ParsePage_ListsRltsDoisOfAnotherForm() {
        var page = CrossrefIucnWorks.ParsePage(Page2);
        Assert.Empty(page.Works);
        Assert.Equal(new[] { "10.2305/iucn.uk.2020-3.rlts.e.t1a2.en" }, page.UnreadRlts);
    }

    [Fact]
    public async Task Download_ReadsEveryPageUntilAnEmptyOne_AndCompletesTheListing() {
        var handler = new StubHandler((request, call) => StubHandler.Json(200, call switch {
            1 => Page1,
            2 => Page2,
            _ => EmptyPage,
        }));
        using var http = new HttpClient(handler);
        using var store = MemoryStore(out var connection);
        using var _ = connection;

        var pages = new List<CrossrefPage>();
        var listingId = await CrossrefIucnWorks.DownloadAsync(new PoliteHttpGetter(http, TimeSpan.Zero), store, pages.Add, CancellationToken.None);

        Assert.Equal(3, handler.Urls.Count);
        Assert.Equal("https://api.crossref.org/prefixes/10.2305/works?rows=1000&select=DOI,resource,title,created&cursor=%2A", handler.Urls[0]);
        Assert.EndsWith("cursor=abc%2B%2F%3D", handler.Urls[1]);
        Assert.Equal(2, store.CountCrossrefWorks());
        using (var created = connection.CreateCommand()) {
            created.CommandText = "SELECT created FROM crossref_works WHERE assessment_id = 14871490";
            Assert.Equal("2015-11-19", created.ExecuteScalar());
        }
        var listing = Assert.IsType<CrossrefListing>(store.LastCompletedListing());
        Assert.Equal(listingId, listing.Id);
        Assert.Equal(4, listing.TotalResults);
        Assert.Equal(4, listing.WorksSeen);
        Assert.Equal(3, listing.Requests);
        Assert.NotNull(listing.CompletedAtUtc);
    }

    [Fact]
    public async Task Download_StoppedPartWay_LeavesNoCompletedListing() {
        var handler = new StubHandler((_, call) => call == 1 ? StubHandler.Json(200, Page1) : StubHandler.Json(400, "{}"));
        using var http = new HttpClient(handler);
        using var store = MemoryStore(out var connection);
        using var _ = connection;

        await Assert.ThrowsAsync<PoliteHttpException>(() =>
            CrossrefIucnWorks.DownloadAsync(new PoliteHttpGetter(http, TimeSpan.Zero), store, null, CancellationToken.None));

        Assert.Null(store.LastCompletedListing());
        Assert.Equal(2, store.CountCrossrefWorks());
    }

    [Fact]
    public void Store_CrossrefWorksFor_FindsByDoiIdAndByPage() {
        using var store = MemoryStore(out var connection);
        using var _ = connection;
        var listing = store.StartListing(DateTime.UtcNow);
        store.AddListingPage(listing, CrossrefIucnWorks.ParsePage(Page1).Works, 4, 3);

        Assert.Equal("Ursus maritimus: Wiig, \u00d8., Amstrup, S. &amp; Atwood, T.", Assert.Single(store.CrossrefWorksFor(14871490)).Title);
        Assert.Single(store.CrossrefWorksFor(45033386));
        Assert.Equal("10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", Assert.Single(store.CrossrefWorksFor(121745669)).Doi);
        Assert.Empty(store.CrossrefWorksFor(1));
    }

    [Fact]
    public void Store_CacheFromBeforeTitles_GetsTheColumn_AndTheNextListingFillsIt() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var create = connection.CreateCommand()) {
            create.CommandText = """
                CREATE TABLE crossref_works (doi TEXT PRIMARY KEY, taxon_id INTEGER NOT NULL, assessment_id INTEGER NOT NULL,
                    release TEXT, language TEXT, url TEXT, url_taxon_id INTEGER, url_assessment_id INTEGER, listing_id INTEGER NOT NULL);
                INSERT INTO crossref_works VALUES ('10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en', 22823, 14871490, '2015-4', 'en', NULL, NULL, NULL, 1);
                """;
            create.ExecuteNonQuery();
        }
        Assert.False(IucnDoiCacheStore.HasCrossrefTitles(connection));
        using var store = IucnDoiCacheStore.OpenFromConnection(connection);
        Assert.True(IucnDoiCacheStore.HasCrossrefTitles(connection));
        Assert.Null(Assert.Single(store.CrossrefWorksFor(14871490)).Title);

        store.AddListingPage(store.StartListing(DateTime.UtcNow), CrossrefIucnWorks.ParsePage(Page1).Works, 4, 3);
        Assert.StartsWith("Ursus maritimus:", Assert.Single(store.CrossrefWorksFor(14871490)).Title);
    }

    [Fact]
    public void Store_SaveCheck_RoundTrips_AndReplacesAnEarlierCheck() {
        using var store = MemoryStore(out var connection);
        using var _ = connection;
        var at = new DateTime(2026, 10, 3, 1, 2, 3, DateTimeKind.Utc);
        var lookups = new[] {
            new DoiLookupLogRow(10, "10.2305/IUCN.UK.2019-3.RLTS.T1A10.en", at, 404, 100, null, DoiLookupVerdicts.NotFound),
        };
        store.SaveCheck(new DoiCheckRow(10, 1, null, at, 1), null, "global", 2019, null, lookups);

        Assert.Equal(new DoiCheckRow(10, 1, null, at, 1), store.GetCheck(10));
        Assert.Equal(lookups, store.ReadLookups(10));

        var later = at.AddDays(40);
        store.SaveCheck(new DoiCheckRow(10, 1, "10.2305/IUCN.UK.2019-1.RLTS.T1A10.en", later, 0), DoiFoundBy.Crossref, "global", 2019,
            "note", Array.Empty<DoiLookupLogRow>());

        var row = Assert.Single(store.ReadChecks()).Value;
        Assert.Equal("10.2305/IUCN.UK.2019-1.RLTS.T1A10.en", row.Doi);
        Assert.Equal(later, row.CheckedAtUtc);
        Assert.Equal(0, row.CandidatesTried);
        Assert.Equal(DoiFoundBy.Crossref, store.GetFoundBy(10));
        Assert.Single(store.ReadLookups(10));
    }

    [Fact]
    public void Store_DoiCheckTable_HasTheAgreedColumns() {
        using var store = MemoryStore(out var connection);
        using var _ = connection;
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name, type, \"notnull\", pk FROM pragma_table_info('doi_check') ORDER BY cid";
        using var reader = command.ExecuteReader();
        var columns = new List<string>();
        while (reader.Read()) {
            columns.Add($"{reader.GetString(0)} {reader.GetString(1)} {reader.GetInt32(2)} {reader.GetInt32(3)}");
        }
        Assert.Equal(new[] {
            "assessment_id INTEGER 0 1",
            "taxon_id INTEGER 1 0",
            "doi TEXT 0 0",
            "checked_at TEXT 1 0",
            "candidates_tried INTEGER 1 0",
        }, columns);
    }

    // ------------------------------------------------------------ run plan

    private static DoiTarget Target(long assessment) => new() { TaxonId = 1, AssessmentId = assessment, YearPublished = 2020, Scope = "global" };

    [Fact]
    public void Plan_SkipsCheckedAssessments_UnlessRecheck() {
        var now = new DateTime(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc);
        var targets = new[] { Target(1), Target(2), Target(3) };
        var checks = new Dictionary<long, DoiCheckRow> {
            [1] = new(1, 1, "10.2305/IUCN.UK.2020-1.RLTS.T1A1.en", now.AddDays(-1), 0),
            [2] = new(2, 1, null, now.AddDays(-40), 3),
        };

        var plain = DoiRunPlan.Make(targets, checks, recheck: false, recheckMissingAfter: null, now);
        Assert.Equal(new long[] { 3 }, plain.ToCheck.Select(t => t.AssessmentId));
        Assert.Equal(1, plain.CheckedFound);
        Assert.Equal(1, plain.CheckedNotFound);
        Assert.Equal(0, plain.NotFoundWithoutLookups);

        var withoutLookups = new Dictionary<long, DoiCheckRow>(checks) { [3] = new(3, 1, null, now, 0) };
        Assert.Equal(1, DoiRunPlan.Make(targets, withoutLookups, false, null, now).NotFoundWithoutLookups);

        var all = DoiRunPlan.Make(targets, checks, recheck: true, recheckMissingAfter: null, now);
        Assert.Equal(new long[] { 1, 2, 3 }, all.ToCheck.Select(t => t.AssessmentId));

        var missingOld = DoiRunPlan.Make(targets, checks, recheck: false, recheckMissingAfter: TimeSpan.FromDays(30), now);
        Assert.Equal(new long[] { 2, 3 }, missingOld.ToCheck.Select(t => t.AssessmentId));

        var missingRecent = DoiRunPlan.Make(targets, checks, recheck: false, recheckMissingAfter: TimeSpan.FromDays(60), now);
        Assert.Equal(new long[] { 3 }, missingRecent.ToCheck.Select(t => t.AssessmentId));
    }
}
