using BeastieBot3.SiteBuild;
using BeastieBot3.Wikidata;

namespace BeastieBot3.Tests.SiteBuild;

// Pins which cached Wikidata items stand for an assessment, the property list the site judges an
// add batch by, and how an assessment finds its item.
public class SiteWikidataItemsTests {
    private static readonly DateTime Fetched = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    private static WikidataAssessmentItemRow Row(string qid, long taxonId, long assessmentId, params string[] classes) => new() {
        Qid = qid, TaxonId = taxonId, AssessmentId = assessmentId, InstanceOf = classes, FetchedAtUtc = Fetched,
    };

    [Theory]
    [InlineData(true, "Q13442814")]
    [InlineData(true, "Q1172284")]
    [InlineData(true, "Q1379672")]
    [InlineData(true, "Q13442814", "Q1172284")]
    [InlineData(false, "Q16521", "Q55808")]
    [InlineData(false, "Q16521", "Q13442814")]
    [InlineData(false, "Q5")]
    [InlineData(false)]
    public void IsPublication(bool expected, params string[] classes) {
        Assert.Equal(expected, SiteWikidataItems.IsPublication(classes));
    }

    [Fact]
    public void Properties_InTheJudgedOrder() {
        var row = Row("Q1", 1, 2, "Q13442814") with {
            Title = "x", LabelEn = "x", PublishedIn = ["Q32059"], MainSubjects = ["Q5"], Urls = ["https://example.org"],
            PublicationDate = "2014-01-01T00:00:00Z", AllDois = ["10.2305/X"], AuthorStringCount = 2, AuthorItemCount = 1,
        };
        Assert.Equal(new[] { "P31", "P1476", "P1433", "P921", "P953", "P577", "P356", "P2093", "P50", "Len" }, SiteWikidataItems.Properties(row));
        Assert.Equal(new[] { "P31" }, SiteWikidataItems.Properties(Row("Q1", 1, 2, "Q13442814")));
    }

    [Fact]
    public void Find_OwnItem_ThenTheItemOfTheAssessmentTheDoiNames() {
        var items = new SiteWikidataItems();
        items.Add(Row("Q100", 712, 45033386, "Q13442814"));
        items.Add(Row("Q200", 712, 45033386, "Q13442814"));   // a second item for the same assessment
        items.Add(Row("Q300", 999, 5, "Q13442814"));

        Assert.Equal(("Q100", false), Pick(items.Find(712, 45033386, null)));
        // The giant panda's errata version cites the DOI of the assessment it corrects.
        Assert.Equal(("Q100", true), Pick(items.Find(712, 121745669, "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en")));
        Assert.Null(items.Find(712, 121745669, null));
        // An item whose taxon id is another taxon's is not used.
        Assert.Null(items.Find(1, 5, null));
        Assert.Null(items.Find(1, 121745669, "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en"));
        Assert.Equal(1, items.SecondItemForAnAssessment);
    }

    private static (string, bool)? Pick((SiteWikidataItem Item, bool ThroughDoi)? found) =>
        found is { } f ? (f.Item.Qid, f.ThroughDoi) : null;
}
