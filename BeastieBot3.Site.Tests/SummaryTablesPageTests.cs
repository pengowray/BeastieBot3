using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

// The assessment history's "Reason for change" column (IUCN's Table 7) and the [PE] marker for an
// assessment that IUCN's Table 9 lists as Possibly Extinct without a tag of its own.
public sealed class SummaryTablesPageTests : IClassFixture<SiteFactory> {
    private readonly HttpClient _client;

    public SummaryTablesPageTests(SiteFactory factory) => _client = factory.CreateClient();

    private async Task<string> History(long taxonId) =>
        Html.Between(await _client.GetStringAsync($"/species/{taxonId}"), "id=\"history-heading\"", "</section>");

    [Fact]
    public async Task Polar_bear_history_shows_the_reason_for_its_2008_change() {
        var history = await History(FixtureDb.PolarBear);
        var rows = Html.TableRows(history);

        Assert.Equal(new[] { "Year published", "Category", "Reason for change", "Criteria", "Date assessed", "Wikitext", "Assessment" }, rows[0]);
        var row2008 = rows.Single(r => r[0].StartsWith("2008", StringComparison.Ordinal));
        Assert.Equal("Genuine change (G)", row2008[2]);
        Assert.Equal("", rows.Single(r => r[0].StartsWith("2015", StringComparison.Ordinal))[2]);
        Assert.Contains("title=\"Genuine status change: a genuine improvement or deterioration in the species&#x27; status. Source: Table 7 of IUCN Red List version 2008.\"", history);

        var text = Html.Text(history);
        Assert.Contains("The “Reason for change” entries on this page are from Table 7 (“Species changing IUCN Red List Status”) of IUCN Red List version 2008 (PDF).", text);
        Assert.Contains("A blank cell means that Table 7 lists no reason for the assessment in that row", text);
        // The 2008 row has a reason, so the note about the genuine-only 2008 table is left out.
        Assert.DoesNotContain("genuine changes only", text);
        Assert.Contains("<a href=\"https://nc.iucnredlist.org/redlist/content/attachment_files/2008RL_Stats_Table_7.pdf\" aria-label=\"Table 7 of IUCN Red List version 2008 (PDF)\">2008</a>", history);
    }

    private static AssessmentRow Row(long id, string category, int year) =>
        new(id, 1, "Global", false, category, false, false, null, null, year, $"{year}-01-01", null, null);

    private static CategoryChangeRow Reason(long id) => new(id, "N", "VU", "EN", "2016-3", "2016-3", "https://example.org/t7.pdf");

    [Theory]
    // A 2008 row with no reason whose category differs from the one before it: its change may have
    // been non-genuine, which the genuine-only 2008 table leaves out.
    [InlineData("VU", "EN", true)]
    // Same category as before (LR/nt counts as NT): no change in 2008, so no note.
    [InlineData("NT", "LR/nt", false)]
    public void The_2008_note_is_shown_only_for_a_change_without_a_reason(string category2008, string categoryBefore, bool shown) {
        var rows = new[] { Row(3, "EN", 2016), Row(2, category2008, 2008), Row(1, categoryBefore, 2000) };

        var notes = HistoryTableNotes.Build(rows.Select(r => (r, true)).ToList(),
            new Dictionary<long, CategoryChangeRow> { [3] = Reason(3) }, _ => null, "species");

        Assert.True(notes.ShowReasonColumn);
        Assert.Equal(shown, notes.HasBlank2008Row);
    }

    [Fact]
    public async Task About_page_lists_the_summary_tables_as_a_source() {
        var text = Html.Text(await _client.GetStringAsync("/about"));

        Assert.Contains("IUCN Red List summary statistics, Tables 7 and 9 The reason for each change of Red List category (Table 7)", text);
        Assert.Contains("Table 7: version 2008; Table 9: version 2014-1", text);
        Assert.Contains("Reason for change. IUCN publishes summary statistics tables as PDFs with each Red List version.", text);
    }

    [Fact]
    public async Task A_history_with_no_reasons_has_no_reason_column() {
        var history = await History(FixtureDb.Baiji);

        Assert.DoesNotContain("Reason for change", history);
    }

    [Fact]
    public async Task An_assessment_listed_as_possibly_extinct_only_in_IUCNs_tables_gets_a_marker_and_footnote() {
        var history = await History(FixtureDb.Baiji);
        var footnoteId = $"listed-tag-{FixtureDb.Baiji2008Cr}";

        Assert.Contains($"<sup class=\"listed-tag\"><a href=\"#{footnoteId}\" aria-label=\"Footnote: IUCN&#x27;s summary tables list this species as Possibly Extinct. The assessment has no Possibly Extinct tag.\">[PE]</a></sup>", history);
        Assert.Contains($"<p class=\"note listed-tag-note\" id=\"{footnoteId}\">[PE] Assessment published in 2008: Table 9 of IUCN Red List versions 2014-1 to 2016-3 lists this species as Possibly Extinct. The assessment itself has no Possibly Extinct tag.</p>", history);
        // The latest assessment is tagged PE itself: its listing gets no marker.
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(history, "class=\"listed-tag\""));
        Assert.DoesNotContain($"listed-tag-{FixtureDb.BaijiLatest}", history);
    }
}
