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
        Assert.Equal("Genuine status change (G)1", rows.Single(r => r[0].StartsWith("2008", StringComparison.Ordinal))[2]);
        Assert.Equal("no change", rows.Single(r => r[0].StartsWith("2015", StringComparison.Ordinal))[2]);
        Assert.Equal("\u2014", rows.Single(r => r[0].StartsWith("1996", StringComparison.Ordinal))[2]);
        Assert.Contains("<sup class=\"fn-ref\"><a href=\"#history-fn-1\" aria-label=\"Footnote 1\">1</a></sup>", history);
        Assert.DoesNotContain("title=", history);

        Assert.Contains("<li id=\"history-fn-1\" value=\"1\">From <a href=\"https://nc.iucnredlist.org/redlist/content/attachment_files/2008RL_Stats_Table_7.pdf\">"
            + "Table 7 (\u201cSpecies changing IUCN Red List Status\u201d) of IUCN Red List version 2008</a> (PDF). The 2008 table lists genuine changes only.</li>", history);
        Assert.Contains("\u2014 Published before 2007, the year of IUCN's first Table 7.", Html.Text(history));
    }

    private static AssessmentRow Row(long id, string category, int year) =>
        new(id, 1, "Global", false, category, false, false, null, null, year, $"{year}-01-01", null, null);

    private static CategoryChangeRow Reason(long id) => new(id, "N", "VU", "EN", "2016-3", "2016-3", "https://example.org/t7.pdf");

    [Theory]
    // A 2008 row with no reason whose category differs from the one before it: its change may have
    // been non-genuine, which the genuine-only 2008 table leaves out.
    [InlineData("VU", "EN", true)]
    // Same category as before (LR/nt counts as NT): no change.
    [InlineData("NT", "LR/nt", false)]
    public void A_2008_change_without_a_reason_is_no_reason_given(string category2008, string categoryBefore, bool shown) {
        var rows = new[] { Row(3, "EN", 2016), Row(2, category2008, 2008), Row(1, categoryBefore, 2000) };

        var notes = HistoryTableNotes.Build(rows.Select(r => (r, true)).ToList(),
            new Dictionary<long, CategoryChangeRow> { [3] = Reason(3) }, _ => null, "species", "https://example.org/2008.pdf");

        Assert.True(notes.ShowReasonColumn);
        Assert.Equal(shown ? ReasonCellKind.NoneGiven : ReasonCellKind.NoChange, notes.CellFor(2)!.Kind);
        Assert.Equal(ReasonCellKind.BeforeTables, notes.CellFor(1)!.Kind);
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
    public async Task An_assessment_listed_as_possibly_extinct_only_in_IUCNs_tables_shows_CR_PE_with_a_footnote() {
        var history = await History(FixtureDb.Baiji);
        var rows = Html.TableRows(history);

        Assert.Equal("CR (PE) Critically Endangered (Possibly Extinct)1", rows.Single(r => r[0].StartsWith("2008", StringComparison.Ordinal))[1]);
        Assert.Contains("<li id=\"history-fn-1\" value=\"1\">Table 9 of IUCN Red List versions 2014-1 to 2016-3 lists this species as Possibly Extinct. "
            + "The assessment itself is not flagged as Possibly Extinct.</li>", history);
        // The latest assessment is tagged PE itself: no footnote.
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(history, "class=\"fn-ref\""));
    }

    [Fact]
    public void Cells_say_why_there_is_no_reason() {
        AssessmentRow R(long id, string category, int year) => Row(id, category, year);
        var rows = new[] { R(6, "CR", 2020), R(5, "EN", 2016), R(4, "EN", 2012), R(3, "VU", 2010), R(2, "VU", 2008) };

        var notes = HistoryTableNotes.Build(rows.Select(r => (r, true)).ToList(),
            new Dictionary<long, CategoryChangeRow> { [6] = Reason(6) }, _ => null, "species", null);

        Assert.Equal(ReasonCellKind.Reason, notes.CellFor(6)!.Kind);
        Assert.Equal(ReasonCellKind.NoChange, notes.CellFor(5)!.Kind);
        Assert.Equal(ReasonCellKind.NotFound, notes.CellFor(4)!.Kind);
        Assert.Equal(ReasonCellKind.NoChange, notes.CellFor(3)!.Kind);
        Assert.Equal(ReasonCellKind.FirstAssessment, notes.CellFor(2)!.Kind);
        Assert.Equal(1, notes.CellFor(6)!.Footnote);
        Assert.False(notes.HasBeforeTables);
    }
}
