using BeastieBot3.Iucn.SummaryTables;
using BeastieBot3.SiteBuild;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The values read from IUCN's summary tables, the list of table files, the store, and the linking
// of table rows to the site's assessments.
public sealed class SummaryTableLinkTests {
    [Theory]
    [InlineData("CR (PE)", "CR", "PE")]
    [InlineData("CR(PEW)", "CR", "PEW")]
    [InlineData("V U", "VU", null)]
    [InlineData("LR/nt", "LR/nt", null)]
    [InlineData("lr/CD", "LR/cd", null)]
    [InlineData("VY", null, null)]
    [InlineData("DD(PE)", null, null)]
    [InlineData("0 NT", null, null)]
    public void Categories(string text, string? category, string? tag) =>
        Assert.Equal(new TableCategory(category, tag), SummaryTableValues.ReadCategory(text));

    [Theory]
    [InlineData("G", "G")]
    [InlineData("N", "N")]
    [InlineData("E", "E")]
    [InlineData("Non-genuien", "N")]
    [InlineData("synonym of A. nigriceps", null)]
    [InlineData("0", null)]
    public void Reasons(string text, string? reason) => Assert.Equal(reason, SummaryTableValues.ReadReason(text));

    [Theory]
    [InlineData("2019‐3", "2019-3")]
    [InlineData("2019 ‐3", "2019-3")]
    [InlineData("2010.4", "2010-4")]
    [InlineData("2013..1", "2013-1")]
    [InlineData("2007", "2007")]
    [InlineData("hybrid", null)]
    public void Versions(string text, string? version) => Assert.Equal(version, SummaryTableValues.ReadVersion(text));

    [Theory]
    [InlineData("MAMMALS", "ANIMALIA")]
    [InlineData("MOLLUSCS - Gastropods", "ANIMALIA")]
    [InlineData("PLANTS", "PLANTAE")]
    [InlineData("FERNS & ALLIES (Polypodiopsida, Marattiopsida, Equisetopsida)", null)]
    [InlineData("FUNGI & PROTISTS", null)]
    [InlineData("FUNGI", "FUNGI")]
    public void Kingdom_of_a_group(string group, string? kingdom) => Assert.Equal(kingdom, SummaryTableValues.KingdomOfGroup(group));

    [Fact]
    public void The_list_of_tables_loads_and_names_each_file_once() {
        var files = SummaryTableManifest.Load(Path.Combine(AppContext.BaseDirectory, "rules", SummaryTableManifest.FileName));

        Assert.Contains(files, f => f.Table == 7 && f.Release == "2024-2" && f.FileName == "2024-2_RL_Table_7.pdf");
        Assert.Contains(files, f => f.Table == 9 && f.Release == "2020-2");
        Assert.Equal(files.Count, files.Select(f => f.Priority).Distinct().Count());
        // A corrected table comes after the one it corrects, so it wins.
        var corrected = files.Single(f => f.FileName == "2024-1_RL_Table_7_corrected_20240916.pdf");
        Assert.True(corrected.Priority > files.Single(f => f.FileName == "2024-1_RL_Table_7.pdf").Priority);
    }

    [Fact]
    public void The_next_releases_and_their_file_names() {
        Assert.Equal(new[] { "2026-2", "2027-1" }, SummaryTableManifest.NextReleases("2026-1"));
        Assert.Contains("https://nc.iucnredlist.org/redlist/content/attachment_files/2026-2_RL_Table7.pdf",
            SummaryTableManifest.Table7Urls("2026-2"));
    }

    [Fact]
    public void The_store_replaces_a_files_rows_and_reads_them_back() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = SummaryTableStore.OpenFromConnection(connection);
        var facts = new SummaryTableStore.FileFacts("t7.pdf", 7, "2024-2", "https://example.org/t7.pdf", "abc", 10, null, 1, 3, null, 2);
        var info = new SummaryTableInfo("Table 7", "2024-2", "28 October 2024", "2023-1", "2024-2", true);
        CategoryChangeRow Row(string name) => new(1, 1, "MAMMALS", null, name, null, "EN", "CR", "G", "2024-2", null);

        store.ReplaceTable7(facts, new SummaryTableParse<CategoryChangeRow>(info, new[] { Row("Bos javanicus"), Row("Capra walie") }, [], true));
        store.ReplaceTable7(facts, new SummaryTableParse<CategoryChangeRow>(info, new[] { Row("Bos javanicus") }, [], true));

        var stored = Assert.Single(store.ReadCategoryChanges());
        Assert.Equal(("Bos javanicus", "2024-2", 3), (stored.Row.ScientificName, stored.Source.Release, stored.Source.Priority));
        Assert.Equal(1, store.GetSources()["t7.pdf"].RowCount);
    }

    // ------------------------------------------------------------ linking

    private static SiteHistoryEntry A(long id, string category, int year, bool pe = false) => new(id, category, pe, false, year, $"{year}-01-01");

    [Fact]
    public void A_change_goes_on_the_first_assessment_of_the_new_category_in_the_versions_year() {
        var history = new[] { A(1, "VU", 2008), A(2, "EN", 2016), A(3, "EN", 2016), A(4, "EN", 2020) };

        Assert.Equal((1, false), SiteSummaryTables.FindChange(history, "EN", 2016));
    }

    [Fact]
    public void An_assessment_published_again_the_year_after_takes_the_change() {
        // Banteng: Table 7 for 2024-2 lists EN to CR; the assessment on the site is the amended one of 2025.
        var history = new[] { A(1, "EN", 2008), A(2, "CR", 2025) };

        Assert.Equal((1, true), SiteSummaryTables.FindChange(history, "CR", 2024));
    }

    [Fact]
    public void LR_nt_counts_as_NT() {
        var history = new[] { A(1, "LR/nt", 2000), A(2, "VU", 2008) };

        Assert.Equal((1, false), SiteSummaryTables.FindChange(history, "VU", 2008));
        Assert.Equal("NT", SiteSummaryTables.Family("LR/nt"));
    }

    private static SummaryTableSource Source(long id, int table, string release, int priority) =>
        new(id, $"{release}_{table}.pdf", table, release, "https://example.org", "sha", 1, null, "", 1, priority, null, null, null,
            null, null, true, 1, 1, 0, 0);

    private static SiteTaxon Taxon(long id, string name) =>
        new() { TaxonId = id, ScientificName = name, Kind = SiteTaxonKind.Species, Kingdom = "ANIMALIA" };

    [Fact]
    public void Linking_keeps_the_latest_tables_reason_and_lists_possibly_extinct_assessments() {
        var earlier = Source(1, 7, "2016-1", 1);
        var later = Source(2, 7, "2016-3", 2);
        var table9 = Source(3, 9, "2017-1", 3);
        var changes = new[] {
            new StoredCategoryChange(earlier, new CategoryChangeRow(1, 1, "MAMMALS", null, "Bos sauveli", null, "EN", "CR (PE)", "N", "2016-1", null)),
            new StoredCategoryChange(later, new CategoryChangeRow(1, 1, "MAMMALS", null, "Bos sauveli", null, "EN", "CR(PE)", "G", "2016-1", null)),
            new StoredCategoryChange(later, new CategoryChangeRow(1, 2, "MAMMALS", null, "Nobody here", null, "EN", "CR", "G", "2016-3", null)),
        };
        var listings = new[] {
            new StoredPossiblyExtinct(table9, new PossiblyExtinctRow(1, 1, "MAMMALS", "Bos sauveli", "Kouprey", "CR(PE)", "2016", "1969/70", null)),
        };
        var history = new Dictionary<long, List<SiteHistoryEntry>> {
            [10] = new() { A(100, "EN", 2008), A(101, "CR", 2016) },
        };

        var result = SiteSummaryTables.Link(new[] { earlier, later, table9 }, changes, listings,
            new StatusListNameIndex(new[] { Taxon(10, "Bos sauveli") }), history);

        var change = Assert.Single(result.Changes);
        Assert.Equal((101L, "G", 100L, "EN", "CR(PE)"), (change.AssessmentId, change.Reason, change.PreviousAssessmentId,
            change.OldCategory, change.NewCategory));
        Assert.Equal(1, result.ReasonConflicts);
        Assert.Equal(1, result.ChangeRowsNoTaxon);
        var listing = Assert.Single(result.Listings);
        Assert.Equal((101L, "PE", "2016-1", "2017-1", "7 9"), (listing.AssessmentId, listing.Tag, listing.FirstRelease,
            listing.LastRelease, listing.Tables));
        Assert.Equal(1, result.ListedWithoutTag);
    }
}
