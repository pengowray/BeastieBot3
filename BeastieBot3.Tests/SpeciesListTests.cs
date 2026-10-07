using System;
using System.Linq;
using System.Text.Json;
using BeastieBot3.Site.Update;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins `wikipedia fetch-species-lists` (which templates and categories are read again, which pages
// are downloaded) and the parts of `wikipedia report-species-lists` that sort a status change and
// choose the group articles worth checking.
public class SpeciesListTests {
    private static readonly DateTime Now = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("EN", "CR", StatusItemKind.StatusTemplate, "Category")]
    [InlineData(null, "CR", StatusItemKind.TableCell, "Category")]
    [InlineData("LR/nt", "VU", StatusItemKind.ListLine, "Category")]
    [InlineData("CR", "CR(PE)", StatusItemKind.StatusTemplate, "PossiblyExtinct")]
    [InlineData("CR(PE)", "CR(PEW)", StatusItemKind.StatusTemplate, "PossiblyExtinct")]
    [InlineData("CR(PE)", "CR", StatusItemKind.StatusTemplate, "PossiblyExtinct")]
    [InlineData("LC", "LC", StatusItemKind.SpeciesTableRow, "Trend")]
    [InlineData("NA", "LC", StatusItemKind.StatusTemplate, "RegionalCode")]
    [InlineData("RE", "EN", StatusItemKind.TableCell, "RegionalCode")]
    [InlineData(" lc ", "LC", StatusItemKind.StatusTemplate, "Assessment")]
    [InlineData("LC", "LR/lc", StatusItemKind.StatusTemplate, "Assessment")]
    [InlineData("NT", "LR/cd", StatusItemKind.StatusTemplate, "Assessment")]
    public void Classify_sorts_a_change_by_what_it_changes(string? written, string iucn, StatusItemKind kind, string expected) =>
        Assert.Equal(expected, SpeciesListSurvey.Classify(written, iucn, kind).ToString());

    [Theory]
    [InlineData("Text {{IUCN status|EN}} here", true)]
    [InlineData("== Species ==\n{{Species table|genus=Felis}}", true)]
    [InlineData("Intro\n{| class=\"wikitable\"\n|}", true)]
    [InlineData("* ''Acer one''\n* ''Acer two''\n* ''Acer three''", true)]
    [InlineData("* ''Acer one''\n* ''Acer two''\n* Acer three", false)]
    [InlineData("A genus of maples. See ''Acer''.", false)]
    public void LooksLikeList_picks_group_articles_with_a_list(string wikitext, bool expected) =>
        Assert.Equal(expected, WikipediaReportSpeciesListsCommand.LooksLikeList(wikitext));

    [Fact]
    public void ReadListedPages_reads_titles_and_namespaces() {
        using var doc = JsonDocument.Parse("""
            {"query":{"categorymembers":[{"ns":0,"title":"List of mammals of Peru"},{"ns":14,"title":"Category:Lists of birds"}]}}
            """);
        var pages = WikipediaApiClient.ReadListedPages(doc.RootElement, "categorymembers").ToList();
        Assert.Equal([new WikipediaListedPage("List of mammals of Peru", 0), new WikipediaListedPage("Category:Lists of birds", 14)], pages);
    }

    [Fact]
    public void A_category_adds_its_subcategories_one_level_down_and_is_not_read_again_until_it_is_old() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = WikipediaCacheStore.OpenFromConnection(connection);
        store.AddSpeciesListSource("Template:IUCN status", SpeciesListSourceKinds.Template, 0);
        store.AddSpeciesListSource("Category:Lists of animals", SpeciesListSourceKinds.Category, 0);

        var due = WikipediaFetchSpeciesListsCommand.DueSources(store, Now.AddDays(-30), maxDepth: 1);
        Assert.Equal(2, due.Count);

        var category = due.Single(s => s.Kind == SpeciesListSourceKinds.Category);
        store.SaveSpeciesListFind(category, ["List of mammals of Peru"], ["Category:Lists of mammals"], Now);
        var template = due.Single(s => s.Kind == SpeciesListSourceKinds.Template);
        store.SaveSpeciesListFind(template, ["Felis", "List of mammals of Peru"], [], Now);

        var next = WikipediaFetchSpeciesListsCommand.DueSources(store, Now.AddDays(-30), maxDepth: 1);
        Assert.Equal(["Category:Lists of mammals"], next.Select(s => s.Name));
        Assert.Equal(1, next[0].Depth);
        Assert.Empty(WikipediaFetchSpeciesListsCommand.DueSources(store, Now.AddDays(-30), maxDepth: 0));

        // Once the cutoff is after the time they were read, the two starting sources are due again,
        // and are read before the subcategory below them.
        Assert.Equal(["Category:Lists of animals", "Template:IUCN status"],
            WikipediaFetchSpeciesListsCommand.DueSources(store, Now.AddDays(1), maxDepth: 1).Select(s => s.Name).Order());

        var pages = store.GetSpeciesListPages();
        Assert.Equal(["Felis", "List of mammals of Peru"], pages.Select(p => p.Title));
        Assert.Equal(["Category:Lists of animals", "Template:IUCN status"], pages[1].Sources.Order());
    }

    [Fact]
    public void Pages_to_download_are_the_ones_not_downloaded_or_older_than_the_refresh_date() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = WikipediaCacheStore.OpenFromConnection(connection);
        store.AddSpeciesListSource("Template:IUCN status", SpeciesListSourceKinds.Template, 0);
        var source = store.GetSpeciesListSources().Single();
        store.SaveSpeciesListFind(source, ["New page", "Old page", "Recent page", "Failed page", "Gone page"], [], Now);
        AddPage(connection, "Old page", "cached", Now.AddDays(-100));
        AddPage(connection, "Recent page", "cached", Now.AddDays(-1));
        AddPage(connection, "Failed page", "failed", null);
        AddPage(connection, "Gone page", "missing", null);

        Assert.Equal(["Failed page", "New page"], WikipediaFetchSpeciesListsCommand.PagesToDownload(store, refreshBefore: null).Order());
        Assert.Equal(["Failed page", "New page", "Old page"], WikipediaFetchSpeciesListsCommand.PagesToDownload(store, Now.AddDays(-30)).Order());
    }

    private static void AddPage(SqliteConnection connection, string title, string status, DateTime? downloaded) {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO wiki_pages(page_title, normalized_title, discovered_at, last_seen_at, download_status, downloaded_at)
            VALUES (@t, @t, @d, @d, @s, @dl)
            """;
        cmd.Parameters.AddWithValue("@t", title);
        cmd.Parameters.AddWithValue("@d", Now.ToString("O"));
        cmd.Parameters.AddWithValue("@s", status);
        cmd.Parameters.AddWithValue("@dl", downloaded?.ToString("O") ?? (object)DBNull.Value);
        cmd.ExecuteNonQuery();
    }
}
