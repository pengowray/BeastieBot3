using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

// The species page's list of subspecies and varieties: one row per name from IUCN, the Catalogue of
// Life and Wikidata (SubspeciesRows.Build), and the section on the page (_Subspecies).
public sealed class SubspeciesRowsTests {
    private static InfraspecificNameRow Row(string source, string id, string name, string? authority = null, string rank = "subspecies") =>
        new(source, id, rank, name, authority);

    [Fact]
    public void MergesNamesThatDifferOnlyInTheRankMarkerOrCase() {
        var list = SubspeciesRows.Build([
            Row("iucn", "15954", "Panthera pardus ssp. orientalis", "(Schlegel, 1857)"),
            Row("col", "C1", "Panthera pardus orientalis", "(Schlegel, 1857)"),
            Row("wikidata", "Q1", "Panthera Pardus  orientalis"),
        ], "ANIMALIA")!;

        var row = Assert.Single(list.Rows);
        // An animal subspecies is written without a rank marker.
        Assert.Equal("Panthera pardus orientalis", row.Name);
        Assert.Equal("(Schlegel, 1857)", row.Authority);
        // The order of the names tables: IUCN, Wikidata, the Catalogue of Life.
        Assert.Equal(["iucn", "wikidata", "col"], row.Sources.Select(s => s.Source));
        Assert.Equal("/species/15954", row.Sources[0].Records.Single().Url);
        Assert.Equal("https://www.wikidata.org/wiki/Q1", row.Sources[1].Records.Single().Url);
        Assert.Equal("https://www.catalogueoflife.org/data/taxon/C1", row.Sources[2].Records.Single().Url);
        Assert.All(row.Sources, s => Assert.Null(s.OtherAuthority));
    }

    [Fact]
    public void KeepsSubspeciesAndVarietiesApartAndWritesPlantMarkers() {
        var list = SubspeciesRows.Build([
            Row("wikidata", "Q2", "Cupressus arizonica subsp. glabra"),
            Row("wikidata", "Q3", "Cupressus arizonica var. glabra", rank: "variety"),
            Row("iucn", "34010", "Cupressus arizonica var. glabra", "(Sudw.) Little", rank: "variety"),
            Row("col", "C2", "Cupressus arizonica ssp. nevadensis"),
        ], "PLANTAE")!;

        Assert.Equal(["Cupressus arizonica subsp. glabra", "Cupressus arizonica subsp. nevadensis", "Cupressus arizonica var. glabra"],
            list.Rows.Select(r => r.Name));
        Assert.Equal(["wikidata"], list.Rows[0].Sources.Select(s => s.Source));
        Assert.Equal(["iucn", "wikidata"], list.Rows[2].Sources.Select(s => s.Source));
        Assert.True(list.HasSubspecies);
        Assert.True(list.HasVarieties);
    }

    [Fact]
    public void ShowsASourcesOwnAuthorityWhenItDiffers() {
        var row = SubspeciesRows.Build([
            Row("wikidata", "Q4", "Panthera leo melanochaita"),
            Row("col", "C3", "Panthera leo melanochaita", "(C. E. H. Smith, 1858)"),
            Row("iucn", "99", "Panthera leo ssp. melanochaita", "(Smith, 1842)"),
        ], "ANIMALIA")!.Rows.Single();

        // The first source with an authority gives the row's; another source's own differs.
        Assert.Equal("(Smith, 1842)", row.Authority);
        Assert.Null(row.Sources.Single(s => s.Source == "wikidata").OtherAuthority);
        Assert.Equal("(C. E. H. Smith, 1858)", row.Sources.Single(s => s.Source == "col").OtherAuthority);
    }

    [Fact]
    public void ListsEachRecordOfASourceWithTwoRecordsOfTheName() {
        var row = SubspeciesRows.Build([
            Row("wikidata", "Q56289810", "Panthera leo leo"),
            Row("wikidata", "Q221094", "Panthera leo leo"),
        ], "ANIMALIA")!.Rows.Single();

        var wikidata = row.Sources.Single();
        Assert.Equal(["Q221094", "Q56289810"], wikidata.Records.Select(r => r.Id));
    }

    [Fact]
    public void SortsByNameAndMarksRowsOnlyWikidataLists() {
        var list = SubspeciesRows.Build([
            Row("col", "C5", "Panthera leo melanochaita"),
            Row("wikidata", "Q5", "Panthera leo krugeri"),
            Row("col", "C6", "Panthera leo leo"),
        ], "ANIMALIA")!;

        Assert.Equal(["Panthera leo krugeri", "Panthera leo leo", "Panthera leo melanochaita"], list.Rows.Select(r => r.Name));
        Assert.True(list.HasWikidataOnlyRows);
        Assert.False(SubspeciesRows.Build([Row("col", "C6", "Panthera leo leo")], "ANIMALIA")!.HasWikidataOnlyRows);
    }

    [Fact]
    public void NoRowsIsNoList() => Assert.Null(SubspeciesRows.Build([], "ANIMALIA"));

    [Theory]
    [InlineData("Panthera leo melanochaita", "Panthera", "leo", "melanochaita")]
    [InlineData("Panthera pardus ssp. orientalis", "Panthera", "pardus", "orientalis")]
    [InlineData("Abies alba var. acutifolia", "Abies", "alba", "acutifolia")]
    [InlineData("Abies alba subsp. apennina", "Abies", "alba", "apennina")]
    public void SplitReadsGenusSpeciesAndInfraspecificEpithet(string name, string genus, string species, string infra) =>
        Assert.Equal((genus, species, infra), InfraspecificNames.Split(name));

    [Theory]
    [InlineData("Panthera leo")]
    [InlineData("Panthera leo leo leo")]
    [InlineData("Rosa × alba var. x")]
    [InlineData("Panthera leo Barbary")]
    [InlineData("panthera leo leo")]
    public void SplitLeavesOutOtherNames(string name) => Assert.Null(InfraspecificNames.Split(name));
}

public sealed class SubspeciesSectionTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task LionPageListsTheSubspeciesFromEverySource() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        var section = Section(html);
        var text = Html.Text(section);

        Assert.Contains("<h2 id=\"subspecies-list-heading\">Subspecies</h2>", section);
        Assert.Contains("Sources often disagree about which subspecies this species has. "
            + "A name listed only by Wikidata may be an older name that current classifications treat as a synonym.", text);
        Assert.Contains("<th scope=\"row\"><i>Panthera leo melanochaita</i> <span class=\"authority\">(C. E. H. Smith, 1858)</span></th>", section);
        Assert.Contains("<a href=\"https://www.wikidata.org/wiki/Q20907143\">Wikidata</a>, <a href=\"https://www.catalogueoflife.org/data/taxon/7KGW9\">Catalogue of Life</a>", section);
        // Two Wikidata items with one name: each linked by its id.
        Assert.Contains("Wikidata (<a href=\"https://www.wikidata.org/wiki/Q221094\">Q221094</a>, <a href=\"https://www.wikidata.org/wiki/Q56289810\">Q56289810</a>), "
            + "<a href=\"https://www.catalogueoflife.org/data/taxon/5K5L8\">Catalogue of Life</a>", section);
        Assert.Equal(["Panthera leo krugeri", "Panthera leo leo", "Panthera leo melanochaita"],
            Regex.Matches(section, "<th scope=\"row\"><i>([^<]+)</i>").Select(m => m.Groups[1].Value));
        // The subpopulation is not a subspecies, and three rows need no "Show all" box.
        Assert.DoesNotContain("West Africa", text);
        Assert.DoesNotContain("Show all", text);
    }

    [Fact]
    public async Task SectionComesAfterTheNames() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.True(html.IndexOf(">Names</h2>", StringComparison.Ordinal) < html.IndexOf("id=\"subspecies-list-heading\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TigerPageLinksIucnSubspeciesToItsPage() {
        var section = Section(await _client.GetStringAsync($"/species/{FixtureDb.Tiger}"));
        Assert.Contains("<th scope=\"row\"><i>Panthera tigris sumatrae</i> <span class=\"authority\">Pocock, 1929</span></th>", section);
        Assert.Contains($"<a href=\"/species/{FixtureDb.SumatranTiger}\">IUCN Red List</a>", section);
        // Its only source is IUCN, so the line under the heading has one sentence.
        Assert.DoesNotContain("Wikidata", Html.Text(section));
    }

    [Fact]
    public async Task RowsAfterTheFirstTenAreBehindShowAll() {
        var section = Section(await _client.GetStringAsync($"/species/{FixtureDb.Leopard}"));
        Assert.Contains("Show all subspecies (1 more)", Html.Text(section));
        Assert.Single(Regex.Matches(section, "<tr class=\"more-item\">"));
        Assert.Contains("<tr class=\"more-item\">\n                            <th scope=\"row\"><i>Panthera pardus tulliana</i>", section);
        // The Amur leopard's IUCN taxon is not in the release, so orientalis is listed by CoL alone.
        Assert.DoesNotContain($"/species/{FixtureDb.AmurLeopard}\"", section);
    }

    [Fact]
    public async Task SubspeciesPagesAndSpeciesWithNoneHaveNoSection() {
        Assert.DoesNotContain("subspecies-list-heading", await _client.GetStringAsync($"/species/{FixtureDb.SumatranTiger}"));
        Assert.DoesNotContain("subspecies-list-heading", await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}"));
    }

    private static string Section(string html) {
        var start = html.IndexOf("<section class=\"subspecies-list\"", StringComparison.Ordinal);
        Assert.True(start >= 0, "the page should have the subspecies section");
        var end = html.IndexOf("</section>", start, StringComparison.Ordinal);
        return html[start..end];
    }
}
