using System.Text.RegularExpressions;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

// Subspecies from the Mammal Diversity Database and the Reptile Database (infraspecific_name sources
// 'mdd' and 'reptiledb', from the checklists store): after IUCN, Wikidata and the Catalogue of Life, each
// linked to the species' page on the source's site, which lists its subspecies.
public sealed class ChecklistSubspeciesRowsTests {
    [Fact]
    public void ChecklistSourcesComeAfterTheOthersAndLinkTheSpeciesPage() {
        var row = SubspeciesRows.Build([
            new InfraspecificNameRow("reptiledb", "genus=Python&species=regius", "subspecies", "Python regius regius", "(Shaw, 1802)"),
            new InfraspecificNameRow("mdd", "1006023", "subspecies", "Python regius regius", "(Shaw, 1802)"),
            new InfraspecificNameRow("col", "C1", "subspecies", "Python regius regius", null),
            new InfraspecificNameRow("wikidata", "Q1", "subspecies", "Python regius regius", null),
            new InfraspecificNameRow("iucn", "7", "subspecies", "Python regius ssp. regius", null),
        ], "ANIMALIA")!.Rows.Single();

        Assert.Equal(["iucn", "wikidata", "col", "mdd", "reptiledb"], row.Sources.Select(s => s.Source));
        Assert.Equal(["Mammal Diversity Database", "The Reptile Database"], row.Sources.Skip(3).Select(s => s.Label));
        Assert.Equal("https://www.mammaldiversity.org/taxon/1006023/", row.Sources[3].Records.Single().Url);
        Assert.Equal("https://reptile-database.reptarium.cz/species?genus=Python&species=regius", row.Sources[4].Records.Single().Url);
        Assert.Equal("(Shaw, 1802)", row.Authority);
    }
}

public sealed class ChecklistSubspeciesSectionTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    [Fact]
    public async Task TigerPageListsTheMammalDiversityDatabasesSubspecies() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var start = html.IndexOf("<section class=\"subspecies-list\"", StringComparison.Ordinal);
        Assert.True(start >= 0, "the page should have the subspecies section");
        var section = html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];

        Assert.Contains("<th scope=\"row\"><i>Panthera tigris sondaica</i> <span class=\"authority\">(Temminck, 1844)</span></th>", section);
        Assert.Contains("<a href=\"https://www.mammaldiversity.org/taxon/1006023/\">Mammal Diversity Database</a>", section);
        // IUCN's sumatrae, and the two MDD subspecies, by name.
        Assert.Equal(["Panthera tigris sondaica", "Panthera tigris sumatrae", "Panthera tigris tigris"],
            Regex.Matches(section, "<th scope=\"row\"><i>([^<]+)</i>").Select(m => m.Groups[1].Value));
    }
}
