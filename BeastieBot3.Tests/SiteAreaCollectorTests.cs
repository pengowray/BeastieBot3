using System.Linq;
using System.Text.Json;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests;

// The countries and areas of the site database (taxon_area): codings merged, and endemism to a part
// of a country derived from the country's flag.
public class SiteAreaCollectorTests {
    private static string Location(string code, string name, string origin = "Native", string presence = "Extant", bool endemic = false) =>
        $$"""{"code":"{{code}}","description":{"en":"{{name}}"},"origin":"{{origin}}","presence":"{{presence}}","is_endemic":{{(endemic ? "true" : "false")}}}""";

    private static void Add(SiteAreaCollector areas, long taxonId, params string[] locations) {
        using var doc = JsonDocument.Parse($$"""{"locations":[{{string.Join(",", locations)}}]}""");
        areas.Add(taxonId, doc.RootElement);
    }

    [Fact]
    public void AnEndemicTaxonRecordedInOnePartOfItsCountryIsEndemicToThatPart() {
        var areas = new SiteAreaCollector();
        // Tasmanian devil: Australia (endemic) and Tasmania.
        Add(areas, 1, Location("AU", "Australia", endemic: true), Location("TAS-OO", "Tasmania"));
        // Koala: Australia (endemic) and four states.
        Add(areas, 2, Location("AU", "Australia", endemic: true), Location("QLD-QU", "Queensland"), Location("NSW-NS", "New South Wales"));
        // Not endemic to the country.
        Add(areas, 3, Location("AU", "Australia"), Location("TAS-OO", "Tasmania"), Location("NZ", "New Zealand"));
        var rows = areas.TaxonAreaRows().ToDictionary(r => ((string)r[0]!, (long)r[1]!), r => (int)r[4]!);
        Assert.Equal(1, rows[("TAS-OO", 1)]);
        Assert.Equal(0, rows[("QLD-QU", 2)]);
        Assert.Equal(1, rows[("AU", 2)]);
        Assert.Equal(0, rows[("TAS-OO", 3)]);
        Assert.Equal("AU", areas.AreaRows().Single(r => (string)r[0]! == "TAS-OO")[2]);
    }

    [Fact]
    public void AnAreaCodedTwiceKeepsTheStrongerOriginAndPresenceAndHashRegionsAreLeftOut() {
        var areas = new SiteAreaCollector();
        Add(areas, 1, Location("US", "United States", "Vagrant", "Possibly Extinct"), Location("US", "United States", "Native", "Extant"),
            Location("912D59CDF1D3F551FAE21F6F062258F", "Europe"));
        var row = Assert.Single(areas.TaxonAreaRows());
        Assert.Equal(new object?[] { "US", 1L, 1, 1, 0 }, row);
    }
}
