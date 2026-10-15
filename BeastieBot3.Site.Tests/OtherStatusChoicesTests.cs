using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Tests;

// The lists other than the IUCN Red List that the status update page and the species list maker can
// show beside IUCN statuses: their choice, the default for a page about a country, and the text of a
// status in a report cell and at the end of a list line.
public sealed class OtherStatusChoicesTests {
    private static OtherStatusListCount List(string system, string? listKey = null, string? listCountry = null, string? name = null,
        int taxa = 10, string? area = null, string? country = null, int order = 0) =>
        new(system, listKey, country, listCountry, name, order, taxa, area);

    private static readonly OtherStatusChoices Choices = new([
        List("national-red-list", "redlist:se-redlist-2025", "SE", "The Swedish Red List 2025"),
        List("gb-jncc", "jncc-redlist-gb", "GB", "Great Britain Red List", area: "Great Britain", order: 1),
        List("gb-jncc", "jncc-bocc", "GB", "Birds of Conservation Concern 5", order: 3),
        List("au-vic"),
        List("au-epbc"),
        List("cites"),
        List("natureserve-global"),
        List("natureserve-national", country: "US"),
        List("national-red-list", "redlist:ch-plants-2016", "CH", "Red List of the vascular plants of Switzerland (2016)"),
        List("national-red-list", "redlist:ch-butterflies-2014", "CH", "Red List of the butterflies and burnet moths of Switzerland (2014)"),
    ]);

    [Fact]
    public void Lists_are_grouped_as_on_the_species_page() {
        Assert.Equal(["international", "AU", "SE", "CH", "GB", "US", "natureserve"], Choices.Groups.Select(g => g.Group));
        Assert.Equal(["au-epbc", "au-vic"], Choices.Groups[1].Choices.Select(c => c.Key));
        Assert.Equal(["jncc-redlist-gb", "jncc-bocc"], Choices.Groups[4].Choices.Select(c => c.Key));
        Assert.Equal("natureserve-national:US", Choices.Groups[5].Choices[0].Key);
    }

    [Fact]
    public void A_list_whose_name_does_not_say_its_country_is_headed_with_it() {
        Assert.Equal(("Victoria", "Victoria (Australia)"), (Choices.Find("au-vic")!.Name, Choices.Find("au-vic")!.Heading));
        Assert.Equal("EPBC Act", Choices.Find("au-epbc")!.Heading);
        Assert.Equal("NatureServe national rank (United States)", Choices.Find("natureserve-national:US")!.Heading);
        Assert.Equal("Great Britain Red List", Choices.Find("jncc-redlist-gb")!.Heading);
        Assert.Null(Choices.Find("natureserve-subnational"));
        Assert.Null(Choices.Find(""));
    }

    [Fact]
    public void A_page_about_a_country_gets_its_main_list_when_the_list_has_its_taxa() {
        Assert.Equal("jncc-redlist-gb", Choices.DefaultFor("GB", _ => 3)?.Key);
        Assert.Null(Choices.DefaultFor("GB", _ => 0));
        Assert.Equal("au-epbc", Choices.DefaultFor("AU", c => c.Key == "au-epbc" ? 1 : 50)?.Key);
        Assert.Null(Choices.DefaultFor("DE", _ => 3));
        Assert.Null(Choices.DefaultFor(null, _ => 3));
    }

    [Fact]
    public void A_country_with_several_national_red_lists_gets_the_one_with_most_of_the_pages_taxa() {
        Assert.Equal("redlist:ch-butterflies-2014", Choices.DefaultFor("CH", c => c.Key == "redlist:ch-butterflies-2014" ? 4 : 1)?.Key);
        Assert.Null(Choices.DefaultFor("CH", _ => 0));
    }

    [Fact]
    public void A_cell_leaves_out_the_area_the_whole_list_is_of() {
        var gb = Choices.Find("jncc-redlist-gb")!;
        Assert.Equal("Vulnerable (breeding); Least Concern (non-breeding)", gb.Cell([
            new ChosenStatus(1, "Vulnerable (breeding)", null, "Great Britain", "x"),
            new ChosenStatus(1, "Least Concern (non-breeding)", null, "Great Britain", "y"),
        ]));
        var epbc = Choices.Find("au-epbc")!;
        Assert.Equal("Endangered (Western Australian population)",
            epbc.Cell([new ChosenStatus(1, "Endangered", null, "Western Australian population", "89857")]));
        Assert.Equal(string.Empty, epbc.Cell(null));
    }

    [Theory]
    [InlineData("au-epbc", "Critically Endangered", "{{EPBC status|CR}}")]
    [InlineData("au-epbc", "Extinct in the wild", "{{EPBC status|EW}}")]
    [InlineData("au-epbc", "Conservation Dependent", "{{EPBC status|CD}}")]
    [InlineData("us-esa", "Threatened", "{{ESA status|T}}")]
    [InlineData("us-esa", "Experimental Population, Non-Essential", "{{ESA status|XN}}")]
    [InlineData("us-esa", "Similarity of Appearance (Threatened)", "{{ESA status|T(S/A)}}")]
    [InlineData("us-esa", "Under Review", null)]
    [InlineData("au-vic", "Endangered", null)]
    public void The_EPBC_Act_and_the_ESA_have_templates(string system, string status, string? template) =>
        Assert.Equal(template, OtherStatusLineEnd.Template(system, status));

    [Fact]
    public void Line_ends_use_the_template_or_the_text() {
        var epbc = Choices.Find("au-epbc")!;
        var gb = Choices.Find("jncc-redlist-gb")!;
        Assert.Contains("{{EPBC status|EN}}", OtherStatusLineEnd.For(epbc, [new ChosenStatus(1, "Endangered", null, null, "1")]));
        Assert.Contains("Great Britain Red List", OtherStatusLineEnd.For(gb, [new ChosenStatus(1, "Near Threatened", null, "Great Britain", "1")]));
        Assert.DoesNotContain("(Great Britain)", OtherStatusLineEnd.For(gb, [new ChosenStatus(1, "Near Threatened", null, "Great Britain", "1")]));
        Assert.Null(OtherStatusLineEnd.For(gb, []));
    }
}

public sealed class OtherStatusListPageTests : IClassFixture<SiteFactory> {
    private readonly HttpClient _client;

    public OtherStatusListPageTests(SiteFactory factory) {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task ListLinesEndWithTheChosenListsStatus() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae/list?other=us-esa");
        Assert.StartsWith("* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}} (Endangered Species Act: {{ESA status|T}})",
            Html.Textarea(html, "list-wikitext"));
        Assert.Contains("<option value=\"us-esa\" selected=\"selected\">Endangered Species Act</option>", html);
        var without = await _client.GetStringAsync("/taxa/family/ursidae/list");
        Assert.Equal("* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(without, "list-wikitext"));
    }

    [Fact]
    public async Task AnUnknownListAddsNothing() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae/list?other=xx-none");
        Assert.Equal("* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "list-wikitext"));
    }
}
