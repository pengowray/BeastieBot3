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
    public void Line_labels_have_no_brackets_or_leading_the() {
        Assert.Equal("Victoria, Australia", Choices.Find("au-vic")!.LineLabel);
        Assert.Equal("Swedish Red List 2025", Choices.Find("redlist:se-redlist-2025")!.LineLabel);
        Assert.Equal("Red List of the vascular plants of Switzerland, 2016", Choices.Find("redlist:ch-plants-2016")!.LineLabel);
        Assert.Equal("EPBC Act", Choices.Find("au-epbc")!.LineLabel);
    }

    [Fact]
    public void Status_choices_follow_the_categories_and_count_each_taxon_once() {
        ChosenStatus Status(long taxon, string status, string? code = null) => new(taxon, status, code, null, "x");
        var statuses = new Dictionary<long, IReadOnlyList<ChosenStatus>> {
            [1] = [Status(1, "Vulnerable (breeding)"), Status(1, "Least Concern (non-breeding)")],
            [2] = [Status(2, "Not Evaluated")],
            [3] = [Status(3, "Vulnerable"), Status(3, "Vulnerable")],
            [4] = [Status(4, "Extinct in the Wild")],
            [5] = [Status(5, "Waiting List")],
            [6] = [Status(6, "Extinct")],
        };
        var skip = new HashSet<string> { "Not Evaluated" };
        var choices = OtherStatusFilter.Choices("gb-jncc", [1, 2, 3, 4, 5, 6, 7], statuses, skip);
        Assert.Equal(["Extinct", "Extinct in the Wild", "Vulnerable", "Vulnerable (breeding)", "Least Concern (non-breeding)", "Not Evaluated", "Waiting List"],
            choices.Select(c => c.Key));
        Assert.Equal(1, choices.Single(c => c.Key == "Vulnerable").Lines);
        Assert.False(choices.Single(c => c.Key == "Not Evaluated").Ticked);

        // A taxon stays when any of its statuses is ticked.
        Assert.True(OtherStatusFilter.Keeps("gb-jncc", statuses[1], new HashSet<string> { "Vulnerable (breeding)" }));
        Assert.False(OtherStatusFilter.Keeps("gb-jncc", statuses[2], skip));
        Assert.False(OtherStatusFilter.Keeps("gb-jncc", null, skip));
    }

    [Fact]
    public void NatureServe_global_ranks_are_chosen_by_their_rounded_rank() {
        var g2g3 = new ChosenStatus(1, "G2G3", "G2", null, "x");
        Assert.Equal("G2", OtherStatusFilter.Key("natureserve-global", g2g3));
        Assert.Equal("G2G3", OtherStatusFilter.Key("natureserve-national", g2g3));
    }

    [Fact]
    public void Line_ends_use_the_template_or_the_text() {
        var epbc = Choices.Find("au-epbc")!;
        var gb = Choices.Find("jncc-redlist-gb")!;
        Assert.Equal(" (EPBC Act: {{EPBC status|EN}}, Western Australian population; {{EPBC status|VU}})", OtherStatusLineEnd.For(epbc, [
            new ChosenStatus(1, "Endangered", null, "Western Australian population", "1"),
            new ChosenStatus(1, "Vulnerable", null, null, "2"),
        ]));
        Assert.Equal(" (Great Britain Red List: Near Threatened)",
            OtherStatusLineEnd.For(gb, [new ChosenStatus(1, "Near Threatened", null, "Great Britain", "1")]));
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
    public async Task OnlyTaxaOnTheListLeavesTheOthersOutAndAddsNoStatus() {
        var us = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1&oline=0");
        var wikitext = Html.Textarea(us, "list-wikitext")!;
        Assert.Contains("megacarpa", wikitext);
        Assert.DoesNotContain("cryptadenia", wikitext);
        Assert.DoesNotContain("NatureServe", wikitext);
        Assert.Contains("data-live-count=\"other:only\">(1)</span>", us);
        Assert.Contains("<input type=\"checkbox\" name=\"oonly\" value=\"1\" checked=\"checked\">", us);
        Assert.Contains("<input type=\"checkbox\" name=\"oline\" value=\"1\">", us);

        // NatureServe's national ranks are one list per country. cryptadenia has no global assessment,
        // so it is listed when NE is ticked.
        var canada = Html.Textarea(await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&cat=NE&other=natureserve-national:CA&oonly=1&oline=0"), "list-wikitext")!;
        Assert.Contains("cryptadenia", canada);
        Assert.DoesNotContain("megacarpa", canada);
    }

    [Fact]
    public async Task ALeftOutStatusLeavesOutItsTaxaAndStaysOffered() {
        var html = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1&oline=0&oskip=N1");
        Assert.Null(Html.Textarea(html, "list-wikitext"));
        Assert.Contains("<input type=\"hidden\" name=\"oshown\" value=\"N1\"><input type=\"checkbox\" name=\"ostatus\" value=\"N1\"> N1", html);
        Assert.Contains("data-live-count=\"ostatus:N1\">(1)</span>", html);

        // The form sends the shown statuses and the ticked ones.
        var ticked = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1&oline=0&oskip=N1&oshown=N1&ostatus=N1");
        Assert.Contains("megacarpa", Html.Textarea(ticked, "list-wikitext"));

        // A left-out status that the group's taxa do not have is sent on in a hidden field.
        var other = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1&oline=0&oskip=NNA");
        Assert.Contains("<input type=\"hidden\" name=\"oskip\" value=\"NNA\">", other);
        Assert.Contains("megacarpa", Html.Textarea(other, "list-wikitext"));
    }

    [Fact]
    public async Task BothListsOnlyTheTaxaOnTheListWithTheirStatus() {
        var html = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1");
        var wikitext = Html.Textarea(html, "list-wikitext")!;
        Assert.Contains("megacarpa", wikitext);
        Assert.Contains("(NatureServe national rank, United States: N1)", wikitext);
        Assert.DoesNotContain("cryptadenia", wikitext);
    }

    [Fact]
    public async Task TheCategoryCountsAreOfTheTaxaOnTheList() {
        // megacarpa (NT) is on the US list, cryptadenia (no global assessment) on the Canadian one.
        var us = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:US&oonly=1&oline=0");
        Assert.Contains(CategoryCount("NT", 1), us);
        Assert.Contains(CategoryCount("NE", 0), us);
        var canada = await _client.GetStringAsync("/taxa/genus/hirtella/list?infra=separate&other=natureserve-national:CA&oonly=1&oline=0");
        Assert.Contains(CategoryCount("NT", 0), canada);
        Assert.Contains(CategoryCount("NE", 1), canada);
    }

    private static string CategoryCount(string category, int count) =>
        $"data-live-count=\"cat:{category}\">({count})</span>";

    [Fact]
    public async Task SpeciesFromCatalogueOfLifeAndWikidataAreLeftOut() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus/list?src=iucn&src=col&src=wd&style=sci&sort=sci&other=us-esa&oonly=1&oline=0");
        Assert.Equal("* [[Polar bear|''Ursus maritimus'']], Polar bear {{IUCN status|VU|22823/14871490|1|year=2015}}",
            Html.Textarea(html, "list-wikitext"));
    }

    [Fact]
    public async Task ATaxonNotOnTheListLeavesTheListEmpty() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae/list?other=au-epbc&oonly=1&oline=0");
        Assert.Null(Html.Textarea(html, "list-wikitext"));
        Assert.Contains("No taxa in Ursidae match these options.", html);
        Assert.Contains("data-live-count=\"other:only\">(0)</span>", html);
    }

    [Fact]
    public async Task SpeciesTablesListOnlyTheTaxaOnTheList() {
        var onList = await _client.GetStringAsync("/taxa/family/ursidae/list?type=table&other=us-esa&oonly=1&oline=0");
        Assert.Contains("|name=[[Polar bear]]", Html.Textarea(onList, "list-wikitext"));
        Assert.DoesNotContain("ESA status", Html.Textarea(onList, "list-wikitext"));
        var notOnList = await _client.GetStringAsync("/taxa/family/ursidae/list?type=table&other=au-epbc&oonly=1");
        Assert.Null(Html.Textarea(notOnList, "list-wikitext"));
    }

    [Fact]
    public async Task AnUnknownListAddsNothing() {
        var html = await _client.GetStringAsync("/taxa/family/ursidae/list?other=xx-none");
        Assert.Equal("* [[Polar bear]] {{IUCN status|VU|22823/14871490|1|year=2015}}", Html.Textarea(html, "list-wikitext"));
    }
}
