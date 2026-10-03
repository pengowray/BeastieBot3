namespace BeastieBot3.Site.Tests;

/// The "Full given names instead of initials" option of the citation options form. What the
/// citation then says is CiteIucnRenderer's business; these tests pin the form and the links.
public sealed class FullGivenNamesPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private const string Checkbox = "<input type=\"checkbox\" name=\"fullnames\" value=\"1\"";

    [Fact]
    public async Task OptionIsShownOnlyWhenAnAuthorHasFullGivenNames() {
        var polarBear = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        Assert.DoesNotContain(Checkbox, polarBear);
        Assert.DoesNotContain("Full given names", polarBear);
        Assert.DoesNotContain("name=\"fullnames\"", polarBear);

        var tiger = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        Assert.Contains(Checkbox + " aria-describedby=\"fullnames-help\"> Full given names instead of initials</label>", tiger);
        Assert.DoesNotContain(Checkbox + " checked", tiger);
        Assert.Contains("IUCN gives full given names for 1 of the 2 authors, such as “John” for “Goodrich, J.”. The other author is written as in IUCN's citation.",
            Html.Text(tiger));
        // The option sits in the Author names group.
        var group = Html.IndexOf(tiger, "<legend>Author names</legend>");
        var access = Html.IndexOf(tiger, "<legend>Access date</legend>");
        var option = Html.IndexOf(tiger, Checkbox);
        Assert.True(group < option && option < access);

        var lion = Html.Text(await _client.GetStringAsync($"/species/{FixtureDb.Lion}"));
        Assert.Contains("IUCN gives full given names for both authors, such as “Samantha” for “Nicholson, S.”.", lion);
    }

    [Fact]
    public async Task ChoiceIsKept() {
        var tiger = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}?fullnames=1");
        Assert.Contains(Checkbox + " checked=\"checked\"", tiger);
        // The form sent without the box ticked turns it off.
        var off = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}?opts=1&ref=1&refname=iucn");
        Assert.DoesNotContain(Checkbox + " checked", off);
    }

    [Fact]
    public async Task ChoiceGoesToOtherAssessments() {
        // The polar bear's citations have no full given names, so the option is not shown, but the
        // choice is kept in the form and in the links to other assessments.
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?fullnames=1");
        Assert.DoesNotContain(Checkbox, html);
        Assert.Contains("<input type=\"hidden\" name=\"fullnames\" value=\"1\">", html);
        Assert.Contains($"href=\"/species/22823?assessment={FixtureDb.PolarBear2008}&amp;fullnames=1#wikitext\"", html);
    }

    [Fact]
    public async Task OptionIsPartOfTheCacheKey() {
        var plain = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var full = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}?fullnames=1");
        Assert.DoesNotContain(Checkbox + " checked", plain);
        Assert.Contains(Checkbox + " checked=\"checked\"", full);
        Assert.Contains("fullnames", Web.SiteCachePolicies.SpeciesQueryKeys);
    }
}
