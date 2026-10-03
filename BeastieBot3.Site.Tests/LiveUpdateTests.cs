using System.Text.RegularExpressions;
using BeastieBot3.Site.Web;

namespace BeastieBot3.Site.Tests;

/// The markup site.js uses to update the wikitext as the citation options change, and the form
/// without JavaScript. The script itself is checked in a browser by browser/live-update.cjs.
public sealed class LiveUpdateTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private Task<string> Page(string query = "") => _client.GetStringAsync($"/species/{FixtureDb.PolarBear}{query}");

    private static string Attribute(string html, string name) =>
        System.Net.WebUtility.HtmlDecode(Regex.Match(html, $"<section class=\"wikitext\"[^>]*\\s{Regex.Escape(name)}=\"([^\"]*)\"").Groups[1].Value);

    [Fact]
    public async Task SectionCarriesTheStatusTextAndItsOwnAddress() {
        var html = await Page();
        Assert.Equal("Wikitext updated", Attribute(html, "data-live-updated"));
        Assert.Equal("/species/22823", Attribute(html, "data-options-url"));

        // The address the form's query stands for, without what is the default.
        var chosen = await Page("?opts=1&authors=lastfirst&access=download&ref=1&refname=iucn&amp=1");
        Assert.Equal("/species/22823?authors=lastfirst&opts=1&ref=1&amp=1&refname=iucn", Attribute(chosen, "data-options-url"));

        var earlier = await Page($"?assessment={FixtureDb.PolarBear2008}&opts=1&authors=author&access=none&ref=1&refname=iucn2008");
        Assert.Equal($"/species/22823?assessment={FixtureDb.PolarBear2008}&access=none", Attribute(earlier, "data-options-url"));
    }

    [Fact]
    public async Task RegionsHaveIdsAndTheFormIsOutsideThem() {
        var html = await Page();
        var regions = Regex.Matches(html, "<(div|section)[^>]*\\sdata-live-region[\\s>]");
        Assert.Equal(2, regions.Count);
        Assert.All(regions, region => Assert.Matches("\\sid=\"[a-z-]+\"", region.Value));
        Assert.Contains("<div class=\"wikitext-output\" id=\"wikitext-output\" data-live-region>", html);
        Assert.Contains("<section class=\"wikidata-cite\" id=\"wikidata-cite\" aria-labelledby=\"wikidata-cite-heading\" data-live-region>", html);

        // The boxes are in the first region, which is closed before the form starts.
        var output = html.LastIndexOf("<div", Html.IndexOf(html, "id=\"wikitext-output\""), StringComparison.Ordinal);
        var cite = Html.IndexOf(html, "<textarea id=\"wikitext-cite\"");
        var form = Html.IndexOf(html, "<form class=\"options-form\"");
        Assert.True(output < cite && cite < form);
        var depth = 0;
        foreach (Match tag in Regex.Matches(html[output..form], "<div[\\s>]|</div>")) {
            depth += tag.Value.StartsWith("</", StringComparison.Ordinal) ? -1 : 1;
        }
        Assert.Equal(0, depth);
    }

    [Fact]
    public async Task FormKeepsItsButtonAndHasAStatusLine() {
        var html = await Page();
        Assert.Contains("<button type=\"submit\">Update wikitext</button>", html);
        Assert.Contains("<p class=\"live-status\" role=\"status\" data-live-status hidden></p>", html);
        Assert.Contains("<form class=\"options-form\" method=\"get\" action=\"/species/22823#wikitext\">", html);
    }

    [Fact]
    public async Task TheQueryTheFormSendsWorksWithoutJavaScript() {
        // A browser without JavaScript sends every field of the form.
        var html = await Page("?opts=1&authors=lastfirst&access=none&ref=1&refname=bear&amp=1");
        var cite = Html.Textarea(html, "wikitext-cite")!;
        Assert.StartsWith("<ref name=\"bear\">{{cite iucn |last1=Wiig |first1=Ø.", cite);
        Assert.Contains("|name-list-style=amp", cite);
        Assert.DoesNotContain("access-date", cite);
        Assert.Contains("value=\"lastfirst\" checked=\"checked\"", html);
        Assert.Contains("name=\"refname\" value=\"bear\"", html);
    }

    [Fact]
    public async Task AboutPageSaysTheWikitextChangesWithTheOptions() {
        var text = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("The wikitext changes as soon as you change an option. In a browser without JavaScript, select Update wikitext to load the page with the new wikitext.", text);
        Assert.DoesNotContain("to reload the page", text);
        Assert.Contains("whether to use the authors' full given names instead of initials.", text);
    }

    [Fact]
    public async Task ShowWikitextLinksHaveKeys() {
        var latest = await Page("?authors=lastfirst");
        Assert.Contains($"<a data-options-link=\"{FixtureDb.PolarBear2008}\" href=\"/species/22823?assessment={FixtureDb.PolarBear2008}&amp;authors=lastfirst#wikitext\"", latest);

        // On an earlier assessment's page the latest one is the page's default.
        var earlier = await Page($"?assessment={FixtureDb.PolarBear2008}&authors=lastfirst");
        Assert.Contains("<a data-options-link=\"default\" href=\"/species/22823?authors=lastfirst#wikitext\"", earlier);

        var sparrow = await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}?access=none");
        Assert.Contains($"<a data-options-link=\"{FixtureDb.HouseSparrowEurope}\" href=\"/species/{FixtureDb.HouseSparrow}?assessment={FixtureDb.HouseSparrowEurope}&amp;access=none#wikitext\"", sparrow);
    }

    [Fact]
    public async Task NoInlineScriptOrHandlers() {
        foreach (var id in new[] { FixtureDb.PolarBear, FixtureDb.Tiger, FixtureDb.WestAfricanLion }) {
            var html = await _client.GetStringAsync($"/species/{id}");
            Assert.Empty(Regex.Matches(html, "<script(?![^>]*\\ssrc=)[^>]*>"));
            Assert.Empty(Regex.Matches(html, "\\son[a-z]+=", RegexOptions.IgnoreCase));
            Assert.DoesNotContain(" style=\"", html);
        }
    }

    [Fact]
    public async Task PageIsServedWithThePolicyThatAllowsTheRequest() {
        var response = await _client.GetAsync($"/species/{FixtureDb.PolarBear}");
        var policy = string.Join(";", response.Headers.GetValues("Content-Security-Policy"));
        Assert.Equal(SiteMiddleware.ContentSecurityPolicy, policy);
        // site.js asks the site for the page again: same origin.
        Assert.Contains("connect-src 'self'", policy);
        Assert.Contains("script-src 'self';", policy);
    }

    [Fact]
    public async Task ScriptUsesTheHooksAndWritesNoMarkup() {
        var js = await _client.GetStringAsync("/site.js");
        foreach (var hook in new[] { "data-live-region", "data-options-url", "data-options-link", "data-live-status", "data-live-updated",
                     "history.replaceState", "button[data-copy]", "form.options-form" }) {
            Assert.Contains(hook, js);
        }
        // New wikitext comes in as parsed nodes, never as HTML strings, and the script runs no code
        // it receives.
        Assert.DoesNotContain("innerHTML", js);
        Assert.DoesNotContain("outerHTML", js);
        Assert.DoesNotContain("insertAdjacentHTML", js);
        Assert.DoesNotContain("eval(", js);
        Assert.DoesNotContain("new Function", js);
    }
}
