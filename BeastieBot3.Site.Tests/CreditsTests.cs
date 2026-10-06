using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

// The credits section under the {{cite iucn}} box: IUCN's headings in IUCN's order, a count on each
// list, the total counting a name once, and no count where IUCN gives a group only in citation form.
public sealed class CreditsTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static string Credits(string html) {
        var start = html.IndexOf("<details class=\"iucn-credits\"", StringComparison.Ordinal);
        Assert.True(start >= 0, "the page should have the credits section");
        return html[start..html.IndexOf("</details>", start, StringComparison.Ordinal)];
    }

    [Fact]
    public async Task LatestAssessment_ListsTheCredits_UnderIucnsHeadings() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var credits = Credits(html);
        var text = Html.Text(credits);

        // Closed by default, after the box of the {{cite iucn}} wikitext.
        Assert.DoesNotContain(" open", credits[..credits.IndexOf('>')]);
        Assert.True(html.IndexOf("id=\"wikitext-cite\"", StringComparison.Ordinal) < html.IndexOf("id=\"iucn-credits\"", StringComparison.Ordinal));
        // John Goodrich is an assessor and a facilitator: four names, not five.
        Assert.Contains("<summary>Credits as given by IUCN (4 names)</summary>", credits);
        string[] inOrder = [
            "Assessor(s) (2)", "John Goodrich (Panthera)", "Hariyo Wibisono (WCS)",
            "Reviewer(s) (1)", "Sugoto Roy (IUCN)",
            "Facilitator(s) / Compiler(s) (1)", "John Goodrich (IUCN SSC Cat Specialist Group)",
            "Partner(s) / Institution(s) (1)", "Wildlife Conservation Society",
        ];
        var last = -1;
        foreach (var part in inOrder) {
            var at = text.IndexOf(part, last + 1, StringComparison.Ordinal);
            Assert.True(at > last, $"'{part}' should come after the previous part");
            last = at;
        }
        Assert.DoesNotContain("Contributor(s)", text);
    }

    [Fact]
    public async Task GroupInCitationForm_HasNoCount_AndTheTotalIsLeftOut() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}");
        var credits = Credits(html);

        Assert.Contains("<summary>Credits as given by IUCN</summary>", credits);
        Assert.Contains("Assessor(s) <span class=\"credit-count\">(1)</span>", credits);
        Assert.Contains("<p class=\"credit-type\">Reviewer(s)</p>", credits);
        Assert.Contains("<p class=\"credit-full\">Derocher, A. &amp; Lunn, N.</p>", credits);
        Assert.Contains("IUCN does not give full names for this group.", credits);
    }

    // The section is for the assessment the wikitext is for: none for an assessment with no credits.
    [Fact]
    public async Task AssessmentWithNoCredits_HasNoSection() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        Assert.DoesNotContain("iucn-credits", html);
    }

    [Theory]
    [InlineData("Krystal Tolley (IUCN SSC Chameleon Specialist Group / SANBI-ABR)", "krystal tolley")]
    [InlineData("James Kalema (Makerere University (Uganda))", "james kalema")]
    [InlineData("Wildlife Conservation Society", "wildlife conservation society")]
    [InlineData("(WCS)", "(wcs)")]
    public void NameKey_LeavesOutTheAffiliation(string entry, string key) =>
        Assert.Equal(key, CreditsView.NameKey(entry));
}
