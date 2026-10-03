using System.Net;
using System.Text.Json;

namespace BeastieBot3.Site.Tests;

// Pages for taxa that are not in the release (the Amur leopard, an old id of the woylie), SPRAT
// profiles and EPBC listings that apply to a population (the koala) or use another name (the southern
// cassowary), and the note for a DOI found by checking doi.org.
public sealed class NotInReleaseAndListingTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    // ------------------------------------------------------------ taxa not in the release

    [Fact]
    public async Task AmurLeopardPageSaysThereIsNoCurrentAssessment_AndListsItsHistory() {
        var response = await _client.GetAsync($"/species/{FixtureDb.AmurLeopard}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var html = await response.Content.ReadAsStringAsync();
        var text = Html.Text(html);

        Assert.Contains("<h2 id=\"status-heading\">IUCN Red List status</h2>", html);
        Assert.Contains("<p class=\"no-global-line\">No current assessment in IUCN Red List version 2026-1.</p>", html);
        Assert.DoesNotContain("Latest global assessment", text);
        Assert.DoesNotContain("Read the full assessment on the IUCN Red List website", text);
        Assert.DoesNotContain("No global assessment", text);
        // No taxon in the release has its name.
        Assert.DoesNotContain("class=\"current-taxon\"", html);
        Assert.Contains($"Subspecies of <a href=\"/species/{FixtureDb.Leopard}\"><i>Panthera pardus</i></a>", html);

        // The history, with a wikitext link on every row and none marked latest; no wikitext is
        // shown until one is chosen.
        Assert.Contains("Assessment history", text);
        foreach (var id in new[] { FixtureDb.AmurLeopard2016Ne, FixtureDb.AmurLeopard2008, FixtureDb.AmurLeopard1996 }) {
            Assert.Contains($"href=\"/species/{FixtureDb.AmurLeopard}?assessment={id}", html);
        }
        Assert.Contains("NE Not Evaluated", text);
        Assert.DoesNotContain("<span class=\"tag\">Latest</span>", html);
        Assert.DoesNotContain("id=\"wikitext\"", html);
        Assert.Null(Html.Textarea(html, "wikitext-status"));
    }

    // None of the regional assessments of a taxon not in the release is current, so the table lists
    // all of them, not only the newest in each region.
    [Fact]
    public async Task TaxonNotInTheReleaseListsEveryRegionalAssessment() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.BombusPyrrhopygus}");
        var text = Html.Text(html);

        Assert.Contains("<p class=\"no-global-line\">No current assessment in IUCN Red List version 2026-1.</p>", html);
        Assert.DoesNotContain("No global assessment", text);
        Assert.DoesNotContain("Assessment history", text);
        var rows = new[] { FixtureDb.BombusEurope2016, FixtureDb.BombusEurope2015, FixtureDb.BombusEurope2013 }
            .Select(id => Html.IndexOf(html, $"href=\"/species/{FixtureDb.BombusPyrrhopygus}?assessment={id}"))
            .ToList();
        Assert.All(rows, at => Assert.True(at > 0));
        // Newest first.
        Assert.Equal(rows.Order(), rows);
        Assert.Contains("aria-label=\"Show wikitext for the Europe assessment published in 2013\"", html);
    }

    [Fact]
    public async Task AmurLeopardEarlierAssessmentStillHasWikitext() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.AmurLeopard}?assessment={FixtureDb.AmurLeopard2008}");
        var text = Html.Text(html);

        Assert.Contains("Wikitext for an earlier assessment: Critically Endangered, published 2008.", text);
        Assert.DoesNotContain("Show wikitext for the latest assessment", text);
        Assert.StartsWith("<ref name=\"iucn2008\">{{cite iucn", Html.Textarea(html, "wikitext-cite"));
        Assert.Equal($"{{{{IUCN status|CR|{FixtureDb.AmurLeopard}/{FixtureDb.AmurLeopard2008}|1|year=2008}}}}", Html.Textarea(html, "wikitext-status"));
    }

    [Fact]
    public async Task OldIdLinksToTheTaxonWithItsName_AndThatTaxonLinksBack() {
        var old = await _client.GetStringAsync($"/species/{FixtureDb.WoylieOld}");
        Assert.Contains("No current assessment in IUCN Red List version 2026-1.", Html.Text(old));
        Assert.Contains($"<p class=\"current-taxon\">IUCN Red List version 2026-1 lists <span class=\"sci-name\"><i>Bettongia penicillata</i></span> under <a href=\"/species/{FixtureDb.Woylie}\">IUCN id {FixtureDb.Woylie}</a>.</p>", old);

        // The old id has a global assessment, so the taxon's page combines the two histories and its
        // legend links the old id (CombinedHistoryPageTests).
        var current = await _client.GetStringAsync($"/species/{FixtureDb.Woylie}");
        Assert.Contains($"<a href=\"/species/{FixtureDb.WoylieOld}\">IUCN id {FixtureDb.WoylieOld}</a>", current);
        Assert.Contains($"IUCN id {FixtureDb.WoylieOld} (Bettongia penicillata): not in Red List version 2026-1; 1 global assessment, published 2008. "
            + $"Same scientific name as IUCN id {FixtureDb.Woylie}.", Html.Text(current));
        Assert.Contains("Latest global assessment", Html.Text(current));
    }

    [Fact]
    public async Task SearchGoesToTheTaxonInTheRelease_AndListsTheOldIdLast() {
        var redirect = await _client.GetAsync("/search?q=Bettongia+penicillata");
        Assert.Equal(HttpStatusCode.Redirect, redirect.StatusCode);
        Assert.Equal($"/species/{FixtureDb.Woylie}", redirect.Headers.Location?.OriginalString);

        var name = await _client.GetAsync("/name/Bettongia_penicillata");
        Assert.Equal($"/species/{FixtureDb.Woylie}", name.Headers.Location?.OriginalString);

        var listed = await _client.GetStringAsync("/search?q=Bettongia+penicillata&all=1");
        var current = Html.IndexOf(listed, $"/species/{FixtureDb.Woylie}\"");
        var old = Html.IndexOf(listed, $"/species/{FixtureDb.WoylieOld}\"");
        Assert.True(current > 0 && old > current, "the taxon in the release comes first");
        Assert.Contains("<span class=\"no-global\">No current assessment</span>", listed);
        Assert.Contains("2 taxa found", Html.Text(listed));
    }

    [Fact]
    public async Task SearchForANameOnlyATaxonNotInTheReleaseHas_GoesToItsPage() {
        var response = await _client.GetAsync("/search?q=amur%20leopard");
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var location = response.Headers.Location!.OriginalString;
        Assert.Equal($"/species/{FixtureDb.AmurLeopard}?q=amur%20leopard", location);
        Assert.Contains("“amur leopard” is a common name of this taxon.", Html.Text(await _client.GetStringAsync(location)));
    }

    [Fact]
    public async Task ChildrenAndSuggestionsShowTaxaNotInTheRelease() {
        var leopard = await _client.GetStringAsync($"/species/{FixtureDb.Leopard}");
        Assert.Contains($"/species/{FixtureDb.AmurLeopard}\"", leopard);
        Assert.Contains("<span class=\"no-global\">No current assessment</span>", leopard);

        using var suggest = JsonDocument.Parse(await _client.GetStringAsync("/api/suggest?q=Bettongia"));
        var ids = suggest.RootElement.EnumerateArray().Select(e => e.GetProperty("taxonId").GetInt64()).ToList();
        Assert.Equal(new[] { FixtureDb.Woylie, FixtureDb.WoylieOld }, ids);
        Assert.Equal(JsonValueKind.Null, suggest.RootElement[1].GetProperty("category").ValueKind);
    }

    // ------------------------------------------------------------ SPRAT and the EPBC Act

    [Fact]
    public async Task KoalaShowsBothSpratProfiles_AndThePopulationTheListingAppliesTo() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.Koala}")).Replace("&#x27;", "'");
        var text = Html.Text(html);

        Assert.Contains("<abbr title=\"Species Profile and Threats Database, Australian Government\">SPRAT profiles</abbr>", html);
        Assert.Contains($"<a href=\"https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id={FixtureDb.KoalaSprat}\"><i>Phascolarctos cinereus</i></a>", html);
        Assert.Contains($"<a href=\"https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id={FixtureDb.KoalaPopulationSprat}\"><i>Phascolarctos cinereus</i> (combined populations of Qld, NSW and the ACT)</a>", html);
        Assert.Contains("Listed as Endangered under Australia's <abbr title=\"Environment Protection and Biodiversity Conservation Act 1999\">EPBC Act</abbr>.", html);
        Assert.Contains("Listed as Endangered under Australia's EPBC Act. This listing applies only to the combined populations of Qld, NSW and the ACT.", text);
        // The listing follows the population's profile, not the species'.
        Assert.True(Html.IndexOf(html, $"taxon_id={FixtureDb.KoalaPopulationSprat}") < Html.IndexOf(html, "Listed as Endangered"));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(html, "Listed as "));
    }

    [Fact]
    public async Task ListingUnderAnotherNameSaysTheName() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}")).Replace("&#x27;", "'");
        var text = Html.Text(html);

        Assert.Contains("<abbr title=\"Species Profile and Threats Database, Australian Government\">SPRAT profile</abbr>", html);
        Assert.Contains("Listed as Endangered under Australia's EPBC Act. The listing uses the name Casuarius casuarius johnsonii.", text);
        Assert.Contains("<span class=\"sci-name\"><i>Casuarius casuarius johnsonii</i></span>", html);
    }

    // ------------------------------------------------------------ DOI found at doi.org

    [Fact]
    public async Task DoiFoundAtDoiOrgHasItsNote() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Woylie}");
        Assert.Contains($"|doi={FixtureDb.WoylieDoi}", Html.Textarea(html, "wikitext-cite"));
        Assert.Contains("<p class=\"note\">DOI found in Crossref&#x27;s list of IUCN DOIs, or by checking possible DOIs at doi.org.</p>", html);

        var about = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("for recent assessments missing from it, possible DOIs checked at doi.org A note", about);
        Assert.Contains("A note under the citation says when the DOI came from GBIF, Wikidata, Crossref or doi.org, or when no DOI was found.", about);
    }
}
