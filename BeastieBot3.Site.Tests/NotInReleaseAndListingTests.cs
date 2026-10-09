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

        // The history, with none marked latest.
        Assert.Contains("Assessment history", text);
        Assert.Contains("NE Not Evaluated", text);
        Assert.DoesNotContain("<span class=\"tag\">Latest</span>", html);

        // On the wikitext page, a wikitext link on every row; no wikitext is shown until one is chosen.
        var tool = await _client.GetStringAsync($"/species/{FixtureDb.AmurLeopard}/wikitext");
        foreach (var id in new[] { FixtureDb.AmurLeopard2016Ne, FixtureDb.AmurLeopard2008, FixtureDb.AmurLeopard1996 }) {
            Assert.Contains($"href=\"/species/{FixtureDb.AmurLeopard}/wikitext?assessment={id}", tool);
        }
        Assert.DoesNotContain("id=\"wikitext\"", tool);
        Assert.Contains("Choose an assessment in the tables below to get its wikitext.", Html.Text(tool));
        Assert.Null(Html.Textarea(tool, "wikitext-status"));
    }

    // None of the regional assessments of a taxon not in the release is current, so the table lists
    // all of them, not only the newest in each region.
    [Fact]
    public async Task TaxonNotInTheReleaseListsEveryRegionalAssessment() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.BombusPyrrhopygus}/wikitext");
        var text = Html.Text(html + await _client.GetStringAsync($"/species/{FixtureDb.BombusPyrrhopygus}"));

        Assert.Contains("<p class=\"no-global-line\">No current assessment in IUCN Red List version 2026-1.</p>", await _client.GetStringAsync($"/species/{FixtureDb.BombusPyrrhopygus}"));
        Assert.DoesNotContain("No global assessment", text);
        Assert.DoesNotContain("Assessment history", text);
        var rows = new[] { FixtureDb.BombusEurope2016, FixtureDb.BombusEurope2015, FixtureDb.BombusEurope2013 }
            .Select(id => Html.IndexOf(html, $"href=\"/species/{FixtureDb.BombusPyrrhopygus}/wikitext?assessment={id}"))
            .ToList();
        Assert.All(rows, at => Assert.True(at > 0));
        // Newest first.
        Assert.Equal(rows.Order(), rows);
        Assert.Contains("aria-label=\"Show wikitext for the Europe assessment published in 2013\"", html);
    }

    [Fact]
    public async Task AmurLeopardEarlierAssessmentStillHasWikitext() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.AmurLeopard}/wikitext?assessment={FixtureDb.AmurLeopard2008}");
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

        Assert.Contains("<abbr title=\"Species Profile and Threats Database, Australian Government\">SPRAT profiles</abbr>", html);
        Assert.Contains($"<a href=\"https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id={FixtureDb.KoalaSprat}\"><i>Phascolarctos cinereus</i></a>", html);
        Assert.Contains($"<a href=\"https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id={FixtureDb.KoalaPopulationSprat}\"><i>Phascolarctos cinereus</i> (combined populations of Qld, NSW and the ACT)</a>", html);
    }

    // Other conservation statuses: one table per country, the EPBC Act first, then the states in
    // alphabetical order; the population a listing applies to, else the whole species.
    [Fact]
    public async Task KoalaShowsItsEpbcAndStateStatuses() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.Koala}")).Replace("&#x27;", "'");
        var section = Html.Section(html, "other-statuses");
        var rows = Html.TableRows(section);

        Assert.Contains("<h2 id=\"other-statuses-heading\">Other conservation statuses</h2>", section);
        Assert.Contains("<h3>Australia</h3>", section);
        Assert.Equal(new[] { "List", "Status", "Applies to", "In effect from", "Source" }, rows[0]);
        Assert.Equal(new[] { "EPBC Act", "Endangered", "combined populations of Qld, NSW and the ACT", "12 February 2022", "SPRAT profile" }, rows[1]);
        Assert.Equal(new[] { "Australian Capital Territory", "Endangered", "combined populations of Qld, NSW and the ACT", "not given", "SPRAT profile" }, rows[2]);
        Assert.Equal(new[] { "New South Wales", "Endangered", "whole species", "not given", "SPRAT profile" }, rows[3]);
        Assert.Equal(new[] { "Queensland", "Endangered", "whole species", "not given", "SPRAT profile" }, rows[4]);
        Assert.Contains("<abbr title=\"Environment Protection and Biodiversity Conservation Act 1999\">EPBC Act</abbr>", section);
        Assert.Contains($"publicspecies.pl?taxon_id={FixtureDb.KoalaPopulationSprat}\">SPRAT profile</a>", section);
        Assert.Contains("All Australian statuses are from SPRAT", Html.Text(section));
        // The section comes after the IUCN sections and before the names.
        Assert.True(Html.IndexOf(html, "id=\"other-statuses\"") < Html.IndexOf(html, "id=\"names-heading\""));
    }

    [Fact]
    public async Task ListingUnderAnotherNameSaysTheName() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}")).Replace("&#x27;", "'");
        var section = Html.Section(html, "other-statuses");
        var rows = Html.TableRows(section);

        Assert.Contains("<abbr title=\"Species Profile and Threats Database, Australian Government\">SPRAT profile</abbr>", html);
        Assert.Equal(new[] { "List", "Status", "Name in list", "In effect from", "Source" }, rows[0]);
        Assert.Equal(new[] { "EPBC Act", "Endangered", "Casuarius casuarius johnsonii", "16 July 1999", "SPRAT profile" }, rows[1]);
        Assert.Contains("<span class=\"sci-name\"><i>Casuarius casuarius johnsonii</i></span>", section);
    }

    // The United States, Canada and NatureServe's global rank: one table each, each with only the
    // columns its rows have values for, and one note for each source.
    [Fact]
    public async Task PolarBearShowsItsUsCanadianAndNatureServeStatuses() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}")).Replace("&#x27;", "'");
        var section = Html.Section(html, "other-statuses");
        var text = Html.Text(section);
        var rows = Html.TableRows(section);

        Assert.True(Html.IndexOf(section, "<h3>Canada</h3>") < Html.IndexOf(section, "<h3>United States</h3>"));
        Assert.True(Html.IndexOf(section, "<h3>United States</h3>") < Html.IndexOf(section, "<h3>Global</h3>"));
        Assert.Equal(new[] {
            new[] { "List", "Status", "Source" },
            new[] { "COSEWIC", "Special Concern", "NatureServe Explorer" },
            new[] { "Species at Risk Act", "Special Concern", "NatureServe Explorer" },
            new[] { "List", "Status", "First listed", "Source" },
            new[] { "Endangered Species Act", "Threatened", "15 May 2008", "ECOS profile" },
            new[] { "List", "Status", "Source" },
            new[] { "NatureServe global rank", "G3G4 Vulnerable (rounded rank G3)", "NatureServe Explorer" },
        }, rows);
        Assert.Contains("<abbr title=\"Committee on the Status of Endangered Wildlife in Canada, an independent committee that assesses species\">COSEWIC</abbr>", section);
        Assert.Contains("<a href=\"https://ecos.fws.gov/ecp/species/4958\">ECOS profile</a>", section);
        // ECOS's date is the first listing, which a later change of status leaves as it is.
        Assert.Contains("<th scope=\"col\" title=\"ECOS gives the date the species or population was first listed. The status may have changed since then.\">First listed</th>", section);
        Assert.Contains("United States statuses are from ECOS, the US Fish and Wildlife Service's Environmental Conservation Online System, downloaded on 7 October 2026.", text);
        Assert.Contains("Canadian statuses and NatureServe global ranks are from NatureServe Explorer (© NatureServe, CC BY 4.0), downloaded on 8 October 2026. "
            + "NatureServe's copy of the Canadian statuses may differ from Canada's Species at Risk Public Registry.", text);
        Assert.Contains("<a href=\"https://creativecommons.org/licenses/by/4.0/\">CC BY 4.0</a>", section);
        Assert.DoesNotContain("SPRAT", text);
    }

    [Fact]
    public async Task HouseSparrowShowsItsBrazilianAndNztcsStatuses() {
        var html = (await _client.GetStringAsync($"/species/{FixtureDb.HouseSparrow}")).Replace("&#x27;", "'");
        var section = Html.Section(html, "other-statuses");

        Assert.True(Html.IndexOf(section, "<h3>Brazil</h3>") < Html.IndexOf(section, "<h3>New Zealand</h3>"));
        Assert.Equal(new[] {
            new[] { "List", "Status", "Assessed", "Source" },
            new[] { "ICMBio", "Not Applicable", "1 October 2018", "SALVE assessment" },
            new[] { "List", "Status", "Published in", "Source" },
            new[] { "NZTCS", "Introduced and Naturalised", "Birds 2021 (Robertson et al. 2021)", "NZTCS assessment" },
        }, Html.TableRows(section));
        Assert.Contains("<a href=\"https://doi.org/10.37002/salve.ficha.77.1\">SALVE assessment</a>", section);
        Assert.Contains("Brazilian statuses are ICMBio's national assessments of Brazil's fauna, from SALVE (Sistema de Avaliação do Risco de Extinção da Biodiversidade), "
            + "downloaded on 8 October 2026. Brazil's official list of threatened species (Portaria MMA 148/2022) can differ.", Html.Text(section));
        Assert.Contains("<abbr title=\"New Zealand Threat Classification System\">NZTCS</abbr>", section);
        Assert.Contains("<a href=\"https://nztcs.org.nz/assessments/70001\">NZTCS assessment</a>", section);
        Assert.Contains("New Zealand statuses are from the New Zealand Threat Classification System database (Department of Conservation, CC BY 4.0), downloaded on 8 October 2026.",
            Html.Text(section));
    }

    [Fact]
    public async Task TaxonWithNoOtherStatusHasNoSection() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.DoesNotContain("other-statuses", html);
    }

    // ------------------------------------------------------------ DOI found at doi.org

    [Fact]
    public async Task DoiFoundAtDoiOrgHasItsNote() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Woylie}/wikitext");
        Assert.Contains($"|doi={FixtureDb.WoylieDoi}", Html.Textarea(html, "wikitext-cite"));
        Assert.Contains("<p class=\"note\">DOI found in Crossref&#x27;s list of IUCN DOIs, or by checking possible DOIs at doi.org.</p>", html);

        var about = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("for recent assessments missing from it, possible DOIs checked at doi.org A note", about);
        Assert.Contains("A note under the citation says when the DOI came from GBIF, Wikidata, Crossref or doi.org, or when no DOI was found.", about);
    }
}
