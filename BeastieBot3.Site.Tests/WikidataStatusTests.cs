using System.Net;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Tests;

/// The "IUCN conservation status on Wikidata" part of the wikitext section, and the title and label
/// changes in the {{cite Q}} part. The commands come from WikidataStatusEdit and
/// WikidataCitation.FixCommands in BeastieBot3.Shared; these tests pin which case the page shows
/// for each kind of taxon, the words that describe what the commands do, and where the part sits.
public sealed class WikidataStatusTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static string Part(string html) {
        var start = Html.IndexOf(html, "<section class=\"wikidata-status\" id=\"wikidata-status\"");
        Assert.True(start > 0, "the page has the IUCN status part");
        return html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
    }

    private async Task<string> PartText(long taxonId) => Html.Text(Part(await _client.GetStringAsync($"/species/{taxonId}")));

    [Fact]
    public async Task ChangedStatus_ReplaceFirst_KeepInDetails() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var text = Html.Text(Part(html));

        Assert.Contains("IUCN conservation status on Wikidata", text);
        Assert.Contains("IUCN Red List 2026-1 endangered (Q96377276)", text);
        Assert.Contains("Wikidata item Q132186, downloaded 13 September 2026 vulnerable (Q278113)", text);
        Assert.Contains("Wikidata gives a different status.", text);
        Assert.Contains("The commands: add endangered (Q96377276) with the reference below remove vulnerable (Q278113)", text);
        Assert.Contains("None of the item's statements has preferred rank, so these commands remove the old status.", text);
        // The tiger's assessment has an item, so the reference states it, and only the release is left out.
        Assert.Contains($"stated in (P248): {FixtureDb.TigerLatestItem}, this assessment's Wikidata item", text);
        Assert.Contains("IUCN taxon ID (P627): 15955", text);
        Assert.Contains("reference URL (P854): https://www.iucnredlist.org/species/15955/214862019", text);
        Assert.Contains("Left out of the reference: stated in (P248) for IUCN Red List 2026-1: this site has no Wikidata item for the release", text);
        Assert.DoesNotContain("own Wikidata item", text);

        var commands = Html.Textarea(html, WikidataCite.StatusCommandsBoxId)!.Split('\n');
        Assert.Equal(2, commands.Length);
        Assert.StartsWith($"Q132186\tP141\tQ96377276\tS248\t{FixtureDb.TigerLatestItem}\tS627\t\"15955\"\tS854\t", commands[0]);
        Assert.Equal($"-STATEMENT\t{FixtureDb.TigerP141Statement}", commands[1]);
        Assert.Contains("https://quickstatements.toolforge.org/#/v1=", Part(html));

        Assert.Contains("<details class=\"status-keep\" id=\"wikidata-status-alt\">", html);
        var alt = Html.Text(Part(html)[Part(html).IndexOf("wikidata-status-alt", StringComparison.Ordinal)..]);
        Assert.Contains("Keep the old status on the item instead", alt);
        Assert.Contains("The commands: add endangered (Q96377276) with the same reference", alt);
        Assert.DoesNotContain("remove", alt);
        Assert.Contains("QuickStatements commands that keep the old status", alt);
        Assert.Contains("QuickStatements cannot set ranks. After the commands run, on the item's page set endangered (Q96377276) to preferred rank.", alt);
        Assert.Equal(commands[0], Html.Textarea(html, WikidataCite.StatusAltCommandsBoxId));
    }

    [Fact]
    public async Task SameStatus_AddsTheReferenceOnly() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PolarBear}");
        var text = Html.Text(Part(html));

        Assert.Contains("Wikidata gives the same status.", text);
        Assert.Contains("The commands: add the reference below to the vulnerable (Q278113) statement", text);
        Assert.Contains("stated in (P248) for this assessment's own Wikidata item: this site's data has none", text);
        Assert.DoesNotContain("remove", text);
        Assert.DoesNotContain("status-keep", html);
        Assert.Equal("Q33609\tP141\tQ278113\tS627\t\"22823\"\tS854\t\"https://www.iucnredlist.org/species/22823/14871490\"\tS813\t+2026-08-18T00:00:00Z/11",
            Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
        Assert.Contains("retrieved (P813): 18 August 2026, when this site downloaded the assessment", text);
    }

    [Fact]
    public async Task PreferredStatement_KeepFirst_AndNoCommandsOnlyRanks() {
        // The koala's item gives endangered at preferred rank, and the assessment's vulnerable at
        // normal rank with a reference to the assessment's item.
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Koala}");
        var part = Part(html);
        var text = Html.Text(part);

        Assert.Contains("endangered (Q96377276), preferred rank vulnerable (Q278113), normal rank", text);
        Assert.Contains("Wikidata gives a different status.", text);
        Assert.Contains("The item has a statement at preferred rank. Items like this keep earlier statuses at normal rank.", text);
        Assert.DoesNotContain("so these commands keep the old status", text);
        // Keeping the old status needs no commands, only ranks, and the text says so.
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
        var main = Html.Text(part[..part.IndexOf("wikidata-status-alt", StringComparison.Ordinal)]);
        Assert.DoesNotContain("After the commands run", main);
        Assert.Contains("No QuickStatements commands are needed. On the item's page, set vulnerable (Q278113) to preferred rank and set endangered (Q96377276) to normal rank.", main);
        // The vulnerable statement already cites the assessment's item, so no reference is described.
        Assert.DoesNotContain("The reference:", text);
        Assert.DoesNotContain("Left out of the reference", text);

        var alt = Html.Text(part[part.IndexOf("wikidata-status-alt", StringComparison.Ordinal)..]);
        Assert.Contains("Remove the old status from the item instead", alt);
        Assert.Contains("The commands: remove endangered (Q96377276), preferred rank", alt);
        Assert.Contains("QuickStatements commands that remove the old status", alt);
        Assert.Equal($"-STATEMENT\t{FixtureDb.KoalaP141Endangered}", Html.Textarea(html, WikidataCite.StatusAltCommandsBoxId));
    }

    [Fact]
    public async Task OtherSourceStatement_IsShownButNotCompared_AndATaxonIdReferenceCounts() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Woylie}");
        var text = Html.Text(Part(html));

        Assert.Contains("critically endangered (Q219127), normal rank endangered (Q96377276), normal rank, no reference to IUCN", text);
        Assert.Contains("Only statements with a reference to IUCN are compared with the assessment, and the commands never remove the other statements.", text);
        Assert.Contains("Wikidata gives the same status, with a reference that has IUCN taxon ID (P627) 2790. No commands needed.", text);
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
    }

    [Fact]
    public async Task TaxonIdOnSeveralItems_EveryItemIsCompared_NoCommands() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Leopard}");
        var text = Html.Text(Part(html));

        Assert.Contains("IUCN Red List 2026-1 vulnerable (Q278113)", text);
        Assert.Contains($"Wikidata item {FixtureDb.LeopardItem}, downloaded 13 September 2026 none", text);
        Assert.Contains($"Wikidata item {FixtureDb.LeopardOtherItem} vulnerable (Q278113)", text);
        Assert.Contains("No commands: 2 Wikidata items state IUCN taxon ID (P627) 15954, so first check which one is the item for this taxon.", text);
        // Another item has a status, so the page never says Wikidata has none.
        Assert.DoesNotContain("Wikidata gives no IUCN conservation status", text);
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
    }

    [Fact]
    public async Task TaxonIdAtDeprecatedRank_NoCommands() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Cassowary}");
        var text = Html.Text(Part(html));

        Assert.Contains($"Wikidata item {FixtureDb.CassowaryItem}, downloaded 13 September 2026, IUCN taxon ID at deprecated rank least concern (Q211005)", text);
        Assert.Contains($"No commands: the Wikidata item {FixtureDb.CassowaryItem} states IUCN taxon ID (P627) 22678108 only at deprecated rank, so it may not be the item for this taxon.", text);
        Assert.DoesNotContain("Wikidata gives", text);
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
    }

    [Fact]
    public async Task NoStatus_AddsIt_PossiblyExtinctIsExplained() {
        var text = await PartText(FixtureDb.Baiji);

        Assert.Contains("Wikidata item Q190826, downloaded 14 September 2026 none", text);
        Assert.Contains("Critically Endangered (Possibly Extinct) is critically endangered (Q219127) on Wikidata, which has no value for possibly extinct.", text);
        Assert.Contains("Wikidata gives no IUCN conservation status for this taxon.", text);
        Assert.Contains("add critically endangered (Q219127) with the reference below", text);
    }

    [Fact]
    public async Task ConservationDependent_ComparisonButNoCommands() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.ArtemiaMonica}");
        var text = Html.Text(Part(html));

        Assert.Contains("IUCN Red List 2026-1 no value for LR/cd", text);
        Assert.Contains("least concern (Q211005)", text);
        Assert.Contains("No commands: Wikidata has no IUCN conservation status value for Lower Risk/conservation dependent (LR/cd).", text);
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
    }

    [Fact]
    public async Task DeprecatedStatementWithTheValue_NoCommands() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.PlantSubspecies}");
        var text = Html.Text(Part(html));

        Assert.Contains("least concern (Q211005)", text);
        Assert.Contains("No commands: a deprecated statement on the item has near threatened (Q719675), and QuickStatements could add the reference to that statement.", text);
        Assert.Null(Html.Textarea(html, WikidataCite.StatusCommandsBoxId));
    }

    [Fact]
    public async Task ItemMatchedByName_NoCommands() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Lion}");
        Assert.Contains("No commands: the Wikidata item <a href=\"https://www.wikidata.org/wiki/Q140\">Q140</a> was matched to this taxon by name and does not state IUCN taxon ID (P627) 15951.", Part(html));
        Assert.DoesNotContain("status-compare", Part(html));
    }

    [Fact]
    public async Task OtherCasesWithNoCommands() {
        Assert.Contains("No commands: this site has not downloaded the Wikidata item Q28922.", await PartText(FixtureDb.HouseSparrow));
        Assert.Contains("No commands: no Wikidata item found that states IUCN taxon ID (P627) 15966.", await PartText(FixtureDb.SumatranTiger));
        Assert.Contains("No commands: this site offers them for species and subspecies only.", await PartText(FixtureDb.WestAfricanLion));
        Assert.Contains("No commands: this site offers them for species and subspecies only.", await PartText(FixtureDb.Variety));
    }

    [Fact]
    public async Task OnlyForTheLatestGlobalAssessment() {
        foreach (var url in new[] {
                     $"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}",
                     $"/species/{FixtureDb.HouseSparrow}?assessment={FixtureDb.HouseSparrowEurope}",
                     $"/species/{FixtureDb.RegionalOnly}",
                     $"/species/{FixtureDb.AmurLeopard}",
                 }) {
            var response = await _client.GetAsync(url);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.DoesNotContain("id=\"wikidata-status\"", await response.Content.ReadAsStringAsync());
        }
    }

    [Fact]
    public async Task PartComesAfterTheCiteQPartInTheWikitextSection() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var citeQ = Html.IndexOf(html, "id=\"wikidata-cite\"");
        var status = Html.IndexOf(html, "id=\"wikidata-status\"");
        var history = Html.IndexOf(html, "<section class=\"history\"");
        Assert.True(citeQ > 0 && status > citeQ && history > status);
        // Outside the live region that site.js replaces, so a citation option change leaves it alone.
        var citeQEnd = html.IndexOf("</section>", citeQ, StringComparison.Ordinal);
        Assert.True(status > citeQEnd);
    }

    // ------------------------------------------------------------ title and label in the {{cite Q}} part

    [Fact]
    public async Task ItemTitleAndLabel_AreListedAsReplaced() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Tiger}");
        var start = Html.IndexOf(html, "id=\"wikidata-cite\"");
        var citeQ = html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
        var text = Html.Text(citeQ);

        // Added and replaced are separate: the title the item has is not listed as missing.
        Assert.Contains("The item is missing these statements: full work available at URL (P953), publication date (P577), DOI (P356), author name string (P2093).", text);
        Assert.Contains("The commands also replace the item's title and English label:", text);
        Assert.Contains($"Replaced On Wikidata now After the commands title (P1476) {FixtureDb.TigerItemOldTitle} Panthera tigris", text);
        Assert.Contains($"label (en) {FixtureDb.TigerItemOldTitle} Panthera tigris. The IUCN Red List of Threatened Species 2022: e.T15955A214862019", text);
        Assert.Contains("{{cite Q}} shows the item's title as the title of the work.", text);
        Assert.Contains("Run these commands in QuickStatements with your Wikidata account.", text);
        Assert.Contains("QuickStatements commands to update the item", text);

        var commands = Html.Textarea(html, WikidataCite.CommandsBoxId)!.Split('\n');
        Assert.Contains($"{FixtureDb.TigerLatestItem}\tP1476\ten:\"Panthera tigris\"", commands);
        Assert.Contains($"-{FixtureDb.TigerLatestItem}\tP1476\ten:\"{FixtureDb.TigerItemOldTitle}\"", commands);
        Assert.Contains($"{FixtureDb.TigerLatestItem}\tLen\t\"Panthera tigris. The IUCN Red List of Threatened Species 2022: e.T15955A214862019\"", commands);
    }

    private async Task<string> CiteQPart(string url) {
        var html = await _client.GetStringAsync(url);
        var start = Html.IndexOf(html, "id=\"wikidata-cite\"");
        return html[start..html.IndexOf("</section>", start, StringComparison.Ordinal)];
    }

    [Fact]
    public async Task ErrataVersionSharingAnItem_OnlyCiteQ_AndALinkToTheAssessmentItIsFor() {
        var html = await _client.GetStringAsync($"/species/{FixtureDb.Micropyropsis}");
        var part = await CiteQPart($"/species/{FixtureDb.Micropyropsis}");
        var text = Html.Text(part);

        Assert.Contains($"{{{{cite Q|{FixtureDb.MicropyropsisItem}", Html.Textarea(html, WikidataCite.CiteQBoxId));
        Assert.Contains("This errata version has the same DOI as the assessment it corrects, so it shares that assessment's Wikidata item. Commands to update the item are on that assessment's page.", text);
        Assert.Contains($"assessment={FixtureDb.MicropyropsisReplaced}", part);
        Assert.Null(Html.Textarea(html, WikidataCite.CommandsBoxId));
        Assert.DoesNotContain("QuickStatements", text);
    }

    [Fact]
    public async Task TheAssessmentAnItemIsFor_HasTheCommands_BuiltFromItsOwnIds() {
        var url = $"/species/{FixtureDb.Micropyropsis}?assessment={FixtureDb.MicropyropsisReplaced}";
        var html = await _client.GetStringAsync(url);
        var commands = Html.Textarea(html, WikidataCite.CommandsBoxId)!.Split('\n');

        Assert.Contains($"{FixtureDb.MicropyropsisItem}\tP1476\ten:\"Micropyropsis tuberosa\"", commands);
        Assert.Contains($"{FixtureDb.MicropyropsisItem}\tLen\t\"Micropyropsis tuberosa. The IUCN Red List of Threatened Species 2010: e.T162107A5539282\"", commands);
        Assert.Contains($"{FixtureDb.MicropyropsisItem}\tP953\t\"https://www.iucnredlist.org/species/162107/5539282\"", commands);
        Assert.DoesNotContain("errata version has the same DOI", Html.Text(await CiteQPart(url)));
    }

    [Fact]
    public async Task NewItem_UsesTheRegisteredName_AndSaysSo() {
        var url = $"/species/{FixtureDb.PolarBear}?assessment={FixtureDb.PolarBear2008}";
        var html = await _client.GetStringAsync(url);
        var text = Html.Text(await CiteQPart(url));

        Assert.Contains($"This assessment was published under the name {FixtureDb.PolarBear2008RegisteredName}, which is the name in the title registered with Crossref for its DOI. IUCN's citation now gives the name Ursus maritimus. The commands use the published name.", text);
        var commands = Html.Textarea(html, WikidataCite.CommandsBoxId)!.Split('\n');
        Assert.Contains($"LAST\tP1476\ten:\"{FixtureDb.PolarBear2008RegisteredName}\"", commands);
        Assert.Contains(commands, c => c.StartsWith($"LAST\tLen\t\"{FixtureDb.PolarBear2008RegisteredName}. The IUCN Red List", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NewItem_WithIucnsCitationName_SaysThePublishedNameIsNotKnown() {
        var text = Html.Text(await CiteQPart($"/species/{FixtureDb.Bromus}"));
        Assert.Contains("The commands use the name in IUCN's citation, Bromus interruptus. This site does not know the name the assessment was published under, which for an older assessment can be an earlier name of the taxon.", text);
    }

    [Fact]
    public async Task AboutPageSaysWhatTheCommandsDo() {
        var text = Html.Text(await _client.GetStringAsync("/about"));
        Assert.Contains("for a species or subspecies, QuickStatements commands that bring the IUCN conservation status on its Wikidata item up to date", text);
        Assert.Contains("Titles and labels of assessment items.", text);
        Assert.Contains("IUCN conservation status on Wikidata.", text);
        Assert.Contains("QuickStatements cannot set ranks, so you then set the ranks on the item's page.", text);
        Assert.Contains("Only statements with a reference to IUCN are compared, and the commands never remove the others.", text);
        Assert.Contains("The name in the title and label is the name the assessment was published under", text);
        Assert.Contains("An errata version that has the same DOI as the assessment it corrects shares that assessment's item", text);
    }
}

public sealed class WikidataStatusUnitTests {
    private static TaxonRow Taxon(string kind = TaxonKinds.Species, string? qid = "Q132186", string? source = "p627",
        string? p141 = "[]", string? downloaded = "2026-09-13") =>
        new(15955, "Panthera tigris", kind, "ANIMALIA", null, null, null, null, null, null, null, null, null, null, qid, null, 214862019,
            WikidataQidSource: source, WikidataP141: p141, WikidataItemDownloaded: downloaded);

    private static AssessmentRow Latest(string category = "EN", string? item = null) =>
        new(214862019, 15955, "Global", true, category, false, false, null, "3.1", 2022, null, null, null, null, item);

    private static string Statement(string value, string rank = "normal") =>
        WikidataStatusStatement.ListToJson([new("Q132186$1A2B3C4D-0000-4000-8000-000000000001", value, rank, [], [], 1, CitesIucn: true)]);

    [Theory]
    [InlineData(TaxonKinds.Variety)]
    [InlineData(TaxonKinds.Subpopulation)]
    public void NotSpeciesOrSubspecies(string kind) =>
        Assert.Equal(WikidataStatusScope.NotSpeciesOrSubspecies, WikidataCite.BuildStatus(Taxon(kind), Latest(), null).Scope);

    [Fact]
    public void ScopeChecks() {
        Assert.Equal(WikidataStatusScope.Offered, WikidataCite.BuildStatus(Taxon(TaxonKinds.Subspecies), Latest(), null).Scope);
        Assert.Equal(WikidataStatusScope.NoItem, WikidataCite.BuildStatus(Taxon(qid: null), Latest(), null).Scope);
        Assert.Equal(WikidataStatusScope.NoItem, WikidataCite.BuildStatus(Taxon(qid: "not an item"), Latest(), null).Scope);
        Assert.Equal(WikidataStatusScope.MatchedByName, WikidataCite.BuildStatus(Taxon(source: "name-match"), Latest(), null).Scope);
        Assert.Equal(WikidataStatusScope.StatementsUnknown, WikidataCite.BuildStatus(Taxon(p141: null), Latest(), null).Scope);
    }

    [Fact]
    public void UnreadableStatements_AreReportedAndTreatedAsUnknown() {
        var failed = new List<string>();
        var view = WikidataCite.BuildStatus(Taxon(p141: "{ not json"), Latest(), null, (what, _) => failed.Add(what));
        Assert.Equal(WikidataStatusScope.StatementsUnknown, view.Scope);
        Assert.Single(failed);
    }

    [Fact]
    public void ChangedStatus_HasBothPlans() {
        var view = WikidataCite.BuildStatus(Taxon(p141: Statement("Q278113")), Latest(), null);
        Assert.Equal(StatusEditOutcome.Differs, view.Plan!.Outcome);
        Assert.Equal(StatusEditChoice.Replace, view.Plan.Choice);
        Assert.Single(view.Plan.Removes);
        Assert.Equal(StatusEditChoice.Keep, view.AltPlan!.Choice);
        Assert.Empty(view.AltPlan.Removes);
        Assert.NotNull(view.Commands);
        Assert.NotNull(view.AltCommands);
        Assert.Equal("13 September 2026", view.DownloadedText);
    }

    [Fact]
    public void PreferredStatement_KeepIsShownFirst() {
        var view = WikidataCite.BuildStatus(Taxon(p141: Statement("Q278113", "preferred")), Latest(), null);
        Assert.Equal(StatusEditChoice.Keep, view.Plan!.Choice);
        Assert.Equal(StatusEditChoice.Replace, view.AltPlan!.Choice);
    }

    [Fact]
    public void SameStatus_HasNoOtherChoice() {
        var view = WikidataCite.BuildStatus(Taxon(p141: Statement("Q96377276")), Latest(), null);
        Assert.Equal(StatusEditOutcome.Agrees, view.Plan!.Outcome);
        Assert.Null(view.AltPlan);
        Assert.Null(view.AltCommands);
    }

    [Fact]
    public void ReviewScopes_HaveNoPlan() {
        var others = WikidataOtherTaxonItem.ListToJson([new WikidataOtherTaxonItem("Q1588648", false, [])]);
        var several = WikidataCite.BuildStatus(Taxon(p141: Statement("Q278113")) with { WikidataOtherItems = others }, Latest(), null);
        Assert.Equal(WikidataStatusScope.TaxonIdOnSeveralItems, several.Scope);
        Assert.Null(several.Plan);
        Assert.Equal("Q1588648", Assert.Single(several.OtherItems).Qid);
        Assert.True(several.HasContent);

        var deprecated = WikidataCite.BuildStatus(Taxon(p141: Statement("Q278113")) with { WikidataP627Deprecated = true }, Latest(), null);
        Assert.Equal(WikidataStatusScope.TaxonIdDeprecated, deprecated.Scope);
        Assert.Null(deprecated.Commands);
    }

    [Fact]
    public void RetrievedIsTheDayTheAssessmentWasDownloaded() {
        var parts = new IucnCitationParts {
            TaxonId = 15955, AssessmentId = 214862019, Year = 2022, ScientificName = "Panthera tigris",
            DownloadedAtUtc = new DateTime(2026, 8, 21, 23, 30, 0, DateTimeKind.Utc),
        };
        var view = WikidataCite.BuildStatus(Taxon(), Latest(item: "Q900000001"), parts);
        Assert.Equal(new DateOnly(2026, 8, 21), view.Plan!.Reference!.Retrieved);
        Assert.Equal("Q900000001", view.Plan.Reference.StatedIn);
        Assert.Contains("S813\t+2026-08-21T00:00:00Z/11", view.Commands!.Text);
    }

    [Fact]
    public void MalformedStatementId_LeavesOutTheCommandsAndReportsIt() {
        var failed = new List<string>();
        var bad = WikidataStatusStatement.ListToJson([new("Q132186$not-a-guid", "Q278113", "normal", [], [], 1, CitesIucn: true)]);
        var view = WikidataCite.BuildStatus(Taxon(p141: bad), Latest(), null, (what, _) => failed.Add(what));
        Assert.Equal(WikidataStatusScope.Offered, view.Scope);
        Assert.Null(view.Plan);
        Assert.Null(view.Commands);
        Assert.False(view.HasContent);
        Assert.Equal(["WikidataStatusEdit.Plan"], failed);
    }
}
