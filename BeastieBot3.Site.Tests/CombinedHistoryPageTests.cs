using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Tests;

// The combined assessment history on the pages of old IUCN ids and of the taxa linked to them: the
// woylie pair (same name), Acropora minuta (a synonym of Acropora palmerae), Platanista gangetica
// (an old id linked to two taxa), and ids with no global assessments.
public sealed class CombinedHistoryPageTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private Task<string> Page(long taxonId, string query = "") => _client.GetStringAsync($"/species/{taxonId}{query}");

    // The rows of the combined table: (IUCN id, assessment id) in page order, read from each row's
    // IUCN Red List link.
    private static List<(long TaxonId, long AssessmentId)> Rows(string html) {
        var table = Regex.Match(html, "<table class=\"combined-table\">.*?</table>", RegexOptions.Singleline);
        Assert.True(table.Success, "the page has a combined table");
        return Regex.Matches(table.Value, "href=\"https://www.iucnredlist.org/species/(\\d+)/(\\d+)\"")
            .Select(m => (long.Parse(m.Groups[1].Value), long.Parse(m.Groups[2].Value)))
            .ToList();
    }

    // The class attribute of the table row that links this assessment on the IUCN Red List website.
    private static string RowClass(string html, long taxonId, long assessmentId) {
        var row = Regex.Matches(html, "<tr( class=\"[^\"]*\")?>(?:(?!</tr>).)*</tr>", RegexOptions.Singleline)
            .Single(m => m.Value.Contains($"iucnredlist.org/species/{taxonId}/{assessmentId}\"", StringComparison.Ordinal));
        return row.Groups[1].Value;
    }

    private static string Section(string html) =>
        Regex.Match(html, "<section class=\"history combined-history\".*?</section>", RegexOptions.Singleline).Value;

    [Fact]
    public async Task CurrentTaxonPage_CombinesItsHistoryWithTheOldIdsWithItsName() {
        var html = await Page(FixtureDb.Woylie);
        var text = Html.Text(html);

        Assert.Contains("<h2 id=\"history-heading\">Combined assessment history</h2>", html);
        Assert.DoesNotContain(">Assessment history</h2>", html);
        Assert.Equal(new[] { (FixtureDb.Woylie, FixtureDb.WoylieLatest), (FixtureDb.WoylieOld, FixtureDb.WoylieOld2008) }, Rows(html));
        // The taxon in the release has the first colour on both pages of the pair.
        Assert.Contains("id-tint-1 shown", RowClass(html, FixtureDb.Woylie, FixtureDb.WoylieLatest));
        Assert.Equal(" class=\"id-tint-2\"", RowClass(html, FixtureDb.WoylieOld, FixtureDb.WoylieOld2008));
        Assert.Contains("<span class=\"id-swatch id-tint-1\" aria-hidden=\"true\"></span>", html);
        // The same name: no Name column.
        Assert.DoesNotContain("<th scope=\"col\">Name</th>", Section(html));
        // The old id links to its page, and its wikitext link goes there with the assessment.
        Assert.Contains($"<a href=\"/species/{FixtureDb.WoylieOld}\" aria-label=\"IUCN id {FixtureDb.WoylieOld}\">{FixtureDb.WoylieOld}</a>", html);
        Assert.Contains($"data-options-link=\"{FixtureDb.WoylieOld}-{FixtureDb.WoylieOld2008}\" href=\"/species/{FixtureDb.WoylieOld}?assessment={FixtureDb.WoylieOld2008}#wikitext\"", html);
        // The legend, in place of the line "Earlier assessments of a taxon with this name ...".
        Assert.Contains("Global assessments of IUCN ids 2790 and 2785, newest first.", text);
        Assert.Contains("IUCN id 2790 This page (Bettongia penicillata): in Red List version 2026-1; 1 global assessment, published 2015.", text);
        Assert.Contains("IUCN id 2785 (Bettongia penicillata): not in Red List version 2026-1; 1 global assessment, published 2008. "
            + "Same scientific name as IUCN id 2790.", text);
        Assert.DoesNotContain("Earlier assessments of a taxon with this name", text);
        Assert.Contains("aria-label=\"See IUCN id 2785 for the wikitext of its 2008 assessment\">See IUCN id 2785</a>", html);
        Assert.Contains("IUCN's taxonomic notes may explain why these assessments are under more than one IUCN id: see the 2015 assessment of "
            + "IUCN id 2790 and the 2008 assessment of IUCN id 2785 on the IUCN Red List website.", text);
        // Both newest assessments have taxonomic notes: links to them, the taxon in the release first.
        var notes = Regex.Match(html, "<p class=\"note taxonomic-notes\">.*?</p>", RegexOptions.Singleline).Value;
        var current = notes.IndexOf($"iucnredlist.org/species/{FixtureDb.Woylie}/{FixtureDb.WoylieLatest}\"", StringComparison.Ordinal);
        var old = notes.IndexOf($"iucnredlist.org/species/{FixtureDb.WoylieOld}/{FixtureDb.WoylieOld2008}\"", StringComparison.Ordinal);
        Assert.True(current > 0 && old > current, "both assessments with notes are linked, the current one first");
    }

    [Fact]
    public async Task OldIdPage_ShowsTheSameTableWithItsOwnRowsMarked() {
        var html = await Page(FixtureDb.WoylieOld);

        Assert.Equal(new[] { (FixtureDb.Woylie, FixtureDb.WoylieLatest), (FixtureDb.WoylieOld, FixtureDb.WoylieOld2008) }, Rows(html));
        Assert.Equal(" class=\"id-tint-1\"", RowClass(html, FixtureDb.Woylie, FixtureDb.WoylieLatest));
        Assert.Equal(" class=\"id-tint-2\"", RowClass(html, FixtureDb.WoylieOld, FixtureDb.WoylieOld2008));
        Assert.Contains($"{FixtureDb.WoylieOld}<span class=\"tag\">This page</span>", html);
        // Its own rows keep their wikitext links on this page; the current taxon's latest goes to its page.
        Assert.Contains($"href=\"/species/{FixtureDb.WoylieOld}?assessment={FixtureDb.WoylieOld2008}#wikitext\"", html);
        Assert.Contains($"href=\"/species/{FixtureDb.Woylie}?assessment={FixtureDb.WoylieLatest}#wikitext\"", html);
        // The status section still names the taxon in the release.
        Assert.Contains($"lists <span class=\"sci-name\"><i>Bettongia penicillata</i></span> under <a href=\"/species/{FixtureDb.Woylie}\">IUCN id {FixtureDb.Woylie}</a>.</p>", html);
    }

    // The visitor's citation options go along to the other id's page, with that page's own default
    // ref name for the assessment ("iucn2008" there too, so no refname in the link).
    [Fact]
    public async Task OtherIdWikitextLinksKeepTheOptions() {
        var html = await Page(FixtureDb.Woylie, "?authors=lastfirst&access=none");
        Assert.Contains($"href=\"/species/{FixtureDb.WoylieOld}?assessment={FixtureDb.WoylieOld2008}&amp;authors=lastfirst&amp;access=none#wikitext\"", html);

        var named = await Page(FixtureDb.Woylie, "?opts=1&ref=1&refname=woylie");
        Assert.Contains($"href=\"/species/{FixtureDb.WoylieOld}?assessment={FixtureDb.WoylieOld2008}&amp;opts=1&amp;ref=1&amp;refname=woylie#wikitext\"", named);
    }

    [Fact]
    public async Task SynonymLink_AddsANameColumn_AndSaysWhichNameIsTheSynonym() {
        var html = await Page(FixtureDb.Palmerae);

        Assert.Equal(new[] {
            (FixtureDb.Palmerae, FixtureDb.PalmeraeLatest),
            (FixtureDb.Palmerae, FixtureDb.Palmerae2008),
            (FixtureDb.Minuta, FixtureDb.Minuta2008),
        }, Rows(html));
        Assert.Contains("<th scope=\"col\">Name</th>", Section(html));
        // Only the newest assessment of Acropora palmerae has notes.
        var notes = Regex.Match(html, "<p class=\"note taxonomic-notes\">.*?</p>", RegexOptions.Singleline).Value;
        Assert.Contains($"iucnredlist.org/species/{FixtureDb.Palmerae}/{FixtureDb.PalmeraeLatest}\"", notes);
        Assert.DoesNotContain($"iucnredlist.org/species/{FixtureDb.Minuta}/", notes);
        Assert.Contains("IUCN id 133018 (Acropora minuta): not in Red List version 2026-1; 1 global assessment, published 2008. "
            + "IUCN lists Acropora minuta as a synonym of Acropora palmerae.", Html.Text(html));
        Assert.Contains("see the 2024 assessment of IUCN id 133531 on the IUCN Red List website.", Html.Text(html));

        var old = await Page(FixtureDb.Minuta);
        // The same synonym sentence, from the other side.
        Assert.Contains("IUCN id 133531 (Acropora palmerae): in Red List version 2026-1; 2 global assessments, published 2008 to 2024. "
            + "IUCN lists Acropora minuta as a synonym of Acropora palmerae.", Html.Text(old));
        Assert.Contains("IUCN lists Acropora minuta as a synonym of Acropora palmerae (IUCN id 133531).", Html.Text(old));
        Assert.Contains($"<p class=\"current-taxon\">IUCN lists <span class=\"sci-name\"><i>Acropora minuta</i></span> as a synonym of <span class=\"sci-name\"><i>Acropora palmerae</i></span>", old);
        Assert.Contains($"<a href=\"/species/{FixtureDb.Palmerae}\">IUCN id {FixtureDb.Palmerae}</a>", old);
        Assert.DoesNotContain("IUCN Red List version 2026-1 lists", Html.Text(old));
    }

    // An old id linked to two taxa in the release: three ids, three colours, taxa in the release
    // first; both status lines.
    [Fact]
    public async Task OldIdWithTwoLinks_ShowsThreeIds() {
        var html = await Page(FixtureDb.GangeticaOld);

        Assert.Equal(new[] {
            (FixtureDb.Minor, FixtureDb.MinorLatest),
            (FixtureDb.Gangetica, FixtureDb.GangeticaLatest),
            (FixtureDb.GangeticaOld, FixtureDb.GangeticaOld2012),
            (FixtureDb.GangeticaOld, FixtureDb.GangeticaOld1996),
        }, Rows(html));
        Assert.Equal(" class=\"id-tint-1\"", RowClass(html, FixtureDb.Gangetica, FixtureDb.GangeticaLatest));
        Assert.Equal(" class=\"id-tint-2\"", RowClass(html, FixtureDb.Minor, FixtureDb.MinorLatest));
        Assert.Equal(" class=\"id-tint-3\"", RowClass(html, FixtureDb.GangeticaOld, FixtureDb.GangeticaOld2012));
        Assert.Contains($"lists <span class=\"sci-name\"><i>Platanista gangetica</i></span> under <a href=\"/species/{FixtureDb.Gangetica}\">", html);
        Assert.Contains($"IUCN lists <span class=\"sci-name\"><i>Platanista gangetica</i></span> as a synonym of <span class=\"sci-name\"><i>Platanista minor</i></span>", html);
        // Each taxon in the release has a latest assessment, tagged in the table.
        Assert.Equal(2, Regex.Matches(Section(html), "<span class=\"tag\">Latest</span>").Count);
        Assert.Contains("Global assessments of IUCN ids 41756, 41757 and 41758, newest first.", Html.Text(html));
        // The page's own name is the synonym.
        Assert.Contains("IUCN id 41757 (Platanista minor): in Red List version 2026-1; 1 global assessment, published 2022. "
            + "IUCN lists Platanista gangetica as a synonym of Platanista minor.", Html.Text(html));
        // Its Asia assessment stays in its own Regional assessments table.
        Assert.Contains($"iucnredlist.org/species/{FixtureDb.GangeticaOld}/{FixtureDb.GangeticaOldAsia}\"", html);
        // Three assessments with taxonomic notes, in the order of the legend.
        Assert.Contains("see the 2022 assessment of IUCN id 41756, the 2022 assessment of IUCN id 41757 and the 2012 assessment of "
            + "IUCN id 41758 on the IUCN Red List website.", Html.Text(html));
    }

    // The page of a taxon in the release names the regional assessments of a linked old id.
    [Fact]
    public async Task LinkedIdWithRegionalAssessments_GetsALineUnderTheTable() {
        var html = await Page(FixtureDb.Gangetica);
        Assert.Equal(new[] { (FixtureDb.Gangetica, FixtureDb.GangeticaLatest), (FixtureDb.GangeticaOld, FixtureDb.GangeticaOld2012),
            (FixtureDb.GangeticaOld, FixtureDb.GangeticaOld1996) }, Rows(html));
        Assert.Contains($"<p class=\"note\">Regional assessments of <a href=\"/species/{FixtureDb.GangeticaOld}#regional\">IUCN id {FixtureDb.GangeticaOld}</a> are on its page.</p>", html);
        Assert.DoesNotContain($"iucnredlist.org/species/{FixtureDb.GangeticaOld}/{FixtureDb.GangeticaOldAsia}\"", html);
        Assert.DoesNotContain("id=\"regional\"", html);
    }

    // Clessiniola variabilis has no global assessment, but the old id linked to it has one.
    [Fact]
    public async Task TaxonWithNoGlobalAssessment_StillGetsTheOldIdsHistory() {
        var html = await Page(FixtureDb.Clessiniola);

        Assert.Equal(new[] { (FixtureDb.Turricaspia, FixtureDb.Turricaspia2011) }, Rows(html));
        Assert.Contains("No global assessment. This taxon has been assessed in ", Html.Text(html));
        Assert.Contains("IUCN id 212620635 This page (Clessiniola variabilis): in Red List version 2026-1; no global assessments.", Html.Text(html));
        // The Regional assessments table is still the taxon's own.
        Assert.Contains($"iucnredlist.org/species/{FixtureDb.Clessiniola}/{FixtureDb.ClessiniolaEurope}\"", html);
    }

    // Pupilla bigranata has only a Europe assessment: on Pupilla muscorum's page it adds no rows, so
    // that page keeps its own history and a line for the old id; on the old id's page the combined
    // table holds Pupilla muscorum's assessments.
    [Fact]
    public async Task OldIdWithNoGlobalAssessment_GetsALineOnTheCurrentPage() {
        var current = await Page(FixtureDb.PupillaMuscorum);
        Assert.Contains(">Assessment history</h2>", current);
        Assert.DoesNotContain("combined-table", current);
        Assert.Contains($"<p class=\"note earlier-id\">Earlier assessments of <span class=\"sci-name\"><i>Pupilla bigranata</i></span>", current);
        Assert.Contains($"<a href=\"/species/{FixtureDb.PupillaBigranata}\">IUCN id {FixtureDb.PupillaBigranata}</a>", current);
        Assert.Contains("Earlier assessments of Pupilla bigranata are under IUCN id 156831. IUCN lists Pupilla bigranata as a synonym of Pupilla muscorum.",
            Html.Text(current));

        var old = await Page(FixtureDb.PupillaBigranata);
        Assert.Equal(new[] { (FixtureDb.PupillaMuscorum, FixtureDb.PupillaMuscorumLatest) }, Rows(old));
        // Its Europe assessment stays in its own Regional assessments table.
        Assert.Contains($"iucnredlist.org/species/{FixtureDb.PupillaBigranata}/{FixtureDb.PupillaBigranataEurope}\"", old);
    }

    // A taxon with no links keeps the plain history.
    [Fact]
    public async Task TaxonWithNoLinks_HasNoCombinedHistory() {
        var html = await Page(FixtureDb.PolarBear);
        Assert.Contains(">Assessment history</h2>", html);
        Assert.DoesNotContain("combined", html);
        Assert.DoesNotContain("id-tint", html);
    }
}
