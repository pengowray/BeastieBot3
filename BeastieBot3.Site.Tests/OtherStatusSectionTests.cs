using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

// The species page's "Other conservation statuses" section as OtherStatusSection.Build makes it:
// which columns each table shows, the date column's heading, the text of each cell, and the notes.
public sealed class OtherStatusSectionTests {
    private static OtherStatusRow Row(string system, string source, string status = "Endangered", string? population = null,
        string? listedName = null, string? listedOn = null, string? url = null, string? statusCode = null, string? report = null) =>
        new(system, status, statusCode, listedName, population, source, "1", url, listedOn, report);

    private static readonly Dictionary<string, string> Downloaded = new() {
        ["sprat"] = "1 October 2026",
        ["ecos"] = "7 October 2026",
        ["natureserve"] = "8 October 2026",
        ["nztcs"] = "8 October 2026",
        ["salve"] = "8 October 2026",
    };

    private static OtherStatusSection Build(string kind, params OtherStatusRow[] rows) =>
        OtherStatusSection.Build(rows, kind, source => Downloaded.GetValueOrDefault(source));

    private static string Text(IReadOnlyList<NoteSegment> note) => string.Concat(note.Select(s => s.Text));

    private static (string Text, string Href)[] Links(IReadOnlyList<NoteSegment> note) =>
        note.Where(s => s.Href is not null).Select(s => (s.Text, s.Href!)).ToArray();

    // ------------------------------------------------------------ tables and columns

    [Fact]
    public void OneTablePerGroupInTheOrderOfTheRows() {
        var section = Build(TaxonKinds.Species,
            Row("au-epbc", "sprat"), Row("br-salve", "salve"), Row("ca-cosewic", "natureserve"), Row("nz-nztcs", "nztcs"),
            Row("us-esa", "ecos"), Row("natureserve-global", "natureserve", "G3", statusCode: "G3"), Row("zz-unknown", "sprat"));
        Assert.Equal(["Australia", "Brazil", "Canada", "New Zealand", "United States", "Global", ""], section.Tables.Select(t => t.Heading));
    }

    // ------------------------------------------------------------ NatureServe national and state ranks

    private static OtherStatusRow Local(string system, string country, string rank, string? place = null, string? qualifier = null) =>
        new(system, rank, rank, null, place, "natureserve", "1", null, null, Country: country, Qualifier: qualifier);

    [Fact]
    public void NationalRanksGoInTheirCountrysTableAndStateRanksUnderIt() {
        var section = Build(TaxonKinds.Species,
            Row("us-esa", "ecos"),
            Local("natureserve-national", "US", "N3"),
            Local("natureserve-subnational", "US", "S1", "Texas"),
            Local("natureserve-subnational", "US", "S4B,S5N", "Alabama"),
            Local("natureserve-subnational", "US", "SNA", "Hawaii", "exotic"),
            Local("natureserve-national", "CA", "N2"));
        Assert.Equal(["Canada", "United States"], section.Tables.Select(t => t.Heading));
        var us = section.Tables[1];
        Assert.Equal(["Endangered Species Act", "NatureServe national rank"], us.Rows.Select(r => r.ListLabel));
        Assert.Equal("Vulnerable", us.Rows[1].RankMeaning);
        var places = us.PlaceRanks!;
        Assert.Equal(["Alabama", "Hawaii", "Texas"], places.Rows.Select(p => p.Place));
        Assert.Equal("Apparently Secure (breeding); Secure (non-breeding)", places.Rows[0].Meaning);
        Assert.Equal("Not Applicable: introduced there", places.Rows[1].Meaning);
        Assert.Equal("Critically Imperiled", places.Rows[2].Meaning);
        Assert.Equal("NatureServe ranks for 3 states, Imperiled or worse in 1 of them", places.Summary);
        Assert.Equal("State", places.PlaceHeading);
        Assert.Null(section.Tables[0].PlaceRanks);
    }

    [Theory]
    [InlineData("S3", "S3", "3", "Any")]
    [InlineData("N5B,N5N", "N5", "5", "Breeding")]
    [InlineData("SNRN", "SNR", "NR", "Nonbreeding")]
    [InlineData("SHM", "SH", "H", "Migrant")]
    [InlineData("SZN", "SZ", "Z", "Nonbreeding")]
    public void NatureServeLocalRanksAreReadPartByPart(string rank, string firstRank, string firstCode, string firstSeason) {
        var parts = BeastieBot3.Shared.SiteData.OtherStatusSystems.NatureServeRankParts(rank);
        Assert.Equal(firstRank, parts[0].Rank);
        Assert.Equal(firstCode, parts[0].Code);
        Assert.Equal(firstSeason, parts[0].Season.ToString());
    }

    [Theory]
    [InlineData("G3")]
    [InlineData("SQ")]
    [InlineData("S3Q")]
    public void ARankThatIsNotANationalOrStateRankHasNoParts(string rank) =>
        Assert.Empty(BeastieBot3.Shared.SiteData.OtherStatusSystems.NatureServeRankParts(rank));

    [Fact]
    public void ATableShowsAColumnOnlyWhenOneOfItsRowsHasAValue() {
        var section = Build(TaxonKinds.Species,
            Row("au-epbc", "sprat", population: "combined populations of Qld, NSW and the ACT", listedOn: "2022-02-12"),
            Row("au-nsw", "sprat"),
            Row("nz-nztcs", "nztcs", "Introduced and Naturalised", report: "Birds 2021 (Robertson et al. 2021)"),
            Row("natureserve-global", "natureserve", "G5", listedName: "Casuarius casuarius johnsonii", statusCode: "G5"));

        var australia = section.Tables[0];
        Assert.True(australia.ShowAppliesTo);
        Assert.False(australia.ShowListedName);
        Assert.False(australia.ShowReport);
        Assert.Equal(OtherStatusDateHeading.InEffectFrom, australia.DateHeading);

        var newZealand = section.Tables[1];
        Assert.False(newZealand.ShowAppliesTo);
        Assert.False(newZealand.ShowListedName);
        Assert.True(newZealand.ShowReport);
        Assert.Null(newZealand.DateHeading);

        var global = section.Tables[2];
        Assert.False(global.ShowAppliesTo);
        Assert.True(global.ShowListedName);
        Assert.False(global.ShowReport);
        Assert.Null(global.DateHeading);
    }

    // ------------------------------------------------------------ the date column's heading

    [Theory]
    [InlineData(new[] { "ecos" }, "First listed")]
    [InlineData(new[] { "salve" }, "Assessed")]
    [InlineData(new[] { "sprat" }, "In effect from")]
    [InlineData(new[] { "natureserve" }, "In effect from")]
    [InlineData(new[] { "nztcs" }, "In effect from")]
    [InlineData(new[] { "mystery" }, "In effect from")]
    // An ECOS date keeps "First listed" whatever the other dates are; SALVE's "Assessed" needs every date to be SALVE's.
    [InlineData(new[] { "salve", "ecos" }, "First listed")]
    [InlineData(new[] { "sprat", "ecos" }, "First listed")]
    [InlineData(new[] { "salve", "sprat" }, "In effect from")]
    [InlineData(new[] { "salve", "mystery" }, "In effect from")]
    public void DateHeadingFollowsTheSourcesOfTheDates(string[] datedSources, string expected) {
        var heading = OtherStatusSection.DateHeadingFor(datedSources);
        Assert.Equal(expected, heading.Text);
        Assert.Equal(expected == "First listed" ? SiteText.ColOtherFirstListedTitle : null, heading.Title);
    }

    [Fact]
    public void OnlyTheRowsWithADateDecideTheHeading() {
        // SALVE's date and an undated NZTCS row: every date is SALVE's.
        var section = Build(TaxonKinds.Species,
            Row("zz-a", "salve", listedOn: "2019-09-09"),
            Row("zz-b", "nztcs", report: "Report X"));
        Assert.Equal(new OtherStatusDateHeading(SiteText.ColOtherAssessed), section.Tables[0].DateHeading);

        var mixed = Build(TaxonKinds.Species,
            Row("zz-a", "salve", listedOn: "2002-02-02"),
            Row("zz-b", "ecos", listedOn: "2001-01-01"));
        Assert.Equal(new OtherStatusDateHeading(SiteText.ColOtherFirstListed, SiteText.ColOtherFirstListedTitle), mixed.Tables[0].DateHeading);
    }

    // ------------------------------------------------------------ cells

    [Fact]
    public void DateCellGivesTheDateOrSaysItIsNotGiven() {
        var rows = Build(TaxonKinds.Species, Row("au-epbc", "sprat", listedOn: "2022-02-12"), Row("au-qld", "sprat")).Tables[0].Rows;
        Assert.Equal(new OtherStatusCell("12 February 2022"), rows[0].Date);
        Assert.Equal(new OtherStatusCell("not given", NoValue: true), rows[1].Date);
    }

    [Theory]
    [InlineData(TaxonKinds.Species, "whole species")]
    [InlineData(TaxonKinds.Subspecies, "whole subspecies")]
    [InlineData(TaxonKinds.Variety, "whole variety")]
    public void AppliesToGivesThePopulationElseTheWholeTaxon(string kind, string whole) {
        var rows = Build(kind, Row("au-epbc", "sprat", population: "Southern population"), Row("au-wa", "sprat")).Tables[0].Rows;
        Assert.Equal(new OtherStatusCell("Southern population"), rows[0].AppliesTo);
        Assert.Equal(new OtherStatusCell(whole), rows[1].AppliesTo);
    }

    // NatureServe does not say which populations its COSEWIC and SARA statuses apply to.
    [Fact]
    public void CanadianStatusWithNoPopulationSaysAppliesToIsNotGiven() {
        var rows = Build(TaxonKinds.Species,
            Row("ca-cosewic", "natureserve", population: "Atlantic population"),
            Row("ca-sara", "natureserve")).Tables[0].Rows;
        Assert.Equal(new OtherStatusCell("Atlantic population"), rows[0].AppliesTo);
        Assert.Equal(new OtherStatusCell("not given", NoValue: true), rows[1].AppliesTo);
    }

    [Fact]
    public void NatureServeRankHasAMeaningLineAndATitleForItsQualifiers() {
        var row = Build(TaxonKinds.Species, Row("natureserve-global", "natureserve", "G2?Q", statusCode: "G2")).Tables[0].Rows[0];
        Assert.Equal("G2?Q", row.Status);
        Assert.Equal("Imperiled (rounded rank G2)", row.RankMeaning);
        Assert.Equal("The question mark means the rank is uncertain. Q means the taxonomy is questionable.", row.StatusTitle);

        var subspecies = Build(TaxonKinds.Subspecies, Row("natureserve-global", "natureserve", "G5T2T3", statusCode: "T2")).Tables[0].Rows[0];
        Assert.Equal("Imperiled (rounded subspecies rank T2)", subspecies.RankMeaning);
        Assert.Null(subspecies.StatusTitle);

        var other = Build(TaxonKinds.Species, Row("us-esa", "ecos", "Threatened")).Tables[0].Rows[0];
        Assert.Null(other.RankMeaning);
        Assert.Null(other.StatusTitle);
    }

    [Fact]
    public void ListLabelIsAnAbbreviationAPlainNameOrANameWithANote() {
        var rows = Build(TaxonKinds.Species,
            Row("au-epbc", "sprat"), Row("au-qld", "sprat"), Row("ca-sara", "natureserve"), Row("zz-unknown", "sprat")).Tables
            .SelectMany(t => t.Rows).ToList();
        Assert.Equal(("EPBC Act", SiteText.EpbcFullName, true), (rows[0].ListLabel, rows[0].ListTitle, rows[0].ListIsAbbreviation));
        Assert.Equal(("Queensland", (string?)null, false), (rows[1].ListLabel, rows[1].ListTitle, rows[1].ListIsAbbreviation));
        Assert.Equal(("Species at Risk Act", "Canada's Species at Risk Act, the federal law", false), (rows[2].ListLabel, rows[2].ListTitle, rows[2].ListIsAbbreviation));
        Assert.Equal(("zz-unknown", (string?)null, false), (rows[3].ListLabel, rows[3].ListTitle, rows[3].ListIsAbbreviation));
    }

    [Fact]
    public void ListedNameIsWhollyItalicWhenPlainElseMarkedUpAsIucnNamesAre() {
        var rows = Build(TaxonKinds.Species,
            Row("au-epbc", "sprat", listedName: "Casuarius casuarius johnsonii"),
            Row("au-qld", "sprat", listedName: "Panthera leo ssp. senegalensis"),
            Row("au-wa", "sprat")).Tables[0].Rows;
        Assert.Equal("<i>Casuarius casuarius johnsonii</i>", rows[0].ListedNameHtml);
        Assert.Equal("<i>Panthera leo</i> ssp. <i>senegalensis</i>", rows[1].ListedNameHtml);
        Assert.Null(rows[2].ListedNameHtml);
    }

    [Fact]
    public void SourceLinkHasTheSourcesTextAndTheRecordsUrl() {
        var rows = Build(TaxonKinds.Species,
            Row("au-epbc", "sprat", url: "https://example.org/sprat"),
            Row("br-salve", "salve"),
            Row("ca-cosewic", "natureserve"),
            Row("nz-nztcs", "nztcs"),
            Row("us-esa", "ecos", url: "https://ecos.fws.gov/ecp/species/4958"),
            Row("zz-unknown", "mystery")).Tables.SelectMany(t => t.Rows).ToList();
        Assert.Equal(["SPRAT profile", "SALVE assessment", "NatureServe Explorer", "NZTCS assessment", "ECOS profile", "mystery"],
            rows.Select(r => r.SourceLinkText));
        Assert.Equal(["https://example.org/sprat", null, null, null, "https://ecos.fws.gov/ecp/species/4958", null], rows.Select(r => r.SourceUrl));
    }

    // ------------------------------------------------------------ notes

    [Fact]
    public void OneNotePerSourceInTheOrderTheSourcesFirstAppear_NoneForAnUnknownSource() {
        var section = Build(TaxonKinds.Species,
            Row("ca-cosewic", "natureserve"), Row("us-esa", "ecos"), Row("natureserve-global", "natureserve", "G3", statusCode: "G3"),
            Row("zz-unknown", "mystery"));
        Assert.Equal(2, section.Notes.Count);
        Assert.StartsWith("Canadian statuses and NatureServe global ranks are from ", Text(section.Notes[0]));
        Assert.StartsWith("United States statuses are from ECOS", Text(section.Notes[1]));
    }

    [Fact]
    public void NatureServeNoteLinksExplorerAndTheLicenceAndAddsTheCanadianSentence() {
        var note = Build(TaxonKinds.Species, Row("ca-cosewic", "natureserve"), Row("natureserve-global", "natureserve", "G3", statusCode: "G3")).Notes.Single();
        Assert.Equal(
            "Canadian statuses and NatureServe global ranks are from NatureServe Explorer (© NatureServe, CC BY 4.0), downloaded on 8 October 2026. "
            + "NatureServe's copy of the Canadian statuses may differ from Canada's Species at Risk Public Registry.",
            Text(note));
        Assert.Equal([("NatureServe Explorer", SiteText.NatureServeExplorerUrl), ("CC BY 4.0", SiteText.LicenceCcBy)], Links(note));
    }

    [Fact]
    public void NatureServeNoteForGlobalRanksOnlyWithNoDate() {
        var note = OtherStatusSection.Build([Row("natureserve-global", "natureserve", "G3", statusCode: "G3")], TaxonKinds.Species, _ => null).Notes.Single();
        Assert.Equal("NatureServe global ranks are from NatureServe Explorer (© NatureServe, CC BY 4.0).", Text(note));
    }

    [Fact]
    public void SpratAndEcosNotesAreTextOnly() {
        var notes = Build(TaxonKinds.Species, Row("au-epbc", "sprat"), Row("us-esa", "ecos")).Notes;
        Assert.Equal(SiteText.OtherStatusSpratNote("1 October 2026"), Text(notes[0]));
        Assert.Equal(SiteText.OtherStatusEcosNote("7 October 2026"), Text(notes[1]));
        Assert.All(notes, note => Assert.Empty(Links(note)));
    }

    [Fact]
    public void SalveAndNztcsNotesLinkTheirDatabases() {
        var notes = Build(TaxonKinds.Species, Row("br-salve", "salve"), Row("nz-nztcs", "nztcs")).Notes;
        Assert.Equal("Brazilian statuses are ICMBio's national assessments of Brazil's fauna, from SALVE (Sistema de Avaliação do Risco de Extinção da Biodiversidade), "
            + "downloaded on 8 October 2026. Brazil's official list of threatened species (Portaria MMA 148/2022) can differ.", Text(notes[0]));
        Assert.Equal([("SALVE", SiteText.SalveUrl)], Links(notes[0]));
        Assert.Equal("New Zealand statuses are from the New Zealand Threat Classification System database (Department of Conservation, CC BY 4.0), "
            + "downloaded on 8 October 2026.", Text(notes[1]));
        Assert.Equal([("New Zealand Threat Classification System database", SiteText.NztcsUrl), ("CC BY 4.0", SiteText.LicenceCcBy)], Links(notes[1]));
    }

    [Fact]
    public void EverySourceTheSiteDatabaseHasIsDescribed() {
        var sources = typeof(BeastieBot3.Shared.SiteData.OtherStatusSources)
            .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
        Assert.NotEmpty(sources);
        Assert.Equal(sources.Order(), OtherStatusSourceInfo.All.Select(s => s.Key).Order());
    }
}
