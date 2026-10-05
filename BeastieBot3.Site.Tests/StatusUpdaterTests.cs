using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

/// A site database in memory for the status updater.
internal sealed class FakeStatusLookup : IStatusLookup {
    private readonly Dictionary<long, StatusTaxon> _taxa = [];
    private readonly List<(string Key, long TaxonId, bool Synonym)> _names = [];

    public FakeStatusLookup Taxon(long id, string name, string? category, int? year = null, long? assessmentId = null,
        bool inRelease = true, long? current = null, bool pe = false, string? criteriaVersion = "3.1", string? citationJson = null) {
        AssessmentRow? latest = category is null ? null : new AssessmentRow(assessmentId ?? id * 10, id, "Global", true, category, pe, false,
            null, criteriaVersion, year, null, null, citationJson);
        _taxa[id] = new StatusTaxon(id, name, inRelease, current, inRelease ? latest : null);
        if (inRelease) {
            _names.Add((SiteNameKey.Fold(name), id, false));
        }
        return this;
    }

    public FakeStatusLookup Synonym(long id, string name) {
        _names.Add((SiteNameKey.Fold(name), id, true));
        return this;
    }

    public int Lookups { get; private set; }

    public StatusTaxon? GetTaxon(long taxonId) {
        Lookups++;
        return _taxa.GetValueOrDefault(taxonId);
    }

    public IReadOnlyCollection<long> InReleaseTaxaWithName(string name, bool synonyms) {
        Lookups++;
        var key = SiteNameKey.Fold(name);
        return _names.Where(n => n.Key == key && n.Synonym == synonyms).Select(n => n.TaxonId).Distinct().ToList();
    }
}

public sealed class StatusUpdaterTests {
    private static readonly DateOnly Today = new(2026, 10, 5);

    private static FakeStatusLookup Lookup() => new FakeStatusLookup()
        .Taxon(4828, "Amblysomus hottentotus", "EN", 2015, 21289898)
        .Taxon(1087, "Neamblysomus gunningi", "VU", 2024, 99999)
        .Taxon(12119, "Lipotes vexillifer", "CR", 2017, 50358152, pe: true)
        .Taxon(165247, "Bromus interruptus", "EW", 2011, 5995954)
        .Taxon(2785, "Bettongia penicillata", "CR", inRelease: false, current: 2790)
        .Taxon(2790, "Bettongia penicillata", "CR", 2015, 2790001)
        .Taxon(15957, "Panthera pardus ssp. orientalis", "CR", inRelease: false)
        .Taxon(15955, "Panthera tigris", "EN", 2022, 214862019)
        .Taxon(15966, "Panthera tigris ssp. sumatrae", "CR", 2008, 136285)
        .Taxon(500, "Ficus variegata", "LC", 2019, 5001)
        .Taxon(501, "Ficus variegata", "NT", 2020, 5011)
        .Taxon(600, "Arrau turtle", "LR/cd", 1996, 6001, criteriaVersion: "2.3")
        .Taxon(601, "Podocnemis expansa", "LR/cd", 1996, 6011, criteriaVersion: "2.3")
        .Synonym(15955, "Felis tigris");

    private static StatusUpdateResult Run(string text, FakeStatusLookup? lookup = null, int max = StatusUpdater.DefaultMaxItems) =>
        new StatusUpdater(lookup ?? Lookup(), Today, max).Update(text);

    // ---------------------------------------------------------------- {{IUCN status}} with ids

    [Fact]
    public void StatusTemplateGetsLatestCodeIdsAndYear() {
        var result = Run("* [[Giant golden mole]] {{IUCN status|VU|4828/111|1|year=2008}}\n");
        Assert.Equal("* [[Giant golden mole]] {{IUCN status|EN|4828/21289898|1|year=2015}}\n", result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.Updated, finding.Outcome);
        Assert.Equal(1, finding.Line);
        Assert.Equal("{{IUCN status|VU|4828/111|1|year=2008}}", finding.Before);
        Assert.Equal("{{IUCN status|EN|4828/21289898|1|year=2015}}", finding.After);
        Assert.Equal(4828, finding.Taxon!.TaxonId);
    }

    [Fact]
    public void StatusTemplateKeepsSpacingCaseLabelAndOtherParameters() {
        var input = "{{iucn_status | vu | 1087/5 | 1 | label = 2008 | ref = x }}";
        var result = Run(input);
        // The code already matches ("vu" is VU to the template), so its case is kept.
        Assert.Equal("{{iucn_status | vu | 1087/99999 | 1 | label = 2024 | ref = x }}", result.Text);
    }

    [Fact]
    public void LabelThatIsNotAYearIsKept() {
        var result = Run("{{IUCN status|EN|1087/5|1|label=see note}}");
        Assert.Equal("{{IUCN status|VU|1087/99999|1|label=see note}}", result.Text);
    }

    [Fact]
    public void CurrentTemplateIsReportedAndUnchanged() {
        var input = "{{IUCN status|EN|4828/21289898|1|year=2015}}";
        var result = Run(input);
        Assert.Equal(input, result.Text);
        Assert.Equal(StatusOutcome.Current, Assert.Single(result.Findings).Outcome);
        Assert.Equal(1, result.Current);
    }

    [Fact]
    public void PossiblyExtinctGetsCrPe() {
        var result = Run("{{IUCN status|CR|12119/1|1|year=2008}}");
        Assert.Equal("{{IUCN status|CR(PE)|12119/50358152|1|year=2017}}", result.Text);
    }

    [Fact]
    public void ExtinctInTheWildLosesItsYear() {
        var result = Run("{{IUCN status|CR|165247/1|1|year=1998}} and {{IUCN status|CR|165247/1|1|label=1998}}");
        Assert.Equal("{{IUCN status|EW|165247/5995954|1}} and {{IUCN status|EW|165247/5995954|1}}", result.Text);
    }

    [Fact]
    public void TemplateWithoutYearGetsNoYear() {
        var result = Run("{{IUCN status|VU|1087/5|1}}");
        Assert.Equal("{{IUCN status|VU|1087/99999|1}}", result.Text);
    }

    [Fact]
    public void OldIdUsesTheCurrentTaxon() {
        var result = Run("{{IUCN status|EN|2785/6143|1|year=2008}}");
        Assert.Equal("{{IUCN status|CR|2790/2790001|1|year=2015}}", result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(2790, finding.Taxon!.TaxonId);
        Assert.Contains(finding.Notes, n => n.Kind == StatusNoteKind.UsedCurrentTaxon && n.Id == 2785);
    }

    [Theory]
    [InlineData("{{IUCN status|CR|15957/1|1|year=2008}}", StatusNoteKind.NotInRelease)]
    [InlineData("{{IUCN status|CR|7/1|1|year=2008}}", StatusNoteKind.TaxonNotFound)]
    [InlineData("{{IUCN status|CR|abc|1|year=2008}}", StatusNoteKind.BadTaxonId)]
    [InlineData("{{IUCN status|CR}}", StatusNoteKind.NoTaxonId)]
    public void TemplateNotUpdated(string input, StatusNoteKind reason) {
        var result = Run(input);
        Assert.Equal(input, result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.Equal(reason, Assert.Single(finding.Notes).Kind);
    }

    [Fact]
    public void TaxonIdWithoutAssessmentIdIsAccepted() {
        Assert.Equal("{{IUCN status|EN|4828/21289898|1|year=2015}}", Run("{{IUCN status|EN|4828|1|year=2015}}").Text);
    }

    [Fact]
    public void CommentsNowikiAndPreAreLeftAlone() {
        var input = "<!-- {{IUCN status|VU|4828/1|1|year=2008}} --><nowiki>{{IUCN status|VU|4828/1|1}}</nowiki>\n<pre>{{IUCN status|VU|4828/1}}</pre>";
        var result = Run(input);
        Assert.Equal(input, result.Text);
        Assert.Empty(result.Findings);
    }

    [Fact]
    public void CommentInsideAValueIsKept() {
        var result = Run("{{IUCN status|VU <!-- was NT -->|4828/1|1|year=2008}}");
        Assert.Equal("{{IUCN status|EN <!-- was NT -->|4828/21289898|1|year=2015}}", result.Text);
    }

    [Fact]
    public void TextOutsideTheReplacementsIsByteIdentical() {
        var input = "Intro with ünïcödé, CRLF\r\n{{Infobox|a=[[b|c]]|d={{e|f}}}}\r\n* {{IUCN status|VU|4828/1|1|year=2008}} tail\t \r\n"
            + "{{cite web |title=x}}\r\n* [[Neamblysomus gunningi]] {{IUCN status|EN|1087/2|1|year=2015}}\r\nend";
        var result = Run(input);
        var expected = input
            .Replace("{{IUCN status|VU|4828/1|1|year=2008}}", "{{IUCN status|EN|4828/21289898|1|year=2015}}")
            .Replace("{{IUCN status|EN|1087/2|1|year=2015}}", "{{IUCN status|VU|1087/99999|1|year=2024}}");
        Assert.Equal(expected, result.Text);
        Assert.Equal(2, result.Updated);
        Assert.Equal(new[] { 3, 5 }, result.Findings.Select(f => f.Line));
    }

    [Fact]
    public void NestedTemplatesInParametersDoNotSplitThem() {
        var result = Run("{{IUCN status|VU|4828/1|1|year=2008|note={{efn|a|b=[[x|y]]}}}}");
        Assert.Equal("{{IUCN status|EN|4828/21289898|1|year=2015|note={{efn|a|b=[[x|y]]}}}}", result.Text);
    }

    // ---------------------------------------------------------------- table cells

    private const string Table = """
        {| class="wikitable sortable"
        ! Common name !! Scientific name !! IUCN status
        |-
        | Giant golden mole || ''[[Amblysomus hottentotus]]'' || VU
        |-
        | Gunning's golden mole
        | ''Neamblysomus gunningi'' {{efn|Formerly ''[[Amblysomus gunningi]]''.}}
        | style="text-align:center" | {{IUCN status|EN}}
        |-
        | Tiger || {{sp|Panthera|tigris}} || EN
        |-
        | Baiji || [[Baiji|''Lipotes vexillifer'']] || CR
        |}
        """;

    [Fact]
    public void StatusCellsAreUpdatedByTheNameInTheRow() {
        var result = Run(Table);
        var expected = Table
            .Replace("|| ''[[Amblysomus hottentotus]]'' || VU", "|| ''[[Amblysomus hottentotus]]'' || EN")
            .Replace("| style=\"text-align:center\" | {{IUCN status|EN}}", "| style=\"text-align:center\" | {{IUCN status|VU}}");
        Assert.Equal(expected, result.Text);
        Assert.Equal(new[] { StatusOutcome.Updated, StatusOutcome.Updated, StatusOutcome.Current, StatusOutcome.Current },
            result.Findings.Select(f => f.Outcome));
        Assert.Equal(new[] { 4, 8, 10, 12 }, result.Findings.Select(f => f.Line));
        Assert.Equal("VU", result.Findings[0].Before);
        Assert.Equal("EN", result.Findings[0].After);
        // A bare CR stays CR for a possibly extinct taxon, with a note.
        Assert.Contains(result.Findings[3].Notes, n => n.Kind == StatusNoteKind.PossiblyExtinctKept);
    }

    [Fact]
    public void CellsOutsideAStatusColumnAreNotTouched() {
        var input = """
            {| class="wikitable"
            ! Name !! EPBC status !! Code
            |-
            | ''Amblysomus hottentotus'' || VU || VU
            |}
            """;
        var result = Run(input);
        Assert.Equal(input, result.Text);
        Assert.Empty(result.Findings);
    }

    [Fact]
    public void RowspanAndColspanShiftColumns() {
        var input = """
            {| class="wikitable"
            ! rowspan="2" | Name !! colspan="2" | Notes !! rowspan="2" | Status
            |-
            ! A !! B
            |-
            | ''Amblysomus hottentotus'' || x || y || VU
            |-
            | rowspan="2" | ''Neamblysomus gunningi'' || x || y || EN
            |-
            | x || y || LC
            |}
            """;
        var result = Run(input);
        Assert.Equal(new[] { "VU", "EN", "LC" }, result.Findings.Select(f => f.Before));
        Assert.Equal(new[] { "EN", "VU", "VU" }, result.Findings.Select(f => f.After));
        Assert.Contains("|| y || EN\n|-\n| rowspan", result.Text);
    }

    [Fact]
    public void AmbiguousMissingAndNoNames() {
        var input = """
            {| class="wikitable"
            ! Name !! Status
            |-
            | ''Ficus variegata'' || LC
            |-
            | ''Ficus nonexistens'' || LC
            |-
            | A fig || LC
            |-
            | ''Panthera tigris'' and ''Amblysomus hottentotus'' || LC
            |}
            """;
        var result = Run(input);
        Assert.Equal(input, result.Text);
        Assert.Equal(new[] { StatusNoteKind.NameAmbiguous, StatusNoteKind.NameNotFound, StatusNoteKind.NoName, StatusNoteKind.NameAmbiguous },
            result.Findings.Select(f => Assert.Single(f.Notes).Kind));
        Assert.All(result.Findings, f => Assert.Equal(StatusOutcome.NotUpdated, f.Outcome));
    }

    [Fact]
    public void SubspeciesAndSynonymNamesAreFound() {
        var input = """
            {| class="wikitable"
            ! Name !! Red List category
            |-
            | ''Panthera tigris sumatrae'' || EN
            |-
            | ''Felis tigris'' || VU
            |}
            """;
        var result = Run(input);
        Assert.Equal(new[] { "CR", "EN" }, result.Findings.Select(f => f.After));
        Assert.Equal(new long[] { 15966, 15955 }, result.Findings.Select(f => f.Taxon!.TaxonId));
    }

    [Fact]
    public void TemplateWithIdsInAStatusCellIsUpdatedOnce() {
        var input = """
            {| class="wikitable"
            ! Name !! Status
            |-
            | ''Ficus variegata'' || {{IUCN status|VU|4828/1|1|year=2008}}
            |}
            """;
        var result = Run(input);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusItemKind.StatusTemplate, finding.Kind);
        Assert.Contains("{{IUCN status|EN|4828/21289898|1|year=2015}}", result.Text);
    }

    // ---------------------------------------------------------------- taxoboxes

    private static string CitationJson(long taxonId, long assessmentId, int year, string name) => new IucnCitationParts {
        TaxonId = taxonId,
        AssessmentId = assessmentId,
        Year = year,
        ScientificName = name,
        Authors = [new CitationAuthor(CitationAuthorKind.Person, "Smith, B.D.", "Smith", "B.D.")],
        Doi = $"10.2305/IUCN.UK.{year}-1.RLTS.T{taxonId}A{assessmentId}.en",
        DownloadedAtUtc = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc),
    }.ToJson();

    private static FakeStatusLookup TaxoboxLookup() => new FakeStatusLookup()
        .Taxon(12119, "Lipotes vexillifer", "CR", 2017, 50358152, pe: true, citationJson: CitationJson(12119, 50358152, 2017, "Lipotes vexillifer"))
        .Taxon(4828, "Amblysomus hottentotus", "EN", 2015, 21289898)
        .Taxon(601, "Podocnemis expansa", "LR/cd", 1996, 6011, criteriaVersion: "2.3")
        .Taxon(15966, "Panthera tigris ssp. sumatrae", "CR", 2008, 136285);

    [Fact]
    public void SpeciesboxStatusSystemAndCitationAreUpdated() {
        var input = """
            {{Speciesbox
            | name = Baiji
            | status = EN
            | status_system = IUCN3.1
            | status_ref = <ref name="iucn">{{cite iucn |author=Smith, B.D. |year=2008 |title=''Lipotes vexillifer'' |volume=2008 |article-number=e.T12119A3322533 |access-date=1 May 2010}}</ref>
            | taxon = Lipotes vexillifer
            }}
            Body text.
            """;
        var result = Run(input, TaxoboxLookup());
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.Updated, finding.Outcome);
        Assert.Equal(3, finding.Line);
        Assert.Contains("| status = PE\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\">{{cite iucn |author=Smith, B.D. |year=2017 |title=''Lipotes vexillifer'' |volume=2017 |article-number=e.T12119A50358152 |doi=10.2305/IUCN.UK.2017-1.RLTS.T12119A50358152.en |access-date=1 September 2026}}</ref>\n| taxon",
            result.Text);
        Assert.EndsWith("}}\nBody text.", result.Text);
        Assert.Contains(finding.Notes, n => n.Kind == StatusNoteKind.CitationReplaced);
        Assert.StartsWith("| status = EN\n| status_system = IUCN3.1\n| status_ref = <ref", finding.Before);
        Assert.StartsWith("| status = PE\n", finding.After);
    }

    [Fact]
    public void CurrentCitationIsKept() {
        var input = """
            {{Speciesbox|taxon=Lipotes vexillifer|status=PE|status_system=IUCN3.1|status_ref=<ref>{{cite iucn|year=2017|doi=10.2305/IUCN.UK.2017-3.RLTS.T12119A50358152.en}}</ref>}}
            """;
        var result = Run(input, TaxoboxLookup());
        Assert.Equal(input, result.Text);
        Assert.Equal(StatusOutcome.Current, Assert.Single(result.Findings).Outcome);
    }

    [Fact]
    public void MissingStatusSystemIsAddedInTheStatusLineStyle() {
        var input = "{{Taxobox\n|status=VU\n|binomial=''Amblysomus hottentotus''\n}}";
        var result = Run(input, TaxoboxLookup());
        Assert.Equal("{{Taxobox\n|status=EN\n|status_system=IUCN3.1\n|binomial=''Amblysomus hottentotus''\n}}", result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Contains(finding.Notes, n => n.Kind == StatusNoteKind.StatusSystemAdded);
        Assert.Contains(finding.Notes, n => n.Kind == StatusNoteKind.NoStatusRef);
    }

    [Fact]
    public void LowerRiskUsesIucn23() {
        var input = "{{Speciesbox\n| genus = Podocnemis\n| species = expansa\n| status = VU\n| status_system = IUCN3.1\n}}";
        var result = Run(input, TaxoboxLookup());
        Assert.Equal("{{Speciesbox\n| genus = Podocnemis\n| species = expansa\n| status = LR/cd\n| status_system = IUCN2.3\n}}", result.Text);
    }

    [Fact]
    public void SubspeciesboxNameAndRefReuse() {
        var input = "{{Subspeciesbox\n| genus = Panthera\n| species = tigris\n| subspecies = sumatrae\n| status = EN\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\" />\n}}";
        var result = Run(input, TaxoboxLookup());
        Assert.Contains("| status = CR\n", result.Text);
        var note = Assert.Single(Assert.Single(result.Findings).Notes);
        Assert.Equal(StatusNoteKind.RefDefinedElsewhere, note.Kind);
        Assert.Equal("iucn", note.Detail);
    }

    [Theory]
    [InlineData("{{Speciesbox|taxon=Lipotes vexillifer|status=EN|status_system=EPBC}}", StatusNoteKind.OtherStatusSystem)]
    [InlineData("{{Speciesbox|taxon=Lipotes vexillifer|status=fossil}}", StatusNoteKind.UnknownStatusCode)]
    [InlineData("{{Speciesbox|taxon=Nonexistent species|status=EN|status_system=IUCN3.1}}", StatusNoteKind.NameNotFound)]
    [InlineData("{{Speciesbox|name=Baiji|status=EN|status_system=IUCN3.1}}", StatusNoteKind.NoName)]
    public void TaxoboxNotUpdated(string input, StatusNoteKind reason) {
        var result = Run(input, TaxoboxLookup());
        Assert.Equal(input, result.Text);
        Assert.Equal(reason, Assert.Single(Assert.Single(result.Findings).Notes).Kind);
    }

    [Fact]
    public void TaxoboxWithoutStatusIsNotAnItem() {
        Assert.Empty(Run("{{Speciesbox|taxon=Lipotes vexillifer}}", TaxoboxLookup()).Findings);
    }

    // ---------------------------------------------------------------- the cap

    [Fact]
    public void OnlyTheFirstItemsAreChecked() {
        var input = string.Concat(Enumerable.Repeat("* {{IUCN status|VU|4828/1|1|year=2008}}\n", 5));
        var lookup = Lookup();
        var result = Run(input, lookup, max: 3);
        Assert.Equal(3, result.Findings.Count);
        Assert.Equal(2, result.NotChecked);
        Assert.Equal(3, result.Text.Split("4828/21289898").Length - 1);
        Assert.Equal(2, result.Text.Split("4828/1|").Length - 1);
        Assert.Equal(3, lookup.Lookups);
    }

    [Fact]
    public void DefaultCapIsTheListCap() => Assert.Equal(3600, StatusUpdater.DefaultMaxItems);

    // 2 MB texts made to be slow to read: many taxoboxes and templates, one template with many
    // named parameters, braces that never close, and a long wikitable.
    [Theory]
    [InlineData("{{Taxobox|status=}}{{IUCN status}}")]
    [InlineData("|a=")]
    [InlineData("{{")]
    [InlineData("{|\n! Name !! Status\n|-\n| ''Ursus imaginarius'' || EN\n")]
    public void LargeTextsAreReadQuickly(string unit) {
        var text = new System.Text.StringBuilder("{{x");
        while (text.Length < 2 * 1024 * 1024) {
            text.Append(unit);
        }
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var result = Run(text.ToString());
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"{watch.Elapsed} for {unit}");
        Assert.True(result.Findings.Count <= StatusUpdater.DefaultMaxItems);
    }

    [Fact]
    public void StatusSystemLineKeepsCrlf() {
        var result = Run("{{Taxobox\r\n|status=VU\r\n|binomial=Amblysomus hottentotus\r\n}}", TaxoboxLookup());
        Assert.Equal("{{Taxobox\r\n|status=EN\r\n|status_system=IUCN3.1\r\n|binomial=Amblysomus hottentotus\r\n}}", result.Text);
    }

    // ---------------------------------------------------------------- helpers

    [Theory]
    [InlineData("IUCN status", true)]
    [InlineData("[[IUCN Red List|Status]]", true)]
    [InlineData("Conservation status", true)]
    [InlineData("Status<ref>x</ref>", true)]
    [InlineData("EPBC status", false)]
    [InlineData("CITES status", false)]
    [InlineData("Name", false)]
    public void StatusHeaders(string header, bool expected) => Assert.Equal(expected, StatusUpdater.IsStatusHeader(header));

    [Theory]
    [InlineData("EN", "EN")]
    [InlineData("lr/nt", "LR/nt")]
    [InlineData("CR (PE)", "CR(PE)")]
    [InlineData("Endangered", null)]
    [InlineData("", null)]
    public void BareCodes(string text, string? expected) => Assert.Equal(expected, StatusUpdater.BareCode(text));
}
