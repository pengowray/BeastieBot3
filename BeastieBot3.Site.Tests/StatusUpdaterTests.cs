using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

/// A site database in memory for the status updater.
internal sealed class FakeStatusLookup : IStatusLookup {
    private readonly Dictionary<long, StatusTaxon> _taxa = [];
    private readonly List<(string Key, long TaxonId, StatusNameKind Kind)> _names = [];

    public FakeStatusLookup Taxon(long id, string name, string? category, int? year = null, long? assessmentId = null,
        bool inRelease = true, long? current = null, bool pe = false, string? criteriaVersion = "3.1", string? citationJson = null, string? trend = null,
        string? populationSize = null, string? wikidataItem = null, string? itemProperties = null, int? node = null, string kind = TaxonKinds.Species) {
        AssessmentRow? latest = category is null ? null : new AssessmentRow(assessmentId ?? id * 10, id, "Global", true, category, pe, false,
            null, criteriaVersion, year, null, trend, citationJson, WikidataItemQid: wikidataItem, WikidataItemProperties: itemProperties,
            PopulationSize: populationSize);
        _taxa[id] = new StatusTaxon(id, name, inRelease, current, inRelease ? latest : null, kind, node);
        if (inRelease) {
            _names.Add((SiteNameKey.Fold(name), id, StatusNameKind.Scientific));
        }
        return this;
    }

    public FakeStatusLookup Synonym(long id, string name) {
        _names.Add((SiteNameKey.Fold(name), id, StatusNameKind.Synonym));
        return this;
    }

    /// The title of the taxon's English Wikipedia article.
    public FakeStatusLookup Article(long id, string title) {
        _names.Add((SiteNameKey.Fold(title), id, StatusNameKind.ArticleTitle));
        return this;
    }

    public FakeStatusLookup CommonName(long id, string name) {
        _names.Add((SiteNameKey.Fold(name), id, StatusNameKind.EnglishCommonName));
        return this;
    }

    private readonly Dictionary<long, string> _scopes = [];

    /// An assessment that is not the latest global one (an older global or a regional one).
    public FakeStatusLookup Assessment(long id, string scope) {
        _scopes[id] = scope;
        return this;
    }

    public string? AssessmentScope(long assessmentId) =>
        _scopes.TryGetValue(assessmentId, out var scope) ? scope
        : _taxa.Values.Any(t => t.LatestGlobal?.AssessmentId == assessmentId) ? "Global"
        : null;

    public int Lookups { get; private set; }

    public StatusTaxon? GetTaxon(long taxonId) {
        Lookups++;
        return _taxa.GetValueOrDefault(taxonId);
    }

    public IReadOnlyCollection<long> InReleaseTaxaWithName(string name, StatusNameKind kind) {
        Lookups++;
        var key = SiteNameKey.Fold(name);
        return _names.Where(n => n.Key == key && n.Kind == kind).Select(n => n.TaxonId).Distinct().ToList();
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

    private static StatusUpdateResult Run(string text, FakeStatusLookup? lookup = null, int max = StatusUpdater.DefaultMaxItems,
        StatusUpdateOptions? options = null) =>
        new StatusUpdater(lookup ?? Lookup(), Today, max, options).Update(text);

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
    public void TaxonIdWithoutAssessmentIdKeepsItsFormUnlessIdsAreAsked() {
        var plain = Run("{{IUCN status|VU|4828|1|year=2008}}");
        Assert.Equal("{{IUCN status|EN|4828|1|year=2015}}", plain.Text);
        Assert.Equal(1, plain.CountNotes(StatusNoteKind.AssessmentIdNotAdded));
        Assert.Equal("{{IUCN status|EN|4828/21289898|1|year=2015}}",
            Run("{{IUCN status|EN|4828|1|year=2015}}", options: new StatusUpdateOptions { AddIds = true }).Text);
    }

    [Fact]
    public void UnknownTaxonIdFallsBackToTheNameOnTheLine() {
        var result = Run("*[[Tiger]], ''Panthera tigris'' {{IUCN status|VU|22679798}}\n");
        Assert.Equal("*[[Tiger]], ''Panthera tigris'' {{IUCN status|EN|15955}}\n", result.Text);
        Assert.Contains(Assert.Single(result.Findings).Notes, n => n.Kind == StatusNoteKind.IdNotFoundMatchedByName);
    }

    [Fact]
    public void ATaxonIdOfAnotherTaxonThanTheNameOnTheLineIsLeftAsItIs() {
        // 4828 is the id of Amblysomus hottentotus; the line names Neamblysomus gunningi.
        var text = "*''Neamblysomus gunningi'' {{IUCN status|EN|4828}}\n";
        var result = Run(text);
        Assert.Equal(text, result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.Contains(finding.Notes, n => n.Kind == StatusNoteKind.IdOfAnotherTaxon && n.Id == 4828);
    }

    [Fact]
    public void AnIdCopiedToARowThatNamesNoKnownTaxonIsLeftAsItIs() {
        // The second line names Neamblysomus gunningi with Amblysomus hottentotus's id, so the id was
        // copied; the third line's name is not on the site, and its template keeps the copied id too.
        var text = "*''Amblysomus hottentotus'' {{IUCN status|VU|4828}}\n*''Neamblysomus gunningi'' {{IUCN status|VU|4828}}\n*''Unknownia nova'' {{IUCN status|VU|4828}}\n";
        var result = Run(text);
        Assert.Equal(text.Replace("{{IUCN status|VU|4828}}\n*''Neamblysomus", "{{IUCN status|EN|4828}}\n*''Neamblysomus"), result.Text);
        Assert.Equal([StatusOutcome.Updated, StatusOutcome.NotUpdated, StatusOutcome.NotUpdated], result.Findings.Select(f => f.Outcome));
        Assert.Equal(1, result.CountNotes(StatusNoteKind.IdUsedForOtherTaxa));
    }

    [Fact]
    public void AnIdOnARowThatNamesNoKnownTaxonIsUpdatedWhenNoOtherRowShowsItWasCopied() {
        var result = Run("*''Unknownia nova'' {{IUCN status|VU|4828}}\n");
        Assert.Equal("*''Unknownia nova'' {{IUCN status|EN|4828}}\n", result.Text);
    }

    [Fact]
    public void AbbreviatedNameOnAListLineTakesTheGenusLineAbove() {
        var result = Run("*** Genus: ''[[Panthera]]''\n**** [[Tiger]], ''P. tigris'' {{IUCN status|VU}}\n");
        Assert.Contains("{{IUCN status|EN}}", result.Text);
    }

    [Fact]
    public void SpeciesTableGenusIsTheLinkText() {
        var result = Run("{{Species table |genus=[[Panthera (genus)|Panthera]]}}\n{{Species table/row |binomial=P. tigris |iucn-status=VU}}");
        Assert.Contains("|iucn-status=EN}}", result.Text);
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
        Assert.Contains("| status = PE\n| status_system = IUCN3.1\n| status_ref = <ref name=\"iucn\">{{cite iucn |last1=Smith |first1=B.D. |year=2017 |title=''Lipotes vexillifer'' |volume=2017 |article-number=e.T12119A50358152 |doi=10.2305/IUCN.UK.2017-1.RLTS.T12119A50358152.en |access-date=1 September 2026}}</ref>\n| taxon",
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
        // One lookup per item checked, and one more for the id the first three items share, to see
        // whether it was copied (StatusUpdater.CopiedIds).
        Assert.Equal(4, lookup.Lookups);
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

    // ---------------------------------------------------------------- options

    private const string PeTable = "{| class=\"wikitable\"\n! Species !! IUCN status\n|-\n| ''Lipotes vexillifer'' || CR\n|}\n";

    [Fact]
    public void BareCrStaysForAPossiblyExtinctTaxonUnlessAsked() {
        var kept = Run(PeTable);
        Assert.Equal(PeTable, kept.Text);
        Assert.Equal(1, kept.CountNotes(StatusNoteKind.PossiblyExtinctKept));

        var changed = Run(PeTable, options: new StatusUpdateOptions { PossiblyExtinctCodes = true });
        Assert.Contains("| ''Lipotes vexillifer'' || CR(PE)\n", changed.Text);
        Assert.Equal(0, changed.CountNotes(StatusNoteKind.PossiblyExtinctKept));
    }

    [Fact]
    public void TemplateWithNoIdsGetsIdsAndYearOnlyWhenAsked() {
        const string table = "{| class=\"wikitable\"\n! Species !! IUCN status\n|-\n| ''Panthera tigris'' || {{IUCN status|VU}}\n|}\n";
        var plain = Run(table);
        Assert.Contains("{{IUCN status|EN}}", plain.Text);
        var notes = Assert.Single(plain.Findings).Notes;
        Assert.Contains(notes, n => n.Kind == StatusNoteKind.IdsNotAdded && n.Detail == "15955/214862019");
        Assert.Contains(notes, n => n.Kind == StatusNoteKind.YearNotAdded && n.Detail == "2022");

        var both = Run(table, options: new StatusUpdateOptions { AddIds = true, AddYear = true });
        Assert.Contains("{{IUCN status|EN|15955/214862019|1|year=2022}}", both.Text);
        var ids = Run(table, options: new StatusUpdateOptions { AddIds = true });
        Assert.Contains("{{IUCN status|EN|15955/214862019|1}}", ids.Text);
    }

    [Fact]
    public void TemplateWithIdsAndNoYearGetsAYearWhenAsked() {
        Assert.Equal("{{IUCN status|EN|4828/21289898|1}}", Run("{{IUCN status|VU|4828/111|1}}").Text);
        Assert.Equal("{{IUCN status|EN|4828/21289898|1|year=2015}}",
            Run("{{IUCN status|VU|4828/111|1}}", options: new StatusUpdateOptions { AddYear = true }).Text);
        // EX and EW have no year.
        Assert.Equal("{{IUCN status|EW|165247/5995954|1}}",
            Run("{{IUCN status|CR|165247/1|1}}", options: new StatusUpdateOptions { AddYear = true }).Text);
    }

    // ---------------------------------------------------------------- list lines

    [Fact]
    public void ListLineIsMatchedByTheScientificNameBeforeTheTemplate() {
        const string text = "***** [[Tiger]], ''Panthera tigris'' {{IUCN status|VU}} <ref>{{cite web |title=Felis tigris}}</ref>\n";
        var result = Run(text);
        Assert.Equal(text.Replace("{{IUCN status|VU}}", "{{IUCN status|EN}}"), result.Text);
        var finding = Assert.Single(result.Findings, f => f.Kind == StatusItemKind.ListLine);
        Assert.Equal(15955, finding.Taxon!.TaxonId);
    }

    [Fact]
    public void TemplateWithNoIdsOutsideAListOrTableIsLeft() {
        var result = Run("The tiger is {{IUCN status|VU}}.\n");
        Assert.Equal(StatusNoteKind.NoTaxonId, Assert.Single(Assert.Single(result.Findings).Notes).Kind);
    }

    // ---------------------------------------------------------------- {{Species table/row}}

    [Fact]
    public void SpeciesTableRowTakesTheGenusFromTheTableAbove() {
        const string text = """
            {{Species table |genus=[[Panthera]] |species-count=two}}
            {{Species table/row
            |name=[[Tiger]] |binomial=P. tigris
            |iucn-status=VU |population=3,000
            }}
            {{Species table/row
            |name=[[Unknown cat]] |binomial=P. nemo
            |iucn-status=LC
            }}
            {{Species table/end}}
            """;
        var result = Run(text);
        Assert.Contains("|iucn-status=EN |population=3,000", result.Text);
        Assert.Contains("|iucn-status=LC\n", result.Text);
        var rows = result.Findings.Where(f => f.Kind == StatusItemKind.SpeciesTableRow).ToList();
        Assert.Equal([StatusOutcome.Updated, StatusOutcome.NotUpdated], rows.Select(r => r.Outcome));
        Assert.Equal("|iucn-status=EN", rows[0].After);
    }

    [Fact]
    public void SpeciesTableRowWithNoGenusAboveIsLeft() {
        var result = Run("{{Species table/row |binomial=P. tigris |iucn-status=VU}}");
        Assert.Equal(StatusNoteKind.NoGenus, Assert.Single(Assert.Single(result.Findings).Notes).Kind);
    }

    [Fact]
    public void SpeciesTableRowKeepsCrForAPossiblyExtinctTaxonUnlessAsked() {
        const string text = "{{Species table |genus=Lipotes}}\n{{Species table/row |binomial=L. vexillifer |iucn-status=CR}}";
        Assert.Equal(text, Run(text).Text);
        Assert.Contains("|iucn-status=CR(PE)}}", Run(text, options: new StatusUpdateOptions { PossiblyExtinctCodes = true }).Text);
    }

    private static FakeStatusLookup TrendLookup(string? trend) =>
        new FakeStatusLookup().Taxon(15955, "Panthera tigris", "EN", 2022, 214862019, trend: trend);

    private static string TigerRow(string direction) =>
        "{{Species table |genus=[[Panthera]]}}\n{{Species table/row\n|binomial=P. tigris\n|iucn-status=EN |population=Unknown\n"
        + $"|direction={direction}\n}}}}";

    [Fact]
    public void SpeciesTableRowDirectionIsChangedAndItsReferenceKept() {
        var result = Run(TigerRow("{{steady|Population steady}}<ref name=\"IUCNTiger\"/>"), TrendLookup("Decreasing"));
        Assert.Equal(TigerRow("{{decrease|Population declining}}<ref name=\"IUCNTiger\"/>"), result.Text);
        var row = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.Updated, row.Outcome);
        Assert.Equal("|iucn-status=EN\n|direction={{steady|Population steady}}<ref name=\"IUCNTiger\"/>", row.Before);
        Assert.Equal("|iucn-status=EN\n|direction={{decrease|Population declining}}<ref name=\"IUCNTiger\"/>", row.After);
    }

    [Theory]
    [InlineData("Unknown", "{{Population change unknown}}")]
    [InlineData("Decreasing", "{{Down|Falling}}<ref name=\"x\"/>")]
    [InlineData("Increasing", "{{increase|Population increasing}}")]
    [InlineData("Stable", "{{steady}}")]
    public void SpeciesTableRowDirectionWithTheSameTrendIsKeptAsWritten(string trend, string direction) {
        var text = TigerRow(direction);
        var result = Run(text, TrendLookup(trend));
        Assert.Equal(text, result.Text);
        Assert.Equal(StatusOutcome.Current, Assert.Single(result.Findings).Outcome);
    }

    [Fact]
    public void SpeciesTableRowDirectionUnknownTrend() {
        var result = Run(TigerRow("{{decrease|Population declining}}"), TrendLookup("Unknown"));
        Assert.Contains("|direction={{population change unknown}}\n", result.Text);
    }

    [Fact]
    public void EmptySpeciesTableRowDirectionIsFilled() {
        var result = Run(TigerRow(""), TrendLookup("Stable"));
        Assert.Contains("|direction={{steady|Population steady}}\n", result.Text);
    }

    [Fact]
    public void SpeciesTableRowDirectionIsLeftWithNoTrendOrNoTemplate() {
        var noTrend = Run(TigerRow("{{decrease|Population declining}}"), TrendLookup(null));
        Assert.Equal(TigerRow("{{decrease|Population declining}}"), noTrend.Text);
        Assert.Equal(1, noTrend.CountNotes(StatusNoteKind.NoPopulationTrend));

        var text = TigerRow("Declining");
        var noTemplate = Run(text, TrendLookup("Decreasing"));
        Assert.Equal(text, noTemplate.Text);
        var note = Assert.Single(Assert.Single(noTemplate.Findings).Notes);
        Assert.Equal(StatusNoteKind.DirectionNotRecognised, note.Kind);
        Assert.Equal("{{decrease|Population declining}}", note.Detail);
    }

    [Theory]
    [InlineData("Unknown", "1000-1200", "1,000\u20131,200")]
    [InlineData("Unknown<ref name=\"x\"/>", "500000-999999,800000", "500,000\u2013999,999")]
    [InlineData("2,500", "U", "Unknown")]
    [InlineData("3,000", "2177", "2,177")]
    [InlineData("Unknown", "U", null)]
    [InlineData("Unknown", null, null)]
    [InlineData("8,000\u201310,000", "8000-10000", null)]
    [InlineData("8,000&ndash;10,000", "8000-10000", null)]
    [InlineData("2,500\u201310,000", "2500-9999", null)]
    [InlineData("2,500 to 5,000", "2500-9999,2500-5000", null)]
    [InlineData("800,000", "500000-999999,800000", null)]
    [InlineData("about 50", "0-1,.5", null)]
    [InlineData("2,200", "2177", null)]
    [InlineData("9,000\u201310,000", "8932-10208", null)]
    [InlineData("1,700\u20132,500", "1750-2450", null)]
    [InlineData("2,300\u20134,600", "2360-4560", "2,360\u20134,560")]
    [InlineData("10,000", "14000", "14,000")]
    [InlineData("11,200{{efn|Not counting farms.}}", "11158", null)]
    public void PopulationSuggestion(string current, string? iucn, string? suggested) =>
        Assert.Equal(suggested, PopulationValues.Suggest(current, iucn));

    [Fact]
    public void SpeciesTableRowPopulationIsListedAndNotChanged() {
        var lookup = new FakeStatusLookup().Taxon(15955, "Panthera tigris", "EN", 2022, 214862019, trend: "Increasing",
            populationSize: "2608-3905");
        var text = TigerRow("{{increase|Population increasing}}");
        var result = Run(text, lookup);
        Assert.Equal(text, result.Text);
        var p = Assert.Single(result.Populations);
        Assert.Equal(("Unknown", "2608-3905", 2022, "2,608\u20133,905", 4), (p.Current, p.IucnValue, p.Year, p.Suggested, p.Line));
    }

    // ---------------------------------------------------------------- {{cite iucn}}

    [Fact]
    public void CitationOfAnOlderAssessmentIsReplacedOnlyWhenAsked() {
        var parts = new IucnCitationParts {
            TaxonId = 4828, AssessmentId = 21289898, Year = 2015, ScientificName = "Amblysomus hottentotus",
            Authors = [new CitationAuthor(CitationAuthorKind.Person, "Bronner, G.", "Bronner", "G.")],
        };
        var lookup = new FakeStatusLookup().Taxon(4828, "Amblysomus hottentotus", "EN", 2015, 21289898, citationJson: parts.ToJson())
            .Assessment(111, "Global").Assessment(222, "Europe");
        const string text = "<ref>{{cite iucn |author=Old, A. |year=2008 |title=''Amblysomus hottentotus'' |article-number=e.T4828A111}}</ref>"
            + " <ref>{{cite iucn |year=2008 |article-number=e.T4828A222}}</ref>";

        var plain = Run(text, lookup);
        Assert.Equal(text, plain.Text);
        Assert.Equal(1, plain.CountNotes(StatusNoteKind.CitationOlder));
        // A citation of a regional assessment is not an item.
        Assert.Single(plain.Findings);

        var replaced = Run(text, lookup, options: new StatusUpdateOptions { UpdateCitations = true });
        Assert.Contains("|last1=Bronner |first1=G. |year=2015", replaced.Text);
        Assert.Contains("e.T4828A222}}</ref>", replaced.Text);
        Assert.Equal(StatusOutcome.Updated, Assert.Single(replaced.Findings).Outcome);
    }

    [Fact]
    public void CitationOfTheLatestAssessmentIsCurrent() {
        var result = Run("{{cite iucn |year=2015 |article-number=e.T4828A21289898}}");
        Assert.Equal(StatusOutcome.Current, Assert.Single(result.Findings).Outcome);
    }

    // ---------------------------------------------------------------- matching by other names

    [Fact]
    public void ASynonymMatchSaysWhichSynonym() {
        var result = Run("* ''Felis tigris'' {{IUCN status|VU}}\n");
        var finding = Assert.Single(result.Findings);
        Assert.Equal(15955, finding.Taxon!.TaxonId);
        Assert.Equal(new StatusNote(StatusNoteKind.MatchedBySynonym, "Felis tigris"), finding.Notes[0]);
    }

    [Fact]
    public void ACommonNameIsUsedOnlyWhenAsked() {
        var lookup = Lookup().CommonName(4828, "Giant golden mole").CommonName(15955, "Tiger").CommonName(15966, "Tiger");
        const string text = "{| class=\"wikitable\"\n! Name !! Status\n|-\n| ''Chrysospalax giganteus'', [[Giant golden mole]] || VU\n|}\n";

        var off = Assert.Single(Run(text, lookup).Findings);
        Assert.Equal(StatusOutcome.NotUpdated, off.Outcome);
        Assert.Equal(new StatusNote(StatusNoteKind.CommonNameNotUsed, "Giant golden mole"), off.Notes[0]);
        Assert.Equal(StatusNoteKind.NameNotFound, off.Notes[1].Kind);

        var on = Assert.Single(Run(text, lookup, options: new StatusUpdateOptions { MatchCommonNames = true }).Findings);
        Assert.Equal(StatusOutcome.Updated, on.Outcome);
        Assert.Equal(4828, on.Taxon!.TaxonId);
        Assert.Equal(new StatusNote(StatusNoteKind.MatchedByCommonName, "Giant golden mole"), on.Notes[0]);
    }

    [Fact]
    public void ACommonNameIsNotUsedForAnItemTheArticleGivesNE() {
        var lookup = Lookup().CommonName(4828, "Giant golden mole");
        var finding = Assert.Single(Run("* ''Chrysospalax novus'', [[Giant golden mole]] {{IUCN status|NE}}\n", lookup,
            options: new StatusUpdateOptions { MatchCommonNames = true }).Findings);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.DoesNotContain(finding.Notes, n => n.Kind is StatusNoteKind.CommonNameNotUsed or StatusNoteKind.MatchedByCommonName);
    }

    [Fact]
    public void ACommonNameOfTwoTaxaIsNotUsed() {
        var lookup = Lookup().CommonName(15955, "Striped cat").CommonName(15966, "Striped cat");
        var finding = Assert.Single(Run("* ''Tigris unknownus'', [[Striped cat]] {{IUCN status|VU}}\n", lookup,
            options: new StatusUpdateOptions { MatchCommonNames = true }).Findings);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.DoesNotContain(finding.Notes, n => n.Kind is StatusNoteKind.CommonNameNotUsed or StatusNoteKind.MatchedByCommonName);
    }

    [Fact]
    public void ARowWithNoKnownNameIsFoundByTheIucnCitationItUses() {
        const string text = """
            {{Species table/row
            |name=[[Giant golden mole]] |binomial=C. giganteus
            |iucn-status=VU |population=Unknown
            |direction={{population change unknown}}<ref name="IUCNmole"/>
            }}
            == References ==
            <ref name="IUCNmole">{{cite iucn |title=''Amblysomus hottentotus'' |article-number=e.T4828A21289898}}</ref>
            """;
        var finding = Assert.Single(Run(text).Findings, f => f.Kind == StatusItemKind.SpeciesTableRow);
        Assert.Equal(4828, finding.Taxon!.TaxonId);
        Assert.Equal(new StatusNote(StatusNoteKind.MatchedByCitation, "IUCNmole"), finding.Notes[0]);
    }

    [Fact]
    public void ACitationForAnotherClaimInTheRowIsNotUsed() {
        const string text = """
            {{Species table/row
            |name=[[Some mole]] |binomial=C. aliena
            |habitat=Forest<ref name="IUCNmole"/>
            |iucn-status=NE |population=Unknown
            |direction={{population change unknown}}
            }}
            <ref name="IUCNmole">{{cite iucn |article-number=e.T4828A21289898}}</ref>
            """;
        var finding = Assert.Single(Run(text).Findings, f => f.Kind == StatusItemKind.SpeciesTableRow);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.Null(finding.Taxon);
    }

    [Fact]
    public void AnAmbiguousNameIsSettledByTheRowsCitation() {
        // Ficus variegata is the name of taxa 500 and 501.
        var finding = Assert.Single(Run("* ''Ficus variegata'' {{IUCN status|LC}}<ref>{{cite iucn |article-number=e.T501A5011}}</ref>\n").Findings,
            f => f.Kind == StatusItemKind.ListLine);
        Assert.Equal(501, finding.Taxon!.TaxonId);
    }

    [Fact]
    public void ARowCitingTwoTaxaIsNotMatchedByCitation() {
        const string text = "* [[Unknown mole]] {{IUCN status|VU}}{{cite iucn |article-number=e.T4828A1}}{{cite iucn |article-number=e.T1087A1}}\n";
        var finding = Assert.Single(Run(text).Findings, f => f.Kind == StatusItemKind.ListLine);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
    }

    // ---------------------------------------------------------------- edit summary

    [Fact]
    public void EditSummaryNamesEachCategoryChange() {
        var result = Run("* {{IUCN status|VU|4828/111|1|year=2008}}\n* ''Panthera tigris'' {{IUCN status|EN}}\n* {{IUCN status|VU|1087/1|1}}\n");
        Assert.Equal("IUCN Red List 2026-1: Amblysomus hottentotus VU\u2192EN; 1 other IUCN status updated (ids, year, reference or trend) (assisted by Beastie Bot Species Status)",
            EditSummary.For(result, "2026-1"));
    }

    [Fact]
    public void EditSummaryCountsManyChangesByCategory() {
        var lookup = new FakeStatusLookup();
        var text = new System.Text.StringBuilder();
        for (var i = 1; i <= 40; i++) {
            lookup.Taxon(1000 + i, $"Genus speciesnumber{i}", i % 2 == 0 ? "EN" : "LC", 2020, 9000 + i);
            text.Append($"* {{{{IUCN status|VU|{1000 + i}/1|1|year=2008}}}}\n");
        }
        Assert.Equal("IUCN Red List: 40 IUCN statuses changed (20 to EN, 20 to LC) (assisted by Beastie Bot Species Status)",
            EditSummary.For(Run(text.ToString(), lookup), null));
    }

    [Fact]
    public void EditSummaryIsNullWhenNothingChanged() {
        Assert.Null(EditSummary.For(Run("* {{IUCN status|EN|4828/21289898|1|year=2015}}\n"), "2026-1"));
    }

    [Theory]
    [InlineData("{{IUCN status|EN|4828/1|1}}", "EN")]
    [InlineData("| status = CR\n| status_system = IUCN3.1", "CR")]
    [InlineData("| iucn-status = lr/nt", "lr/nt")]
    [InlineData(" VU ", "VU")]
    [InlineData("Panthera", null)]
    public void EditSummaryReadsTheCode(string text, string? code) => Assert.Equal(code, EditSummary.CodeIn(text));
}

public sealed class StatusTaxonResolverTests {
    private static readonly FakeStatusLookup Lookup = new FakeStatusLookup()
        .Taxon(500, "Ficus variegata", "LC", 2019, 5001)
        .Taxon(501, "Ficus variegata", "NT", 2020, 5011)
        .Taxon(15955, "Panthera tigris", "EN", 2022, 214862019)
        .Synonym(15955, "Felis tigris")
        .CommonName(15955, "Tiger");

    [Fact]
    public void ScientificNameFirstThenSynonymWithANote() {
        var resolver = new StatusTaxonResolver(Lookup, matchCommonNames: false);
        var byName = resolver.Resolve(["Panthera tigris"], null, null, false);
        Assert.Equal((15955L, (StatusNote?)null), (byName.Taxon!.TaxonId, byName.HowFound));
        var bySynonym = resolver.Resolve(["Felis tigris"], null, null, false);
        Assert.Equal(new StatusNote(StatusNoteKind.MatchedBySynonym, "Felis tigris"), bySynonym.HowFound);
    }

    [Fact]
    public void AnAmbiguousNameWithNoCitationFails() {
        var match = new StatusTaxonResolver(Lookup, matchCommonNames: true).Resolve(["Ficus variegata"], null, null, false);
        Assert.Null(match.Taxon);
        Assert.Equal(StatusNoteKind.NameAmbiguous, match.Failure!.Kind);
        Assert.Null(match.HowFound);
    }

    [Fact]
    public void ACommonNameIsOfferedWhenNotAskedFor() {
        var match = new StatusTaxonResolver(Lookup, matchCommonNames: false).Resolve(["Tiger"], null, null, false);
        Assert.Null(match.Taxon);
        Assert.Equal(new StatusNote(StatusNoteKind.CommonNameNotUsed, "Tiger"), match.HowFound);
        Assert.Null(new StatusTaxonResolver(Lookup, matchCommonNames: true).Resolve(["Tiger"], null, null, notEvaluated: true).Taxon);
    }
}
