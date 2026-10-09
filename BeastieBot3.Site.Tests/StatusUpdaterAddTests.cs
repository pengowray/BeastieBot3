using BeastieBot3.Site.Display;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

/// Statuses added where the text has none: on list lines and as a new table column.
public sealed class StatusUpdaterAddTests {
    private static readonly DateOnly Today = new(2026, 10, 6);

    private static FakeStatusLookup Lookup() => new FakeStatusLookup()
        .Taxon(4828, "Amblysomus hottentotus", "EN", 2015, 21289898)
        .Taxon(1087, "Neamblysomus gunningi", "VU", 2024, 99999)
        .Taxon(15955, "Panthera tigris", "EN", 2022, 214862019)
        .Taxon(165247, "Bromus interruptus", "EW", 2011, 5995954)
        .Taxon(700, "Felis silvestris", "LC", 2022, 7001)
        .Taxon(800, "Regional only", null)
        .Synonym(15955, "Felis tigris")
        .Article(15955, "Tiger")
        .Article(700, "Wildcat")
        .CommonName(4828, "Giant golden mole");

    private static readonly StatusUpdateOptions Lines = new() { AddToListLines = true };
    private static readonly StatusUpdateOptions Columns = new() { AddStatusColumns = true };

    private static StatusUpdateResult Run(string text, StatusUpdateOptions? options = null, int max = StatusUpdater.DefaultMaxItems) =>
        new StatusUpdater(Lookup(), Today, max, options).Update(text);

    // ---------------------------------------------------------------- list lines

    [Fact]
    public void ListLinesAreOnlyCountedWhenTheOptionIsOff() {
        var input = "* [[Tiger]], ''Panthera tigris'', found in Asia\n* ''[[Amblysomus hottentotus]]''\n";
        var result = Run(input);
        Assert.Equal(input, result.Text);
        Assert.Empty(result.Findings);
        Assert.Equal(2, result.ListLinesWithoutStatus);
    }

    [Fact]
    public void StatusGoesAfterTheScientificName() {
        var result = Run("* [[Tiger]], ''Panthera tigris'', found in Asia<ref>x</ref>\n# ''[[Amblysomus hottentotus]]'' (Broom, 1907) – a golden mole\n", Lines);
        Assert.Equal("* [[Tiger]], ''Panthera tigris'' {{IUCN status|EN}}, found in Asia<ref>x</ref>\n"
            + "# ''[[Amblysomus hottentotus]]'' (Broom, 1907) {{IUCN status|EN}} – a golden mole\n", result.Text);
        Assert.All(result.Findings, f => Assert.Equal(StatusItemKind.ListLineAdded, f.Kind));
        Assert.Equal(new[] { 1, 2 }, result.Findings.Select(f => f.Line));
        Assert.Equal("* [[Tiger]], ''Panthera tigris'' {{IUCN status|EN}}, found in Asia<ref>x</ref>", result.Findings[0].After);
        Assert.Equal(0, result.ListLinesWithoutStatus);
    }

    [Fact]
    public void AddedTemplateHasIdsAndYearWhenAsked() {
        var result = Run("* ''Panthera tigris''\n* ''Bromus interruptus''\n", Lines with { AddIds = true, AddYear = true });
        Assert.Equal("* ''Panthera tigris'' {{IUCN status|EN|15955/214862019|1|year=2022}}\n"
            + "* ''Bromus interruptus'' {{IUCN status|EW|165247/5995954|1}}\n", result.Text);
    }

    [Theory]
    [InlineData("* ''Felis catus'' and ''Felis silvestris''")]       // two names: the line is about two taxa
    [InlineData("* ''Panthera tigris'' {{IUCN status|EN}}")]          // already has a status template
    [InlineData("* ''Homo erectus''")]                                // no taxon has the name
    [InlineData("* [[List of felids]]")]                              // no scientific name
    [InlineData(": ''Panthera tigris''")]                             // not a "*" or "#" line
    [InlineData("{{Navbox|list=\n* ''Panthera tigris''\n}}")]         // inside a template
    [InlineData("<!--\n* ''Panthera tigris''\n-->")]                  // inside a comment
    public void LinesThatAreNotGivenAStatus(string input) {
        var result = Run(input, Lines);
        Assert.DoesNotContain(result.Findings, f => f.Kind == StatusItemKind.ListLineAdded);
        Assert.Equal(0, Run(input).ListLinesWithoutStatus);
    }

    [Fact]
    public void StatusGoesAfterASmallAuthority() {
        var result = Run("*''[[Panthera tigris]]'' {{small|(L.)}} – Asia\n*''Felis silvestris'' <small>Schreber</small>\n", Lines);
        Assert.Equal("*''[[Panthera tigris]]'' {{small|(L.)}} {{IUCN status|EN}} – Asia\n*''Felis silvestris'' <small>Schreber</small> {{IUCN status|LC}}\n",
            result.Text);
    }

    [Theory]
    [InlineData("* ''Panthera tigris'' Kosterm. – Asia", "* ''Panthera tigris'' Kosterm. {{IUCN status|EN}} – Asia")]
    [InlineData("* ''Panthera tigris'' (C.K.Allen) Kosterm. – Asia", "* ''Panthera tigris'' (C.K.Allen) Kosterm. {{IUCN status|EN}} – Asia")]
    [InlineData("* ''Panthera tigris'' Brown & Wright, 1978", "* ''Panthera tigris'' Brown & Wright, 1978 {{IUCN status|EN}}")]
    [InlineData("* ''Panthera tigris'' L.", "* ''Panthera tigris'' L. {{IUCN status|EN}}")]
    [InlineData("* ''Panthera tigris'' – Asia and Russia", "* ''Panthera tigris'' {{IUCN status|EN}} – Asia and Russia")]
    [InlineData("* ''Panthera tigris'', a big cat", "* ''Panthera tigris'' {{IUCN status|EN}}, a big cat")]
    [InlineData("* ''Panthera tigris'' Tiger of Asia", "* ''Panthera tigris'' {{IUCN status|EN}} Tiger of Asia")]
    public void StatusGoesAfterAPlainAuthority(string line, string expected) {
        Assert.Equal(expected + "\n", Run(line + "\n", Lines).Text);
    }

    [Fact]
    public void TheStatusCanGoAtTheEndOfTheLine() {
        var result = Run("* ''Panthera tigris'' – Asia<ref>x</ref>\n", Lines with { StatusAtLineEnd = true });
        Assert.Equal("* ''Panthera tigris'' – Asia {{IUCN status|EN}}<ref>x</ref>\n", result.Text);
    }

    [Fact]
    public void AReferenceIsAddedOrReused() {
        var text = "* ''Panthera tigris''\n* ''Felis silvestris''\n<ref name=\"wildcat\">{{cite iucn |doi=10.2305/IUCN.UK.2022.RLTS.T700A7001.en}}</ref>\n";
        var result = Run(text, Lines with { AddReferences = true });
        // The tiger has no citation on this fake site; the wildcat's assessment is cited by "wildcat".
        Assert.StartsWith("* ''Panthera tigris'' {{IUCN status|EN}}\n* ''Felis silvestris'' {{IUCN status|LC}}<ref name=\"wildcat\"/>\n", result.Text);
    }

    [Fact]
    public void ANewColumnCanHaveAnotherHeadingAndLeaveTablesOut() {
        var result = Run(InlineTable, Columns with { ColumnHeader = "Conservation status" });
        Assert.Contains("! Common name !! Scientific name !! Conservation status !! Range", result.Text);
        var none = Run(InlineTable, Columns with { ColumnTables = new HashSet<int>() });
        Assert.Equal(InlineTable, none.Text);
        Assert.Equal(StatusNoteKind.ColumnNotChosen, Assert.Single(none.Findings).Notes.Single().Kind);
        Assert.NotEqual(InlineTable, Run(InlineTable, Columns with { ColumnTables = new HashSet<int> { 2 } }).Text);
    }

    [Fact]
    public void AStatusCellWithAReferenceIsUpdated() {
        var text = "{|\n! Name !! IUCN status\n|-\n| ''Panthera tigris'' || VU<ref name=\"a\"/>\n|}";
        Assert.Equal("{|\n! Name !! IUCN status\n|-\n| ''Panthera tigris'' || EN<ref name=\"a\"/>\n|}", Run(text).Text);
    }

    [Fact]
    public void ListsInReferenceSectionsAndExternalLinksAreLeftOut() {
        var input = "== Species ==\n* ''Panthera tigris''\n== External links ==\n* ''Felis silvestris''\n=== Sub ===\n* ''Felis silvestris''\n"
            + "== Other ==\n* [https://example.org/x Profile: ''Neamblysomus gunningi'']\n";
        var result = Run(input, Lines);
        Assert.Equal(15955, Assert.Single(result.Findings).Taxon!.TaxonId);
    }

    [Fact]
    public void AnAbbreviatedNameTakesTheGenusFromAGenusLineWithoutALink() {
        var result = Run("* '''Genus ''Panthera'''''\n** [[Tiger]] (''P. tigris'') – Asia\n", Lines);
        Assert.Equal("* '''Genus ''Panthera'''''\n** [[Tiger]] (''P. tigris'') {{IUCN status|EN}} – Asia\n", result.Text);
    }

    [Fact]
    public void InlineRowsWithoutSpacesGetASpaceBeforeTheNextSeparator() {
        var result = Run("{|\n!Name!!Range\n|-\n|''Panthera tigris''||Asia\n|-\n|''Felis silvestris''||Europe\n|-\n|''Neamblysomus gunningi''||Africa\n|}", Columns);
        Assert.Contains("!Name !! IUCN status !!Range", result.Text);
        Assert.Contains("|''Panthera tigris'' || {{IUCN status|EN}} ||Asia", result.Text);
    }

    [Fact]
    public void SynonymSectionsAreLeftOut() {
        var result = Run("==Synonyms==\n* ''Felis tigris'' Linnaeus, 1758\n", Lines);
        Assert.Empty(result.Findings);
    }

    [Fact]
    public void LinesInsideAColumnsListTemplateCount() {
        var result = Run("{{columns-list|colwidth=30em|\n* ''Panthera tigris''\n* ''Felis silvestris''\n}}\n", Lines);
        Assert.Equal("{{columns-list|colwidth=30em|\n* ''Panthera tigris'' {{IUCN status|EN}}\n* ''Felis silvestris'' {{IUCN status|LC}}\n}}\n", result.Text);
    }

    [Fact]
    public void ALineWithOnlyALinkIsFoundByTheArticle() {
        var result = Run("* [[Tiger]] – Asia\n* [[wildcat|Wildcats]]\n* [[Giant golden mole]]\n* [[Wildcat]] and [[Tiger]]\n", Lines);
        Assert.Equal("* [[Tiger]] {{IUCN status|EN}} – Asia\n* [[wildcat|Wildcats]] {{IUCN status|LC}}\n* [[Giant golden mole]]\n"
            + "* [[Wildcat]] and [[Tiger]]\n", result.Text);
        Assert.Equal(new StatusNote(StatusNoteKind.MatchedByArticle, "Tiger"), result.Findings[0].Notes.Single());
    }

    [Theory]
    [InlineData("* Family [[Tiger|Pantherinae]] <small>Pocock, 1917</small>")]   // a rank line
    [InlineData("** Genus ''[[Tiger|Pantheroides]]''")]                         // a rank line
    [InlineData("* [[Tiger|Pantherinae]]")]                                     // a link labelled with a group name
    [InlineData("** ''Panthera tigris'' complex")]                              // a species group
    [InlineData("** ''Panthera tigris'' species group")]
    public void GroupLinesGetNoStatus(string line) {
        Assert.Equal(0, Run(line + "\n").ListLinesWithoutStatus);
    }

    [Fact]
    public void AbbreviatedNameInBracketsAfterALink() {
        var result = Run("* '''Genus ''Panthera'''''<ref name=msw3>x</ref> – big cats\n** [[Kashmir cat]] (''P. tigris'') – [[India]] and [[Pakistan]]\n", Lines);
        Assert.Equal("* '''Genus ''Panthera'''''<ref name=msw3>x</ref> – big cats\n** [[Kashmir cat]] (''P. tigris'') {{IUCN status|EN}} – [[India]] and [[Pakistan]]\n", result.Text);
    }

    [Fact]
    public void StatusGoesAfterBoldAroundALink() {
        var result = Run("* '''[[Tiger|the tiger]]''' of Asia\n", Lines);
        Assert.Equal("* '''[[Tiger|the tiger]]''' {{IUCN status|EN}} of Asia\n", result.Text);
    }

    [Theory]
    [InlineData("* ''[[Tiger|the tiger]]'' of Asia")]                 // a link in italics names a scientific name
    [InlineData("** [[Tiger|''P. tigris altaica'']], Amur tiger")]    // an italic label: a subspecies, not the article's species
    public void ItalicLinksDoNotFindTaxaByTheirArticle(string line) {
        Assert.Equal(0, Run(line + "\n").ListLinesWithoutStatus);
    }

    [Fact]
    public void ATableRowWithOnlyALinkIsFoundByTheArticle() {
        var result = Run("{|\n! Name !! Range\n|-\n| [[Tiger]] || Asia\n|-\n| [[Wildcat]] || Europe\n|-\n| ''Neamblysomus gunningi'' || Africa\n|}", Columns);
        Assert.Contains("| [[Tiger]] || {{IUCN status|EN}} || Asia", result.Text);
        Assert.Contains("| [[Wildcat]] || {{IUCN status|LC}} || Europe", result.Text);
    }

    [Fact]
    public void AStatusTemplateOnALineWithOnlyALinkIsFoundByTheArticle() {
        var result = Run("* [[Tiger]] {{IUCN status|VU}}\n");
        Assert.Equal("* [[Tiger]] {{IUCN status|EN}}\n", result.Text);
    }

    [Fact]
    public void ANameInAReferenceDoesNotCount() {
        var result = Run("* ''Panthera tigris''<ref>See ''Felis silvestris''.</ref>\n", Lines);
        Assert.Equal("* ''Panthera tigris'' {{IUCN status|EN}}<ref>See ''Felis silvestris''.</ref>\n", result.Text);
    }

    [Fact]
    public void ALineThatGivesAStatusAnotherWayIsReported() {
        var result = Run("* ''Panthera tigris'' (EN)\n* ''Amblysomus hottentotus'' [[File:Status iucn3.1 EN.svg|20px]]\n", Lines);
        Assert.Equal(new[] { StatusOutcome.NotUpdated, StatusOutcome.NotUpdated }, result.Findings.Select(f => f.Outcome));
        Assert.All(result.Findings, f => Assert.Equal(StatusNoteKind.StatusInText, Assert.Single(f.Notes).Kind));
        Assert.Equal("(EN)", result.Findings[0].Notes[0].Detail);
        Assert.Equal(0, Run("* ''Panthera tigris'' (EN)\n").ListLinesWithoutStatus);
    }

    [Fact]
    public void ATaxonWithoutAGlobalAssessmentGetsNoStatus() {
        var result = Run("* ''Regional only''x\n* ''Neamblysomus gunningi''\n", Lines);
        Assert.Equal("* ''Regional only''x\n* ''Neamblysomus gunningi'' {{IUCN status|VU}}\n", result.Text);
    }

    [Fact]
    public void ASynonymFindsTheTaxonWithANote() {
        var result = Run("* ''Felis tigris''\n", Lines);
        Assert.Equal("* ''Felis tigris'' {{IUCN status|EN}}\n", result.Text);
        Assert.Contains(Assert.Single(result.Findings).Notes, n => n.Kind == StatusNoteKind.MatchedBySynonym);
    }

    [Fact]
    public void CountedLinesDoNotTakeTheItemLimit() {
        var input = "{{IUCN status|EN|4828/1|1}}\n* ''Panthera tigris''\n{{IUCN status|EN|1087/1|1}}\n";
        var off = Run(input, max: 2);
        Assert.Equal(2, off.Findings.Count);
        Assert.Equal(0, off.NotChecked);
        Assert.Equal(1, off.ListLinesWithoutStatus);
        var on = Run(input, Lines, max: 2);
        Assert.Equal(1, on.NotChecked);
    }

    [Fact]
    public void CrlfLinesKeepTheirEndings() {
        var result = Run("* ''Panthera tigris''\r\n* ''Felis silvestris''\r\n", Lines);
        Assert.Equal("* ''Panthera tigris'' {{IUCN status|EN}}\r\n* ''Felis silvestris'' {{IUCN status|LC}}\r\n", result.Text);
        Assert.Equal("* ''Panthera tigris''", result.Findings[0].Before);
    }

    // ---------------------------------------------------------------- table columns

    private const string InlineTable = """
        {| class="wikitable sortable"
        ! Common name !! Scientific name !! Range
        |-
        | Tiger || ''[[Panthera tigris]]'' || Asia
        |-
        | Giant golden mole || ''Amblysomus hottentotus'' || South Africa
        |-
        | Somebody || ''Homo erectus'' || Africa
        |-
        | Wildcat || ''Felis silvestris'' || Europe
        |}
        """;

    [Fact]
    public void TablesAreOnlyCountedWhenTheOptionIsOff() {
        var result = Run(InlineTable);
        Assert.Equal(InlineTable, result.Text);
        Assert.Empty(result.Findings);
        Assert.Equal(1, result.TablesWithoutStatus);
    }

    [Fact]
    public void ColumnGoesAfterTheScientificNames() {
        var result = Run(InlineTable, Columns);
        var expected = InlineTable
            .Replace("! Common name !! Scientific name !! Range", "! Common name !! Scientific name !! IUCN status !! Range")
            .Replace("''[[Panthera tigris]]'' || Asia", "''[[Panthera tigris]]'' || {{IUCN status|EN}} || Asia")
            .Replace("''Amblysomus hottentotus'' || South Africa", "''Amblysomus hottentotus'' || {{IUCN status|EN}} || South Africa")
            .Replace("''Homo erectus'' || Africa", "''Homo erectus'' || || Africa")
            .Replace("''Felis silvestris'' || Europe", "''Felis silvestris'' || {{IUCN status|LC}} || Europe");
        Assert.Equal(expected, result.Text);
        Assert.Equal(new[] { StatusItemKind.TableColumnAdded, StatusItemKind.TableRowAdded, StatusItemKind.TableRowAdded,
            StatusItemKind.TableRowAdded, StatusItemKind.TableRowAdded }, result.Findings.Select(f => f.Kind));
        var header = result.Findings[0];
        Assert.Equal(new StatusNote(StatusNoteKind.ColumnAdded, "3/4", 2), Assert.Single(header.Notes));
        Assert.Equal("Added an IUCN status column after column 2. 3 of 4 rows have a status.", UpdateText.Note(header.Notes[0], header.Kind));
        var missing = result.Findings[3];
        Assert.Equal(StatusOutcome.NotUpdated, missing.Outcome);
        Assert.Contains(missing.Notes, n => n.Kind == StatusNoteKind.EmptyCellAdded);
        Assert.Equal("''[[Panthera tigris]]'' || {{IUCN status|EN}}", result.Findings[1].After);
        Assert.Equal("IUCN Red List 2026-1: IUCN status column added (3 statuses) (assisted by Species Check)", EditSummary.For(result, "2026-1"));
    }

    [Fact]
    public void OneCellPerLineRowsGetANewLine() {
        var input = "{|\n! Name\n! Notes\n|-\n| ''Panthera tigris''\n| big\n|-\n| ''Felis silvestris''\n| small\n|-\n| ''Neamblysomus gunningi''\n| mole\n|}";
        var result = Run(input, Columns);
        Assert.Equal("{|\n! Name\n! IUCN status\n! Notes\n|-\n| ''Panthera tigris''\n| {{IUCN status|EN}}\n| big\n|-\n| ''Felis silvestris''\n"
            + "| {{IUCN status|LC}}\n| small\n|-\n| ''Neamblysomus gunningi''\n| {{IUCN status|VU}}\n| mole\n|}", result.Text);
    }

    [Fact]
    public void ColumnAtTheEndOfInlineAndCrlfRows() {
        var input = "{|\r\n! Notes !! Name\r\n|-\r\n| big || ''Panthera tigris''\r\n|-\r\n| small || ''Felis silvestris''\r\n|-\r\n| mole || ''Neamblysomus gunningi''\r\n|}";
        var result = Run(input, Columns);
        Assert.Equal("{|\r\n! Notes !! Name !! IUCN status\r\n|-\r\n| big || ''Panthera tigris'' || {{IUCN status|EN}}\r\n|-\r\n"
            + "| small || ''Felis silvestris'' || {{IUCN status|LC}}\r\n|-\r\n| mole || ''Neamblysomus gunningi'' || {{IUCN status|VU}}\r\n|}", result.Text);
    }

    [Theory]
    [InlineData("! Name !! IUCN status\n|-\n| ''Panthera tigris'' || \n|-\n| ''Felis silvestris'' || \n|-\n| ''Neamblysomus gunningi'' || ")]
    [InlineData("! Name !! Threat\n|-\n| ''Panthera tigris'' || EN\n|-\n| ''Felis silvestris'' || LC\n|-\n| ''Neamblysomus gunningi'' || VU")]
    [InlineData("! Name\n|-\n| ''Panthera tigris''\n|-\n| ''Felis silvestris''")]                                  // two rows only
    [InlineData("! Name\n|-\n| ''Panthera tigris''\n|-\n| ''Homo erectus''\n|-\n| ''Homo habilis''\n|-\n| x")]      // under half named
    public void TablesThatGetNoColumn(string rows) {
        var input = "{| class=\"wikitable\"\n" + rows + "\n|}";
        Assert.Equal(0, Run(input).TablesWithoutStatus);
        Assert.DoesNotContain(Run(input, Columns).Findings, f => f.Kind == StatusItemKind.TableColumnAdded);
    }

    [Theory]
    [InlineData("! Genus !! Species\n|-\n| rowspan=2 | ''Panthera'' || ''Panthera tigris''\n|-\n| ''Panthera leo''\n|-\n| ''Felis'' || ''Felis silvestris''\n|-\n| ''Amblysomus'' || ''Amblysomus hottentotus''",
        StatusUpdater.LayoutSpan, 4)]
    [InlineData("! Name !! Range\n|-\n| ''Panthera tigris'' || Asia\n|-\n| ''Felis silvestris''\n|-\n| ''Neamblysomus gunningi'' || Africa",
        "cells:1:2", 6)]
    [InlineData("! Name\n|-\n| ''Panthera tigris''\n|-\n! Moles\n|-\n| ''Amblysomus hottentotus''\n|-\n| ''Neamblysomus gunningi''",
        StatusUpdater.LayoutSecondHeader, 6)]
    [InlineData("| ''Panthera tigris''\n|-\n| ''Felis silvestris''\n|-\n| ''Neamblysomus gunningi''", StatusUpdater.LayoutNoHeader, 1)]
    public void ComplexTablesAreReportedAndLeftAsTheyAre(string rows, string cause, int line) {
        var input = "{|\n" + rows + "\n|}";
        Assert.Equal(1, Run(input).TablesWithoutStatus);
        var result = Run(input, Columns);
        Assert.Equal(input, result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal(StatusOutcome.NotUpdated, finding.Outcome);
        Assert.Equal(new StatusNote(StatusNoteKind.ColumnLayout, cause, line), Assert.Single(finding.Notes));
        Assert.DoesNotContain("ColumnLayout", UpdateText.Note(finding.Notes[0], finding.Kind));
    }

    // ---------------------------------------------------------------- found in review

    [Fact]
    public void ALinkDoesNotOverrideAScientificNameThatIsNotFound() {
        var table = "{|\n! Species !! Notes !! IUCN status\n|-\n| ''Panthera spelaea'' || related to the [[Tiger]] || EX\n|}";
        Assert.Equal(table, Run(table).Text);
        var line = "* ''Panthera zdanskyi'', a relative of the [[Tiger]] {{IUCN status|EX}}\n";
        Assert.Equal(line, Run(line).Text);
        Assert.Equal(0, Run("* †''Panthera spelaea'' (cave lion), related to the [[Tiger]]\n").ListLinesWithoutStatus);
    }

    [Fact]
    public void AStatusAtTheLineEndStaysOutOfAReferenceOnTheNextLines() {
        var result = Run("* ''Panthera tigris''<ref>{{cite web\n |title=x}}</ref>\n", Lines with { StatusAtLineEnd = true });
        Assert.Equal("* ''Panthera tigris'' {{IUCN status|EN}}<ref>{{cite web\n |title=x}}</ref>\n", result.Text);
    }

    [Fact]
    public void AStatusAtTheLineEndGoesBeforeTheEndOfAColumnsList() {
        var result = Run("{{columns-list|colwidth=20em|\n* ''Felis silvestris''\n* ''Panthera tigris''}}\n", Lines with { StatusAtLineEnd = true });
        Assert.Equal("{{columns-list|colwidth=20em|\n* ''Felis silvestris'' {{IUCN status|LC}}\n* ''Panthera tigris'' {{IUCN status|EN}}}}\n", result.Text);
    }

    [Fact]
    public void TheColumnGoesAfterTheScientificNamesNotAfterCommonNameLinks() {
        var text = "{|\n! Name !! Scientific name\n|-\n| [[Snow leopard]] || ''Panthera tigris''\n|-\n| [[Wild cat]] || ''Felis silvestris''\n"
            + "|-\n| [[Golden mole]] || ''Neamblysomus gunningi''\n|}";
        Assert.Contains("! Name !! Scientific name !! IUCN status", Run(text, Columns).Text);
    }

    [Fact]
    public void ACommentAtTheEndOfTheNameCellStaysInIt() {
        var text = "{|\n! Name !! Range\n|-\n| ''Panthera tigris'' <!-- c --> || a\n|-\n| ''Felis silvestris'' || b\n|-\n| ''Neamblysomus gunningi'' || c\n|}";
        Assert.Contains("| ''Panthera tigris'' <!-- c --> || {{IUCN status|EN}} || a", Run(text, Columns).Text);
    }

    [Fact]
    public void ManyTablesAndSectionsStayQuick() {
        var tables = string.Concat(Enumerable.Repeat("{|\n|a\n|}\n", 20_000));
        var sections = string.Concat(Enumerable.Repeat("==Notes==\n*a\n", 20_000));
        var watch = System.Diagnostics.Stopwatch.StartNew();
        Run(tables + sections, Lines with { AddStatusColumns = true });
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), watch.Elapsed.ToString());
    }

    // ---------------------------------------------------------------- round trip

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AddedStatusesAreUpToDateOnTheNextRun(bool idsAndYear) {
        var input = "Intro.\n* [[Tiger]], ''Panthera tigris'' (Linnaeus, 1758), Asia\n* ''Bromus interruptus''\n\n" + InlineTable + "\n";
        var options = new StatusUpdateOptions { AddToListLines = true, AddStatusColumns = true, AddIds = idsAndYear, AddYear = idsAndYear };
        var first = Run(input, options);
        Assert.Equal(5, first.Findings.Count(f => f.Outcome == StatusOutcome.Updated && f.Kind != StatusItemKind.TableColumnAdded));
        var second = Run(first.Text, options);
        Assert.Equal(first.Text, second.Text);
        Assert.Equal(0, second.Updated);
        Assert.Equal(5, second.Current);
        Assert.Equal(0, second.ListLinesWithoutStatus);
        Assert.Equal(0, second.TablesWithoutStatus);
        Assert.DoesNotContain(second.Findings, f => f.Kind is StatusItemKind.ListLineAdded or StatusItemKind.TableColumnAdded or StatusItemKind.TableRowAdded);
    }
}
