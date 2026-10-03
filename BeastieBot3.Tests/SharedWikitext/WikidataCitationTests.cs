using BeastieBot3.Shared.Wikitext;
using BeastieBot3.WikidataEdits;

namespace BeastieBot3.Tests.SharedWikitext;

// Pins {{cite Q}} and the QuickStatements v1 batches the public site offers for an assessment's
// Wikidata item: the statements and their order (the dry run's assessment item model), the
// value syntax QuickStatements' parser accepts, what an add batch leaves out, and the link form.
// The {{cite Q}} output was checked against en.wikipedia.org's parser (items Q56227924, Q56532172).
public class WikidataCitationTests {
    private static readonly WikidataItemModel Model = new();

    private static CitationAuthor Person(string last, string initials) => new(CitationAuthorKind.Person, $"{last}, {initials}", last, initials);

    // Sayer, C. & Lajus, D. 2023. Salmo salar Kola subpopulation, e.T196579964A196580951. IUCN's
    // citation has no DOI; the one here is made up in IUCN's pattern for the test.
    private static readonly IucnCitationParts Salmon = new() {
        TaxonId = 196579964,
        AssessmentId = 196580951,
        Year = 2023,
        ScientificName = "Salmo salar Kola subpopulation",
        SubpopulationName = "Kola",
        Authors = [Person("Sayer", "C."), Person("Lajus", "D.")],
        Doi = "10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.en",
        DoiSource = DoiSource.Citation,
    };

    private const string Tab = "\t";

    private static string L(params string[] columns) => string.Join(Tab, columns);

    // ------------------------------------------------------------ {{cite Q}}

    [Fact]
    public void CiteQ_Plain() {
        Assert.Equal("{{cite Q|Q56227924}}", WikidataCitation.CiteQ("Q56227924"));
    }

    [Fact]
    public void CiteQ_AccessDate_OnlyWhenTheItemHasAUrl() {
        var date = new DateOnly(2026, 10, 3);
        Assert.Equal("{{cite Q|Q56227924 |access-date=3 October 2026}}",
            WikidataCitation.CiteQ("Q56227924", new CiteQOptions { AccessDate = date, ItemHasUrl = true }));
        // CS1: "|access-date= requires |url=". Without P953 the date is left out.
        Assert.Equal("{{cite Q|Q56532172}}",
            WikidataCitation.CiteQ("Q56532172", new CiteQOptions { AccessDate = date, ItemHasUrl = false }));
    }

    [Fact]
    public void CiteQ_WrappedInRef_NameSanitisedLikeCiteIucn() {
        Assert.Equal("<ref>{{cite Q|Q1}}</ref>", WikidataCitation.CiteQ("Q1", new CiteQOptions { WrapInRef = true }));
        Assert.Equal("<ref name=\"iucn\">{{cite Q|Q1}}</ref>",
            WikidataCitation.CiteQ("Q1", new CiteQOptions { WrapInRef = true, RefName = "iu\"cn/" }));
        Assert.Equal("<ref name=\"iucn-7\">{{cite Q|Q1}}</ref>",
            WikidataCitation.CiteQ("Q1", new CiteQOptions { WrapInRef = true, RefName = "7" }));
    }

    // ------------------------------------------------------------ create batch

    [Fact]
    public void CreateItemCommands_EveryStatementInTheDryRunsOrder() {
        var commands = WikidataCitation.CreateItemCommands(Salmon, "Q188879", Model);
        Assert.Equal(new[] {
            "CREATE",
            L("LAST", "Len", "\"Salmo salar Kola subpopulation. The IUCN Red List of Threatened Species 2023: e.T196579964A196580951\""),
            L("LAST", "Den", "\"IUCN Red List assessment of Salmo salar Kola subpopulation\""),
            L("LAST", "P31", "Q13442814"),
            L("LAST", "P1476", "en:\"Salmo salar Kola subpopulation\""),
            L("LAST", "P1433", "Q32059"),
            L("LAST", "P123", "Q48268"),
            L("LAST", "P921", "Q188879"),
            L("LAST", "P407", "Q1860"),
            L("LAST", "P953", "\"https://www.iucnredlist.org/species/196579964/196580951\""),
            L("LAST", "P577", "+2023-00-00T00:00:00Z/9"),
            L("LAST", "P356", "\"10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.EN\""),
            L("LAST", "P2093", "\"Sayer, C.\"", "P1545", "\"1\""),
            L("LAST", "P2093", "\"Lajus, D.\"", "P1545", "\"2\""),
        }, commands);
    }

    [Fact]
    public void CreateItemCommands_NoTaxonItem_NoMainSubject() {
        var commands = WikidataCitation.CreateItemCommands(Salmon, null, Model);
        Assert.DoesNotContain(commands, c => c.Contains("\tP921\t", StringComparison.Ordinal));
        // A value that is not an item id is not written either.
        Assert.DoesNotContain(WikidataCitation.CreateItemCommands(Salmon, "Q0", Model), c => c.Contains("\tP921\t", StringComparison.Ordinal));
    }

    [Fact]
    public void CreateItemCommands_TheModelDecidesClassLabelAndTitleLanguage() {
        var model = new WikidataItemModel {
            InstanceOf = "Q1172284",
            TitleLanguage = "mul",
            LabelTemplate = "{name} ({year}), T{taxon_id}A{assessment_id}",
            DescriptionTemplate = "",
        };
        var commands = WikidataCitation.CreateItemCommands(Salmon, null, model);
        Assert.Contains(L("LAST", "Len", "\"Salmo salar Kola subpopulation (2023), T196579964A196580951\""), commands);
        Assert.DoesNotContain(commands, c => c.StartsWith("LAST\tDen", StringComparison.Ordinal));
        Assert.Contains(L("LAST", "P31", "Q1172284"), commands);
        Assert.Contains(L("LAST", "P1476", "mul:\"Salmo salar Kola subpopulation\""), commands);
    }

    [Theory]
    [InlineData("10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.es", "Q1321")]
    [InlineData("10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.fr", "Q150")]
    [InlineData("10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.pt", "Q5146")]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.en", "Q1860")]
    [InlineData(null, "Q1860")]
    public void CreateItemCommands_LanguageFromTheDoi(string? doi, string language) {
        var commands = WikidataCitation.CreateItemCommands(Salmon with { Doi = doi }, null, Model);
        Assert.Contains(L("LAST", "P407", language), commands);
    }

    [Fact]
    public void CreateItemCommands_ADoiNamingAnotherAssessment_IsLeftOut() {
        // An errata version cites the DOI of the assessment it corrects (giant panda); that DOI belongs
        // on the corrected assessment's item, and P356 must stay unique.
        var panda = new IucnCitationParts {
            TaxonId = 712, AssessmentId = 121745669, Year = 2016, ErrataYear = 2017,
            ScientificName = "Ailuropoda melanoleuca",
            Doi = "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en",
        };
        var commands = WikidataCitation.CreateItemCommands(panda, null, Model);
        Assert.DoesNotContain(commands, c => c.Contains("\tP356\t", StringComparison.Ordinal));
        Assert.Contains(L("LAST", "P407", "Q1860"), commands);
    }

    [Fact]
    public void CreateItemCommands_RepeatedAuthorName_IsANewStatement() {
        // Shambel Alemu and Sisay Alemu: without "!" QuickStatements would add the second ordinal
        // as another qualifier on the first statement.
        var parts = Salmon with { Authors = [Person("Alemu", "S."), Person("Alemu", "S."), Person("Abebe", "T.")] };
        var authors = WikidataCitation.CreateItemCommands(parts, null, Model).Where(c => c.Contains("P2093", StringComparison.Ordinal)).ToList();
        Assert.Equal(new[] {
            L("LAST", "P2093", "\"Alemu, S.\"", "P1545", "\"1\""),
            L("LAST", "!P2093", "\"Alemu, S.\"", "P1545", "\"2\""),
            L("LAST", "P2093", "\"Abebe, T.\"", "P1545", "\"3\""),
        }, authors);
    }

    [Fact]
    public void CreateItemCommands_Values_AreOneLineAndQuotedVerbatim() {
        var parts = Salmon with {
            ScientificName = "Panthera leo\tWest Africa\nsubpopulation",
            Authors = [
                new CitationAuthor(CitationAuthorKind.Organisation, "IUCN SSC \"Cat\" Specialist Group"),
                new CitationAuthor(CitationAuthorKind.Verbatim, "Jaffré, T. <i>et al.</i>"),
                new CitationAuthor(CitationAuthorKind.Verbatim, "A || B"),
            ],
        };
        var commands = WikidataCitation.CreateItemCommands(parts, null, Model);
        Assert.Contains(L("LAST", "P1476", "en:\"Panthera leo West Africa subpopulation\""), commands);
        // QuickStatements takes everything between the first and the last quote, so inner quotes stay.
        Assert.Contains(L("LAST", "P2093", "\"IUCN SSC \"Cat\" Specialist Group\"", "P1545", "\"1\""), commands);
        Assert.Contains(L("LAST", "P2093", "\"Jaffré, T.\"", "P1545", "\"2\""), commands);
        Assert.Contains(L("LAST", "P2093", "\"A | B\"", "P1545", "\"3\""), commands);
        Assert.All(commands, c => Assert.DoesNotContain('\n', c));
    }

    [Fact]
    public void CreateItemCommands_LabelOver250Characters_IsLeftOut() {
        var parts = Salmon with { ScientificName = new string('a', 200) };
        var commands = WikidataCitation.CreateItemCommands(parts, null, Model);
        Assert.DoesNotContain(commands, c => c.StartsWith("LAST\tLen", StringComparison.Ordinal));
        Assert.Contains(commands, c => c.StartsWith("LAST\tDen", StringComparison.Ordinal));
    }

    // ------------------------------------------------------------ add batch

    [Fact]
    public void AddMissingCommands_AddsOnlyJudgedPropertiesTheItemLacks() {
        var present = new HashSet<string> { "P31", "P1433", "P356", "P577", "P1476", "Len" };
        var commands = WikidataCitation.AddMissingCommands(Salmon, "Q56502866", present, "Q188879", Model);
        Assert.Equal(new[] {
            L("Q56502866", "P921", "Q188879"),
            L("Q56502866", "P953", "\"https://www.iucnredlist.org/species/196579964/196580951\""),
            L("Q56502866", "P2093", "\"Sayer, C.\"", "P1545", "\"1\""),
            L("Q56502866", "P2093", "\"Lajus, D.\"", "P1545", "\"2\""),
        }, commands);
    }

    [Fact]
    public void AddMissingCommands_NeverPublisherLanguageOrDescription() {
        var commands = WikidataCitation.AddMissingCommands(Salmon, "Q1", new HashSet<string>(), "Q188879", Model);
        Assert.DoesNotContain(commands, c => c.Contains("\tP123\t", StringComparison.Ordinal));
        Assert.DoesNotContain(commands, c => c.Contains("\tP407\t", StringComparison.Ordinal));
        Assert.DoesNotContain(commands, c => c.Contains("\tDen\t", StringComparison.Ordinal));
        Assert.Equal(L("Q1", "Len", "\"Salmo salar Kola subpopulation. The IUCN Red List of Threatened Species 2023: e.T196579964A196580951\""), commands[0]);
        Assert.Contains(L("Q1", "P31", "Q13442814"), commands);
        Assert.Contains(L("Q1", "P356", "\"10.2305/IUCN.UK.2023-1.RLTS.T196579964A196580951.EN\""), commands);
    }

    [Fact]
    public void AddMissingCommands_AuthorItemsCountAsAuthors() {
        var present = new HashSet<string> { "P50" };
        var commands = WikidataCitation.AddMissingCommands(Salmon, "Q1", present, null, Model);
        Assert.DoesNotContain(commands, c => c.Contains("P2093", StringComparison.Ordinal));
    }

    [Fact]
    public void AddMissingCommands_NothingMissing_IsEmpty() {
        var present = WikidataCitation.JudgedProperties.ToHashSet();
        Assert.Empty(WikidataCitation.AddMissingCommands(Salmon, "Q1", present, "Q188879", Model));
    }

    // ------------------------------------------------------------ link

    [Fact]
    public void QuickStatementsUrl_PipesForTabsAndDoublePipesForLines() {
        var url = WikidataCitation.QuickStatementsUrl(new[] {
            L("Q37887397", "P214", "\"96480189\"", "S143", "Q565"),
            L("Q1", "P31", "Q5"),
        });
        // Help:QuickStatements' own example, then a second command.
        Assert.Equal("https://quickstatements.toolforge.org/#/v1=Q37887397%7CP214%7C%2296480189%22%7CS143%7CQ565%7C%7CQ1%7CP31%7CQ5", url);
        Assert.True(WikidataCitation.QuickStatementsUrlFits(url));
    }

    [Fact]
    public void QuickStatementsUrl_ALineWithAPipeInAValue_KeepsItsTabs() {
        var url = WikidataCitation.QuickStatementsUrl(new[] { L("LAST", "P2093", "\"A | B\""), L("LAST", "P31", "Q5") });
        var decoded = Uri.UnescapeDataString(url[WikidataCitation.QuickStatementsBase.Length..]);
        Assert.Equal("LAST\tP2093\t\"A | B\"||LAST|P31|Q5", decoded);
    }

    [Fact]
    public void QuickStatementsUrl_EncodesEverythingOutsideTheUnreservedSet() {
        var commands = WikidataCitation.CreateItemCommands(Salmon with { Authors = [Person("Jaffré", "T.")] }, "Q188879", Model);
        var url = WikidataCitation.QuickStatementsUrl(commands);
        var data = url[WikidataCitation.QuickStatementsBase.Length..];
        Assert.DoesNotContain('/', data);
        Assert.DoesNotContain('+', data);
        Assert.DoesNotContain('"', data);
        Assert.DoesNotContain('#', data);
        Assert.DoesNotContain('?', data);
        Assert.Contains("Jaffr%C3%A9", data);
        Assert.Equal(string.Join("||", commands.Select(c => c.Replace('\t', '|'))), Uri.UnescapeDataString(data));
    }

    [Fact]
    public void QuickStatementsUrlFits_LimitIsInclusive() {
        Assert.True(WikidataCitation.QuickStatementsUrlFits(new string('a', WikidataCitation.MaxQuickStatementsUrlLength)));
        Assert.False(WikidataCitation.QuickStatementsUrlFits(new string('a', WikidataCitation.MaxQuickStatementsUrlLength + 1)));
    }

    // ------------------------------------------------------------ title and label fixes

    // Q29010489, the item for Batrachuperus karlschmidti assessment 59115/11869436 (2004): SourceMD
    // gave it "Name: author list" as its title and label (cached 3 October 2026).
    private static readonly IucnCitationParts Karlschmidti = new() {
        TaxonId = 59115,
        AssessmentId = 11869436,
        Year = 2004,
        ScientificName = "Batrachuperus karlschmidti",
        Authors = [Person("Xie", "F.")],
    };

    private const string KarlschmidtiOld = "Batrachuperus karlschmidti: Xie Feng";
    private const string KarlschmidtiLabel = "Batrachuperus karlschmidti. The IUCN Red List of Threatened Species 2004: e.T59115A11869436";

    [Fact]
    public void FixCommands_SourceMdTitleAndLabel_AreReplaced() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q29010489", [new WikidataTitle(KarlschmidtiOld, "en")], KarlschmidtiOld, Model);

        // The new title goes in before the old one is removed, so a batch that stops early leaves a title.
        Assert.Equal(new[] {
            L("Q29010489", "P1476", "en:\"Batrachuperus karlschmidti\""),
            L("-Q29010489", "P1476", "en:\"" + KarlschmidtiOld + "\""),
            L("Q29010489", "Len", "\"" + KarlschmidtiLabel + "\""),
        }, fix.Commands);
        Assert.Equal(new[] {
            new WikidataItemChange(WikidataItemChangeKind.Title, KarlschmidtiOld, "Batrachuperus karlschmidti", "en", "en"),
            new WikidataItemChange(WikidataItemChangeKind.EnglishLabel, KarlschmidtiOld, KarlschmidtiLabel),
        }, fix.Changes);
    }

    [Fact]
    public void FixCommands_ModelTitleAndLabel_NothingToChange() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q29010489",
            [new WikidataTitle("Batrachuperus karlschmidti", "en")], KarlschmidtiLabel, Model);
        Assert.Empty(fix.Commands);
        Assert.Empty(fix.Changes);
    }

    [Fact]
    public void FixCommands_TitlesNotRecorded_LeaveTheTitleAlone() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q29010489", titles: null, KarlschmidtiLabel, Model);
        Assert.Empty(fix.Commands);
    }

    [Fact]
    public void FixCommands_TitleWithNoAuthorList_IsLeftAsItIs() {
        // Q56227924 has la:"Myrmecophaga tridactyla": a name and no author list, so nothing is
        // removed from it, and its language stays, though the model's title language is en.
        var fix = WikidataCitation.FixCommands(Karlschmidti with { ScientificName = "Myrmecophaga tridactyla" }, "Q56227924",
            [new WikidataTitle("Myrmecophaga tridactyla", "la")], labelEn: null, Model);
        Assert.Empty(fix.Commands);
        // Q56804492: "<i>Myotis nattereri</i>" has no author list either.
        var tagged = WikidataCitation.FixCommands(Karlschmidti with { ScientificName = "Myotis nattereri" }, "Q56804492",
            [new WikidataTitle("<i>Myotis nattereri</i>", "en")], labelEn: null, Model);
        Assert.Empty(tagged.Commands);
    }

    // Q56481550, the item for the 2014 assessment of taxon 3755, published as Canis mesomelas
    // (Crossref's title for 10.2305/IUCN.UK.2014-1.RLTS.T3755A46122476.en is "Canis mesomelas:
    // Hoffmann, M."). IUCN's citation now gives the current name, Lupulella mesomelas.
    private static readonly IucnCitationParts Lupulella = new() {
        TaxonId = 3755,
        AssessmentId = 46122476,
        Year = 2014,
        ScientificName = "Lupulella mesomelas",
        Authors = [Person("Hoffmann", "M.")],
    };

    [Fact]
    public void FixCommands_KeepsTheNameTheAssessmentWasPublishedUnder() {
        const string old = "Canis mesomelas: Hoffmann, M";
        var fix = WikidataCitation.FixCommands(Lupulella, "Q56481550", [new WikidataTitle(old, "en")], old, Model);
        Assert.Equal(new[] {
            L("Q56481550", "P1476", "en:\"Canis mesomelas\""),
            L("-Q56481550", "P1476", "en:\"" + old + "\""),
            L("Q56481550", "Len", "\"Canis mesomelas. The IUCN Red List of Threatened Species 2014: e.T3755A46122476\""),
        }, fix.Commands);
        Assert.DoesNotContain(fix.Commands, c => c.Contains("Lupulella", StringComparison.Ordinal));
    }

    [Fact]
    public void FixCommands_IucnInternalName_IsNeverWritten() {
        // Q56226968: IUCN's citation calls taxon 22694346 "Larus glaucoides_old"; the item's title has
        // the name it was published under.
        var parts = new IucnCitationParts {
            TaxonId = 22694346, AssessmentId = 39183818, Year = 2012, ScientificName = "Larus glaucoides_old",
            Authors = [new CitationAuthor(CitationAuthorKind.Organisation, "BirdLife International")],
        };
        const string old = "Larus glaucoides: BirdLife International";
        var fix = WikidataCitation.FixCommands(parts, "Q56226968", [new WikidataTitle(old, "en")], old, Model);
        Assert.Equal(new[] {
            L("Q56226968", "P1476", "en:\"Larus glaucoides\""),
            L("-Q56226968", "P1476", "en:\"" + old + "\""),
            L("Q56226968", "Len", "\"Larus glaucoides. The IUCN Red List of Threatened Species 2012: e.T22694346A39183818\""),
        }, fix.Commands);

        // A title whose own name has the marker is left alone, and so is the label when no other
        // name is known.
        const string marked = "Larus glaucoides_old: BirdLife International";
        var none = WikidataCitation.FixCommands(parts, "Q1", [new WikidataTitle(marked, "en")], marked, Model);
        Assert.Empty(none.Commands);
    }

    [Fact]
    public void FixCommands_DeprecatedTitleWithTheSameText_LeavesTheTitleAlone() {
        // QuickStatements removes the last statement whose value matches, of any rank, so it could
        // remove the deprecated copy and keep the title the page says is replaced.
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [
            new WikidataTitle(KarlschmidtiOld, "en"),
            new WikidataTitle(KarlschmidtiOld, "en", "deprecated"),
        ], KarlschmidtiLabel, Model);
        Assert.DoesNotContain(fix.Commands, c => c.Contains("P1476", StringComparison.Ordinal));
        // The same text in another language is not a match.
        var other = WikidataCitation.FixCommands(Karlschmidti, "Q1", [
            new WikidataTitle(KarlschmidtiOld, "en"),
            new WikidataTitle(KarlschmidtiOld, "de", "deprecated"),
        ], KarlschmidtiLabel, Model);
        Assert.Contains(L("-Q1", "P1476", "en:\"" + KarlschmidtiOld + "\""), other.Commands);
    }

    [Fact]
    public void FixCommands_ModelTitleAlreadyThere_TheOtherTitlesStay() {
        // Q113815710: the model's title, plus the SourceMD one at deprecated rank.
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [
            new WikidataTitle("Batrachuperus karlschmidti", "en"),
            new WikidataTitle(KarlschmidtiOld, "en", "deprecated"),
        ], KarlschmidtiLabel, Model);
        Assert.Empty(fix.Commands);
    }

    [Fact]
    public void FixCommands_SeveralTitles_NoneIsRemoved() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [
            new WikidataTitle(KarlschmidtiOld, "en"),
            new WikidataTitle("Batrachuperus karlschmidti: Xie, F.", "en"),
        ], KarlschmidtiLabel, Model);
        Assert.Empty(fix.Commands);
    }

    [Fact]
    public void FixCommands_OnlyTitleDeprecated_IsLeftToTheAddBatch() {
        // wdt:P1476 leaves the deprecated title out, so P1476 is missing and AddMissingCommands adds the title.
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle(KarlschmidtiOld, "en", "deprecated")],
            KarlschmidtiLabel, Model);
        Assert.Empty(fix.Commands);
    }

    [Theory]
    [InlineData("Name: A\tB", "en")]
    [InlineData("Name: A || B", "en")]
    [InlineData(" Name: A", "en")]
    [InlineData("Name: A ", "en")]
    [InlineData("Name: A", "be-x-old2")]
    [InlineData("Name: A", "")]
    public void FixCommands_OldTitleQuickStatementsCannotMatch_IsLeftAlone(string text, string language) {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle(text, language)], KarlschmidtiLabel, Model);
        Assert.DoesNotContain(fix.Commands, c => c.Contains("P1476", StringComparison.Ordinal));
    }

    [Fact]
    public void FixCommands_OldTitleWithQuotesAndAmpersandEntity_IsWrittenExactly() {
        // SourceMD left "&amp;" in some titles; the removal has to name the text as stored.
        const string old = "Muscardinus avellanarius: Hutterer, R. &amp; \"Juškaitis\", R.";
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle(old, "en")], KarlschmidtiLabel, Model);
        Assert.Equal(L("-Q1", "P1476", "en:\"" + old + "\""), fix.Commands[1]);
    }

    [Fact]
    public void FixCommands_LabelOnly_WhenTheTitleIsRight() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle("Batrachuperus karlschmidti", "en")],
            "Spiny Giant Frog, Batrachuperus karlschmidti", Model);
        Assert.Equal(new[] { L("Q1", "Len", "\"" + KarlschmidtiLabel + "\"") }, fix.Commands);
        Assert.Equal(WikidataItemChangeKind.EnglishLabel, Assert.Single(fix.Changes).Kind);
    }

    [Fact]
    public void FixCommands_NoLabel_IsLeftToTheAddBatch() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle("Batrachuperus karlschmidti", "en")], labelEn: null, Model);
        Assert.Empty(fix.Commands);
        var add = WikidataCitation.AddMissingCommands(Karlschmidti, "Q1", new HashSet<string> { "P31", "P1476" }, null, Model);
        Assert.Contains(L("Q1", "Len", "\"" + KarlschmidtiLabel + "\""), add);
    }

    [Fact]
    public void FixCommands_LinkKeepsTheTabsOfALineWithAPipe() {
        var fix = WikidataCitation.FixCommands(Karlschmidti, "Q1", [new WikidataTitle("Name: A | B", "en")], KarlschmidtiLabel, Model);
        var url = WikidataCitation.QuickStatementsUrl(fix.Commands);
        var data = Uri.UnescapeDataString(url[WikidataCitation.QuickStatementsBase.Length..]);
        Assert.Contains("-Q1\tP1476\ten:\"Name: A | B\"", data);
    }

    // ------------------------------------------------------------ the published name

    [Theory]
    [InlineData("Canis mesomelas: Hoffmann, M.", "Canis mesomelas")]
    [InlineData("Opsanus tau : Collette, B.B", "Opsanus tau")]
    [InlineData("Sepia orbignyana: Barratt, I. &amp; Allcock, L.", "Sepia orbignyana")]
    [InlineData("<i>Myotis nattereri</i>", "Myotis nattereri")]
    [InlineData("Myrmecophaga  tridactyla", "Myrmecophaga tridactyla")]
    [InlineData("Oncorhynchus nerka (COLUMBIA RIVER: Redfish Lk): Rand, P.S.", "Oncorhynchus nerka (COLUMBIA RIVER: Redfish Lk)")]
    [InlineData("Oncorhynchus nerka (FRASER RIVER, LILLOOET: Birkenhead (late)): Rand, P.S.", "Oncorhynchus nerka (FRASER RIVER, LILLOOET: Birkenhead (late))")]
    [InlineData("Oncorhynchus nerka (COLUMBIA RIVER: Redfish Lk)", "Oncorhynchus nerka (COLUMBIA RIVER: Redfish Lk)")]
    [InlineData(": Hoffmann, M.", null)]
    [InlineData("  ", null)]
    [InlineData(null, null)]
    public void NameFromTitle_TheTextBeforeTheAuthorList(string? title, string? name) {
        Assert.Equal(name, WikidataCitation.NameFromTitle(title));
    }

    [Theory]
    // Taxon 30321 (1998): Crossref's title has "ssp.", IUCN's citation "subsp.".
    [InlineData("Apollonias barbujana ssp. ceballosi", "Apollonias barbujana subsp. ceballosi", true)]
    // Taxon 2468 (1996): Crossref's title has the subpopulation in brackets.
    [InlineData("Balaena mysticetus (Bering-Chukchi-Beaufort Sea subpopulation)", "Balaena mysticetus Bering-Chukchi-Beaufort Sea subpopulation", true)]
    [InlineData("Oncorhynchus nerka [Redfish Lk]", "Oncorhynchus  nerka Redfish Lk", true)]
    [InlineData("<i>Myotis nattereri</i>", "Myotis nattereri", true)]
    [InlineData("Canis mesomelas", "Lupulella mesomelas", false)]
    [InlineData("Larus glaucoides", "Larus glaucoides_old", false)]
    // Only the rank marker is read as "ssp.": an epithet that starts with "subsp" is a word.
    [InlineData("Abies subspinosa", "Abies sspinosa", false)]
    [InlineData("Apollonias barbujana var. ceballosi", "Apollonias barbujana subsp. ceballosi", false)]
    public void SameName_SspIsSubspAndBracketsAndSpacesAreIgnored(string a, string b, bool same) {
        Assert.Equal(same, WikidataCitation.SameName(a, b));
        Assert.Equal(same, WikidataCitation.SameName(b, a));
    }

    [Fact]
    public void PublishedNameFor_ItemTitleThenCrossrefThenIucn() {
        var registered = Lupulella with { RegisteredName = "Canis mesomelas" };
        Assert.Equal(new PublishedName("Canis mesomelas", PublishedNameSource.ItemTitle),
            WikidataCitation.PublishedNameFor(Lupulella, [new WikidataTitle("Canis mesomelas: Hoffmann, M", "en")]));
        Assert.Equal(new PublishedName("Canis mesomelas", PublishedNameSource.Crossref), WikidataCitation.PublishedNameFor(registered));
        // Several titles that are not deprecated: none is the item's title.
        Assert.Equal(PublishedNameSource.Crossref, WikidataCitation.PublishedNameFor(registered,
            [new WikidataTitle("A b: C", "en"), new WikidataTitle("D e: F", "en")])!.Source);
        Assert.Equal(new PublishedName("Lupulella mesomelas", PublishedNameSource.IucnCitation), WikidataCitation.PublishedNameFor(Lupulella));
        // "_old" names are skipped, wherever they come from.
        Assert.Equal(PublishedNameSource.IucnCitation,
            WikidataCitation.PublishedNameFor(Lupulella with { RegisteredName = "Canis mesomelas_old" })!.Source);
        Assert.Null(WikidataCitation.PublishedNameFor(Lupulella with { ScientificName = "Larus glaucoides_old" }));
    }

    [Fact]
    public void CreateItemCommands_UseTheRegisteredName() {
        var commands = WikidataCitation.CreateItemCommands(Lupulella with { RegisteredName = "Canis mesomelas" }, null, Model);
        Assert.Contains(L("LAST", "P1476", "en:\"Canis mesomelas\""), commands);
        Assert.Contains(L("LAST", "Len", "\"Canis mesomelas. The IUCN Red List of Threatened Species 2014: e.T3755A46122476\""), commands);
        Assert.Contains(L("LAST", "Den", "\"IUCN Red List assessment of Canis mesomelas\""), commands);
        Assert.DoesNotContain(commands, c => c.Contains("Lupulella", StringComparison.Ordinal));
    }

    [Fact]
    public void CreateItemCommands_NoUsableName_NoCommands() {
        Assert.Empty(WikidataCitation.CreateItemCommands(Lupulella with { ScientificName = "Larus glaucoides_old" }, null, Model));
        var registered = WikidataCitation.CreateItemCommands(
            Lupulella with { ScientificName = "Larus glaucoides_old", RegisteredName = "Larus glaucoides" }, null, Model);
        Assert.Contains(L("LAST", "P1476", "en:\"Larus glaucoides\""), registered);
    }

    [Fact]
    public void AddMissingCommands_TitleAndLabelUseThePublishedName() {
        // No title: the registered name. A title but no label: the name in the title.
        var noTitle = WikidataCitation.AddMissingCommands(Lupulella with { RegisteredName = "Canis mesomelas" }, "Q1",
            new HashSet<string> { "P31" }, null, Model);
        Assert.Contains(L("Q1", "P1476", "en:\"Canis mesomelas\""), noTitle);
        var noLabel = WikidataCitation.AddMissingCommands(Lupulella, "Q1", new HashSet<string> { "P31", "P1476" }, null, Model,
            [new WikidataTitle("Canis mesomelas: Hoffmann, M", "en")]);
        Assert.Contains(L("Q1", "Len", "\"Canis mesomelas. The IUCN Red List of Threatened Species 2014: e.T3755A46122476\""), noLabel);
        // No usable name: the title and label are left out, the rest is added.
        var none = WikidataCitation.AddMissingCommands(Lupulella with { ScientificName = "Larus glaucoides_old" }, "Q1",
            new HashSet<string>(), null, Model);
        Assert.DoesNotContain(none, c => c.Contains("\tP1476\t", StringComparison.Ordinal) || c.Contains("\tLen\t", StringComparison.Ordinal));
        Assert.Contains(none, c => c.Contains("\tP953\t", StringComparison.Ordinal));
    }

    [Fact]
    public void WikidataTitle_JsonRoundTrip() {
        IReadOnlyList<WikidataTitle> titles = [new("Rusa unicolor: Timmins, R.", "en"), new("Rusa unicolor", "la", "deprecated")];
        var json = WikidataTitle.ListToJson(titles);
        Assert.Contains("\"lang\":\"la\"", json);
        Assert.Equal(titles, WikidataTitle.ListFromJson(json));
        Assert.Null(WikidataTitle.ListFromJson(null));
        Assert.Null(WikidataTitle.ListFromJson("not json"));
        Assert.Empty(WikidataTitle.ListFromJson("[]")!);
    }

    // ------------------------------------------------------------ model

    [Fact]
    public void ShippedYaml_MatchesTheSharedModelDefaults() {
        var rules = Path.Combine(AppContext.BaseDirectory, "rules");
        // Load falls back to the defaults when the file is missing, which would make this test pass on nothing.
        Assert.True(File.Exists(WikidataIucnEditConfig.PathFor(rules)));
        var config = WikidataIucnEditConfig.Load(rules);
        Assert.Equal(new WikidataItemModel(), config.ToItemModel());
        Assert.Equal(new WikidataItemModel(), new WikidataIucnEditConfig().ToItemModel());
    }

    [Fact]
    public void Model_JsonRoundTrip() {
        var model = new WikidataItemModel { InstanceOf = "Q1172284", LabelTemplate = "{name}" };
        Assert.Equal(model, WikidataItemModel.FromJson(model.ToJson()));
        Assert.Equal(new WikidataItemModel(), WikidataItemModel.FromJson(null));
    }
}
