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

    // ------------------------------------------------------------ model

    [Fact]
    public void ShippedYaml_MatchesTheSharedModelDefaults() {
        var config = WikidataIucnEditConfig.Load(Path.Combine(AppContext.BaseDirectory, "rules"));
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
