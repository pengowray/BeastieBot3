using System.Text.Json;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.Citations;

// Pins the repair of author names that lost a letter to an encoding error. The damaged names are
// real ones from the IUCN API cache (Oct 2026); the replacement character is written as Lost so it
// stays visible in the source.
public class AssessorNamePoolTests {
    private const char Lost = (char)0xFFFD;

    private static AssessorNamePool Pool(params string[] names) {
        var pool = new AssessorNamePool();
        foreach (var name in names) {
            pool.Add(name);
        }
        return pool;
    }

    [Theory]
    [InlineData("Kry?tufek, B.", true)]
    [InlineData("U?ur Kaya", true)]
    [InlineData("Yusuf Kumluta?", true)]
    [InlineData("Mehmet ?z", true)]
    [InlineData("Kryštufek, B.", false)]
    [InlineData("Smith, J. ?", false)]
    [InlineData("? Smith", false)]
    public void IsDamaged_QuestionMarkNextToALetter(string name, bool damaged) {
        Assert.Equal(damaged, AssessorNamePool.IsDamaged(name));
    }

    [Fact]
    public void IsDamaged_ReplacementCharacterAnywhere() {
        Assert.True(AssessorNamePool.IsDamaged($"Kry{Lost}tufek, B."));
        Assert.True(AssessorNamePool.IsDamaged($"{Lost}"));
    }

    [Fact]
    public void Repair_TakesTheOneMatchingName() {
        var pool = Pool("Kryštufek, B.", "Amori, G.", "Uğur Kaya", "Yusuf Kumlutaş", "Mehmet Öz");

        Assert.Equal("Kryštufek, B.", pool.Repair($"Kry{Lost}tufek, B."));
        Assert.Equal("Kryštufek, B.", pool.Repair("Kry?tufek, B."));
        Assert.Equal("Uğur Kaya", pool.Repair("U?ur Kaya"));
        Assert.Equal("Yusuf Kumlutaş", pool.Repair("Yusuf Kumluta?"));
        Assert.Equal("Mehmet Öz", pool.Repair("Mehmet ?z"));
    }

    // An encoding error only loses letters outside ASCII, so "Ugur Kaya" (also in IUCN's credits) is
    // not a second candidate for "U?ur Kaya".
    [Fact]
    public void Repair_MatchesOnlyALetterOutsideAscii() {
        var pool = Pool("Ugur Kaya", "Uğur Kaya", "Krystufek, B.");

        Assert.Equal("Uğur Kaya", pool.Repair("U?ur Kaya"));
        Assert.Null(pool.Repair("Kry?tufek, B."));
    }

    [Fact]
    public void Repair_LeavesTheNameWhenSeveralOrNoNamesMatch() {
        var pool = Pool("Kryštufek, B.", "Kryžtufek, B.", "Petrović, J.");

        Assert.Null(pool.Repair("Kry?tufek, B."));
        Assert.Null(pool.Repair("Bedjani?, M."));
        // One damaged character is one letter: "Petrovi?" is not "Petrović, J." without the initials.
        Assert.Null(pool.Repair("Petrovi?"));
    }

    [Fact]
    public void Add_LeavesOutDamagedNames() {
        var pool = Pool("Kry?tufek, B.", $"Kry{Lost}tufek, B.");

        Assert.Equal(0, pool.Count);
        Assert.Null(pool.Repair("Kry?tufek, B."));
    }

    // ------------------------------------------------------------ through the parser

    private static readonly DateTime Downloaded = new(2026, 8, 20, 0, 0, 0, DateTimeKind.Utc);

    // Microtus majori (aid 22346545): IUCN's credit and citation both have U+FFFD.
    private static string MicrotusMajori() {
        var assessor = $"Kry{Lost}tufek, B., Shenbrot, G. & Sozen, M.";
        return JsonSerializer.Serialize(new Dictionary<string, object?> {
            ["assessment_id"] = 22346545,
            ["sis_taxon_id"] = 13460,
            ["year_published"] = "2008",
            ["citation"] = $"{assessor} 2008. Microtus majori. The IUCN Red List of Threatened Species 2008: e.T13460A22346545. Accessed on 20 August 2026.",
            ["taxon"] = new Dictionary<string, object?> { ["sis_id"] = 13460, ["scientific_name"] = "Microtus majori" },
            ["credits"] = new[] {
                new Dictionary<string, object?> { ["credit_type_name"] = "assessor", ["full"] = assessor, ["value"] = new[] { "a", "b", "c" } },
            },
            ["scopes"] = new[] { new Dictionary<string, object?> { ["code"] = "1", ["description"] = new Dictionary<string, string> { ["en"] = "Global" } } },
        });
    }

    private static IucnCitationParse Parse(string json, Func<string, string?>? repair) {
        using var document = JsonDocument.Parse(json);
        return IucnCitationPartsParser.Parse(document.RootElement, Downloaded, null, repair);
    }

    [Fact]
    public void Parse_RepairsTheNameAndReadsItAsAPerson() {
        var pool = Pool("Kryštufek, B.");
        var parse = Parse(MicrotusMajori(), pool.Repair);

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Person, "Kryštufek, B.", "Kryštufek", "B."), parse.Parts!.Authors[0]);
        Assert.Equal(new[] { new AuthorNameRepair($"Kry{Lost}tufek, B.", "Kryštufek, B.") }, parse.RepairedAuthorNames);
        Assert.Empty(parse.DamagedAuthorNames);
    }

    [Fact]
    public void Parse_WithoutARepair_KeepsTheNameAndReportsIt() {
        var parse = Parse(MicrotusMajori(), new AssessorNamePool().Repair);

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Verbatim, $"Kry{Lost}tufek, B."), parse.Parts!.Authors[0]);
        Assert.Equal(new[] { $"Kry{Lost}tufek, B." }, parse.DamagedAuthorNames);
        Assert.Empty(parse.RepairedAuthorNames);
        Assert.Equal(new[] { $"Kry{Lost}tufek, B." }, Parse(MicrotusMajori(), null).DamagedAuthorNames);
    }

    [Fact]
    public void AddFrom_TakesAssessorNamesOnly() {
        var pool = new AssessorNamePool();
        pool.AddFrom(Parse(MicrotusMajori(), null));

        // Shenbrot and Sozen are added; the damaged name is not.
        Assert.Equal(2, pool.Count);
    }
}
