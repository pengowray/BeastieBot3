using System.Text.Json;
using BeastieBot3.Iucn.Citations;

namespace BeastieBot3.Tests.Citations;

// Pins when an author's full given names are taken from the assessor credit's value[] list. The
// names and entries are real ones from the 2026-1 API cache unless a comment says otherwise.
public class AssessorGivenNamesTests {
    private static IReadOnlyList<ValueName> Values(params string[] entries) =>
        entries.Select(AssessorGivenNames.Clean).Where(v => v is not null).Select(v => v!).ToList();

    private static GivenNameMatch Match(string author, params string[] entries) {
        var parsed = IucnAuthorNameParser.Parse(author);
        return AssessorGivenNames.Match(parsed.Author, parsed.Shape, Values(entries));
    }

    private static void AssertGiven(string expected, GivenNameOutcome outcome, string author, params string[] entries) {
        var match = Match(author, entries);
        Assert.Equal(outcome, match.Outcome);
        Assert.Equal(expected, match.GivenNames);
    }

    [Fact]
    public void TheUsersExample_SayerAndLajus() {
        var entries = new[] { "Catherine Sayer (IUCN Red List Unit)", "Dmitry Lajus" };
        AssertGiven("Catherine", GivenNameOutcome.Matched, "Sayer, C.", entries);
        AssertGiven("Dmitry", GivenNameOutcome.Matched, "Lajus, D.", entries);
    }

    [Fact]
    public void EmailEntriesAreDropped_AndNotesInBracketsRemoved() {
        var values = Values("false.email@globaltrees.org", "Justyna Kierat (Jagiellonian University, Poland (ERL Bees))", "  ");
        Assert.Equal(new[] { "Justyna Kierat" }, values.Select(v => v.Name));
        AssertGiven("Justyna", GivenNameOutcome.Matched, "Kierat, J.", "false.email@globaltrees.org", "Justyna Kierat (Jagiellonian University, Poland (ERL Bees))");
        Assert.Equal(GivenNameOutcome.NoValueList, Match("Oldfield, S.", "false.email@globaltrees.org").Outcome);
    }

    [Fact]
    public void PeopleWhoGoByAMiddleName_DoNotMatch() {
        Assert.Equal(GivenNameOutcome.InitialsDisagree, Match("Liddle, T.A.", "Adam Liddle").Outcome);
        Assert.Equal(GivenNameOutcome.InitialsDisagree, Match("Ainsworth, A.M.", "Martyn Ainsworth (Royal Botanic Gardens, Kew, UK)").Outcome);
        Assert.Equal(GivenNameOutcome.InitialsDisagree, Match("Measey, G.J.", "John Measey (Stellenbosch University)").Outcome);
        Assert.Null(Match("Liddle, T.A.", "Adam Liddle").GivenNames);
    }

    [Fact]
    public void FewerGivenNamesThanInitials_KeepsTheOtherInitialsAsPublished() {
        AssertGiven("Dennis R.", GivenNameOutcome.MatchedFewerGivenNames, "Paulson, D.R.", "Dennis Paulson (University of Puget Sound)");
        AssertGiven("Alejandra C.D.", GivenNameOutcome.MatchedFewerGivenNames, "Fuentes, A.C.D.", "Alejandra Fuentes (National Autonomous University of Mexico)");
        AssertGiven("Cristiano de C.", GivenNameOutcome.MatchedFewerGivenNames, "Nogueira, C. de C.", "Cristiano Nogueira (Universidade de São Paulo, Brazil)");
        AssertGiven("Carlos D", GivenNameOutcome.MatchedFewerGivenNames, "DoNascimiento, CD", "Carlos DoNascimiento (Universidad de Antioquia)");
    }

    [Fact]
    public void MoreGivenNamesThanInitials_KeepsTheOnesTheInitialsStandFor() {
        AssertGiven("Juan", GivenNameOutcome.MatchedMoreGivenNames, "Reppucci, J.", "Juan Ignacio Reppucci (Andean Cat Alliance)");
        AssertGiven("Keir", GivenNameOutcome.MatchedMoreGivenNames, "Lynch, K.", "Keir M.J. Lynch");
    }

    [Fact]
    public void MiddleInitialsAndSeveralGivenNames() {
        AssertGiven("Susana C.", GivenNameOutcome.Matched, "Gonçalves, S.C.", "Susana C. Gonçalves");
        AssertGiven("Benjamín Juan", GivenNameOutcome.Matched, "Gómez-Moliner, B.J.", "Benjamín Juan Gómez-Moliner (Universidad del País Vasco, UPV/EHU / IUCN SSC Spain Species Specialist Group)");
        AssertGiven("Abdullah Sulaiman Musabah", GivenNameOutcome.Matched, "Al Kindi, A.S.M.", "Abdullah Sulaiman Musabah Al Kindi (Sultan Qaboos University)");
    }

    [Fact]
    public void HyphenatedInitialsAndGivenNames() {
        AssertGiven("Jean-Marie", GivenNameOutcome.Matched, "Veillon, J.-M.", "Jean-Marie Veillon (IUCN SSC New Caledonia Plants RLA)");
        AssertGiven("Yong-Shik", GivenNameOutcome.Matched, "Kim, Y.-S.", "Yong-Shik Kim (Korean Association of Botanical Gardens and Arboreta)");
        // Made up: initials without the hyphen, and one initial for a hyphenated name.
        AssertGiven("Jean-Pierre", GivenNameOutcome.Matched, "Butin, J.P.", "Jean-Pierre Butin");
        AssertGiven("Jean-Pierre", GivenNameOutcome.Matched, "Butin, J.", "Jean-Pierre Butin");
    }

    [Fact]
    public void SurnamesWithParticles_HyphensAndSeveralWords() {
        AssertGiven("Chris", GivenNameOutcome.Matched, "van Swaay, C.", "Chris van Swaay (De Vlinderstichting / Moth and Butterfly Specialist Group)");
        AssertGiven("Rogier", GivenNameOutcome.Matched, "de Kok, R.", "Rogier de Kok (formerly Royal Botanic Gardens, Kew, U.K.)");
        AssertGiven("Karina", GivenNameOutcome.Matched, "Machuca Machuca, K.", "Karina Machuca Machuca");
        AssertGiven("Rafael", GivenNameOutcome.Matched, "Borroto-Páez, R.", "Rafael Borroto-Páez (Sociedad Cubana de Zoología)");
        // Made up: IUCN prints only "Kok"; the particle before it in value[] is part of the surname.
        AssertGiven("Rogier", GivenNameOutcome.Matched, "Kok, R.", "Rogier de Kok");
    }

    [Fact]
    public void SurnameCompare_IgnoresCaseAndAccents_ButNotSpelling() {
        AssertGiven("Yvonne A.", GivenNameOutcome.MatchedFewerGivenNames, "De Jong, Y.A.", "Yvonne de Jong");
        AssertGiven("Ana", GivenNameOutcome.Matched, "Gonçalves, A.", "Ana Goncalves");
        Assert.Equal(GivenNameOutcome.NoSurnameMatch, Match("Borotto-Páez, R.", "Rafael Borroto-Páez (Sociedad Cubana de Zoología)").Outcome);
        // The surname must be a whole word at the end.
        Assert.Equal(GivenNameOutcome.NoSurnameMatch, Match("Kok, R.", "Rogier Dekok").Outcome);
    }

    [Fact]
    public void GenerationalSuffixes() {
        // Made up entries in the shape value[] uses.
        AssertGiven("Porter P.", GivenNameOutcome.Matched, "Lowry II, P.P.", "Porter P. Lowry II (Missouri Botanical Garden)");
        AssertGiven("Antonio", GivenNameOutcome.Matched, "Golamco, A., Jr.", "Antonio Golamco, Jr.");
    }

    [Fact]
    public void CompactNames() {
        // Made up entries.
        AssertGiven("Nirina Hasina", GivenNameOutcome.Matched, "N.H. Rakotoarivelo", "Nirina Hasina Rakotoarivelo");
        AssertGiven("Karen", GivenNameOutcome.Matched, "Tamanyan K.", "Karen Tamanyan");
    }

    [Fact]
    public void TwoLetterInitials() {
        // Made up entry.
        AssertGiven("Theophanis", GivenNameOutcome.Matched, "Constantinidis, Th.", "Theophanis Constantinidis");
        Assert.Equal(GivenNameOutcome.InitialsDisagree, Match("Constantinidis, Th.", "Tasos Constantinidis").Outcome);
    }

    [Fact]
    public void TwoEntriesFit_NeitherIsUsed() {
        // Shambel Alemu and Sisay Alemu, both "Alemu, S.".
        var match = Match("Alemu, S.", "Shambel Alemu (Ethiopian Biodiversity Institute)", "Sisay Alemu (Addis Ababa University)");
        Assert.Equal(GivenNameOutcome.Ambiguous, match.Outcome);
        Assert.Null(match.GivenNames);
        // The same entry twice is one entry.
        AssertGiven("Matthew", GivenNameOutcome.Matched, "Ford, M.", "Matthew Ford (Seriously Fish)", "Matthew Ford (Seriously Fish)");
    }

    [Fact]
    public void EntriesThatAreNotAFullName() {
        Assert.Equal(GivenNameOutcome.EntryInitialsOnly, Match("Nores, C.", "C. Nores").Outcome);
        Assert.Equal(GivenNameOutcome.EntryInitialsOnly, Match("Achyuthan, N.S.", "N.S Achyuthan (Freelance consultant)").Outcome);
        Assert.Equal(GivenNameOutcome.EntryInitialsOnly, Match("Aliaga-Rossel, E.", "E Aliaga-Rossel").Outcome);
        // "Evan" is a name, although "E" + "van" reads as initials and a particle.
        AssertGiven("Evan", GivenNameOutcome.Matched, "Quah, E.", "Evan Quah (University Sains Malaysia)");
        Assert.Equal(GivenNameOutcome.EntryNotAName, Match("Campbell, R.", "Ruairidh (Roo) Campbell (NatureScot)").Outcome);
        // Made up: a postal address before the name.
        Assert.Equal(GivenNameOutcome.EntryNotAName, Match("Smith, J.", "PO Box 12 John Smith").Outcome);
    }

    [Fact]
    public void OnlyPersonsWrittenWithInitialsAreLookedUp() {
        Assert.Equal(GivenNameOutcome.NotInitials, Match("BirdLife International", "BirdLife International (BirdLife International)").Outcome);
        Assert.Equal(GivenNameOutcome.NotInitials, Match("Sebsebe Demissew", "Sebsebe Demissew").Outcome);
        Assert.Equal(GivenNameOutcome.NotInitials, Match("Mohd Yusof, Nur Adillah", "Nur Adillah Mohd Yusof").Outcome);
    }

    [Fact]
    public void ReadValues_EveryAssessorCredit_Distinct() {
        using var doc = JsonDocument.Parse("""
            [{"credit_type_name":"assessor","full":"Sayer, C. & Lajus, D.","value":["Catherine Sayer (IUCN Red List Unit)","Dmitry Lajus"]},
             {"credit_type_name":"assessor","full":"Sayer, C.","value":["Catherine Sayer (IUCN Red List Unit)","false.email@x.org",3]}]
            """);
        var values = AssessorGivenNames.ReadValues(doc.RootElement.EnumerateArray());
        Assert.Equal(new[] { "Catherine Sayer", "Dmitry Lajus" }, values.Select(v => v.Name));
        Assert.Equal("Catherine Sayer (IUCN Red List Unit)", values[0].Raw);
    }
}
