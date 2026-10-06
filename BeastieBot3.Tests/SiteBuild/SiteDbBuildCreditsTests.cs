using System.Text.Json;
using System.Text.Json.Nodes;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// The credits of each assessment (assessment.credits, credit_name): AssessmentCreditsReader's rules,
// and what `site build-db` stores. Names from IUCN's payloads for the tiger (15955) and a chameleon,
// cut down.
public sealed class SiteDbBuildCreditsTests : IDisposable {
    private const long Tiger = 15955;
    private const long Latest = 214862019;
    private const long Earlier = 50659951;

    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    // ------------------------------------------------------------ the reader

    private static IReadOnlyList<CreditGroup> Read(string creditsJson) {
        using var document = JsonDocument.Parse($$"""{"credits":{{creditsJson}}}""");
        return AssessmentCreditsReader.Read(document.RootElement);
    }

    [Fact]
    public void Read_CollapsesWhitespace_AndKeepsTheRestAsWritten() {
        var groups = Read("""
            [{"credit_type_name":"contributor","full":"Chanchani, P. & Kawthoolei Forest Department","value":[
              "Pranav  Chanchani"," Kawthoolei Forest Department (Karen National Union)","Dale Miquelle\n(WCS)"]}]
            """);

        var contributors = Assert.Single(groups);
        Assert.Equal(["Pranav Chanchani", "Kawthoolei Forest Department (Karen National Union)", "Dale Miquelle (WCS)"], contributors.Names);
        Assert.False(contributors.IsFullOnly);
    }

    [Fact]
    public void Read_PutsTheTypesInIucnsOrder_AndUsesFullWhenValueIsEmpty() {
        var groups = Read("""
            [{"credit_type_name":"institutions","full":"Wildlife Conservation Society","value":["Wildlife Conservation Society"]},
             {"credit_type_name":"contributor","full":"Tilbury, C.","value":["Colin Tilbury"]},
             {"credit_type_name":"evaluator","full":"Mauchamp, A. <i>et al.</i>","value":[]},
             {"credit_type_name":"assessor","full":"Tolley, K.","value":["Krystal Tolley (IUCN SSC Chameleon Specialist Group)"]},
             {"credit_type_name":"facilitators","full":"Jenkins, R.K.B.","value":["Richard Jenkins"]}]
            """);

        Assert.Equal([CreditTypes.Assessor, CreditTypes.Evaluator, CreditTypes.Contributor, CreditTypes.Facilitators, CreditTypes.Institutions],
            groups.Select(g => g.Type));
        var evaluator = groups[1];
        Assert.True(evaluator.IsFullOnly);
        Assert.Equal(["Mauchamp, A. et al."], evaluator.Names);
    }

    // IUCN's value[] is in no fixed order; the entries follow the order of "full", as the citation and
    // IUCN's pages do.
    [Fact]
    public void Read_OrdersTheEntriesByFull_WhenEverySurnameIsFoundOnce() {
        var groups = Read("""
            [{"credit_type_name":"assessor","full":"Tolley, K., Menegon, M. & Plumptre, A.","value":[
              "Andrew Plumptre (WCS)","Michele Menegon (Museo Tridentino di Scienze Naturali, Via Calepina 14, 38122, Trento Italy)",
              "Krystal Tolley (IUCN SSC Chameleon Specialist Group / SANBI-ABR)"]}]
            """);

        Assert.Equal(["Krystal Tolley (IUCN SSC Chameleon Specialist Group / SANBI-ABR)",
            "Michele Menegon (Museo Tridentino di Scienze Naturali, Via Calepina 14, 38122, Trento Italy)",
            "Andrew Plumptre (WCS)"], groups[0].Names);
    }

    // Two entries with the same surname, or one whose surname "full" does not have: value[]'s order is kept.
    [Theory]
    [InlineData("Alemu, S. & Tolley, K.", new[] { "Sisay Alemu", "Krystal Tolley", "Shambel Alemu" })]
    [InlineData("Tolley, K.", new[] { "Richard Jenkins", "Krystal Tolley" })]
    public void Read_KeepsValuesOrder_WhenASurnameIsNotFoundExactlyOnce(string full, string[] value) {
        var groups = Read($$"""[{"credit_type_name":"assessor","full":{{JsonSerializer.Serialize(full)}},"value":{{JsonSerializer.Serialize(value)}}}]""");

        Assert.Equal(value, groups[0].Names);
    }

    [Fact]
    public void Read_LeavesOutEmailAddresses() {
        var groups = Read("""
            [{"credit_type_name":"contributor","full":"Biggs, N., Kalema, J., Whitaker, A. & Ogielska, M.","value":[
              "false.email@globaltrees.org","Nicola Biggs (n.biggs@kew.org)",
              "James Kalema (Makerere University, Uganda (jkalema@cns.mak.ac.ug) / IUCN SSC East African Plants RLA)",
              "Anthony Whitaker (Deceased was @ Whitaker Consultants)","Maria Ogielska (a@b.pl / Wroclaw University)"]},
             {"credit_type_name":"evaluator","full":"Roy, S.","value":["sugoto.roy@iucn.org"]}]
            """);

        Assert.Equal(["Nicola Biggs", "James Kalema (Makerere University, Uganda / IUCN SSC East African Plants RLA)",
            "Anthony Whitaker (Deceased was @ Whitaker Consultants)", "Maria Ogielska (Wroclaw University)"], groups[1].Names);
        // Only an address in value[]: the group is shown as its "full" string.
        Assert.Equal((true, "Roy, S."), (groups[0].IsFullOnly, groups[0].Names.Single()));
    }

    [Fact]
    public void Read_MergesRepeatedTypes_SkipsNulls_AndKeepsEachEntryOnce() {
        var groups = Read("""
            [{"credit_type_name":"assessor","full":"Sayer, C.","value":["Catherine Sayer (IUCN Red List Unit)",null,"Catherine  Sayer (IUCN Red List Unit)"]},
             {"credit_type_name":"assessor","full":"Sayer, C. & Lajus, D.","value":["Catherine Sayer (IUCN Red List Unit)","Dmitry Lajus"]},
             {"credit_type_name":"","full":"No type","value":["Someone"]},
             {"credit_type_name":"evaluator","full":"","value":[]}]
            """);

        var assessors = Assert.Single(groups);
        Assert.Equal(["Catherine Sayer (IUCN Red List Unit)", "Dmitry Lajus"], assessors.Names);
    }

    [Fact]
    public void Read_ReturnsNothing_ForAPayloadWithNoCredits() {
        using var document = JsonDocument.Parse("""{"credits":null}""");
        Assert.Empty(AssessmentCreditsReader.Read(document.RootElement));
        Assert.Empty(Read("[]"));
    }

    [Fact]
    public void StoredCredits_RoundTrips() {
        var groups = new[] { new StoredCreditGroup(CreditTypes.Assessor, [3, 1], null), new StoredCreditGroup(CreditTypes.Evaluator, [], 2) };
        var json = StoredCredits.ToJson(groups);

        Assert.Equal("""[{"type":"assessor","names":[3,1]},{"type":"evaluator","full":2}]""", json);
        var read = StoredCredits.FromJson(json);
        Assert.Equal([3L, 1L], read[0].Names);
        Assert.Equal((CreditTypes.Evaluator, 2L, true), (read[1].Type, read[1].Full, read[1].IsFullOnly));
        Assert.Equal([3L, 1L, 2L], StoredCredits.NameIds(read));
        Assert.Empty(StoredCredits.FromJson("not json"));
        Assert.Empty(StoredCredits.FromJson(null));
    }

    // ------------------------------------------------------------ the build

    // Both assessments' credits are stored as ids into credit_name, which lists a name credited on
    // both assessments once. The default Payload's assessor credit is replaced by the tiger's.
    [Fact]
    public void Build_StoresTheCreditsOfEveryAssessment_WithEachNameOnce() {
        var (output, stats) = Build();
        using var db = OpenReadOnly(output);
        var names = Rows(db, "SELECT credit_name_id, text FROM credit_name").ToDictionary(r => (long)r[0]!, r => (string)r[1]!);

        string[] Texts(StoredCreditGroup g) => (g.Full is { } full ? [full] : g.Names).Select(id => names[id]).ToArray();
        var latest = StoredCredits.FromJson(Scalar(db, $"SELECT credits FROM assessment WHERE assessment_id = {Latest}"));
        var earlier = StoredCredits.FromJson(Scalar(db, $"SELECT credits FROM assessment WHERE assessment_id = {Earlier}"));

        Assert.Equal([CreditTypes.Assessor, CreditTypes.Evaluator, CreditTypes.Contributor], latest.Select(g => g.Type));
        Assert.Equal(["Dale Miquelle (Wildlife Conservation Society Russian Far East Program)", "Hariyo Wibisono (WCS)"], Texts(latest[0]));
        Assert.Equal(["Sugoto Roy (IUCN)"], Texts(latest[1]));
        Assert.Equal(["Pranav Chanchani"], Texts(latest[2]));

        Assert.Equal(2, earlier.Count);
        Assert.Equal(["Dale Miquelle (Wildlife Conservation Society Russian Far East Program)"], Texts(earlier[0]));
        Assert.True(earlier[1].IsFullOnly);
        Assert.Equal(["Breitenmoser, U."], Texts(earlier[1]));

        Assert.Equal(5, names.Count);
        Assert.Equal((2, 6L, 5), (stats.AssessmentsWithCredits, stats.CreditEntries, stats.CreditNames));
        // The citation's assessors still come from the payload as before.
        var parts = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Latest}"))!;
        Assert.Equal(2, parts.Authors.Count);
    }

    private (string Output, SiteBuildStats Stats) Build() {
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var output = _sources.PathOf("site.sqlite");
        WriteIucnCsv(iucn, "2026-1", """
                (1, 15955, 'Panthera tigris', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'tigris', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL)
            """, """
                (1, 214862019, 15955, 'Panthera tigris', 'Endangered', 'A2abcd', '2022', '2021-12-21 00:00:00 UTC', '3.1', NULL, NULL, 'Stable', 'false', 'false', 'Global')
            """);
        var record = $$"""
            {"sis_id":15955,"taxon":{"sis_id":15955,"scientific_name":"Panthera tigris","species_taxa":[],"subpopulation_taxa":[],"infrarank_taxa":[],
              "kingdom_name":"ANIMALIA","phylum_name":"CHORDATA","class_name":"MAMMALIA","order_name":"CARNIVORA","family_name":"FELIDAE",
              "genus_name":"Panthera","species_name":"tigris","subpopulation_name":null,"infra_name":null,"authority":"(Linnaeus, 1758)",
              "species":true,"subpopulation":false,"infrarank":false,"common_names":[],"synonyms":[]},
             "assessments":[{{Header(Latest, Tiger, true, "2022", "EN")}},{{Header(Earlier, Tiger, false, "2015", "EN")}}]}
            """;
        const string downloaded = "2026-08-21T00:00:00Z";
        WriteApiCache(cache,
            new[] { new CachedTaxonRecord(1, Tiger, record) },
            new[] {
                new CachedAssessment(Latest, Tiger, downloaded, WithCredits(Payload(Latest, Tiger, "Panthera tigris", "2022",
                    "Goodrich, J. & Wibisono, H. 2022. Panthera tigris. The IUCN Red List of Threatened Species 2022: e.T15955A214862019. Accessed on 21 August 2026.",
                    "Goodrich, J. & Wibisono, H."), """
                    [{"credit_type_name":"assessor","full":"Miquelle, D. & Wibisono, H.","value":["Hariyo Wibisono (WCS)","Dale Miquelle (Wildlife Conservation Society Russian Far East Program)"]},
                     {"credit_type_name":"evaluator","full":"Roy, S.","value":["Sugoto Roy (IUCN)"]},
                     {"credit_type_name":"contributor","full":"Chanchani, P.","value":["Pranav  Chanchani"]}]
                    """)),
                new CachedAssessment(Earlier, Tiger, downloaded, WithCredits(Payload(Earlier, Tiger, "Panthera tigris", "2015",
                    "Goodrich, J. 2015. Panthera tigris. The IUCN Red List of Threatened Species 2015: e.T15955A50659951. Accessed on 21 August 2026.",
                    "Goodrich, J."), """
                    [{"credit_type_name":"evaluator","full":"Breitenmoser, U.","value":[]},
                     {"credit_type_name":"assessor","full":"Miquelle, D.","value":["Dale Miquelle (Wildlife Conservation Society Russian Far East Program)"]}]
                    """)),
            });
        var stats = new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn,
            ApiCache = cache,
            WikidataItemModel = new WikidataItemModel(),
            Output = output,
        }, QuietConsole()).Run(CancellationToken.None);
        return (output, stats);
    }

    private static string WithCredits(string payload, string credits) {
        var node = JsonNode.Parse(payload)!.AsObject();
        node["credits"] = JsonNode.Parse(credits);
        return node.ToJsonString();
    }
}
