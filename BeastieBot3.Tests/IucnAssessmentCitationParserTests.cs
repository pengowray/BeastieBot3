using BeastieBot3.WikidataEdits;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the IUCN side of the Wikidata status dry run: which assessment is a taxon's latest global
// one, what a reference can cite from it, and how credits[].full strings split into names.
// Fixtures are real 2026-1 API payloads cut down to the fields read here; credits[].value entries
// (names with affiliations, email addresses) are replaced by distinct placeholders, keeping only
// their count.
public class IucnAssessmentCitationParserTests {
    private static readonly DateTime Downloaded = new(2026, 8, 18, 15, 58, 27, DateTimeKind.Utc);

    // ------------------------------------------------------------ Parse

    [Fact]
    public void Parse_Species_PossiblyExtinct_WithDoi() {
        const string json = """
        {"assessment_date":"2024-08-22T01:00:00.000+01:00","year_published":"2025","latest":true,
         "possibly_extinct":true,"possibly_extinct_in_the_wild":false,"sis_taxon_id":22732931,"criteria":"D",
         "url":"https://www.iucnredlist.org/species/22732931/250382422",
         "citation":"BirdLife International 2025. Pomarea mira. The IUCN Red List of Threatened Species 2025: e.T22732931A250382422. https://dx.doi.org/10.2305/IUCN.UK.2025-2.RLTS.T22732931A250382422.en. Accessed on 18 August 2026.",
         "assessment_id":250382422,
         "taxon":{"sis_id":22732931,"scientific_name":"Pomarea mira","kingdom_name":"ANIMALIA","infra_name":null,
                  "subpopulation_name":null,"species":true,"subpopulation":false,"infrarank":false},
         "red_list_category":{"version":"3.1","description":{"en":"Critically Endangered"},"code":"CR"},
         "credits":[
           {"credit_type_name":"assessor","full":"BirdLife International","value":["x1"]},
           {"credit_type_name":"evaluator","full":"Berryman, A.","value":["x1"]},
           {"credit_type_name":"contributor","full":"Raust, P., Meyer, J.-Y., Blanvillain, C., Temple, H., Westrip, J.R.S., Derhé, M., Mahood, S., Symes, A., Khwaja, N., Thibault, J.-C. & Ghestemme, T.","value":["x1","x2","x3","x4","x5","x6","x7","x8","x9","x10","x11"]},
           {"credit_type_name":"facilitators","full":"Gray, F. & Vine, J.","value":["x1","x2"]},
           {"credit_type_name":"institutions","full":"BirdLife International","value":["x1"]}],
         "errata":[],
         "scopes":[{"description":{"en":"Global"},"code":"1"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.Equal(22732931, a!.TaxonId);
        Assert.Equal(250382422, a.AssessmentId);
        Assert.Equal("Pomarea mira", a.ScientificName);
        Assert.False(a.IsInfrarank);
        Assert.Equal("CR", a.CategoryCode);
        Assert.True(a.PossiblyExtinct);
        Assert.False(a.PossiblyExtinctInTheWild);
        Assert.Equal("D", a.Criteria);
        Assert.Equal("3.1", a.CriteriaVersion);
        Assert.Equal(2025, a.YearPublished);
        Assert.Equal(new DateOnly(2024, 8, 22), a.AssessmentDate);
        Assert.Equal("https://www.iucnredlist.org/species/22732931/250382422", a.Url);
        Assert.Equal("BirdLife International 2025. Pomarea mira. The IUCN Red List of Threatened Species 2025: e.T22732931A250382422. https://dx.doi.org/10.2305/IUCN.UK.2025-2.RLTS.T22732931A250382422.en.", a.Citation);
        Assert.Equal("10.2305/IUCN.UK.2025-2.RLTS.T22732931A250382422.en", a.Doi);
        Assert.Equal(Downloaded, a.DownloadedAtUtc);
        Assert.False(a.IsAmended);

        Assert.Equal(new IucnCredit("assessor", "BirdLife International", 1), a.Credits[0]);
        Assert.Equal(new IucnCredit("evaluator", "Berryman, A.", 1), a.Credits[1]);
        var contributors = a.Credits.Where(c => c.Type == "contributor").ToList();
        Assert.Equal(11, contributors.Count);
        Assert.Equal(new IucnCredit("contributor", "Meyer, J.-Y.", 2), contributors[1]);
        Assert.Equal(new IucnCredit("contributor", "Ghestemme, T.", 11), contributors[10]);
        Assert.Equal(new[] { "Gray, F.", "Vine, J." }, a.Credits.Where(c => c.Type == "facilitators").Select(c => c.Name));
        Assert.Equal(new IucnCredit("institutions", "BirdLife International", 1), a.Credits[^1]);
    }

    [Fact]
    public void Parse_Subspecies_WithGroupAssessor() {
        const string json = """
        {"assessment_date":"2016-12-17T00:00:00.000+00:00","year_published":"2017","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":false,"sis_taxon_id":809,
         "criteria":"B1ab(i,ii,iii,iv)+2ab(i,ii,iii,iv)","url":"https://www.iucnredlist.org/species/809/3145291",
         "citation":"IUCN SSC Antelope Specialist Group 2017. Alcelaphus buselaphus ssp. swaynei. The IUCN Red List of Threatened Species 2017: e.T809A3145291. Accessed on 20 August 2026.",
         "assessment_id":3145291,
         "taxon":{"sis_id":809,"scientific_name":"Alcelaphus buselaphus ssp. swaynei","kingdom_name":"ANIMALIA",
                  "infra_name":"swaynei","subpopulation_name":null,"species":false,"subpopulation":false,"infrarank":true},
         "red_list_category":{"version":"3.1","description":{"en":"Endangered"},"code":"EN"},
         "credits":[
           {"credit_type_name":"assessor","full":"IUCN SSC Antelope Specialist Group","value":["x1"]},
           {"credit_type_name":"evaluator","full":"Cooke, R.","value":["x1"]}],
         "errata":[],
         "scopes":[{"description":{"en":"Global"},"code":"1"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.True(a!.IsInfrarank);
        Assert.Equal("Alcelaphus buselaphus ssp. swaynei", a.ScientificName);
        Assert.Equal("EN", a.CategoryCode);
        Assert.Equal(new DateOnly(2016, 12, 17), a.AssessmentDate);
        Assert.Equal("IUCN SSC Antelope Specialist Group 2017. Alcelaphus buselaphus ssp. swaynei. The IUCN Red List of Threatened Species 2017: e.T809A3145291.", a.Citation);
        Assert.Null(a.Doi);
        Assert.Equal(
            new[] { new IucnCredit("assessor", "IUCN SSC Antelope Specialist Group", 1), new IucnCredit("evaluator", "Cooke, R.", 1) },
            a.Credits);
    }

    [Fact]
    public void Parse_RegionalOnlyAssessment_ReturnsNull() {
        // Erebia ottomana: its latest assessment is the European one; the taxon has no global assessment.
        const string json = """
        {"assessment_date":"2023-04-18T01:00:00.000+01:00","year_published":"2025","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":false,"sis_taxon_id":7982,"criteria":null,
         "url":"https://www.iucnredlist.org/species/7982/211379133",
         "citation":"van Swaay, C., Ellis, S. & Warren, M. 2025. Erebia ottomana (Europe assessment). The IUCN Red List of Threatened Species 2025: e.T7982A211379133. Accessed on 18 August 2026.",
         "assessment_id":211379133,
         "taxon":{"sis_id":7982,"scientific_name":"Erebia ottomana","infrarank":false},
         "red_list_category":{"version":"3.1","description":{"en":"Least Concern"},"code":"LC"},
         "credits":[],"errata":[],
         "scopes":[{"description":{"en":"Europe"},"code":"2"}]}
        """;

        Assert.Null(IucnAssessmentCitationParser.Parse(json, Downloaded));
    }

    [Fact]
    public void Parse_AmendedAssessment_KeepsAmendmentYearAndOriginalDate() {
        // Ahaetulla nasuta: "amended version of 2021 assessment", published 2025, assessed 2019.
        const string json = """
        {"assessment_date":"2019-09-02T01:00:00.000+01:00","year_published":"2025","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":false,"sis_taxon_id":172707,"criteria":null,
         "url":"https://www.iucnredlist.org/species/172707/286750830",
         "citation":"Stuart, B.L., Grismer, L., Auliya, M., Chan-Ard, T., Srinivasulu, C., Srinivasulu, B., Mohapatra, P. & Achyuthan, N.S. 2025. Ahaetulla nasuta (amended version of 2021 assessment). The IUCN Red List of Threatened Species 2025: e.T172707A286750830. Accessed on 18 August 2026.",
         "assessment_id":286750830,
         "taxon":{"sis_id":172707,"scientific_name":"Ahaetulla nasuta","infrarank":false},
         "red_list_category":{"version":"3.1","description":{"en":"Least Concern"},"code":"LC"},
         "credits":[
           {"credit_type_name":"assessor","full":"Stuart, B.L., Grismer, L., Auliya, M., Chan-Ard, T., Srinivasulu, C., Srinivasulu, B., Mohapatra, P. & Achyuthan, N.S.","value":["x1","x2","x3","x4","x5","x6","x7","x8"]},
           {"credit_type_name":"evaluator","full":"Cox, N.A.","value":["x1"]},
           {"credit_type_name":"facilitators","full":"Bowles, P.","value":["x1"]}],
         "errata":[{"reason":"The assessment date has been corrected to 2019, the time of the final workshop which assessed this species under the concept described in the published Taxonomic Note (incorporating a taxonomic change made in 2017). The map has been corrected accordingly."}],
         "scopes":[{"description":{"en":"Global"},"code":"1"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.True(a!.IsAmended);
        Assert.Equal(2025, a.YearPublished);
        Assert.Equal(new DateOnly(2019, 9, 2), a.AssessmentDate);
        Assert.Null(a.Criteria);
        var assessors = a.Credits.Where(c => c.Type == "assessor").ToList();
        Assert.Equal(
            new[] { "Stuart, B.L.", "Grismer, L.", "Auliya, M.", "Chan-Ard, T.", "Srinivasulu, C.", "Srinivasulu, B.", "Mohapatra, P.", "Achyuthan, N.S." },
            assessors.Select(c => c.Name));
        Assert.Equal(Enumerable.Range(1, 8), assessors.Select(c => c.Order));
    }

    [Fact]
    public void Parse_PossiblyExtinctInTheWild_PutsAssessorsFirstAndSplitsConfirmedInstitutions() {
        // Gracupica jalla lists its evaluator before its assessor in the payload.
        const string json = """
        {"assessment_date":"2024-12-17T00:00:00.000+00:00","year_published":"2025","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":true,"sis_taxon_id":103890801,"criteria":"D",
         "url":"https://www.iucnredlist.org/species/103890801/224012768",
         "citation":"BirdLife International 2025. Gracupica jalla. The IUCN Red List of Threatened Species 2025: e.T103890801A224012768. Accessed on 18 August 2026.",
         "assessment_id":224012768,
         "taxon":{"sis_id":103890801,"scientific_name":"Gracupica jalla","infrarank":false},
         "red_list_category":{"version":"3.1","description":{"en":"Critically Endangered"},"code":"CR"},
         "credits":[
           {"credit_type_name":"evaluator","full":"Vine, J.","value":["x1"]},
           {"credit_type_name":"assessor","full":"BirdLife International","value":["x1"]},
           {"credit_type_name":"facilitators","full":"Berryman, A.","value":["x1"]},
           {"credit_type_name":"institutions","full":"BirdLife International & IUCN SSC Asian Songbird Trade Specialist Group","value":["x1","x2"]}],
         "errata":[],
         "scopes":[{"description":{"en":"Global"},"code":"1"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.True(a!.PossiblyExtinctInTheWild);
        Assert.False(a.PossiblyExtinct);
        Assert.Equal(
            new[] {
                new IucnCredit("assessor", "BirdLife International", 1),
                new IucnCredit("evaluator", "Vine, J.", 1),
                new IucnCredit("facilitators", "Berryman, A.", 1),
                new IucnCredit("institutions", "BirdLife International", 1),
                new IucnCredit("institutions", "IUCN SSC Asian Songbird Trade Specialist Group", 2),
            },
            a.Credits);
    }

    [Fact]
    public void Parse_Criteria23_ErrataAndEmptyValueArray() {
        const string json = """
        {"assessment_date":"1998-01-01T00:00:00.000+00:00","year_published":"2025","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":false,"sis_taxon_id":32226,"criteria":null,
         "url":"https://www.iucnredlist.org/species/32226/275116864",
         "citation":"Assi, A. 2025. Byttneria ivorensis (amended version of 1998 assessment). The IUCN Red List of Threatened Species 2025: e.T32226A275116864. Accessed on 18 August 2026.",
         "assessment_id":275116864,
         "taxon":{"sis_id":32226,"scientific_name":"Byttneria ivorensis","infrarank":false},
         "red_list_category":{"version":"2.3","description":{"en":"Extinct"},"code":"EX"},
         "credits":[{"credit_type_name":"assessor","full":"Assi, A.","value":[]}],
         "errata":[{"reason":"The assessment has been corrected from tree to shrub."}],
         "scopes":[{"description":{"en":"Global"},"code":"1"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.Equal("EX", a!.CategoryCode);
        Assert.Equal("2.3", a.CriteriaVersion);
        Assert.True(a.IsAmended);
        Assert.Equal(new[] { new IucnCredit("assessor", "Assi, A.", 1) }, a.Credits);
    }

    [Fact]
    public void Parse_RepeatedCreditsBlock_DeduplicatesPerType() {
        // Achondrostoma arcasii: the payload repeats its whole credits list. Its scopes are Global + Europe.
        // An errata version keeps the original year_published (2024), unlike an amended version.
        const string json = """
        {"assessment_date":"2023-11-15T00:00:00.000+00:00","year_published":"2024","latest":true,
         "possibly_extinct":false,"possibly_extinct_in_the_wild":false,"sis_taxon_id":275787647,"criteria":"A2ace",
         "url":"https://www.iucnredlist.org/species/275787647/275869011",
         "citation":"Ford, M. 2024. Achondrostoma arcasii (errata version published in 2025). The IUCN Red List of Threatened Species 2024: e.T275787647A275869011. Accessed on 18 August 2026.",
         "assessment_id":275869011,
         "taxon":{"sis_id":275787647,"scientific_name":"Achondrostoma arcasii","infrarank":false},
         "red_list_category":{"version":"3.1","description":{"en":"Near Threatened"},"code":"NT"},
         "credits":[
           {"credit_type_name":"assessor","full":"Ford, M.","value":["x1"]},
           {"credit_type_name":"evaluator","full":"Clavero, M., Perea, S., Filipe, A.F., Magalhães, M.F., Ribeiro, F., Doadrio, I. & Freyhof, J.","value":["x1","x2","x3","x4","x5","x6","x7"]},
           {"credit_type_name":"contributor","full":"Crivelli, A.J.","value":["x1"]},
           {"credit_type_name":"assessor","full":"Ford, M.","value":["x1"]},
           {"credit_type_name":"evaluator","full":"Clavero, M., Perea, S., Filipe, A.F., Magalhães, M.F., Ribeiro, F., Doadrio, I. & Freyhof, J.","value":["x1","x2","x3","x4","x5","x6","x7"]},
           {"credit_type_name":"contributor","full":"Crivelli, A.J.","value":["x1"]}],
         "errata":[{"reason":"This errata version of the assessment was created to publish the distribution map for this species."}],
         "scopes":[{"description":{"en":"Global"},"code":"1"},{"description":{"en":"Europe"},"code":"2"}]}
        """;

        var a = IucnAssessmentCitationParser.Parse(json, Downloaded);

        Assert.NotNull(a);
        Assert.Equal(2024, a!.YearPublished);
        Assert.True(a.IsAmended);
        Assert.Equal(9, a.Credits.Count);
        Assert.Single(a.Credits, c => c.Type == "assessor");
        Assert.Equal(7, a.Credits.Count(c => c.Type == "evaluator"));
        Assert.Equal(new IucnCredit("evaluator", "Magalhães, M.F.", 4), a.Credits.Single(c => c.Name == "Magalhães, M.F."));
    }

    [Fact]
    public void Parse_MissingFieldsOrBadJson_ReturnsNull() {
        Assert.Null(IucnAssessmentCitationParser.Parse("not json", Downloaded));
        Assert.Null(IucnAssessmentCitationParser.Parse("""{"assessment_id":1,"scopes":[{"code":"1"}]}""", Downloaded));
    }

    // ------------------------------------------------------------ credits blocks and value[] counts

    private static IReadOnlyList<IucnCredit> Credits(string creditsJson) {
        using var document = System.Text.Json.JsonDocument.Parse($$"""{"credits":{{creditsJson}}}""");
        return IucnAssessmentCitationParser.ReadCredits(document.RootElement);
    }

    [Fact]
    public void ReadCredits_KeepsTwoPeopleWithTheSameShortName() {
        // Commiphora ogadensis (aid 223078443): Shambel Alemu and Sisay Alemu.
        var credits = Credits("""
            [{"credit_type_name":"assessor","full":"Alemu, S., Alemu, S., Atnafu, H., Awas, T., Birhanu Belay, Sebsebe Demissew, Luke, W.R.Q., Musili, P., Nemomissa, S., Bahdon, J. & Efrata Mekbib",
              "value":["Birhanu Belay (a)","Shambel Alemu (b)","Jamal Bahdon (c)","Sileshi Nemomissa (d)","Sebsebe Demissew (e)","Quentin Luke (f)","Paul Musili (g)","Tesfaye Awas (h)","Hailu Atnafu (i)","Efrata Mekbib (j)","Sisay Alemu (k)"]}]
            """);

        Assert.Equal(11, credits.Count);
        Assert.Equal(new[] { 1, 2 }, credits.Where(c => c.Name == "Alemu, S.").Select(c => c.Order));
    }

    [Fact]
    public void ReadCredits_ValueRepeatingOnePersonDoesNotConfirmARepeat() {
        // Evaluators of aid 176828359: value[] lists Suzanne Livingstone twice, word for word.
        var credits = Credits("""
            [{"credit_type_name":"evaluator","full":"Livingstone, S., Livingstone, S. & Neubert, E.",
              "value":["Eike Neubert (Bern)","Suzanne Livingstone (GMSA)","Suzanne Livingstone (GMSA)"]}]
            """);

        Assert.Equal(new[] { "Livingstone, S.", "Neubert, E." }, credits.Select(c => c.Name));
    }

    [Fact]
    public void ReadCredits_RepeatedBlockAddsOnlyNamesNotYetHeld() {
        // Shapes seen in aids 221328142 and 275869011: the same assessor block reordered, an
        // evaluator block repeated with one more person, and a block with two Harolds repeated whole.
        var credits = Credits("""
            [{"credit_type_name":"assessor","full":"Wood, T.J., Devalez, J. & Kierat, J.","value":["a","b","c"]},
             {"credit_type_name":"evaluator","full":"Ghisbain, G., Put, S. & Bellotto, V.","value":["a","b","c"]},
             {"credit_type_name":"contributor","full":"Harold, A. & Harold, A.","value":["Anthony Harold","Antony Harold"]},
             {"credit_type_name":"assessor","full":"Wood, T.J., Kierat, J. & Devalez, J.","value":["a","b","c"]},
             {"credit_type_name":"evaluator","full":"Ghisbain, G., Michez, D., Put, S. & Bellotto, V.","value":["a","b","c","d"]},
             {"credit_type_name":"contributor","full":"Harold, A. & Harold, A.","value":["Anthony Harold","Antony Harold"]}]
            """);

        Assert.Equal(new[] { "Wood, T.J.", "Devalez, J.", "Kierat, J." }, credits.Where(c => c.Type == "assessor").Select(c => c.Name));
        Assert.Equal(new[] { "Ghisbain, G.", "Put, S.", "Bellotto, V.", "Michez, D." }, credits.Where(c => c.Type == "evaluator").Select(c => c.Name));
        Assert.Equal(new[] { "Harold, A.", "Harold, A." }, credits.Where(c => c.Type == "contributor").Select(c => c.Name));
    }

    // ------------------------------------------------------------ picking the assessment

    private const string GlobalScope = """[{"description":{"en":"Global"},"code":"1"}]""";
    private const string EuropeScope = """[{"description":{"en":"Europe"},"code":"2"}]""";

    private static string TaxaPayload(string taxon, params string[] assessments) =>
        $$"""{"sis_id":1,"taxon":{{{taxon}}},"assessments":[{{string.Join(",", assessments)}}]}""";

    private static string Header(long id, bool latest, string year, string scopes) =>
        $$"""{"assessment_id":{{id}},"latest":{{(latest ? "true" : "false")}},"year_published":"{{year}}","scopes":{{scopes}}}""";

    private const string SpeciesTaxon = """ "scientific_name":"Acinonyx jubatus","species":true,"subpopulation":false,"infrarank":false,"infra_name":null,"subpopulation_name":null """;

    [Fact]
    public void Pick_LatestGlobalWinsOverLatestRegional() {
        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(SpeciesTaxon,
            Header(259025524, true, "2025", EuropeScope),
            Header(13034035, true, "2008", GlobalScope),
            Header(13033669, false, "1996", GlobalScope)));

        Assert.Equal(IucnGlobalAssessmentReader.TaxonPickOutcome.Species, pick.Outcome);
        Assert.Equal(13034035, pick.AssessmentId);
        Assert.Equal(1, pick.LatestGlobalCount);
    }

    [Fact]
    public void Pick_OlderGlobalIsNotLatest_NoGlobal() {
        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(SpeciesTaxon,
            Header(211379133, true, "2025", EuropeScope),
            Header(53714322, false, "2014", GlobalScope)));

        Assert.Equal(IucnGlobalAssessmentReader.TaxonPickOutcome.NoGlobal, pick.Outcome);
    }

    [Fact]
    public void Pick_NothingLatest_NoLatest() {
        // Bettongia penicillata: global assessments exist but none is flagged latest; not in the 2026-1 CSV.
        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(SpeciesTaxon,
            Header(258664485, false, "2025", GlobalScope),
            Header(21961347, false, "2016", GlobalScope)));

        Assert.Equal(IucnGlobalAssessmentReader.TaxonPickOutcome.NoLatest, pick.Outcome);
    }

    [Fact]
    public void Pick_BlankScopeIsNotGlobal() {
        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(SpeciesTaxon, Header(5, true, "2020", "[]")));

        Assert.Equal(IucnGlobalAssessmentReader.TaxonPickOutcome.NoGlobal, pick.Outcome);
        Assert.True(pick.LatestHasBlankScope);
    }

    [Fact]
    public void Pick_TwoLatestGlobal_TakesMostRecentlyPublished() {
        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(SpeciesTaxon,
            Header(10, true, "2019", GlobalScope),
            Header(9, true, "2021", GlobalScope)));

        Assert.Equal(9, pick.AssessmentId);
        Assert.Equal(2, pick.LatestGlobalCount);
    }

    [Theory]
    [InlineData("Alcelaphus buselaphus ssp. swaynei", true, false, null, "Subspecies")]
    [InlineData("Warburgia ugandensis subsp. longifolia", true, false, null, "Subspecies")]
    [InlineData("Quercus robur var. fastigiata", true, false, null, "Variety")]
    [InlineData("Zea mays subsp. mexicana Chalco subpopulation", false, true, "Chalco subpopulation", "Subpopulation")]
    public void Pick_ClassifiesRank(string name, bool infrarank, bool subpopulation, string? subpopulationName, string expectedOutcome) {
        var expected = Enum.Parse<IucnGlobalAssessmentReader.TaxonPickOutcome>(expectedOutcome);
        var taxon = $$""" "scientific_name":"{{name}}","species":false,"infrarank":{{(infrarank ? "true" : "false")}},"subpopulation":{{(subpopulation ? "true" : "false")}},"infra_name":"x","subpopulation_name":{{(subpopulationName is null ? "null" : $"\"{subpopulationName}\"")}} """;

        var pick = IucnGlobalAssessmentReader.PickFromTaxaPayload(TaxaPayload(taxon, Header(1, true, "2020", GlobalScope)));

        Assert.Equal(expected, pick.Outcome);
    }

    // ------------------------------------------------------------ reader over a cache file

    [Fact]
    public void ReadAll_EmitsLatestGlobalAndCountsTheRest() {
        var path = Path.Combine(Path.GetTempPath(), $"iucn-global-reader-{Guid.NewGuid():N}.sqlite");
        try {
            using (var connection = new SqliteConnection($"Data Source={path}")) {
                connection.Open();
                Exec(connection, """
                    CREATE TABLE taxa (id INTEGER PRIMARY KEY AUTOINCREMENT, root_sis_id INTEGER NOT NULL UNIQUE, json TEXT NOT NULL);
                    CREATE TABLE assessments (id INTEGER PRIMARY KEY AUTOINCREMENT, assessment_id INTEGER NOT NULL UNIQUE,
                        sis_id INTEGER NOT NULL, downloaded_at TEXT NOT NULL, json TEXT NOT NULL);
                    """);
                // 1: species, emitted. 2: regional only. 3: variety. 4: latest in the taxa payload but
                // the assessment payload says otherwise. 5: picked assessment not downloaded.
                AddTaxon(connection, 1, SpeciesTaxon, Header(100, true, "2020", GlobalScope), Header(101, true, "2022", EuropeScope));
                AddTaxon(connection, 2, SpeciesTaxon, Header(200, true, "2020", EuropeScope));
                AddTaxon(connection, 3, """ "scientific_name":"Quercus robur var. fastigiata","infrarank":true,"infra_name":"fastigiata" """, Header(300, true, "2020", GlobalScope));
                AddTaxon(connection, 4, """ "scientific_name":"Renea moutonii ssp. singularis","infrarank":true,"infra_name":"singularis" """, Header(400, true, "1996", GlobalScope));
                AddTaxon(connection, 5, SpeciesTaxon, Header(500, true, "2020", GlobalScope));
                AddAssessment(connection, 100, 1, true, "2026-08-20T21:01:51.0515285Z");
                AddAssessment(connection, 101, 1, true, "2026-08-20T21:01:51.0515285Z", EuropeScope);
                AddAssessment(connection, 400, 4, false, "2026-08-21T21:00:40.4539813Z");
            }
            SqliteConnection.ClearAllPools();

            var counts = new IucnGlobalAssessmentReadCounts();
            List<IucnGlobalAssessment> emitted;
            using (var reader = IucnGlobalAssessmentReader.Open(path)) {
                emitted = reader.ReadAll(counts, CancellationToken.None).ToList();
            }

            var only = Assert.Single(emitted);
            Assert.Equal(100, only.AssessmentId);
            Assert.Equal(new DateTime(2026, 8, 20, 21, 1, 51, DateTimeKind.Utc).AddTicks(515285), only.DownloadedAtUtc);
            Assert.Equal(5, counts.TaxaSeen);
            Assert.Equal(1, counts.EmittedSpecies);
            Assert.Equal(0, counts.EmittedSubspecies);
            Assert.Equal(1, counts.NoGlobalAssessment);
            Assert.Equal(1, counts.SkippedVarieties);
            Assert.Equal(1, counts.StaleTaxaPayload);
            Assert.Equal(1, counts.AssessmentNotCached);
        } finally {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    private static void Exec(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void AddTaxon(SqliteConnection connection, long rootSisId, string taxon, params string[] assessments) {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO taxa(root_sis_id, json) VALUES (@root, @json)";
        command.Parameters.AddWithValue("@root", rootSisId);
        command.Parameters.AddWithValue("@json", TaxaPayload(taxon, assessments));
        command.ExecuteNonQuery();
    }

    private static void AddAssessment(SqliteConnection connection, long assessmentId, long sisId, bool latest, string downloadedAt, string scopes = GlobalScope) {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO assessments(assessment_id, sis_id, downloaded_at, json) VALUES (@id, @sis, @downloaded, @json)";
        command.Parameters.AddWithValue("@id", assessmentId);
        command.Parameters.AddWithValue("@sis", sisId);
        command.Parameters.AddWithValue("@downloaded", downloadedAt);
        command.Parameters.AddWithValue("@json", $$"""
            {"assessment_id":{{assessmentId}},"sis_taxon_id":{{sisId}},"latest":{{(latest ? "true" : "false")}},
             "year_published":"2020","taxon":{"sis_id":{{sisId}},"scientific_name":"Acinonyx jubatus","infrarank":false},
             "red_list_category":{"version":"3.1","code":"VU"},"citation":"Someone, A. 2020. Acinonyx jubatus. Accessed on 1 January 2026.",
             "credits":[{"credit_type_name":"assessor","full":"Someone, A.","value":["x1"]}],"errata":[],"scopes":{{scopes}}}
            """);
        command.ExecuteNonQuery();
    }
}
