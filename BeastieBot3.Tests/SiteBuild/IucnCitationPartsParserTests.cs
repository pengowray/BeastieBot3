using System.Text.Json;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using BeastieBot3.WikidataEdits;

namespace BeastieBot3.Tests.SiteBuild;

// Pins how one cached assessment payload becomes the citation parts the public site renders
// {{cite iucn}} from. Fixtures are real 2026-1 API payloads cut down to the fields the parser reads
// (assessor credit only); value[] entries are replaced by distinct placeholders, keeping their count.
public class IucnCitationPartsParserTests {
    private static readonly DateTime Downloaded = new(2026, 8, 20, 5, 43, 7, DateTimeKind.Utc);

    private static IucnCitationParse Parse(string json, IReadOnlyCollection<long>? predecessors = null) {
        using var document = JsonDocument.Parse(json);
        return IucnCitationPartsParser.Parse(document.RootElement, Downloaded, predecessors);
    }

    private static IucnCitationParts Parts(string json, IReadOnlyCollection<long>? predecessors = null) {
        var parse = Parse(json, predecessors);
        Assert.Equal(CitationParseFailure.None, parse.Failure);
        return Assert.IsType<IucnCitationParts>(parse.Parts);
    }

    private static CitationAuthor Person(string display, string last, string initials) =>
        new(CitationAuthorKind.Person, display, last, initials);

    private static CitationAuthor Organisation(string display) => new(CitationAuthorKind.Organisation, display);

    private static CitationAuthor Verbatim(string display) => new(CitationAuthorKind.Verbatim, display);

    private const string GlobalScope = """[{"description":{"en":"Global"},"code":"1"}]""";

    // A payload with the fields the parser reads.
    private static string Payload(long taxonId, long assessmentId, string? year, string citation, string scientificName,
        string? assessor = null, int values = 0, string scopes = GlobalScope, string? subpopulation = null) {
        var credits = assessor is null
            ? "[]"
            : $$"""[{"credit_type_name":"assessor","full":{{JsonSerializer.Serialize(assessor)}},"value":[{{string.Join(",", Enumerable.Range(1, values).Select(i => $"\"v{i}\""))}}]}]""";
        return $$"""
            {"assessment_id":{{assessmentId}},"sis_taxon_id":{{taxonId}},"year_published":{{(year is null ? "null" : $"\"{year}\"")}},"latest":true,
             "citation":{{JsonSerializer.Serialize(citation)}},
             "taxon":{"sis_id":{{taxonId}},"scientific_name":{{JsonSerializer.Serialize(scientificName)}},"subpopulation_name":{{(subpopulation is null ? "null" : JsonSerializer.Serialize(subpopulation))}}},
             "credits":{{credits}},"errata":[],"scopes":{{scopes}}}
            """;
    }

    // ------------------------------------------------------------ the whole record

    [Fact]
    public void Dugong_AmendedVersion_KeepsItsCitationDoiFromTheAmendedRelease() {
        const string json = """
            {"assessment_id": 160756767, "sis_taxon_id": 6909, "year_published": "2019", "latest": true,
             "citation": "Marsh, H. & Sobtzick, S. 2019. Dugong dugon (amended version of 2015 assessment). The IUCN Red List of Threatened Species 2019: e.T6909A160756767. https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en. Accessed on 20 August 2026.",
             "taxon": {"sis_id": 6909, "scientific_name": "Dugong dugon", "subpopulation_name": null, "infra_name": null, "species": true, "subpopulation": false, "infrarank": false},
             "credits": [{"credit_type_name": "assessor", "full": "Marsh, H. & Sobtzick, S.", "value": ["v1", "v2"]}],
             "errata": [{"reason": "x"}], "scopes": [{"description": {"en": "Global"}, "code": "1"}]}
            """;

        var parse = Parse(json);
        var parts = Assert.IsType<IucnCitationParts>(parse.Parts);

        Assert.Equal(new IucnCitationParts {
            TaxonId = 6909,
            AssessmentId = 160756767,
            Year = 2019,
            ScientificName = "Dugong dugon",
            AmendsYear = 2015,
            Authors = parts.Authors,
            Doi = "10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en",
            DoiSource = DoiSource.Citation,
            IucnCitationText = "Marsh, H. & Sobtzick, S. 2019. Dugong dugon (amended version of 2015 assessment). The IUCN Red List of Threatened Species 2019: e.T6909A160756767. https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en.",
            DownloadedAtUtc = Downloaded,
        }, parts);
        Assert.Equal(new[] { Person("Marsh, H.", "Marsh", "H."), Person("Sobtzick, S.", "Sobtzick", "S.") }, parts.Authors);
        Assert.Equal("e.T6909A160756767", parts.ArticleNumber);
        Assert.Equal(DoiVerdict.Accepted, parse.CitationDoiVerdict);
        Assert.True(parse.HasAmendedAnnotation);
        Assert.Equal(CitationAuthorSource.AssessorCredit, parse.AuthorSource);
        Assert.Equal(IucnAssessmentCitationParser.CreditSplitRule.Strict, parse.SplitRule);
    }

    [Fact]
    public void GiantPanda_ErrataVersion_HasTheErrataYearAndNoCitationDoi() {
        const string json = """
            {"assessment_id": 121745669, "sis_taxon_id": 712, "year_published": "2016", "latest": true,
             "citation": "Swaisgood, R., Wang, D. & Wei, F. 2016. Ailuropoda melanoleuca (errata version published in 2017). The IUCN Red List of Threatened Species 2016: e.T712A121745669. Accessed on 20 August 2026.",
             "taxon": {"sis_id": 712, "scientific_name": "Ailuropoda melanoleuca", "subpopulation_name": null},
             "credits": [{"credit_type_name": "assessor", "full": "Swaisgood, R., Wang, D. & Wei, F.", "value": ["v1", "v2", "v3"]}],
             "errata": [{"reason": "x"}], "scopes": [{"description": {"en": "Global"}, "code": "1"}]}
            """;

        var parse = Parse(json, new long[] { 45033386, 102080907 });
        var parts = parse.Parts!;

        Assert.Equal(2016, parts.Year);
        Assert.Equal(2017, parts.ErrataYear);
        Assert.Null(parts.AmendsYear);
        Assert.Null(parts.Doi);
        Assert.Equal(DoiSource.None, parts.DoiSource);
        Assert.Equal(DoiVerdict.Missing, parse.CitationDoiVerdict);
        Assert.Equal(new[] { "Swaisgood, R.", "Wang, D.", "Wei, F." }, parts.Authors.Select(a => a.Display));
        // The site build adds GBIF's DOI for the replaced assessment.
        Assert.Equal(new DoiChoice("10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", DoiSource.Gbif),
            IucnDoiSelector.Select(parts, null, "10.2305/IUCN.UK.2016-2.RLTS.T712A45033386.en", null, new long[] { 45033386, 102080907 }));
    }

    [Fact]
    public void ErrataVersion_CitationDoiOfTheReplacedAssessment_AcceptedWithPredecessors() {
        // Lepidium orbiculare: errata versions published 2015 to 2018 carry the replaced assessment's DOI.
        var json = Payload(80230301, 115510951, "2016",
            "Clark, M. 2016. Lepidium orbiculare (errata version published in 2017). The IUCN Red List of Threatened Species 2016: e.T80230301A115510951. https://dx.doi.org/10.2305/IUCN.UK.2016-1.RLTS.T80230301A80230334.en. Accessed on 20 August 2026.",
            "Lepidium orbiculare", "Clark, M.", 1);

        var accepted = Parse(json, new long[] { 80230334 });
        Assert.Equal("10.2305/IUCN.UK.2016-1.RLTS.T80230301A80230334.en", accepted.Parts!.Doi);
        Assert.Equal(DoiVerdict.AcceptedPredecessor, accepted.CitationDoiVerdict);

        // Without the taxon's assessment list the replaced assessment can't be confirmed.
        var unconfirmed = Parse(json);
        Assert.Null(unconfirmed.Parts!.Doi);
        Assert.Equal(DoiVerdict.AssessmentMismatch, unconfirmed.CitationDoiVerdict);
        Assert.Equal("10.2305/IUCN.UK.2016-1.RLTS.T80230301A80230334.en", unconfirmed.CitationDoi);
    }

    [Fact]
    public void CitationDoiOfAnUnrelatedEarlierAssessment_Rejected() {
        // Gryllotalpa major: IUCN's citation names the 2000 assessment, not the 1996 one the errata replaced.
        var json = Payload(9530, 85829841, "1996",
            "Orthopteroid Specialist Group 1996. Gryllotalpa major (errata version published in 2015). The IUCN Red List of Threatened Species 1996: e.T9530A85829841. https://dx.doi.org/10.2305/IUCN.UK.2000.RLTS.T9530A12998967.en. Accessed on 21 August 2026.",
            "Gryllotalpa major", "Orthopteroid Specialist Group", 1);

        var parse = Parse(json, new long[] { 12998880 });

        Assert.Null(parse.Parts!.Doi);
        Assert.Equal(DoiVerdict.AssessmentMismatch, parse.CitationDoiVerdict);
        Assert.Equal(new[] { Organisation("Orthopteroid Specialist Group") }, parse.Parts.Authors);
    }

    [Fact]
    public void SpanishDoi_AndASecondSurnameWrittenAsAnInitial() {
        var parts = Parts(Payload(218171971, 286370469, "2025",
            "Lopez-Gallego, C. & Morales M, P.A. 2025. Piper perverrucosum (amended version of 2024 assessment). The IUCN Red List of Threatened Species 2025: e.T218171971A286370469. https://dx.doi.org/10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es. Accessed on 18 August 2026.",
            "Piper perverrucosum", "Lopez-Gallego, C. & Morales M, P.A.", 2));

        Assert.Equal("10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es", parts.Doi);
        Assert.Equal(2024, parts.AmendsYear);
        Assert.Equal(new[] { Person("Lopez-Gallego, C.", "Lopez-Gallego", "C."), Person("Morales M, P.A.", "Morales M", "P.A.") }, parts.Authors);
    }

    // ------------------------------------------------------------ title annotations

    [Fact]
    public void RegionalErrataVersion() {
        var parts = Parts(Payload(211166544, 283653276, "2025",
            "van Swaay, C., Ellis, S. & Warren, M. 2025. Spialia sertorius (Europe assessment) (errata version published in 2025). The IUCN Red List of Threatened Species 2025: e.T211166544A283653276. Accessed on 18 August 2026.",
            "Spialia sertorius", "van Swaay, C., Ellis, S. & Warren, M.", 3,
            scopes: """[{"description":{"en":"Europe"},"code":"2"}]"""));

        Assert.Equal("Spialia sertorius", parts.ScientificName);
        Assert.Equal("Europe", parts.RegionalScope);
        Assert.Equal(2025, parts.ErrataYear);
        Assert.Equal(Person("van Swaay, C.", "van Swaay", "C."), parts.Authors[0]);
    }

    [Fact]
    public void RegionalAmendedVersion() {
        var parts = Parts(Payload(19519, 278890780, "2025",
            "Russo, D. & Cistrone, L. 2025. Rhinolophus mehelyi (Europe assessment) (amended version of 2023 assessment). The IUCN Red List of Threatened Species 2025: e.T19519A278890780. https://dx.doi.org/10.2305/IUCN.UK.2025-2.RLTS.T19519A278890780.en. Accessed on 18 August 2026.",
            "Rhinolophus mehelyi", "Russo, D. & Cistrone, L.", 2,
            scopes: """[{"description":{"en":"Europe"},"code":"2"}]"""));

        Assert.Equal("Europe", parts.RegionalScope);
        Assert.Equal(2023, parts.AmendsYear);
        Assert.Equal(DoiSource.Citation, parts.DoiSource);
    }

    [Fact]
    public void AmendedVersionWithTheYearLeftOut() {
        // aid 125330816: "(amended version of  assessment)", with two spaces.
        var parse = Parse(Payload(17838, 125330816, "2018",
            "Contreras-Balderas, S. & Almada-Villela, P. 2018. Poeciliopsis sonoriensis (amended version of  assessment). The IUCN Red List of Threatened Species 2018: e.T17838A125330816. https://dx.doi.org/10.2305/IUCN.UK.2018-1.RLTS.T17838A125330816.en. Accessed on 21 August 2026.",
            "Poeciliopsis sonoriensis", "Contreras-Balderas, S. & Almada-Villela, P."));

        Assert.True(parse.HasAmendedAnnotation);
        Assert.Null(parse.Parts!.AmendsYear);
        Assert.Equal("10.2305/IUCN.UK.2018-1.RLTS.T17838A125330816.en", parse.Parts.Doi);
    }

    [Fact]
    public void ErrataVersionWithTheYearLeftOut() {
        var parse = Parse(Payload(284299267, 284300286, "2025",
            "El Zein, H. 2025. Astragalus trifoliolatus (errata version published in ). The IUCN Red List of Threatened Species 2025: e.T284299267A284300286. Accessed on 21 August 2026.",
            "Astragalus trifoliolatus", "El Zein, H.", 1));

        Assert.True(parse.HasErrataAnnotation);
        Assert.Null(parse.Parts!.ErrataYear);
        Assert.Equal(Person("El Zein, H.", "El Zein", "H."), parse.Parts.Authors.Single());
    }

    [Fact]
    public void Subpopulation_KeepsItsNameInTheTitle() {
        var parts = Parts(Payload(111341754, 111341757, "2019",
            "Ruíz Corral, J.A., de la Cruz Larios, L., Oliveros, O., Contreras, A. & Sánchez, J.J. 2019. Zea mays subsp. mexicana Durango subpopulation. The IUCN Red List of Threatened Species 2019: e.T111341754A111341757. Accessed on 20 August 2026.",
            "Zea mays subsp. mexicana Durango subpopulation",
            "Ruíz Corral, J.A., de la Cruz Larios, L., Oliveros, O., Contreras, A. & Sánchez, J.J.", 5,
            subpopulation: "Durango subpopulation"));

        Assert.Equal("Zea mays subsp. mexicana Durango subpopulation", parts.ScientificName);
        Assert.Equal("Durango", parts.SubpopulationName);
        Assert.Null(parts.RegionalScope);
        Assert.Equal(Person("de la Cruz Larios, L.", "de la Cruz Larios", "L."), parts.Authors[1]);
    }

    [Fact]
    public void Subspecies() {
        var parts = Parts(Payload(809, 3145291, "2017",
            "IUCN SSC Antelope Specialist Group 2017. Alcelaphus buselaphus ssp. swaynei. The IUCN Red List of Threatened Species 2017: e.T809A3145291. Accessed on 20 August 2026.",
            "Alcelaphus buselaphus ssp. swaynei", "IUCN SSC Antelope Specialist Group", 1));

        Assert.Equal("Alcelaphus buselaphus ssp. swaynei", parts.ScientificName);
        Assert.Null(parts.SubpopulationName);
        Assert.Equal(new[] { Organisation("IUCN SSC Antelope Specialist Group") }, parts.Authors);
    }

    [Fact]
    public void YearInsideTheAuthorList_IsNotTakenForThePublicationYear() {
        // aid 9303466: "Madagascar 2001." comes before the real year, 2004.
        var parts = Parts(Payload(21628, 9303466, "2004",
            "Loiselle, P. & participants of the CBSG/ANGAP CAMP \"Faune de Madagascar\" workshop, Mantasoa, Madagascar 2001. 2004. Teramulus kieneri. The IUCN Red List of Threatened Species 2004: e.T21628A9303466. Accessed on 23 August 2026.",
            "Teramulus kieneri",
            "Loiselle, P. & participants of the CBSG/ANGAP CAMP \"Faune de Madagascar\" workshop, Mantasoa, Madagascar 2001."));

        Assert.Equal(2004, parts.Year);
        Assert.Equal("Teramulus kieneri", parts.ScientificName);
        Assert.Single(parts.Authors);
    }

    // ------------------------------------------------------------ authors

    [Fact]
    public void NoAuthors() {
        var parse = Parse(Payload(163790, 138283372, "2020",
            " 2020. Macromia euterpe. The IUCN Red List of Threatened Species 2020: e.T163790A138283372. Accessed on 20 August 2026.",
            "Macromia euterpe"));

        Assert.Empty(parse.Parts!.Authors);
        Assert.Equal(CitationAuthorSource.None, parse.AuthorSource);
        Assert.Equal("2020. Macromia euterpe. The IUCN Red List of Threatened Species 2020: e.T163790A138283372.", parse.Parts.IucnCitationText);
    }

    [Fact]
    public void NoAssessorCredit_ReadsTheAuthorsFromTheCitation() {
        var parse = Parse(Payload(4631, 147680762, "2020",
            "Allen, G.R., Hammer, M. & Kadarusman 2020. Chilatherina sentaniensis. The IUCN Red List of Threatened Species 2020: e.T4631A147680762. Accessed on 20 August 2026.",
            "Chilatherina sentaniensis"));

        Assert.Equal(CitationAuthorSource.CitationPrefix, parse.AuthorSource);
        // No value[] count to confirm "Kadarusman" as a name of its own, so the string stays whole.
        Assert.Equal(new[] { Verbatim("Allen, G.R., Hammer, M. & Kadarusman") }, parse.Parts!.Authors);
    }

    [Fact]
    public void SingleNamePerson_ConfirmedByCount() {
        var parse = Parse(Payload(4631, 147680762, "2020",
            "Allen, G.R., Hammer, M. & Kadarusman 2020. Chilatherina sentaniensis. The IUCN Red List of Threatened Species 2020: e.T4631A147680762. Accessed on 20 August 2026.",
            "Chilatherina sentaniensis", "Allen, G.R., Hammer, M. & Kadarusman", 3));

        Assert.Equal(IucnAssessmentCitationParser.CreditSplitRule.CountStandalone, parse.SplitRule);
        Assert.Equal(new[] { Person("Allen, G.R.", "Allen", "G.R."), Person("Hammer, M.", "Hammer", "M."), Verbatim("Kadarusman") }, parse.Parts!.Authors);
        Assert.Equal(new[] { AuthorNameShape.SurnameInitials, AuthorNameShape.SurnameInitials, AuthorNameShape.SingleName }, parse.AuthorShapes);
    }

    [Fact]
    public void EtAl_InHtml() {
        var parse = Parse(Payload(37427, 10053658, "1998",
            "Jaffré, T. <i>et al.</i> 1998. Oxera macrocalyx. The IUCN Red List of Threatened Species 1998: e.T37427A10053658. https://dx.doi.org/10.2305/IUCN.UK.1998.RLTS.T37427A10053658.en. Accessed on 21 August 2026.",
            "Oxera macrocalyx", "Jaffré, T. <i>et al.</i>"));

        var parts = parse.Parts!;
        Assert.True(parts.AuthorsEtAl);
        Assert.Equal(new[] { Person("Jaffré, T.", "Jaffré", "T.") }, parts.Authors);
        Assert.Equal(IucnAssessmentCitationParser.CreditSplitRule.EtAl, parse.SplitRule);
        Assert.StartsWith("Jaffré, T. et al. 1998. Oxera macrocalyx.", parts.IucnCitationText);
        Assert.Equal("10.2305/IUCN.UK.1998.RLTS.T37427A10053658.en", parts.Doi);
    }

    [Fact]
    public void EtAl_AfterAList() {
        var parts = Parts(Payload(40115, 10314691, "2000",
            "Schnell, D., Catling, P., Folkerts, G., Frost, C., Gardner, R., <i>et al.</i> 2000. Nepenthes northiana. The IUCN Red List of Threatened Species 2000: e.T40115A10314691. https://dx.doi.org/10.2305/IUCN.UK.2000.RLTS.T40115A10314691.en. Accessed on 21 August 2026.",
            "Nepenthes northiana", "Schnell, D., Catling, P., Folkerts, G., Frost, C., Gardner, R., <i>et al.</i>"));

        Assert.True(parts.AuthorsEtAl);
        Assert.Equal(new[] { "Schnell", "Catling", "Folkerts", "Frost", "Gardner" }, parts.Authors.Select(a => a.Last));
    }

    [Fact]
    public void WhitespaceOddities_AreCleanedFromNamesAndText() {
        // aid 200532952 has a narrow no-break space before the comma; 7606335 has "Riservato,E .\n";
        // 2796250 starts with a newline and ends its author list with two spaces.
        var narrow = Parts(Payload(200532832, 200532952, "2025",
            "Dombrowski , A. & Barker, A. 2025. Pandanus hystrix. The IUCN Red List of Threatened Species 2025: e.T200532832A200532952. Accessed on 18 August 2026.",
            "Pandanus hystrix", "Dombrowski , A. & Barker, A.", 2));
        var stray = Parts(Payload(178739, 7606335, "2010",
            "Anderson, S.C., Sindaco, R., Grieco, C., Ineich, I., Pupin, F. & Riservato,E .\n 2010. Trachylepis socotrana. The IUCN Red List of Threatened Species 2010: e.T178739A7606335. https://dx.doi.org/10.2305/IUCN.UK.2010-4.RLTS.T178739A7606335.en. Accessed on 21 August 2026.",
            "Trachylepis socotrana", "Anderson, S.C., Sindaco, R., Grieco, C., Ineich, I., Pupin, F. & Riservato,E .\n"));
        var newline = Parts(Payload(30758, 2796250, "2014",
            "\nLuna-Vega, I., Gonzalez-Espinosa, M.  2014. Magnolia schiedeana. The IUCN Red List of Threatened Species 2014: e.T30758A2796250. Accessed on 21 August 2026.",
            "Magnolia schiedeana", "\nLuna-Vega, I., Gonzalez-Espinosa, M. "));

        Assert.Equal(Person("Dombrowski, A.", "Dombrowski", "A."), narrow.Authors[0]);
        Assert.Equal(Person("Riservato, E.", "Riservato", "E."), stray.Authors[^1]);
        Assert.Equal(new[] { "Luna-Vega, I.", "Gonzalez-Espinosa, M." }, newline.Authors.Select(a => a.Display));
        Assert.Equal("Luna-Vega, I., Gonzalez-Espinosa, M. 2014. Magnolia schiedeana. The IUCN Red List of Threatened Species 2014: e.T30758A2796250.", newline.IucnCitationText);
    }

    [Fact]
    public void TwoPeopleWithTheSameShortName_AndEthiopianNamesKeptAsPublished() {
        // aid 223078443: Shambel Alemu and Sisay Alemu; Ethiopian names have no surname.
        var parse = Parse(Payload(133268514, 223078443, "2021",
            "Alemu, S., Alemu, S., Atnafu, H., Awas, T., Birhanu Belay, Sebsebe Demissew, Luke, W.R.Q., Musili, P., Nemomissa, S., Bahdon, J. & Efrata Mekbib 2021. Commiphora ogadensis (errata version published in 2022). The IUCN Red List of Threatened Species 2021: e.T133268514A223078443. Accessed on 19 August 2026.",
            "Commiphora ogadensis",
            "Alemu, S., Alemu, S., Atnafu, H., Awas, T., Birhanu Belay, Sebsebe Demissew, Luke, W.R.Q., Musili, P., Nemomissa, S., Bahdon, J. & Efrata Mekbib", 11));

        var authors = parse.Parts!.Authors;
        Assert.Equal(11, authors.Count);
        Assert.Equal(new[] { Person("Alemu, S.", "Alemu", "S."), Person("Alemu, S.", "Alemu", "S.") }, authors.Take(2));
        Assert.Equal(new[] { Verbatim("Birhanu Belay"), Verbatim("Sebsebe Demissew") }, authors.Skip(4).Take(2));
        Assert.Equal(Verbatim("Efrata Mekbib"), authors[^1]);
        Assert.Equal(AuthorNameShape.GivenNameFirst, parse.AuthorShapes[^1]);
        Assert.Equal(2022, parse.Parts.ErrataYear);
    }

    [Fact]
    public void PersonShapes() {
        var dotless = Parts(Payload(176113246, 245432249, "2024",
            "Villa-Navarro, F., DoNascimiento, CD, Mojica, J.I., Rodríguez-Olarte, D., Usma, S., Herrera-Collazos, E.E. & Fernando, E. 2024. Hypophthalmus oremaculatus. The IUCN Red List of Threatened Species 2024: e.T176113246A245432249. Accessed on 18 August 2026.",
            "Hypophthalmus oremaculatus",
            "Villa-Navarro, F., DoNascimiento, CD, Mojica, J.I., Rodríguez-Olarte, D., Usma, S., Herrera-Collazos, E.E. & Fernando, E.", 7));
        var given = Parts(Payload(175762777, 246519514, "2025",
            "Damit, A., Mohd Yusof, Nur Adillah & Sugau, J. 2025. Elaeocarpus euneurus. The IUCN Red List of Threatened Species 2025: e.T175762777A246519514. Accessed on 18 August 2026.",
            "Elaeocarpus euneurus", "Damit, A., Mohd Yusof, Nur Adillah & Sugau, J.", 3));
        var suffix = Parts(Payload(127826787, 182525770, "2020",
            "Pitman, R.L. & Brownell Jr., R.L. 2020. Mesoplodon hotaula. The IUCN Red List of Threatened Species 2020: e.T127826787A182525770. Accessed on 19 August 2026.",
            "Mesoplodon hotaula", "Pitman, R.L. & Brownell Jr., R.L.", 2));
        var compact = Parts(Payload(70102843, 70164025, "2019",
            "N.H. Rakotoarivelo, L. Faranirina 2019. Nesogordonia macrophylla. The IUCN Red List of Threatened Species 2019: e.T70102843A70164025. Accessed on 20 August 2026.",
            "Nesogordonia macrophylla", "N.H. Rakotoarivelo, L. Faranirina"));
        var surnameFirst = Parts(Payload(200495, 2664144, "2014",
            "Tamanyan K. 2014. Seseli leptocladum. The IUCN Red List of Threatened Species 2014: e.T200495A2664144. Accessed on 21 August 2026.",
            "Seseli leptocladum", "Tamanyan K."));

        Assert.Equal(Person("DoNascimiento, CD", "DoNascimiento", "CD"), dotless.Authors[1]);
        Assert.Equal(Person("Mohd Yusof, Nur Adillah", "Mohd Yusof", "Nur Adillah"), given.Authors[1]);
        Assert.Equal(Person("Brownell Jr., R.L.", "Brownell", "R.L., Jr."), suffix.Authors[1]);
        Assert.Equal(new[] { Person("N.H. Rakotoarivelo", "Rakotoarivelo", "N.H."), Person("L. Faranirina", "Faranirina", "L.") }, compact.Authors);
        Assert.Equal(Person("Tamanyan K.", "Tamanyan", "K."), surnameFirst.Authors.Single());
    }

    // "Surname, Given Names" and two people written given name first look the same. Only a value[]
    // count of two or more that the split matched keeps the name a person.
    [Fact]
    public void SurnameGivenNames_WithoutACount_IsKeptAsPublished() {
        // aid 89373345: Djoko Iskandar and Mumpuni, value null.
        var parse = Parse(Payload(58713, 89373345, "2004",
            "Djoko Iskandar, Mumpuni 2004. Bufo biporcatus. The IUCN Red List of Threatened Species 2004: e.T58713A89373345. Accessed on 20 August 2026.",
            "Bufo biporcatus", "Djoko Iskandar, Mumpuni"));

        Assert.Equal(new[] { Verbatim("Djoko Iskandar, Mumpuni") }, parse.Parts!.Authors);
        Assert.Equal(new[] { AuthorNameShape.SurnameGivenNamesUnconfirmed }, parse.AuthorShapes);
    }

    [Fact]
    public void SurnameGivenNames_WithACountOfOne_IsKeptAsPublished() {
        // Celsa Señaris and Enrique La Marca; the only value[] entry is the Amphibian Specialist Group.
        var parse = Parse(Payload(55358, 11281813, "2004",
            "Celsa Señaris, Enrique La Marca 2004. Atelopus carbonerensis. The IUCN Red List of Threatened Species 2004: e.T55358A11281813. Accessed on 20 August 2026.",
            "Atelopus carbonerensis", "Celsa Señaris, Enrique La Marca", 1));

        Assert.Equal(IucnAssessmentCitationParser.CreditSplitRule.CountGiven, parse.SplitRule);
        Assert.Equal(new[] { Verbatim("Celsa Señaris, Enrique La Marca") }, parse.Parts!.Authors);
        Assert.Equal(new[] { AuthorNameShape.SurnameGivenNamesUnconfirmed }, parse.AuthorShapes);
    }

    [Fact]
    public void SingleLetterSurname_IsKeptAsPublished() {
        // aid 7928819: "G, Ntakimazi" is one person written the wrong way round.
        var parse = Parse(Payload(4991, 7928819, "2006",
            "G, Ntakimazi 2006. Barbus alluaudi. The IUCN Red List of Threatened Species 2006: e.T4991A7928819. Accessed on 20 August 2026.",
            "Barbus alluaudi", "G, Ntakimazi"));

        Assert.Equal(CitationAuthorKind.Verbatim, Assert.Single(parse.Parts!.Authors).Kind);
    }

    [Fact]
    public void OrganisationWithACommaAndAListKeptWhole() {
        var ministry = Parts(Payload(90230615, 223035828, "2022",
            "Ministry of the Environment, Japan 2022. Lilium ukeyuri (errata version published in 2022). The IUCN Red List of Threatened Species 2022: e.T90230615A223035828. Accessed on 19 August 2026.",
            "Lilium ukeyuri", "Ministry of the Environment, Japan", 1));
        var whole = Parse(Payload(201405, 2705516, "2013",
            "Weber, O. & Sebsebe Demissew 2013. Aloe secundiflora. The IUCN Red List of Threatened Species 2013: e.T201405A2705516. Accessed on 21 August 2026.",
            "Aloe secundiflora", "Weber, O. & Sebsebe Demissew"));

        Assert.Equal(new[] { Organisation("Ministry of the Environment, Japan") }, ministry.Authors);
        Assert.Equal(new[] { Verbatim("Weber, O. & Sebsebe Demissew") }, whole.Parts!.Authors);
        Assert.Equal(IucnAssessmentCitationParser.CreditSplitRule.Whole, whole.SplitRule);
    }

    // ------------------------------------------------------------ no parts

    [Fact]
    public void Unpublished_GivesNoParts() {
        // Gorilla beringei 291011372: a draft with no year_published.
        var parse = Parse(Payload(39994, 291011372, null,
            " . Gorilla beringei. The IUCN Red List of Threatened Species : e.T39994A291011372. Accessed on 24 August 2026.",
            "Gorilla beringei"));

        Assert.Null(parse.Parts);
        Assert.Equal(CitationParseFailure.Unpublished, parse.Failure);
        Assert.Equal(291011372, parse.AssessmentId);
    }

    [Theory]
    // The title is not the taxon's name.
    [InlineData("Marsh, H. 2019. Dugong dugong. The IUCN Red List of Threatened Species 2019: e.T6909A160756767. Accessed on 20 August 2026.", "TitleMismatch")]
    // An annotation IUCN doesn't use.
    [InlineData("Marsh, H. 2019. Dugong dugon (Green Status assessment). The IUCN Red List of Threatened Species 2019: e.T6909A160756767. Accessed on 20 August 2026.", "UnknownTitleSuffix")]
    // Another assessment's page id.
    [InlineData("Marsh, H. 2019. Dugong dugon. The IUCN Red List of Threatened Species 2019: e.T6909A43792211. Accessed on 20 August 2026.", "IdMismatch")]
    // No Red List sentence.
    [InlineData("Marsh, H. 2019. Dugong dugon.", "CitationFormat")]
    [InlineData(" ", "NoCitation")]
    public void BrokenCitations_SayWhy(string citation, string failure) {
        var parse = Parse(Payload(6909, 160756767, "2019", citation, "Dugong dugon", "Marsh, H.", 1));

        Assert.Null(parse.Parts);
        Assert.Equal(Enum.Parse<CitationParseFailure>(failure), parse.Failure);
    }

    [Fact]
    public void MissingIds_GiveNoParts() {
        using var document = JsonDocument.Parse("""{"year_published":"2019","citation":"x"}""");
        Assert.Equal(CitationParseFailure.MissingIds, IucnCitationPartsParser.Parse(document.RootElement, null).Failure);
        using var array = JsonDocument.Parse("[]");
        Assert.Null(IucnCitationPartsParser.ParseParts(array.RootElement, null));
    }

    [Fact]
    public void Parts_RoundTripThroughTheSiteDatabaseJson() {
        var parts = Parts(Payload(127826787, 182525770, "2020",
            "Pitman, R.L. & Brownell Jr., R.L. 2020. Mesoplodon hotaula. The IUCN Red List of Threatened Species 2020: e.T127826787A182525770. Accessed on 19 August 2026.",
            "Mesoplodon hotaula", "Pitman, R.L. & Brownell Jr., R.L.", 2));

        var back = IucnCitationParts.FromJson(parts.ToJson())!;

        Assert.Equal(parts.Authors, back.Authors);
        Assert.Equal(parts with { Authors = back.Authors }, back);
    }
}
