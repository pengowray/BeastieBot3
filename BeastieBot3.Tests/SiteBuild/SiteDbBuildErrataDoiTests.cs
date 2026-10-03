using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;
using BeastieBot3.Wikidata;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// An errata version whose DOI, from `iucn resolve-dois`, names an assessment that
// IucnTaxaHeaders.PredecessorIds does not give. Pinus pinea's global assessment 129160976 is the
// 2018 errata version of the 2013 assessment 2977175, whose DOI Crossref links to the errata
// version's page; IUCN's API now gives 2977175 the scope Europe, so the same-year, same-scope rule
// misses it. Ids, years, scopes and citations are IUCN's; the records are cut down.
public sealed class SiteDbBuildErrataDoiTests : IDisposable {
    private const long PinusPinea = 42391;
    private const long Errata2018 = 129160976;
    private const long Europe2013 = 2977175;
    private const long Global1998 = 10690403;
    private const string Doi2013 = "10.2305/IUCN.UK.2013-1.RLTS.T42391A2977175.en";

    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    // The replaced assessment is in another scope: the errata version gets the DOI, and the Europe
    // assessment is linked to it as replaced. The Europe assessment keeps its own DOI.
    [Fact]
    public void Build_UsesTheResolvedDoiOfAnErrataVersion_WhenTheReplacedAssessmentIsInAnotherScope() {
        var (output, stats) = Build(withEuropeHeader: true);
        using var db = OpenReadOnly(output);
        var errata = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Errata2018}"))!;
        var europe = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Europe2013}"))!;

        Assert.Equal((Doi2013, DoiSource.Resolved, 2018), (errata.Doi, errata.DoiSource, errata.ErrataYear));
        Assert.Equal((Doi2013, DoiSource.Resolved), (europe.Doi, europe.DoiSource));
        Assert.Equal(Errata2018.ToString(), Scalar(db, $"SELECT replaced_by_assessment_id FROM assessment WHERE assessment_id = {Europe2013}"));
        Assert.Equal("Europe", Scalar(db, $"SELECT scope FROM assessment WHERE assessment_id = {Europe2013}"));
        Assert.Null(Scalar(db, $"SELECT replaced_by_assessment_id FROM assessment WHERE assessment_id = {Global1998}"));
        Assert.Equal((1, 1, 0), (stats.ReplacedByErrata, stats.ReplacedFoundFromDoi, stats.ReplacedNoCandidate));
    }

    // The replaced assessment is missing from the taxon record: the DOI is still used, and nothing is
    // linked as replaced.
    [Fact]
    public void Build_UsesTheResolvedDoiOfAnErrataVersion_WhenTheReplacedAssessmentIsMissing() {
        var (output, stats) = Build(withEuropeHeader: false);
        using var db = OpenReadOnly(output);
        var errata = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Errata2018}"))!;

        Assert.Equal((Doi2013, DoiSource.Resolved), (errata.Doi, errata.DoiSource));
        Assert.Equal("0", Scalar(db, $"SELECT COUNT(*) FROM assessment WHERE assessment_id = {Europe2013}"));
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM assessment WHERE replaced_by_assessment_id IS NOT NULL"));
        Assert.Equal((0, 0, 1), (stats.ReplacedByErrata, stats.ReplacedFoundFromDoi, stats.ReplacedNoCandidate));
    }

    [Fact]
    public void ErrataPredecessorNamedBy_NamesAnotherAssessmentOfTheTaxon_OnlyForAnErrataVersion() {
        var errata = new IucnCitationParts {
            TaxonId = PinusPinea, AssessmentId = Errata2018, Year = 2013, ScientificName = "Pinus pinea", ErrataYear = 2018,
        };

        Assert.Equal(Europe2013, IucnDoiSelector.ErrataPredecessorNamedBy(errata, Doi2013));
        Assert.Equal(Europe2013, IucnDoiSelector.ErrataPredecessorNamedBy(errata, "https://doi.org/" + Doi2013));
        // Not an errata version, the DOI of another taxon, the assessment's own DOI, no DOI.
        Assert.Null(IucnDoiSelector.ErrataPredecessorNamedBy(errata with { ErrataYear = null }, Doi2013));
        Assert.Null(IucnDoiSelector.ErrataPredecessorNamedBy(errata, "10.2305/IUCN.UK.2013-1.RLTS.T42392A2977175.en"));
        Assert.Null(IucnDoiSelector.ErrataPredecessorNamedBy(errata, "10.2305/IUCN.UK.2018-1.RLTS.T42391A129160976.en"));
        Assert.Null(IucnDoiSelector.ErrataPredecessorNamedBy(errata, null));
    }

    // The widened predecessors apply to the resolved DOI only: the same DOI from Wikidata is rejected.
    [Fact]
    public void Select_WidensThePredecessorsForTheResolvedDoiOnly() {
        var errata = new IucnCitationParts {
            TaxonId = PinusPinea, AssessmentId = Errata2018, Year = 2013, ScientificName = "Pinus pinea", ErrataYear = 2018,
        };
        var widened = new[] { Europe2013 };

        Assert.Equal(new DoiChoice(Doi2013, DoiSource.Resolved),
            IucnDoiSelector.Select(errata, null, null, null, Array.Empty<long>(), Doi2013, widened));
        Assert.Equal(new DoiChoice(null, DoiSource.None),
            IucnDoiSelector.Select(errata, null, null, new[] { Doi2013 }, Array.Empty<long>(), null, widened));
        Assert.Equal(new DoiChoice(null, DoiSource.None),
            IucnDoiSelector.Select(errata, null, null, null, Array.Empty<long>(), Doi2013));
    }

    // ------------------------------------------------------------ Wikidata items for assessments

    // The Europe assessment has its own item; the errata version, whose DOI names the Europe
    // assessment, shares it rather than being offered a second item with the same DOI. A taxon item
    // that carries an assessment DOI is left out, and an item for an assessment the site does not
    // have is not used.
    [Fact]
    public void Build_SetsTheWikidataItemOfEachAssessment() {
        var (output, stats) = Build(withEuropeHeader: true, withWikidata: true);
        using var db = OpenReadOnly(output);
        var rows = Rows(db, "SELECT assessment_id, wikidata_item_qid, wikidata_item_properties FROM assessment ORDER BY assessment_id");

        Assert.Equal(new object?[] { Europe2013, "Q56000001", "P31 P1476 P1433 P921 P577 P356 Len" }, rows.Single(r => (long)r[0]! == Europe2013));
        Assert.Equal(new object?[] { Errata2018, "Q56000001", "P31 P1476 P1433 P921 P577 P356 Len" }, rows.Single(r => (long)r[0]! == Errata2018));
        Assert.Equal(new object?[] { Global1998, null, null }, rows.Single(r => (long)r[0]! == Global1998));

        // Both rows say which assessment the item is for, so the errata version's page can tell it borrows it.
        Assert.Equal(Europe2013.ToString(), Scalar(db, $"SELECT wikidata_item_assessment_id FROM assessment WHERE assessment_id = {Errata2018}"));
        Assert.Equal(Europe2013.ToString(), Scalar(db, $"SELECT wikidata_item_assessment_id FROM assessment WHERE assessment_id = {Europe2013}"));
        Assert.Equal((1, 1), (stats.AssessmentsWithOwnItem, stats.AssessmentsWithItemThroughDoi));
        Assert.Equal(3, stats.WikidataItems.Read);
        Assert.Equal(new Dictionary<string, int> { ["Q13442814 scholarly article"] = 2 }, stats.WikidataItems.KeptByClass);
        Assert.Equal(new Dictionary<string, int> { ["Q16521 Q55808"] = 1 }, stats.WikidataItems.DroppedByClasses);
        Assert.Equal(1, stats.WikidataItems.ByAssessment.Count - stats.WikidataItems.Used.Count);
        Assert.Equal(new WikidataItemModel().ToJson(), Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.WikidataItemModel}'"));
    }

    // The assessment item's titles and label go to the rows that use it; the taxon item's P141
    // statements, with the stated in items of their references, go to the taxon.
    [Fact]
    public void Build_StoresTheItemTitlesAndLabel_AndTheTaxonItemsStatus() {
        var (output, stats) = Build(withEuropeHeader: true, withWikidata: true);
        using var db = OpenReadOnly(output);

        var titles = WikidataTitle.ListFromJson(Scalar(db, $"SELECT wikidata_item_titles FROM assessment WHERE assessment_id = {Errata2018}"));
        Assert.Equal(new[] { new WikidataTitle("Pinus pinea: Farjon, A.", "en") }, titles);
        Assert.Equal("Pinus pinea: Farjon, A.", Scalar(db, $"SELECT wikidata_item_label_en FROM assessment WHERE assessment_id = {Europe2013}"));
        Assert.Null(Scalar(db, $"SELECT wikidata_item_titles FROM assessment WHERE assessment_id = {Global1998}"));

        var taxon = Rows(db, $"SELECT wikidata_qid, wikidata_qid_source, wikidata_p141, wikidata_item_downloaded, wikidata_p627_deprecated FROM taxon WHERE taxon_id = {PinusPinea}").Single();
        Assert.Equal(("Q146992", "p627", "2026-08-20", 0L), ((string)taxon[0]!, (string)taxon[1]!, (string)taxon[3]!, (long)taxon[4]!));
        var statements = WikidataStatusStatement.ListFromJson((string)taxon[2]!)!;
        Assert.Equal(6, statements.Count);
        var iucn = statements.Single(s => s.Id == P141Statement);
        Assert.Equal(("Q211005", "normal"), (iucn.Value, iucn.Rank));
        Assert.Equal(new[] { "Q115962546", "Q136547248" }, iucn.StatedIn);
        Assert.Equal(new[] { PinusPinea.ToString() }, iucn.TaxonIds);
        Assert.Equal((2, true), (iucn.References, iucn.CitesIucn));
        // Stated in an edition of the Red List, with no IUCN taxon ID: cites IUCN.
        var edition = statements.Single(s => s.Id == P141EditionOnly);
        Assert.Equal((1, true), (edition.References, edition.CitesIucn));
        Assert.Empty(edition.TaxonIds!);
        // A national red book, with a reference URL on another site: does not cite IUCN.
        var book = statements.Single(s => s.Id == P141Book);
        Assert.Equal((1, false), (book.References, book.CitesIucn));
        // Stated in an assessment's item (wikidata_iucn_assessment_items), with no IUCN taxon ID.
        Assert.True(statements.Single(s => s.Id == P141AssessmentItem).CitesIucn);
        // The index has no reference URL and only a reference's first stated in, so these two are
        // read from the item's JSON: a pre-publication PDF on nc.iucnredlist.org, and an edition of
        // the Red List as the second stated in.
        var url = statements.Single(s => s.Id == P141UrlOnly);
        Assert.Equal((1, true), (url.References, url.CitesIucn));
        Assert.Empty(url.StatedIn!);
        Assert.True(statements.Single(s => s.Id == P141SecondStatedIn).CitesIucn);
        Assert.Equal((1, 1, 1), (stats.P141ItemsReadAsJson, stats.P141CitesIucnByUrl, stats.P141CitesIucnByLaterStatedIn));
        Assert.Equal((1, 0), (stats.QidsWithP141Known, stats.QidsWithNoP141));
        Assert.Equal(0, stats.WikidataItems.TitlesNotRecorded);
    }

    // Q100 also states the stone pine's IUCN taxon id, at deprecated rank only. It has the lower
    // number, so without the deprecated list it would be chosen; it is named as another item instead.
    [Fact]
    public void Build_AnItemWithTheIdAtDeprecatedRank_IsNotChosen_AndIsListedAsAnotherItem() {
        var (output, stats) = Build(withEuropeHeader: true, withWikidata: true);
        using var db = OpenReadOnly(output);

        Assert.Equal("Q146992", Scalar(db, $"SELECT wikidata_qid FROM taxon WHERE taxon_id = {PinusPinea}"));
        var other = Assert.Single(WikidataOtherTaxonItem.ListFromJson(Scalar(db, $"SELECT wikidata_other_items FROM taxon WHERE taxon_id = {PinusPinea}"))!);
        Assert.Equal(("Q100", true), (other.Qid, other.TaxonIdDeprecated));
        Assert.Equal("Q278113", Assert.Single(other.Statements!).Value);
        Assert.Equal((0, 2, 1), (stats.QidsP627Deprecated, stats.RedListEditions, stats.QidTieBreaks));
    }

    // The DOI names the Europe assessment, so only that row gets the name Crossref registered for it.
    [Fact]
    public void Build_TheRegisteredName_OnlyForTheAssessmentTheDoiNames() {
        var (output, stats) = Build(withEuropeHeader: true);
        using var db = OpenReadOnly(output);
        var errata = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Errata2018}"))!;
        var europe = IucnCitationParts.FromJson(Scalar(db, $"SELECT citation_json FROM assessment WHERE assessment_id = {Europe2013}"))!;

        Assert.Equal("Pinus pinea", europe.RegisteredName);
        Assert.Null(errata.RegisteredName);
        Assert.Equal((1, 0), (stats.RegisteredNames, stats.RegisteredNamesDiffer));
    }

    [Fact]
    public void Build_WithoutAWikidataCache_HasNoItemsAndStoresTheModelItWasGiven() {
        var model = new WikidataItemModel { InstanceOf = "Q1172284" };
        var (output, stats) = Build(withEuropeHeader: true, model: model);
        using var db = OpenReadOnly(output);
        Assert.Equal("0", Scalar(db, "SELECT COUNT(*) FROM assessment WHERE wikidata_item_qid IS NOT NULL"));
        Assert.Equal(model, WikidataItemModel.FromJson(Scalar(db, $"SELECT value FROM meta WHERE key = '{SiteDbSchema.MetaKeys.WikidataItemModel}'")));
        Assert.Equal(0, stats.AssessmentsWithOwnItem);
    }

    private const string P141Statement = "Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A001";
    private const string P141EditionOnly = "Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A002";
    private const string P141Book = "Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A003";
    private const string P141UrlOnly = "q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A005";
    private const string P141SecondStatedIn = "Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A006";
    private const string P141AssessmentItem = "Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A007";

    // Q146992's JSON as the cache stores it, cut down to the P141 statements the index cannot decide:
    // the book, the URL-only reference (a lower-case entity id, as some older statements have), and
    // a reference whose first stated in is another source and whose second is the 2025.2 edition.
    private const string StonePineJson = """
        {"entities":{"Q146992":{"id":"Q146992","claims":{"P141":[
          {"id":"Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A003","rank":"deprecated","mainsnak":{"datavalue":{"value":{"id":"Q219127"}}},
           "references":[{"hash":"h4","snaks":{"P1476":[{"datavalue":{"value":{"text":"Red Book","language":"en"}}}],
             "P854":[{"datavalue":{"value":"https://www.example.org/iucnredlist.org/red-book"}}]}}]},
          {"id":"q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A005","rank":"deprecated","mainsnak":{"datavalue":{"value":{"id":"Q96377276"}}},
           "references":[{"hash":"h5","snaks":{"P854":[{"datavalue":{"value":"https://nc.iucnredlist.org/redlist/content/attachment_files/Pinus_pinea_Pre-Publication_Assessment_RLTS_T42391A1_en.pdf"}}]}}]},
          {"id":"Q146992$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A006","rank":"deprecated","mainsnak":{"datavalue":{"value":{"id":"Q278113"}}},
           "references":[{"hash":"h6","snaks":{"P248":[{"datavalue":{"value":{"id":"Q999"}}},{"snaktype":"somevalue"},{"datavalue":{"value":{"id":"Q136547248"}}}]}}]}
        ]}}}}
        """;

    private static void WriteWikidataCache(string path) {
        var fetched = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
        using (var store = WikidataCacheStore.Open(path)) {
            WriteAssessmentItems(store, fetched);
            store.ReplaceRedListEditions(["Q115962546", "Q136547248"], fetched);
            store.ReplaceDeprecatedIucnTaxonIds([(100, PinusPinea.ToString())], fetched);
        }
        // Q146992, the stone pine's item: it states the IUCN taxon id, and its P141 (least concern)
        // has two references, stated in the 2022.2 and the 2025.2 release items. Made up: a second
        // statement whose one reference is stated in the 2025.2 edition with no IUCN taxon ID, a
        // third whose reference is a book, three whose references the index cannot decide
        // (StonePineJson) or that are stated in an assessment's item, and Q100, which states the id
        // at deprecated rank.
        using var c = OpenWritable(path);
        Execute(c, $"""
            INSERT INTO wikidata_entities (entity_numeric_id, entity_id, discovered_at, last_seen_at, has_p141, has_p627, json_downloaded, downloaded_at, json)
                VALUES (146992, 'Q146992', '2026-08-01T00:00:00.0000000Z', '2026-08-20T00:00:00.0000000Z', 1, 1, 1, '2026-08-20T03:04:05.0000000Z', @json);
            INSERT INTO wikidata_entities (entity_numeric_id, entity_id, discovered_at, last_seen_at, has_p141, has_p627, json_downloaded, downloaded_at, json)
                VALUES (100, 'Q100', '2026-08-01T00:00:00.0000000Z', '2026-08-20T00:00:00.0000000Z', 1, 1, 1, '2026-08-19T03:04:05.0000000Z', '{"{}"}');
            INSERT INTO wikidata_p627_values VALUES (146992, 'claim', '{PinusPinea}');
            INSERT INTO wikidata_p627_values VALUES (100, 'claim', '{PinusPinea}');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141Statement}', 211005, 'Q211005', 'normal');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141Statement}', 'h1', 136547248, '{PinusPinea}');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141Statement}', 'h2', 115962546, '{PinusPinea}');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141EditionOnly}', 211005, 'Q211005', 'deprecated');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141EditionOnly}', 'h3', 136547248, '');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141Book}', 219127, 'Q219127', 'deprecated');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141Book}', 'h4', NULL, '');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141UrlOnly}', 96377276, 'Q96377276', 'deprecated');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141UrlOnly}', 'h5', NULL, '');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141SecondStatedIn}', 278113, 'Q278113', 'deprecated');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141SecondStatedIn}', 'h6', 999, '');
            INSERT INTO wikidata_p141_statements VALUES (146992, '{P141AssessmentItem}', 211005, 'Q211005', 'deprecated');
            INSERT INTO wikidata_p141_references VALUES (146992, '{P141AssessmentItem}', 'h7', 56000001, '');
            INSERT INTO wikidata_p141_statements VALUES (100, 'Q100$0E7A3F55-2E7B-4C60-9F0B-2D8B54F1A004', 278113, 'Q278113', 'normal');
            """, ("@json", StonePineJson));
    }

    private static void WriteAssessmentItems(WikidataCacheStore store, DateTime fetched) {
        store.UpsertAssessmentItems(new[] {
            new WikidataAssessmentItemRow {
                Qid = "Q56000001", Doi = Doi2013.ToUpperInvariant(), AllDois = [Doi2013.ToUpperInvariant()],
                TaxonId = PinusPinea, AssessmentId = Europe2013, IdSource = "doi",
                Title = "Pinus pinea: Farjon, A.", LabelEn = "Pinus pinea: Farjon, A.",
                TitleStatements = [new WikidataTitle("Pinus pinea: Farjon, A.", "en")],
                InstanceOf = ["Q13442814"], MainSubjects = ["Q146992"], PublishedIn = ["Q32059"],
                PublicationDate = "2011-11-08T00:00:00Z", PublicationYear = 2011, FetchedAtUtc = fetched,
            },
            // Made up: a taxon item that carries an assessment DOI, like Zino's petrel's (Q1272830).
            new WikidataAssessmentItemRow {
                Qid = "Q56000002", Doi = "10.2305/IUCN.UK.1998.RLTS.T42391A10690403.EN", TaxonId = PinusPinea, AssessmentId = Global1998,
                IdSource = "doi", LabelEn = "stone pine", InstanceOf = ["Q16521", "Q55808"], FetchedAtUtc = fetched,
            },
            // An assessment the site database does not have.
            new WikidataAssessmentItemRow {
                Qid = "Q56000003", Doi = "10.2305/IUCN.UK.2008.RLTS.T42391A1234.EN", TaxonId = PinusPinea, AssessmentId = 1234,
                IdSource = "doi", InstanceOf = ["Q13442814"], AuthorStringCount = 1, TitleStatements = [], FetchedAtUtc = fetched,
            },
        });
    }

    // ------------------------------------------------------------ the build

    private (string Output, SiteBuildStats Stats) Build(bool withEuropeHeader, bool withWikidata = false, WikidataItemModel? model = null) {
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var doiCache = _sources.PathOf("iucn_doi_cache.sqlite");
        var output = _sources.PathOf("site.sqlite");
        WriteIucn(iucn);
        WriteCache(cache, withEuropeHeader);
        WriteDoiCache(doiCache);
        string? wikidata = null;
        if (withWikidata) {
            wikidata = _sources.PathOf("wikidata.sqlite");
            WriteWikidataCache(wikidata);
        }
        var stats = new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn,
            ApiCache = cache,
            DoiCache = doiCache,
            WikidataCache = wikidata,
            WikidataItemModel = model ?? new WikidataItemModel(),
            Output = output,
        }, QuietConsole()).Run(CancellationToken.None);
        return (output, stats);
    }

    private static void WriteIucn(string path) =>
        WriteIucnCsv(path, "2026-1", """
                (1, 42391, 'Pinus pinea', 'PLANTAE', 'TRACHEOPHYTA', 'PINOPSIDA', 'PINALES', 'PINACEAE', 'Pinus', 'pinea', NULL, NULL, NULL, NULL, 'L.', NULL)
            """, """
                (1, 129160976, 42391, 'Pinus pinea', 'Least Concern', NULL, '2013', '2011-11-08 00:00:00 UTC', '3.1', NULL, NULL, 'Stable', 'false', 'false', 'Global')
            """);

    private static void WriteCache(string path, bool withEuropeHeader) {
        var europe = Scope("2", "Europe");
        var headers = new List<string> {
            Header(Errata2018, PinusPinea, true, "2013", "LC"),
            Header(Global1998, PinusPinea, false, "1998", "LR/lc"),
        };
        if (withEuropeHeader) {
            headers.Add(Header(Europe2013, PinusPinea, false, "2013", "LC", scopes: europe));
        }
        var record = $$"""
            {"sis_id":42391,"taxon":{"sis_id":42391,"scientific_name":"Pinus pinea","species_taxa":[],"subpopulation_taxa":[],"infrarank_taxa":[],
              "kingdom_name":"PLANTAE","phylum_name":"TRACHEOPHYTA","class_name":"PINOPSIDA","order_name":"PINALES","family_name":"PINACEAE",
              "genus_name":"Pinus","species_name":"pinea","subpopulation_name":null,"infra_name":null,"authority":"L.",
              "species":true,"subpopulation":false,"infrarank":false,"common_names":[],"synonyms":[]},
             "assessments":[{{string.Join(",", headers)}}]}
            """;

        const string downloaded = "2026-08-21T00:00:00Z";
        WriteApiCache(path,
            new[] { new CachedTaxonRecord(1, PinusPinea, record) },
            new[] {
                new CachedAssessment(Errata2018, PinusPinea, downloaded, Payload(Errata2018, PinusPinea, "Pinus pinea", "2013",
                    "Farjon, A. 2013. Pinus pinea (errata version published in 2018). The IUCN Red List of Threatened Species 2013: e.T42391A129160976. Accessed on 21 August 2026.",
                    "Farjon, A.")),
                new CachedAssessment(Europe2013, PinusPinea, downloaded, Payload(Europe2013, PinusPinea, "Pinus pinea", "2013",
                    "Farjon, A. 2013. Pinus pinea (Europe assessment). The IUCN Red List of Threatened Species 2013: e.T42391A2977175. Accessed on 22 August 2026.",
                    "Farjon, A.", scopes: europe)),
            });
    }

    // Both assessments got 2977175's DOI from `iucn resolve-dois`: Crossref links it to the errata
    // version's page.
    private static void WriteDoiCache(string path) {
        using var c = OpenWritable(path);
        Execute(c, $"""
            CREATE TABLE doi_check (assessment_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL, doi TEXT, checked_at TEXT NOT NULL,
                candidates_tried INTEGER NOT NULL);
            INSERT INTO doi_check VALUES
                ({Errata2018}, {PinusPinea}, '{Doi2013}', '2026-10-03T00:48:05.2695906Z', 0),
                ({Europe2013}, {PinusPinea}, '{Doi2013}', '2026-10-03T00:48:05.2000000Z', 0);
            CREATE TABLE crossref_works (doi TEXT PRIMARY KEY, taxon_id INTEGER NOT NULL, assessment_id INTEGER NOT NULL,
                release TEXT, language TEXT, url TEXT, url_taxon_id INTEGER, url_assessment_id INTEGER, listing_id INTEGER NOT NULL, title TEXT);
            INSERT INTO crossref_works VALUES ('{Doi2013}', {PinusPinea}, {Europe2013}, '2013-1', 'en',
                'https://www.iucnredlist.org/species/42391/129160976', {PinusPinea}, {Errata2018}, 1, 'Pinus pinea: Farjon, A.');
            """);
    }
}
