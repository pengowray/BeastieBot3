using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The matching behind the "current taxon with the same name" and "via IUCN synonym" columns of
// iucn api report-no-latest and the audit site's no-latest page: name normalisation, the scope and
// kingdom rules, synonym names, the display text, and the two loaders over in-memory databases.
public class IucnSameNameTaxaTests {
    private static IucnCurrentAssessment Assessment(long id, string code, string year, params string[] scopes) =>
        new(id, code, year, scopes);

    private static IucnCurrentTaxon Taxon(long id, string name, string? authority, string kingdom, params IucnCurrentAssessment[] assessments) =>
        new(id, name, authority, kingdom, assessments);

    private static IucnOldTaxon Old(long id, string name, string? authority, string kingdom, params string[] scopes) =>
        new(id, name, authority, kingdom, scopes);

    // 2785 Bettongia penicillata: the name is now used by 2790.
    private static readonly IucnCurrentTaxon Woylie =
        Taxon(2790, "Bettongia penicillata", "J.E. Gray, 1843", "ANIMALIA", Assessment(258664485, "EN", "2025", "Global"));

    // ---- Name normalisation ----

    [Theory]
    [InlineData("Bettongia penicillata", "bettongia penicillata")]
    [InlineData("  Bettongia   PENICILLATA ", "bettongia penicillata")]
    [InlineData("Olea europaea subsp. cerasiformis", "olea europaea ssp. cerasiformis")]
    [InlineData("Olea europaea ssp. cerasiformis", "olea europaea ssp. cerasiformis")]
    [InlineData("Cassia afrofistula var. afrofistula", "cassia afrofistula var. afrofistula")]
    [InlineData(null, "")]
    [InlineData("   ", "")]
    public void NameKey_FoldsCaseWhitespaceAndSubspeciesMarker(string? name, string expected) {
        Assert.Equal(expected, IucnSameNameTaxa.NameKey(name));
    }

    [Fact]
    public void NameKey_KeepsVarietyApartFromSubspecies() {
        Assert.NotEqual(IucnSameNameTaxa.NameKey("Aus bus var. cus"), IucnSameNameTaxa.NameKey("Aus bus ssp. cus"));
    }

    [Theory]
    [InlineData("Global", new[] { "Global" })]
    [InlineData("Global & Europe", new[] { "Global", "Europe" })]
    [InlineData("Global, Europe & Mediterranean", new[] { "Global", "Europe", "Mediterranean" })]
    [InlineData("Global, Central Africa, Eastern Africa & Pan-Africa", new[] { "Global", "Central Africa", "Eastern Africa", "Pan-Africa" })]
    [InlineData("S. Africa FW", new[] { "S. Africa FW" })]
    [InlineData("Global &amp; Europe", new[] { "Global", "Europe" })]
    [InlineData(null, new string[0])]
    public void ParseCsvScopes_SplitsOnCommaAndAmpersand(string? text, string[] expected) {
        Assert.Equal(expected, IucnSameNameTaxa.ParseCsvScopes(text));
    }

    // ---- Same-name matching ----

    [Fact]
    public void SameName_MatchesTheCurrentTaxonWithTheName() {
        var index = new IucnSameNameTaxa(new[] { Woylie });
        var match = Assert.Single(index.SameName(Old(2785, "Bettongia penicillata", "Gray, 1837", "ANIMALIA", "Global")));
        Assert.Equal(2790, match.Taxon.TaxonId);
        Assert.Equal(258664485, match.Assessment.AssessmentId);
        Assert.Equal("https://www.iucnredlist.org/species/2790/258664485", match.Url);
    }

    [Fact]
    public void SameName_IgnoresCaseWhitespaceAndSubspeciesMarkerForm() {
        var olive = Taxon(180550, "Olea europaea subsp. cerasiformis", "G.Kunkel & Sunding", "PLANTAE", Assessment(1, "LC", "2020", "Global"));
        var index = new IucnSameNameTaxa(new[] { olive });
        Assert.Single(index.SameName(Old(30372, "olea  europaea ssp. Cerasiformis", null, "Plantae", "Global")));
    }

    [Fact]
    public void SameName_RequiresTheSameKingdom() {
        var index = new IucnSameNameTaxa(new[] { Woylie });
        Assert.Empty(index.SameName(Old(2785, "Bettongia penicillata", null, "PLANTAE", "Global")));
        Assert.Empty(index.SameName(Old(2785, "Bettongia penicillata", null, null!, "Global")));
    }

    [Fact]
    public void SameName_LeavesOutTheOldIdItself() {
        var index = new IucnSameNameTaxa(new[] { Woylie });
        Assert.Empty(index.SameName(Old(2790, "Bettongia penicillata", null, "ANIMALIA", "Global")));
    }

    [Fact]
    public void SameName_GlobalOldAssessmentNeedsACurrentGlobalAssessment() {
        var europeOnly = Taxon(10, "Aus bus", null, "ANIMALIA", Assessment(100, "LC", "2020", "Europe"));
        var combined = Taxon(10, "Aus bus", null, "ANIMALIA", Assessment(101, "NT", "2021", "Global", "Europe"));
        Assert.Empty(new IucnSameNameTaxa(new[] { europeOnly }).SameName(Old(1, "Aus bus", null, "ANIMALIA", "Global")));
        Assert.Single(new IucnSameNameTaxa(new[] { combined }).SameName(Old(1, "Aus bus", null, "ANIMALIA", "Global")));
        // An old assessment covering Global and Europe together still matches on Global.
        Assert.Single(new IucnSameNameTaxa(new[] { combined }).SameName(Old(1, "Aus bus", null, "ANIMALIA", "Global", "Europe")));
    }

    [Fact]
    public void SameName_RegionalOldAssessmentNeedsTheSameRegion() {
        var taxon = Taxon(10, "Aus bus", null, "ANIMALIA",
            Assessment(100, "LC", "2020", "Global"),
            Assessment(101, "VU", "2019", "Europe"));
        var index = new IucnSameNameTaxa(new[] { taxon });

        var europe = Assert.Single(index.SameName(Old(1, "Aus bus", null, "ANIMALIA", "Europe")));
        Assert.Equal(101, europe.Assessment.AssessmentId);
        // A dropped Mediterranean assessment does not match the current Global or Europe ones.
        Assert.Empty(index.SameName(Old(1, "Aus bus", null, "ANIMALIA", "Mediterranean")));
    }

    [Fact]
    public void SameName_RegionalPrefersTheAssessmentSharingMostRegions() {
        var taxon = Taxon(10, "Aus bus", null, "ANIMALIA",
            Assessment(100, "LC", "2024", "Europe"),
            Assessment(101, "VU", "2019", "Europe", "Mediterranean"));
        var match = Assert.Single(new IucnSameNameTaxa(new[] { taxon }).SameName(Old(1, "Aus bus", null, "ANIMALIA", "Europe", "Mediterranean")));
        Assert.Equal(101, match.Assessment.AssessmentId);
    }

    [Fact]
    public void SameName_OldAssessmentWithNoScopeMatchesNothing() {
        var index = new IucnSameNameTaxa(new[] { Woylie });
        Assert.Empty(index.SameName(Old(2785, "Bettongia penicillata", null, "ANIMALIA")));
    }

    [Fact]
    public void SameName_ReturnsEveryMatchInTaxonIdOrder() {
        var a = Taxon(30, "Aus bus ssp. cus", null, "ANIMALIA", Assessment(300, "LC", "2020", "Global"));
        var b = Taxon(20, "Aus bus subsp. cus", null, "ANIMALIA", Assessment(200, "EN", "2021", "Global"));
        var matches = new IucnSameNameTaxa(new[] { a, b }).SameName(Old(1, "Aus bus ssp. cus", null, "ANIMALIA", "Global"));
        Assert.Equal(new long[] { 20, 30 }, matches.Select(m => m.Taxon.TaxonId));
    }

    // ---- Synonym matching ----

    [Theory]
    [InlineData("Helix (Endodonta) ", "irregularis ", null, null, null, "Helix irregularis")]
    [InlineData("Mirogrex", "terraesanctae", "subspecies", "hulensis", null, "Mirogrex terraesanctae ssp. hulensis")]
    [InlineData("Pycnandra", "filipes", "subspecies (plantae)", "filipes", null, "Pycnandra filipes subsp. filipes")]
    [InlineData("Aus", "bus", "variety", "cus", null, "Aus bus var. cus")]
    [InlineData("Aus", "bus", "forma", "cus", null, "Aus bus f. cus")]
    [InlineData("Aus", "bus", null, "cus", null, "Aus bus cus")]
    [InlineData("Aus", "bus", null, null, "Northern subpopulation", null)]
    [InlineData(null, "bus", null, null, null, null)]
    public void SynonymBareName_BuildsTheNameFromItsParts(string? genus, string? species, string? infraType, string? infraName, string? subpopulation, string? expected) {
        Assert.Equal(expected, IucnSameNameTaxa.SynonymBareName(genus, species, infraType, infraName, subpopulation));
    }

    [Fact]
    public void ViaSynonym_MatchesTheCurrentTaxonListingTheOldName() {
        var current = Taxon(203607, "Tropidodipsas fischeri", "Boulenger, 1894", "ANIMALIA", Assessment(5, "LC", "2013", "Global"));
        var synonyms = new[] { new IucnCurrentSynonym(203607, "Sibon fischeri", "Sibon fischeri (Boulenger, 1894)") };
        var index = new IucnSameNameTaxa(new[] { current }, synonyms);
        var old = Old(63918, "Sibon fischeri", "(Boulenger, 1894)", "ANIMALIA", "Global");

        Assert.Empty(index.SameName(old));
        var match = Assert.Single(index.ViaSynonym(old));
        Assert.Equal(IucnSameNameMatchKind.Synonym, match.Kind);
        Assert.Null(match.ListedAs);
        Assert.Equal("203607 Tropidodipsas fischeri Boulenger, 1894 (LC, 2013)", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void ViaSynonym_ShowsTheSynonymWhenItsAuthorOrNoteDiffers() {
        var current = Taxon(71532724, "Ozimops kitcheneri", "(McKenzie, Reardon & Adams, 2014)", "ANIMALIA", Assessment(5, "LC", "2021", "Global"));
        var synonyms = new[] { new IucnCurrentSynonym(71532724, "Mormopterus planiceps", "Mormopterus planiceps Peters, 1888 [in part]") };
        var old = Old(13888, "Mormopterus planiceps", "(Peters, 1866)", "ANIMALIA", "Global");
        var match = Assert.Single(new IucnSameNameTaxa(new[] { current }, synonyms).ViaSynonym(old));
        Assert.Equal("Mormopterus planiceps Peters, 1888 [in part]", match.ListedAs);
        Assert.EndsWith("(LC, 2021), listed as Mormopterus planiceps Peters, 1888 [in part]", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void ViaSynonym_OneMatchPerTaxonAndNoneAlreadyMatchedByName() {
        var sameName = Taxon(20, "Aus bus", null, "ANIMALIA", Assessment(200, "LC", "2020", "Global"));
        var other = Taxon(30, "Aus cus", null, "ANIMALIA", Assessment(300, "EN", "2021", "Global"));
        var synonyms = new[] {
            new IucnCurrentSynonym(20, "Aus bus", "Aus bus L."),
            new IucnCurrentSynonym(30, "Aus bus", "Aus bus Pitt."),
            new IucnCurrentSynonym(30, "Aus bus", "Aus bus Pittier"),
        };
        var index = new IucnSameNameTaxa(new[] { sameName, other }, synonyms);
        var old = Old(1, "Aus bus", "Pittier", "ANIMALIA", "Global");

        Assert.Equal(20, Assert.Single(index.SameName(old)).Taxon.TaxonId);
        var match = Assert.Single(index.ViaSynonym(old));
        Assert.Equal(30, match.Taxon.TaxonId);
        Assert.Null(match.ListedAs); // one of the two entries has the old taxon's own authority
    }

    [Fact]
    public void ViaSynonym_AppliesTheScopeAndKingdomRules() {
        var current = Taxon(30, "Aus cus", null, "ANIMALIA", Assessment(300, "EN", "2021", "Europe"));
        var synonyms = new[] { new IucnCurrentSynonym(30, "Aus bus", "Aus bus L.") };
        var index = new IucnSameNameTaxa(new[] { current }, synonyms);
        Assert.Empty(index.ViaSynonym(Old(1, "Aus bus", "L.", "ANIMALIA", "Global")));
        Assert.Empty(index.ViaSynonym(Old(1, "Aus bus", "L.", "PLANTAE", "Europe")));
        Assert.Single(index.ViaSynonym(Old(1, "Aus bus", "L.", "ANIMALIA", "Europe")));
    }

    [Fact]
    public void WithoutSynonyms_ViaSynonymFindsNothing() {
        var index = new IucnSameNameTaxa(new[] { Woylie });
        Assert.False(index.HasSynonyms);
        Assert.Empty(index.ViaSynonym(Old(2785, "Bettongia penicillata", null, "ANIMALIA", "Global")));
    }

    // ---- Display ----

    [Fact]
    public void Describe_ShowsOnlyTheIdWhenNameAndAuthorityAgree() {
        var old = Old(2785, "Bettongia penicillata", "J.E.  Gray, 1843", "ANIMALIA", "Global");
        var match = Assert.Single(new IucnSameNameTaxa(new[] { Woylie }).SameName(old));
        Assert.Equal("2790 (EN, 2025)", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void Describe_ShowsNameAndAuthorityWhenTheAuthorityDiffers() {
        var old = Old(2785, "Bettongia penicillata", "Gray, 1837", "ANIMALIA", "Global");
        var match = Assert.Single(new IucnSameNameTaxa(new[] { Woylie }).SameName(old));
        Assert.Equal("2790 Bettongia penicillata J.E. Gray, 1843 (EN, 2025)", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void Describe_ShowsTheNameWhenOnlyTheRankMarkerDiffers() {
        var current = Taxon(20, "Aus bus subsp. cus", "L.", "PLANTAE", Assessment(200, "VU", "2021", "Global"));
        var old = Old(1, "Aus bus ssp. cus", "L.", "PLANTAE", "Global");
        var match = Assert.Single(new IucnSameNameTaxa(new[] { current }).SameName(old));
        Assert.Equal("20 Aus bus subsp. cus L. (VU, 2021)", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void Describe_DecodesEntitiesBeforeComparingAuthorities() {
        var current = Taxon(20, "Aus bus", "Brame &amp; Murray, 1968", "ANIMALIA", Assessment(200, "LC", "2020", "Global"));
        var old = Old(1, "Aus bus", "Brame & Murray, 1968", "ANIMALIA", "Global");
        var match = Assert.Single(new IucnSameNameTaxa(new[] { current }).SameName(old));
        Assert.Equal("20 (LC, 2020)", IucnSameNameTaxa.Describe(match, old));
    }

    [Fact]
    public void DescribeAll_CountsSeveralMatches() {
        var a = Taxon(20, "Aus bus", null, "ANIMALIA", Assessment(200, "LC", "2020", "Global"));
        var b = Taxon(30, "Aus Bus", null, "ANIMALIA", Assessment(300, "CR(PE)", "2022", "Global"));
        var old = Old(1, "Aus bus", null, "ANIMALIA", "Global");
        var matches = new IucnSameNameTaxa(new[] { a, b }).SameName(old);
        Assert.Equal("2 taxa: 20 (LC, 2020); 30 Aus Bus (CR(PE), 2022)", IucnSameNameTaxa.DescribeAll(matches, old));
        Assert.Equal("", IucnSameNameTaxa.DescribeAll(new List<IucnSameNameMatch>(), old));
    }

    // ---- Old taxon from the API cache JSON ----

    [Fact]
    public void OldTaxonFromJson_ReadsNameKingdomAndTheLastAssessmentsScopes() {
        const string json = """
            {"sis_id": 2785,
             "taxon": {"scientific_name": "Bettongia penicillata", "authority": "Gray, 1837", "kingdom_name": "ANIMALIA"},
             "assessments": [
               {"assessment_id": 1, "year_published": "2016", "scopes": [{"description": {"en": "Global"}}]},
               {"assessment_id": 2, "year_published": "2025", "scopes": [{"description": {"en": "Europe"}}, {"description": {"en": "Mediterranean"}}]},
               {"assessment_id": 3, "year_published": "2025", "scopes": [{"description": {"en": "Global"}}]},
               {"year_published": "2026", "scopes": [{"description": {"en": "Pan-Africa"}}]}
             ]}
            """;
        var old = IucnSameNameTaxa.OldTaxonFromJson(2785, json)!;
        Assert.Equal("Bettongia penicillata", old.ScientificName);
        Assert.Equal("Gray, 1837", old.Authority);
        Assert.Equal("ANIMALIA", old.Kingdom);
        // 2025 is the latest year; the first 2025 assessment listed wins, and the one with no id is skipped.
        Assert.Equal(new[] { "Europe", "Mediterranean" }, old.LastScopes);
    }

    [Fact]
    public void OldTaxonFromJson_ReturnsNullForUnreadableJson() {
        Assert.Null(IucnSameNameTaxa.OldTaxonFromJson(1, "{not json"));
        Assert.Null(IucnSameNameTaxa.OldTaxonFromJson(1, "{\"assessments\": []}"));
    }

    // ---- Loaders ----

    [Fact]
    public void LoadCurrentTaxa_GroupsAssessmentsAndMapsCategories() {
        using var csv = new SqliteConnection("Data Source=:memory:");
        csv.Open();
        Execute(csv, """
            CREATE TABLE taxonomy_html (taxonId INTEGER, scientificName TEXT, authority TEXT, infraAuthority TEXT, kingdomName TEXT);
            CREATE TABLE assessments_html (assessmentId INTEGER, taxonId INTEGER, redlistCategory TEXT, yearPublished TEXT,
                scopes TEXT, possiblyExtinct TEXT, possiblyExtinctInTheWild TEXT);
            INSERT INTO taxonomy_html VALUES
                (2790, 'Bettongia penicillata', 'J.E. Gray, 1843', NULL, 'ANIMALIA'),
                (180550, 'Olea europaea subsp. cerasiformis', '(Webb) Kunkel', 'G.Kunkel &amp; Sunding', 'PLANTAE');
            INSERT INTO assessments_html VALUES
                (258664485, 2790, 'Endangered', '2025', 'Global', 'false', 'false'),
                (11, 2790, 'Critically Endangered', '2019', 'Europe & Mediterranean', 'true', 'false'),
                (12, 180550, 'Least Concern', '2020', 'Global', NULL, NULL);
            """);

        var taxa = IucnSameNameTaxa.LoadCurrentTaxa(csv).OrderBy(t => t.TaxonId).ToList();
        Assert.Equal(new long[] { 2790, 180550 }, taxa.Select(t => t.TaxonId));
        var woylie = taxa[0];
        Assert.Equal("J.E. Gray, 1843", woylie.Authority);
        Assert.Equal(new[] { "CR(PE)", "EN" }, woylie.Assessments.Select(a => a.StatusCode));
        Assert.Equal(new[] { "Europe", "Mediterranean" }, woylie.Assessments[0].Scopes);
        // An infraspecific taxon takes its infraspecific authority.
        Assert.Equal("G.Kunkel &amp; Sunding", taxa[1].Authority);
    }

    [Fact]
    public void LoadSynonyms_ReadsOnlyCurrentTaxa() {
        using var api = new SqliteConnection("Data Source=:memory:");
        api.Open();
        Execute(api, """
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, root_sis_id INTEGER, json TEXT);
            INSERT INTO taxa (root_sis_id, json) VALUES
                (203607, '{"taxon": {"synonyms": [
                    {"name": "Sibon fischeri (Boulenger, 1894)", "genus_name": "Sibon", "species_name": "fischeri"},
                    {"name": "Leptognathus fischeri", "genus_name": "Helix (Leptognathus) ", "species_name": "fischeri "}]}}'),
                (999, '{"taxon": {"synonyms": [{"name": "Aus bus", "genus_name": "Aus", "species_name": "bus"}]}}'),
                (5, '{"taxon": {"synonyms": []}}'),
                (6, 'not json');
            """);

        var synonyms = IucnSameNameTaxa.LoadSynonyms(api, new HashSet<long> { 203607, 5, 6 });
        Assert.Equal(new[] { "Sibon fischeri", "Helix fischeri" }, synonyms.Select(s => s.Name));
        Assert.Equal("Sibon fischeri (Boulenger, 1894)", synonyms[0].AsWritten);
        Assert.All(synonyms, s => Assert.Equal(203607, s.TaxonId));
    }

    private static void Execute(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
