using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Wikidata;
using BeastieBot3.WikidataEdits;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins how an IUCN assessment item on Wikidata is read: ids out of its DOI or Red List URL, the
// row built from query-service statements, the stored table, and the counts reported over it.
// The DOI and URL forms here are the ones seen on live items (September 2026).
public class IucnAssessmentItemParsingTests {
    // ------------------------------------------------------------------ DOI

    [Theory]
    [InlineData("10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.EN")]
    [InlineData("10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("10.2305/iucn.uk.2016-3.rlts.t15951a107265605.en")]
    [InlineData("  10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.EN  ")]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("http://doi.org/10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("http://dx.doi.org/10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("https://dx.doi.org/10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("doi:10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    [InlineData("https://doi.org/10.2305%2FIUCN.UK.2016-3.RLTS.T15951A107265605.en")]
    public void Doi_FormsAndCases_ParseToTheSameIds(string doi) {
        var parsed = IucnAssessmentRefParser.TryParseDoi(doi);

        Assert.NotNull(parsed);
        Assert.Equal(15951, parsed!.TaxonId);
        Assert.Equal(107265605, parsed.AssessmentId);
        Assert.Equal("2016-3", parsed.Release);
        Assert.Equal("EN", parsed.Language);
        Assert.Equal(IucnAssessmentRefSource.Doi, parsed.Source);
    }

    [Fact]
    public void Doi_WithoutLanguageSuffix_Parses() {
        var parsed = IucnAssessmentRefParser.TryParseDoi("10.2305/IUCN.UK.2021-1.RLTS.T13992A197519168");

        Assert.Equal(13992, parsed!.TaxonId);
        Assert.Equal(197519168, parsed.AssessmentId);
        Assert.Null(parsed.Language);
    }

    [Fact]
    public void Doi_SpanishSuffix_IsKept() {
        var parsed = IucnAssessmentRefParser.TryParseDoi("10.2305/IUCN.UK.2020-1.RLTS.T132874996A132875597.ES");

        Assert.Equal(132874996, parsed!.TaxonId);
        Assert.Equal(132875597, parsed.AssessmentId);
        Assert.Equal("ES", parsed.Language);
    }

    // Releases before 2012 use a bare year ("IUCN.UK.2008.RLTS"), not YYYY-N.
    [Theory]
    [InlineData("10.2305/IUCN.UK.2008.RLTS.T3746A10057200.EN", "2008", 3746, 10057200)]
    [InlineData("10.2305/IUCN.UK.1996.RLTS.T2477A9443513.EN", "1996", 2477, 9443513)]
    public void Doi_BareYearRelease_Parses(string doi, string release, long taxon, long assessment) {
        var parsed = IucnAssessmentRefParser.TryParseDoi(doi);

        Assert.Equal(release, parsed!.Release);
        Assert.Equal(taxon, parsed.TaxonId);
        Assert.Equal(assessment, parsed.AssessmentId);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("10.1038/nature12345")]
    [InlineData("10.2305/IUCN.CH.2016.RA.1.en")]
    [InlineData("10.2305/IUCN.UK.2016-3.RLTS.T15951.EN")]
    [InlineData("10.2305/IUCN.UK.2016-3.RLTS.T0A107265605.EN")]
    [InlineData("https://www.iucnredlist.org/species/15951/107265605")]
    public void Doi_NotAnAssessmentDoi_IsNull(string? doi) {
        Assert.Null(IucnAssessmentRefParser.TryParseDoi(doi));
    }

    [Theory]
    [InlineData("10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.EN", true)]
    [InlineData("10.2305/iucn.uk.2016-3.RLTS.T15951.EN", true)]
    [InlineData("https://doi.org/10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en", true)]
    [InlineData("10.2305/IUCN.CH.2016.RA.1.en", false)]
    [InlineData("10.1038/nature12345", false)]
    public void IsIucnAssessmentDoi_MatchesThePrefixEvenWhenIdsDontParse(string doi, bool expected) {
        Assert.Equal(expected, IucnAssessmentRefParser.IsIucnAssessmentDoi(doi));
    }

    [Fact]
    public void NormalizeDoi_UpperCasesAndStripsTheResolver() {
        Assert.Equal("10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.EN",
            IucnAssessmentRefParser.NormalizeDoi("http://dx.doi.org/10.2305/iucn.uk.2016-3.rlts.t15951a107265605.en"));
        Assert.Null(IucnAssessmentRefParser.NormalizeDoi("https://www.iucnredlist.org/species/15951/107265605"));
    }

    // ------------------------------------------------------------------ URL

    [Theory]
    [InlineData("https://www.iucnredlist.org/species/15951/107265605")]
    [InlineData("http://www.iucnredlist.org/species/15951/107265605")]
    [InlineData("https://iucnredlist.org/species/15951/107265605")]
    [InlineData("https://www.iucnredlist.org/species/15951/107265605/")]
    [InlineData("https://www.iucnredlist.org/species/15951/107265605?lang=en")]
    [InlineData("HTTPS://WWW.IUCNREDLIST.ORG/species/15951/107265605")]
    public void Url_RedListSpeciesPage_ParsesBothIds(string url) {
        var parsed = IucnAssessmentRefParser.TryParseUrl(url);

        Assert.Equal(15951, parsed!.TaxonId);
        Assert.Equal(107265605, parsed.AssessmentId);
        Assert.Null(parsed.Release);
        Assert.Equal(IucnAssessmentRefSource.Url, parsed.Source);
    }

    [Theory]
    [InlineData("http://www.iucnredlist.org/details/15951/0")]
    [InlineData("http://www.iucnredlist.org/details/15951")]
    [InlineData("https://www.iucnredlist.org/species/15951")]
    public void Url_WithoutAnAssessment_GivesTheTaxonOnly(string url) {
        var parsed = IucnAssessmentRefParser.TryParseUrl(url);

        Assert.Equal(15951, parsed!.TaxonId);
        Assert.Null(parsed.AssessmentId);
    }

    [Theory]
    [InlineData("https://doi.org/10.2305/iucn.uk.2020-1.rlts.t132874996a132875597.es")]
    [InlineData("http://dx.doi.org/10.2305/IUCN.UK.2020-1.RLTS.T132874996A132875597.en")]
    public void Url_DoiResolverLink_ParsesAsADoi(string url) {
        var parsed = IucnAssessmentRefParser.TryParseUrl(url);

        Assert.Equal(132874996, parsed!.TaxonId);
        Assert.Equal(132875597, parsed.AssessmentId);
        Assert.Equal(IucnAssessmentRefSource.Doi, parsed.Source);
    }

    [Theory]
    [InlineData("https://www.iucnredlist.org/")]
    [InlineData("https://www.iucnredlist.org/search?query=lion")]
    [InlineData("https://example.org/species/15951/107265605")]
    [InlineData("https://doi.org/10.1038/nature12345")]
    public void Url_Other_IsNull(string url) {
        Assert.Null(IucnAssessmentRefParser.TryParseUrl(url));
    }

    [Fact]
    public void FromItem_PrefersTheDoi_ThenAUrlWithAnAssessment() {
        var fromDoi = IucnAssessmentRefParser.FromItem(
            new[] { "10.1038/NATURE1", "10.2305/IUCN.UK.2015-4.RLTS.T15951A79929984.EN" },
            new[] { "https://www.iucnredlist.org/species/15951/107265605" });
        Assert.Equal(79929984, fromDoi!.AssessmentId);

        var fromUrl = IucnAssessmentRefParser.FromItem(
            Array.Empty<string>(),
            new[] { "http://www.iucnredlist.org/details/15951/0", "https://www.iucnredlist.org/species/15951/107265605" });
        Assert.Equal(107265605, fromUrl!.AssessmentId);

        var taxonOnly = IucnAssessmentRefParser.FromItem(Array.Empty<string>(), new[] { "http://www.iucnredlist.org/details/15951/0" });
        Assert.Equal(15951, taxonOnly!.TaxonId);
        Assert.Null(taxonOnly.AssessmentId);

        Assert.Null(IucnAssessmentRefParser.FromItem(new[] { "10.1038/NATURE1" }, Array.Empty<string>()));
    }

    // ------------------------------------------------------------------ query service rows

    private const string DetailsJson = """
{
  "head": { "vars": ["item", "p", "v"] },
  "results": { "bindings": [
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P31" },
      "v": { "type": "uri", "value": "http://www.wikidata.org/entity/Q13442814" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P356" },
      "v": { "type": "literal", "value": "10.2305/IUCN.UK.2015-4.RLTS.T15951A79929984.EN" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P1476" },
      "v": { "xml:lang": "en", "type": "literal", "value": "Panthera leo: Bauer, H., Packer, C., Funston, P.F., Henschel, P. & Nowell, K" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P921" },
      "v": { "type": "uri", "value": "http://www.wikidata.org/entity/Q140" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P921" },
      "v": { "type": "uri", "value": "http://www.wikidata.org/entity/Q127960" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P1433" },
      "v": { "type": "uri", "value": "http://www.wikidata.org/entity/Q32059" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P577" },
      "v": { "datatype": "http://www.w3.org/2001/XMLSchema#dateTime", "type": "literal", "value": "2014-06-17T00:00:00Z" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56912428" },
      "p": { "type": "uri", "value": "http://schema.org/dateModified" },
      "v": { "datatype": "http://www.w3.org/2001/XMLSchema#dateTime", "type": "literal", "value": "2025-07-29T02:25:02Z" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56227924" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P2093" },
      "v": { "type": "literal", "value": "Abba, A.M." } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56227924" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P2093" },
      "v": { "type": "literal", "value": "Miranda, F." } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56227924" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P953" },
      "v": { "type": "uri", "value": "https://www.iucnredlist.org/species/14224/47441961" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56227924" },
      "p": { "type": "uri", "value": "http://www.wikidata.org/prop/direct/P577" },
      "v": { "type": "bnode", "value": "http://www.wikidata.org/.well-known/genid/ab41d0d431f2720299de385d9e2409fe" } },
    { "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q56227924" },
      "p": { "type": "uri", "value": "http://www.w3.org/2000/01/rdf-schema#label" },
      "v": { "xml:lang": "en", "type": "literal", "value": "Myrmecophaga tridactyla" } }
  ] }
}
""";

    [Fact]
    public void ParseDetails_GroupsStatementsByItem() {
        var triples = WikidataAssessmentItemQueries.ParseDetails(DetailsJson);

        Assert.Equal(new[] { "Q56227924", "Q56912428" }, triples.Keys.OrderBy(k => k).ToArray());
        Assert.Contains(triples["Q56912428"], t => t is { Property: "P31", Value: "Q13442814" });
        Assert.Contains(triples["Q56912428"], t => t is { Property: "P1476", Language: "en" });
        Assert.Contains(triples["Q56912428"], t => t is { Property: "modified", Value: "2025-07-29T02:25:02Z" });
        Assert.Contains(triples["Q56227924"], t => t is { Property: "label", Value: "Myrmecophaga tridactyla" });
    }

    [Fact]
    public void Build_ScholarlyArticle_ReadsIdsTitleAndCounts() {
        var triples = WikidataAssessmentItemQueries.ParseDetails(DetailsJson);
        var fetched = new DateTime(2026, 9, 13, 3, 0, 0, DateTimeKind.Utc);

        var row = WikidataAssessmentItemBuilder.Build("Q56912428", triples["Q56912428"], new[] { "published-in:scholarly" }, "scholarly", fetched);

        Assert.Equal("10.2305/IUCN.UK.2015-4.RLTS.T15951A79929984.EN", row.Doi);
        Assert.Equal(15951, row.TaxonId);
        Assert.Equal(79929984, row.AssessmentId);
        Assert.Equal("2015-4", row.DoiRelease);
        Assert.Equal("doi", row.IdSource);
        Assert.StartsWith("Panthera leo: Bauer", row.Title);
        Assert.Equal(new[] { "Q13442814" }, row.InstanceOf);
        Assert.Equal(new[] { "Q127960", "Q140" }, row.MainSubjects);
        Assert.Equal(new[] { "Q32059" }, row.PublishedIn);
        // P577 on the 2018 batch is the assessment date, not the release in the DOI.
        Assert.Equal(2014, row.PublicationYear);
        Assert.Equal(0, row.AuthorItemCount);
        Assert.Equal(0, row.AuthorStringCount);
        Assert.False(row.IsTaxonItem);
    }

    [Fact]
    public void Build_UnknownDateAndUrlIds_AreHandled() {
        var triples = WikidataAssessmentItemQueries.ParseDetails(DetailsJson);

        var row = WikidataAssessmentItemBuilder.Build("Q56227924", triples["Q56227924"], new[] { "doi-search" }, "scholarly", DateTime.UtcNow);

        Assert.Null(row.PublicationDate);
        Assert.Null(row.PublicationYear);
        Assert.Equal(2, row.AuthorStringCount);
        Assert.Equal(14224, row.TaxonId);
        Assert.Equal(47441961, row.AssessmentId);
        Assert.Equal("url", row.IdSource);
        Assert.Null(row.Title);
        Assert.Equal("Myrmecophaga tridactyla", row.LabelEn);
        Assert.Equal("Myrmecophaga tridactyla", row.ToExistingAssessmentItem().Title);
    }

    [Theory]
    [InlineData("http://www.wikidata.org/entity/Q56912428", "Q56912428")]
    [InlineData("Q56912428", "Q56912428")]
    [InlineData("56912428", "Q56912428")]
    [InlineData("P31", null)]
    [InlineData("http://www.wikidata.org/.well-known/genid/ab41", null)]
    public void ToItemId_AcceptsEachBindingForm(string value, string? expected) {
        Assert.Equal(expected, WikidataAssessmentItemQueries.ToItemId(value));
    }

    [Fact]
    public void DoiSearchSlices_CoverThe1990sThenEachYear() {
        var slices = WikidataAssessmentItemQueries.DoiSearchSlices(2027);

        Assert.Equal("10.2305/IUCN.UK.199", slices[0]);
        Assert.Equal("10.2305/IUCN.UK.2000", slices[1]);
        Assert.Equal("10.2305/IUCN.UK.2027", slices[^1]);
        Assert.Equal(29, slices.Count);
    }

    // ------------------------------------------------------------------ store and reader

    private static WikidataAssessmentItemRow Row(string qid, long? taxon, long? assessment, params string[] instanceOf) => new() {
        Qid = qid,
        Doi = assessment is null ? null : $"10.2305/IUCN.UK.2016-1.RLTS.T{taxon}A{assessment}.EN",
        AllDois = assessment is null ? Array.Empty<string>() : new[] { $"10.2305/IUCN.UK.2016-1.RLTS.T{taxon}A{assessment}.EN" },
        TaxonId = taxon,
        AssessmentId = assessment,
        DoiRelease = assessment is null ? null : "2016-1",
        DoiLanguage = assessment is null ? null : "EN",
        IdSource = assessment is null ? null : "doi",
        Title = qid + ": Someone",
        InstanceOf = instanceOf,
        MainSubjects = new[] { "Q140" },
        PublishedIn = new[] { "Q32059" },
        PublicationDate = "2016-02-01T00:00:00Z",
        PublicationYear = 2016,
        AuthorStringCount = 2,
        Urls = new[] { "https://www.iucnredlist.org/species/1/2" },
        FoundBy = new[] { "doi-search", "published-in:scholarly" },
        SourceEndpoint = "scholarly",
        ModifiedAt = "2025-07-29T03:08:35Z",
        FetchedAtUtc = new DateTime(2026, 9, 13, 1, 0, 0, DateTimeKind.Utc),
    };

    [Fact]
    public void Store_RoundTrip_KeepsFieldsAndFirstSeen() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = WikidataCacheStore.OpenFromConnection(connection);

        var first = Row("Q54800517", 13119, 22459833, "Q13442814");
        store.UpsertAssessmentItems(new[] { first, Row("Q1272830", 22698062, 132622973, "Q16521", "Q55808") });

        var later = first with { Title = "Protochromys fellowsi: Helgen, K.", FetchedAtUtc = first.FetchedAtUtc.AddDays(1) };
        store.UpsertAssessmentItems(new[] { later });

        var rows = store.ReadAssessmentItems();
        Assert.Equal(new[] { "Q1272830", "Q54800517" }, rows.Select(r => r.Qid).ToArray());

        var stored = rows.Single(r => r.Qid == "Q54800517");
        Assert.Equal("Protochromys fellowsi: Helgen, K.", stored.Title);
        Assert.Equal(later.FetchedAtUtc, stored.FetchedAtUtc);
        Assert.Equal(first.FetchedAtUtc, stored.FirstSeenAtUtc);
        Assert.Equal(13119, stored.TaxonId);
        Assert.Equal(22459833, stored.AssessmentId);
        Assert.Equal("2016-1", stored.DoiRelease);
        Assert.Equal(new[] { "doi-search", "published-in:scholarly" }, stored.FoundBy);
        Assert.Equal(new[] { "Q140" }, stored.MainSubjects);
        Assert.Equal(2, stored.AuthorStringCount);
        Assert.Equal(2016, stored.PublicationYear);

        var set = ExistingAssessmentItemReader.Read(connection);
        Assert.True(set.TableFound);
        Assert.Equal(2, set.All.Count);
        var publication = Assert.Single(set.PublicationItems);
        Assert.Equal("Q54800517", publication.Qid);
        Assert.Equal("Q54800517", Assert.Single(set.ByAssessmentId[22459833]).Qid);
        Assert.Empty(set.ByAssessmentId[132622973]);
    }

    [Fact]
    public void Reader_WithoutTheTable_ReturnsEmpty() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var set = ExistingAssessmentItemReader.Read(connection);

        Assert.False(set.TableFound);
        Assert.Empty(set.All);
    }

    // ------------------------------------------------------------------ summary

    [Fact]
    public void Summary_CountsDuplicatesAndReleaseMatches() {
        var rows = new List<WikidataAssessmentItemRow> {
            Row("Q1", 100, 1000, "Q13442814"),
            Row("Q2", 100, 1000, "Q1172284") with { FoundBy = new[] { "doi-scan:main" }, SourceEndpoint = "main" },
            Row("Q3", 100, 999, "Q13442814"),
            Row("Q4", 200, 2000, "Q13442814"),
            Row("Q5", 300, 3000, "Q16521"),
            Row("Q6", null, null, "Q13442814") with { AllDois = new[] { "10.2305/IUCN.UK.2016-1.RLTS.T5.EN" } },
        };
        var release = new IucnReleaseIds("IUCN_2026-1.sqlite", new HashSet<long> { 1000, 3000 }, new HashSet<long> { 100, 300 });
        var api = new IucnApiBacklogIds(new HashSet<long> { 1000 }, new HashSet<long> { 1000, 999 });

        var summary = WikidataAssessmentItemSummary.Build(rows, release, api);

        Assert.Equal(6, summary.Total);
        var duplicate = Assert.Single(summary.Duplicates);
        Assert.Equal(1000, duplicate.AssessmentId);
        Assert.Equal(new[] { "Q1", "Q2" }, duplicate.Qids);

        // Q5 is a taxon item, so it is left out of the release comparison.
        Assert.Equal(2, summary.LatestInRelease);
        Assert.Equal(1, summary.OlderAssessmentOfTaxonInRelease);
        Assert.Equal(1, summary.TaxonNotInRelease);
        Assert.Equal(2, summary.LatestInApiCache);
        Assert.Equal(1, summary.SupersededInApiCache);
        Assert.Equal(1, summary.NotInApiCache);

        Assert.Equal(new[] { "Q5" }, summary.TaxonItems);
        Assert.Equal(1, summary.IucnDoiNotParsed);
        Assert.Equal(5, summary.WithTaxonAndAssessmentId);
        Assert.Contains(summary.InstanceOf, x => x == ("Q13442814", 4));
        Assert.Contains(summary.ByRoute, x => x == ("doi-scan:main", 1, 1));
        Assert.Contains(summary.ByRoute, x => x == ("doi-search", 5, 0));
        Assert.Contains(summary.ByEndpoint, x => x == ("main", 1));
    }
}
