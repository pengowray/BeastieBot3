using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// StatusListMatcher.OnePerTaxon: how NatureServe, NZTCS and SALVE rows are matched to taxa, one row
// per taxon.
public class StatusListMatcherTests {
    private sealed record Row(string Id, string Name, string? Kingdom = "ANIMALIA", params string[] OtherNames);

    private static readonly SiteTaxon Lion = Taxon(1, "Panthera leo", "Felis leo");
    private static readonly SiteTaxon Tiger = Taxon(2, "Panthera tigris", "Felis tigris");
    private static readonly SiteTaxon Oak = Taxon(3, "Quercus robur", kingdom: "PLANTAE");

    private static SiteTaxon Taxon(long id, string name, string? iucnSynonym = null, string kingdom = "ANIMALIA") => new() {
        TaxonId = id,
        ScientificName = name,
        Kind = SiteTaxonKind.Species,
        Kingdom = kingdom,
        IucnSynonyms = iucnSynonym is null ? new() : new() { new SiteSynonym(iucnSynonym) },
    };

    private static List<(long TaxonId, string RowId, StatusListMatchKind Kind)> Match(params Row[] rows) {
        var index = new StatusListNameIndex(new[] { Lion, Tiger, Oak });
        return StatusListMatcher.OnePerTaxon(rows, index, r => r.Kingdom, r => r.Name, r => r.OtherNames)
            .Select(m => (m.Taxon.TaxonId, m.Row.Id, m.Kind)).ToList();
    }

    [Fact]
    public void FirstRowWithTheNameWins() =>
        Assert.Equal(new[] { (1L, "a", StatusListMatchKind.Name) },
            Match(new Row("a", "Panthera leo"), new Row("b", "Panthera leo")));

    [Fact]
    public void NameMatchBeatsAnEarlierRowsOtherName() =>
        Assert.Equal(new[] { (1L, "b", StatusListMatchKind.Name) },
            Match(new Row("a", "Leo leo", "ANIMALIA", "Panthera leo"), new Row("b", "Panthera leo")));

    [Fact]
    public void NameMatchBeatsAnEarlierRowsIucnSynonym() =>
        Assert.Equal(new[] { (1L, "b", StatusListMatchKind.Name) },
            Match(new Row("a", "Felis leo"), new Row("b", "Panthera leo")));

    [Fact]
    public void OtherNameSkipsATaxonAlreadyMatchedByName() =>
        Assert.Equal(new[] { (1L, "a", StatusListMatchKind.Name), (2L, "b", StatusListMatchKind.OtherName) },
            Match(new Row("a", "Panthera leo"), new Row("b", "Leo leo", "ANIMALIA", "Panthera leo", "Panthera tigris")));

    [Fact]
    public void IucnSynonymDoesNotTakeATaxonAlreadyMatchedByName() =>
        Assert.Equal(new[] { (1L, "a", StatusListMatchKind.Name) },
            Match(new Row("a", "Panthera leo"), new Row("b", "Felis leo")));

    // A row whose own name finds a taxon an earlier row has is dropped: its other names are not tried.
    [Fact]
    public void RowWhoseNameFindsATakenTaxonIsNotLookedUpAgain() =>
        Assert.Equal(new[] { (1L, "a", StatusListMatchKind.Name) },
            Match(new Row("a", "Panthera leo"), new Row("b", "Panthera leo", "ANIMALIA", "Panthera tigris")));

    [Fact]
    public void OtherNamesComeBeforeTheIucnSynonym() =>
        Assert.Equal(new[] { (2L, "a", StatusListMatchKind.OtherName) },
            Match(new Row("a", "Felis leo", "ANIMALIA", "Panthera tigris")));

    // After the name matches, rows are matched in their order, each by its other names and then by
    // IUCN synonym, so an earlier row's IUCN synonym match keeps the taxon from a later row's other name.
    [Fact]
    public void EarlierRowKeepsATaxonFoundByIucnSynonym() =>
        Assert.Equal(new[] { (1L, "a", StatusListMatchKind.IucnSynonym) },
            Match(new Row("a", "Felis leo"), new Row("b", "Leo leo", "ANIMALIA", "Panthera leo")));

    [Fact]
    public void UnmatchedRowsStayUnmatched() =>
        Assert.Empty(Match(new Row("a", "Leo leo"), new Row("b", "Felis catus", "ANIMALIA", "Felis silvestris")));

    [Fact]
    public void KingdomLimitsTheMatchAndNullMeansAnyKingdom() {
        Assert.Empty(Match(new Row("a", "Quercus robur", "ANIMALIA")));
        Assert.Equal(new[] { (3L, "a", StatusListMatchKind.Name) }, Match(new Row("a", "Quercus robur", null)));
    }

    [Fact]
    public void MatchesAreInTheOrderTheyWereMade() =>
        Assert.Equal(new[] { (2L, "b", StatusListMatchKind.Name), (1L, "a", StatusListMatchKind.IucnSynonym) },
            Match(new Row("a", "Felis leo"), new Row("b", "Panthera tigris")));
}
