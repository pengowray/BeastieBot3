using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

/// A tree of groups and taxa in memory for ListScope.
internal sealed class FakeScopeLookup : IListScopeLookup {
    private readonly Dictionary<int, GroupRow> _groups = [];
    private readonly List<(ListTaxonRow Row, StatusTaxon Taxon)> _taxa = [];

    public FakeScopeLookup Group(int id, int? parent, string rank, string name, string source = GroupSources.Iucn, string? common = null, int firstPos = 0) {
        var depth = parent is { } p ? _groups[p].Depth + 1 : 0;
        _groups[id] = new GroupRow(id, parent, depth, rank, name, source, true, "ANIMALIA", null, common, null, null, firstPos, 0, 0, 0, 0);
        return this;
    }

    /// Adds count species to a genus: "Name1", "Name2" ... in category, with taxon ids from firstId.
    public FakeScopeLookup Species(int node, long firstId, int count, string category = "LC", string kind = TaxonKinds.Species, bool pe = false) {
        for (var i = 0; i < count; i++) {
            var id = firstId + i;
            var name = $"{_groups[node].Name} {Epithet(id)}";
            var latest = new AssessmentRow(id * 10, id, "Global", true, category, pe, false, null, "3.1", 2020, null, null, null);
            _taxa.Add((new ListTaxonRow(id, name, kind, "ANIMALIA", _groups[node].Name, Epithet(id), null, null, null, null, null, null, null,
                node, _taxa.Count, id * 10, category, pe, false, 2020), new StatusTaxon(id, name, true, null, latest, kind, node)));
        }
        return this;
    }

    /// "spbaa" for 100: an epithet of letters, so the name has the shape of a scientific name.
    public static string Epithet(long id) => "sp" + string.Concat(id.ToString(System.Globalization.CultureInfo.InvariantCulture).Select(d => (char)('a' + (d - '0'))));

    /// Adds a subspecies "Genus spbaa ssp. infra" of a species already added.
    public FakeScopeLookup Infra(long id, long parent, string infra) {
        var species = _taxa.Single(t => t.Row.TaxonId == parent).Row;
        var name = $"{species.ScientificName} ssp. {infra}";
        var latest = new AssessmentRow(id * 10, id, "Global", true, "LC", false, false, null, "3.1", 2020, null, null, null);
        _taxa.Add((species with { TaxonId = id, ScientificName = name, Kind = TaxonKinds.Subspecies, InfraRank = "ssp.", InfraName = infra,
            ParentTaxonId = parent, TreePos = _taxa.Count, AssessmentId = id * 10, CommonNameEn = null },
            new StatusTaxon(id, name, true, null, latest, TaxonKinds.Subspecies, species.NodeId)));
        return this;
    }

    /// Gives a taxon an English name.
    public FakeScopeLookup Common(long id, string name) {
        var i = _taxa.FindIndex(t => t.Row.TaxonId == id);
        _taxa[i] = (_taxa[i].Row with { CommonNameEn = name }, _taxa[i].Taxon);
        return this;
    }

    public StatusTaxon Taxon(long id) => _taxa.Single(t => t.Row.TaxonId == id).Taxon;

    private bool Under(int node, int group) {
        for (int? at = node; at is { } a; at = _groups[a].ParentNodeId) {
            if (a == group) {
                return true;
            }
        }
        return false;
    }

    private IEnumerable<(ListTaxonRow Row, StatusTaxon Taxon)> Within(int group) => _taxa.Where(t => Under(t.Row.NodeId, group));

    public IReadOnlyList<GroupRow> PathOf(int nodeId) {
        var path = new List<GroupRow>();
        for (int? at = nodeId; at is { } a; at = _groups[a].ParentNodeId) {
            var g = _groups[a];
            var within = Within(a).ToList();
            path.Insert(0, g with {
                SpeciesCount = within.Count(t => t.Row.Kind == TaxonKinds.Species),
                InfraCount = within.Count(t => t.Row.Kind != TaxonKinds.Species),
            });
        }
        return path;
    }

    public IReadOnlyList<GroupCategoryCount> CountsOf(int nodeId) =>
        [.. Within(nodeId).GroupBy(t => GroupList.StatusCode(t.Row)).Select(g => new GroupCategoryCount(g.Key,
            g.Count(t => t.Row.Kind == TaxonKinds.Species), g.Count(t => t.Row.Kind != TaxonKinds.Species), 0))];

    public IReadOnlyDictionary<long, TableTaxonExtra> ExtrasOf(GroupRow group) =>
        Within(group.NodeId).ToDictionary(t => t.Row.TaxonId, t => new TableTaxonExtra(t.Row.TaxonId, "(Smith, 1900)", "Decreasing", "1000", null, null, null));

    private readonly List<ExtraSpeciesRow> _extra = [];

    /// A species of the Catalogue of Life that IUCN does not have, in a genus.
    public FakeScopeLookup Extra(int id, int node, string epithet) {
        var genus = _groups[node].Name;
        _extra.Add(new ExtraSpeciesRow(id, true, false, $"{genus} {epithet}", null, genus, epithet, "ANIMALIA", null, null, null, null, node, id,
            "(Jones, 2001)"));
        return this;
    }

    public IReadOnlyList<ExtraSpeciesRow> ExtraSpeciesIn(GroupRow group) => [.. _extra.Where(e => Under(e.NodeId, group.NodeId))];

    public IReadOnlyList<ListTaxonRow> TaxaIn(GroupRow group, IReadOnlyCollection<string> kinds) =>
        [.. Within(group.NodeId).Select(t => t.Row).Where(r => kinds.Contains(r.Kind))];

    private readonly Dictionary<(string Area, long TaxonId), AreaRecord> _areas = [];

    /// Gives taxa firstId .. firstId + count - 1 a record for the area.
    public FakeScopeLookup Area(string area, long firstId, int count, AreaOrigin origin = AreaOrigin.Native,
        AreaPresence presence = AreaPresence.Extant, bool endemic = false) {
        for (var id = firstId; id < firstId + count; id++) {
            _areas[(area, id)] = new AreaRecord(origin, presence, endemic);
        }
        return this;
    }

    public IReadOnlyList<(ListTaxonRow Row, AreaRecord Record)> TaxaInArea(GroupRow group, IReadOnlyCollection<string> kinds, string area) =>
        [.. TaxaIn(group, kinds).Where(r => _areas.ContainsKey((area, r.TaxonId))).Select(r => (r, _areas[(area, r.TaxonId)]))];

    public IReadOnlyDictionary<long, AreaRecord> AreaRecordsOf(string area, IReadOnlyCollection<long> taxonIds) =>
        taxonIds.Where(id => _areas.ContainsKey((area, id))).Distinct().ToDictionary(id => id, id => _areas[(area, id)]);

    private readonly Dictionary<(string Region, long TaxonId), string> _regional = [];

    /// Gives a taxon an assessment in the region, in the category, with assessment id id * 10 + 1.
    public FakeScopeLookup Regional(string region, long id, string category) {
        _regional[(region, id)] = category;
        return this;
    }

    /// The taxon as a status lookup for the region gives it: its latest assessment there, or none.
    public StatusTaxon RegionalTaxon(string region, long id) {
        var taxon = Taxon(id);
        return taxon with {
            LatestGlobal = _regional.TryGetValue((region, id), out var category)
                ? new AssessmentRow(id * 10 + 1, id, region, true, category, false, false, null, "3.1", 2021, null, null, null)
                : null,
        };
    }

    public IReadOnlyList<ListTaxonRow> TaxaInRegion(GroupRow group, IReadOnlyCollection<string> kinds, string region) =>
        [.. TaxaIn(group, kinds).Where(r => _regional.ContainsKey((region, r.TaxonId)))
            .Select(r => r with { Category = _regional[(region, r.TaxonId)], AssessmentId = r.TaxonId * 10 + 1, YearPublished = 2021 })];
}

public sealed class ListScopeTests {
    // Animalia > Mammalia > (CoL subclass Theria > Felidae > Panthera, Felis), (Tachyglossidae > Zaglossus).
    private static FakeScopeLookup Tree() => new FakeScopeLookup()
        .Group(1, null, "kingdom", "Animalia")
        .Group(2, 1, "class", "Mammalia")
        .Group(7, 2, "subclass", "Theria", GroupSources.Col)
        .Group(3, 7, "family", "Felidae")
        .Group(4, 3, "genus", "Panthera")
        .Group(5, 3, "genus", "Felis")
        .Group(8, 2, "family", "Tachyglossidae")
        .Group(9, 8, "genus", "Zaglossus");

    private static ListMember Member(FakeScopeLookup lookup, long id, int line, string? code = null, string? written = null) {
        var taxon = lookup.Taxon(id);
        return new ListMember(taxon, line, written ?? taxon.ScientificName, code);
    }

    private static List<ListMember> Members(FakeScopeLookup lookup, IEnumerable<long> ids, string? code = null) =>
        [.. ids.Select((id, i) => Member(lookup, id, i + 1, code))];

    [Fact]
    public void FindsTheFamilyAndTheMissingSpecies() {
        var lookup = Tree().Species(4, 100, 3).Species(5, 200, 2);
        var result = ListScope.Check(Members(lookup, [100, 101, 102, 200]), lookup)!;
        Assert.Equal("Felidae", result.Scope.Name);
        Assert.False(result.Partial);
        Assert.Equal(4, result.Species);
        Assert.Equal(5, result.SpeciesInScope);
        Assert.Equal(201, Assert.Single(result.Missing!).TaxonId);
        Assert.Contains("{{IUCN status|LC|201/2010|1|year=2020}}", ListScope.MissingLines(result));
        // Groups above, and below it the genus that holds most of the listed taxa.
        Assert.Equal(["Animalia", "Mammalia", "Theria", "Felidae", "Panthera"], result.Path.Select(g => g.Name));
    }

    [Fact]
    public void WithARegionOnlyTheTaxaAssessedThereAreCompared() {
        var lookup = Tree().Species(4, 100, 3).Species(5, 200, 2)
            .Regional("Europe", 100, "LC").Regional("Europe", 101, "LC").Regional("Europe", 200, "EN");
        List<ListMember> members = [.. new long[] { 100, 101, 102 }.Select((id, i) => {
            var taxon = lookup.RegionalTaxon("Europe", id);
            return new ListMember(taxon, i + 1, taxon.ScientificName, null);
        })];
        var result = ListScope.Check(members, lookup, new ListScopeOptions(Scope: "family/Felidae") { Region = "Europe", Area = "GB" })!;
        Assert.Equal("Europe", result.Region);
        Assert.Null(result.Area);
        Assert.Equal((2, 3), (result.Species, result.SpeciesInScope));
        Assert.Equal(102, Assert.Single(result.NotInRegion).Taxon.TaxonId);
        var missing = Assert.Single(result.Missing!);
        Assert.Equal((200L, "EN"), (missing.TaxonId, missing.Category));
        Assert.Contains("{{IUCN status|EN|200/2001|1|year=2021}}", ListScope.MissingLines(result));
    }

    [Fact]
    public void TooFewTaxaGiveNoComparison() {
        var lookup = Tree().Species(4, 100, 3);
        Assert.Null(ListScope.Check(Members(lookup, [100, 101]), lookup));
    }

    [Fact]
    public void APartListListsNothingUnlessAsked() {
        var lookup = Tree().Species(4, 100, 30);
        var members = Members(lookup, Enumerable.Range(100, 10).Select(i => (long)i));
        var result = ListScope.Check(members, lookup)!;
        Assert.True(result.Partial);
        Assert.Null(result.Missing);
        var anyway = ListScope.Check(members, lookup, new ListScopeOptions(ListAnyway: true))!;
        Assert.Equal(20, anyway.Missing!.Count);
    }

    [Fact]
    public void APartListOfFewTaxaIsNotReported() {
        var lookup = Tree().Species(4, 100, 30);
        Assert.Null(ListScope.Check(Members(lookup, [100, 101, 102, 103]), lookup));
    }

    [Fact]
    public void WrittenCodesMakeItAListOfThatCategory() {
        // Panthera: 6 CR, 20 LC, and one taxon the text gives as CR that is now EN.
        var lookup = Tree().Species(4, 100, 6, "CR").Species(4, 200, 20).Species(4, 300, 1, "EN");
        var members = Members(lookup, [100, 101, 102, 103, 104, 300], code: "CR");
        var result = ListScope.Check(members, lookup)!;
        Assert.Equal(new HashSet<string> { "CR" }, result.Categories);
        Assert.True(result.CategoriesFromCodes);
        Assert.Equal(105, Assert.Single(result.Missing!).TaxonId);
        var moved = Assert.Single(result.OtherCategory);
        Assert.Equal(300, moved.Taxon.TaxonId);
        Assert.Equal("CR", moved.WrittenCode);
        Assert.Equal("EN", moved.CurrentCode);
    }

    [Fact]
    public void AListWithNoCodesTakesTheCategoryItsSpeciesAreInNow() {
        var lookup = Tree().Species(4, 100, 10, "EN").Species(4, 200, 30);
        var result = ListScope.Check(Members(lookup, Enumerable.Range(100, 8).Select(i => (long)i)), lookup)!;
        Assert.Equal(new HashSet<string> { "EN" }, result.Categories);
        Assert.False(result.CategoriesFromCodes);
        Assert.Equal(2, result.Missing!.Count);
    }

    [Fact]
    public void ChosenCategoriesAreComparedWhateverTheTextWrites() {
        // 8 of the genus's 10 EN species, written as VU: the codes alone would make it a list of VU.
        var lookup = Tree().Species(4, 100, 10, "EN").Species(4, 200, 30);
        var result = ListScope.Check(Members(lookup, Enumerable.Range(100, 8).Select(i => (long)i), code: "VU"), lookup,
            new ListScopeOptions { Categories = ListCategories.Threatened })!;
        Assert.True(result.CategoriesChosen);
        Assert.Equal(ListCategories.Threatened.Codes, result.Categories);
        Assert.Equal([108L, 109L], result.Missing!.Select(t => t.TaxonId));
    }

    [Fact]
    public void AListOfFewOfTheChosenCategoriesTaxaIsPartial() {
        // 3 of 10 EN species and 20 LC: a list of threatened species of one country, say.
        var lookup = Tree().Species(4, 100, 10, "EN").Species(4, 200, 30);
        var listed = Enumerable.Range(100, 3).Concat(Enumerable.Range(200, 10)).Select(i => (long)i);
        var result = ListScope.Check(Members(lookup, listed), lookup, new ListScopeOptions { Categories = ListCategories.Threatened })!;
        Assert.True(result.Partial);
        Assert.Null(result.Missing);
        Assert.Equal(ListCategories.Threatened.Codes, result.Categories);
    }

    // Genus Panthera: 10 native to BR, 2 introduced, 1 vagrant, 7 not in BR.
    private static FakeScopeLookup BrazilTree() => Tree().Species(4, 100, 20)
        .Area("BR", 100, 10).Area("BR", 106, 2, presence: AreaPresence.PossiblyExtinct)
        .Area("BR", 110, 2, AreaOrigin.Introduced).Area("BR", 112, 1, AreaOrigin.Vagrant).Area("BR", 100, 3, endemic: true);

    [Fact]
    public void AListOfAnAreaIsComparedWithTheGroupsNativeTaxaThere() {
        var lookup = BrazilTree();
        var listed = Enumerable.Range(100, 6).Concat([113]).Select(i => (long)i);
        var result = ListScope.Check(Members(lookup, listed), lookup, new ListScopeOptions { Area = "BR" })!;
        Assert.Equal("BR", result.Area);
        Assert.Equal(10, result.SpeciesInScope);
        Assert.Equal(6, result.Species);
        Assert.False(result.Partial);
        Assert.Equal([106L, 107L, 108L, 109L], result.Missing!.Select(t => t.TaxonId));
        // 106 and 107 are possibly extinct in BR: a note beside them.
        Assert.Equal([106L, 107L], result.MissingAreaRecords.Keys.Order());
        var notThere = Assert.Single(result.NotInArea);
        Assert.Equal(113, notThere.Member.Taxon.TaxonId);
        Assert.Null(notThere.Record);
    }

    [Fact]
    public void IntroducedAndVagrantTaxaCountOnlyWhenAskedFor() {
        var lookup = BrazilTree();
        var listed = Enumerable.Range(100, 10).Select(i => (long)i);
        var withIntroduced = ListScope.Check(Members(lookup, listed), lookup, new ListScopeOptions { Area = "BR", AreaMode = AreaMode.NativeAndIntroduced })!;
        Assert.Equal([110L, 111L], withIntroduced.Missing!.Select(t => t.TaxonId));
        Assert.Equal(AreaOrigin.Introduced, withIntroduced.MissingAreaRecords[110].Origin);
        var all = ListScope.Check(Members(lookup, listed), lookup, new ListScopeOptions { Area = "BR", AreaMode = AreaMode.All })!;
        Assert.Equal([110L, 111L, 112L], all.Missing!.Select(t => t.TaxonId));
    }

    [Fact]
    public void AListOfEndemicsIsComparedWithTheEndemicTaxa() {
        var lookup = BrazilTree();
        var result = ListScope.Check(Members(lookup, [100L, 101L, 105L]), lookup, new ListScopeOptions { Area = "BR", AreaMode = AreaMode.Endemic })!;
        Assert.Equal([102L], result.Missing!.Select(t => t.TaxonId));
        Assert.Equal(105, Assert.Single(result.NotInArea).Member.Taxon.TaxonId);
    }

    [Fact]
    public void ChoosingAllCategoriesComparesTheWholeGroup() {
        var lookup = Tree().Species(4, 100, 10, "EN").Species(4, 200, 30);
        var listed = Enumerable.Range(100, 10).Concat(Enumerable.Range(200, 20)).Select(i => (long)i);
        var result = ListScope.Check(Members(lookup, listed, code: "EN"), lookup, new ListScopeOptions { Categories = ListCategories.All })!;
        Assert.Null(result.Categories);
        Assert.Equal(10, result.MissingTotal);
    }

    [Fact]
    public void TheExtinctChoiceTakesPossiblyExtinctTaxaButNotOtherCriticallyEndangeredOnes() {
        var lookup = Tree().Species(4, 100, 6, "EX").Species(4, 200, 4, "CR", pe: true).Species(4, 300, 5, "CR").Species(4, 400, 20);
        var result = ListScope.Check(Members(lookup, Enumerable.Range(100, 6).Select(i => (long)i)), lookup,
            new ListScopeOptions { Categories = ListCategories.Extinct })!;
        Assert.Equal(10, result.SpeciesInScope);
        Assert.Equal([200L, 201L, 202L, 203L], result.Missing!.Select(t => t.TaxonId));
    }

    [Theory]
    [InlineData("List of threatened birds of Brazil", "threatened")]
    [InlineData("List of critically endangered amphibians", "CR")]
    [InlineData("List of endangered mammals", "EN")]
    [InlineData("List of vulnerable fish", "VU")]
    [InlineData("List of near threatened reptiles", "NT")]
    [InlineData("List of near-threatened insects", "NT")]
    [InlineData("List of recently extinct mammals", "extinct")]
    [InlineData("List of extinct animals of Europe", "extinct")]
    [InlineData("List of birds that are extinct in the wild", "EW")]
    [InlineData("List of least concern birds", "LC")]
    [InlineData("List of data deficient molluscs", "DD")]
    [InlineData("List of birds of Brazil", null)]
    [InlineData("Endangered Species Act", "EN")]
    public void TheTitleNamesTheCategories(string title, string? key) => Assert.Equal(key, ListCategories.FromTitle(title)?.Key);

    [Fact]
    public void WithoutGuessingCategoriesAListWithNoCodesIsOfTheWholeGroup() {
        var lookup = Tree().Species(4, 100, 10, "EN").Species(4, 200, 30);
        // 8 of the genus's 40 species: too little of the genus to compare with (the test above, with
        // guessing, compares it with the genus's 10 EN species).
        var result = ListScope.Check(Members(lookup, Enumerable.Range(100, 8).Select(i => (long)i)), lookup,
            new ListScopeOptions { GuessCategories = false });
        Assert.Null(result);
    }

    [Fact]
    public void ACatalogueOfLifeGroupMustHoldEveryListedTaxon() {
        // 28 cats and 2 monotremes: no IUCN group below the class holds 95%, and subclass Theria
        // holds all but the monotremes.
        var lookup = Tree().Species(4, 100, 14).Species(5, 200, 14).Species(9, 900, 2);
        var members = Members(lookup, [.. Enumerable.Range(100, 14).Select(i => (long)i), .. Enumerable.Range(200, 14).Select(i => (long)i), 900, 901]);
        var result = ListScope.Check(members, lookup)!;
        Assert.Equal("Mammalia", result.Scope.Name);
        Assert.Empty(result.Outside);
    }

    [Fact]
    public void ATaxonOutsideTheGroupIsReported() {
        var lookup = Tree().Species(4, 100, 30).Species(5, 200, 1);
        var members = Members(lookup, [.. Enumerable.Range(100, 30).Select(i => (long)i), 200]);
        var result = ListScope.Check(members, lookup)!;
        Assert.Equal("Panthera", result.Scope.Name);
        Assert.Equal(200, Assert.Single(result.Outside).Taxon.TaxonId);
        var family = ListScope.Check(members, lookup, new ListScopeOptions("family/Felidae"))!;
        Assert.Equal("Felidae", family.Scope.Name);
        Assert.Empty(family.Outside);
    }

    [Fact]
    public void OneTaxonUnderTwoNamesIsReportedOnlyWhenTheNamesDiffer() {
        var lookup = Tree().Species(4, 100, 4);
        var members = Members(lookup, [100, 101, 102, 103]);
        members.Add(Member(lookup, 100, 10, written: "Felis tigris"));
        members.Add(Member(lookup, 101, 11));
        var result = ListScope.Check(members, lookup)!;
        var duplicate = Assert.Single(result.Duplicates);
        Assert.Equal(100, duplicate.Taxon.TaxonId);
        Assert.Equal(["Panthera spbaa", "Felis tigris"], duplicate.Names);
        Assert.Equal([1, 10], duplicate.Lines);
    }

    [Fact]
    public void SubspeciesAreComparedOnlyWhenTheTextListsMostOfThem() {
        var lookup = Tree().Species(4, 100, 4).Species(4, 500, 10, kind: TaxonKinds.Subspecies);
        var few = ListScope.Check(Members(lookup, [100, 101, 102, 103, 500]), lookup)!;
        Assert.False(few.InfraChecked);
        Assert.DoesNotContain(few.Missing!, t => t.Kind == TaxonKinds.Subspecies);
        var most = ListScope.Check(Members(lookup, [100, 101, 102, 103, 500, 501, 502, 503, 504, 505]), lookup)!;
        Assert.True(most.InfraChecked);
        Assert.Equal(4, most.Missing!.Count);
    }

    [Fact]
    public void AMissingTaxonWhoseArticleIsAListIsNotLinked() {
        var lookup = Tree().Species(4, 100, 4);
        var result = ListScope.Check(Members(lookup, [100, 101, 102]), lookup)!;
        var missing = result with { Missing = [result.Missing![0] with { ListArticleTitle = "List of Panthera species", ScientificName = "Panthera spbad" }] };
        var line = ListScope.MissingLines(missing);
        Assert.DoesNotContain("List of", line);
        Assert.StartsWith("* ''Panthera spbad'' {{IUCN status|LC", line);
        Assert.Contains("[[List of Panthera species", SpeciesListLine.Format(GroupList.Entry(missing.Missing![0]), new SpeciesListLineOptions { Style = missing.Style }));
    }

    [Fact]
    public void SpeciesIucnHasNotAssessedAreComparedWhenAsked() {
        var lookup = Tree().Species(4, 100, 3).Extra(1, 4, "spzz").Extra(2, 4, "spyy");
        var members = Members(lookup, [100, 101, 102]);
        Assert.Null(ListScope.Check(members, lookup)!.MissingExtra);
        var result = ListScope.Check(members, lookup, new ListScopeOptions(Extra: true, WrittenNames: new HashSet<string> { SiteNameKey.Fold("Panthera spyy") }))!;
        var extra = Assert.Single(result.MissingExtra!);
        Assert.Equal(-1, extra.TaxonId);
        Assert.Equal("* ''[[Panthera spzz]]''", ListScope.MissingLines(result.MissingExtra!, SpeciesListStyle.ScientificNameFirst));
        Assert.Equal(3, result.ListedIn[4]);
    }

    [Theory]
    [InlineData("CR(PE)", "CR")]
    [InlineData("LR/nt", "NT")]
    [InlineData("lr/cd", "NT")]
    [InlineData("LR/lc", "LC")]
    [InlineData("en", "EN")]
    [InlineData(null, null)]
    public void CodesAreComparedByCategory(string? code, string? category) => Assert.Equal(category, ListScope.Normalize(code));

    [Fact]
    public void TheUpdaterListsTheTaxaOfTheText() {
        var statuses = new FakeStatusLookup()
            .Taxon(15955, "Panthera tigris", "EN", 2022, 214862019)
            .Taxon(700, "Felis silvestris", "LC", 2022, 7001)
            .Synonym(15955, "Felis tigris");
        var result = new StatusUpdater(statuses, new DateOnly(2026, 10, 6)).Update(
            "* ''Panthera tigris'' {{IUCN status|VU}}\n* ''Felis silvestris''\n* ''Felis tigris''\n{{Speciesbox|genus=Felis|species=silvestris|status=LC}}\n");
        Assert.Equal([(15955L, 1, "Panthera tigris", "VU"), (700L, 2, "Felis silvestris", (string?)null), (15955L, 3, "Felis tigris", null)],
            result.Members!.Select(m => (m.Taxon.TaxonId, m.Line, m.Written, m.WrittenCode)));
    }
}
