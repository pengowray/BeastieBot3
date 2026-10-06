using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

/// A tree of groups and taxa in memory for ListScope.
internal sealed class FakeScopeLookup : IListScopeLookup {
    private readonly Dictionary<int, GroupRow> _groups = [];
    private readonly List<(ListTaxonRow Row, StatusTaxon Taxon)> _taxa = [];

    public FakeScopeLookup Group(int id, int? parent, string rank, string name, string source = GroupSources.Iucn) {
        var depth = parent is { } p ? _groups[p].Depth + 1 : 0;
        _groups[id] = new GroupRow(id, parent, depth, rank, name, source, true, "ANIMALIA", null, null, null, null, 0, 0, 0, 0, 0);
        return this;
    }

    /// Adds count species to a genus: "Name1", "Name2" ... in category, with taxon ids from firstId.
    public FakeScopeLookup Species(int node, long firstId, int count, string category = "LC", string kind = TaxonKinds.Species) {
        for (var i = 0; i < count; i++) {
            var id = firstId + i;
            var name = $"{_groups[node].Name} sp{id}";
            var latest = new AssessmentRow(id * 10, id, "Global", true, category, false, false, null, "3.1", 2020, null, null, null);
            _taxa.Add((new ListTaxonRow(id, name, kind, "ANIMALIA", _groups[node].Name, $"sp{id}", null, null, null, null, null, null, null,
                node, _taxa.Count, id * 10, category, false, false, 2020), new StatusTaxon(id, name, true, null, latest, kind, node)));
        }
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
        [.. Within(nodeId).GroupBy(t => t.Row.Category!).Select(g => new GroupCategoryCount(g.Key,
            g.Count(t => t.Row.Kind == TaxonKinds.Species), g.Count(t => t.Row.Kind != TaxonKinds.Species), 0))];

    public IReadOnlyList<ListTaxonRow> TaxaIn(GroupRow group, IReadOnlyCollection<string> kinds) =>
        [.. Within(group.NodeId).Select(t => t.Row).Where(r => kinds.Contains(r.Kind))];
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
        Assert.Equal(["Panthera sp100", "Felis tigris"], duplicate.Names);
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
        var missing = result with { Missing = [result.Missing![0] with { ListArticleTitle = "List of Panthera species", ScientificName = "Panthera sp103" }] };
        var line = ListScope.MissingLines(missing);
        Assert.DoesNotContain("List of", line);
        Assert.StartsWith("* ''Panthera sp103'' {{IUCN status|LC", line);
        Assert.Contains("[[List of Panthera species", SpeciesListLine.Format(GroupList.Entry(missing.Missing![0]), new SpeciesListLineOptions { Style = missing.Style }));
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
