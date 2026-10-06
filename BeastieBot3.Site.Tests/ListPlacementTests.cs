using BeastieBot3.Site.Data;
using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

public sealed class ListPlacementTests {
    // Panthera (node 4) has species 100 to 103, Felis (node 5) 200 and 201; all LC in 2020.
    private static FakeScopeLookup Tree() => new FakeScopeLookup()
        .Group(1, null, "kingdom", "Animalia")
        .Group(2, 1, "class", "Mammalia")
        .Group(3, 2, "family", "Felidae")
        .Group(4, 3, "genus", "Panthera")
        .Group(5, 3, "genus", "Felis")
        .Species(4, 100, 4)
        .Species(5, 200, 2);

    // The same taxa for the status updater.
    private static FakeStatusLookup Statuses() {
        var lookup = new FakeStatusLookup();
        foreach (var id in new long[] { 100, 101, 102, 103 }) {
            lookup.Taxon(id, $"Panthera {FakeScopeLookup.Epithet(id)}", "LC", 2020, id * 10, node: 4);
        }
        foreach (var id in new long[] { 200, 201 }) {
            lookup.Taxon(id, $"Felis {FakeScopeLookup.Epithet(id)}", "LC", 2020, id * 10, node: 5);
        }
        return lookup;
    }

    // The page's steps: update, compare, put the missing species in.
    private static (string Text, ListPlacementResult Placement) Run(string text, StatusUpdateOptions? options = null, string? scopeKey = null) {
        options ??= new StatusUpdateOptions();
        var updater = new StatusUpdater(Statuses(), new DateOnly(2026, 10, 7), options: options);
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, Tree(), new ListScopeOptions(scopeKey))!;
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions(options.AddIds, options.AddYear), Tree());
        return (updater.TextWith(ListPlacement.Insertions(text, placement)), placement);
    }

    [Fact]
    public void AMissingSpeciesGoesInAlphabeticalOrder() {
        var (text, placement) = Run("== Panthera ==\n* ''Panthera spbaa''\n* ''Panthera spbac''\n* ''Panthera spbad''\n== Felis ==\n* ''Felis spcaa''\n");
        Assert.Equal("== Panthera ==\n* ''Panthera spbaa''\n* ''[[Panthera spbab]]''\n* ''Panthera spbac''\n* ''Panthera spbad''\n"
            + "== Felis ==\n* ''Felis spcaa''\n* ''[[Felis spcab]]''\n", text);
        // In order of scientific name: Felis first.
        Assert.Equal([201L, 101L], placement.Placed.Select(p => p.Taxon.TaxonId));
        Assert.Equal(2, placement.Placed[1].NeighbourLine);
        Assert.Equal("Panthera spbaa", placement.Placed[1].NeighbourName);
        Assert.False(placement.Placed[1].Before);
    }

    [Fact]
    public void ASpeciesThatSortsFirstGoesBeforeTheFirst() {
        var (text, _) = Run("* ''Panthera spbab''\n* ''Panthera spbac''\n* ''Panthera spbad''\n");
        Assert.StartsWith("* ''[[Panthera spbaa]]''\n* ''Panthera spbab''\n", text);
    }

    [Fact]
    public void AGenusNotInOrderGetsItAfterItsLastSpecies() {
        var (text, _) = Run("* ''Panthera spbad''\n* ''Panthera spbaa''\n* ''Panthera spbac''\n");
        Assert.Equal("* ''Panthera spbad''\n* ''Panthera spbaa''\n* ''Panthera spbac''\n* ''[[Panthera spbab]]''\n", text);
    }

    [Fact]
    public void TheNewLineCopiesTheMarkersAndStatusAndSkipsSubspeciesLines() {
        var (text, _) = Run("**''Panthera spbaa'' {{IUCN status|NT}}\n***''Panthera spbaa'' subsp. x\n:note\n**''Panthera spbac'' {{IUCN status|LC}}\n"
            + "**''Panthera spbad'' {{IUCN status|LC}}\n", new StatusUpdateOptions { AddIds = true, AddYear = true });
        Assert.Equal("**''Panthera spbaa'' {{IUCN status|LC|100/1000|1|year=2020}}\n***''Panthera spbaa'' subsp. x\n:note\n"
            + "**''[[Panthera spbab]]'' {{IUCN status|LC|101/1010|1|year=2020}}\n**''Panthera spbac'' {{IUCN status|LC|102/1020|1|year=2020}}\n"
            + "**''Panthera spbad'' {{IUCN status|LC|103/1030|1|year=2020}}\n", text);
    }

    [Fact]
    public void AStatusAddedAtTheEndOfTheLineComesBeforeTheNewLine() {
        var (text, _) = Run("* ''Panthera spbaa''\n* ''Panthera spbac''\n* ''Panthera spbad''\n", new StatusUpdateOptions { AddToListLines = true });
        Assert.Equal("* ''Panthera spbaa'' {{IUCN status|LC}}\n* ''[[Panthera spbab]]'' {{IUCN status|LC}}\n"
            + "* ''Panthera spbac'' {{IUCN status|LC}}\n* ''Panthera spbad'' {{IUCN status|LC}}\n", text);
    }

    [Fact]
    public void ListsWithIdsInTheirTemplatesCount() {
        var (text, placement) = Run("* ''Panthera spbaa'' {{IUCN status|LC|100/1000|1}}\n* ''Panthera spbac'' {{IUCN status|LC|102/1020|1}}\n"
            + "* ''Panthera spbad'' {{IUCN status|LC|103/1030|1}}\n");
        Assert.Contains("\n* ''[[Panthera spbab]]'' {{IUCN status|LC}}\n", text);
        Assert.Single(placement.Placed);
    }

    [Fact]
    public void CommonNameLinesGiveTheStyle() {
        var (text, _) = Run("* [[Panthera spbaa|Big cat]] (''Panthera spbaa'')\n* [[Panthera spbac|Other cat]] (''Panthera spbac'')\n"
            + "* [[Panthera spbad|Third cat]] (''Panthera spbad'')\n");
        Assert.Contains("\n* ''[[Panthera spbab]]''\n", text);
    }

    [Fact]
    public void ASynonymSortsWhereTheListWritesIt() {
        // spbad is written as its synonym "Panthera aaa", first in the list.
        var statuses = Statuses().Synonym(103, "Panthera aaa");
        var updater = new StatusUpdater(statuses, new DateOnly(2026, 10, 7));
        var text = "* ''Panthera aaa''\n* ''Panthera spbaa''\n* ''Panthera spbac''\n";
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, Tree())!;
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions(), Tree());
        Assert.Equal("* ''Panthera aaa''\n* ''Panthera spbaa''\n* ''[[Panthera spbab]]''\n* ''Panthera spbac''\n",
            updater.TextWith(ListPlacement.Insertions(text, placement)));
    }

    [Fact]
    public void AListStartingOnItsTemplateLine() {
        var (text, _) = Run("{{columns-list|colwidth=30em|*''Panthera spbaa''\n*''Panthera spbac''\n*''Panthera spbad''}}\n");
        Assert.Equal("{{columns-list|colwidth=30em|*''Panthera spbaa''\n*''[[Panthera spbab]]''\n*''Panthera spbac''\n*''Panthera spbad''}}\n", text);
        var (first, _) = Run("{{columns-list|colwidth=30em|*''Panthera spbab''\n*''Panthera spbac''\n*''Panthera spbad''}}\n");
        Assert.Equal("{{columns-list|colwidth=30em|*''[[Panthera spbaa]]''\n*''Panthera spbab''\n*''Panthera spbac''\n*''Panthera spbad''}}\n", first);
    }

    [Fact]
    public void LinesOfSpeciesIucnDoesNotHaveKeepTheOrder() {
        // "Panthera spbaaz" is not an IUCN species; it sorts before spbab.
        var (text, placement) = Run("* ''Panthera spbaa''\n** ''Panthera spbaa'' subsp. x\n* ''Panthera spbaaz''\n* ''Panthera spbabz''\n"
            + "* ''Panthera spbac''\n* ''Panthera spbad''\n");
        Assert.Equal("* ''Panthera spbaa''\n** ''Panthera spbaa'' subsp. x\n* ''Panthera spbaaz''\n* ''[[Panthera spbab]]''\n* ''Panthera spbabz''\n"
            + "* ''Panthera spbac''\n* ''Panthera spbad''\n", text);
        Assert.Null(placement.Placed[0].NeighbourTaxon);
        Assert.Equal("Panthera spbaaz", placement.Placed[0].NeighbourName);
    }

    [Fact]
    public void CrlfTextGetsCrlfLines() {
        var (text, _) = Run("* ''Panthera spbaa''\r\n* ''Panthera spbac''\r\n* ''Panthera spbad''\r\n");
        Assert.Equal("* ''Panthera spbaa''\r\n* ''[[Panthera spbab]]''\r\n* ''Panthera spbac''\r\n* ''Panthera spbad''\r\n", text);
    }

    [Fact]
    public void ASpeciesOfAGenusTheListDoesNotHaveIsNotPlaced() {
        var (text, placement) = Run("* ''Panthera spbaa''\n* ''Panthera spbab''\n* ''Panthera spbac''\n* ''Panthera spbad''\n* ''Felis spcaa''\n| table\n");
        Assert.Contains("[[Felis spcab]]", text);
        var (_, none) = Run("* ''Panthera spbaa''\n* ''Panthera spbab''\n* ''Panthera spbac''\n* ''Panthera spbad''\n", scopeKey: "family/Felidae");
        Assert.Empty(none.Placed);
        Assert.Equal([200L, 201L], none.Unplaced.Select(t => t.TaxonId));
        Assert.Single(placement.Placed);
    }

    [Fact]
    public void PlacingAgainAddsNothing() {
        var (first, _) = Run("* ''Panthera spbaa''\n* ''Panthera spbac''\n* ''Panthera spbad''\n* ''Felis spcaa''\n");
        var (second, placement) = Run(first);
        Assert.Equal(first, second);
        Assert.Empty(placement.Placed);
    }

    private static (string Text, ListPlacementResult Placement) RunWith(FakeScopeLookup tree, string text, string? scopeKey = null) {
        var updater = new StatusUpdater(Statuses(), new DateOnly(2026, 10, 7));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree, new ListScopeOptions(scopeKey))!;
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions(), tree);
        return (updater.TextWith(ListPlacement.Insertions(text, placement)), placement);
    }

    private static string Row(string name, string binomial, string extra = "|image=File:x.jpg |image-size=180px\n|range=Asia\n") =>
        $"{{{{Species table/row\n|name=[[{name}]] |binomial={binomial}\n{extra}|authority-name=X |authority-year=1900\n|iucn-status=LC |population=Unknown\n"
        + "|direction={{decrease|Population declining}}<ref name=\"IUCN\"/>\n}}";

    [Fact]
    public void ASpeciesTableGetsARowInCommonNameOrder() {
        // The rows are in order of common name, not of scientific name.
        var tree = Tree().Common(100, "Ccc cat").Common(101, "Bbb cat").Common(102, "Aaa cat").Common(103, "Ddd cat");
        var text = "{{Species table |no-note=y |genus=[[Panthera]] |species-count=four}}\n" + Row("Aaa cat", "P. spbac") + "\n"
            + Row("Ccc cat", "P. spbaa") + "\n" + Row("Ddd cat", "P. spbad") + "\n{{Species table/end}}\n";
        var (result, placement) = RunWith(tree, text);
        var placed = Assert.Single(placement.Placed);
        Assert.Equal("Aaa cat", placed.NeighbourName);
        Assert.False(placed.Before);
        var newRow = "{{Species table/row\n|name=[[Panthera spbab|Bbb cat]] |binomial=P. spbab\n|image= |image-size=\n|range=\n"
            + "|authority-name=Smith |authority-year=1900 |authority-not-original=yes\n|iucn-status=LC |population=1,000\n"
            + "|direction={{decrease|Population declining}}\n}}";
        Assert.Contains(Row("Aaa cat", "P. spbac") + "\n" + newRow + "\n" + Row("Ccc cat", "P. spbaa"), result);
    }

    [Fact]
    public void AGenusWithNoTableGetsATableNextToItsFamily() {
        var tree = Tree().Common(200, "Fff cat").Common(201, "Ggg cat");
        var text = "{{Species table |no-note=y |genus=[[Panthera]] |species-count=four}}\n" + Row("Aaa cat", "P. spbaa") + "\n"
            + Row("Bbb cat", "P. spbab") + "\n" + Row("Ccc cat", "P. spbac") + "\n" + Row("Ddd cat", "P. spbad") + "\n{{Species table/end}}\n";
        var (result, placement) = RunWith(tree, text, "family/Felidae");
        Assert.Equal([200L, 201L], placement.Placed.Select(p => p.Taxon.TaxonId));
        Assert.Empty(placement.Unplaced);
        // Felis sorts before Panthera.
        Assert.StartsWith("{{Species table |no-note=y |genus=[[Felis]] |species-count=two}}\n{{Species table/row\n|name=[[Felis spcaa|Fff cat]] |binomial=F. spcaa", result);
        Assert.Contains("|binomial=F. spcab", result);
        Assert.Contains("}}\n{{Species table/end}}\n{{Species table |no-note=y |genus=[[Panthera]]", result);
    }

    [Fact]
    public void AWikitableGetsARow() {
        var text = "{| class=\"wikitable\"\n! Name !! Scientific name !! Status\n|-\n| [[Aaa cat]] || ''[[Panthera spbaa]]'' || LC\n|-\n"
            + "| [[Ccc cat]] || ''[[Panthera spbac]]'' || LC\n|-\n| [[Ddd cat]] || ''[[Panthera spbad]]'' || LC\n|}\n";
        var tree = Tree().Common(101, "Bbb cat");
        var (result, _) = RunWith(tree, text);
        Assert.Contains("| [[Aaa cat]] || ''[[Panthera spbaa]]'' || LC\n|-\n| [[Panthera spbab|Bbb cat]] || ''[[Panthera spbab]]'' || LC\n|-\n| [[Ccc cat]]", result);
    }

    [Fact]
    public void ASubspeciesGoesUnderItsSpecies() {
        // Two subspecies of spbaa; the list has one of them.
        var tree = Tree().Infra(500, 100, "alpha").Infra(501, 100, "beta");
        var statuses = Statuses()
            .Taxon(500, "Panthera spbaa ssp. alpha", "LC", 2020, 5000, node: 4, kind: TaxonKinds.Subspecies)
            .Taxon(501, "Panthera spbaa ssp. beta", "LC", 2020, 5010, node: 4, kind: TaxonKinds.Subspecies);
        var text = "* ''Panthera spbaa'' {{IUCN status|LC}}\n** ''Panthera spbaa alpha'' {{IUCN status|LC}}\n* ''Panthera spbab''\n"
            + "* ''Panthera spbac''\n* ''Panthera spbad''\n";
        var updater = new StatusUpdater(statuses, new DateOnly(2026, 10, 7));
        var result = updater.Update(text);
        var scope = ListScope.Check(result.Members!, tree)!;
        Assert.True(scope.InfraChecked);
        var placement = ListPlacement.Place(text, result.Members!, scope, new ListPlacementOptions(), tree);
        Assert.Equal("* ''Panthera spbaa'' {{IUCN status|LC}}\n** ''Panthera spbaa alpha'' {{IUCN status|LC}}\n** ''Panthera spbaa beta'' {{IUCN status|LC}}\n"
            + "* ''Panthera spbab''\n* ''Panthera spbac''\n* ''Panthera spbad''\n", updater.TextWith(ListPlacement.Insertions(text, placement)));
    }

    [Fact]
    public void ABulletListInCommonNameOrder() {
        var tree = Tree().Common(100, "Ccc cat").Common(101, "Bbb cat").Common(102, "Aaa cat").Common(103, "Ddd cat");
        var text = "* [[Aaa cat]] (''Panthera spbac'')\n* [[Ccc cat]] (''Panthera spbaa'')\n* [[Ddd cat]] (''Panthera spbad'')\n";
        var (result, _) = RunWith(tree, text);
        Assert.Equal("* [[Aaa cat]] (''Panthera spbac'')\n* [[Panthera spbab|Bbb cat]] (''Panthera spbab'')\n* [[Ccc cat]] (''Panthera spbaa'')\n"
            + "* [[Ddd cat]] (''Panthera spbad'')\n", result);
    }

    [Fact]
    public void NothingIsPlacedInAPartialList() {
        var lookup = new FakeScopeLookup().Group(1, null, "kingdom", "Animalia").Group(4, 1, "genus", "Panthera").Species(4, 100, 30);
        var members = Enumerable.Range(100, 10).Select(i => new ListMember(lookup.Taxon(i), i - 99, $"Panthera {FakeScopeLookup.Epithet(i)}", null, ListMemberSource.ListLine)).ToList();
        var scope = ListScope.Check(members, lookup, new ListScopeOptions(ListAnyway: true))!;
        Assert.True(scope.Partial);
        Assert.Empty(ListPlacement.Place("", members, scope, new ListPlacementOptions(), lookup).Placed);
    }
}
