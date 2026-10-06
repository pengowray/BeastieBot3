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
        var placement = ListPlacement.Place(text, result.Members!, scope, options.AddIds, options.AddYear);
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

    [Fact]
    public void NothingIsPlacedInAPartialList() {
        var lookup = new FakeScopeLookup().Group(1, null, "kingdom", "Animalia").Group(4, 1, "genus", "Panthera").Species(4, 100, 30);
        var members = Enumerable.Range(100, 10).Select(i => new ListMember(lookup.Taxon(i), i - 99, $"Panthera {FakeScopeLookup.Epithet(i)}", null, OnListLine: true)).ToList();
        var scope = ListScope.Check(members, lookup, new ListScopeOptions(ListAnyway: true))!;
        Assert.True(scope.Partial);
        Assert.Empty(ListPlacement.Place("", members, scope, false, false).Placed);
    }
}
