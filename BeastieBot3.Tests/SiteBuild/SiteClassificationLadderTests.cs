using BeastieBot3.Classifications;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// How `site build-db` matches IUCN species to the classifications store's species
// (SiteLadders.ReadClassifications): by id first (AviList's BirdLife id, the Wikidata item's id for
// ITIS, POWO and WoRMS), then by the species' own name in IUCN's kingdom, then by a synonym unless
// another IUCN species leads to the same species.
public sealed class SiteClassificationLadderTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "beastiebot-classification-tests", Guid.NewGuid().ToString("N"));

    public SiteClassificationLadderTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static SiteTaxon Taxon(long id, string name, string kingdom, string className) =>
        new() { TaxonId = id, ScientificName = name, Kind = SiteTaxonKind.Species, Kingdom = kingdom, ClassName = className };

    private string Store(Action<ClassificationStore> fill) {
        var path = Path.Combine(_dir, "classifications.sqlite");
        using var store = ClassificationStore.Open(path);
        fill(store);
        return path;
    }

    private static ClassificationNode N(string id, string? parent, string rank, string name) => new(id, parent, rank, name);

    [Fact]
    public void ABirdIsMatchedByItsBirdLifeIdAndGetsClassAvesAboveTheList() {
        var path = Store(s => s.Replace(ClassificationSources.AviList, "v2025b", null, null,
            [N("order:Struthioniformes", null, "order", "Struthioniformes"), N("species:Struthio camelus", "order:Struthioniformes", "species", "Struthio camelus")],
            [new ClassificationName("Struthio camelus", "species:Struthio camelus", false)],
            [new ClassificationLink("iucn", "45020636", "species:Struthio camelus")]));
        // IUCN's name differs; the id still matches.
        var (nodes, starts, _) = SiteLadders.ReadClassifications(path, [Taxon(45020636, "Struthio camelus old", "ANIMALIA", "AVES")], [], CancellationToken.None);
        Assert.Equal([new TaxonLadderStart(45020636, "avilist", "species:Struthio camelus")], starts);
        Assert.Contains(nodes, n => n is { Id: "order:Struthioniformes", ParentId: "class:Aves" });
        Assert.Contains(nodes, n => n is { Id: "class:Aves", Rank: "class", Name: "Aves", ParentId: null });
    }

    [Fact]
    public void ANameIsMatchedOnlyInIucnsKingdom() {
        var path = Store(s => s.Replace(ClassificationSources.Itis, "2026", null, null, [
                N("1", null, "kingdom", "Animalia"), N("2", null, "kingdom", "Plantae"),
                N("10", "1", "species", "Ficus variegata"), N("20", "2", "species", "Ficus variegata"),
            ], [new ClassificationName("Ficus variegata", "10", false), new ClassificationName("Ficus variegata", "20", false)]));
        var (_, starts, _) = SiteLadders.ReadClassifications(path, [Taxon(5, "Ficus variegata", "PLANTAE", "MAGNOLIOPSIDA")], [], CancellationToken.None);
        Assert.Equal([new TaxonLadderStart(5, "itis", "20")], starts);
    }

    [Fact]
    public void TheWikidataIdComesFirstAndASynonymSharedByTwoIucnSpeciesMatchesNeither() {
        var path = Store(s => s.Replace(ClassificationSources.Worms, "2026", null, null, [
                N("2", null, "kingdom", "Animalia"),
                N("100", "2", "species", "Aus bus"), N("200", "2", "species", "Cus dus"), N("300", "2", "species", "Eus fus"),
            ], [
                new ClassificationName("Aus bus", "100", false), new ClassificationName("Cus dus", "200", false), new ClassificationName("Eus fus", "300", false),
                new ClassificationName("Gus hus", "300", true), new ClassificationName("Ius jus", "300", true),
            ]));
        SiteTaxon[] taxa = [Taxon(1, "Aus bus", "ANIMALIA", "ACTINOPTERYGII"), Taxon(2, "Gus hus", "ANIMALIA", "ACTINOPTERYGII"),
            Taxon(3, "Ius jus", "ANIMALIA", "ACTINOPTERYGII")];
        // Taxon 1's Wikidata item gives WoRMS 200, which wins over its own name.
        var (_, starts, summary) = SiteLadders.ReadClassifications(path, taxa, [new SiteExternalId(1, "P850", "200")], CancellationToken.None);
        Assert.Equal([new TaxonLadderStart(1, "worms", "200")], starts);
        Assert.Equal(["worms 1 taxa (1 by id)"], summary);
    }
}
