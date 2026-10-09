using BeastieBot3.Checklists;
using BeastieBot3.SiteBuild;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s subspecies from the Mammal Diversity Database and the Reptile Database
// (infraspecific_name sources 'mdd' and 'reptiledb', SiteSubspecies.ReadChecklists). IUCN has the lion
// (and its subspecies persica), the viviparous lizard under its old name Lacerta vivipara, and two
// lizards that the Reptile Database lumps into one species.
public sealed class SiteDbBuildChecklistSubspeciesTests : IDisposable {
    private const long Lion = 15951;
    private const long ViviparousLizard = 61741;
    private const long LumpedA = 1001;
    private const long LumpedB = 1002;
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_StoresMddSubspeciesUnderTheSpeciesOfTheSameNameLinkingItsRecord() {
        using var db = OpenReadOnly(Build(out _));
        var rows = Rows(db, "SELECT source_id, rank, name, authority FROM infraspecific_name WHERE taxon_id = @t AND source = 'mdd' ORDER BY name",
            ("@t", Lion)).Select(r => string.Join(" | ", r)).ToList();
        Assert.Equal([
            "1006020 | subspecies | Panthera leo leo | (Linnaeus, 1758)",
            "1006020 | subspecies | Panthera leo melanochaita | (Smith, 1842)",
            // Recently extinct: kept.
            "1006020 | subspecies | Panthera leo webbiensis | Zukowsky, 1964",
        ], rows);
        // Left out: the fossil sinhaleya and a name that is not genus, species and one more epithet.
    }

    [Fact]
    public void Build_StoresReptileDatabaseSubspeciesThroughTheSourcesSynonymsButNotForLumpedSpecies() {
        using var db = OpenReadOnly(Build(out _));
        var rows = Rows(db, "SELECT taxon_id, source_id, name, authority FROM infraspecific_name WHERE source = 'reptiledb' ORDER BY name")
            .Select(r => string.Join(" | ", r)).ToList();
        // IUCN's Lacerta vivipara is a synonym of the Reptile Database's Zootoca vivipara. Aus bus and
        // Aus cus both lead to Aus dus, so neither takes its subspecies.
        Assert.Equal([
            $"{ViviparousLizard} | genus=Zootoca&species=vivipara | Zootoca vivipara louislantzi | Arribas, 2009",
            $"{ViviparousLizard} | genus=Zootoca&species=vivipara | Zootoca vivipara vivipara | (Lichtenstein, 1823)",
        ], rows);
    }

    [Fact]
    public void Build_CountsEachChecklist() {
        Build(out var stats);
        var s = stats.Subspecies;
        Assert.Equal(2, s.Mdd.SourceSpecies);           // the lion and the wildcat
        Assert.Equal(1, s.Mdd.SourceSpeciesMatched);
        Assert.Equal(3, s.Mdd.Rows);
        Assert.Equal(1, s.Mdd.LeftOutFossil);
        Assert.Equal(1, s.Mdd.Unreadable);
        Assert.Equal([Lion], s.Mdd.Species);
        Assert.Equal(2, s.ReptileDb.SourceSpecies);      // Zootoca vivipara and Aus dus
        Assert.Equal(1, s.ReptileDb.SourceSpeciesMatched);
        Assert.Equal(2, s.ReptileDb.Rows);
        Assert.Equal(2, s.SpeciesWithList);              // the lion, the lizard
        Assert.Equal(1, s.SpeciesWithSeveralSources);    // the lion: IUCN's persica and MDD
    }

    // ------------------------------------------------------------ sources

    private string Build(out SiteBuildStats stats) {
        var output = _sources.PathOf("site.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var checklists = _sources.PathOf("checklists.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", $"""
                    (1, {Lion}, 'Panthera leo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, 15952, 'Panthera leo ssp. persica', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', 'subspecies', 'persica', NULL, NULL, '(Meyer, 1826)', NULL),
                    (1, {ViviparousLizard}, 'Lacerta vivipara', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Lacerta', 'vivipara', NULL, NULL, NULL, NULL, 'Lichtenstein, 1823', NULL),
                    (1, {LumpedA}, 'Aus bus', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Aus', 'bus', NULL, NULL, NULL, NULL, 'A, 1900', NULL),
                    (1, {LumpedB}, 'Aus cus', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Aus', 'cus', NULL, NULL, NULL, NULL, 'B, 1900', NULL)
                """, $"""
                    (1, 1, {Lion}, 'Panthera leo', 'Vulnerable', NULL, '2025', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 2, 15952, 'Panthera leo ssp. persica', 'Endangered', NULL, '2023', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 3, {ViviparousLizard}, 'Lacerta vivipara', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 4, {LumpedA}, 'Aus bus', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 5, {LumpedB}, 'Aus cus', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteChecklists(checklists);
        }
        stats = new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, Checklists = checklists, Output = output,
            ExtraSpecies = BeastieBot3.SiteBuild.ExtraSpecies.ExtraPlacement.None,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    private static void WriteChecklists(string path) {
        using var store = ChecklistStore.Open(path);
        store.Replace("mdd", "v2.5", "CC BY 4.0", null, [],
            species: [new ChecklistSpecies("Panthera leo", "1006020"), new ChecklistSpecies("Felis silvestris", "1006042")],
            infraspecific: [
                new ChecklistInfraspecific("Panthera leo", "Panthera leo leo", "subspecies", "(Linnaeus, 1758)"),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo melanochaita", "subspecies", "(Smith, 1842)"),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo sinhaleya", "subspecies", "Deraniyagala, 1938", MddSubspecies.Fossil),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo webbiensis", "subspecies", "Zukowsky, 1964", MddSubspecies.RecentlyExtinct),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo Barbary", "subspecies"),
                new ChecklistInfraspecific("Felis silvestris", "Felis silvestris lybica", "subspecies", "Forster, 1780"),
            ]);
        store.Replace("reptiledb", "ChecklistBank 1008", "CC BY", null, [],
            synonyms: [("Lacerta vivipara", "Zootoca vivipara"), ("Aus bus", "Aus dus"), ("Aus cus", "Aus dus")],
            species: [
                new ChecklistSpecies("Zootoca vivipara", "genus=Zootoca&species=vivipara"),
                new ChecklistSpecies("Aus dus", "genus=Aus&species=dus"),
            ],
            infraspecific: [
                new ChecklistInfraspecific("Zootoca vivipara", "Zootoca vivipara vivipara", "subspecies", "(Lichtenstein, 1823)"),
                new ChecklistInfraspecific("Zootoca vivipara", "Zootoca vivipara louislantzi", "subspecies", "Arribas, 2009"),
                new ChecklistInfraspecific("Aus dus", "Aus dus eus", "subspecies"),
            ]);
    }
}
