using BeastieBot3.Checklists;
using BeastieBot3.SiteBuild;
using static BeastieBot3.Tests.SiteBuild.SiteBuildSourceFixture;

namespace BeastieBot3.Tests.SiteBuild;

// `site build-db`'s classifications from the Mammal Diversity Database, the Reptile Database and
// AmphibiaWeb (ladder_node sources 'mdd', 'reptiledb', 'amphibiaweb', linked by taxon_ladder). IUCN has
// the lion, the viviparous lizard under its old name Lacerta vivipara, two lizards that the Reptile
// Database lumps into one species, and two frogs whose subgenus has the same name in two genera.
public sealed class SiteDbBuildChecklistLadderTests : IDisposable {
    private const long Lion = 15951;
    private const long ViviparousLizard = 61741;
    private const long LumpedA = 1001;
    private const long LumpedB = 1002;
    private const long FrogA = 2001;
    private const long FrogB = 2002;
    private readonly SiteBuildSourceFixture _sources = new();

    public void Dispose() => _sources.Dispose();

    [Fact]
    public void Build_LinksEachSpeciesToItsRecordInTheChecklistOfItsClass() {
        using var db = OpenReadOnly(Build());
        var rows = Rows(db, "SELECT taxon_id, source, node_id FROM taxon_ladder ORDER BY taxon_id").Select(r => string.Join(" | ", r)).ToList();
        // The lizard through the Reptile Database's synonym; neither of the lumped lizards.
        Assert.Equal([
            $"{FrogA} | amphibiaweb | 11",
            $"{FrogB} | amphibiaweb | 12",
            $"{Lion} | mdd | 1006020",
            $"{ViviparousLizard} | reptiledb | genus=Zootoca&species=vivipara",
        ], rows);
    }

    [Fact]
    public void Build_StoresTheGroupsAboveEachSpeciesWithTheClassOfAChecklistOfOneClass() {
        using var db = OpenReadOnly(Build());
        Assert.Equal(["class Mammalia", "order Carnivora", "family Felidae", "subfamily Pantherinae", "genus Panthera", "species Panthera leo"],
            Ladder(db, "mdd", "1006020"));
        // The Reptile Database gives its class, so no class is added.
        Assert.Equal(["class Reptilia", "order Squamata", "family Lacertidae", "genus Zootoca", "species Zootoca vivipara"],
            Ladder(db, "reptiledb", "genus=Zootoca&species=vivipara"));
    }

    [Fact]
    public void Build_KeepsGroupsOfOneRankAndNameUnderTwoParentsApart() {
        using var db = OpenReadOnly(Build());
        Assert.Equal(["class Amphibia", "order Anura", "genus Aa", "subgenus Xx", "species Aa ba"], Ladder(db, "amphibiaweb", "11"));
        Assert.Equal(["class Amphibia", "order Anura", "genus Bb", "subgenus Xx", "species Bb ca"], Ladder(db, "amphibiaweb", "12"));
        Assert.Equal(["subgenus:Xx", "subgenus:Xx#2"],
            Rows(db, "SELECT id FROM ladder_node WHERE source = 'amphibiaweb' AND rank = 'subgenus' ORDER BY id").Select(r => (string)r[0]!));
    }

    // The classification from the species' node up, highest first, as "<rank> <name>".
    private static List<string> Ladder(Microsoft.Data.Sqlite.SqliteConnection db, string source, string id) {
        var steps = new List<string>();
        for (string? at = id; at is not null;) {
            var row = Rows(db, "SELECT parent_id, rank, name FROM ladder_node WHERE source = @s AND id = @i", ("@s", source), ("@i", at)).Single();
            steps.Add($"{row[1]} {row[2]}");
            at = row[0] as string;
        }
        steps.Reverse();
        return steps;
    }

    // ------------------------------------------------------------ sources

    private string Build() {
        var output = _sources.PathOf("site.sqlite");
        var iucn = _sources.PathOf("iucn.sqlite");
        var cache = _sources.PathOf("cache.sqlite");
        var checklists = _sources.PathOf("checklists.sqlite");
        if (!File.Exists(iucn)) {
            WriteIucnCsv(iucn, "2026-1", $"""
                    (1, {Lion}, 'Panthera leo', 'ANIMALIA', 'CHORDATA', 'MAMMALIA', 'CARNIVORA', 'FELIDAE', 'Panthera', 'leo', NULL, NULL, NULL, NULL, '(Linnaeus, 1758)', NULL),
                    (1, {ViviparousLizard}, 'Lacerta vivipara', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Lacerta', 'vivipara', NULL, NULL, NULL, NULL, 'Lichtenstein, 1823', NULL),
                    (1, {LumpedA}, 'Aus bus', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Aus', 'bus', NULL, NULL, NULL, NULL, 'A, 1900', NULL),
                    (1, {LumpedB}, 'Aus cus', 'ANIMALIA', 'CHORDATA', 'REPTILIA', 'SQUAMATA', 'LACERTIDAE', 'Aus', 'cus', NULL, NULL, NULL, NULL, 'B, 1900', NULL),
                    (1, {FrogA}, 'Aa ba', 'ANIMALIA', 'CHORDATA', 'AMPHIBIA', 'ANURA', 'HYLIDAE', 'Aa', 'ba', NULL, NULL, NULL, NULL, 'C, 1900', NULL),
                    (1, {FrogB}, 'Bb ca', 'ANIMALIA', 'CHORDATA', 'AMPHIBIA', 'ANURA', 'HYLIDAE', 'Bb', 'ca', NULL, NULL, NULL, NULL, 'D, 1900', NULL)
                """, $"""
                    (1, 1, {Lion}, 'Panthera leo', 'Vulnerable', NULL, '2025', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 3, {ViviparousLizard}, 'Lacerta vivipara', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 4, {LumpedA}, 'Aus bus', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 5, {LumpedB}, 'Aus cus', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 6, {FrogA}, 'Aa ba', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global'),
                    (1, 7, {FrogB}, 'Bb ca', 'Least Concern', NULL, '2009', NULL, '3.1', NULL, NULL, NULL, 'false', 'false', 'Global')
                """);
            WriteApiCache(cache, [], []);
            WriteChecklists(checklists);
        }
        new SiteDbBuild(new SiteBuildInputs {
            IucnDatabase = iucn, ApiCache = cache, Checklists = checklists, Output = output,
            ExtraSpecies = BeastieBot3.SiteBuild.ExtraSpecies.ExtraPlacement.None,
        }, QuietConsole()).Run(CancellationToken.None);
        return output;
    }

    private static IEnumerable<ChecklistRank> Groups(string species, params (string Rank, string Name)[] groups) =>
        groups.Select((g, i) => new ChecklistRank(species, i, g.Rank, g.Name));

    private static void WriteChecklists(string path) {
        using var store = ChecklistStore.Open(path);
        store.Replace("mdd", "v2.5", "CC BY 4.0", null, [],
            species: [new ChecklistSpecies("Panthera leo", "1006020")],
            ranks: Groups("Panthera leo", ("order", "Carnivora"), ("family", "Felidae"), ("subfamily", "Pantherinae"), ("genus", "Panthera")));
        store.Replace("reptiledb", "ChecklistBank 1008", "CC BY", null, [],
            synonyms: [("Lacerta vivipara", "Zootoca vivipara"), ("Aus bus", "Aus dus"), ("Aus cus", "Aus dus")],
            species: [new ChecklistSpecies("Zootoca vivipara", "genus=Zootoca&species=vivipara"), new ChecklistSpecies("Aus dus", "genus=Aus&species=dus")],
            ranks: [
                .. Groups("Zootoca vivipara", ("class", "Reptilia"), ("order", "Squamata"), ("family", "Lacertidae"), ("genus", "Zootoca")),
                .. Groups("Aus dus", ("class", "Reptilia"), ("order", "Squamata"), ("family", "Lacertidae"), ("genus", "Aus")),
            ]);
        store.Replace("amphibiaweb", "2026-04-01", "CC BY-NC 4.0", null, [],
            species: [new ChecklistSpecies("Aa ba", "11"), new ChecklistSpecies("Bb ca", "12")],
            ranks: [
                .. Groups("Aa ba", ("order", "Anura"), ("genus", "Aa"), ("subgenus", "Xx")),
                .. Groups("Bb ca", ("order", "Anura"), ("genus", "Bb"), ("subgenus", "Xx")),
            ]);
    }
}
