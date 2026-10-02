using BeastieBot3.Col;
using BeastieBot3.Taxonomy;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The CoL placement data layer: the matcher over a small synthetic CoL nameusage table, the
// placement file's round trip, and a full build from temp files (cold, then served from the stored
// placement, then rebuilt from the warm caches without opening CoL).
public class TaxonPlacementStoreTests {
    // ---- a tiny CoL ----

    private static readonly (string Id, string? Parent, string Status, string Name, string Rank, string? Genus, string? Epithet, string? Kingdom)[] ColRows = {
        ("K", null, "accepted", "Animalia", "kingdom", null, null, "Animalia"),
        ("P", "K", "accepted", "Chordata", "phylum", null, null, "Animalia"),
        ("C", "P", "accepted", "Reptilia", "class", null, null, "Animalia"),
        ("O", "C", "accepted", "Squamata", "order", null, null, "Animalia"),
        ("SERP", "O", "accepted", "Serpentes", "suborder", null, null, "Animalia"),
        ("COLU", "SERP", "accepted", "Colubridae", "family", null, null, "Animalia"),
        ("NATX", "COLU", "accepted", "Natrix", "genus", null, null, "Animalia"),
        ("N1", "NATX", "accepted", "Natrix natrix", "species", "Natrix", "natrix", "Animalia"),
        ("N2", "NATX", "provisionally accepted", "Natrix tessellata", "species", "Natrix", "tessellata", "Animalia"),
        // A synonym row carries no classification; its parent is the accepted name.
        ("S1", "N1", "synonym", "Tropidonotus natrix", "species", "Tropidonotus", "natrix", null),
        // A misapplied name is never a match.
        ("M1", "N1", "misapplied", "Natrix maura", "species", "Natrix", "maura", null),
        // An accepted species whose genus row is missing from the database (dropped on import).
        ("LOST", "GONE", "accepted", "Lostus lostus", "species", "Lostus", "lostus", "Animalia"),
        // A plant homonym of an animal name.
        ("PK", null, "accepted", "Plantae", "kingdom", null, null, "Plantae"),
        ("PG", "PK", "accepted", "Natrix", "genus", null, null, "Plantae"),
        ("PN", "PG", "accepted", "Natrix plantae", "species", "Natrix", "plantae", "Plantae"),
    };

    private static void CreateCol(SqliteConnection connection) {
        using var create = connection.CreateCommand();
        create.CommandText = """
            CREATE TABLE nameusage (ID TEXT, parentID TEXT, status TEXT, scientificName TEXT, rank TEXT,
                genericName TEXT, specificEpithet TEXT, infraspecificEpithet TEXT, kingdom TEXT);
            CREATE INDEX idx_nameusage_ID ON nameusage(ID);
            """;
        create.ExecuteNonQuery();
        foreach (var r in ColRows) {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO nameusage VALUES (@id, @parent, @status, @name, @rank, @genus, @epithet, NULL, @kingdom)";
            insert.Parameters.AddWithValue("@id", r.Id);
            insert.Parameters.AddWithValue("@parent", (object?)r.Parent ?? DBNull.Value);
            insert.Parameters.AddWithValue("@status", r.Status);
            insert.Parameters.AddWithValue("@name", r.Name);
            insert.Parameters.AddWithValue("@rank", r.Rank);
            insert.Parameters.AddWithValue("@genus", (object?)r.Genus ?? DBNull.Value);
            insert.Parameters.AddWithValue("@epithet", (object?)r.Epithet ?? DBNull.Value);
            insert.Parameters.AddWithValue("@kingdom", (object?)r.Kingdom ?? DBNull.Value);
            insert.ExecuteNonQuery();
        }
    }

    private static ColLineageMatcher InMemoryMatcher() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        CreateCol(connection);
        return new ColLineageMatcher(() => connection);
    }

    private static IucnSpeciesKey Snake(string genus, string species) =>
        new("ANIMALIA", "REPTILIA", "SQUAMATA", "COLUBRIDAE", genus, species);

    // ---- matcher ----

    [Fact]
    public void Matcher_FindsAcceptedName_AndItsLineage() {
        using var matcher = InMemoryMatcher();
        var match = matcher.Match(Snake("Natrix", "natrix"));
        Assert.Equal(ColMatchKind.Accepted, match.Kind);
        Assert.Equal("N1", match.AcceptedId);

        var lineage = matcher.LineageOf("N1");
        Assert.False(lineage.CutShort);
        Assert.Equal(new[] { "Animalia", "Chordata", "Reptilia", "Squamata", "Serpentes", "Colubridae", "Natrix" },
            lineage.Nodes.Select(n => n.Name).ToArray());
    }

    [Fact]
    public void Matcher_AcceptsProvisionallyAccepted() {
        using var matcher = InMemoryMatcher();
        var match = matcher.Match(Snake("Natrix", "tessellata"));
        Assert.Equal(ColMatchKind.ProvisionallyAccepted, match.Kind);
        Assert.Equal("N2", match.AcceptedId);
    }

    [Fact]
    public void Matcher_FollowsSynonymToAcceptedName() {
        using var matcher = InMemoryMatcher();
        var match = matcher.Match(Snake("Tropidonotus", "natrix"));
        Assert.Equal(ColMatchKind.Synonym, match.Kind);
        Assert.Equal("N1", match.AcceptedId);
    }

    [Fact]
    public void Matcher_IgnoresMisappliedNames() {
        using var matcher = InMemoryMatcher();
        Assert.Equal(ColMatchKind.NotFound, matcher.Match(Snake("Natrix", "maura")).Kind);
    }

    [Fact]
    public void Matcher_ChecksKingdomOnTheLineage() {
        using var matcher = InMemoryMatcher();
        Assert.Equal(ColMatchKind.NotFound, matcher.Match(Snake("Natrix", "plantae")).Kind);
        var plant = matcher.Match(new IucnSpeciesKey("PLANTAE", "MAGNOLIOPSIDA", null, null, "Natrix", "plantae"));
        Assert.Equal("PN", plant.AcceptedId);
    }

    [Fact]
    public void Matcher_EndsChainAtMissingParent() {
        using var matcher = InMemoryMatcher();
        var match = matcher.Match(new IucnSpeciesKey("ANIMALIA", null, null, null, "Lostus", "lostus"));
        Assert.Equal("LOST", match.AcceptedId);
        var lineage = matcher.LineageOf("LOST");
        Assert.True(lineage.CutShort);
        Assert.Empty(lineage.Nodes);
        Assert.Contains(matcher.NewNodes, kv => kv.Key == "GONE" && kv.Value is null);
    }

    [Fact]
    public void Matcher_ServedFromKnownNodes_NeverOpensCol() {
        List<KeyValuePair<string, ColNodeRow?>> known;
        using (var warm = InMemoryMatcher()) {
            warm.LineageOf(warm.Match(Snake("Natrix", "natrix")).AcceptedId!);
            known = warm.NewNodes.ToList();
        }
        using var cold = new ColLineageMatcher(() => throw new InvalidOperationException("CoL opened"), known);
        Assert.Equal(7, cold.LineageOf("N1").Nodes.Count);
        Assert.False(cold.UsedColDatabase);
    }

    // ---- placement file ----

    private static PlacementBuildOutput SmallOutput() {
        IReadOnlyList<ColLineageNode> Lineage(params string[] steps) =>
            steps.Select(s => s.Split(':')).Select(p => new ColLineageNode(p[1], p[1], p[0])).ToList();
        var natrix = Lineage("kingdom:Animalia", "class:Reptilia", "order:Squamata", "suborder:Serpentes", "family:Colubridae", "subfamily:Natricinae", "genus:Natrix");
        var iguana = Lineage("kingdom:Animalia", "class:Reptilia", "order:Squamata", "suborder:Iguania", "family:Iguanidae", "genus:Iguana");
        return TaxonPlacementBuilder.Build(new[] {
            new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE", "Natrix", natrix),
            new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE", "Natrix", natrix),
            new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", "IGUANIDAE", "Iguana", iguana),
            new PlacementSample("ANIMALIA", "REPTILIA", "SQUAMATA", "IGUANIDAE", "Iguana", null),
        });
    }

    private static PlacementSourceRow Source(string key, string path) =>
        new(key, path, "1:1", "/col.sqlite", "9:9", TaxonPlacementStore.AlgorithmVersion, 0.8, 0.9,
            new DateTime(2026, 10, 2, 0, 0, 0, DateTimeKind.Utc), 4, 3, 3, 1.5);

    [Fact]
    public void Placement_RoundTripsThroughTheStore() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = TaxonPlacementStore.OpenFromConnection(connection);
        var output = SmallOutput();

        store.SavePlacement(Source("iucn-a|1:1", "/iucn-a.sqlite"), output);
        var loaded = store.LoadPlacement("iucn-a|1:1");

        Assert.Equal(output.Index.Count, loaded.Count);
        foreach (var path in output.Index.Paths) {
            var again = loaded.Paths.Single(p => p.Span == path.Span && p.Key == path.Key);
            Assert.Equal(path.Nodes, again.Nodes);
        }
        Assert.Equal(new[] { "Serpentes" }, loaded.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE").Select(n => n.Name));
        Assert.Equal(new[] { "Natricinae" }, loaded.BetweenFamilyAndGenus("ANIMALIA", "NATRICIDAE", "Natrix").Select(n => n.Name));

        var source = store.ReadSource("iucn-a|1:1");
        Assert.NotNull(source);
        Assert.Equal(new DateTime(2026, 10, 2, 0, 0, 0, DateTimeKind.Utc), source!.BuiltAtUtc);
        Assert.Equal(3, source.Matched);
    }

    [Fact]
    public void Placement_RowsCarryTheIucnColumnsForJoins() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = TaxonPlacementStore.OpenFromConnection(connection);
        store.SavePlacement(Source("k", "/iucn.sqlite"), SmallOutput());

        using var query = connection.CreateCommand();
        query.CommandText = """
            SELECT kingdom, class_name, order_name, family_name, genus_name, name, show_rank FROM placement
            WHERE source_key = 'k' AND span = 'FamilyToGenus'
            """;
        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("ANIMALIA", reader.GetString(0));
        Assert.True(reader.IsDBNull(1));
        Assert.True(reader.IsDBNull(2));
        Assert.Equal("NATRICIDAE", reader.GetString(3));
        Assert.Equal("Natrix", reader.GetString(4));
        Assert.Equal("Natricinae", reader.GetString(5));
        Assert.Equal(1, reader.GetInt32(6));
        Assert.False(reader.Read());
    }

    [Fact]
    public void Placement_ForANewerVersionOfTheSameFile_ReplacesTheOldOne() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = TaxonPlacementStore.OpenFromConnection(connection);
        store.SavePlacement(Source("old", "/iucn.sqlite"), SmallOutput());
        store.SavePlacement(Source("other", "/iucn-api.sqlite"), SmallOutput());
        store.SavePlacement(Source("new", "/iucn.sqlite"), SmallOutput());

        Assert.Null(store.ReadSource("old"));
        Assert.Equal(0, store.LoadPlacement("old").Count);
        Assert.NotNull(store.ReadSource("other"));
        Assert.NotNull(store.ReadSource("new"));
    }

    [Fact]
    public void Caches_RoundTrip_AndEmptyWhenColChanges() {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        var store = TaxonPlacementStore.OpenFromConnection(connection);
        Assert.True(store.ResetCachesUnlessFor("col-1"));

        var snake = Snake("Natrix", "natrix");
        store.SaveSpeciesMatches(new[] { (snake, new ColSpeciesMatch(ColMatchKind.Synonym, "N1", 2)) });
        store.SaveNodes(new[] {
            new KeyValuePair<string, ColNodeRow?>("N1", new ColNodeRow("N1", "NATX", "Natrix natrix", "species", "accepted", "Animalia")),
            new KeyValuePair<string, ColNodeRow?>("GONE", null),
        });

        var matches = store.LoadSpeciesMatches();
        var hit = matches[TaxonPlacementStore.MatchKey("animalia", "Natrix", "natrix")];
        Assert.Equal(ColMatchKind.Synonym, hit.Match.Kind);
        Assert.Equal("N1", hit.Match.AcceptedId);
        Assert.Equal(2, hit.Match.Candidates);
        Assert.Equal("COLUBRIDAE", hit.FamilyName);
        var nodes = store.LoadNodes().ToDictionary(kv => kv.Key, kv => kv.Value);
        Assert.Equal("NATX", nodes["N1"]!.ParentId);
        Assert.Null(nodes["GONE"]);

        Assert.False(store.ResetCachesUnlessFor("col-1"));
        Assert.Single(store.LoadSpeciesMatches());
        Assert.True(store.ResetCachesUnlessFor("col-2"));
        Assert.Empty(store.LoadSpeciesMatches());
        Assert.Empty(store.LoadNodes());
    }

    // ---- full build from files ----

    private static void CreateIucn(string path) {
        using var connection = new SqliteConnection($"Data Source={path}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE view_assessments_html_taxonomy_html (kingdomName TEXT, className TEXT, orderName TEXT,
                familyName TEXT, genusName TEXT, speciesName TEXT, infraType TEXT, subpopulationName TEXT, scopes TEXT);
            INSERT INTO view_assessments_html_taxonomy_html VALUES
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Natrix', 'natrix', NULL, NULL, 'Global'),
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Natrix', 'tessellata', NULL, NULL, 'Global'),
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Tropidonotus', 'natrix', NULL, NULL, 'Global'),
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Natrix', 'unknownus', NULL, NULL, 'Global'),
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Natrix', 'natrix', 'ssp.', NULL, 'Global'),
                ('ANIMALIA', 'REPTILIA', 'SQUAMATA', 'NATRICIDAE', 'Natrix', 'natrix', NULL, 'Europe', 'Global');
            """;
        command.ExecuteNonQuery();
    }

    [Fact]
    public void Run_BuildsOnce_ThenLoads_ThenRebuildsFromTheCaches() {
        var dir = Path.Combine(Path.GetTempPath(), "bb3-placement-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try {
            var iucn = Path.Combine(dir, "iucn.sqlite");
            var col = Path.Combine(dir, "col.sqlite");
            CreateIucn(iucn);
            using (var colConnection = new SqliteConnection($"Data Source={col}")) {
                colConnection.Open();
                CreateCol(colConnection);
            }
            SqliteConnection.ClearAllPools();

            Assert.Equal(PlacementState.NoFile, TaxonPlacementStore.Status(iucn, col).State);

            var first = TaxonPlacementBuild.Run(iucn, col, new PlacementRunOptions(), null, CancellationToken.None);
            Assert.True(first.Built);
            Assert.Equal(4, first.Matching!.Species);
            Assert.Equal(3, first.Matching.Matched);
            Assert.Equal(1, first.Matching.ByKind[ColMatchKind.Synonym]);
            Assert.Equal(1, first.Matching.ByKind[ColMatchKind.NotFound]);
            Assert.Equal(new[] { "Serpentes" }, first.Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE").Select(n => n.Name));
            Assert.True(File.Exists(TaxonPlacementStore.SidecarPath(col)));

            Assert.Equal(PlacementState.Current, TaxonPlacementStore.Status(iucn, col).State);
            var second = TaxonPlacementBuild.Run(iucn, col, new PlacementRunOptions(), null, CancellationToken.None);
            Assert.False(second.Built);
            Assert.Equal(first.Index.Count, second.Index.Count);

            var forced = TaxonPlacementBuild.Run(iucn, col, new PlacementRunOptions { Force = true }, null, CancellationToken.None);
            Assert.True(forced.Built);
            Assert.Equal(4, forced.Matching!.FromCache);
            Assert.Equal(0, forced.Matching.ColQueries);
            Assert.Equal(new[] { "Serpentes" }, forced.Index.BetweenOrderAndFamily("ANIMALIA", "REPTILIA", "SQUAMATA", "NATRICIDAE").Select(n => n.Name));
        } finally {
            SqliteConnection.ClearAllPools();
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
