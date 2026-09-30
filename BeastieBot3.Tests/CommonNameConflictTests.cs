using System.Collections.Generic;
using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins `common-names detect-conflicts` re-runs: the store keeps one row per conflicting taxon pair
// and name, so a second run without --clear-existing adds nothing it already recorded. Before this,
// InsertConflict was a plain INSERT and every re-run stored a second copy of every conflict.
public class CommonNameConflictTests {
    private static SqliteConnection OpenConnection() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return conn;
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string sourceId, string kingdom = "ANIMALIA") =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", kingdom,
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "iucn", primarySourceId: sourceId);

    private static long AddName(CommonNameStore store, long taxonId, string name, string source = "iucn") =>
        store.InsertCommonName(taxonId, name, name.ToLowerInvariant(), "en", source, "id-" + taxonId, false);

    private static long Scalar(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    private static void Execute(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void InsertConflict_SamePairTwice_StoresOneRow() {
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "panthera onca", "2");

        Assert.True(store.InsertConflict("lion", "ambiguous", a, null, b, null));
        Assert.False(store.InsertConflict("lion", "ambiguous", a, null, b, null));

        Assert.Equal(1, store.GetStatistics().ConflictCount);
    }

    [Fact]
    public void InsertConflict_ReversedPair_IsTheSameConflict() {
        // Detection meets the two taxa in the order the names query returns them, which changes
        // when a source adds a preferred name, so (b, a) must match a stored (a, b).
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "panthera onca", "2");
        var nameA = AddName(store, a, "Lion");
        var nameB = AddName(store, b, "Lion");

        Assert.True(store.InsertConflict("lion", "ambiguous", b, nameB, a, nameA));
        Assert.False(store.InsertConflict("lion", "ambiguous", a, nameA, b, nameB));

        Assert.Equal(1, store.GetStatistics().ConflictCount);
        // Stored smaller taxon id first, with each common name id still beside its own taxon.
        Assert.Equal(a, Scalar(conn, "SELECT taxon_id_a FROM common_name_conflicts"));
        Assert.Equal(nameA, Scalar(conn, "SELECT common_name_id_a FROM common_name_conflicts"));
        Assert.Equal(b, Scalar(conn, "SELECT taxon_id_b FROM common_name_conflicts"));
        Assert.Equal(nameB, Scalar(conn, "SELECT common_name_id_b FROM common_name_conflicts"));
    }

    [Fact]
    public void InsertConflict_SamePairUnderAnotherName_IsASeparateConflict() {
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "panthera onca", "2");

        Assert.True(store.InsertConflict("lion", "ambiguous", a, null, b, null));
        Assert.True(store.InsertConflict("big cat", "ambiguous", a, null, b, null));

        Assert.Equal(2, store.GetStatistics().ConflictCount);
    }

    [Fact]
    public void OpeningAnOlderStore_RemovesDuplicateConflictsAndKeepsTheOldestRow() {
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var a = AddTaxon(store, "panthera leo", "1");
        var b = AddTaxon(store, "panthera onca", "2");
        var c = AddTaxon(store, "panthera tigris", "3");

        // A store written before the unique index existed: three copies of one conflict (one of
        // them with the taxa the other way round) and one conflict stored once.
        Execute(conn, "DROP INDEX ux_conflicts_pair;");
        Execute(conn,
            $"""
            INSERT INTO common_name_conflicts (id, normalized_name, conflict_type, taxon_id_a, taxon_id_b, detected_at) VALUES
                (10, 'lion', 'ambiguous', {a}, {b}, '2026-01-01T00:00:00Z'),
                (11, 'lion', 'ambiguous', {a}, {b}, '2026-02-01T00:00:00Z'),
                (12, 'lion', 'ambiguous', {b}, {a}, '2026-03-01T00:00:00Z'),
                (13, 'tiger', 'ambiguous', {c}, {a}, '2026-01-01T00:00:00Z');
            """);
        Assert.Equal(4, Scalar(conn, "SELECT COUNT(*) FROM common_name_conflicts"));

        CommonNameStore.OpenFromConnection(conn);

        Assert.Equal(1, Scalar(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'ux_conflicts_pair'"));
        Assert.Equal(2, Scalar(conn, "SELECT COUNT(*) FROM common_name_conflicts"));
        Assert.Equal(10, Scalar(conn, "SELECT id FROM common_name_conflicts WHERE normalized_name = 'lion'"));
        // The reversed row that survives is put in the same order InsertConflict uses.
        Assert.Equal(a, Scalar(conn, "SELECT taxon_id_a FROM common_name_conflicts WHERE normalized_name = 'tiger'"));
        Assert.Equal(c, Scalar(conn, "SELECT taxon_id_b FROM common_name_conflicts WHERE normalized_name = 'tiger'"));
    }

    [Fact]
    public void ScanForConflicts_SecondRun_AddsNothing() {
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var leo = AddTaxon(store, "panthera leo", "1");
        var onca = AddTaxon(store, "panthera onca", "2");
        var plant = AddTaxon(store, "leontodon hispidus", "3", kingdom: "PLANTAE");
        AddName(store, leo, "Lion");
        AddName(store, onca, "Lion", source: "wikidata");
        AddName(store, plant, "Lion");   // other kingdom: not a conflict

        var names = new List<string> { "lion" };
        var first = CommonNameDetectConflictsCommand.ScanForConflicts(store, names, "en", includeFossil: false);
        var second = CommonNameDetectConflictsCommand.ScanForConflicts(store, names, "en", includeFossil: false);

        Assert.Equal(new CommonNameDetectConflictsCommand.ConflictScanResult(1, 1, 1), first);
        Assert.Equal(new CommonNameDetectConflictsCommand.ConflictScanResult(1, 1, 0), second);
        Assert.Equal(1, store.GetStatistics().ConflictCount);
    }

    [Fact]
    public void ClearConflictsThenScan_DropsNamesThatAreNoLongerAmbiguous() {
        // --clear-existing: a conflict stored by an earlier run for a name that is no longer
        // ambiguous is removed, and only the current conflicts are stored again.
        using var conn = OpenConnection();
        var store = CommonNameStore.OpenFromConnection(conn);
        var leo = AddTaxon(store, "panthera leo", "1");
        var onca = AddTaxon(store, "panthera onca", "2");
        var tigris = AddTaxon(store, "panthera tigris", "3");
        AddName(store, leo, "Lion");
        AddName(store, onca, "Lion");
        store.InsertConflict("tiger", "ambiguous", tigris, null, leo, null);

        store.ClearConflicts();
        var scan = CommonNameDetectConflictsCommand.ScanForConflicts(store, new[] { "lion", "tiger" }, "en", includeFossil: false);

        Assert.Equal(1, scan.NewConflicts);
        Assert.Equal(1, store.GetStatistics().ConflictCount);
        Assert.Equal(0, Scalar(conn, "SELECT COUNT(*) FROM common_name_conflicts WHERE normalized_name = 'tiger'"));
    }
}
