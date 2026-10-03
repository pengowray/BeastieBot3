using System;
using System.IO;
using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `common-names report` and `common-names sources` open the store with OpenReadOnly: no schema
// work, no writes. These pin that a read-only store still answers what those commands ask, also
// for a store written before source_replacements existed.
public class CommonNameReadOnlyStoreTests : IDisposable {
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"cn-readonly-{Guid.NewGuid():N}.sqlite");

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" }) {
            try { File.Delete(_path + suffix); } catch (IOException) { }
        }
    }

    private void WriteStore(bool dropReplacementsTable) {
        using (var store = CommonNameStore.Open(_path)) {
            var taxon = store.InsertOrUpdateTaxon("panthera leo", "Panthera leo", "species", "ANIMALIA",
                isExtinct: false, isFossil: false, validityStatus: "valid", primarySource: "iucn", primarySourceId: "15951");
            store.InsertCommonName(taxon, "Lion", "lion", "en", "iucn", "15951", isPreferred: true);
        }
        if (dropReplacementsTable) {
            using var conn = new SqliteConnection($"Data Source={_path}");
            conn.Open();
            using var drop = conn.CreateCommand();
            drop.CommandText = "DROP TABLE source_replacements";
            drop.ExecuteNonQuery();
        }
        SqliteConnection.ClearAllPools();
    }

    [Fact]
    public void ReadOnlyStore_AnswersTheSourcesCommandsQueries() {
        WriteStore(dropReplacementsTable: false);
        using var store = CommonNameStore.OpenReadOnly(_path);

        Assert.Contains(("iucn", 1), store.GetCommonNameCountsBySource());
        Assert.Empty(store.GetSourceReplacements());
        Assert.Equal((1, 0, 1), store.GetStatistics());
    }

    [Fact]
    public void ReadOnlyStore_WithoutSourceReplacementsTable_HasNoReplacements() {
        WriteStore(dropReplacementsTable: true);
        using var store = CommonNameStore.OpenReadOnly(_path);

        Assert.Empty(store.GetSourceReplacements());
    }

    [Fact]
    public void ReadOnlyStore_RefusesWrites() {
        WriteStore(dropReplacementsTable: false);
        using var store = CommonNameStore.OpenReadOnly(_path);

        Assert.Throws<SqliteException>(() => store.InsertCapsRule("lion", "Lion"));
    }
}
