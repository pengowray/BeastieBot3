using System;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins C3: WriteTaxonAtomic persists a taxon's taxa row, its taxa_lookup mappings, and its
// assessment backlog in a single transaction (previously three separate commits that could
// leave the taxa row written while lookups/backlog stayed stale on a crash).
public class IucnApiCacheStoreWriteTests {
    private static long Scalar(SqliteConnection conn, string sql) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar() ?? 0L);
    }

    [Fact]
    public void WriteTaxonAtomic_PersistsTaxaLookupsAndBacklogTogether() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);

        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        var taxaId = store.WriteTaxonAtomic(
            rootSisId: 100,
            importId: importId,
            json: "{\"taxon\":1}",
            downloadedAt: DateTime.UtcNow,
            mappings: new[] {
                new TaxaLookupRow(100, 100, "species"),
                new TaxaLookupRow(101, 100, "infrarank"),
            },
            assessments: new[] {
                new IucnAssessmentHeader(900, 100, Latest: true, YearPublished: 2020),
            });

        Assert.True(taxaId > 0);
        Assert.Equal(1L, Scalar(conn, $"SELECT COUNT(*) FROM taxa WHERE id={taxaId}"));
        Assert.Equal(2L, Scalar(conn, $"SELECT COUNT(*) FROM taxa_lookup WHERE taxa_id={taxaId}"));
        Assert.Equal(1L, Scalar(conn, $"SELECT COUNT(*) FROM taxa_assessment_backlog WHERE taxa_id={taxaId}"));
        // The denormalized "has a latest assessment" flag is set within the same write.
        Assert.Equal(1L, Scalar(conn, $"SELECT has_latest_flag_in_assessments FROM taxa WHERE id={taxaId}"));
    }
}
