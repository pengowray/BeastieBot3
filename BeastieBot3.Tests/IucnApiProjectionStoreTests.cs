using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the coverage-honesty contract of the API→CSV projection store: CompleteImport must record
// whether the projection was partial (some taxa's latest assessment JSON wasn't downloaded), the
// INSERT OR IGNORE writer must dedupe a taxon to one taxonomy row, and EnsureSchema must migrate a
// pre-coverage import_metadata table in place.
public class IucnApiProjectionStoreTests {
    private static SqliteConnection OpenMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return conn;
    }

    private static ProjectedAssessment Assessment(long assessmentId, long taxonId, string category) =>
        new(
            AssessmentId: assessmentId,
            TaxonId: taxonId,
            Latest: true,
            YearPublished: "2024",
            RedlistCategoryCode: category,
            RedlistCategoryEn: null,
            PossiblyExtinct: "false",
            PossiblyExtinctInTheWild: "false",
            Scopes: "Global",
            Systems: "Terrestrial",
            ScientificName: "Genus species",
            Authority: null,
            KingdomName: "ANIMALIA",
            PhylumName: "CHORDATA",
            ClassName: "MAMMALIA",
            OrderName: "CARNIVORA",
            FamilyName: "FELIDAE",
            GenusName: "Genus",
            SpeciesName: "species",
            SubpopulationName: null,
            InfraType: null,
            InfraName: null,
            InfraAuthority: null);

    [Fact]
    public void CompleteImport_RecordsPartialCoverage() {
        using var conn = OpenMemory();
        var store = IucnApiProjectionStore.OpenFromConnection(conn);
        store.ResetData();
        var importId = store.InsertImport("cache.sqlite", "api-cache");

        using (var writer = store.BeginWrite()) {
            writer.AddTaxonomy(importId, 100, "Genus species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Genus", "species");
            writer.AddAssessment(importId, Assessment(1, 100, "VU"), "Vulnerable");
            writer.Commit();
        }
        store.BuildView();
        store.CompleteImport(importId, projectedTaxa: 1, projectedAssessments: 1, latestNotDownloaded: 7, isPartial: true);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT is_partial, latest_not_downloaded, projected_taxa, projected_assessments, ended_at FROM import_metadata WHERE id=@id";
        cmd.Parameters.AddWithValue("@id", importId);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(7L, reader.GetInt64(1));
        Assert.Equal(1L, reader.GetInt64(2));
        Assert.Equal(1L, reader.GetInt64(3));
        Assert.False(reader.IsDBNull(4)); // ended_at stamped
    }

    [Fact]
    public void CompleteImport_RecordsCompleteCoverage_WhenNothingMissing() {
        using var conn = OpenMemory();
        var store = IucnApiProjectionStore.OpenFromConnection(conn);
        var importId = store.InsertImport("cache.sqlite", "api-cache");
        store.CompleteImport(importId, projectedTaxa: 5, projectedAssessments: 6, latestNotDownloaded: 0, isPartial: false);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT is_partial, latest_not_downloaded FROM import_metadata WHERE id=@id";
        cmd.Parameters.AddWithValue("@id", importId);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(0L, reader.GetInt64(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }

    [Fact]
    public void Writer_DedupesTaxonomyByTaxonId() {
        using var conn = OpenMemory();
        var store = IucnApiProjectionStore.OpenFromConnection(conn);
        var importId = store.InsertImport("cache.sqlite", "api-cache");

        using (var writer = store.BeginWrite()) {
            // A taxon with a global + regional latest assessment writes the same taxon block twice;
            // INSERT OR IGNORE must collapse it to one taxonomy row but keep both assessments.
            writer.AddTaxonomy(importId, 100, "Genus species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Genus", "species");
            writer.AddAssessment(importId, Assessment(1, 100, "VU"), "Vulnerable");
            writer.AddTaxonomy(importId, 100, "Genus species", "ANIMALIA", "CHORDATA", "MAMMALIA", "CARNIVORA", "FELIDAE", "Genus", "species");
            writer.AddAssessment(importId, Assessment(2, 100, "EN"), "Endangered");
            writer.Commit();
        }

        Assert.Equal(1L, store.CountRows("taxonomy_html"));
        Assert.Equal(2L, store.CountRows("assessments_html"));
    }

    [Fact]
    public void EnsureSchema_MigratesPreCoverageMetadataTableInPlace() {
        using var conn = OpenMemory();
        // Simulate an old projection DB: import_metadata without the coverage columns.
        using (var seed = conn.CreateCommand()) {
            seed.CommandText = @"CREATE TABLE import_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                redlist_version TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT);
                INSERT INTO import_metadata(filename, redlist_version, started_at) VALUES ('old.sqlite','api-cache','2024-01-01T00:00:00Z');";
            seed.ExecuteNonQuery();
        }

        // Opening the store runs EnsureSchema, which must ALTER the existing table to add the columns.
        var store = IucnApiProjectionStore.OpenFromConnection(conn);
        var importId = store.InsertImport("new.sqlite", "api-cache");
        store.CompleteImport(importId, projectedTaxa: 2, projectedAssessments: 3, latestNotDownloaded: 0, isPartial: false);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM pragma_table_info('import_metadata') WHERE name IN ('projected_taxa','projected_assessments','latest_not_downloaded','is_partial')";
        Assert.Equal(4L, (long)cmd.ExecuteScalar()!);
    }
}
