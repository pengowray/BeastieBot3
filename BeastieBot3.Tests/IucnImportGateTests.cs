using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins the one-release-per-DB import gate (audit C1): importing a different IUCN release into a DB
// that already holds one used to silently accumulate rows and double-count every downstream
// COUNT(*)/DISTINCT redlist_version consumer. The gate refuses a cross-release import, accumulates a
// same-release multi-zip import, and rebuilds cleanly under --force.
public class IucnImportGateTests {
    // ---- pure decision ----

    [Fact]
    public void FindReleaseConflict_EmptyDb_NoConflict() =>
        Assert.Null(IucnImporter.FindReleaseConflict(Array.Empty<string>(), "2025-2"));

    [Fact]
    public void FindReleaseConflict_SameVersion_NoConflict() =>
        Assert.Null(IucnImporter.FindReleaseConflict(new[] { "2025-2" }, "2025-2"));

    [Fact]
    public void FindReleaseConflict_DifferentVersion_ReturnsExisting() =>
        Assert.Equal("2025-2", IucnImporter.FindReleaseConflict(new[] { "2025-2" }, "2026-1"));

    [Fact]
    public void FindReleaseConflict_KnownVsUnknown_Conflicts() {
        Assert.Equal("2025-2", IucnImporter.FindReleaseConflict(new[] { "2025-2" }, "unknown"));
        Assert.Equal("unknown", IucnImporter.FindReleaseConflict(new[] { "unknown" }, "2025-2"));
    }

    [Fact]
    public void FindReleaseConflict_UnknownVsUnknown_NoConflict() =>
        Assert.Null(IucnImporter.FindReleaseConflict(new[] { "unknown" }, "unknown"));

    // ---- end-to-end over :memory: ----

    [Fact]
    public void Import_SecondReleaseWithoutForce_IsRefused() {
        using var ctx = new GateFixture();
        ctx.Import(ctx.MakeZip("export-a.zip", taxonId: 1, assessmentId: 100), "2025-2", force: false);
        Assert.Equal(1, ctx.TaxonomyCount());

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.Import(ctx.MakeZip("export-b.zip", taxonId: 2, assessmentId: 200), "2026-1", force: false));
        Assert.Contains("2026-1", ex.Message);
        Assert.Contains("2025-2", ex.Message);

        // The refused release left nothing behind.
        Assert.Equal(1, ctx.TaxonomyCount());
        Assert.Equal(1, ctx.ScalarLong("SELECT COUNT(DISTINCT redlist_version) FROM import_metadata WHERE ended_at IS NOT NULL;"));
    }

    [Fact]
    public void Import_NewReleaseUnderReusedFilename_IsRefusedNotSkipped() {
        // Regression: when both releases ship as the same zip filename, the per-filename "already
        // imported" skip must NOT pre-empt the cross-release refusal (else 2026-1 silently no-ops and
        // the user believes they upgraded while the DB still holds 2025-2).
        using var ctx = new GateFixture();
        ctx.Import(ctx.MakeZip("redlist_export.zip", taxonId: 1, assessmentId: 100), "2025-2", force: false);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.Import(ctx.MakeZip("redlist_export.zip", taxonId: 2, assessmentId: 200), "2026-1", force: false));
        Assert.Contains("Refusing", ex.Message);
        Assert.Equal(1, ctx.ScalarLong("SELECT COUNT(*) FROM taxonomy WHERE taxonId = 1;"));
        Assert.Equal(0, ctx.ScalarLong("SELECT COUNT(*) FROM taxonomy WHERE taxonId = 2;"));
    }

    [Fact]
    public void Import_SameZipTwiceSameRelease_IsSkippedNotRefused() {
        // The idempotency skip still works within a release: re-running the same completed zip is a
        // no-op, not a refusal.
        using var ctx = new GateFixture();
        var zip = ctx.MakeZip("redlist_export.zip", taxonId: 1, assessmentId: 100);
        ctx.Import(zip, "2025-2", force: false);
        ctx.Import(zip, "2025-2", force: false); // must not throw
        Assert.Equal(1, ctx.TaxonomyCount());
    }

    [Fact]
    public void Import_SameReleaseSecondZip_Accumulates() {
        using var ctx = new GateFixture();
        ctx.Import(ctx.MakeZip("export-a.zip", taxonId: 1, assessmentId: 100), "2025-2", force: false);
        ctx.Import(ctx.MakeZip("export-b.zip", taxonId: 3, assessmentId: 300), "2025-2", force: false);

        Assert.Equal(2, ctx.TaxonomyCount());
        Assert.Equal(1, ctx.ScalarLong("SELECT COUNT(DISTINCT redlist_version) FROM import_metadata WHERE ended_at IS NOT NULL;"));
    }

    [Fact]
    public void Import_DifferentReleaseWithForce_WipesAndRebuilds() {
        using var ctx = new GateFixture();
        ctx.Import(ctx.MakeZip("export-a.zip", taxonId: 1, assessmentId: 100), "2025-2", force: false);
        ctx.Import(ctx.MakeZip("export-b.zip", taxonId: 2, assessmentId: 200), "2026-1", force: true);

        // Only the new release survives — no accumulation, single version.
        Assert.Equal(1, ctx.TaxonomyCount());
        Assert.Equal(0, ctx.ScalarLong("SELECT COUNT(*) FROM taxonomy WHERE taxonId = 1;"));
        Assert.Equal(1, ctx.ScalarLong("SELECT COUNT(*) FROM taxonomy WHERE taxonId = 2;"));
        Assert.Equal("2026-1", ctx.ScalarString("SELECT DISTINCT redlist_version FROM import_metadata;"));
    }

    private sealed class GateFixture : IDisposable {
        private readonly string _dir;
        private readonly SqliteConnection _conn;

        public GateFixture() {
            _dir = Path.Combine(Path.GetTempPath(), "bb3-gate-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_dir);
            _conn = new SqliteConnection("Data Source=:memory:");
            _conn.Open();
            using var pragma = _conn.CreateCommand();
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        // A minimal IUCN export zip: taxonomy.csv + assessments.csv with one row each. The filename
        // carries no YYYY-N, so the importer falls back to the version hint we pass to Import().
        public string MakeZip(string name, int taxonId, int assessmentId) {
            var path = Path.Combine(_dir, name);
            if (File.Exists(path)) File.Delete(path); // a reused filename across releases overwrites
            using var zip = ZipFile.Open(path, ZipArchiveMode.Create);
            WriteEntry(zip, "taxonomy.csv",
                "internalTaxonId,scientificName\n" + taxonId + ",Genus species" + taxonId + "\n");
            WriteEntry(zip, "assessments.csv",
                "assessmentId,internalTaxonId,scientificName\n" + assessmentId + "," + taxonId + ",Genus species" + taxonId + "\n");
            return path;
        }

        private static void WriteEntry(ZipArchive zip, string entryName, string content) {
            var entry = zip.CreateEntry(entryName);
            using var writer = new StreamWriter(entry.Open());
            writer.Write(content);
        }

        public void Import(string zipPath, string versionHint, bool force) {
            var importer = new IucnImporter(AnsiConsole.Console, _conn, _dir, force, versionHint);
            importer.ProcessZip(zipPath, CancellationToken.None);
        }

        public long TaxonomyCount() => ScalarLong("SELECT COUNT(*) FROM taxonomy;");

        public long ScalarLong(string sql) {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = sql;
            return (long)cmd.ExecuteScalar()!;
        }

        public string? ScalarString(string sql) {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar() as string;
        }

        public void Dispose() {
            _conn.Dispose();
            try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        }
    }
}
