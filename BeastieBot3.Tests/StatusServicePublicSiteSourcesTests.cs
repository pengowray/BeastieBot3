using System;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn.Doi;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Web.Flows;
using BeastieBot3.Web.Status;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The Data sources page's cards for the public site's files: the GBIF checklist folder, the DOI
// cache and the site database, read through paths.ini like the other sources. The site database is
// replaced by a rename (`site build-db`), so the status service must open it fresh every time.
public class StatusServicePublicSiteSourcesTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-status-site-" + Guid.NewGuid().ToString("N"));

    private sealed class ManualClock : TimeProvider {
        public DateTimeOffset Now { get; set; } = new(2026, 10, 3, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    public StatusServicePublicSiteSourcesTests() {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(P("paths.ini"), $"""
            [Datasets]
            GBIF_IUCN_dir={P("gbif")}
            [Datastore]
            IUCN_doi_cache_sqlite={P("doi.sqlite")}
            site_sqlite={P("site.sqlite")}
            """);
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { /* a locked temp dir is not a test failure */ }
    }

    private string P(string name) => Path.Combine(_dir, name);

    private PathsService Paths() => new(P("paths.ini"), _dir);

    private static void WriteSiteDatabase(string path, int taxa) {
        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        var rows = string.Join(", ", Enumerable.Range(1, taxa).Select(i => $"({i}, 'Taxon {i}', 'species', 1)"));
        cmd.CommandText = $"""
            {SiteDbSchema.Ddl}
            INSERT INTO meta VALUES ('{SiteDbSchema.MetaKeys.SchemaVersion}', '{SiteDbSchema.Version}');
            INSERT INTO taxon (taxon_id, scientific_name, kind, in_release) VALUES {rows};
            INSERT INTO assessment (assessment_id, taxon_id, scope, is_latest, category) VALUES (100, 1, 'Global', 1, 'LC');
            """;
        cmd.ExecuteNonQuery();
    }

    private static DataSourceStatus Source(System.Collections.Generic.IReadOnlyList<DataSourceStatus> all, string id) =>
        all.Single(s => s.Id == id);

    private static long? Value(DataSourceStatus s, string label) => s.Metrics.Single(m => m.Label == label).Value;

    [Fact]
    public void Collect_ReadsTheGbifFolder_TheDoiCache_AndTheSiteDatabase() {
        Directory.CreateDirectory(P("gbif"));
        File.WriteAllBytes(P("gbif/iucn-checklist-2026-07-28.zip"), new byte[1234]);
        using (var store = IucnDoiCacheStore.Open(P("doi.sqlite"))) {
            store.SaveCheck(new DoiCheckRow(10, 1, "10.2305/IUCN.UK.2020-1.RLTS.T1A10.en", DateTime.UtcNow, 0),
                DoiFoundBy.Crossref, "global", 2020, null, Array.Empty<DoiLookupLogRow>());
            store.SaveCheck(new DoiCheckRow(11, 1, null, DateTime.UtcNow, 2),
                DoiFoundBy.DoiOrg, "global", 2024, null, Array.Empty<DoiLookupLogRow>());
        }
        SqliteConnection.ClearAllPools();
        WriteSiteDatabase(P("site.sqlite"), taxa: 2);

        var all = new StatusService(Paths()).Collect();

        var gbif = Source(all, "gbif-checklist");
        Assert.Equal("directory", gbif.Kind);
        Assert.Equal(Path.GetFullPath(P("gbif")), gbif.Path);
        Assert.True(gbif.Exists);
        Assert.Equal(1234, gbif.SizeBytes);
        Assert.NotNull(gbif.LastModified);
        Assert.Equal(1L, Value(gbif, "files"));

        var doi = Source(all, "iucn-doi-cache");
        Assert.True(doi.Exists);
        Assert.Null(doi.Error);
        Assert.NotNull(doi.SizeBytes);
        Assert.NotNull(doi.LastModified);
        Assert.Equal(2L, Value(doi, "assessments checked"));
        Assert.Equal(1L, Value(doi, "DOI found"));
        Assert.Equal(1L, Value(doi, "no DOI found"));

        var site = Source(all, "site-sqlite");
        Assert.True(site.Exists);
        Assert.Null(site.Error);
        Assert.Equal(2L, Value(site, "taxa"));
        Assert.Equal(1L, Value(site, "assessments"));
        Assert.Equal((long)SiteDbSchema.Version, Value(site, "schema version"));
        Assert.Equal("2 taxa", FlowEvaluator.SummariseHeadline(site));
    }

    // `site build-db` writes site.sqlite.building and renames it over site.sqlite. A pooled
    // connection kept the old file open: on Linux the card went on counting the old file's rows,
    // and on Windows the rename would fail while the web UI was open.
    [Fact]
    public void Collect_ReadsTheSiteDatabaseAgainAfterABuildRenamesANewFileOverIt() {
        WriteSiteDatabase(P("site.sqlite"), taxa: 2);
        var clock = new ManualClock();
        var service = new StatusService(Paths(), clock);
        Assert.Equal(2L, Value(Source(service.Collect(), "site-sqlite"), "taxa"));

        WriteSiteDatabase(P("site.sqlite.building"), taxa: 5);
        File.Move(P("site.sqlite.building"), P("site.sqlite"), overwrite: true);
        clock.Now += StatusService.ReuseFor;

        Assert.Equal(5L, Value(Source(service.Collect(), "site-sqlite"), "taxa"));
    }

    [Fact]
    public void Collect_SaysWhenTheFilesAreNotThereYet() {
        var all = new StatusService(Paths()).Collect();
        foreach (var id in new[] { "gbif-checklist", "iucn-doi-cache", "site-sqlite" }) {
            var s = Source(all, id);
            Assert.False(s.Exists);
            Assert.Null(s.Error);
            Assert.Equal("missing", FlowEvaluator.SummariseHeadline(s));
        }
        // Reading a missing database must not create it.
        Assert.False(File.Exists(P("doi.sqlite")));
        Assert.False(File.Exists(P("site.sqlite")));
    }
}
