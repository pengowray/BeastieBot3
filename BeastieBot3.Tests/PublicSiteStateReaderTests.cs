using System;
using System.IO;
using System.Linq;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Tests.Gbif;
using BeastieBot3.Web.Flows;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// The public site workflow's reader, over small files in a temporary folder: the release and import
// time of the IUCN Red List database, the newest download in the API cache, the newest GBIF
// checklist zip, the site database's meta table, and the change time of every other input.
public class PublicSiteStateReaderTests : IDisposable {
    private static readonly DateTime Old = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Recent = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-site-flow-" + Guid.NewGuid().ToString("N"));

    public PublicSiteStateReaderTests() {
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private string P(string name) => Path.Combine(_dir, name);

    private void Exec(string name, string sql) {
        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = P(name), Pooling = false }.ConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private PublicSitePaths Paths() => new() {
        IucnDatabase = P("iucn.sqlite"),
        ApiCache = P("api.sqlite"),
        GbifDir = P("gbif"),
        DoiCache = P("doi.sqlite"),
        CommonNames = P("names.sqlite"),
        WikidataCache = P("wikidata.sqlite"),
        WikipediaCache = P("enwiki.sqlite"),
        ColPlacement = P("col.sqlite.placement.sqlite"),
        SpratDatabase = P("sprat.sqlite"),
        StatusLists = P("status_lists.sqlite"),
        SiteDatabase = P("site.sqlite"),
    };

    [Fact]
    public void Reads_releases_build_meta_and_input_times() {
        Exec("iucn.sqlite", """
            CREATE TABLE import_metadata (id INTEGER PRIMARY KEY, filename TEXT, redlist_version TEXT, started_at TEXT, ended_at TEXT);
            INSERT INTO import_metadata (filename, redlist_version, started_at, ended_at) VALUES
                ('a.zip', '2026-1', '2026-08-14T07:00:00Z', '2026-08-14T07:30:00.0000000+00:00'),
                ('b.zip', '2026-1', '2026-08-14T07:31:00Z', '2026-08-14T07:48:00.0000000+00:00'),
                ('c.zip', '2026-1', '2026-08-15T00:00:00Z', NULL);
            """);
        Exec("api.sqlite", """
            CREATE TABLE taxa (id INTEGER PRIMARY KEY, downloaded_at TEXT NOT NULL);
            CREATE TABLE assessments (id INTEGER PRIMARY KEY, downloaded_at TEXT NOT NULL);
            INSERT INTO taxa (downloaded_at) VALUES ('2026-09-01T00:00:00.0000000Z'), ('2026-09-05T08:00:00.0000000Z');
            INSERT INTO assessments (downloaded_at) VALUES ('2026-09-04T00:00:00.0000000Z');
            """);
        Exec("site.sqlite", $"""
            {SiteDbSchema.Ddl}
            INSERT INTO meta VALUES ('schema_version', '{SiteDbSchema.Version}'), ('built_at_utc', '2026-10-03T03:14:00Z'),
                                    ('iucn_release', '2026-1'), ('taxon_count', '237412');
            """);
        Directory.CreateDirectory(P("gbif"));
        using (var archive = GbifIucnChecklistReaderTests.BuildArchive()) {
            File.WriteAllBytes(P("gbif/iucn-checklist-2026-07-28.zip"), archive.ToArray());
        }
        File.WriteAllText(P("gbif/notes.zip"), "not a checklist");
        Exec("wikidata.sqlite", "CREATE TABLE t (x INTEGER);");
        File.SetLastWriteTimeUtc(P("wikidata.sqlite"), Recent);

        var s = PublicSiteStateReader.Read(Paths());

        Assert.True(s.IucnExists);
        Assert.Equal("2026-1", s.IucnRelease);
        Assert.Equal("iucn-checklist-2026-07-28.zip", s.GbifZipName);
        Assert.Equal("2026-1", s.GbifRelease);
        Assert.Equal("2026-07-28", s.GbifPublished);
        Assert.Null(s.GbifReadError);
        Assert.False(s.DoiCacheExists);

        Assert.True(s.SiteExists);
        Assert.Null(s.SiteReadError);
        Assert.Equal(SiteDbSchema.Version, s.SiteSchemaVersion);
        Assert.Equal(new DateTime(2026, 10, 3, 3, 14, 0, DateTimeKind.Utc), s.SiteBuiltAtUtc);
        Assert.Equal(DateTimeKind.Utc, s.SiteBuiltAtUtc!.Value.Kind);
        Assert.Equal("2026-1", s.SiteIucnRelease);
        Assert.Equal(237_412, s.SiteTaxonCount);

        // Only inputs that exist, in build order; the import time is the last finished import, and
        // the API cache's time is its newest download, not the file's time.
        Assert.Equal(
            new[] { PublicSiteStateReader.IucnInput, PublicSiteStateReader.ApiCacheInput, PublicSiteStateReader.GbifInput, PublicSiteStateReader.WikidataInput },
            s.Inputs.Select(i => i.Name));
        Assert.Equal(new DateTime(2026, 8, 14, 7, 48, 0, DateTimeKind.Utc), s.Inputs[0].ChangedAtUtc);
        Assert.Equal(new DateTime(2026, 9, 5, 8, 0, 0, DateTimeKind.Utc), s.Inputs[1].ChangedAtUtc);
        Assert.Equal(Recent, s.Inputs[3].ChangedAtUtc);

        Assert.Equal("ok", PublicSiteProbes.GbifStep(s).Status);
    }

    // Opening a WAL-mode database read-write touches an empty -wal file without changing anything,
    // and a WAL-mode database can keep its latest writes in the -wal file for days.
    [Fact]
    public void Sqlite_change_time_counts_the_wal_file_only_when_it_has_content() {
        File.WriteAllText(P("db.sqlite"), "main");
        File.SetLastWriteTimeUtc(P("db.sqlite"), Old);

        Assert.Equal(Old, PublicSiteStateReader.SqliteChangedAt(P("db.sqlite")));

        File.WriteAllText(P("db.sqlite-wal"), "");
        File.SetLastWriteTimeUtc(P("db.sqlite-wal"), Recent);
        File.WriteAllText(P("db.sqlite-shm"), "shared memory");
        File.SetLastWriteTimeUtc(P("db.sqlite-shm"), Recent.AddDays(1));
        Assert.Equal(Old, PublicSiteStateReader.SqliteChangedAt(P("db.sqlite")));

        File.WriteAllText(P("db.sqlite-wal"), "frames");
        File.SetLastWriteTimeUtc(P("db.sqlite-wal"), Recent);
        Assert.Equal(Recent, PublicSiteStateReader.SqliteChangedAt(P("db.sqlite")));

        Assert.Null(PublicSiteStateReader.SqliteChangedAt(P("missing.sqlite")));
        Assert.Null(PublicSiteStateReader.SqliteChangedAt(null));
    }

    [Fact]
    public void Missing_files_read_as_missing_without_throwing() {
        var s = PublicSiteStateReader.Read(Paths());
        Assert.False(s.IucnExists);
        Assert.False(s.SiteExists);
        Assert.Null(s.GbifZipName);
        Assert.Empty(s.Inputs);
        Assert.Equal("todo", PublicSiteProbes.BuildStep(s).Status);
        Assert.Equal("todo", PublicSiteProbes.GbifStep(s).Status);
    }

    [Fact]
    public void Site_database_without_a_meta_table_reports_why() {
        Exec("site.sqlite", "CREATE TABLE other (x INTEGER);");
        var s = PublicSiteStateReader.Read(Paths());
        Assert.True(s.SiteExists);
        Assert.Contains("meta", s.SiteReadError);
        Assert.Equal("todo", PublicSiteProbes.BuildStep(s).Status);
    }

    [Fact]
    public void Unreadable_checklist_zip_reports_why() {
        Directory.CreateDirectory(P("gbif"));
        File.WriteAllText(P("gbif/iucn-checklist-2026-07-28.zip"), "not a zip");
        var s = PublicSiteStateReader.Read(Paths());
        Assert.Equal("iucn-checklist-2026-07-28.zip", s.GbifZipName);
        Assert.NotNull(s.GbifReadError);
        Assert.Equal("todo", PublicSiteProbes.GbifStep(s).Status);
    }

    // The status lists store: the source rows of finished downloads, and the NatureServe download
    // under way with the records it has stored.
    [Fact]
    public void Reads_the_status_lists_store() {
        var path = P("status_lists.sqlite");
        var started = new DateTime(2026, 10, 8, 1, 0, 0, DateTimeKind.Utc);
        using (var store = BeastieBot3.StatusLists.StatusListStore.Open(path)) {
            store.UpsertSource(new BeastieBot3.StatusLists.StatusSourceInfo("ecos", "ECOS", "https://ecos.fws.gov/ecp/", "Public domain", null, null, Recent, 2478));
            store.StartNatureServePass(new System.Collections.Generic.Dictionary<string, string?> {
                [BeastieBot3.StatusLists.NatureServePassKeys.Started] = BeastieBot3.StatusLists.StatusListStore.Stamp(started),
                [BeastieBot3.StatusLists.NatureServePassKeys.Total] = "113530",
            }, BeastieBot3.StatusLists.NatureServePassKeys.PassKeys, "");
        }
        Exec("status_lists.sqlite", """
            INSERT INTO natureserve_species (element_global_id, unique_id, scientific_name, infraspecies, nsx_url, fetched_at) VALUES
                (1, 'ELEMENT_GLOBAL.2.1', 'Acris blanchardi', 0, 'https://explorer.natureserve.org/', '2026-10-08T01:05:00.0000000Z'),
                (2, 'ELEMENT_GLOBAL.2.2', 'Acris crepitans', 0, 'https://explorer.natureserve.org/', '2026-09-01T00:00:00.0000000Z');
            """);
        SqliteConnection.ClearAllPools();

        var s = PublicSiteStateReader.Read(Paths());
        Assert.Equal(path, s.StatusListsPath);
        Assert.Equal(new StatusListSourceState(Recent, 2478), s.Ecos);
        Assert.Null(s.NatureServe);
        Assert.Equal(started, s.NatureServePassStartedUtc);
        Assert.Equal(1, s.NatureServePassStored);
        Assert.Equal(113530, s.NatureServePassTotal);
        Assert.Contains(s.Inputs, i => i.Name == PublicSiteStateReader.StatusListsInput);
        Assert.Equal("backlog", PublicSiteProbes.NatureServeStep(s).Status);
        Assert.Equal("ok", PublicSiteProbes.EcosStep(s with { ReadAtUtc = Recent.AddDays(1) }).Status);
    }
}
