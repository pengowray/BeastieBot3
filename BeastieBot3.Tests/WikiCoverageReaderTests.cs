using System;
using System.IO;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Flows;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// The coverage counts span three databases, so the reader ATTACHes the two caches onto the IUCN
// connection. Microsoft.Data.Sqlite pools connections by connection string and returns them with
// their ATTACHes intact, so a pooled handle made the second ATTACH fail with "database wd is
// already in use". Everything after the first measurement in a process then read as "not known":
// `serve` showed no wiki counts after its first minute, and `wikipedia update` ran every step
// blind. Reading twice is the whole test.
public class WikiCoverageReaderTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-wikicov-" + Guid.NewGuid().ToString("N"));

    public WikiCoverageReaderTests() {
        Directory.CreateDirectory(_dir);
        Exec("iucn.sqlite", """
            CREATE TABLE taxonomy_html(taxonId INTEGER, subpopulationName TEXT);
            INSERT INTO taxonomy_html VALUES (1, NULL), (2, ''), (3, 'Lake Turkana');
            """);
        Exec("wikidata.sqlite", """
            CREATE TABLE wikidata_entities(entity_numeric_id INTEGER, json_downloaded INTEGER,
                                           attempt_count INTEGER DEFAULT 0, last_error TEXT);
            INSERT INTO wikidata_entities(entity_numeric_id, json_downloaded) VALUES (10, 1), (11, 0);
            CREATE TABLE wikidata_p627_values(entity_numeric_id INTEGER, value TEXT);
            INSERT INTO wikidata_p627_values VALUES (10, '1');
            CREATE TABLE wikidata_pending_iucn_matches(iucn_taxon_id TEXT);
            CREATE TABLE wikidata_sync_state(key TEXT, value TEXT);
            """);
        Exec("enwiki.sqlite", """
            CREATE TABLE wiki_pages(id INTEGER PRIMARY KEY, normalized_title TEXT, download_status TEXT,
                                    downloaded_at TEXT);
            INSERT INTO wiki_pages VALUES (1, 'Panthera leo', 'cached', '2026-01-01T00:00:00.0000000Z');
            CREATE TABLE taxon_wiki_matches(taxon_source TEXT, taxon_identifier TEXT, match_status TEXT,
                                            page_row_id INTEGER);
            INSERT INTO taxon_wiki_matches VALUES ('iucn', '1', 'matched', 1);
            CREATE TABLE wiki_missing_titles(title TEXT);
            """);
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), $"""
            [Datastore]
            IUCN_sqlite_from_cvs={Path.Combine(_dir, "iucn.sqlite")}
            wikidata_cache_sqlite={Path.Combine(_dir, "wikidata.sqlite")}
            enwiki_cache_sqlite={Path.Combine(_dir, "enwiki.sqlite")}
            """);
    }

    [Fact]
    public void CountsStayAvailableAcrossRepeatedReads() {
        var paths = new PathsService(Path.Combine(_dir, "paths.ini"), _dir);

        for (var read = 1; read <= 3; read++) {
            WikiCoverageStateReader.Invalidate();
            var state = WikiCoverageStateReader.ReadNow(paths);
            Assert.True(state.Known, $"read {read} could not measure");
            Assert.Equal(2, state.IucnTaxa);            // the subpopulation row is not a taxon either cache places
            Assert.Equal(1, state.WikidataEntitiesCached);
            Assert.Equal(1, state.WikidataEntitiesQueued);
            Assert.Equal(1, state.TaxaWithoutWikidata);
            Assert.Equal(1, state.TaxaWithArticle);
            Assert.Equal(1, state.TaxaNeverMatched);
        }
    }

    private void Exec(string file, string sql) {
        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = Path.Combine(_dir, file), Mode = SqliteOpenMode.ReadWriteCreate
        }.ConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { /* a locked temp dir is not a test failure */ }
    }
}
