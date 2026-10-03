using System;
using System.Linq;
using BeastieBot3.Iucn;
using BeastieBot3.Iucn.Doi;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Web.Status;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the counts on the Data sources page cards. Each metric is looked up by its label, because
// app.js keys the amber attention pill on the label text ("assessments to download").
public class DataSourceMetricTests {
    private static MetricSpec Metric(string sourceId, string label) =>
        DataSourceCatalogue.All.Single(d => d.Id == sourceId).Metrics.Single(m => m.Label == label);

    private static long? Run(SqliteConnection conn, string sourceId, string label) {
        var result = StatusService.RunMetric(conn, Metric(sourceId, label));
        Assert.Null(result.Error);
        return result.Value;
    }

    [Fact]
    public void AssessmentsToDownload_CountsQueuedIdsNeitherDownloadedNorFailed() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnApiCacheStore.OpenFromConnection(conn);

        var importId = store.BeginImport("/api/v4/taxa/sis/100");
        store.WriteTaxonAtomic(
            rootSisId: 100,
            importId: importId,
            json: "{\"taxon\":1}",
            downloadedAt: DateTime.UtcNow,
            mappings: new[] { new TaxaLookupRow(100, 100, "species") },
            assessments: new[] {
                new IucnAssessmentHeader(900, 100, Latest: true, YearPublished: 2024),
                new IucnAssessmentHeader(901, 100, Latest: false, YearPublished: 2016),
                new IucnAssessmentHeader(902, 100, Latest: false, YearPublished: 2008),
            });

        Assert.Equal(3L, Run(conn, "iucn-api-cache", "assessments to download"));

        // Downloaded: leaves the count, though its backlog row stays.
        store.UpsertAssessment(900, 100, importId, "{\"assessment\":900}", DateTime.UtcNow);
        // Tombstoned by a 404: counted under "failed requests" instead.
        store.RecordFailedRequest("assessment", 902, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);
        // A failure on another endpoint with the same id does not hide assessment 901.
        store.RecordFailedRequest("taxa_sis", 901, "Not found", 404, IucnApiCacheStore.PermanentRetryDelay);

        Assert.Equal(1L, Run(conn, "iucn-api-cache", "assessments to download"));
        Assert.Equal(1L, Run(conn, "iucn-api-cache", "assessments cached"));
        Assert.Equal(2L, Run(conn, "iucn-api-cache", "failed requests"));

        store.UpsertAssessment(901, 100, importId, "{\"assessment\":901}", DateTime.UtcNow);
        Assert.Equal(0L, Run(conn, "iucn-api-cache", "assessments to download"));
    }

    [Fact]
    public void MatchedTaxa_CountsOnlyMatchedRows() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = WikipediaCacheStore.OpenFromConnection(conn);

        void Match(string id, string status) => store.UpsertTaxonMatch(new TaxonWikiMatch(
            "iucn", id, status, PageRowId: null, CandidateTitle: "Title " + id, NormalizedTitle: null,
            SynonymUsed: null, RedirectFinalTitle: null, MatchMethod: null, Notes: null, MatchedAt: DateTime.UtcNow));

        Match("1", TaxonWikiMatchStatus.Matched);
        Match("2", TaxonWikiMatchStatus.Matched);
        Match("3", TaxonWikiMatchStatus.Missing);
        Match("4", TaxonWikiMatchStatus.Pending);
        Match("5", TaxonWikiMatchStatus.Rejected);

        Assert.Equal(2L, Run(conn, "wikipedia-cache", "matched taxa"));
        Assert.Equal(store.GetCacheStats().MatchedTaxa, Run(conn, "wikipedia-cache", "matched taxa"));
    }

    [Fact]
    public void MissingTable_IsReportedAsNotCreated() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();

        var result = StatusService.RunMetric(conn, Metric("iucn-api-cache", "assessments to download"));

        Assert.Null(result.Value);
        Assert.Null(result.Error);
        Assert.Equal("none in this file yet", result.Note);
    }

    [Fact]
    public void DoiCache_CountsCheckedAssessmentsWithAndWithoutADoi() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = IucnDoiCacheStore.OpenFromConnection(conn);

        void Check(long assessmentId, string? doi) => store.SaveCheck(
            new DoiCheckRow(assessmentId, 1, doi, DateTime.UtcNow, CandidatesTried: 0),
            DoiFoundBy.Crossref, "global", 2020, note: null, Array.Empty<DoiLookupLogRow>());

        Check(10, "10.2305/IUCN.UK.2020-1.RLTS.T1A10.en");
        Check(11, null);
        Check(12, null);

        Assert.Equal(3L, Run(conn, "iucn-doi-cache", "assessments checked"));
        Assert.Equal(1L, Run(conn, "iucn-doi-cache", "DOI found"));
        Assert.Equal(2L, Run(conn, "iucn-doi-cache", "no DOI found"));
    }

    [Fact]
    public void SiteDatabase_CountsTaxaAndAssessments_AndReadsTheSchemaVersion() {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        using (var cmd = conn.CreateCommand()) {
            cmd.CommandText = $"""
                {SiteDbSchema.Ddl}
                INSERT INTO meta VALUES ('{SiteDbSchema.MetaKeys.SchemaVersion}', '{SiteDbSchema.Version}');
                INSERT INTO taxon (taxon_id, scientific_name, kind, in_release) VALUES
                    (1, 'Panthera leo', 'species', 1), (2, 'Panthera pardus', 'species', 1);
                INSERT INTO assessment (assessment_id, taxon_id, scope, is_latest, category) VALUES
                    (100, 1, 'Global', 1, 'VU'), (101, 1, 'Global', 0, 'NT'), (200, 2, 'Global', 1, 'VU');
                """;
            cmd.ExecuteNonQuery();
        }

        Assert.Equal(2L, Run(conn, "site-sqlite", "taxa"));
        Assert.Equal(3L, Run(conn, "site-sqlite", "assessments"));
        Assert.Equal((long)SiteDbSchema.Version, Run(conn, "site-sqlite", "schema version"));
    }
}
