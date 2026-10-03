using System;
using System.Collections.Generic;
using System.Threading;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins which cached assessments `iucn api project-view` treats as current. The taxon record's
// header (taxa_assessment_backlog.latest) decides, because a payload keeps the latest flag it had
// when it was downloaded: in the 2026-1 cache, payload 86378350 still said latest=true after
// 2215707 replaced it, and taxon 193274 got two Global rows. The payload's own flag decides only
// for a payload no taxon record lists.
public class IucnApiProjectViewTests {
    private const long Taxon = 193274;

    private static string AssessmentJson(long assessmentId, long sisId, bool latest, string category = "LC", string scope = "Global") =>
        $$"""
        {
          "assessment_id": {{assessmentId}},
          "sis_taxon_id": {{sisId}},
          "latest": {{(latest ? "true" : "false")}},
          "year_published": "2019",
          "red_list_category": { "code": "{{category}}", "description": { "en": "x" } },
          "scopes": [ { "description": { "en": "{{scope}}" }, "code": "1" } ],
          "taxon": {
            "scientific_name": "Trinectes paulistanus",
            "kingdom_name": "ANIMALIA",
            "class_name": "ACTINOPTERYGII",
            "genus_name": "Trinectes",
            "species_name": "paulistanus"
          }
        }
        """;

    // A cache holding taxon 193274 with two listed assessments, plus payloads no taxon lists.
    private static IucnApiCacheStore SeedCache(SqliteConnection conn) {
        var cache = IucnApiCacheStore.OpenFromConnection(conn);
        var importId = cache.BeginImport("/api/v4/taxa/sis/193274");
        cache.WriteTaxonAtomic(
            rootSisId: Taxon,
            importId: importId,
            json: "{\"taxon\":1}",
            downloadedAt: DateTime.UtcNow,
            mappings: new[] { new TaxaLookupRow(Taxon, Taxon, "species") },
            assessments: new[] {
                new IucnAssessmentHeader(2215707, Taxon, Latest: true, YearPublished: 2015),
                new IucnAssessmentHeader(86378350, Taxon, Latest: false, YearPublished: 2019),
                // Listed as latest, but the payload was downloaded while it was not.
                new IucnAssessmentHeader(5000001, 500, Latest: true, YearPublished: 2020),
            });

        var now = DateTime.UtcNow;
        cache.UpsertAssessment(2215707, Taxon, importId, AssessmentJson(2215707, Taxon, latest: true), now);
        // Stale payload: says latest=true, but the taxon record says it is not the latest.
        cache.UpsertAssessment(86378350, Taxon, importId, AssessmentJson(86378350, Taxon, latest: true), now);
        cache.UpsertAssessment(5000001, 500, importId, AssessmentJson(5000001, 500, latest: false), now);
        // Payloads no taxon record lists (as downloaded by cache-assessments --csv-missing).
        cache.UpsertAssessment(7000001, 700, importId, AssessmentJson(7000001, 700, latest: true, scope: "Europe"), now);
        cache.UpsertAssessment(7000002, 700, importId, AssessmentJson(7000002, 700, latest: false, scope: "Europe"), now);
        return cache;
    }

    private static (IucnApiProjectViewCommand.ProjectionCounts Counts, List<(long TaxonId, long AssessmentId)> Rows) RunProjection(
        int? limit = null, List<(long Read, long Current)>? progress = null, int progressEvery = IucnApiProjectViewCommand.ProgressEvery) {
        using var cacheConn = new SqliteConnection("Data Source=:memory:");
        cacheConn.Open();
        SeedCache(cacheConn);

        using var projectionConn = new SqliteConnection("Data Source=:memory:");
        projectionConn.Open();
        var store = IucnApiProjectionStore.OpenFromConnection(projectionConn);
        var importId = store.InsertImport("cache.sqlite", "api-cache");

        var counts = IucnApiProjectViewCommand.Project(cacheConn, store, importId, limit,
            progress is null ? null : (read, current) => progress.Add((read, current)),
            CancellationToken.None, progressEvery);

        var rows = new List<(long, long)>();
        using var cmd = projectionConn.CreateCommand();
        cmd.CommandText = "SELECT taxonId, assessmentId FROM assessments_html ORDER BY assessmentId";
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) rows.Add((reader.GetInt64(0), reader.GetInt64(1)));
        return (counts, rows);
    }

    [Fact]
    public void TaxonRecordDecidesLatest_StalePayloadIsLeftOut() {
        var (counts, rows) = RunProjection();

        Assert.Equal(new (long, long)[] {
            (Taxon, 2215707),   // listed as latest, payload agrees
            (500, 5000001),     // listed as latest, payload says not: the taxon record wins
            (700, 7000001),     // not listed: the payload's latest=true decides
        }, rows);
        Assert.DoesNotContain(rows, r => r.AssessmentId == 86378350);
        Assert.Equal(5, counts.Processed);
        Assert.Equal(3, counts.LatestRows);
    }

    // Progress used to be checked only after a current row was written, so a line appeared only
    // when the Nth row read happened to be current. Row 4 (7000002) is not current.
    [Fact]
    public void Progress_IsReportedEveryNRowsRead_CurrentOrNot() {
        var progress = new List<(long Read, long Current)>();
        RunProjection(progress: progress, progressEvery: 2);

        Assert.Equal(new (long, long)[] { (2, 2), (4, 3) }, progress);
    }

    [Fact]
    public void Limit_CountsRowsRead() {
        var (counts, rows) = RunProjection(limit: 2);

        // Rows are read in assessment id order: 2215707 (projected), 5000001 (projected).
        Assert.Equal(2, counts.Processed);
        Assert.Equal(new (long, long)[] { (Taxon, 2215707), (500, 5000001) }, rows);
    }
}
