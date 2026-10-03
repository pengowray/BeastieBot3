using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Endpoints;
using BeastieBot3.Web.Status;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

// The CSV vs API card on the Data sources page (/api/dataset-compare). The API projection counts as
// built only when its newest build record has an end time, as the workflow lights decide. The
// 2026-09-01 project-view build stopped part way, leaving empty tables and a record with no end
// time, and the card showed it as built and complete, with 0 assessments against the CSV release's
// 197,315.
public sealed class DatasetCompareTests : IDisposable {
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("beastiebot-dataset-compare-");
    private string ProjectionPath => Path.Combine(_dir.FullName, "iucn_api_projected.sqlite");

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        _dir.Delete(recursive: true);
    }

    private static readonly DatasetStats Csv = new() {
        Exists = true,
        Path = "/data/IUCN_2026-1.sqlite",
        Version = "2026-1",
        TotalAssessments = 197_315,
        DistinctTaxa = 196_000,
        GlobalSpecies = 176_000,
        ByCategory = new[] { new DatasetCategoryCount("Least Concern", 90_000), new DatasetCategoryCount("Data Deficient", 86_000) },
    };

    private void Build(bool finish) {
        using var store = IucnApiProjectionStore.Open(ProjectionPath);
        store.ResetData();
        var importId = store.InsertImport("iucn_api_cache.sqlite", "api-cache");
        store.BuildView();
        if (finish) {
            store.CompleteImport(importId, projectedTaxa: 0, projectedAssessments: 0, latestNotDownloaded: 0, isPartial: false);
        }
    }

    private JsonElement Respond() {
        var api = DatasetStatsService.Compute(ProjectionPath);
        var build = IucnApiCacheStateReader.ReadProjectionFile(ProjectionPath);
        var json = JsonSerializer.Serialize(DatasetCompareEndpoints.Response(Csv, api, build, DateTimeOffset.UnixEpoch));
        return JsonDocument.Parse(json).RootElement;
    }

    private static JsonElement Row(JsonElement response, string label) =>
        response.GetProperty("comparison").EnumerateArray().Single(r => r.GetProperty("label").GetString() == label);

    [Fact]
    public void Projection_whose_last_build_did_not_finish_is_not_built_and_has_no_counts() {
        Build(finish: true);
        Build(finish: false);

        // The file reads as an IUCN dataset with no error, which is all the old rule looked at.
        var stats = DatasetStatsService.Compute(ProjectionPath);
        Assert.True(stats.Exists);
        Assert.Null(stats.Error);
        Assert.Equal(0, stats.TotalAssessments);

        var r = Respond();
        var api = r.GetProperty("api");
        Assert.True(api.GetProperty("exists").GetBoolean());
        Assert.False(api.GetProperty("built").GetBoolean());
        Assert.Equal(JsonValueKind.String, api.GetProperty("unfinishedBuildStartedAt").ValueKind);
        Assert.Equal(JsonValueKind.Null, api.GetProperty("totalAssessments").ValueKind);
        Assert.Equal(JsonValueKind.Null, api.GetProperty("partial").ValueKind);
        Assert.Equal(JsonValueKind.Null, api.GetProperty("version").ValueKind);

        // No difference is shown against a projection with nothing in it.
        var total = Row(r, "Total assessments");
        Assert.Equal(197_315, total.GetProperty("csv").GetInt64());
        Assert.Equal(JsonValueKind.Null, total.GetProperty("api").ValueKind);
        Assert.Equal(JsonValueKind.Null, total.GetProperty("delta").ValueKind);
        Assert.Equal(JsonValueKind.Null, Row(r, "Least Concern").GetProperty("api").ValueKind);
    }

    [Fact]
    public void Projection_with_a_finished_build_is_built_with_its_counts() {
        Build(finish: true);

        var r = Respond();
        var api = r.GetProperty("api");
        Assert.True(api.GetProperty("built").GetBoolean());
        Assert.Equal(JsonValueKind.Null, api.GetProperty("unfinishedBuildStartedAt").ValueKind);
        Assert.Equal("api-cache", api.GetProperty("version").GetString());
        Assert.False(api.GetProperty("partial").GetBoolean());

        var total = Row(r, "Total assessments");
        Assert.Equal(0, total.GetProperty("api").GetInt64());
        Assert.Equal(-197_315, total.GetProperty("delta").GetInt64());
        Assert.Equal(0, Row(r, "Least Concern").GetProperty("api").GetInt64());
    }

    [Fact]
    public void Missing_projection_is_not_built() {
        var r = Respond();
        var api = r.GetProperty("api");
        Assert.False(api.GetProperty("exists").GetBoolean());
        Assert.False(api.GetProperty("built").GetBoolean());
        Assert.Equal(JsonValueKind.Null, api.GetProperty("unfinishedBuildStartedAt").ValueKind);
        Assert.True(r.GetProperty("csv").GetProperty("built").GetBoolean());
    }

    // Without a readable build record the file's stats decide, as before.
    [Fact]
    public void Built_falls_back_to_the_stats_when_the_build_state_is_unknown() {
        Assert.True(DatasetCompareEndpoints.IsBuilt(Csv, null));
        Assert.False(DatasetCompareEndpoints.IsBuilt(Csv with { Error = "file is not a database" }, null));
        Assert.False(DatasetCompareEndpoints.IsBuilt(Csv, new IucnProjectionState { Path = "p", Exists = false }));
    }

    // A database in WAL mode can keep its latest writes in the -wal file, so the cached stats must
    // be counted again when only that file changes.
    [Fact]
    public void Stats_are_counted_again_when_only_the_wal_file_changes() {
        Build(finish: true);
        var before = DatasetStatsService.Compute(ProjectionPath);
        SqliteConnection.ClearAllPools();
        File.WriteAllBytes(ProjectionPath + "-wal", Array.Empty<byte>());
        File.SetLastWriteTimeUtc(ProjectionPath + "-wal", DateTime.UtcNow.AddMinutes(5));
        var after = DatasetStatsService.Compute(ProjectionPath);
        Assert.NotSame(before, after);
    }
}
