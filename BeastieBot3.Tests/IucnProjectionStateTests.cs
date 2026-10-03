using System;
using System.IO;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins how the API projection's state is read for cache-all and the workflow light. project-view
// empties the projection before it writes, so a build that stops part way leaves no data. The
// 2026-09-01 build stopped that way, and the reader skipped to the last finished build record,
// which the stopped build had deleted, so cache-all reported "complete · 0 taxa · built
// 0001-01-01" for an empty projection.
public class IucnProjectionStateTests : IDisposable {
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("beastiebot-projection-state-");
    private string ProjectionPath => Path.Combine(_dir.FullName, "iucn_api_projected.sqlite");

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        _dir.Delete(recursive: true);
    }

    private void Build(bool finish) {
        using var store = IucnApiProjectionStore.Open(ProjectionPath);
        store.ResetData();
        var importId = store.InsertImport("iucn_api_cache.sqlite", "api-cache");
        if (finish) {
            store.CompleteImport(importId, projectedTaxa: 181_338, projectedAssessments: 181_400, latestNotDownloaded: 0, isPartial: false);
        }
    }

    [Fact]
    public void NoFile_IsNotBuilt() {
        var state = IucnApiCacheStateReader.ReadProjectionFile(ProjectionPath);

        Assert.False(state.Exists);
        Assert.Null(state.UnfinishedBuildStartedAt);
    }

    [Fact]
    public void FinishedBuild_IsReadFromItsRecord() {
        Build(finish: true);

        var state = IucnApiCacheStateReader.ReadProjectionFile(ProjectionPath);

        Assert.True(state.Exists);
        Assert.False(state.IsPartial);
        Assert.Equal(181_338, state.ProjectedTaxa);
        Assert.NotNull(state.BuiltAt);
        Assert.Null(state.UnfinishedBuildStartedAt);
    }

    [Fact]
    public void BuildThatStoppedPartWay_IsNotBuilt_AfterAFinishedOne() {
        Build(finish: true);
        Build(finish: false);

        var state = IucnApiCacheStateReader.ReadProjectionFile(ProjectionPath);

        Assert.False(state.Exists);
        Assert.NotNull(state.UnfinishedBuildStartedAt);
        Assert.Equal(0, state.ProjectedTaxa);
    }

    [Fact]
    public void FileWithNoBuildRecord_IsNotBuilt() {
        using (IucnApiProjectionStore.Open(ProjectionPath)) { }

        var state = IucnApiCacheStateReader.ReadProjectionFile(ProjectionPath);

        Assert.False(state.Exists);
        Assert.Null(state.UnfinishedBuildStartedAt);
    }
}
