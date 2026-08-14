using System;
using System.Collections.Generic;
using BeastieBot3.Iucn;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// Pins the workflow page's IUCN CSV step lights. The bug these replace: a step done by hand
// could only ever say "you do this yourself", and a step with a command could only say when
// that command last ran — so new zips in the folder changed nothing, and repointing paths.ini
// plus restarting changed nothing. Each probe now answers a question about the release itself.
public class FlowStepProbeTests {
    private const string Release = "2026-1";
    private static readonly string[] TwoZips = { "2026-1 non-passerines/a.zip", "2026-1 passerines/b.zip" };

    private static IucnDatabaseState Db(string path, string? release = null, params string[] zips) =>
        new() {
            Path = path,
            Exists = true,
            Releases = release is null ? Array.Empty<string>() : new[] { release },
            ImportedZips = zips,
        };

    private static IucnReleaseState State(
        string? release = Release,
        IReadOnlyList<string>? zips = null,
        IucnDatabaseState? configured = null,
        IucnDatabaseState? releaseDb = null,
        IReadOnlyList<string>? misread = null,
        string? misreadRelease = null,
        bool dirExists = true,
        string? dir = @"D:\datasets\IUCN_CVS_2026-1") =>
        new() {
            InputDir = dir,
            InputDirExists = dirExists,
            InputRelease = release,
            Zips = zips ?? TwoZips,
            MisreadZips = misread ?? Array.Empty<string>(),
            MisreadRelease = misreadRelease,
            ConfiguredDb = configured,
            ReleaseDb = releaseDb,
        };

    // ---- download ----

    [Fact]
    public void Download_NoFolderConfigured_IsTodo() {
        var r = FlowStepProbes.Download(State(dir: null, dirExists: false));
        Assert.Equal("todo", r.Status);
        Assert.Contains("IUCN_CVS_dir", r.Detail);
    }

    [Fact]
    public void Download_FolderMissing_IsTodo() =>
        Assert.Equal("todo", FlowStepProbes.Download(State(dirExists: false)).Status);

    [Fact]
    public void Download_NoZips_IsTodo() {
        var r = FlowStepProbes.Download(State(zips: Array.Empty<string>()));
        Assert.Equal("todo", r.Status);
        Assert.Contains("No zip files", r.Detail);
    }

    // The folder name is the only place the release version comes from — IUCN's export carries
    // it nowhere inside the zip — so an unnamed folder is a problem, not a pass.
    [Fact]
    public void Download_ZipsButNoReleaseInFolderName_IsTodo() {
        var r = FlowStepProbes.Download(State(release: null));
        Assert.Equal("todo", r.Status);
        Assert.Contains("which release", r.Detail);
    }

    // IUCN's random zip filenames contain digit pairs the version regex matches; the import
    // stops on this, so the step must show it rather than read as done.
    [Fact]
    public void Download_ZipReadingAsAnotherRelease_IsTodo() {
        var r = FlowStepProbes.Download(State(misread: new[] { "redlist_1373-414.zip" }, misreadRelease: "1373-414"));
        Assert.Equal("todo", r.Status);
        Assert.Contains("1373-414", r.Detail);
    }

    [Fact]
    public void Download_ReleaseZipsPresent_IsOk() {
        var r = FlowStepProbes.Download(State());
        Assert.Equal("ok", r.Status);
        Assert.Contains("2026-1", r.Detail);
        Assert.Contains("2 zip files", r.Detail);
    }

    // ---- import ----

    [Fact]
    public void Import_NothingDownloaded_SaysNothing() =>
        Assert.Null(FlowStepProbes.Import(State(zips: Array.Empty<string>())));

    // The case that made the old light lie: the command ran (for the previous release), but this
    // release is not in.
    [Fact]
    public void Import_ConfiguredDbHoldsAnotherRelease_IsTodo() {
        var r = FlowStepProbes.Import(State(configured: Db("IUCN_2025-2.sqlite", "2025-2")));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("2025-2", r.Detail);
    }

    [Fact]
    public void Import_NothingImportedYet_IsTodo() {
        var r = FlowStepProbes.Import(State(configured: Db("IUCN_2026-1.sqlite")));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("not imported", r.Detail);
    }

    // The release is always two downloads (passerines and everything else). One zip in is not done.
    [Fact]
    public void Import_OnlyOneOfTwoZipsIn_IsTodo() {
        var r = FlowStepProbes.Import(State(configured: Db("IUCN_2026-1.sqlite", Release, TwoZips[0])));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("1 of its 2 zip files", r.Detail);
    }

    [Fact]
    public void Import_AllZipsIn_IsOk() {
        var r = FlowStepProbes.Import(State(configured: Db("IUCN_2026-1.sqlite", Release, TwoZips)));
        Assert.Equal("ok", r!.Status);
        Assert.Contains("IUCN_2026-1.sqlite", r.Detail);
    }

    // A new release goes into its own file beside the configured one — the import is done even
    // though paths.ini has not caught up.
    [Fact]
    public void Import_IntoTheReleaseNamedFile_IsOk() {
        var r = FlowStepProbes.Import(State(
            configured: Db("IUCN_2025-2.sqlite", "2025-2"),
            releaseDb: Db("IUCN_2026-1.sqlite", Release, TwoZips)));
        Assert.Equal("ok", r!.Status);
        Assert.Contains("IUCN_2026-1.sqlite", r.Detail);
    }

    // ---- repoint ----

    [Fact]
    public void Repoint_NothingImported_SaysNothing() =>
        Assert.Null(FlowStepProbes.Repoint(State(configured: Db("IUCN_2025-2.sqlite", "2025-2"))));

    // paths.ini is read once at startup, so the configured database only reads as the new release
    // after both the edit and the restart — which is exactly what this step asks for.
    [Fact]
    public void Repoint_ConfiguredDbHoldsTheRelease_IsOk() {
        var r = FlowStepProbes.Repoint(State(configured: Db("IUCN_2026-1.sqlite", Release, TwoZips)));
        Assert.Equal("ok", r!.Status);
        Assert.Contains("already points at", r.Detail);
    }

    [Fact]
    public void Repoint_ReleaseInAnotherFile_IsTodo() {
        var r = FlowStepProbes.Repoint(State(
            configured: Db("IUCN_2025-2.sqlite", "2025-2"),
            releaseDb: Db("IUCN_2026-1.sqlite", Release, TwoZips)));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("IUCN_2026-1.sqlite", r.Detail);
        Assert.Contains("IUCN_2025-2.sqlite", r.Detail);
    }

    // ---- state helpers ----

    [Fact]
    public void HoldingDb_PrefersTheConfiguredFile() {
        var s = State(
            configured: Db("IUCN_2026-1.sqlite", Release, TwoZips),
            releaseDb: Db("copy.sqlite", Release, TwoZips));
        Assert.Equal("IUCN_2026-1.sqlite", s.HoldingDb!.Path);
    }

    [Fact]
    public void ImportedZipCount_CountsOnlyThisFoldersZips() {
        var s = State(configured: Db("db.sqlite", Release, TwoZips[0], "2025-2 old/c.zip"));
        Assert.Equal(1, s.ImportedZipCount(s.ConfiguredDb));
    }

    [Fact]
    public void ReleaseDbPath_IsNullWhenItWouldBeTheConfiguredFile() =>
        Assert.Null(IucnReleaseStateReader.ResolveReleaseDbPath(@"D:\store\IUCN_2026-1.sqlite", "2026-1"));

    [Fact]
    public void ReleaseDbPath_SitsBesideTheConfiguredFile() =>
        Assert.Equal(
            System.IO.Path.Combine(@"D:\store", "IUCN_2026-1.sqlite"),
            IucnReleaseStateReader.ResolveReleaseDbPath(@"D:\store\IUCN_2025-2.sqlite", "2026-1"));
}
