using System;
using System.IO;
using BeastieBot3.Configuration;

namespace BeastieBot3.Tests;

// Pins the "path is not configured" errors: each names the paths.ini key and the ini file, and
// names a command-line option only when the caller says which one it has. The IUCN error used to
// say "pass --database" for every caller, including commands whose option is --iucn-db,
// --iucn-database or --source-db, and web endpoints that have no option at all.
public class PathsNotConfiguredTests : IDisposable {
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("beastiebot-paths-test-");

    public void Dispose() => _dir.Delete(recursive: true);

    // No ini file in an empty temp directory, so nothing is configured whatever the machine has.
    private PathsService EmptyPaths() => new("paths-not-present.ini", _dir.FullName);

    [Fact]
    public void IucnDatabase_WithoutOptionName_NamesTheIniKeyAndNoOption() {
        var paths = EmptyPaths();

        var ex = Assert.Throws<InvalidOperationException>(() => paths.ResolveIucnDatabasePath(null));

        Assert.Contains("Datastore:IUCN_sqlite_from_cvs", ex.Message);
        Assert.Contains(paths.SourceFilePath, ex.Message);
        Assert.DoesNotContain("--", ex.Message);
        Assert.DoesNotContain("[", ex.Message);
    }

    [Theory]
    [InlineData("--iucn-db")]
    [InlineData("--source-db")]
    public void IucnDatabase_WithOptionName_NamesThatOption(string option) {
        var ex = Assert.Throws<InvalidOperationException>(() => EmptyPaths().ResolveIucnDatabasePath(null, option));

        Assert.Contains($"or pass {option} <PATH>.", ex.Message);
        Assert.DoesNotContain("--database", ex.Message);
    }

    [Fact]
    public void IucnDatabase_OverridePath_IsUsedWithoutConfiguration() {
        var file = Path.Combine(_dir.FullName, "iucn.sqlite");

        Assert.Equal(file, EmptyPaths().ResolveIucnDatabasePath(file, "--iucn-db"));
    }

    [Fact]
    public void CommonNameStore_WithOptionName_NamesThatOption() {
        var paths = EmptyPaths();

        var ex = Assert.Throws<InvalidOperationException>(() => paths.ResolveCommonNameStorePath(null, "--common-names-db"));

        Assert.Contains("Datastore:common_names_sqlite", ex.Message);
        Assert.Contains(paths.SourceFilePath, ex.Message);
        Assert.Contains("or pass --common-names-db <PATH>.", ex.Message);
    }

    [Fact]
    public void CommonNameStore_WithoutOptionName_NamesNoOption() {
        // `sprat generate-lists` resolves the store with no option of its own, and its --database
        // is the SPRAT database, so the error must not suggest it.
        var ex = Assert.Throws<InvalidOperationException>(() => EmptyPaths().ResolveCommonNameStorePath(null));

        Assert.DoesNotContain("--", ex.Message);
    }
}
