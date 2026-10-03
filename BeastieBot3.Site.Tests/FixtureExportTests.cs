namespace BeastieBot3.Site.Tests;

/// Writes a copy of the fixture database for the browser check in browser/live-update.mjs, which
/// runs the site on it. Does nothing unless SITE_FIXTURE_DB_OUT names the file to write:
///   SITE_FIXTURE_DB_OUT=/tmp/site-fixture.sqlite dotnet test BeastieBot3.Site.Tests --filter FixtureExport
public sealed class FixtureExportTests {
    public const string OutVariable = "SITE_FIXTURE_DB_OUT";

    [Fact]
    public void WritesTheFixtureDatabaseWhenAsked() {
        var target = Environment.GetEnvironmentVariable(OutVariable);
        if (string.IsNullOrWhiteSpace(target)) {
            return;
        }
        var directory = Path.GetDirectoryName(Path.GetFullPath(target));
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        File.Copy(FixtureDb.Path, target, overwrite: true);
        Assert.True(new FileInfo(target).Length > 0);
    }
}
