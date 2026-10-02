using BeastieBot3.Iucn.Gbif;

namespace BeastieBot3.Tests.Gbif;

// Pins how downloaded checklist zips are named and which one counts as the newest.
public sealed class GbifIucnChecklistFilesTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "gbif-iucn-tests-" + Guid.NewGuid().ToString("N"));

    public GbifIucnChecklistFilesTests() => Directory.CreateDirectory(_dir);

    public void Dispose() {
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private string Touch(string name, DateTime? modifiedUtc = null) {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, name);
        if (modifiedUtc is { } time) {
            File.SetLastWriteTimeUtc(path, time);
        }
        return path;
    }

    [Fact]
    public void FileNameFor_FirstCopy_HasNoNumber() {
        Assert.Equal("iucn-checklist-2026-07-28.zip", GbifIucnChecklistFiles.FileNameFor(new DateOnly(2026, 7, 28)));
        Assert.Equal("iucn-checklist-2026-07-28-2.zip", GbifIucnChecklistFiles.FileNameFor(new DateOnly(2026, 7, 28), 2));
    }

    [Theory]
    [InlineData("iucn-checklist-2026-07-28.zip", 2026, 7, 28, 1)]
    [InlineData("/some/folder/iucn-checklist-2025-11-01-3.zip", 2025, 11, 1, 3)]
    public void ParseFileName_ReadsDateAndCopy(string name, int year, int month, int day, int copy) =>
        Assert.Equal((new DateOnly(year, month, day), copy), GbifIucnChecklistFiles.ParseFileName(name));

    [Theory]
    [InlineData("iucn-latest.zip")]
    [InlineData("iucn-checklist-2026-13-01.zip")]
    [InlineData("iucn-checklist-2026-07-28.zip.part")]
    [InlineData("iucn-checklist-download.zip.part")]
    public void ParseFileName_OtherNames_GiveNull(string name) =>
        Assert.Null(GbifIucnChecklistFiles.ParseFileName(name));

    [Fact]
    public void FindNewest_TakesTheLatestDate_ThenTheHighestCopy() {
        var now = DateTime.UtcNow;
        Touch("iucn-checklist-2025-11-01.zip", now);
        Touch("iucn-checklist-2026-07-28.zip", now.AddDays(-2));
        var second = Touch("iucn-checklist-2026-07-28-2.zip", now.AddDays(-3));
        Touch("iucn-latest.zip", now.AddDays(1));
        Touch("iucn-checklist-download.zip.part", now.AddDays(1));

        Assert.Equal(second, GbifIucnChecklistFiles.FindNewest(_dir));
        Assert.Equal(second, GbifIucnChecklistReader.FindNewest(_dir));
    }

    [Fact]
    public void FindNewest_FolderWithNoChecklist_GivesNull() {
        Touch("iucn-latest.zip");
        Assert.Null(GbifIucnChecklistFiles.FindNewest(_dir));
        Assert.Null(GbifIucnChecklistFiles.FindNewest(Path.Combine(_dir, "missing")));
        Assert.Null(GbifIucnChecklistFiles.FindNewest(null));
    }

    [Fact]
    public void ChooseTargetPath_AddsACopyNumberWhenTheNameIsTaken() {
        var date = new DateOnly(2026, 7, 28);
        Assert.Equal(Path.Combine(_dir, "iucn-checklist-2026-07-28.zip"), GbifIucnChecklistFiles.ChooseTargetPath(_dir, date));

        Touch("iucn-checklist-2026-07-28.zip");
        Touch("iucn-checklist-2026-07-28-2.zip");
        Assert.Equal(Path.Combine(_dir, "iucn-checklist-2026-07-28-3.zip"), GbifIucnChecklistFiles.ChooseTargetPath(_dir, date));
    }

    [Fact]
    public void Sha256_SameBytesGiveTheSameHash() {
        var a = Touch("a.zip");
        var b = Path.Combine(_dir, "b.zip");
        File.Copy(a, b);
        var c = Touch("c.zip");

        Assert.Equal(GbifIucnChecklistFiles.Sha256(a), GbifIucnChecklistFiles.Sha256(b));
        Assert.NotEqual(GbifIucnChecklistFiles.Sha256(a), GbifIucnChecklistFiles.Sha256(c));
        Assert.Equal(64, GbifIucnChecklistFiles.Sha256(a).Length);
    }
}
