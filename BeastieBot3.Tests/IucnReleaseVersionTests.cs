using BeastieBot3.Iucn;

namespace BeastieBot3.Tests;

// Pins how a release version ("2026-1") is read out of a path. IUCN names each downloaded zip
// redlist_species_data_<uuid>.zip, and the uuid's hex groups contain number pairs like "1373-414";
// a loose \d{4}-\d+ pattern read one of those as the release, so two downloads of the SAME release
// got different bogus versions and the one-release-per-DB gate refused the second one.
public class IucnReleaseVersionTests {
    [Theory]
    // The real download filename from the bug report: the uuid must yield nothing.
    [InlineData("redlist_species_data_581ed650-1373-414a-a785-081f3250d4b3.zip", "unknown")]
    // A uuid whose second group is a plausible year followed by a digit — only stripping uuids
    // outright catches this one; the tightened version regex alone would match "2011-4".
    [InlineData("redlist_species_data_a1b2c3d4-2011-4abc-8def-0123456789ab.zip", "unknown")]
    // The documented on-disk convention: version-first subfolder, uuid-named zip inside it.
    [InlineData("2026-1 non-passerines/redlist_species_data_581ed650-1373-414a-a785-081f3250d4b3.zip", "2026-1")]
    [InlineData("2025-2 all species but LC and DD - Oct 24 2025/export.zip", "2025-2")]
    // The CSV-directory hint shape (an absolute path, version embedded in the folder name).
    [InlineData(@"D:\datasets\IUCN_CVS_2026-1", "2026-1")]
    [InlineData(@"D:\datasets\IUCN_CVS_2026-1\2026-1 passerines\export.zip", "2026-1")]
    // Two-digit release numbers stay valid.
    [InlineData("2026-12 everything/export.zip", "2026-12")]
    // An ISO date is not a release: "2025-10-24" must not read as release 2025-10.
    [InlineData("backup 2025-10-24/export.zip", "unknown")]
    // Nor is a year range.
    [InlineData("2019-2020 archive/export.zip", "unknown")]
    // No version anywhere: the caller falls back to its own hint.
    [InlineData("downloads/export.zip", "unknown")]
    [InlineData("", "unknown")]
    public void ExtractRedlistVersionFromPath_ReadsOnlyPlausibleReleases(string path, string expected) =>
        Assert.Equal(expected, IucnImporter.ExtractRedlistVersionFromPath(path));
}
