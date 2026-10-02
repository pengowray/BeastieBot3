using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

// Where the downloaded copies of GBIF's IUCN Red List checklist live and how they are named:
// iucn-checklist-<yyyy-MM-dd>.zip, dated from the server's Last-Modified header. When a different
// file already has that name, the new copy gets "-2", "-3" and so on before ".zip".

namespace BeastieBot3.Iucn.Gbif;

internal static class GbifIucnChecklistFiles {
    /// GBIF's hosted copy of the Darwin Core Archive IUCN publishes (dataset 19491596-35ae-4a91-9a98-85cf505f1bd3).
    public const string DownloadUrl = "https://hosted-datasets.gbif.org/datasets/iucn/iucn-latest.zip";

    public const string DatasetPageUrl = "https://www.gbif.org/dataset/19491596-35ae-4a91-9a98-85cf505f1bd3";

    /// The dataset's DOI, cited as https://doi.org/10.15468/0qnb58.
    public const string DatasetDoi = "10.15468/0qnb58";

    public const string UserAgent = "BeastieBot3/1.0 (+https://en.wikipedia.org/wiki/User:Beastie_Bot)";

    private static readonly Regex FileNamePattern = new(
        @"^iucn-checklist-(?<date>\d{4}-\d{2}-\d{2})(?:-(?<copy>\d+))?\.zip$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static string FileNameFor(DateOnly date, int copy = 1) =>
        copy <= 1
            ? $"iucn-checklist-{date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}.zip"
            : $"iucn-checklist-{date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}-{copy}.zip";

    /// <summary>
    /// The date and copy number in a name like "iucn-checklist-2026-07-28.zip" (copy 1) or
    /// "iucn-checklist-2026-07-28-2.zip" (copy 2). Null for any other name.
    /// </summary>
    public static (DateOnly Date, int Copy)? ParseFileName(string path) {
        var match = FileNamePattern.Match(Path.GetFileName(path));
        if (!match.Success
            || !DateOnly.TryParseExact(match.Groups["date"].Value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)) {
            return null;
        }
        var copy = match.Groups["copy"].Success
            && int.TryParse(match.Groups["copy"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var n)
            ? n
            : 1;
        return (date, copy);
    }

    /// <summary>
    /// The newest iucn-checklist-*.zip in <paramref name="directory"/>: latest date in the name,
    /// then highest copy number, then latest modification time. Null when the folder has none.
    /// </summary>
    public static string? FindNewest(string? directory) {
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory)) {
            return null;
        }
        return Directory.EnumerateFiles(directory, "iucn-checklist-*.zip")
            .Select(path => (Path: path, Name: ParseFileName(path)))
            .Where(f => f.Name is not null)
            .OrderByDescending(f => f.Name!.Value.Date)
            .ThenByDescending(f => f.Name!.Value.Copy)
            .ThenByDescending(f => File.GetLastWriteTimeUtc(f.Path))
            .Select(f => f.Path)
            .FirstOrDefault();
    }

    /// <summary>
    /// The path to save a checklist dated <paramref name="date"/> under: the plain name, or the
    /// first "-N" copy name that no file in the folder has yet.
    /// </summary>
    public static string ChooseTargetPath(string directory, DateOnly date) {
        for (var copy = 1; ; copy++) {
            var path = Path.Combine(directory, FileNameFor(date, copy));
            if (!File.Exists(path)) {
                return path;
            }
        }
    }

    /// Lower-case hex SHA-256 of a file's bytes.
    public static string Sha256(string path) {
        using var stream = File.OpenRead(path);
        return Convert.ToHexStringLower(SHA256.HashData(stream));
    }
}
