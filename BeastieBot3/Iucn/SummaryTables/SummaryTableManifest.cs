using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The list of IUCN summary table files (rules/iucn-summary-tables.yml), and the file names IUCN has
// used for Table 7, for finding the table of a release the list does not have yet.

namespace BeastieBot3.Iucn.SummaryTables;

/// One file. Priority is its place in the list: when two files give one change, the later file wins.
internal sealed record SummaryTableFile(int Table, string Release, string Url, string? ArchiveUrl, string? Note, int Priority) {
    /// The name the file is saved under: the last part of its URL.
    public string FileName => Path.GetFileName(new Uri(Url).AbsolutePath);
}

internal static class SummaryTableManifest {
    public const string FileName = "iucn-summary-tables.yml";

    public static IReadOnlyList<SummaryTableFile> LoadForPaths(PathsService paths) =>
        Load(Path.Combine(RulesPaths.Resolve(paths).SourceRulesDir, FileName));

    public static IReadOnlyList<SummaryTableFile> Load(string path) {
        if (!File.Exists(path)) {
            throw new InvalidOperationException($"The list of IUCN summary tables was not found: {path}");
        }
        return Parse(File.ReadAllText(path), path);
    }

    internal static IReadOnlyList<SummaryTableFile> Parse(string yaml, string source) {
        var deserializer = new DeserializerBuilder().WithNamingConvention(UnderscoredNamingConvention.Instance).IgnoreUnmatchedProperties().Build();
        var document = deserializer.Deserialize<ManifestDocument?>(yaml) ?? new ManifestDocument();
        var files = new List<SummaryTableFile>();
        foreach (var entry in document.Files ?? new List<ManifestEntry>()) {
            if (entry.Table is not (7 or 9) || string.IsNullOrWhiteSpace(entry.Release) || string.IsNullOrWhiteSpace(entry.Url)) {
                throw new InvalidOperationException($"{source}: every file needs table (7 or 9), release and url (entry {files.Count + 1}).");
            }
            files.Add(new SummaryTableFile(entry.Table, entry.Release.Trim(), entry.Url.Trim(),
                string.IsNullOrWhiteSpace(entry.ArchiveUrl) ? null : entry.ArchiveUrl.Trim(),
                string.IsNullOrWhiteSpace(entry.Note) ? null : entry.Note.Trim(), files.Count + 1));
        }
        var duplicate = files.GroupBy(f => f.FileName, StringComparer.OrdinalIgnoreCase).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null) {
            throw new InvalidOperationException($"{source}: two files are named {duplicate.Key}.");
        }
        return files;
    }

    /// The releases that may come after <paramref name="release"/> ("2026-1"): the next one in the
    /// same year and the first one of the next year. IUCN has published up to four in a year.
    public static IEnumerable<string> NextReleases(string release) {
        var parts = release.Split('-');
        if (parts.Length != 2 || !int.TryParse(parts[0], out var year) || !int.TryParse(parts[1], out var number)) yield break;
        yield return $"{year}-{number + 1}";
        yield return $"{year + 1}-1";
    }

    /// The URLs IUCN has used for a release's Table 7, newest naming first.
    public static IEnumerable<string> Table7Urls(string release) {
        const string folder = "https://nc.iucnredlist.org/redlist/content/attachment_files/";
        yield return $"{folder}{release}_RL_Table7.pdf";
        yield return $"{folder}{release}_RL_Table_7.pdf";
    }

    private sealed class ManifestDocument {
        public List<ManifestEntry>? Files { get; set; }
    }

    private sealed class ManifestEntry {
        public int Table { get; set; }
        public string? Release { get; set; }
        public string? Url { get; set; }
        public string? ArchiveUrl { get; set; }
        public string? Note { get; set; }
    }
}
