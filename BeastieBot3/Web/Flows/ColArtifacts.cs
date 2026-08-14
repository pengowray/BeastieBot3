using System;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;

namespace BeastieBot3.Web.Flows;

// When each thing built from Catalogue of Life was last written. Compared against the point the
// current CoL release became the one being read, this answers the question the CoL flow's notes
// could previously only warn about in prose: which outputs are still on the previous release.
//
// One file (or one folder's newest file) per artifact, so the timestamp means what it says.

public sealed record ColArtifacts {
    public DateTime? CommonNamesModified { get; init; }
    public DateTime? AuditSiteModified { get; init; }
    public DateTime? WikipediaListsModified { get; init; }

    public static ColArtifacts Read(PathsService paths) => new() {
        CommonNamesModified = FileTime(Try(() => paths.GetCommonNameStorePath())),
        AuditSiteModified = FileTime(AuditSiteIndex(paths)),
        WikipediaListsModified = NewestIn(Try(() => paths.GetWikipediaOutputDirectory()), "*.wikitext"),
    };

    // The audit site's entry page: the generator rewrites it every run, so its timestamp is the
    // site's. The reports folder as a whole would pick up any unrelated report.
    private static string? AuditSiteIndex(PathsService paths) {
        var reports = Try(() => paths.GetReportOutputDirectory());
        if (string.IsNullOrWhiteSpace(reports) || !Directory.Exists(reports)) return null;
        try {
            var newest = new DirectoryInfo(reports)
                .EnumerateDirectories("redlist-audit-*", SearchOption.TopDirectoryOnly)
                .Select(d => Path.Combine(d.FullName, "index.html"))
                .Where(File.Exists)
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();
            return newest;
        } catch {
            return null;
        }
    }

    private static string? Try(Func<string?> get) {
        try { return get(); } catch { return null; }
    }

    private static DateTime? FileTime(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return null;
        try {
            var full = Path.GetFullPath(path);
            return File.Exists(full) ? File.GetLastWriteTimeUtc(full) : null;
        } catch {
            return null;
        }
    }

    private static DateTime? NewestIn(string? dir, string pattern) {
        if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) return null;
        try {
            var times = new DirectoryInfo(dir)
                .EnumerateFiles(pattern, SearchOption.TopDirectoryOnly)
                .Select(f => f.LastWriteTimeUtc)
                .ToList();
            return times.Count == 0 ? null : times.Max();
        } catch {
            return null;
        }
    }
}
