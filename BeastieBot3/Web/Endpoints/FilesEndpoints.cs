using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// File listing + read endpoints for the Workflows page.
//
//   GET /api/files/roots                              -> list of safe roots
//   GET /api/files/list?root=<r>&subdir=<s>           -> directory listing
//   GET /api/files/read?root=<r>&path=<p>             -> file content (text)
//
// Each request resolves the target path under a named safe root and rejects
// anything that escapes that root via "..". The set of allowed roots is
// fixed at startup:
//
//   "rules"            -> AppContext.BaseDirectory/rules
//   "reports"          -> paths.GetReportOutputDirectory()
//   "wikipedia-output" -> paths.GetWikipediaOutputDirectory()
//
// Reading is text-only and capped at 1 MB. Binary content is rejected with
// 415 so the browser doesn't accidentally try to render a SQLite blob.

public static class FilesEndpoints {
    private const long MaxReadBytes = 1024 * 1024;

    public static void MapFilesEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/files/roots", () => {
            var roots = ResolveRoots();
            return Results.Json(roots.Select(kv => new {
                root = kv.Key,
                path = kv.Value,
                exists = Directory.Exists(kv.Value),
            }));
        });

        app.MapGet("/api/files/list", (string root, string? subdir) => {
            if (!TryResolveRoot(root, out var rootPath, out var err))
                return Results.BadRequest(new { error = err });
            if (!TryResolveTarget(rootPath, subdir ?? "", out var target, out err))
                return Results.BadRequest(new { error = err });
            if (!Directory.Exists(target))
                return Results.NotFound(new { error = $"Directory not found: {subdir}" });

            var entries = new DirectoryInfo(target)
                .EnumerateFileSystemInfos()
                .OrderBy(e => e is DirectoryInfo ? 0 : 1)
                .ThenBy(e => e.Name, StringComparer.OrdinalIgnoreCase)
                .Select(e => new {
                    name = e.Name,
                    kind = e is DirectoryInfo ? "directory" : "file",
                    size = e is FileInfo fi ? (long?)fi.Length : null,
                    modified = e.LastWriteTimeUtc,
                    path = Path.GetRelativePath(rootPath, e.FullName).Replace('\\', '/'),
                });
            return Results.Json(new { root, subdir = subdir ?? "", entries });
        });

        app.MapGet("/api/files/read", (string root, string path) => {
            if (!TryResolveRoot(root, out var rootPath, out var err))
                return Results.BadRequest(new { error = err });
            if (!TryResolveTarget(rootPath, path, out var target, out err))
                return Results.BadRequest(new { error = err });
            if (!File.Exists(target))
                return Results.NotFound(new { error = $"File not found: {path}" });

            var info = new FileInfo(target);
            if (info.Length > MaxReadBytes) {
                return Results.Json(new { error = $"File too large ({info.Length} bytes); max {MaxReadBytes}" }, statusCode: 413);
            }
            try {
                var text = File.ReadAllText(target);
                return Results.Json(new {
                    root,
                    path = path.Replace('\\', '/'),
                    size = info.Length,
                    modified = info.LastWriteTimeUtc,
                    content = text,
                });
            } catch (Exception ex) {
                return Results.Json(new { error = ex.Message }, statusCode: 500);
            }
        });
    }

    private static Dictionary<string, string> ResolveRoots() {
        var paths = new PathsService();
        var rules = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "rules"));
        var roots = new Dictionary<string, string>(StringComparer.Ordinal) {
            ["rules"] = rules,
        };
        if (paths.GetReportOutputDirectory() is { Length: > 0 } reports) {
            roots["reports"] = Path.GetFullPath(reports);
        }
        if (paths.GetWikipediaOutputDirectory() is { Length: > 0 } wiki) {
            roots["wikipedia-output"] = Path.GetFullPath(wiki);
        }
        return roots;
    }

    private static bool TryResolveRoot(string root, out string rootPath, out string error) {
        rootPath = "";
        error = "";
        var roots = ResolveRoots();
        if (!roots.TryGetValue(root, out var r)) {
            error = $"Unknown root '{root}'. Allowed: {string.Join(", ", roots.Keys)}";
            return false;
        }
        rootPath = r;
        return true;
    }

    private static bool TryResolveTarget(string rootPath, string relative, out string fullPath, out string error) {
        fullPath = "";
        error = "";
        var combined = Path.Combine(rootPath, relative);
        var resolved = Path.GetFullPath(combined);
        // Path-traversal guard: resolved must sit under rootPath.
        var rootWithSep = rootPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                          + Path.DirectorySeparatorChar;
        if (!resolved.StartsWith(rootWithSep, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(resolved, rootPath, StringComparison.OrdinalIgnoreCase)) {
            error = "Path escapes its root.";
            return false;
        }
        fullPath = resolved;
        return true;
    }
}
