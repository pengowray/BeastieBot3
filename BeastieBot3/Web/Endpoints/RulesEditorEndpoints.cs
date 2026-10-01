using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Write-capable rule editor (the first mutating endpoints in the web layer). The browser edits rules
// files as RAW TEXT. Saving keeps the edit as a draft (RulesDrafts: only edited files have a draft;
// every other file is read from rules/), and an explicit Apply copies drafts to the SOURCE rules/ tree
// and deletes them. Files are never YAML round-tripped (that would drop comments + custom_groups) —
// only byte-for-byte text. All targets are sandboxed under their root via SafePaths.
//
//   GET  /api/rules/locations                    -> { sourceRulesDir, draftRoot, draftCount, ... }
//   GET  /api/rules-draft/list                    -> every rules file, with hasDraft
//   GET  /api/rules-draft/read?path=              -> the draft if there is one, otherwise the rules/ file
//   POST /api/rules-draft/write {path,content,baseModifiedUtc?} -> save as draft (mtime 409 on conflict)
//   POST /api/rules-draft/revert {path}           -> discard the draft
//   GET  /api/rules/diff                          -> each draft's status (+ unified diff via git --no-index)
//   POST /api/rules/apply {paths[], force?}       -> copy drafts to source, then delete them

public static class RulesEditorEndpoints {
    private const long MaxWriteBytes = 1024 * 1024;
    private static readonly HashSet<string> EditableExtensions =
        new(StringComparer.OrdinalIgnoreCase) { ".yml", ".yaml", ".txt", ".mustache" };

    private static readonly JsonSerializerOptions JsonOpts =
        new(JsonSerializerDefaults.Web) { WriteIndented = false };

    public static void MapRulesEditorEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/rules/locations", (PathsService paths) => {
            var loc = RulesPaths.Resolve(paths);
            return Results.Json(new {
                sourceRulesDir = loc.SourceRulesDir,
                draftRoot = loc.DraftRoot,
                buildOutputRulesDir = loc.BuildOutputRulesDir,
                isBuildOutputFallback = loc.IsBuildOutputFallback,
                draftCount = RulesDrafts.For(loc).DraftFiles().Count,
            }, JsonOpts);
        });

        app.MapGet("/api/rules-draft/list", (PathsService paths) => {
            var loc = RulesPaths.Resolve(paths);
            var drafts = RulesDrafts.For(loc);
            var entries = drafts.AllFiles()
                .Select(rel => new FileInfo(drafts.EffectivePath(rel)!))
                .Select(f => new {
                    path = RelativeOf(drafts, f.FullName),
                    size = f.Length,
                    modified = f.LastWriteTimeUtc,
                    editable = EditableExtensions.Contains(f.Extension),
                    hasDraft = drafts.HasDraft(RelativeOf(drafts, f.FullName)),
                });
            return Results.Json(new { root = loc.SourceRulesDir, entries }, JsonOpts);
        });

        app.MapGet("/api/rules-draft/read", (string path, PathsService paths) => {
            var drafts = RulesDrafts.For(RulesPaths.Resolve(paths));
            var target = drafts.EffectivePath(path);
            if (target is null)
                return Results.BadRequest(new { error = "Path escapes the rules folder." });
            if (!File.Exists(target))
                return Results.NotFound(new { error = $"File not found: {path}" });
            var info = new FileInfo(target);
            if (info.Length > MaxWriteBytes)
                return Results.Json(new { error = $"File too large ({info.Length} bytes)" }, statusCode: 413);
            return Results.Json(new {
                path = path.Replace('\\', '/'),
                size = info.Length,
                modified = info.LastWriteTimeUtc,
                hasDraft = drafts.HasDraft(path),
                content = File.ReadAllText(target),
            }, JsonOpts);
        });

        app.MapPost("/api/rules-draft/write", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<WriteRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "path is required" });
            if (req.Content is null)
                return Results.BadRequest(new { error = "content is required" });

            var drafts = RulesDrafts.For(RulesPaths.Resolve(paths));
            var target = drafts.EffectivePath(req.Path);
            if (target is null)
                return Results.BadRequest(new { error = "Path escapes the rules folder." });
            if (!EditableExtensions.Contains(Path.GetExtension(target)))
                return Results.BadRequest(new { error = $"Only {string.Join(", ", EditableExtensions)} files may be edited." });
            if (Encoding.UTF8.GetByteCount(req.Content) > MaxWriteBytes)
                return Results.Json(new { error = "Content exceeds 1 MB." }, statusCode: 413);

            // Optimistic concurrency: if the client passed the mtime of the text it loaded (the draft, or
            // the rules/ file when there was no draft) and that file changed underneath, refuse so a
            // background edit can't silently clobber.
            if (req.BaseModifiedUtc is { } baseMtime && File.Exists(target)) {
                var current = new FileInfo(target).LastWriteTimeUtc;
                if (Math.Abs((current - baseMtime).TotalSeconds) > 1) {
                    return Results.Json(new {
                        error = "conflict",
                        currentModified = current,
                        content = File.ReadAllText(target),
                    }, statusCode: 409);
                }
            }

            var hasDraft = drafts.Write(req.Path, req.Content);
            var info = new FileInfo(drafts.EffectivePath(req.Path)!);
            return Results.Json(new { path = req.Path.Replace('\\', '/'), size = info.Length, modified = info.LastWriteTimeUtc, hasDraft }, JsonOpts);
        });

        app.MapPost("/api/rules-draft/revert", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<PathRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "path is required" });
            var drafts = RulesDrafts.For(RulesPaths.Resolve(paths));
            if (drafts.EffectivePath(req.Path) is null)
                return Results.BadRequest(new { error = "Path escapes the rules folder." });
            var had = drafts.HasDraft(req.Path);
            drafts.Discard(req.Path);
            return Results.Json(new { path = req.Path.Replace('\\', '/'), discarded = had }, JsonOpts);
        });

        app.MapGet("/api/rules/diff", (PathsService paths) => {
            var loc = RulesPaths.Resolve(paths);
            var drafts = RulesDrafts.For(loc);
            var files = drafts.DraftFiles()
                .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
                .Select(rel => {
                    var status = drafts.StatusOf(rel);
                    var sourcePath = Path.Combine(loc.SourceRulesDir, rel);
                    return new {
                        path = rel.Replace('\\', '/'),
                        status = StatusName(status),
                        diff = status == RulesDrafts.DraftStatus.DraftOnly ? null : GitDiff(sourcePath, Path.Combine(loc.DraftRoot, rel)),
                    };
                })
                .ToList();
            return Results.Json(new {
                sourceRulesDir = loc.SourceRulesDir,
                isBuildOutputFallback = loc.IsBuildOutputFallback,
                files,
            }, JsonOpts);
        });

        app.MapPost("/api/rules/apply", async (HttpContext ctx, PathsService paths) => {
            var req = await JsonSerializer.DeserializeAsync<ApplyRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            var loc = RulesPaths.Resolve(paths);
            if (loc.IsBuildOutputFallback) {
                return Results.Json(new {
                    error = "Project rules folder not found, so no files were copied. Set rules_source_dir under [Dirs] in paths.ini, or the environment variable BEASTIEBOT3_RULES_SOURCE, to the folder's full path, then restart serve.",
                }, statusCode: 409);
            }
            var drafts = RulesDrafts.For(loc);
            var applied = new List<string>();
            var skipped = new List<object>();
            foreach (var rel in req?.Paths ?? Array.Empty<string>()) {
                var reason = drafts.Apply(rel, req?.Force == true);
                if (reason is null) applied.Add(rel.Replace('\\', '/'));
                else skipped.Add(new { path = rel, reason });
            }
            return Results.Json(new { applied, skipped, sourceRulesDir = loc.SourceRulesDir }, JsonOpts);
        });
    }

    // ---- helpers ----

    private static string RelativeOf(RulesDrafts drafts, string fullPath) {
        var root = fullPath.StartsWith(drafts.DraftDir, StringComparison.OrdinalIgnoreCase) ? drafts.DraftDir : drafts.SourceDir;
        return Path.GetRelativePath(root, fullPath).Replace('\\', '/');
    }

    private static string StatusName(RulesDrafts.DraftStatus status) => status switch {
        RulesDrafts.DraftStatus.SourceChanged => "source-changed",
        RulesDrafts.DraftStatus.UnknownBase => "unknown-base",
        RulesDrafts.DraftStatus.DraftOnly => "draft-only",
        _ => "modified",
    };

    // Unified diff via `git diff --no-index` (works outside a repo). Returns null if git is absent.
    private static string? GitDiff(string sourcePath, string draftPath) {
        try {
            var psi = new ProcessStartInfo("git") {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            psi.ArgumentList.Add("diff");
            psi.ArgumentList.Add("--no-index");
            psi.ArgumentList.Add("--no-color");
            psi.ArgumentList.Add("--");
            psi.ArgumentList.Add(sourcePath);
            psi.ArgumentList.Add(draftPath);
            using var proc = Process.Start(psi);
            if (proc is null) return null;
            var outText = proc.StandardOutput.ReadToEnd();
            proc.WaitForExit(5000);
            return string.IsNullOrWhiteSpace(outText) ? null : outText;
        } catch {
            return null; // git not available; status alone still drives the UI
        }
    }

    private sealed class WriteRequest {
        public string? Path { get; set; }
        public string? Content { get; set; }
        public DateTime? BaseModifiedUtc { get; set; }
    }

    private sealed class PathRequest {
        public string? Path { get; set; }
    }

    private sealed class ApplyRequest {
        public string[]? Paths { get; set; }
        // Apply drafts whose rules/ file changed after the draft was started, or whose starting point
        // is unknown. The page sends it only after the user confirms.
        public bool Force { get; set; }
    }
}
