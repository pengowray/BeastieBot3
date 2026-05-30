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

// Write-capable rule editor (the first mutating endpoints in the web layer). The browser edits a
// DRAFT working copy as RAW TEXT; an explicit Apply step copies changed files back to the SOURCE
// rules/ tree. Files are never YAML round-tripped (that would drop comments + custom_groups) — only
// byte-for-byte text. All targets are sandboxed under the draft root via SafePaths.
//
//   GET  /api/rules/locations                    -> { sourceRulesDir, draftRoot, buildOutputRulesDir, isBuildOutputFallback }
//   POST /api/rules/draft/init                    -> (re)seed the draft from source
//   GET  /api/rules-draft/list                    -> recursive draft file listing
//   GET  /api/rules-draft/read?path=              -> raw text of one draft file
//   POST /api/rules-draft/write {path,content,baseModifiedUtc?} -> save (mtime 409 on conflict)
//   POST /api/rules-draft/revert {path}           -> restore one file from source
//   GET  /api/rules/diff                          -> per-file status (+ unified diff via git --no-index)
//   POST /api/rules/apply {paths[]}               -> copy approved draft files to source

public static class RulesEditorEndpoints {
    private const long MaxWriteBytes = 1024 * 1024;
    private static readonly HashSet<string> EditableExtensions =
        new(StringComparer.OrdinalIgnoreCase) { ".yml", ".yaml", ".txt", ".mustache" };

    private static readonly JsonSerializerOptions JsonOpts =
        new(JsonSerializerDefaults.Web) { WriteIndented = false };

    public static void MapRulesEditorEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/rules/locations", () => {
            var loc = RulesPaths.Resolve();
            return Results.Json(new {
                sourceRulesDir = loc.SourceRulesDir,
                draftRoot = loc.DraftRoot,
                buildOutputRulesDir = loc.BuildOutputRulesDir,
                isBuildOutputFallback = loc.IsBuildOutputFallback,
                draftSeeded = Directory.Exists(loc.DraftRoot) && Directory.EnumerateFiles(loc.DraftRoot, "*", SearchOption.AllDirectories).Any(),
            }, JsonOpts);
        });

        app.MapPost("/api/rules/draft/init", () => {
            var loc = RulesPaths.Resolve();
            var copied = SeedDraft(loc, overwrite: true);
            return Results.Json(new { draftRoot = loc.DraftRoot, filesCopied = copied }, JsonOpts);
        });

        app.MapGet("/api/rules-draft/list", () => {
            var loc = RulesPaths.Resolve();
            EnsureSeeded(loc);
            if (!Directory.Exists(loc.DraftRoot)) {
                return Results.Json(new { root = loc.DraftRoot, entries = Array.Empty<object>() }, JsonOpts);
            }
            var entries = Directory.EnumerateFiles(loc.DraftRoot, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfo(f))
                .OrderBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
                .Select(f => new {
                    path = Path.GetRelativePath(loc.DraftRoot, f.FullName).Replace('\\', '/'),
                    size = f.Length,
                    modified = f.LastWriteTimeUtc,
                    editable = EditableExtensions.Contains(f.Extension),
                });
            return Results.Json(new { root = loc.DraftRoot, entries }, JsonOpts);
        });

        app.MapGet("/api/rules-draft/read", (string path) => {
            var loc = RulesPaths.Resolve();
            EnsureSeeded(loc);
            if (!SafePaths.TryResolveUnder(loc.DraftRoot, path, out var target, out var err))
                return Results.BadRequest(new { error = err });
            if (!File.Exists(target))
                return Results.NotFound(new { error = $"File not found: {path}" });
            var info = new FileInfo(target);
            if (info.Length > MaxWriteBytes)
                return Results.Json(new { error = $"File too large ({info.Length} bytes)" }, statusCode: 413);
            return Results.Json(new {
                path = path.Replace('\\', '/'),
                size = info.Length,
                modified = info.LastWriteTimeUtc,
                content = File.ReadAllText(target),
            }, JsonOpts);
        });

        app.MapPost("/api/rules-draft/write", async (HttpContext ctx) => {
            var req = await JsonSerializer.DeserializeAsync<WriteRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "path is required" });
            if (req.Content is null)
                return Results.BadRequest(new { error = "content is required" });

            var loc = RulesPaths.Resolve();
            EnsureSeeded(loc);
            if (!SafePaths.TryResolveUnder(loc.DraftRoot, req.Path, out var target, out var err))
                return Results.BadRequest(new { error = err });
            if (!EditableExtensions.Contains(Path.GetExtension(target)))
                return Results.BadRequest(new { error = $"Only {string.Join(", ", EditableExtensions)} files may be edited." });
            if (Encoding.UTF8.GetByteCount(req.Content) > MaxWriteBytes)
                return Results.Json(new { error = "Content exceeds 1 MB." }, statusCode: 413);

            // Optimistic concurrency: if the client passed the mtime it loaded and the file changed
            // underneath, refuse so a background poll/edit can't silently clobber.
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

            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            File.WriteAllText(target, req.Content);
            var info = new FileInfo(target);
            return Results.Json(new { path = req.Path.Replace('\\', '/'), size = info.Length, modified = info.LastWriteTimeUtc }, JsonOpts);
        });

        app.MapPost("/api/rules-draft/revert", async (HttpContext ctx) => {
            var req = await JsonSerializer.DeserializeAsync<PathRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            if (req is null || string.IsNullOrWhiteSpace(req.Path))
                return Results.BadRequest(new { error = "path is required" });
            var loc = RulesPaths.Resolve();
            if (!SafePaths.TryResolveUnder(loc.DraftRoot, req.Path, out var draftTarget, out var err))
                return Results.BadRequest(new { error = err });
            if (!SafePaths.TryResolveUnder(loc.SourceRulesDir, req.Path, out var sourceTarget, out err))
                return Results.BadRequest(new { error = err });
            if (!File.Exists(sourceTarget))
                return Results.NotFound(new { error = $"No source file to revert from: {req.Path}" });
            Directory.CreateDirectory(Path.GetDirectoryName(draftTarget)!);
            File.Copy(sourceTarget, draftTarget, overwrite: true);
            return Results.Json(new { path = req.Path.Replace('\\', '/'), reverted = true }, JsonOpts);
        });

        app.MapGet("/api/rules/diff", () => {
            var loc = RulesPaths.Resolve();
            EnsureSeeded(loc);
            var files = new List<object>();
            foreach (var draftFile in EnumerateRelative(loc.DraftRoot)) {
                var draftPath = Path.Combine(loc.DraftRoot, draftFile);
                var sourcePath = Path.Combine(loc.SourceRulesDir, draftFile);
                var status = ComputeStatus(sourcePath, draftPath);
                files.Add(new {
                    path = draftFile.Replace('\\', '/'),
                    status,
                    diff = status == "modified" ? GitDiff(sourcePath, draftPath) : null,
                });
            }
            return Results.Json(new {
                sourceRulesDir = loc.SourceRulesDir,
                isBuildOutputFallback = loc.IsBuildOutputFallback,
                files = files.OrderBy(f => ((dynamic)f).status == "unchanged" ? 1 : 0),
            }, JsonOpts);
        });

        app.MapPost("/api/rules/apply", async (HttpContext ctx) => {
            var req = await JsonSerializer.DeserializeAsync<ApplyRequest>(ctx.Request.Body, JsonOpts).ConfigureAwait(false);
            var loc = RulesPaths.Resolve();
            if (loc.IsBuildOutputFallback) {
                return Results.Json(new {
                    error = "Refusing to apply: the source rules directory could not be located (it resolved to the build-output copy). Set [Dirs] rules_source_dir in paths.ini or BEASTIEBOT3_RULES_SOURCE.",
                }, statusCode: 409);
            }
            var paths = req?.Paths ?? Array.Empty<string>();
            var applied = new List<string>();
            var skipped = new List<object>();
            foreach (var rel in paths) {
                if (!SafePaths.TryResolveUnder(loc.DraftRoot, rel, out var draftTarget, out var err1)) {
                    skipped.Add(new { path = rel, reason = err1 });
                    continue;
                }
                if (!SafePaths.TryResolveUnder(loc.SourceRulesDir, rel, out var sourceTarget, out var err2)) {
                    skipped.Add(new { path = rel, reason = err2 });
                    continue;
                }
                if (!File.Exists(draftTarget)) {
                    skipped.Add(new { path = rel, reason = "draft file missing" });
                    continue;
                }
                Directory.CreateDirectory(Path.GetDirectoryName(sourceTarget)!);
                File.Copy(draftTarget, sourceTarget, overwrite: true); // byte-for-byte; comments + custom_groups preserved
                applied.Add(rel.Replace('\\', '/'));
            }
            return Results.Json(new { applied, skipped, sourceRulesDir = loc.SourceRulesDir }, JsonOpts);
        });
    }

    // ---- helpers ----

    private static void EnsureSeeded(RulesLocations loc) {
        if (Directory.Exists(loc.DraftRoot) && Directory.EnumerateFiles(loc.DraftRoot, "*", SearchOption.AllDirectories).Any())
            return;
        SeedDraft(loc, overwrite: false);
    }

    private static int SeedDraft(RulesLocations loc, bool overwrite) {
        if (!Directory.Exists(loc.SourceRulesDir)) return 0;
        var copied = 0;
        foreach (var rel in EnumerateRelative(loc.SourceRulesDir)) {
            var src = Path.Combine(loc.SourceRulesDir, rel);
            var dst = Path.Combine(loc.DraftRoot, rel);
            if (!overwrite && File.Exists(dst)) continue;
            Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
            File.Copy(src, dst, overwrite: true);
            copied++;
        }
        return copied;
    }

    private static IEnumerable<string> EnumerateRelative(string root) {
        if (!Directory.Exists(root)) yield break;
        foreach (var f in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)) {
            yield return Path.GetRelativePath(root, f);
        }
    }

    private static string ComputeStatus(string sourcePath, string draftPath) {
        var sourceExists = File.Exists(sourcePath);
        if (!sourceExists) return "draft-only";
        // Compare with normalized line endings so cosmetic CRLF/LF differences don't read as changes.
        var s = File.ReadAllText(sourcePath).Replace("\r\n", "\n");
        var d = File.ReadAllText(draftPath).Replace("\r\n", "\n");
        return string.Equals(s, d, StringComparison.Ordinal) ? "unchanged" : "modified";
    }

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
    }
}
