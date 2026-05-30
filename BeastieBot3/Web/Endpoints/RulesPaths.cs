using System;
using System.IO;
using BeastieBot3.Configuration;

namespace BeastieBot3.Web.Endpoints;

// Resolves the three rules/ locations the web editor cares about:
//
//   SourceRulesDir      - the editable repo copy (what Apply writes back to)
//   DraftRoot           - a writable working copy the browser edits (outside bin/)
//   BuildOutputRulesDir - AppContext.BaseDirectory/rules, the copy the default
//                         `generate-lists` job actually reads (csproj PreserveNewest copy)
//
// The crux (see plan): at runtime everything reads BuildOutputRulesDir, so editing
// it is lost on rebuild and editing source needs a rebuild (or --config/--rules).
// Source resolution order: (1) explicit paths.ini key rules_source_dir, (2) env
// BEASTIEBOT3_RULES_SOURCE, (3) walk up from the build dir for BeastieBot3.csproj +
// rules/, (4) fall back to the build-output copy with IsBuildOutputFallback=true so
// the UI can warn loudly that Apply won't survive a rebuild.

internal sealed record RulesLocations(
    string SourceRulesDir,
    string DraftRoot,
    string BuildOutputRulesDir,
    bool IsBuildOutputFallback);

internal static class RulesPaths {
    public static RulesLocations Resolve(PathsService? paths = null) {
        paths ??= new PathsService();

        var buildOutput = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "rules"));

        var (source, isFallback) = ResolveSource(paths, buildOutput);
        var draft = ResolveDraftRoot(paths, source);

        return new RulesLocations(source, draft, buildOutput, isFallback);
    }

    private static (string Source, bool IsFallback) ResolveSource(PathsService paths, string buildOutput) {
        // (1) explicit config
        var configured = paths.GetRulesSourceDir();
        if (!string.IsNullOrWhiteSpace(configured) && Directory.Exists(configured)) {
            return (Path.GetFullPath(configured), false);
        }

        // (2) environment override
        var env = Environment.GetEnvironmentVariable("BEASTIEBOT3_RULES_SOURCE");
        if (!string.IsNullOrWhiteSpace(env) && Directory.Exists(env)) {
            return (Path.GetFullPath(env), false);
        }

        // (3) walk up from the build dir looking for BeastieBot3.csproj + a rules/ subdir.
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; dir != null && i < 8; i++, dir = dir.Parent) {
            if (File.Exists(Path.Combine(dir.FullName, "BeastieBot3.csproj"))) {
                var rules = Path.Combine(dir.FullName, "rules");
                if (Directory.Exists(rules)) {
                    return (Path.GetFullPath(rules), false);
                }
            }
        }

        // (4) give up: point at the build-output copy and flag it.
        return (buildOutput, true);
    }

    private static string ResolveDraftRoot(PathsService paths, string source) {
        // Prefer a writable location under the datastore dir; never under bin/.
        var datastore = paths.GetDatastoreDir();
        if (!string.IsNullOrWhiteSpace(datastore)) {
            return Path.GetFullPath(Path.Combine(datastore, "rules-draft"));
        }
        // Fallback: a sibling of the source tree (still outside bin/).
        var parent = Directory.GetParent(source)?.FullName ?? AppContext.BaseDirectory;
        return Path.GetFullPath(Path.Combine(parent, "rules-draft"));
    }
}
