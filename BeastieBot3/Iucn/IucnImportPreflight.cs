using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Configuration;

// Answers "what will `iucn import` actually do if I run it right now?" by looking at the
// configured CSV folder and target database rather than at the command's fixed description.
// Used by the web UI's confirmation dialog so it can say "creates a new database" instead of
// warning about dropping data that isn't there.

namespace BeastieBot3.Iucn;

public sealed record ImportPreflight {
    public required bool Confirm { get; init; }              // is there anything worth stopping the user for
    public required string Headline { get; init; }           // one line: what this run does
    public IReadOnlyList<string> Details { get; init; } = Array.Empty<string>();
    public string? Warning { get; init; }                    // shown last; destructive or blocking
}

public static class IucnImportPreflight {
    public static ImportPreflight Describe(PathsService paths, bool force, bool replaceRelease) {
        var cvsDir = paths.GetIucnCvsDir();
        if (string.IsNullOrWhiteSpace(cvsDir) || !Directory.Exists(cvsDir)) {
            return new ImportPreflight {
                Confirm = false,
                Headline = "Nothing to import.",
                Warning = "The IUCN CSV folder is not set up: check [Datasets] IUCN_CVS_dir in paths.ini.",
            };
        }

        var release = IucnImporter.ExtractRedlistVersionFromPath(cvsDir);
        var zips = Directory.EnumerateFiles(cvsDir, "*.zip", SearchOption.AllDirectories).ToList();
        var zipCount = zips.Count;
        var releaseLabel = string.Equals(release, "unknown", StringComparison.OrdinalIgnoreCase)
            ? "an unnamed release"
            : "release " + release;

        if (zipCount == 0) {
            return new ImportPreflight {
                Confirm = false,
                Headline = "Nothing to import.",
                Details = new[] { "No zip files under " + cvsDir },
            };
        }

        var details = new List<string> {
            $"{zipCount} zip file{(zipCount == 1 ? "" : "s")} in {cvsDir}, read as {releaseLabel}.",
        };

        // A zip whose path reads as some other release stops the run before anything is opened.
        if (!string.Equals(release, "unknown", StringComparison.OrdinalIgnoreCase)) {
            var misread = zips
                .Select(z => IucnImporter.ExtractRedlistVersionFromPath(Path.GetRelativePath(cvsDir, z)))
                .Where(v => !string.Equals(v, "unknown", StringComparison.OrdinalIgnoreCase)
                            && !string.Equals(v, release, StringComparison.OrdinalIgnoreCase))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (misread.Count > 0) {
                return new ImportPreflight {
                    Confirm = false,
                    Headline = "This run will stop without importing anything.",
                    Details = details,
                    Warning = $"Some files read as release {string.Join(", ", misread)} rather than {release}. "
                              + $"Put each download in its own subfolder whose name starts with \"{release}\".",
                };
            }
        }

        var target = ResolveTargetPath(paths, release);
        if (target is null) {
            return new ImportPreflight {
                Confirm = false,
                Headline = "Nothing to import.",
                Details = details,
                Warning = "No database location is configured: set [Datastore] IUCN_sqlite_from_cvs or datastore_dir in paths.ini.",
            };
        }

        var held = ReadCompletedReleases(target);
        var conflict = IucnImporter.FindReleaseConflict(held, release);

        if (!File.Exists(target)) {
            details.Add("Target database: " + target + " (does not exist yet).");
            return new ImportPreflight {
                Confirm = false,
                Headline = "Creates a new database. Nothing existing is changed.",
                Details = details,
            };
        }

        if (conflict is not null) {
            if (replaceRelease && force) {
                details.Add($"Target database: {target}, holding release {conflict}.");
                return new ImportPreflight {
                    Confirm = true,
                    Headline = $"Erases release {conflict} and rebuilds this database as {release}.",
                    Details = details,
                    Warning = $"Everything already imported from release {conflict} is deleted and cannot be recovered without re-importing that release's zips.",
                };
            }

            var switched = Path.Combine(Path.GetDirectoryName(target) ?? "", $"IUCN_{release}.sqlite");
            details.Add($"{target} holds release {conflict} and is left untouched.");
            details.Add("New database instead: " + switched);
            return new ImportPreflight {
                Confirm = false,
                Headline = $"Release {release} is new, so it goes into its own database file.",
                Details = details,
                Warning = "Afterwards, point [Datastore] IUCN_sqlite_from_cvs at the new file and restart the server, or everything else keeps reading release " + conflict + ".",
            };
        }

        var heldLabel = held.Count > 0 ? $", already holding release {held[0]}" : ", currently empty";
        details.Add($"Target database: {target}{heldLabel}.");

        if (force) {
            return new ImportPreflight {
                Confirm = true,
                Headline = $"Re-imports all {zipCount} zip file{(zipCount == 1 ? "" : "s")}, replacing the rows they loaded before.",
                Details = details,
                Warning = held.Count > 0
                    ? "Rows from earlier imports of these same files are deleted and reloaded. Anything imported from other files stays."
                    : null,
            };
        }

        return new ImportPreflight {
            Confirm = false,
            Headline = "Adds these zip files to the database. Ones already imported are skipped.",
            Details = details,
        };
    }

    // The database the import would write to: the configured path, or the release-named default
    // when none is set. Mirrors IucnImportCommand's own resolution.
    private static string? ResolveTargetPath(PathsService paths, string release) =>
        IucnReleaseStateReader.ResolveConfiguredDbPath(
            paths,
            string.Equals(release, "unknown", StringComparison.OrdinalIgnoreCase) ? null : release);

    // Which release(s) a database file already holds, read without opening it for writing.
    // No import_metadata table yet counts as empty.
    internal static IReadOnlyList<string> ReadCompletedReleases(string databasePath) =>
        IucnReleaseStateReader.ReadDatabase(databasePath).Releases;
}
