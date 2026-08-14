using BeastieBot3.Iucn;

namespace BeastieBot3.Web.Flows;

// Turns the on-disk state of a route into a step status the workflow page can show.
//
// Without this, a step done by hand can only ever say "you do this one yourself", and a step
// with a command can only say when that command last ran — which is not the same question.
// `iucn import` running three minutes ago says nothing about whether the release downloaded
// since then went in, and a database holding the previous release looks perfectly healthy.
//
// Pure functions over a state record so the decisions can be pinned by tests.

public sealed record FlowProbeResult(string Status, string? Detail);

public static class FlowStepProbes {
    // Probe keys, referenced from FlowCatalogue steps.
    public const string IucnCsvDownload = "iucn-csv-download";
    public const string IucnCsvImport = "iucn-csv-import";
    public const string IucnCsvRepoint = "iucn-csv-repoint";

    public static bool IsIucnCsvProbe(string probe) =>
        probe is IucnCsvDownload or IucnCsvImport or IucnCsvRepoint;

    // Null = this probe has nothing to say; the caller falls back to its usual status.
    public static FlowProbeResult? Evaluate(string probe, IucnReleaseState state) => probe switch {
        IucnCsvDownload => Download(state),
        IucnCsvImport => Import(state),
        IucnCsvRepoint => Repoint(state),
        _ => null,
    };

    // Are the release's zip files where the import will look for them?
    internal static FlowProbeResult Download(IucnReleaseState s) {
        if (s.InputDir is null) {
            return new FlowProbeResult("todo", "No download folder set: add [Datasets] IUCN_CVS_dir to paths.ini.");
        }
        if (!s.InputDirExists) {
            return new FlowProbeResult("todo", $"Download folder not found: {s.InputDir}");
        }
        if (s.Zips.Count == 0) {
            return new FlowProbeResult("todo", $"No zip files in {s.InputDir}");
        }
        if (s.InputRelease is null) {
            return new FlowProbeResult("todo",
                $"Nothing in the folder name says which release these {Zips(s.Zips.Count)} are. "
                + $"Rename {s.InputDir} to something like IUCN_CVS_2026-1.");
        }
        if (s.MisreadZips.Count > 0) {
            var n = s.MisreadZips.Count;
            return new FlowProbeResult("todo",
                $"The import will stop: {(n == 1 ? "1 zip file here reads" : $"{n} zip files here read")} "
                + $"as release {s.MisreadRelease}, not {s.InputRelease}. "
                + $"Each download needs its own subfolder whose name starts with {s.InputRelease}.");
        }
        return new FlowProbeResult("ok", $"Release {s.InputRelease} · {Zips(s.Zips.Count)} in {s.InputDir}");
    }

    // Is this release in a database — and all of it, not just the first of the two downloads?
    internal static FlowProbeResult? Import(IucnReleaseState s) {
        // Nothing to measure against: no release to name, or nothing downloaded yet.
        if (s.InputRelease is null || s.Zips.Count == 0) return null;

        var holding = s.HoldingDb;
        if (holding is null) {
            var blocker = s.ConfiguredDb?.HeldRelease;
            return new FlowProbeResult("todo", blocker is null
                ? $"Release {s.InputRelease} is not imported yet."
                : $"Release {s.InputRelease} is not imported. {s.ConfiguredDb!.FileName} holds release {blocker}.");
        }

        var imported = s.ImportedZipCount(holding);
        if (imported < s.Zips.Count) {
            return new FlowProbeResult("todo",
                $"Only part of release {s.InputRelease} is imported: {holding.FileName} has {imported} of its "
                + $"{s.Zips.Count} zip files. Re-run to add the rest.");
        }

        return new FlowProbeResult("ok",
            $"Release {s.InputRelease} is in {holding.FileName} · {imported} of {s.Zips.Count} zip files imported");
    }

    // Does everything else actually read the database this release went into? True only once
    // paths.ini has been edited AND serve restarted, because paths.ini is read at startup only.
    internal static FlowProbeResult? Repoint(IucnReleaseState s) {
        if (s.InputRelease is null) return null;

        var holding = s.HoldingDb;
        if (holding is null) return null;   // nothing imported to point at yet

        if (s.ConfiguredDb is not null && s.ConfiguredDb.Holds(s.InputRelease)) {
            return new FlowProbeResult("ok",
                $"paths.ini already points at {holding.FileName}, which holds release {s.InputRelease}.");
        }

        var stale = s.ConfiguredDb?.HeldRelease;
        var staleName = s.ConfiguredDb?.FileName ?? "the old database";
        return new FlowProbeResult("todo",
            $"Release {s.InputRelease} went into {holding.FileName}, but everything still reads {staleName}"
            + (stale is null ? "." : $" (release {stale}).")
            + " Point [Datastore] IUCN_sqlite_from_cvs at the new file and restart serve.");
    }

    private static string Zips(int count) => count == 1 ? "1 zip file" : $"{count} zip files";
}
