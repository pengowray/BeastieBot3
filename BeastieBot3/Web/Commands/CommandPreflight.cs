using BeastieBot3.Configuration;
using BeastieBot3.Iucn;

namespace BeastieBot3.Web.Commands;

// Decides whether the web UI asks for confirmation before a run, and what the confirmation says.
// The rule is written above CommandKind in CommandClassification.cs. The web UI asks the server
// (/api/commands/preflight) before every run it starts, from the Run command page and from the
// Workflows command buttons, and asks the user only when Confirm is true.
//
// Three ways a command is decided:
//   - Mutates and ReadOnly commands: never asked about.
//   - Destructive commands: asked about when the run has one of CommandInfo.ConfirmWhen, or
//     always when ConfirmWhen is empty. Pure: reads only the attribute and the options.
//   - Commands in FilePreflights: decided from the configured files, so the dialog can say what
//     is really there ("creates a new database" rather than a warning about data that does not
//     exist). Only iucn import so far.
//
// An option counts under any name the command declares for it: with "-f|--force" declared,
// ConfirmWhen = { "--force" } also matches a run given "-f" (CommandReflector.AllNamesOf).

public sealed record CommandPreflightResult {
    public required bool Confirm { get; init; }             // ask the user before this run
    public required string Headline { get; init; }          // one line: what this run does or deletes
    public IReadOnlyList<string> Details { get; init; } = Array.Empty<string>();
    public string? Warning { get; init; }                   // shown last
    // Options the web UI adds to the run once the user has confirmed: the command's PromptOption,
    // so the command does not stop at its own terminal prompt.
    public IReadOnlyList<string> AddArgs { get; init; } = Array.Empty<string>();
}

public static class CommandPreflight {
    // Each preflight gets `has`, which says whether the run has an option under any of its names.
    private static readonly IReadOnlyDictionary<string, Func<PathsService, Func<string, bool>, CommandPreflightResult>> FilePreflights =
        new Dictionary<string, Func<PathsService, Func<string, bool>, CommandPreflightResult>>(StringComparer.Ordinal) {
            ["iucn import"] = (paths, has) => FromImport(IucnImportPreflight.Describe(
                paths,
                force: has("--force"),
                replaceRelease: has("--replace-release"))),
        };

    public static bool HasFilePreflight(string path) => FilePreflights.ContainsKey(path);

    // How the command's confirmation is decided, served with /api/commands so the web UI can
    // label the Run button before the preflight answers: "never", "always", "options" (only runs
    // with one of ConfirmWhen) or "files" (depends on the configured files).
    public static string ConfirmMode(CommandInfoAttribute info) {
        if (HasFilePreflight(info.Path)) return "files";
        if (info.Kind != CommandKind.Destructive) return "never";
        return info.ConfirmWhen.Length == 0 ? "always" : "options";
    }

    // Null means no confirmation and nothing to show before the run.
    public static CommandPreflightResult? Describe(RegisteredCommand cmd, IReadOnlyList<string> argv, PathsService paths) =>
        FilePreflights.TryGetValue(cmd.Path, out var preflight)
            ? preflight(paths, OptionLookup(cmd.Type, argv))
            : Decide(cmd, argv);

    // The decision for every command without a file preflight.
    public static CommandPreflightResult? Decide(RegisteredCommand cmd, IReadOnlyList<string> argv) {
        var info = cmd.Info;
        if (info.Kind != CommandKind.Destructive) {
            return null;
        }
        var has = OptionLookup(cmd.Type, argv);
        if (info.ConfirmWhen.Length > 0 && !info.ConfirmWhen.Any(has)) {
            return null;
        }
        var addArgs = info.PromptOption is { } prompt && !has(prompt)
            ? new[] { prompt }
            : Array.Empty<string>();
        return new CommandPreflightResult {
            Confirm = true,
            Headline = info.ConfirmText ?? info.Reason ?? "This run deletes downloaded or imported data.",
            AddArgs = addArgs,
        };
    }

    // Whether the run has the option under any name the command declares for it.
    internal static Func<string, bool> OptionLookup(Type commandType, IReadOnlyList<string> argv) =>
        option => CommandReflector.AllNamesOf(commandType, option).Any(name => HasOption(argv, name));

    // "--force" or "--force=true". The web UI sends a ticked flag as the bare option.
    internal static bool HasOption(IReadOnlyList<string> argv, string option) =>
        argv.Any(a => string.Equals(a, option, StringComparison.Ordinal)
                      || a.StartsWith(option + "=", StringComparison.Ordinal));

    private static CommandPreflightResult FromImport(ImportPreflight pre) => new() {
        Confirm = pre.Confirm,
        Headline = pre.Headline,
        Details = pre.Details,
        Warning = pre.Warning,
    };
}
