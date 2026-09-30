using System;
using BeastieBot3;

// Branch descriptions are assembly-level attributes because branches are
// implicit — they have no class to attach to. Keeping the declaration here,
// next to the per-command [CommandInfo] attribute, gives the whole command
// tree (branches + commands) a single discovery point.
[assembly: CommandBranch("col",          "Catalogue of Life related commands")]
[assembly: CommandBranch("iucn",         "IUCN Red List dataset commands")]
[assembly: CommandBranch("iucn api",     "Commands that cache data from the live IUCN API")]
[assembly: CommandBranch("wikidata",     "Wikidata caching and reporting commands")]
[assembly: CommandBranch("wikipedia",    "Wikipedia caching and inspection commands")]
[assembly: CommandBranch("common-names", "Common name disambiguation and reporting commands")]
[assembly: CommandBranch("sprat",        "Australian SPRAT (EPBC threatened species) dataset commands")]
[assembly: CommandBranch("redlist",      "Unofficial IUCN Red List data-observation site generation")]

namespace BeastieBot3;

// Single source of truth for every CLI command. The attribute drives:
//   - Spectre.Console.Cli configuration (path, description, examples) — see
//     `CommandRegistry.ConfigureAll` which scans for [CommandInfo] and
//     builds the entire branch tree at startup.
//   - The web UI catalogue (kind, reason, description) — served via
//     `/api/commands` and rendered as the command browser.
//
// Branch structure is encoded in `Path` (space-separated): for example
//   "iucn import"                -> root "iucn" branch, "import" command
//   "iucn api cache-taxa"        -> nested "iucn" > "api" branch
//   "show-paths"                 -> top-level command (no branch)

// When the web UI asks for confirmation. It asks before a run only when that run, with the
// options chosen, deletes data that took downloads or an import to build:
//   - it deletes downloaded data and does not download it again (wikidata reset-cache), or
//   - it deletes an imported dataset in order to import it again (iucn import, col import and
//     sprat import with --force).
// It does not ask before a run that only reports, adds, downloads again over copies already
// downloaded (the --force of the download commands: each old copy stays until its new copy
// arrives), rebuilds something from data already stored locally (the IUCN API projection, the
// common names store, the Wikidata name index), or deletes only entries that no command can use
// (wikipedia prune-queue --apply, which wikipedia update runs every time).
//
// A command whose runs can meet that rule is Destructive. CommandInfo.ConfirmWhen names the
// options that make a run meet it; with none named, every run asks. A command with a preflight
// (Web/Commands/CommandPreflight.cs, only iucn import so far) decides from the files on disk
// instead. The decision is made on the server, in CommandPreflight, and the web UI only follows it.
public enum CommandKind {
    ReadOnly,     // reads data and writes reports or output files only; changes no cache or database
    Mutates,      // writes caches or databases; no run deletes downloaded or imported data
    Destructive,  // some runs delete downloaded or imported data; the web UI asks before those runs
}

// "What happens if I run this (again)?", orthogonal to CommandKind. The web UI shows it as a pill
// on the Run command page and beside Workflows command buttons, with a hover hint (app.js EFFECTS)
// that has to be true for every command in the category; RerunNote adds command-specific detail.
// Every Mutates or Destructive command sets it explicitly (pinned by CommandClassificationTests).
// A ReadOnly command may leave it as Default, which means ReadOnly.
public enum RerunEffect {
    Default,        // unset: allowed only on ReadOnly commands, where it means ReadOnly
    ReadOnly,       // reads data and writes reports or output files; changes no cache or database
    IdempotentAdd,  // by default adds only what is missing and keeps what is already there (downloads, queues, upserts)
    Discovers,      // searches the IUCN Red List API or Wikidata for records not cached yet; adds them or queues them
    Rebuilds,       // rebuilds its result from data already stored locally and replaces the previous result
    PlansDownloads, // changes only which records the download commands fetch next; downloads and deletes no downloaded data
    ClearsCache,    // deletes downloaded data from a cache; the next download run fetches it again
    Imports,        // imports downloaded files into a database; a re-run skips what is imported, --force imports it again
}

// Describes a CLI branch (intermediate node in the path tree). One per
// branch path, declared as an assembly-level attribute at the top of this
// file because branches don't have a class to attach to.
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
public sealed class CommandBranchAttribute : Attribute {
    public string Path { get; }
    public string Description { get; }
    public CommandBranchAttribute(string path, string description) {
        Path = path;
        Description = description;
    }
}

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class CommandInfoAttribute : Attribute {
    // Full branch-prefixed command name, space-separated.
    public string Path { get; }
    public CommandKind Kind { get; }
    public string Description { get; }

    // Shown with a warning sign in the web UI's form for a Destructive command: which runs delete
    // what. Not shown for other kinds.
    public string? Reason { get; init; }

    // Destructive commands only: the options that make a run delete downloaded or imported data
    // (see the rule above CommandKind). The web UI asks for confirmation only when the run has
    // one of them. Left empty, every run asks. Not used for a command with its own preflight.
    public string[] ConfirmWhen { get; init; } = Array.Empty<string>();

    // Destructive commands only: the first line of the confirmation, saying what this run
    // deletes. Defaults to Reason.
    public string? ConfirmText { get; init; }

    // The option that skips the command's own terminal prompt (wikidata reset-cache --force).
    // A web UI job cannot answer a terminal prompt, so once the user confirms in the web UI's
    // dialog, the web UI adds this option to the run.
    public string? PromptOption { get; init; }

    // What happens on (re-)run. Must be set on Mutates and Destructive commands; Default means
    // ReadOnly (see RegisteredCommand.Rerun).
    public RerunEffect Rerun { get; init; } = RerunEffect.Default;

    // Optional one-line specific about the re-run effect (e.g. "--force re-downloads
    // everything already cached"). Surfaced beside the effect hint in the web UI.
    public string? RerunNote { get; init; }

    // CLI usage examples. Each string is one example command line; the shell-quote
    // tokenizer in `CommandRegistry.ParseShellTokens` understands `"quoted values"`.
    public string[] Examples { get; init; } = Array.Empty<string>();

    public CommandInfoAttribute(string path, CommandKind kind, string description) {
        Path = path;
        Kind = kind;
        Description = description;
    }
}
