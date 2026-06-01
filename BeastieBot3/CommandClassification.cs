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

public enum CommandKind {
    ReadOnly,     // pure query or report (output files are fine; not state)
    Mutates,      // additive cache/db write; idempotent on re-run
    Destructive,  // wholesale rewrite or deletion of state; UI re-confirms
}

// Finer-grained "what happens if I run this (again)?" classification, orthogonal
// to CommandKind. Surfaced in the web UI so a user can tell, before clicking,
// whether a run is cheap-and-safe, will discover new work, rebuilds a derived
// artifact, or should target a fresh database. Unset (= default) is derived from
// CommandKind in RegisteredCommand.Rerun so most commands need no annotation.
public enum RerunEffect {
    Default,        // unspecified — derive from CommandKind
    ReadOnly,       // reads/produces reports only; never changes cached state
    IdempotentAdd,  // additive: skips entries already present, only fetches/adds new (safe, cheap re-run)
    Discovers,      // scans an external source to find entries not yet known locally
    Rebuilds,       // recomputes/replaces a derived artifact from data already held locally
    ClearsCache,    // deletes cached payloads in place (a queue/seed is kept; next fetch re-downloads)
    FreshDataset,   // establishes or replaces a dataset; for a NEW release, target a fresh database file
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

    // Human-readable note shown alongside Destructive commands ("Rewrites X tables...").
    public string? Reason { get; init; }

    // What happens on (re-)run. Default is derived from Kind (see RegisteredCommand.Rerun).
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
