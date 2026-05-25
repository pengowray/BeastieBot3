using System.Reflection;
using System.Text;
using Spectre.Console.Cli;

namespace BeastieBot3.Web.Commands;

// Assembly-scan registry. The set of CLI commands is whatever has a
// [CommandInfo] attribute in this assembly — no hand-maintained list.
// Used in two places:
//   1. Spectre configuration (Program.cs.BuildApp -> CommandRegistry.ConfigureAll)
//   2. Web /api/commands endpoint (CommandsEndpoints)

public sealed record RegisteredCommand(Type Type, CommandInfoAttribute Info) {
    public string Path => Info.Path;
    public CommandKind Kind => Info.Kind;
    public string Description => Info.Description;
    public string? Reason => Info.Reason;
    public IReadOnlyList<string> Examples => Info.Examples;
    public string[] PathSegments => Info.Path.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    public string CommandName => PathSegments[^1];
    public string Branch {
        get {
            var segs = PathSegments;
            return segs.Length <= 1 ? "" : string.Join(' ', segs.Take(segs.Length - 1));
        }
    }
}

public static class CommandRegistry {
    private static readonly Lazy<IReadOnlyList<RegisteredCommand>> _all = new(LoadAll);

    public static IReadOnlyList<RegisteredCommand> All => _all.Value;

    public static RegisteredCommand? FindByPath(string path) =>
        All.FirstOrDefault(c => c.Path == path);

    private static IReadOnlyList<RegisteredCommand> LoadAll() {
        var asm = typeof(CommandRegistry).Assembly;
        var found = asm.GetTypes()
            .Where(t => !t.IsAbstract && !t.IsInterface)
            .Select(t => (Type: t, Info: t.GetCustomAttribute<CommandInfoAttribute>()))
            .Where(x => x.Info is not null)
            .Select(x => new RegisteredCommand(x.Type, x.Info!))
            .OrderBy(c => c.Path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        // Sanity check at startup: catch duplicate paths or empty segments before
        // they manifest as confusing Spectre errors at first command invocation.
        var dupes = found.GroupBy(c => c.Path).Where(g => g.Count() > 1).ToList();
        if (dupes.Count > 0) {
            throw new InvalidOperationException(
                "Duplicate command paths: " +
                string.Join(", ", dupes.Select(g => $"'{g.Key}' ({g.Count()})")));
        }
        foreach (var c in found) {
            if (c.PathSegments.Length == 0 || c.PathSegments.Any(string.IsNullOrWhiteSpace)) {
                throw new InvalidOperationException(
                    $"Command {c.Type.FullName} has invalid path '{c.Path}'.");
            }
        }
        return found;
    }

    // Branch descriptions come from [assembly: CommandBranch(path, description)]
    // declarations (top of CommandClassification.cs). Implicit branches with
    // no attribute are silently skipped — the --help listing shows them
    // without descriptions, same as if they had been left out of the dict.
    private static readonly IReadOnlyDictionary<string, string> BranchDescriptions = LoadBranchDescriptions();

    private static IReadOnlyDictionary<string, string> LoadBranchDescriptions() {
        return typeof(CommandRegistry).Assembly
            .GetCustomAttributes(typeof(CommandBranchAttribute), inherit: false)
            .Cast<CommandBranchAttribute>()
            .ToDictionary(a => a.Path, a => a.Description, StringComparer.Ordinal);
    }

    // Configure the Spectre.Console.Cli command tree from the scanned attributes.
    // The branch structure is implicit in each command's path: every prefix
    // segment becomes a branch in the tree.
    public static void ConfigureAll(IConfigurator config) {
        var root = BuildTree(All);
        foreach (var (name, node) in root.Children) {
            if (node.Command is not null) {
                ConfigureLeaf(config, node.Command, name);
            } else {
                config.AddBranch<CommandSettings>(name, branch => {
                    ApplyBranchDescription(branch, name);
                    ConfigureBranch(branch, node, branchPath: name);
                });
            }
        }
    }

    private static void ConfigureBranch(IConfigurator<CommandSettings> config, TreeNode node, string branchPath) {
        foreach (var (name, child) in node.Children) {
            if (child.Command is not null) {
                ConfigureLeafInBranch(config, child.Command, name);
            } else {
                var childPath = branchPath + " " + name;
                config.AddBranch<CommandSettings>(name, sub => {
                    ApplyBranchDescription(sub, childPath);
                    ConfigureBranch(sub, child, childPath);
                });
            }
        }
    }

    private static void ApplyBranchDescription(IConfigurator<CommandSettings> branch, string branchPath) {
        if (BranchDescriptions.TryGetValue(branchPath, out var desc)) {
            branch.SetDescription(desc);
        }
    }

    // Root-level command (e.g. "show-paths"). Uses the non-generic IConfigurator
    // AddCommand<T> method via reflection because T is only known at runtime.
    private static void ConfigureLeaf(IConfigurator config, RegisteredCommand cmd, string name) {
        var method = ConfiguratorAddCommandMethod.MakeGenericMethod(cmd.Type);
        var configurator = (ICommandConfigurator)method.Invoke(config, new object[] { name })!;
        ApplyMetadata(configurator, cmd);
    }

    private static void ConfigureLeafInBranch(IConfigurator<CommandSettings> config, RegisteredCommand cmd, string name) {
        var method = ConfiguratorGenericAddCommandMethod.MakeGenericMethod(cmd.Type);
        var configurator = (ICommandConfigurator)method.Invoke(config, new object[] { name })!;
        ApplyMetadata(configurator, cmd);
    }

    private static void ApplyMetadata(ICommandConfigurator configurator, RegisteredCommand cmd) {
        configurator.WithDescription(cmd.Description);
        foreach (var ex in cmd.Examples) {
            var tokens = ParseShellTokens(ex);
            if (tokens.Length > 0) configurator.WithExample(tokens);
        }
    }

    private static readonly MethodInfo ConfiguratorAddCommandMethod =
        typeof(IConfigurator).GetMethods()
            .Single(m => m.Name == "AddCommand" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1);

    private static readonly MethodInfo ConfiguratorGenericAddCommandMethod =
        typeof(IConfigurator<CommandSettings>).GetMethods()
            .Single(m => m.Name == "AddCommand" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1);

    // Internal tree representation used to drive Spectre's nested-lambda config API.
    private sealed class TreeNode {
        public Dictionary<string, TreeNode> Children { get; } = new(StringComparer.Ordinal);
        public RegisteredCommand? Command { get; set; }
    }

    private static TreeNode BuildTree(IEnumerable<RegisteredCommand> commands) {
        var root = new TreeNode();
        foreach (var cmd in commands) {
            var segs = cmd.PathSegments;
            var node = root;
            for (int i = 0; i < segs.Length - 1; i++) {
                if (!node.Children.TryGetValue(segs[i], out var next)) {
                    next = new TreeNode();
                    node.Children[segs[i]] = next;
                }
                node = next;
            }
            var leafName = segs[^1];
            if (node.Children.TryGetValue(leafName, out var existing)) {
                if (existing.Command is not null) {
                    throw new InvalidOperationException(
                        $"Two commands claim path '{cmd.Path}': {existing.Command.Type.FullName} and {cmd.Type.FullName}.");
                }
                // A branch already exists with this name; promote it to also hold
                // the command. (Not currently exercised, but tolerated.)
                existing.Command = cmd;
            } else {
                node.Children[leafName] = new TreeNode { Command = cmd };
            }
        }
        return root;
    }

    // Minimal shell-like tokenizer for example strings:
    //   "wikipedia fetch-pages --title \"Ursus maritimus\""
    // Splits on spaces; respects double-quoted runs as single tokens.
    public static string[] ParseShellTokens(string s) {
        var tokens = new List<string>();
        var current = new StringBuilder();
        bool inQuote = false;
        for (int i = 0; i < s.Length; i++) {
            var c = s[i];
            if (c == '"') { inQuote = !inQuote; continue; }
            if (c == ' ' && !inQuote) {
                if (current.Length > 0) { tokens.Add(current.ToString()); current.Clear(); }
            } else {
                current.Append(c);
            }
        }
        if (current.Length > 0) tokens.Add(current.ToString());
        return tokens.ToArray();
    }
}
