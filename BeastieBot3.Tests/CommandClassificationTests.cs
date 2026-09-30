using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using BeastieBot3.Web.Commands;

namespace BeastieBot3.Tests;

// Pins the command classification the web UI reads: every mutating command states its re-run
// effect, a command's kind and effect agree, every confirmation option is a real option, and the
// confirmation rule written above CommandKind gives the answers it promises for the commands it
// names (CommandPreflight.Decide, which the web UI follows before every run).
public class CommandClassificationTests {
    private static RegisteredCommand Command(string path) =>
        CommandRegistry.FindByPath(path) ?? throw new InvalidOperationException("No command " + path);

    private static CommandPreflightResult? Decide(string path, params string[] argv) =>
        CommandPreflight.Decide(Command(path).Info, argv);

    // ---- every command ----

    [Fact]
    public void EveryMutatingCommand_SetsItsRerunEffect() {
        var missing = CommandRegistry.All
            .Where(c => c.Kind != CommandKind.ReadOnly && c.Info.Rerun == RerunEffect.Default)
            .Select(c => c.Path)
            .ToList();
        Assert.True(missing.Count == 0, "Set Rerun on: " + string.Join(", ", missing));
    }

    [Fact]
    public void ReadOnlyKind_AndReadOnlyEffect_GoTogether() {
        var mismatched = CommandRegistry.All
            .Where(c => (c.Kind == CommandKind.ReadOnly) != (c.Rerun == RerunEffect.ReadOnly))
            .Select(c => $"{c.Path} ({c.Kind}, {c.Rerun})")
            .ToList();
        Assert.True(mismatched.Count == 0, "Kind and Rerun disagree: " + string.Join(", ", mismatched));
    }

    [Fact]
    public void ConfirmationSettings_AreOnlyOnDestructiveCommands() {
        var misplaced = CommandRegistry.All
            .Where(c => c.Kind != CommandKind.Destructive
                        && (c.Info.ConfirmWhen.Length > 0 || c.Info.ConfirmText is not null || c.Info.PromptOption is not null))
            .Select(c => c.Path)
            .ToList();
        Assert.True(misplaced.Count == 0, "ConfirmWhen, ConfirmText and PromptOption do nothing on: " + string.Join(", ", misplaced));
    }

    [Fact]
    public void ConfirmationOptions_AreOptionsTheCommandHas() {
        foreach (var c in CommandRegistry.All) {
            var names = CommandReflector.BuildSchema(c.Type).Fields
                .SelectMany(f => f.AltNames.Prepend(f.Name))
                .ToHashSet(StringComparer.Ordinal);
            foreach (var option in c.Info.ConfirmWhen.Append(c.Info.PromptOption).OfType<string>()) {
                Assert.True(names.Contains(option), $"{c.Path} has no option {option}");
            }
        }
    }

    [Fact]
    public void EveryDestructiveCommand_SaysWhatItDeletes() {
        var silent = CommandRegistry.All
            .Where(c => c.Kind == CommandKind.Destructive
                        && !CommandPreflight.HasFilePreflight(c.Path)
                        && string.IsNullOrWhiteSpace(c.Info.ConfirmText ?? c.Info.Reason))
            .Select(c => c.Path)
            .ToList();
        Assert.True(silent.Count == 0, "Give ConfirmText or Reason to: " + string.Join(", ", silent));
    }

    [Fact]
    public void EveryRerunEffect_HasAnEntryInTheWebUi() {
        var appJs = File.ReadAllText(FindRepoFile("BeastieBot3/Web/wwwroot/app.js"));
        var effects = Regex.Match(appJs, @"const EFFECTS = \{(?<body>.*?)\n  \};", RegexOptions.Singleline);
        Assert.True(effects.Success, "EFFECTS table not found in app.js");
        foreach (var effect in Enum.GetValues<RerunEffect>().Where(e => e != RerunEffect.Default)) {
            var key = effect.ToString().ToLowerInvariant();
            Assert.Matches(new Regex(@"^\s*" + key + @":", RegexOptions.Multiline), effects.Groups["body"].Value);
        }
    }

    // ---- the rule, for the commands it names ----

    [Fact]
    public void MutatingCommands_NeverAsk() {
        Assert.Null(Decide("wikipedia prune-queue", "--apply"));
        Assert.Null(Decide("common-names aggregate", "--source", "col", "--replace"));
        Assert.Null(Decide("iucn api cache-assessments", "--force"));
        Assert.Null(Decide("iucn api project-view"));
        Assert.Equal("never", CommandPreflight.ConfirmMode(Command("wikipedia prune-queue").Info));
    }

    [Fact]
    public void ResetCache_AlwaysAsks_AndAddsForceAfterTheUserConfirms() {
        var pre = Decide("wikidata reset-cache");
        Assert.NotNull(pre);
        Assert.True(pre!.Confirm);
        Assert.Equal(new[] { "--force" }, pre.AddArgs);
        Assert.Contains("Deletes all downloaded Wikidata item data", pre.Headline);
        Assert.Equal("always", CommandPreflight.ConfirmMode(Command("wikidata reset-cache").Info));
    }

    [Fact]
    public void ResetCache_WithForceTicked_DoesNotAddItTwice() {
        var pre = Decide("wikidata reset-cache", "--force");
        Assert.True(pre!.Confirm);
        Assert.Empty(pre.AddArgs);
    }

    [Theory]
    [InlineData("col import")]
    [InlineData("sprat import")]
    public void DatasetImports_AskOnlyWithForce(string path) {
        Assert.Null(Decide(path));
        var pre = Decide(path, "--force");
        Assert.True(pre!.Confirm);
        Assert.Empty(pre.AddArgs);
        Assert.Equal(Command(path).Info.ConfirmText, pre.Headline);
        Assert.Equal("options", CommandPreflight.ConfirmMode(Command(path).Info));
    }

    [Fact]
    public void IucnImport_IsDecidedFromTheFiles() =>
        Assert.Equal("files", CommandPreflight.ConfirmMode(Command("iucn import").Info));

    [Theory]
    [InlineData("--force", true)]
    [InlineData("--force=true", true)]
    [InlineData("--forced", false)]
    [InlineData("--no-force", false)]
    public void HasOption_MatchesTheOptionOnly(string arg, bool expected) =>
        Assert.Equal(expected, CommandPreflight.HasOption(new[] { "--limit", "5", arg }, "--force"));

    private static string FindRepoFile(string relative) {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent) {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate)) return candidate;
        }
        throw new FileNotFoundException("Not found above the test output folder: " + relative);
    }
}
