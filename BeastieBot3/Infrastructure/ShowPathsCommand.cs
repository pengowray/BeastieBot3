using System;
using System.IO;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;
using System.Threading;

// Diagnostic command to display resolved paths from paths.ini configuration.
// Shows all [Datastore] and [Dirs] values with resolved absolute paths.
// Useful for debugging path resolution issues across platforms.
// Run via: show-paths

namespace BeastieBot3.Infrastructure;
    [CommandInfo("show-paths", CommandKind.ReadOnly,
        "Show all key/value pairs from the settings INI file.",
        Examples = new[] { "show-paths", "show-paths --settings-dir /config" })]
    public sealed class ShowPathsCommand : Command<CommonSettings> {
        public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken) {
            var paths = settings.CreatePaths();

            AnsiConsole.MarkupLine($"[grey]Reading paths from:[/] {paths.SourceFilePath}");
            var all = paths.GetAll();
            if (all.Count == 0) {
                AnsiConsole.MarkupLine("[yellow]No values found.[/]");
                return 0;
            }

            var table = new Table().AddColumns("Key", "Value");
            foreach (var kv in all) table.AddRow(kv.Key, kv.Value);
            AnsiConsole.Write(table);
            return 0;
        }
    }
