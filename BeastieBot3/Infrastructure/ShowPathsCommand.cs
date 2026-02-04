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
    public sealed class ShowPathsCommand : Command<CommonSettings> {
        public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken) {
            var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
            var iniFile = settings.IniFile ?? "paths.ini";
            var paths = new PathsService(iniFile, baseDir);

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
