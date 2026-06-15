using BeastieBot3.Web;
using BeastieBot3.Web.Commands;
using Spectre.Console;
using Spectre.Console.Cli;
using System.ComponentModel;

// Main entry point. The bulk of CLI configuration lives in the [CommandInfo]
// attributes on each command class; CommandRegistry scans for them at
// startup and configures the entire branch tree. The only thing wired up
// by hand here is ServeCommand, which intentionally has no [CommandInfo]
// attribute because it's the infrastructure command that exposes the web UI,
// not a workload command we want to surface there.

namespace BeastieBot3;

// Common CLI settings shared by all commands
public class CommonSettings : CommandSettings {
    [CommandOption("-s|--settings-dir <DIR>")]
    [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
    public string? SettingsDir { get; init; }

    [CommandOption("--ini-file <FILE>")]
    [Description("INI filename to read. Defaults to paths.ini.")]
    public string? IniFile { get; init; }

    // Single construction point for the per-command PathsService. Passing the raw
    // (possibly null) flags lets IniPathReader apply its own "paths.ini" /
    // AppContext.BaseDirectory defaults, so every command resolves config the same
    // way and honours --ini-file / --settings-dir uniformly.
    public Configuration.PathsService CreatePaths() => new(IniFile, SettingsDir);
}

internal class Program {
    static int Main(string[] args) {
        var app = BuildApp();
        return app.Run(args);
    }

    // Returns a fresh CommandApp each call so the web job runner can re-invoke
    // commands without sharing state across jobs.
    public static CommandApp BuildApp() {
        var app = new CommandApp();
        app.Configure(config => {
            config.SetApplicationName("beastiebot3");
            config.ValidateExamples();

            // Route uncaught command exceptions through AnsiConsole so the web
            // job runner's tee captures them. Spectre's default handler holds
            // a console reference captured before the tee swap.
            config.SetExceptionHandler((ex, _) => {
                // Cancellation: stay silent — the web job runner appends its own
                // "[cancelled by request]" marker. Print nothing here so the
                // log doesn't show a misleading "Error: The operation was
                // canceled" alongside the friendlier cancellation note.
                if (ex is OperationCanceledException) {
                    return -2;
                }
                AnsiConsole.MarkupLine($"[red]Error:[/] {Markup.Escape(ex.Message)}");
                if (ex.InnerException is { } inner) {
                    AnsiConsole.MarkupLine($"  [grey]Inner:[/] {Markup.Escape(inner.Message)}");
                }
                return -1;
            });

            // Workload commands: derived from [CommandInfo] attributes.
            CommandRegistry.ConfigureAll(config);

            // Infrastructure command: manual, intentionally absent from the web catalogue.
            config.AddCommand<ServeCommand>("serve")
                .WithDescription("Start a local web UI for browsing data sources and running commands.")
                .WithExample(new[] { "serve" })
                .WithExample(new[] { "serve", "--port", "5005" });
        });
        return app;
    }
}
