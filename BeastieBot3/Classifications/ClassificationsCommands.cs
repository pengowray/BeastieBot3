using System.ComponentModel;
using BeastieBot3.Checklists;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using Spectre.Console;
using Spectre.Console.Cli;

// `classifications import`: downloads a classification (ClassificationSources) into the
// classifications folder, unless it is there already, and imports it into the classifications store,
// replacing that source's rows. `classifications status` lists what is imported.

namespace BeastieBot3.Classifications;

[CommandInfo("classifications import", CommandKind.Mutates,
    "Download a classification from another source (avilist: AviList, ioc: IOC World Bird List, itis: ITIS, powo: Plants of the World Online, worms: World Register of Marine Species) and import it into the classifications store, for site build-db.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Replaces the source's rows from the downloaded file. The file is downloaded only when it is not in the classifications folder yet, or with --download.",
    Examples = new[] {
        "classifications import --source avilist",
        "classifications import --source all",
        "classifications import --source worms --download",
    })]
internal sealed class ClassificationsImportCommand : AsyncCommand<ClassificationsImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--source <KEY>")]
        [Description("avilist, ioc, itis, powo, worms, or all.")]
        public string Source { get; init; } = "all";

        [CommandOption("--file <PATH>")]
        [Description("Import this file instead of the downloaded one (one source only).")]
        public string? File { get; init; }

        [CommandOption("--download")]
        [Description("Download the file again even when it is in the classifications folder.")]
        public bool Download { get; init; }

        [CommandOption("--store <PATH>")]
        [Description("Classifications store. Default: Datastore:classifications_sqlite, else classifications.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = settings.StorePath ?? paths.GetClassificationsPath();
        var folder = paths.GetClassificationsDownloadDir();
        if (storePath is null || folder is null) {
            AnsiConsole.MarkupLine("[red]No datastore folder is set:[/] set datastore_dir under [[Datastore]] in paths.ini, or give --store.");
            return -1;
        }
        var sources = settings.Source.Equals("all", StringComparison.OrdinalIgnoreCase)
            ? ClassificationSources.All
            : ClassificationSources.Find(settings.Source) is { } one ? [one] : [];
        if (sources.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[red]Unknown source[/] {settings.Source}: use avilist, ioc, itis, powo, worms or all.");
            return -1;
        }
        if (settings.File is not null && sources.Count != 1) {
            AnsiConsole.MarkupLine("[red]--file needs one --source.[/]");
            return -1;
        }
        Directory.CreateDirectory(folder);
        using var store = ClassificationStore.Open(storePath);
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(30) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(ChecklistsImportCommand.UserAgent());
        var failed = false;
        foreach (var source in sources) {
            var file = settings.File ?? Path.Combine(folder, source.FileName);
            try {
                if (settings.File is null && (settings.Download || !System.IO.File.Exists(file))) {
                    AnsiConsole.MarkupLineInterpolated($"[grey]Downloading {source.Title} from[/] {source.Url}");
                    await ChecklistsImportCommand.Download(http, source.Url, file, cancellationToken);
                }
                AnsiConsole.MarkupLineInterpolated($"[grey]Reading[/] {file}");
                var parse = source.Parse(file);
                store.Replace(source.Key, parse.Version, source.Licence, source.Url, parse.Nodes, parse.Names, parse.Links);
                var species = parse.Nodes.Count(n => n.Rank == "species");
                AnsiConsole.MarkupLineInterpolated(
                    $"[green]{source.Title}:[/] version {parse.Version ?? "not given"}; {ClassificationStore.Count(species)} species and {ClassificationStore.Count(parse.Nodes.Count - species)} groups above them, {ClassificationStore.Count(parse.Names.Count(n => n.Synonym))} synonyms{(parse.Links.Count > 0 ? $", {ClassificationStore.Count(parse.Links.Count)} links to IUCN taxon ids" : "")}. Licence: {source.Licence}.");
            } catch (Exception e) when (e is HttpRequestException or IOException or InvalidDataException or KeyNotFoundException or TaskCanceledException) {
                AnsiConsole.MarkupLineInterpolated($"[red]{source.Title} not imported:[/] {e.Message}");
                failed = true;
            }
        }
        return failed ? 1 : 0;
    }
}

[CommandInfo("classifications status", CommandKind.ReadOnly, "List the classifications imported into the classifications store, with their versions, licences and sizes.",
    Examples = new[] { "classifications status" })]
internal sealed class ClassificationsStatusCommand : Command<ClassificationsStatusCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Classifications store. Default: Datastore:classifications_sqlite, else classifications.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var storePath = settings.StorePath ?? settings.CreatePaths().GetClassificationsPath();
        using var store = storePath is null ? null : ClassificationStore.OpenReadOnly(storePath);
        if (store is null) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]No classifications imported yet[/] ({storePath ?? "no datastore folder set"}). Run classifications import.");
            return 0;
        }
        var table = new Table().AddColumns("Source", "Version", "Licence", "Species", "Taxa", "Imported");
        foreach (var s in store.Sources()) {
            table.AddRow(Markup.Escape(ClassificationSources.Find(s.Source)?.Title ?? s.Source), Markup.Escape(s.Version ?? ""), Markup.Escape(s.Licence ?? ""),
                ClassificationStore.Count(s.Species), ClassificationStore.Count(s.Nodes), s.ImportedAt?.ToString("yyyy-MM-dd HH:mm") ?? "");
        }
        AnsiConsole.Write(table);
        return 0;
    }
}
