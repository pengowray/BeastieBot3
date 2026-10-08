using System.ComponentModel;
using System.Globalization;
using System.Text;
using System.Text.Json;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses salve-import`: downloads the current national assessments of Brazil's fauna from SALVE
// (ICMBio; about 31 requests of 500 rows), keeps them in the status lists folder as
// salve-<date>.json, and replaces the SALVE assessments in the status lists store with them.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses salve-import", CommandKind.Mutates,
    "Download the current national assessments of Brazil's fauna from SALVE (salve.icmbio.gov.br, ICMBio; about 15,400 species and subspecies; public, with the source cited) and store them in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the assessments again, keeps them in the status lists folder with the date in the file name, and replaces the stored SALVE assessments, so assessments that are no longer current are deleted.",
    Examples = new[] {
        "statuses salve-import",
        "statuses salve-import --file ~/datastore/status-lists/salve-2026-10-08.json",
    })]
internal sealed class SalveImportCommand : AsyncCommand<SalveImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Status lists store. Default: Datastore:status_lists_sqlite, else status_lists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this file, kept by an earlier run, instead of downloading.")]
        public string? File { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = paths.ResolveStatusListsPath(settings.StorePath);
        var now = DateTime.UtcNow;

        string file;
        if (settings.File is { } given) {
            file = Path.GetFullPath(given.StartsWith("~/", StringComparison.Ordinal)
                ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), given[2..])
                : given);
            if (!System.IO.File.Exists(file)) {
                AnsiConsole.MarkupLineInterpolated($"[red]File not found:[/] {file}");
                return -1;
            }
        } else {
            var folder = paths.GetStatusListsDownloadDir();
            if (folder is null) {
                AnsiConsole.MarkupLine("[red]No folder for the download:[/] set datastore_dir under [[Datastore]] or status_lists_dir under [[Datasets]] in paths.ini, or give --file.");
                return -1;
            }
            Directory.CreateDirectory(folder);
            file = Path.Combine(folder, $"salve-{now:yyyy-MM-dd}.json");
            try {
                await Download(file, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (ex is HttpRequestException or IOException or JsonException
                                             or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
                return 1;
            }
        }

        IReadOnlyList<SalveAssessment> rows;
        try {
            rows = Read(System.IO.File.ReadAllText(file, Encoding.UTF8));
        } catch (JsonException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read {Path.GetFileName(file)}:[/] {ex.Message}");
            return 1;
        }
        if (rows.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[red]No assessments in {Path.GetFileName(file)}.[/] The store was not changed.");
            return 1;
        }

        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        using var store = StatusListStore.Open(storePath);
        store.ReplaceSalve(rows, now, new StatusSourceInfo(StatusSources.Salve, SalveApi.Title, SalveApi.SiteUrl, SalveApi.Licence,
            SalveApi.Citation(now), Path.GetFileName(file), now, rows.Count));

        var table = new Table().Border(TableBorder.Rounded).AddColumn("Category").AddColumn(new TableColumn("Assessments").RightAligned());
        foreach (var group in rows.GroupBy(r => r.PossiblyExtinct ? r.Category + "(PE)" : r.Category).OrderByDescending(g => g.Count())) {
            table.AddRow(Markup.Escape(group.Key), group.Count().ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} current SALVE assessments[/], {rows.Count(r => r.Doi is not null):N0} with a DOI.");
        AnsiConsole.MarkupLineInterpolated($"[grey]File:[/] {file}");
        return 0;
    }

    // The kept file: the rows of every page as SALVE gave them, in one array.
    internal static IReadOnlyList<SalveAssessment> Read(string json) {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray().Select(SalveApi.ReadAssessment).OfType<SalveAssessment>().ToList();
    }

    private static async Task Download(string file, CancellationToken cancellationToken) {
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(NatureServeClient.UserAgent);
        http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
        var rows = new List<JsonElement>();
        for (var page = 1; ; page++) {
            using var response = await http.GetAsync(SalveApi.PageUrl(page), cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            var pageRows = SalveApi.ReadPage(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
            rows.AddRange(pageRows);
            AnsiConsole.MarkupLineInterpolated($"[grey]SALVE: {rows.Count:N0} assessments read[/]");
            if (pageRows.Count < SalveApi.PageSize) {
                break;
            }
            if (page >= 200) {
                throw new IOException("SALVE gave more than 200 full pages, which is not what it gave before; stopped.");
            }
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
        }
        var partial = file + ".part";
        await using (var output = System.IO.File.Create(partial)) {
            await using var writer = new Utf8JsonWriter(output);
            writer.WriteStartArray();
            foreach (var row in rows) {
                row.WriteTo(writer);
            }
            writer.WriteEndArray();
        }
        System.IO.File.Move(partial, file, overwrite: true);
    }
}
