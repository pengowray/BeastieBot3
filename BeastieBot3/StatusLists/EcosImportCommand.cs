using System.ComponentModel;
using System.Globalization;
using System.Text;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses ecos-import`: downloads the US Fish and Wildlife Service's list of species listed under
// the Endangered Species Act (ECOS) into the status lists folder, as ecos-listed-species-<date>.csv,
// and replaces the ECOS listings in the status lists store with it.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses ecos-import", CommandKind.Mutates,
    "Download the US Fish and Wildlife Service's list of species listed under the US Endangered Species Act (ECOS; about 2,500 species, subspecies and populations; public domain) and store it in the status lists store, with every name that the brackets in its scientific names give.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the list again, keeps the file in the status lists folder with the date in its name, and replaces the stored ECOS listings with the new list, so listings that ECOS has removed are deleted.",
    Examples = new[] {
        "statuses ecos-import",
        "statuses ecos-import --file ~/datastore/status-lists/ecos-listed-species-2026-10-08.csv",
    })]
internal sealed class EcosImportCommand : AsyncCommand<EcosImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Status lists store. Default: Datastore:status_lists_sqlite, else status_lists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this ECOS CSV file instead of downloading the list. The file needs the ECOS Listed Species ID column, which the default ECOS export leaves out.")]
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
                AnsiConsole.MarkupLine("[red]No folder for the downloaded list:[/] set datastore_dir under [[Datastore]] or status_lists_dir under [[Datasets]] in paths.ini, or give --file.");
                return -1;
            }
            Directory.CreateDirectory(folder);
            file = Path.Combine(folder, $"ecos-listed-species-{now:yyyy-MM-dd}.csv");
            AnsiConsole.MarkupLineInterpolated($"[grey]Downloading the ECOS list of listed species from[/] {EcosListedSpecies.ReportUrl}");
            try {
                await Download(file, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (ex is HttpRequestException or IOException or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
                return 1;
            }
        }

        IReadOnlyList<EcosListing> rows;
        int skipped;
        try {
            using var reader = new StreamReader(file, Encoding.UTF8);
            rows = EcosListedSpecies.Read(reader, out skipped);
        } catch (InvalidDataException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read {Path.GetFileName(file)}:[/] {ex.Message}");
            return 1;
        }
        if (rows.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[red]No listings in {Path.GetFileName(file)}.[/] The store was not changed.");
            return 1;
        }

        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        using var store = StatusListStore.Open(storePath);
        store.ReplaceEcos(rows, now, new StatusSourceInfo(StatusSources.Ecos, EcosListedSpecies.Title, EcosListedSpecies.SiteUrl,
            EcosListedSpecies.Licence, EcosListedSpecies.Citation(now), Path.GetFileName(file), now, rows.Count));

        var table = new Table().Border(TableBorder.Rounded).AddColumn("ESA listing status").AddColumn(new TableColumn("Listings").RightAligned());
        foreach (var group in rows.GroupBy(r => r.Status).OrderByDescending(g => g.Count())) {
            table.AddRow(Markup.Escape(group.Key), group.Count().ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(table);
        var withOtherNames = rows.Count(r => r.Names.Count > 1);
        var names = rows.Sum(r => r.Names.Count);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} ECOS listings[/] of {rows.Select(r => r.SpeciesId).Distinct().Count():N0} species pages, with {names:N0} names; {withOtherNames:N0} scientific names give earlier names in brackets.");
        if (skipped > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {skipped:N0} rows with no ECOS Listed Species ID or no scientific name, or with an ID already read.[/]");
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]File:[/] {file}");
        return 0;
    }

    private static async Task Download(string file, CancellationToken cancellationToken) {
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(NatureServeClient.UserAgent);
        var partial = file + ".part";
        using (var response = await http.GetAsync(EcosListedSpecies.DownloadUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false)) {
            response.EnsureSuccessStatusCode();
            await using var output = System.IO.File.Create(partial);
            await response.Content.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
        }
        System.IO.File.Move(partial, file, overwrite: true);
    }
}
