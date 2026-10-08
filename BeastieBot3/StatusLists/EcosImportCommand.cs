using System.ComponentModel;
using System.Text;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses ecos-import`: downloads the US Fish and Wildlife Service's list of species listed under
// the Endangered Species Act (ECOS) into the status lists folder, as ecos-listed-species-<date>.csv,
// and replaces the ECOS listings in the status lists store with it. StatusListImport runs it.

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
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this ECOS CSV file instead of downloading the list. The file needs the ECOS Listed Species ID column, which the default ECOS export leaves out.")]
        public override string? File { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return StatusListImport.RunAsync(settings, Spec(), cancellationToken);
    }

    internal static StatusListImportSpec<EcosListing> Spec() {
        // The rows the reader skipped, for the summary.
        var skipped = 0;
        return new StatusListImportSpec<EcosListing> {
            Source = StatusSources.Ecos,
            Title = EcosListedSpecies.Title,
            SiteUrl = EcosListedSpecies.SiteUrl,
            Licence = EcosListedSpecies.Licence,
            Citation = EcosListedSpecies.Citation,
            FileStem = "ecos-listed-species",
            FileExtension = "csv",
            DownloadNoun = "downloaded list",
            RowsNoun = "listings",
            Download = Download,
            Read = file => {
                using var reader = new StreamReader(file, Encoding.UTF8);
                return EcosListedSpecies.Read(reader, out skipped);
            },
            IsReadError = ex => ex is InvalidDataException,
            Replace = (store, rows, now, source) => store.ReplaceEcos(rows, now, source),
            GroupColumn = "ESA listing status",
            CountColumn = "Listings",
            GroupOf = r => r.Status,
            Summary = rows => {
                var withOtherNames = rows.Count(r => r.Names.Count > 1);
                var names = rows.Sum(r => r.Names.Count);
                AnsiConsole.MarkupLineInterpolated(
                    $"[green]Stored {rows.Count:N0} ECOS listings[/] of {rows.Select(r => r.SpeciesId).Distinct().Count():N0} species pages, with {names:N0} names; {withOtherNames:N0} scientific names give earlier names in brackets.");
                if (skipped > 0) {
                    AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {skipped:N0} rows with no ECOS Listed Species ID or no scientific name, or with an ID already read.[/]");
                }
            },
        };
    }

    private static async Task Download(string file, CancellationToken cancellationToken) {
        AnsiConsole.MarkupLineInterpolated($"[grey]Downloading the ECOS list of listed species from[/] {EcosListedSpecies.ReportUrl}");
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5));
        await StatusListDownload.SaveAsync(http, EcosListedSpecies.DownloadUrl, file, cancellationToken).ConfigureAwait(false);
    }
}
