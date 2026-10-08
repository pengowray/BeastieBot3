using System.ComponentModel;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses cites-import`: downloads every taxon of the Checklist of CITES Species with its current
// listings (44 requests of 1,000 taxa, one at a time, 1 second apart; about 10 minutes, because a page
// of plants takes the server about 20 seconds), keeps them in the status lists folder as
// cites-<date>.json.gz, and replaces the CITES rows in the status lists store with them.
// StatusListImport runs it.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses cites-import", CommandKind.Mutates,
    "Download the current CITES Appendix listings of every taxon in the Checklist of CITES Species (checklist.cites.org, CITES Secretariat and UNEP-WCMC; about 43,000 species, subspecies and higher taxa; no commercial use) and store them in the status lists store, with each taxon's synonyms.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the Checklist again, keeps it in the status lists folder with the date in the file name, and replaces the stored CITES taxa and listings, so listings that are no longer current are deleted.",
    Examples = new[] {
        "statuses cites-import",
        "statuses cites-import --limit 2 --store /tmp/status_lists.sqlite",
        "statuses cites-import --file ~/datastore/status-lists/cites-2026-10-08.json.gz",
    })]
internal sealed class CitesImportCommand : AsyncCommand<CitesImportCommand.Settings> {
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this file, kept by an earlier run, instead of downloading.")]
        public override string? File { get; init; }

        [CommandOption("--limit <PAGES>")]
        [Description("Download only the first N pages of 1,000 taxa, for a test. The stored CITES rows are still replaced, with those taxa only, so give --store a test store.")]
        public int? Limit { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        if (settings.Limit is < 1) {
            AnsiConsole.MarkupLine("[red]--limit must be at least 1.[/]");
            return Task.FromResult(-1);
        }
        return StatusListImport.RunAsync(settings, Spec(settings.Limit), cancellationToken);
    }

    /// <paramref name="pageLimit"/>: download only that many pages; the file is then named
    /// cites-first<N>pages-<date>.json.gz.
    internal static StatusListImportSpec<CitesTaxon> Spec(int? pageLimit = null) => new() {
        Source = StatusSources.Cites,
        Title = CitesChecklist.Title,
        SiteUrl = CitesChecklist.SiteUrl,
        Licence = CitesChecklist.Licence,
        Citation = CitesChecklist.Citation,
        FileStem = pageLimit is { } limit ? $"cites-first{limit}pages" : "cites",
        FileExtension = "json.gz",
        DownloadNoun = "download",
        RowsNoun = "taxa",
        Download = (file, cancellationToken) => Download(file, pageLimit, cancellationToken),
        Read = file => CitesChecklist.ReadTaxa(StatusListDownload.ReadJsonArray(file)),
        IsReadError = ex => ex is JsonException or InvalidDataException,
        Replace = (store, rows, now, source) => store.ReplaceCites(rows, now, source),
        GroupColumn = "Current listing",
        CountColumn = "Taxa",
        GroupOf = r => r.CurrentListing ?? "(none)",
        Summary = Summary,
    };

    private static void Summary(IReadOnlyList<CitesTaxon> taxa) {
        var listings = taxa.SelectMany(t => t.Listings).ToList();
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {taxa.Count:N0} CITES taxa[/] ({taxa.Count(t => t.Rank == "SPECIES"):N0} species, {taxa.Count(t => t.Rank is "SUBSPECIES" or "VARIETY"):N0} subspecies and varieties) with {listings.Count:N0} current listings, {listings.Count(l => l.InheritedName is not null):N0} of them inherited from a higher taxon.");
        var split = taxa.Count(t => t.Listings.Select(l => l.Appendix).Distinct().Count() > 1);
        var appendixIII = listings.Where(l => l.Appendix == "III").ToList();
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]{split:N0} taxa have listings in two appendices (a split listing: the notes say which populations are in each). {appendixIII.Count:N0} Appendix III listings by {appendixIII.Select(l => l.PartyIsoCode).Distinct().Count():N0} Parties. {taxa.Sum(t => t.Synonyms.Count):N0} synonyms.[/]");
        var unresolved = listings.Count(l => l.InheritedName is not null && l.InheritedFromId is null);
        if (unresolved > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]{unresolved:N0} inherited listings name a higher taxon that is not in the file, or not once.[/]");
        }
    }

    private static Task Download(string file, int? pageLimit, CancellationToken cancellationToken) =>
        StatusListDownload.WriteGzipJsonArrayAsync(file, async write => {
            // A page of plants takes the server about 20 seconds.
            using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5), acceptJson: true);
            var ids = new HashSet<long>();
            long total = -1;
            var read = 0;
            for (var page = 1; ; page++) {
                var json = await StatusListDownload.GetStringAsync(http, CitesChecklist.PageUrl(page),
                    message => AnsiConsole.MarkupLineInterpolated($"[yellow]CITES Checklist, page {page}: {message}[/]"), cancellationToken)
                    .ConfigureAwait(false);
                var (pageTotal, rows) = CitesChecklist.ReadPage(json);
                if (total >= 0 && pageTotal != total) {
                    throw new IOException($"The Checklist's number of taxa changed from {total:N0} to {pageTotal:N0} during the download, so pages may have moved; run it again.");
                }
                total = pageTotal;
                write(rows);
                read += rows.Count;
                foreach (var row in rows) {
                    if (row.TryGetProperty("id", out var id) && id.TryGetInt64(out var value)) {
                        ids.Add(value);
                    }
                }
                AnsiConsole.MarkupLineInterpolated($"[grey]CITES Checklist: {read:N0} of {total:N0} taxa[/]");
                if (rows.Count < CitesChecklist.PageSize || read >= total || page == pageLimit) {
                    break;
                }
                if (page >= 200) {
                    throw new IOException("The Checklist gave more than 200 full pages, far more than its number of taxa; stopped.");
                }
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
            if (pageLimit is null && (read != total || ids.Count != total)) {
                throw new IOException($"The Checklist gave {read:N0} rows with {ids.Count:N0} different taxa, not the {total:N0} it reported; run it again.");
            }
        });
}
