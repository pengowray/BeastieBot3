using System.ComponentModel;
using ExcelDataReader.Exceptions;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses jncc-import`: finds the current Conservation Designations for UK Taxa workbook on its JNCC
// resource page, downloads it into the status lists folder under JNCC's own file name (which has
// the release date, taxon-designations-20260609.xlsx; a file already there is read again, not
// downloaded again), and replaces the JNCC designations in the status lists store with its Master
// List. StatusListImport runs it.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses jncc-import", CommandKind.Mutates,
    "Download JNCC's Conservation Designations for UK Taxa (about 27,000 designations of 15,000 taxa: UK, GB and country red lists, legislation and priority lists, and international conventions; Open Government Licence) and store it in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run reads the link to the current workbook from JNCC's resource page, downloads the workbook when the status lists folder does not have a file of that name, and replaces the stored JNCC designations, so designations that JNCC has removed are deleted.",
    Examples = new[] {
        "statuses jncc-import",
        "statuses jncc-import --file ~/datastore/status-lists/taxon-designations-20260609.xlsx",
    })]
internal sealed class JnccImportCommand : AsyncCommand<JnccImportCommand.Settings> {
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this workbook (.xlsx), kept by an earlier run or downloaded from JNCC, instead of downloading. The citation takes the year of the release from the date in the file name.")]
        public override string? File { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return StatusListImport.RunAsync(settings, Spec(), cancellationToken);
    }

    internal static StatusListImportSpec<JnccDesignation> Spec() {
        // The link found on the resource page (a download only), and the rows the reader skipped.
        JnccSpreadsheetLink? link = null;
        var skipped = 0;
        return new StatusListImportSpec<JnccDesignation> {
            Source = StatusSources.Jncc,
            Title = JnccDesignations.Title,
            SiteUrl = JnccDesignations.ResourcePageUrl,
            Licence = JnccDesignations.Licence,
            Citation = now => JnccDesignations.Attribution(now.Year),
            FileStem = "taxon-designations",
            FileExtension = "xlsx",
            FindFileName = async cancellationToken => {
                link = await FindLink(cancellationToken).ConfigureAwait(false);
                return link.FileName;
            },
            SourceForFile = (file, source) => JnccDesignations.SourceFor(file, link, source),
            DownloadNoun = "download",
            RowsNoun = "designations",
            Download = (file, cancellationToken) => Download(link!, file, cancellationToken),
            Read = file => {
                using var stream = File.OpenRead(file);
                return JnccDesignations.Read(stream, out skipped);
            },
            IsReadError = ex => ex is InvalidDataException or ExcelReaderException,
            Replace = (store, rows, now, source) => store.ReplaceJncc(rows, now, source),
            GroupColumn = "Reporting category",
            CountColumn = "Designations",
            GroupOf = r => r.ReportingCategory,
            Summary = rows => Summary(rows, skipped),
        };
    }

    private static void Summary(IReadOnlyList<JnccDesignation> rows, int skipped) {
        var taxa = rows.Select(r => r.TaxonVersionKey).Distinct(StringComparer.Ordinal).Count();
        int Count(string scope) => rows.Count(r => r.Scope == scope);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} JNCC designations[/] of {taxa:N0} taxa: {Count(JnccClassification.Uk):N0} for the UK or Great Britain, {Count(JnccClassification.Country):N0} for part of the UK, {Count(JnccClassification.International):N0} international.");
        var unknown = rows.Where(r => r.Scope is null).GroupBy(r => r.DesignationCode).OrderBy(g => g.Key, StringComparer.Ordinal).ToList();
        if (unknown.Count > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]{unknown.Sum(g => g.Count()):N0} designations have no scope, because JnccClassification has no rule for their codes:[/] {string.Join(", ", unknown.Select(g => $"{g.Key} ({g.Count():N0})"))}");
        }
        if (skipped > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {skipped:N0} rows with no taxon version key, name, reporting category, designation or designation code.[/]");
        }
    }

    // The link to the current workbook on the resource page.
    private static async Task<JnccSpreadsheetLink> FindLink(CancellationToken cancellationToken) {
        AnsiConsole.MarkupLineInterpolated($"[grey]Reading the JNCC resource page[/] {JnccDesignations.ResourcePageUrl}");
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(2));
        var html = await http.GetStringAsync(JnccDesignations.ResourcePageUrl, cancellationToken).ConfigureAwait(false);
        return JnccDesignations.FindSpreadsheetLink(html)
            ?? throw new IOException($"The JNCC resource page has no link to a workbook (.xlsx): {JnccDesignations.ResourcePageUrl}");
    }

    // A file of the same name is the same release, so it is read again rather than downloaded again.
    private static async Task Download(JnccSpreadsheetLink link, string file, CancellationToken cancellationToken) {
        if (File.Exists(file)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Already downloaded:[/] {file}");
            return;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]Downloading[/] {link.Url}");
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5));
        await StatusListDownload.SaveAsync(http, link.Url, file, cancellationToken).ConfigureAwait(false);
    }
}
