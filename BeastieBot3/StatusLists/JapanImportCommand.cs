using System.ComponentModel;
using CsvHelper;
using Spectre.Console;
using Spectre.Console.Cli;
using UglyToad.PdfPig.Core;

// `statuses japan-import`: downloads the latest Red List of Japan's Ministry of the Environment,
// the 5th Red List's eight CSV files from ikilog.biodic.go.jp and the Red List 2020 PDF from
// env.go.jp (one request each, 1 second apart), into a folder in the status lists folder named
// japan-redlist-<date>, and replaces Japan's rows in the status lists store with them. A file
// already in that folder is read again, not downloaded again, so a run stopped by a failed download
// carries on. --dir imports a folder kept by an earlier run, or one filled by hand when a URL is
// gone. StatusListImport runs it.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses japan-import", CommandKind.Mutates,
    "Download the Red List of Japan's Ministry of the Environment (about 5,800 taxa and threatened local populations: the 5th Red List of birds, reptiles, amphibians, plants, algae, lichens and fungi, and the Red List 2020 of mammals, brackish and freshwater fishes, insects, molluscs and other invertebrates; Public Data License 1.0) and store it in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the eight CSV files and the Red List 2020 PDF into a folder named with the date, and replaces the stored rows of Japan's Red List. A file that is already in that folder is read again, not downloaded again.",
    Examples = new[] {
        "statuses japan-import",
        "statuses japan-import --dir ~/datastore/status-lists/japan-redlist-2026-10-09",
    })]
internal sealed class JapanImportCommand : AsyncCommand<JapanImportCommand.Settings> {
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--dir <FOLDER>")]
        [Description("Import the files in this folder instead of downloading: the eight CSV files and the Red List 2020 PDF, under the names in their URLs (redlist2026_birds.csv ... redlist2025_kinrui.csv, 900515981.pdf), as an earlier run keeps them.")]
        public override string? File { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return StatusListImport.RunAsync(settings, Spec(), cancellationToken);
    }

    internal static StatusListImportSpec<JapanRedListEntry> Spec() {
        // What the reader found apart from the rows: the PDF's headings and the lines it could not read.
        JapanRedListRead? read = null;
        return new StatusListImportSpec<JapanRedListEntry> {
            Source = StatusSources.Japan,
            Title = JapanRedList.Title,
            SiteUrl = JapanRedList.SiteUrl,
            Licence = JapanRedList.Licence,
            Citation = _ => JapanRedList.Citation,
            FileStem = "japan-redlist",
            FileExtension = "",
            ImportsFolder = true,
            DownloadNoun = "download",
            RowsNoun = "rows",
            Download = Download,
            Read = folder => (read = JapanRedList.Read(folder)).Entries,
            IsReadError = ex => ex is InvalidDataException or IOException or CsvHelperException or PdfDocumentFormatException,
            Replace = (store, rows, now, source) => store.ReplaceJapan(rows, now, source),
            GroupColumn = "Group (list)",
            CountColumn = "Rows",
            GroupOf = r => r.Group.FromPdf ? $"{r.Group.English} ({r.Group.ListVersion})" : $"{r.Group.English} ({r.Group.ListVersion}, {r.Group.ListYear})",
            Summary = rows => Summary(rows, read!),
        };
    }

    private static void Summary(IReadOnlyList<JapanRedListEntry> rows, JapanRedListRead read) {
        var populations = rows.Count(r => r.Category == JapanRedListCategory.LocalPopulation);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} rows of Japan's Red List:[/] {rows.Count - populations:N0} taxa and {populations:N0} threatened local populations (LP).");
        var categories = JapanRedListCategory.Codes
            .Select(code => (Code: code, Count: rows.Count(r => r.Category == code)))
            .Where(c => c.Count > 0)
            .Select(c => $"{c.Code} {c.Count:N0}");
        AnsiConsole.MarkupLineInterpolated($"[grey]Categories: {string.Join(", ", categories)}.[/]");

        var stated = read.PdfHeadings.Sum(h => h.Stated);
        var fromPdf = read.PdfHeadings.Sum(h => h.Read);
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Red List 2020 PDF: {fromPdf:N0} rows read under {read.PdfHeadings.Count} category headings, which state {stated:N0} rows.[/]");
        foreach (var heading in read.PdfHeadings.Where(h => h.Read != h.Stated)) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]PDF page {heading.Page}, {heading.Group} {heading.CategoryWritten}: the heading states {heading.Stated:N0} rows, {heading.Read:N0} were read.[/]");
        }
        if (read.Problems.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{read.Problems.Count:N0} lines or rows could not be read and were left out:[/]");
            foreach (var problem in read.Problems.Take(20)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]  {problem}[/]");
            }
        }
    }

    // Downloads each file that the folder does not have yet, 1 second apart.
    private static async Task Download(string folder, CancellationToken cancellationToken) {
        Directory.CreateDirectory(folder);
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5));
        var first = true;
        foreach (var (name, url) in JapanRedList.Files) {
            var file = Path.Combine(folder, name);
            if (File.Exists(file)) {
                AnsiConsole.MarkupLineInterpolated($"[grey]Already downloaded:[/] {file}");
                continue;
            }
            if (!first) {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
            first = false;
            AnsiConsole.MarkupLineInterpolated($"[grey]Downloading[/] {url}");
            try {
                await StatusListDownload.SaveAsync(http, url, file, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (ex is HttpRequestException or IOException) {
                throw new IOException(
                    $"{ex.Message}. If the file has moved, save it by hand in {folder} as {name} and run the command again with --dir \"{folder}\".", ex);
            }
        }
    }
}
