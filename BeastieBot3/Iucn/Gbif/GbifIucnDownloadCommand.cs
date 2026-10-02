using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Infrastructure;

// Downloads GBIF's copy of the IUCN Red List checklist: the Darwin Core Archive IUCN publishes on
// GBIF under CC BY 4.0 (dataset 19491596-35ae-4a91-9a98-85cf505f1bd3, DOI 10.15468/0qnb58). For
// nearly every global species it has the latest global assessment's citation with its DOI. The zip
// is saved as iucn-checklist-<date>.zip (date from the server's Last-Modified header) in
// Datasets:GBIF_IUCN_dir; a download identical to the newest copy already there is deleted. The
// download is read in full before it is kept, so a broken archive never replaces a good one.
// Nothing is imported.

namespace BeastieBot3.Iucn.Gbif;

[CommandInfo("iucn gbif-download", CommandKind.Mutates,
    "Download GBIF's copy of the IUCN Red List checklist (a Darwin Core Archive zip, CC BY 4.0) into Datasets:GBIF_IUCN_dir. For nearly every globally assessed species, the checklist includes the citation of the latest global assessment, with its DOI. Does not import anything.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Downloads the checklist zip again and keeps the new file only when it differs from the newest checklist zip already in its folder.",
    Examples = new[] { "iucn gbif-download", "iucn gbif-download --output-dir ~/datasets/gbif-iucn" })]
public sealed class GbifIucnDownloadCommand : AsyncCommand<GbifIucnDownloadCommand.Settings> {
    // GBIF must answer within a minute, and the whole download (about 21 MB) must finish within ten.
    private static readonly TimeSpan HeaderTimeout = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan DownloadTimeout = TimeSpan.FromMinutes(10);
    private const double BytesPerMegabyte = 1024 * 1024;

    public sealed class Settings : CommonSettings {
        [CommandOption("--output-dir <DIR>")]
        [Description("Folder to save the checklist zip in. Default: Datasets:GBIF_IUCN_dir in paths.ini, or a gbif-iucn folder inside datasets_dir.")]
        public string? OutputDir { get; init; }

        [CommandOption("--url <URL>")]
        [Description("Download from this URL instead of GBIF's hosted copy, https://hosted-datasets.gbif.org/datasets/iucn/iucn-latest.zip.")]
        public string? Url { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var outputDir = !string.IsNullOrWhiteSpace(settings.OutputDir) ? settings.OutputDir : paths.GetGbifIucnDir();
        if (string.IsNullOrWhiteSpace(outputDir)) {
            AnsiConsole.MarkupLine("[red]No folder to save into:[/] neither Datasets:GBIF_IUCN_dir nor datasets_dir is set in paths.ini, and --output-dir was not given. Use --output-dir <DIR>, or set Datasets:GBIF_IUCN_dir.");
            return -1;
        }
        outputDir = Path.GetFullPath(outputDir);
        Directory.CreateDirectory(outputDir);

        var partFile = Path.Combine(outputDir, "iucn-checklist-download.zip.part");
        DateOnly? lastModified;
        try {
            lastModified = await DownloadAsync(settings.Url ?? GbifIucnChecklistFiles.DownloadUrl, partFile, cancellationToken).ConfigureAwait(false);
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            File.Delete(partFile);
            throw;
        } catch (DownloadFailedException ex) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
            return -2;
        }

        GbifIucnChecklist checklist;
        try {
            checklist = await Task.Run(() => GbifIucnChecklistReader.Read(partFile, cancellationToken), cancellationToken).ConfigureAwait(false);
        } catch (Exception ex) when (ex is InvalidDataException or CsvHelper.CsvHelperException) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[red]Not the IUCN checklist archive:[/] {ex.Message} The download was deleted.");
            return -3;
        } catch (OperationCanceledException) {
            File.Delete(partFile);
            throw;
        }

        var newest = GbifIucnChecklistFiles.FindNewest(outputDir);
        if (newest is not null && GbifIucnChecklistFiles.Sha256(newest) == GbifIucnChecklistFiles.Sha256(partFile)) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[green]Checklist unchanged:[/] the new download is identical to {Path.GetFileName(newest)}. Deleted the new download.");
            PrintSummary(newest, checklist);
            return 0;
        }

        var date = lastModified ?? DateOnly.FromDateTime(DateTime.UtcNow);
        var target = GbifIucnChecklistFiles.ChooseTargetPath(outputDir, date);
        File.Move(partFile, target);
        var megabytes = new FileInfo(target).Length / BytesPerMegabyte;
        AnsiConsole.MarkupLineInterpolated($"[green]Saved a new checklist zip:[/] {target} ({megabytes:0.0} MB)");
        if (lastModified is null) {
            AnsiConsole.MarkupLine("[yellow]The server sent no Last-Modified date,[/] so the file name has today's date (UTC).");
        }
        if (newest is not null) {
            AnsiConsole.MarkupLineInterpolated($"Kept the older checklist zip, {Path.GetFileName(newest)}.");
        }
        PrintSummary(target, checklist);
        return 0;
    }

    /// <summary>
    /// Downloads the archive into <paramref name="partFile"/> and returns the UTC date of the
    /// server's Last-Modified header, or null when the server sent none.
    /// </summary>
    private static async Task<DateOnly?> DownloadAsync(string url, string partFile, CancellationToken cancellationToken) {
        using var client = new HttpClient { Timeout = HeaderTimeout };
        client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", GbifIucnChecklistFiles.UserAgent);

        // HttpClient.Timeout stops covering the request once the headers arrive, so the whole
        // download has its own limit.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(DownloadTimeout);

        AnsiConsole.MarkupLineInterpolated($"Downloading {url}");
        try {
            using var response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, timeout.Token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode) {
                throw new DownloadFailedException($"the server returned HTTP {(int)response.StatusCode} {response.ReasonPhrase}.");
            }

            var length = response.Content.Headers.ContentLength;
            await using (var output = new FileStream(partFile, FileMode.Create, FileAccess.Write))
            await using (var body = await response.Content.ReadAsStreamAsync(timeout.Token).ConfigureAwait(false)) {
                await ProgressConsole.RunAsync("Downloading the checklist zip (MB)", length is > 0 ? length.Value / BytesPerMegabyte : 0, async progress => {
                    var buffer = new byte[81920];
                    int read;
                    while ((read = await body.ReadAsync(buffer, timeout.Token).ConfigureAwait(false)) > 0) {
                        await output.WriteAsync(buffer.AsMemory(0, read), timeout.Token).ConfigureAwait(false);
                        progress.Increment(read / BytesPerMegabyte);
                    }
                }, timeout.Token).ConfigureAwait(false);
            }

            var modified = response.Content.Headers.LastModified;
            return modified is null ? null : DateOnly.FromDateTime(modified.Value.UtcDateTime);
        } catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) {
            throw new DownloadFailedException(timeout.IsCancellationRequested
                ? $"the download did not finish within {DownloadTimeout.TotalMinutes:0} minutes."
                : $"the server did not answer within {HeaderTimeout.TotalMinutes:0} minute.");
        } catch (HttpRequestException ex) {
            throw new DownloadFailedException(ex.Message);
        } catch (Exception ex) when (ex is InvalidOperationException or UriFormatException) {
            // A --url that HttpClient cannot request, such as a relative path.
            throw new DownloadFailedException(ex.Message);
        }
    }

    private static void PrintSummary(string path, GbifIucnChecklist checklist) {
        var dataset = checklist.Summary.Dataset;
        var taxa = checklist.Taxa.Count;
        var withDoi = checklist.Taxa.Values.Count(t => t.Doi is not null);
        var vernacularRows = checklist.Taxa.Values.Sum(t => t.VernacularNames.Count);
        var vernacularTaxa = checklist.Taxa.Values.Count(t => t.VernacularNames.Count > 0);

        var table = new Table().Border(TableBorder.Rounded).HideHeaders().AddColumn("Field").AddColumn("Value");
        table.AddRow("File", Markup.Escape(Path.GetFileName(path)));
        table.AddRow("Red List version", Markup.Escape(dataset.RedListVersion ?? "(not found in eml.xml)"));
        table.AddRow("Published on GBIF", Markup.Escape(dataset.PubDate ?? "(not found in eml.xml)"));
        table.AddRow("Licence", Markup.Escape(dataset.LicenceUrl ?? dataset.LicenceText ?? "(not found in eml.xml)"));
        table.AddRow("Accepted taxa", $"{taxa:N0}");
        table.AddRow("Accepted taxa with a DOI", taxa == 0 ? "0" : $"{withDoi:N0} ({withDoi / (double)taxa:P1})");
        table.AddRow("Synonyms", $"{checklist.Synonyms.Count:N0}");
        table.AddRow("Vernacular names", $"{vernacularRows:N0} names for {vernacularTaxa:N0} taxa");
        AnsiConsole.Write(table);

        foreach (var file in checklist.Files.Where(f => f.UnusedRows > 0)) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {file.UnusedRows:N0} of {file.Rows:N0} rows in {file.Location}:[/] rows with no accepted taxon, or repeated rows.");
        }
    }

    private sealed class DownloadFailedException : Exception {
        public DownloadFailedException(string message) : base(message) { }
    }
}
