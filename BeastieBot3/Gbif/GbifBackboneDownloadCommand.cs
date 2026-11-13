using System.ComponentModel;
using System.Globalization;
using System.Net.Http;
using BeastieBot3.Infrastructure;
using Spectre.Console;
using Spectre.Console.Cli;

// `gbif backbone-download`: downloads the GBIF Backbone Taxonomy (backbone.zip, about 930 MB, CC BY
// 4.0) into Datasets:GBIF_backbone_dir as backbone-<date>.zip, dated from the server's Last-Modified
// header. It first asks the server for the file's date and size and downloads nothing when the
// newest zip in the folder has both, since GBIF has not changed the file since 2023-08-28. A
// download is checked (meta.xml, Taxon.tsv, VernacularName.tsv, eml.xml naming the backbone) before
// it takes the place of an older copy with the same date. Nothing is imported: `site build-db`
// reads the zip.

namespace BeastieBot3.Gbif;

[CommandInfo("gbif backbone-download", CommandKind.Mutates,
    "Download the GBIF Backbone Taxonomy (a Darwin Core Archive zip of about 930 MB, CC BY 4.0) into Datasets:GBIF_backbone_dir, for the common names and synonyms site build-db adds to the site. Does not download when the newest zip in the folder has the server's date and size. Does not import anything.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Asks the server for the file's date and size, and downloads only when the newest zip in the folder differs, or with --force.",
    Examples = new[] { "gbif backbone-download", "gbif backbone-download --force" })]
internal sealed class GbifBackboneDownloadCommand : AsyncCommand<GbifBackboneDownloadCommand.Settings> {
    private static readonly TimeSpan HeaderTimeout = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan DownloadTimeout = TimeSpan.FromMinutes(90);
    private const double BytesPerMegabyte = 1024 * 1024;
    private const string UserAgent = Iucn.Gbif.GbifIucnChecklistFiles.UserAgent;

    public sealed class Settings : CommonSettings {
        [CommandOption("--output-dir <DIR>")]
        [Description("Folder to save the zip in. Default: Datasets:GBIF_backbone_dir in paths.ini, or a gbif-backbone folder inside datasets_dir.")]
        public string? OutputDir { get; init; }

        [CommandOption("--url <URL>")]
        [Description("Download from this URL instead of GBIF's current backbone, https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip.")]
        public string? Url { get; init; }

        [CommandOption("--force")]
        [Description("Download even when the newest zip in the folder has the server's date and size.")]
        public bool Force { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var outputDir = !string.IsNullOrWhiteSpace(settings.OutputDir) ? settings.OutputDir : paths.GetGbifBackboneDir();
        if (string.IsNullOrWhiteSpace(outputDir)) {
            AnsiConsole.MarkupLine("[red]No folder to save into:[/] neither Datasets:GBIF_backbone_dir nor datasets_dir is set in paths.ini, and --output-dir was not given.");
            return -1;
        }
        outputDir = Path.GetFullPath(outputDir);
        Directory.CreateDirectory(outputDir);
        var url = settings.Url ?? GbifBackboneArchive.DownloadUrl;

        using var client = new HttpClient { Timeout = HeaderTimeout };
        client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", UserAgent);

        var newest = GbifBackboneArchive.FindNewest(outputDir);
        if (!settings.Force && newest is not null) {
            try {
                var (modified, length) = await HeadAsync(client, url, cancellationToken).ConfigureAwait(false);
                if (modified is { } date && GbifBackboneArchive.DateOf(newest) == date && length == new FileInfo(newest).Length) {
                    AnsiConsole.MarkupLineInterpolated($"[green]Already downloaded:[/] {Path.GetFileName(newest)} has the server's date ({date:yyyy-MM-dd}) and size. Nothing was downloaded.");
                    return PrintSummary(newest) ? 0 : -3;
                }
            } catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Could not ask the server for the file's date and size ({ex.Message}).[/] Downloading it.");
            }
        }

        var partFile = Path.Combine(outputDir, "backbone-download.zip.part");
        DateOnly? lastModified;
        try {
            lastModified = await DownloadAsync(client, url, partFile, cancellationToken).ConfigureAwait(false);
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            File.Delete(partFile);
            throw;
        } catch (DownloadFailedException ex) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
            return -2;
        }

        if (Check(partFile) is { } problem) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[red]Not the GBIF Backbone Taxonomy archive:[/] {problem} The download was deleted.");
            return -3;
        }
        var target = Path.Combine(outputDir, GbifBackboneArchive.FileNameFor(lastModified ?? DateOnly.FromDateTime(DateTime.UtcNow)));
        File.Move(partFile, target, overwrite: true);
        AnsiConsole.MarkupLineInterpolated($"[green]Saved:[/] {target} ({new FileInfo(target).Length / BytesPerMegabyte:0.0} MB)");
        if (lastModified is null) {
            AnsiConsole.MarkupLine("[yellow]The server sent no Last-Modified date,[/] so the file name has today's date (UTC).");
        }
        if (newest is not null && !string.Equals(Path.GetFullPath(newest), Path.GetFullPath(target), StringComparison.Ordinal)) {
            AnsiConsole.MarkupLineInterpolated($"Kept the older zip, {Path.GetFileName(newest)}.");
        }
        return PrintSummary(target) ? 0 : -3;
    }

    private static async Task<(DateOnly? Modified, long? Length)> HeadAsync(HttpClient client, string url, CancellationToken ct) {
        using var request = new HttpRequestMessage(HttpMethod.Head, url);
        using var response = await client.SendAsync(request, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        var modified = response.Content.Headers.LastModified;
        return (modified is null ? null : DateOnly.FromDateTime(modified.Value.UtcDateTime), response.Content.Headers.ContentLength);
    }

    // Downloads the archive into partFile and returns the UTC date of the server's Last-Modified
    // header, or null when it sent none.
    private static async Task<DateOnly?> DownloadAsync(HttpClient client, string url, string partFile, CancellationToken cancellationToken) {
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
                await ProgressConsole.RunAsync("Downloading the GBIF Backbone Taxonomy (MB)", length is > 0 ? length.Value / BytesPerMegabyte : 0, async progress => {
                    var buffer = new byte[1 << 16];
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
            throw new DownloadFailedException(ex.Message);
        }
    }

    /// Why the zip is not a backbone archive the site build can read; null when it is.
    internal static string? Check(string path) {
        try {
            using var zip = GbifBackboneArchive.Open(path);
            var meta = GbifBackboneArchive.ReadMeta(zip);
            if (!string.Equals(meta.Core.RowTypeName, "Taxon", StringComparison.OrdinalIgnoreCase) || zip.GetEntry(meta.Core.Location) is null) {
                return "It has no Taxon core file.";
            }
            if (meta.Extension("VernacularName") is not { } vernacular || zip.GetEntry(vernacular.Location) is null) {
                return "It has no VernacularName file.";
            }
            var info = GbifBackboneArchive.ReadInfo(zip);
            return info.Title?.Contains("Backbone", StringComparison.OrdinalIgnoreCase) == true
                ? null
                : $"Its eml.xml names the dataset \"{info.Title ?? "(no title)"}\".";
        } catch (Exception ex) when (ex is InvalidDataException or IOException) {
            return ex.Message;
        }
    }

    private static bool PrintSummary(string path) {
        if (Check(path) is { } problem) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Path.GetFileName(path)} cannot be read:[/] {problem}");
            return false;
        }
        using var zip = GbifBackboneArchive.Open(path);
        var meta = GbifBackboneArchive.ReadMeta(zip);
        var info = GbifBackboneArchive.ReadInfo(zip);
        var table = new Table().Border(TableBorder.Rounded).HideHeaders().AddColumn("Field").AddColumn("Value");
        table.AddRow("File", Markup.Escape(Path.GetFileName(path)));
        table.AddRow("Dataset", Markup.Escape(info.Title ?? "-"));
        table.AddRow("Published", Markup.Escape(info.PubDate ?? "-"));
        table.AddRow("Licence", Markup.Escape(info.LicenceUrl ?? "-"));
        table.AddRow("DOI", Markup.Escape(info.Doi ?? "-"));
        foreach (var location in new[] { meta.Core.Location, meta.Extension("VernacularName")!.Location }) {
            var entry = zip.GetEntry(location)!;
            table.AddRow(Markup.Escape(location), $"{(entry.Length / BytesPerMegabyte).ToString("N0", CultureInfo.InvariantCulture)} MB unzipped");
        }
        AnsiConsole.Write(table);
        return true;
    }

    private sealed class DownloadFailedException : Exception {
        public DownloadFailedException(string message) : base(message) { }
    }
}
