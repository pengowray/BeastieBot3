using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Downloads the SPRAT "Select ALL" report CSV the way a browser does: GET the report page (which
// sets the session cookie and names the form's submit URL), then POST the form with report type
// CSV and every column selected. The file lands beside the configured Datasets:SPRAT_csv under the
// name the site gives it; a download identical to the newest report already there is deleted.
// Never edits paths.ini and never imports: it prints the next step instead, like `iucn import`.
// The form fields and file checks live in SpratReportDownload.

namespace BeastieBot3.Sprat;

[CommandInfo("sprat download", CommandKind.Mutates,
    "Download the SPRAT report CSV, with every column, into the folder of the file set in Datasets:SPRAT_csv. Does not import it or change paths.ini.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Downloads the SPRAT report CSV again and keeps the new file only when it differs from the newest SPRAT report CSV already in its folder. The SPRAT (EPBC) database stays as it is until you run sprat import --force.",
    Examples = new[] { "sprat download", "sprat download --output-dir ~/datasets/sprat" })]
public sealed class SpratDownloadCommand : AsyncCommand<SpratDownloadCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--output-dir <DIR>")]
        [Description("Folder to save the SPRAT report CSV in. Default: the folder of the file set in Datasets:SPRAT_csv.")]
        public string? OutputDir { get; init; }

        [CommandOption("--user-agent <TEXT>")]
        [Description("User agent to send. Default: a Chrome user agent, because the SPRAT site accepts only user agents that look like a web browser's.")]
        public string? UserAgent { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var configuredCsv = paths.GetSpratCsvPath() is { Length: > 0 } csv ? Path.GetFullPath(csv) : null;

        var outputDir = ResolveOutputDir(settings.OutputDir, configuredCsv, paths.GetDatasetsDir());
        if (outputDir is null) {
            AnsiConsole.MarkupLine("[red]No folder to save into:[/] Datasets:SPRAT_csv is not set in paths.ini and --output-dir was not given. Use --output-dir <DIR>, or set Datasets:SPRAT_csv.");
            return -1;
        }
        Directory.CreateDirectory(outputDir);

        var partFile = Path.Combine(outputDir, "sprat-report-download.csv.part");
        string? siteFileName;
        try {
            siteFileName = await DownloadAsync(settings.UserAgent ?? SpratReportDownload.DefaultUserAgent, partFile, cancellationToken).ConfigureAwait(false);
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            File.Delete(partFile);
            throw;
        } catch (TaskCanceledException) {
            File.Delete(partFile);
            AnsiConsole.MarkupLine("[red]Download failed:[/] the SPRAT site did not answer within 5 minutes.");
            PrintManualSteps();
            return -2;
        } catch (HttpRequestException ex) {
            File.Delete(partFile);
            AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
            PrintManualSteps();
            return -2;
        }
        if (siteFileName is null) {
            File.Delete(partFile);
            PrintManualSteps();
            return -2;
        }

        var check = SpratReportDownload.CheckReport(partFile);
        if (!check.IsValid) {
            File.Delete(partFile);
            if (check.MissingColumns.Count > 0) {
                AnsiConsole.MarkupLineInterpolated($"[red]Missing columns:[/] the downloaded CSV does not have {string.Join(", ", check.MissingColumns)}, which sprat import needs. The SPRAT report format may have changed. The download was deleted.");
            } else {
                AnsiConsole.MarkupLineInterpolated($"[red]Not the SPRAT report CSV:[/] {check.Problem}. The report format may have changed. The download was deleted.");
            }
            return -3;
        }

        var newest = SpratReportDownload.FindNewestReport(outputDir);
        string report;
        if (newest is not null && SpratReportDownload.SameContent(partFile, newest)) {
            File.Delete(partFile);
            report = newest;
            AnsiConsole.MarkupLineInterpolated($"[green]SPRAT report CSV unchanged:[/] the new download is identical to {Path.GetFileName(newest)}. Deleted the new download.");
        } else {
            report = Path.Combine(outputDir, siteFileName);
            File.Move(partFile, report, overwrite: true);
            var megabytes = new FileInfo(report).Length / (1024.0 * 1024.0);
            AnsiConsole.MarkupLineInterpolated($"[green]Saved a new SPRAT report CSV:[/] {report} ({check.Taxa:N0} taxa, {megabytes:0.0} MB)");
        }

        PrintNextStep(report, configuredCsv, paths);
        return 0;
    }

    private static string? ResolveOutputDir(string? explicitDir, string? configuredCsv, string? datasetsDir) {
        if (!string.IsNullOrWhiteSpace(explicitDir)) {
            return Path.GetFullPath(explicitDir);
        }
        if (configuredCsv is not null) {
            return Path.GetDirectoryName(configuredCsv);
        }
        return string.IsNullOrWhiteSpace(datasetsDir) ? null : Path.Combine(Path.GetFullPath(datasetsDir), "sprat");
    }

    /// <summary>
    /// Fetches the report into <paramref name="partFile"/>. Returns the file name the site gave it,
    /// or null after printing why the download failed.
    /// </summary>
    private static async Task<string?> DownloadAsync(string userAgent, string partFile, CancellationToken cancellationToken) {
        using var handler = new HttpClientHandler {
            CookieContainer = new CookieContainer(),
            AutomaticDecompression = DecompressionMethods.All,
        };
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromMinutes(5) };
        client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", userAgent);
        client.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        client.DefaultRequestHeaders.TryAddWithoutValidation("Accept-Language", "en-AU,en;q=0.9");

        var pageUri = new Uri(SpratReportDownload.ReportPageUrl);
        using var page = await client.GetAsync(pageUri, cancellationToken).ConfigureAwait(false);
        if (!page.IsSuccessStatusCode) {
            AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] the SPRAT site returned HTTP {(int)page.StatusCode} {page.ReasonPhrase} for the report page.");
            return null;
        }
        var html = await page.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var action = SpratReportDownload.FindSubmitAction(html);
        if (action is null) {
            AnsiConsole.MarkupLine("[red]Report form not found[/] on the SPRAT report page. The page layout may have changed.");
            return null;
        }

        using var form = new MultipartFormDataContent();
        foreach (var (name, value) in SpratReportDownload.BuildFormFields()) {
            var part = new StringContent(value);
            part.Headers.ContentType = null;
            form.Add(part, name);
        }
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(page.RequestMessage?.RequestUri ?? pageUri, action)) {
            Content = form,
        };
        request.Headers.Referrer = pageUri;

        AnsiConsole.MarkupLine("Requesting the SPRAT report CSV (the SPRAT site takes a few seconds to generate it)...");
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode) {
            AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] the SPRAT site returned HTTP {(int)response.StatusCode} {response.ReasonPhrase} for the report.");
            return null;
        }
        var mediaType = response.Content.Headers.ContentType?.MediaType;
        if (!string.Equals(mediaType, "text/csv", StringComparison.OrdinalIgnoreCase)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Not a CSV:[/] the SPRAT site returned {mediaType ?? "a file with no content type"}, not the SPRAT report CSV.");
            return null;
        }

        await using (var output = new FileStream(partFile, FileMode.Create, FileAccess.Write))
        await using (var body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false)) {
            await body.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
        }

        var disposition = response.Content.Headers.ContentDisposition;
        var siteName = Path.GetFileName((disposition?.FileNameStar ?? disposition?.FileName ?? string.Empty).Trim('"'));
        return siteName.Length > 0 ? siteName : $"{DateTime.Now:ddMMyyyy-HHmmss}-report.csv";
    }

    private static void PrintNextStep(string report, string? configuredCsv, PathsService paths) {
        var fileName = Path.GetFileName(report);
        if (!string.Equals(configuredCsv, report, StringComparison.Ordinal)) {
            AnsiConsole.MarkupLine("Next: in paths.ini, replace the SPRAT_csv line with:");
            AnsiConsole.WriteLine($"SPRAT_csv={report}");
            AnsiConsole.MarkupLine("Then run sprat import --force.");
            return;
        }

        string? imported;
        try {
            imported = SpratImporter.ReadImportedFileName(paths.ResolveSpratDatabasePath(null));
        } catch (InvalidOperationException) {
            imported = null;
        }
        if (imported is null) {
            AnsiConsole.MarkupLine("Next: run sprat import. The SPRAT (EPBC) database has no import yet.");
        } else if (string.Equals(imported, fileName, StringComparison.Ordinal)) {
            AnsiConsole.MarkupLineInterpolated($"Nothing to do: the SPRAT (EPBC) database already holds an import of {fileName}.");
        } else {
            AnsiConsole.MarkupLineInterpolated($"Next: run sprat import --force. Datasets:SPRAT_csv names {fileName}, but the SPRAT (EPBC) database holds an import of a different file.");
        }
    }

    private static void PrintManualSteps() {
        AnsiConsole.MarkupLine("Try --user-agent with your own browser's user agent, or download the SPRAT report CSV by hand:");
        AnsiConsole.MarkupLineInterpolated($"  1. Open {SpratReportDownload.ReportPageUrl}");
        AnsiConsole.MarkupLine("  2. Set Report Type to CSV");
        AnsiConsole.MarkupLine("  3. Click Select ALL in every group");
        AnsiConsole.MarkupLine("  4. Click Generate Report");
        AnsiConsole.MarkupLine("  5. Set Datasets:SPRAT_csv in paths.ini to the downloaded file");
    }
}
