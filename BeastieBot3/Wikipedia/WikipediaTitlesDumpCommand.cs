using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;

// Downloads and imports the enwiki all-titles-in-ns0 dump (updated roughly twice a month) into
// the Wikipedia cache as a local existence check. The dump is title-only: every ns0 title,
// articles and redirects alike, one per line with underscores, no page ids. A queued title
// absent from it is almost certainly a redlink, which is what `fetch-pages --exists-first`
// uses to download real pages before spending API calls learning that the rest do not exist.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia titles-dump", CommandKind.Mutates,
    "Download the enwiki all-titles dump and import it into the Wikipedia cache as a local title-existence check. Re-running skips the download and import when the dump has not changed.",
    Reason = "Replaces the imported all-titles dump with the current one.",
    Rerun = RerunEffect.Rebuilds,
    Examples = new[] {
        "wikipedia titles-dump",
        "wikipedia titles-dump --force",
        "wikipedia titles-dump --file enwiki-20260820-all-titles-in-ns0.gz",
        "wikipedia titles-dump --skip-download --limit 100000"
    })]
public sealed class WikipediaTitlesDumpCommand : AsyncCommand<WikipediaTitlesDumpCommand.Settings> {
    private const string DefaultDumpUrl = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles-in-ns0.gz";
    private const int InsertBatchSize = 50_000;

    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--url <URL>")]
        [Description("Dump URL. Defaults to the latest enwiki all-titles-in-ns0 dump on dumps.wikimedia.org.")]
        public string? Url { get; init; }

        [CommandOption("--file <FILE>")]
        [Description("Import an already-downloaded dump file (.gz or plain text) instead of downloading.")]
        public string? File { get; init; }

        [CommandOption("--skip-download")]
        [Description("Import the previously downloaded copy without checking for a newer dump.")]
        public bool SkipDownload { get; init; }

        [CommandOption("--force")]
        [Description("Re-download and re-import even when the dump looks unchanged.")]
        public bool Force { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Import only the first N titles (for testing). The import is recorded as partial and redone in full next run.")]
        public long Limit { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);
        var cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);

        using var store = WikipediaCacheStore.Open(cachePath);

        string dumpFile;
        string? dumpDate = null;
        var source = settings.Url ?? DefaultDumpUrl;

        if (settings.File is not null) {
            dumpFile = settings.File;
            if (!File.Exists(dumpFile)) {
                AnsiConsole.MarkupLineInterpolated($"[red]Dump file not found:[/] {dumpFile}");
                return -1;
            }
            source = Path.GetFullPath(dumpFile);
            dumpDate = ReadDateFromFileName(dumpFile) ?? File.GetLastWriteTimeUtc(dumpFile).ToString("yyyy-MM-dd");
        }
        else {
            dumpFile = Path.Combine(Path.GetDirectoryName(Path.GetFullPath(cachePath)) ?? ".", "enwiki-latest-all-titles-in-ns0.gz");
            if (settings.SkipDownload) {
                if (!File.Exists(dumpFile)) {
                    AnsiConsole.MarkupLineInterpolated($"[red]--skip-download, but no downloaded copy at[/] {dumpFile}");
                    return -1;
                }
                dumpDate = store.GetDumpInfoValue("download_last_modified")
                    ?? File.GetLastWriteTimeUtc(dumpFile).ToString("yyyy-MM-dd");
            }
            else {
                var downloaded = await DownloadAsync(store, source, dumpFile, settings.Force, cancellationToken).ConfigureAwait(false);
                if (downloaded is null) {
                    return -1;
                }
                dumpDate = downloaded;
            }
        }

        // Same dump already imported in full? Nothing to redo.
        var existing = store.GetDumpInfo();
        if (!settings.Force && settings.Limit <= 0 && existing is { Partial: false } prior
            && prior.DumpDate == dumpDate && !string.IsNullOrEmpty(dumpDate)) {
            AnsiConsole.MarkupLineInterpolated(
                $"[green]Already imported:[/] dump of {dumpDate} · {prior.TitleCount:n0} titles. Use --force to re-import.");
            ReportQueueSplit(store);
            return 0;
        }

        AnsiConsole.MarkupLineInterpolated($"Importing titles from {Path.GetFileName(dumpFile)} (dump of {dumpDate ?? "unknown date"})...");
        var imported = await Task.Run(() => Import(store, dumpFile, settings.Limit, cancellationToken), cancellationToken).ConfigureAwait(false);

        store.RecordDumpImport(new EnwikiDumpInfo(
            dumpDate,
            DateTime.UtcNow,
            imported,
            source,
            Partial: settings.Limit > 0));

        if (settings.Limit > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Partial import:[/] {imported:n0} titles (--limit). Re-run without --limit for the full dump.");
        }
        else {
            AnsiConsole.MarkupLineInterpolated($"[green]Imported[/] {imported:n0} titles.");
        }
        ReportQueueSplit(store);
        return 0;
    }

    private static void ReportQueueSplit(WikipediaCacheStore store) {
        var (inDump, absent) = store.CountQueuedAgainstDump();
        if (inDump + absent == 0) {
            return;
        }
        AnsiConsole.MarkupLineInterpolated(
            $"Fetch queue: {inDump:n0} titles are in the dump; {absent:n0} are not (likely redlinks). Use [blue]wikipedia fetch-pages --exists-first[/] to download the real pages first.");
    }

    // Streams the dump into the cache in batches. All-or-nothing per run: the old dump is
    // cleared first, and the info row that says "a dump is imported" is only written by the
    // caller after this returns, so an interrupted import reads as no dump rather than a
    // complete older one.
    private static long Import(WikipediaCacheStore store, string dumpFile, long limit, CancellationToken cancellationToken) {
        store.ClearDumpTitles();

        using Stream file = File.OpenRead(dumpFile);
        using Stream text = dumpFile.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
            ? new GZipStream(file, CompressionMode.Decompress)
            : file;
        using var reader = new StreamReader(text, Encoding.UTF8);

        long total = 0;
        var batch = new List<string>(InsertBatchSize);
        string? line;
        var first = true;
        while ((line = reader.ReadLine()) is not null) {
            cancellationToken.ThrowIfCancellationRequested();

            // The dump starts with a "page_title" header line.
            if (first) {
                first = false;
                if (line == "page_title") {
                    continue;
                }
            }

            if (line.Length == 0) {
                continue;
            }

            // Dump titles use underscores; normalized_title uses spaces. This is the whole
            // normalization: dump titles are already in canonical first-letter-upper form.
            batch.Add(line.Replace('_', ' '));

            if (batch.Count >= InsertBatchSize) {
                total += store.AddDumpTitles(batch);
                batch.Clear();
                AnsiConsole.MarkupLineInterpolated($"[grey]  {total:n0} titles...[/]");
                if (limit > 0 && total >= limit) {
                    return total;
                }
            }
        }

        total += store.AddDumpTitles(batch);
        return total;
    }

    /// Downloads the dump if the server has a newer one than the last download, resuming a
    /// partial .part file over HTTP Range. Returns the dump date (the server's Last-Modified,
    /// as yyyy-MM-dd), or null on failure.
    private static async Task<string?> DownloadAsync(WikipediaCacheStore store, string url, string dumpFile, bool force, CancellationToken cancellationToken) {
        var configuration = WikipediaConfiguration.FromEnvironment();
        using var client = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        client.DefaultRequestHeaders.UserAgent.ParseAdd(configuration.UserAgent);

        // What does the server have?
        using var head = new HttpRequestMessage(HttpMethod.Head, url);
        using var headResponse = await client.SendAsync(head, cancellationToken).ConfigureAwait(false);
        if (!headResponse.IsSuccessStatusCode) {
            AnsiConsole.MarkupLineInterpolated($"[red]HEAD {url} failed:[/] {(int)headResponse.StatusCode} {headResponse.ReasonPhrase}");
            return null;
        }

        var lastModified = headResponse.Content.Headers.LastModified;
        var contentLength = headResponse.Content.Headers.ContentLength;
        var dumpDate = lastModified?.UtcDateTime.ToString("yyyy-MM-dd");

        var storedModified = store.GetDumpInfoValue("download_last_modified");
        if (!force && File.Exists(dumpFile) && storedModified is not null && dumpDate == storedModified
            && (contentLength is null || new FileInfo(dumpFile).Length == contentLength)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Download unchanged since {storedModified}; using the local copy.[/]");
            return dumpDate;
        }

        var partFile = dumpFile + ".part";
        long resumeFrom = 0;
        // Resume only a partial download of the same dump; a new dump starts over.
        if (!force && File.Exists(partFile) && store.GetDumpInfoValue("download_part_last_modified") == dumpDate && dumpDate is not null) {
            resumeFrom = new FileInfo(partFile).Length;
        }
        else if (File.Exists(partFile)) {
            File.Delete(partFile);
        }

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        if (resumeFrom > 0) {
            request.Headers.Range = new RangeHeaderValue(resumeFrom, null);
        }

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable) {
            // The .part file already holds the whole dump.
        }
        else if (!response.IsSuccessStatusCode) {
            AnsiConsole.MarkupLineInterpolated($"[red]GET {url} failed:[/] {(int)response.StatusCode} {response.ReasonPhrase}");
            return null;
        }
        else {
            var append = response.StatusCode == HttpStatusCode.PartialContent && resumeFrom > 0;
            if (!append) {
                resumeFrom = 0;
            }
            store.SetDumpInfoValue("download_part_last_modified", dumpDate);

            var totalBytes = (response.Content.Headers.ContentLength ?? 0) + resumeFrom;
            var sizeText = totalBytes > 0 ? $"{totalBytes / (1024 * 1024)} MB" : "unknown size";
            if (append) {
                AnsiConsole.MarkupLineInterpolated($"Resuming download at {resumeFrom / (1024 * 1024)} MB of {sizeText}...");
            }
            else {
                AnsiConsole.MarkupLineInterpolated($"Downloading {sizeText}...");
            }

            await using (var output = new FileStream(partFile, append ? FileMode.Append : FileMode.Create, FileAccess.Write))
            await using (var body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false)) {
                await body.CopyToAsync(output, cancellationToken).ConfigureAwait(false);
            }
        }

        File.Move(partFile, dumpFile, overwrite: true);
        store.SetDumpInfoValue("download_last_modified", dumpDate);
        store.SetDumpInfoValue("download_part_last_modified", null);
        AnsiConsole.MarkupLineInterpolated($"[green]Downloaded[/] {Path.GetFileName(dumpFile)} (dump of {dumpDate ?? "unknown date"}).");
        return dumpDate ?? DateTime.UtcNow.ToString("yyyy-MM-dd");
    }

    /// A hand-downloaded file like enwiki-20260820-all-titles-in-ns0.gz carries its own date.
    private static string? ReadDateFromFileName(string path) {
        var name = Path.GetFileName(path);
        var match = System.Text.RegularExpressions.Regex.Match(name, @"enwiki-(\d{4})(\d{2})(\d{2})-");
        return match.Success ? $"{match.Groups[1].Value}-{match.Groups[2].Value}-{match.Groups[3].Value}" : null;
    }
}
