using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.StatusLists;
using Spectre.Console;
using Spectre.Console.Cli;

// `iucn summary-tables`: downloads IUCN's summary statistics Table 7 (species changing Red List
// category, with the reason for each change) and Table 9 (Possibly Extinct species, 2014 to 2020)
// from the list in rules/iucn-summary-tables.yml, reads their rows, and stores them in
// Datastore:IUCN_summary_tables_sqlite for `site build-db`. A re-run downloads only files it does
// not have and reads again only files that changed or that an older parser read. It also looks for
// the Table 7 of the releases after the newest one it knows, under the names IUCN has used.

namespace BeastieBot3.Iucn.SummaryTables;

[CommandInfo("iucn summary-tables", CommandKind.Mutates,
    "Download IUCN's summary statistics tables 7 (reasons for category changes) and 9 (Possibly Extinct species) and store their rows.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the tables it does not have yet, including the Table 7 of a new release, and reads again only the tables that changed.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "iucn summary-tables",
        "iucn summary-tables --status",
        "iucn summary-tables --release 2026-2",
    })]
internal sealed class IucnSummaryTablesCommand : AsyncCommand<IucnSummaryTablesCommand.Settings> {
    // Option order is the order the web UI shows the fields in.
    public sealed class Settings : CommonSettings {
        [CommandOption("--status")]
        [Description("Print the stored tables and the listed tables not yet downloaded, then exit. Downloads nothing.")]
        public bool Status { get; init; }

        [CommandOption("--release <RELEASE>")]
        [Description("Also look for this release's Table 7 (for example 2026-2) under the file names IUCN has used.")]
        public string? Release { get; init; }

        [CommandOption("--no-discover")]
        [Description("Do not look for the Table 7 of releases after the newest one known.")]
        public bool NoDiscover { get; init; }

        [CommandOption("--reparse")]
        [Description("Read every downloaded table again, even when it has not changed.")]
        public bool Reparse { get; init; }

        [CommandOption("--force")]
        [Description("Download every listed table again. Each old copy is replaced only when its new copy has arrived.")]
        public bool Force { get; init; }

        [CommandOption("--show-unread")]
        [Description("List every line under a table's header row that was not read as a row or a heading.")]
        public bool ShowUnread { get; init; }

        [CommandOption("--dir <PATH>")]
        [Description("Folder for the PDF files (defaults to Datasets:IUCN_summary_tables_dir, else an iucn-summary-tables folder in the datasets folder).")]
        public string? Directory { get; init; }

        [CommandOption("--store <PATH>")]
        [Description("Override the database path (defaults to Datastore:IUCN_summary_tables_sqlite, else iucn_summary_tables.sqlite in the datastore folder).")]
        public string? StorePath { get; init; }
    }

    private static readonly TimeSpan DelayBetweenDownloads = TimeSpan.FromSeconds(1);

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = settings.StorePath ?? paths.GetIucnSummaryTablesPath()
            ?? throw new InvalidOperationException("Set Datastore:IUCN_summary_tables_sqlite or datastore_dir in paths.ini, or pass --store.");
        var dir = settings.Directory ?? paths.GetIucnSummaryTablesDir()
            ?? throw new InvalidOperationException("Set Datasets:IUCN_summary_tables_dir or datasets_dir in paths.ini, or pass --dir.");
        var listed = SummaryTableManifest.LoadForPaths(paths);
        AnsiConsole.MarkupLineInterpolated($"[grey]Database:[/] {storePath}");
        AnsiConsole.MarkupLineInterpolated($"[grey]PDF folder:[/] {dir}");

        if (settings.Status) {
            using var readOnly = SummaryTableStore.OpenReadOnly(storePath);
            PrintStatus(readOnly?.GetSources() ?? new Dictionary<string, SummaryTableSource>(), listed, dir);
            return 0;
        }

        Directory.CreateDirectory(dir);
        using var store = SummaryTableStore.Open(storePath);
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(2));
        var stored = store.GetSources();

        // Files found in earlier runs that the list does not have yet keep their place after the list.
        var work = listed.ToList();
        foreach (var source in stored.Values.Where(s => !listed.Any(f => f.FileName.Equals(s.FileName, StringComparison.OrdinalIgnoreCase)))
                     .OrderBy(s => s.Priority)) {
            work.Add(new SummaryTableFile(source.Table, source.Release, source.Url, null, source.Note, work.Count + 1));
        }

        var results = new List<FileResult>();
        var downloaded = 0;
        foreach (var file in work) {
            cancellationToken.ThrowIfCancellationRequested();
            var path = Path.Combine(dir, file.FileName);
            if (settings.Force || !File.Exists(path)) {
                if (downloaded++ > 0) await Task.Delay(DelayBetweenDownloads, cancellationToken).ConfigureAwait(false);
                var fetched = await DownloadAsync(http, file, path, cancellationToken).ConfigureAwait(false);
                if (fetched is not null) {
                    results.Add(new FileResult(file, "not downloaded: " + fetched, null));
                    continue;
                }
            }
            results.Add(ReadFile(store, stored, file, path, settings.Reparse));
        }

        // The Table 7 of releases after the newest one known.
        var candidates = new List<string>();
        if (settings.Release is { } release) candidates.Add(release.Trim());
        if (!settings.NoDiscover) {
            var newest = work.Where(f => f.Table == 7).Select(f => f.Release).OrderBy(ReleaseKey).LastOrDefault();
            if (newest is not null) candidates.AddRange(SummaryTableManifest.NextReleases(newest));
        }
        foreach (var candidate in candidates.Distinct(StringComparer.Ordinal)) {
            if (work.Any(f => f.Table == 7 && f.Release == candidate)) continue;
            var found = false;
            foreach (var url in SummaryTableManifest.Table7Urls(candidate)) {
                var file = new SummaryTableFile(7, candidate, url, null, null, work.Count + 1);
                var path = Path.Combine(dir, file.FileName);
                if (!File.Exists(path)) {
                    await Task.Delay(DelayBetweenDownloads, cancellationToken).ConfigureAwait(false);
                    if (await DownloadAsync(http, file, path, cancellationToken).ConfigureAwait(false) is not null) continue;
                }
                AnsiConsole.MarkupLineInterpolated($"[green]Found the Table 7 of release {candidate}:[/] {url}");
                AnsiConsole.MarkupLineInterpolated($"  Add it to rules/{SummaryTableManifest.FileName}.");
                work.Add(file);
                results.Add(ReadFile(store, stored, file, path, settings.Reparse));
                found = true;
                break;
            }
            if (!found) AnsiConsole.MarkupLineInterpolated($"[grey]No Table 7 published for release {candidate} yet.[/]");
        }

        PrintResults(results, settings.ShowUnread);
        return results.Any(r => r.Error is not null && !r.Error.StartsWith("not downloaded", StringComparison.Ordinal)) ? 1 : 0;
    }

    // ------------------------------------------------------------ download

    /// Null when the file was saved; otherwise why not.
    private static async Task<string?> DownloadAsync(HttpClient http, SummaryTableFile file, string path, CancellationToken ct) {
        var reason = await TryDownloadAsync(http, file.Url, path, ct).ConfigureAwait(false);
        if (reason is null) return null;
        if (file.ArchiveUrl is { } archive) {
            var archiveReason = await TryDownloadAsync(http, archive, path, ct).ConfigureAwait(false);
            if (archiveReason is null) return null;
            return $"{reason}; Internet Archive copy: {archiveReason}";
        }
        return reason;
    }

    private static async Task<string?> TryDownloadAsync(HttpClient http, string url, string path, CancellationToken ct) {
        try {
            using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
            if (response.StatusCode != HttpStatusCode.OK) return $"HTTP {(int)response.StatusCode}";
            var bytes = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            if (bytes.Length < 5 || bytes[0] != '%' || bytes[1] != 'P' || bytes[2] != 'D' || bytes[3] != 'F') return "the answer is not a PDF";
            var partial = path + ".part";
            await File.WriteAllBytesAsync(partial, bytes, ct).ConfigureAwait(false);
            File.Move(partial, path, overwrite: true);
            return null;
        } catch (HttpRequestException ex) {
            return ex.Message;
        }
    }

    // ------------------------------------------------------------ read

    private sealed record FileResult(SummaryTableFile File, string? Error, ReadOutcome? Outcome);

    private sealed record ReadOutcome(bool Skipped, int Rows, int Anomalies, IReadOnlyList<string> Unread,
        IReadOnlyList<string> AnomalyLines, SummaryTableInfo? Info);

    private static FileResult ReadFile(SummaryTableStore store, Dictionary<string, SummaryTableSource> stored,
        SummaryTableFile file, string path, bool reparse) {
        var bytes = File.ReadAllBytes(path);
        var sha = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        if (!reparse && stored.TryGetValue(file.FileName, out var existing) && existing.Sha256 == sha
            && existing.ParserVersion == SummaryTableParser.Version && existing.Table == file.Table && existing.Release == file.Release) {
            store.SetPriority(file.FileName, file.Priority, file.Note);
            return new FileResult(file, null, new ReadOutcome(true, existing.RowCount, existing.AnomalyCount, [], [], null));
        }
        try {
            var lines = PdfLines.Read(path, out var pages);
            var facts = new SummaryTableStore.FileFacts(file.FileName, file.Table, file.Release, file.Url, sha, bytes.LongLength,
                File.GetLastWriteTimeUtc(path).ToString("O", CultureInfo.InvariantCulture), SummaryTableParser.Version,
                file.Priority, file.Note, pages);
            if (file.Table == 7) {
                var parse = SummaryTableParser.ParseTable7(lines);
                if (!parse.FoundHeader) return new FileResult(file, "no header row (\"Scientific name\") found", null);
                store.ReplaceTable7(facts, parse);
                return new FileResult(file, null, new ReadOutcome(false, parse.Rows.Count, parse.Rows.Count(r => r.Anomaly is not null),
                    parse.Unread, parse.Rows.Where(r => r.Anomaly is not null).Select(r => $"p{r.Page}: {r.ScientificName}: {r.Anomaly}").ToList(), parse.Info));
            } else {
                var parse = SummaryTableParser.ParseTable9(lines);
                if (!parse.FoundHeader) return new FileResult(file, "no header row (\"Scientific name\") found", null);
                store.ReplaceTable9(facts, parse);
                return new FileResult(file, null, new ReadOutcome(false, parse.Rows.Count, parse.Rows.Count(r => r.Anomaly is not null),
                    parse.Unread, parse.Rows.Where(r => r.Anomaly is not null).Select(r => $"p{r.Page}: {r.ScientificName}: {r.Anomaly}").ToList(), parse.Info));
            }
        } catch (Exception ex) when (ex is not OperationCanceledException) {
            return new FileResult(file, $"could not be read: {ex.Message}", null);
        }
    }

    /// Sorts "2009" before "2010-3" before "2010-4".
    internal static (int Year, int Number) ReleaseKey(string release) {
        var parts = release.Split('-');
        var year = int.TryParse(parts[0], out var y) ? y : 0;
        var number = parts.Length > 1 && int.TryParse(parts[1], out var n) ? n : 0;
        return (year, number);
    }

    // ------------------------------------------------------------ output

    private const int MaxListed = 15;

    private static void PrintResults(List<FileResult> results, bool showUnread) {
        var table = new Table().Border(TableBorder.Rounded).Title("Tables read in this run");
        table.AddColumns("Table", "Release", "File", "Rows", "Rows with unread cells", "Other lines not read", "Last updated");
        foreach (var result in results.Where(r => r.Outcome is { Skipped: false } || r.Error is not null)) {
            var o = result.Outcome;
            table.AddRow(
                result.File.Table.ToString(CultureInfo.InvariantCulture), Markup.Escape(result.File.Release), Markup.Escape(result.File.FileName),
                o is null ? $"[red]{Markup.Escape(result.Error ?? "")}[/]" : o.Rows.ToString("N0", CultureInfo.InvariantCulture),
                o?.Anomalies.ToString("N0", CultureInfo.InvariantCulture) ?? "",
                o?.Unread.Count.ToString("N0", CultureInfo.InvariantCulture) ?? "",
                Markup.Escape(o?.Info?.LastUpdated ?? ""));
        }
        if (table.Rows.Count > 0) AnsiConsole.Write(table);

        var skipped = results.Count(r => r.Outcome is { Skipped: true });
        AnsiConsole.MarkupLineInterpolated($"Tables unchanged since they were last read: {skipped:N0}");
        AnsiConsole.MarkupLineInterpolated($"Tables read in this run: {results.Count(r => r.Outcome is { Skipped: false }):N0}");
        var failed = results.Where(r => r.Error is not null).ToList();
        if (failed.Count > 0) AnsiConsole.MarkupLineInterpolated($"[yellow]Tables not stored: {failed.Count:N0}[/]");

        foreach (var result in results.Where(r => r.Outcome is { Skipped: false })) {
            var o = result.Outcome!;
            if (o.Info is { } info && info.Release is { } inFile && !SameRelease(inFile, result.File.Release)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]{result.File.FileName}: the page header says release {inFile}, the list says {result.File.Release}.[/]");
            }
            WriteLines($"{result.File.FileName}: rows with unread cells", o.AnomalyLines, showUnread);
            WriteLines($"{result.File.FileName}: lines not read", o.Unread, showUnread);
        }
    }

    private static bool SameRelease(string a, string b) => a == b || a.Replace('.', '-') == b;

    private static void WriteLines(string heading, IReadOnlyList<string> lines, bool all) {
        if (lines.Count == 0) return;
        var shown = all ? lines.Count : Math.Min(lines.Count, MaxListed);
        AnsiConsole.MarkupLine($"[bold]{Markup.Escape(heading)}[/] ({shown:N0} of {lines.Count:N0}{(shown < lines.Count ? "; --show-unread lists all" : "")}):");
        foreach (var line in lines.Take(shown)) {
            AnsiConsole.MarkupLineInterpolated($"  {line}");
        }
    }

    private static void PrintStatus(Dictionary<string, SummaryTableSource> stored, IReadOnlyList<SummaryTableFile> listed, string dir) {
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumns("Table", "Release", "File", "Rows", "Rows with unread cells", "Other lines not read", "Period", "Last updated");
        foreach (var source in stored.Values.OrderBy(s => s.Table).ThenBy(s => s.Priority)) {
            table.AddRow(source.Table.ToString(CultureInfo.InvariantCulture), Markup.Escape(source.Release), Markup.Escape(source.FileName),
                source.RowCount.ToString("N0", CultureInfo.InvariantCulture), source.AnomalyCount.ToString("N0", CultureInfo.InvariantCulture),
                source.UnreadCount.ToString("N0", CultureInfo.InvariantCulture),
                Markup.Escape(source.PeriodFrom is null ? "" : $"{source.PeriodFrom} to {source.PeriodTo}"),
                Markup.Escape(source.LastUpdated ?? ""));
        }
        AnsiConsole.Write(table);
        foreach (var kind in new[] { 7, 9 }) {
            var sources = stored.Values.Where(s => s.Table == kind).ToList();
            AnsiConsole.MarkupLineInterpolated($"Table {kind}: {sources.Count:N0} files, {sources.Sum(s => s.RowCount):N0} rows, {sources.Sum(s => s.AnomalyCount):N0} rows with unread cells");
        }
        var missing = listed.Where(f => !stored.ContainsKey(f.FileName)).ToList();
        if (missing.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Listed tables not stored yet: {missing.Count:N0}[/]");
            foreach (var file in missing.Take(MaxListed)) {
                var local = File.Exists(Path.Combine(dir, file.FileName)) ? "downloaded, not read" : "not downloaded";
                AnsiConsole.MarkupLineInterpolated($"  Table {file.Table} {file.Release}: {file.FileName} ({local})");
            }
        }
        var stale = stored.Values.Count(s => s.ParserVersion != SummaryTableParser.Version);
        if (stale > 0) AnsiConsole.MarkupLineInterpolated($"[yellow]{stale:N0} tables were read by an older parser; the next run reads them again.[/]");
    }
}
