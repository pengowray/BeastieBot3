using System.ComponentModel;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses red-lists-import`: imports the national and subnational red lists in
// rules/status-lists/national-red-lists.yml from GBIF into the status lists store. For each dataset,
// one at a time:
//   1. ask the GBIF registry for the dataset (title, licence, pubDate, DOI, archive endpoint);
//   2. stop when the registry's licence is not CC0, CC BY or CC BY-NC;
//   3. skip the download when the registry's pubDate and the archive URL are those of the last
//      import and its archive is still in the folder (RedListPlan.NeedsDownload);
//   4. download the archive into <status lists folder>/red-lists/<key>-<yyyy-MM-dd>.zip (through a
//      .part file); when its SHA-256 is the last import's, delete the new copy and keep the old one;
//   5. read the archive (RedListArchiveReader) and replace the dataset's rows in one transaction,
//      unless it is the archive last imported and was read by the same RedListArchiveReader.Version
//      (RedListPlan.NeedsImport); a new reader version reads the kept archive again without a download.
// A full run (no --dataset, no --limit) also deletes stored datasets the YAML no longer lists.
// --status prints what is stored and sends no requests.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses red-lists-import", CommandKind.Mutates,
    "Download the national and subnational red lists listed in rules/status-lists/national-red-lists.yml (29 lists of 16 countries in October 2026, published on GBIF as Darwin Core checklists; CC0, CC BY or CC BY-NC) and store each taxon's category, as the list writes it and as an IUCN code when it is one, in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "A run asks the GBIF registry about each list and downloads a list's archive again only when GBIF's publication date has changed (or with --force); an archive identical to the last one imported is not imported again. Each list's rows are replaced in one transaction. A run over the whole file deletes stored lists that the file no longer lists.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "statuses red-lists-import",
        "statuses red-lists-import --dataset se-redlist-2025",
        "statuses red-lists-import --limit 3",
        "statuses red-lists-import --force",
        "statuses red-lists-import --status",
    })]
internal sealed class RedListsImportCommand : AsyncCommand<RedListsImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description(StatusListImportSettings.StoreDescription)]
        public string? StorePath { get; init; }

        [CommandOption("--dataset <KEY>")]
        [Description("Import only this list (its key in national-red-lists.yml, such as se-redlist-2025).")]
        public string? Dataset { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Import only the first N lists of the file (0 = all, the default).")]
        public int? Limit { get; init; }

        [CommandOption("--force")]
        [Description("Download and import every list again, even when GBIF's publication date and the archive have not changed.")]
        public bool Force { get; init; }

        [CommandOption("--status")]
        [Description("Print the lists, when each was last imported and its rows, and exit, without asking GBIF.")]
        public bool Status { get; init; }
    }

    // One request at a time, at least this far apart.
    private static readonly TimeSpan RequestGap = TimeSpan.FromSeconds(1);

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = paths.ResolveStatusListsPath(settings.StorePath);
        IReadOnlyList<RedListDataset> manifest;
        var manifestPath = RedListManifest.PathFor(paths);
        try {
            manifest = RedListManifest.Load(manifestPath);
        } catch (Exception ex) when (ex is InvalidOperationException or YamlDotNet.Core.YamlException) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read the list of red lists:[/] {ex.Message}");
            return -1;
        }
        var selected = manifest.ToList();
        if (settings.Dataset is { } only) {
            selected = manifest.Where(d => string.Equals(d.Key, only.Trim(), StringComparison.OrdinalIgnoreCase)).ToList();
            if (selected.Count == 0) {
                AnsiConsole.MarkupLineInterpolated($"[red]No list has the key {only}[/] in {manifestPath}.");
                return -1;
            }
        }
        if (settings.Limit is > 0) {
            selected = selected.Take(settings.Limit.Value).ToList();
        }

        if (settings.Status) {
            using var readOnly = StatusListStore.OpenReadOnly(storePath);
            PrintStatus(readOnly, manifest, storePath);
            return 0;
        }

        var downloads = paths.GetStatusListsDownloadDir();
        if (downloads is null) {
            AnsiConsole.MarkupLine("[red]No folder for the archives:[/] set datastore_dir under [[Datastore]] or status_lists_dir under [[Datasets]] in paths.ini.");
            return -1;
        }
        var folder = Path.Combine(downloads, "red-lists");
        Directory.CreateDirectory(folder);
        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        AnsiConsole.MarkupLine($"[grey]Archives:[/] {Markup.Escape(folder)}");

        using var store = StatusListStore.Open(storePath);
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(10));
        var results = new List<(RedListDataset Dataset, string Outcome)>();
        var failed = false;
        var first = true;
        foreach (var dataset in selected) {
            cancellationToken.ThrowIfCancellationRequested();
            try {
                var outcome = await ImportOne(store, http, dataset, folder, settings.Force, first, cancellationToken).ConfigureAwait(false);
                results.Add((dataset, outcome));
            } catch (Exception ex) when (ex is HttpRequestException or IOException or InvalidDataException or JsonException
                                             or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                AnsiConsole.MarkupLineInterpolated($"[red]{dataset.Key} not imported:[/] {ex.Message}");
                results.Add((dataset, "failed: " + ex.Message));
                failed = true;
            }
            first = false;
        }

        if (settings.Dataset is null && settings.Limit is not > 0) {
            var listed = manifest.Select(d => d.Key).ToHashSet(StringComparer.Ordinal);
            foreach (var stale in store.RedListDatasets().Where(d => !listed.Contains(d.Key))) {
                store.DeleteRedList(stale.Key);
                AnsiConsole.MarkupLineInterpolated($"[yellow]Deleted {stale.Key}[/] ({stale.ListName}): national-red-lists.yml no longer lists it.");
            }
        }

        var table = new Table().Border(TableBorder.Rounded).AddColumn("List").AddColumn("Result");
        foreach (var (dataset, outcome) in results) {
            table.AddRow(Markup.Escape(dataset.Key), Markup.Escape(outcome));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated($"[grey]Statuses stored for all lists:[/] {store.CountRedListTaxa():N0}");
        return failed ? 1 : 0;
    }

    private static async Task<string> ImportOne(StatusListStore store, HttpClient http, RedListDataset dataset, string folder, bool force,
        bool first, CancellationToken cancellationToken) {
        if (!first) {
            await Task.Delay(RequestGap, cancellationToken).ConfigureAwait(false);
        }
        var registryJson = await http.GetStringAsync(GbifRegistry.DatasetUrl(dataset.GbifKey), cancellationToken).ConfigureAwait(false);
        var registry = GbifRegistry.Parse(registryJson);
        if (registry.Deleted) {
            throw new InvalidDataException("GBIF marks the dataset as deleted.");
        }
        var licence = registry.Licence ?? dataset.Licence;
        if (!RedListManifest.Licences.Contains(licence)) {
            throw new InvalidDataException($"its licence in the GBIF registry is {licence}, which the site cannot use.");
        }
        if (licence != dataset.Licence) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{dataset.Key}: the GBIF registry gives the licence {licence}, not {dataset.Licence}; storing {licence}.[/]");
        }
        var url = dataset.ArchiveUrl ?? registry.ArchiveUrl ?? throw new InvalidDataException("the GBIF registry gives no DWC_ARCHIVE endpoint.");
        var now = DateTime.UtcNow;
        var previous = store.GetRedListDataset(dataset.Key);
        var previousFile = previous is null ? null : Path.Combine(folder, previous.ArchiveFile);

        var previousFileExists = previousFile is not null && File.Exists(previousFile);
        string fileName, file, sha256;
        long size;
        if (!RedListPlan.NeedsDownload(previous, registry.PubDate, url, previousFileExists, force)) {
            if (!RedListPlan.NeedsImport(previous, previous!.ArchiveSha256, force)) {
                store.MarkRedListChecked(dataset.Key, now, registry.PubDate);
                return $"unchanged: GBIF publication date {registry.PubDate}, imported {previous.ImportedAtUtc:yyyy-MM-dd}";
            }
            // Unchanged on GBIF, but stored by an older reader: read the kept archive again.
            (fileName, file, sha256, size) = (previous.ArchiveFile, previousFile!, previous.ArchiveSha256, previous.ArchiveSize);
            AnsiConsole.MarkupLineInterpolated($"[grey]{dataset.Key}: reading the kept archive again[/] {fileName}");
        } else {
            await Task.Delay(RequestGap, cancellationToken).ConfigureAwait(false);
            fileName = $"{dataset.Key}-{now:yyyy-MM-dd}.zip";
            file = Path.Combine(folder, fileName);
            AnsiConsole.MarkupLineInterpolated($"[grey]{dataset.Key}: downloading[/] {url}");
            (sha256, size) = await DownloadAsync(http, url, file + ".part", cancellationToken).ConfigureAwait(false);
            if (previous is not null && previous.ArchiveSha256 == sha256 && previousFileExists) {
                // The same archive as the kept one: keep the old copy, not a second one.
                File.Delete(file + ".part");
                (fileName, file) = (previous.ArchiveFile, previousFile!);
                if (!RedListPlan.NeedsImport(previous, sha256, force)) {
                    store.MarkRedListChecked(dataset.Key, now, registry.PubDate);
                    return $"unchanged: the archive is the one imported {previous.ImportedAtUtc:yyyy-MM-dd}";
                }
            } else {
                File.Move(file + ".part", file, overwrite: true);
            }
        }

        var parse = RedListArchiveReader.Read(file, dataset);
        if (parse.Taxa.Count == 0) {
            throw new InvalidDataException($"{fileName} has no taxa with a status; the stored rows were kept.");
        }
        var record = new RedListDatasetRecord(dataset.Key, dataset.GbifKey, registry.Title.Length > 0 ? registry.Title : dataset.Name, dataset.Name,
            dataset.NameEn, dataset.Year, dataset.Publisher, dataset.Country, dataset.Region, dataset.RegionCode, licence, dataset.Citation,
            registry.Citation, registry.Doi, registry.PubDate, url, fileName, sha256, size, dataset.Notes, now, now, parse.Taxa.Count,
            parse.TaxaWithStatus, parse.Synonyms.Count, RedListArchiveReader.Version);
        store.ReplaceRedList(record, parse);

        var codes = string.Join(", ", parse.Taxa.GroupBy(t => t.ThreatStatus).OrderByDescending(g => g.Count())
            .Select(g => $"{g.Key} {g.Count().ToString("N0", CultureInfo.InvariantCulture)}"));
        AnsiConsole.MarkupLineInterpolated($"[green]{dataset.Key}:[/] {parse.Taxa.Count:N0} statuses of {parse.TaxaWithStatus:N0} taxa, {parse.Synonyms.Count:N0} synonyms. {codes}");
        var skipped = parse.Skipped;
        if (skipped.NoStatus + skipped.NotEvaluated + skipped.NoTaxon + skipped.Misapplied > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]  Left out: {skipped.NoStatus:N0} rows with no status, {skipped.NotEvaluated:N0} NE, {skipped.NoTaxon:N0} rows whose taxon is not in the archive, {skipped.Misapplied:N0} misapplied names.[/]");
        }
        return $"imported {parse.Taxa.Count:N0} statuses of {parse.TaxaWithStatus:N0} taxa from {fileName}";
    }

    // Writes the archive to <partial> and returns its SHA-256 (lower-case hex) and size. A file that
    // is not a zip (an error page) is deleted and refused.
    private static async Task<(string Sha256, long Size)> DownloadAsync(HttpClient http, string url, string partial,
        CancellationToken cancellationToken) {
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long size = 0;
        var head = new byte[4];
        await using (var output = File.Create(partial))
        await using (var input = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false)) {
            var buffer = new byte[81920];
            int read;
            while ((read = await input.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0) {
                if (size < head.Length) {
                    Array.Copy(buffer, 0, head, (int)size, (int)Math.Min(read, head.Length - size));
                }
                hash.AppendData(buffer, 0, read);
                await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                size += read;
            }
        }
        if (size < 4 || head[0] != (byte)'P' || head[1] != (byte)'K') {
            File.Delete(partial);
            throw new InvalidDataException($"{url} did not answer with a zip file ({size:N0} bytes, {response.Content.Headers.ContentType?.MediaType ?? "no content type"}).");
        }
        return (Convert.ToHexStringLower(hash.GetHashAndReset()), size);
    }

    private static void PrintStatus(StatusListStore? store, IReadOnlyList<RedListDataset> manifest, string storePath) {
        var stored = store is not null && store.HasRedListTables()
            ? store.RedListDatasets().ToDictionary(d => d.Key, StringComparer.Ordinal)
            : new Dictionary<string, RedListDatasetRecord>();
        // Narrow enough for an 80-column terminal: the key names the list, and --dataset takes it.
        var table = new Table().Border(TableBorder.Rounded).AddColumn("List").AddColumn("Country").AddColumn("Year").AddColumn("Licence")
            .AddColumn("Imported").AddColumn(new TableColumn("Statuses").RightAligned()).AddColumn(new TableColumn("Synonyms").RightAligned());
        foreach (var dataset in manifest) {
            var country = dataset.RegionCode ?? dataset.Country;
            var year = dataset.Year?.ToString(CultureInfo.InvariantCulture) ?? "";
            if (stored.TryGetValue(dataset.Key, out var record)) {
                table.AddRow(Markup.Escape(dataset.Key), country, year, Markup.Escape(record.Licence),
                    record.ImportedAtUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), record.RowCount.ToString("N0", CultureInfo.InvariantCulture),
                    record.SynonymCount.ToString("N0", CultureInfo.InvariantCulture));
            } else {
                table.AddRow(Markup.Escape(dataset.Key), country, year, Markup.Escape(dataset.Licence), "not yet", "", "");
            }
        }
        AnsiConsole.Write(table);
        foreach (var extra in stored.Values.Where(d => manifest.All(m => m.Key != d.Key))) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{extra.Key} is stored but no longer listed;[/] the next full import deletes it.");
        }
        if (store is null) {
            AnsiConsole.MarkupLineInterpolated($"[grey]No status lists store yet:[/] {storePath}");
        }
    }
}

/// When `statuses red-lists-import` downloads a dataset's archive again.
internal static class RedListPlan {
    /// Always with --force, for a dataset never imported, when its archive is no longer in the folder,
    /// when the archive URL has changed, or when GBIF's pubDate is unknown or differs from the last
    /// import's. Otherwise the archive is taken as unchanged and not downloaded.
    public static bool NeedsDownload(RedListDatasetRecord? previous, string? registryPubDate, string archiveUrl, bool previousFileExists, bool force) =>
        force
        || previous is null
        || !previousFileExists
        || !string.Equals(previous.ArchiveUrl, archiveUrl, StringComparison.Ordinal)
        || registryPubDate is null
        || !string.Equals(previous.PubDate, registryPubDate, StringComparison.Ordinal);

    /// Whether an archive with this SHA-256 is read into the store: always with --force, for a
    /// dataset never imported, for a different archive, or when the stored rows were read by an
    /// older RedListArchiveReader.Version.
    public static bool NeedsImport(RedListDatasetRecord? previous, string sha256, bool force) =>
        force
        || previous is null
        || !string.Equals(previous.ArchiveSha256, sha256, StringComparison.Ordinal)
        || previous.ReaderVersion != RedListArchiveReader.Version;
}
