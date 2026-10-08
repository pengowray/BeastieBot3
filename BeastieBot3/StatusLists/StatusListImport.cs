using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// The run shared by `statuses ecos-import`, `statuses nztcs-import`, `statuses salve-import`,
// `statuses jncc-import`, `statuses cites-import` and `statuses japan-import`:
//   1. take the file given with --file, or download the source into the status lists folder as
//      <stem>-<yyyy-MM-dd>.<extension>, or under the name the source gives its file (JNCC's name
//      holds the date of its release); StatusListDownload writes it through a .part file. A source
//      of several files (Japan's Red List: eight CSV files and a PDF) is kept as a folder,
//      <stem>-<yyyy-MM-dd>, and its command names a kept folder with --dir;
//   2. read the file into rows; a file with no rows leaves the store as it was;
//   3. replace the source's rows and its status_source row in the status lists store;
//   4. print a table of counts, the command's summary lines and the file's path.
// Each command gives a StatusListImportSpec: how to download, read and store its source, and the
// words its messages use.

namespace BeastieBot3.StatusLists;

/// The options of every status list import. Each command declares both options itself, --store
/// first, so that the Run command page lists them in that order (CommandReflector reads a settings
/// class before its base class) and each can describe the file it reads.
public abstract class StatusListImportSettings : CommonSettings {
    public const string StoreDescription =
        "Status lists store. Default: Datastore:status_lists_sqlite, else status_lists.sqlite in the datastore folder.";

    public abstract string? StorePath { get; init; }

    public abstract string? File { get; init; }
}

/// What a status list import needs to know about its source.
internal sealed class StatusListImportSpec<TRow> {
    // The source's status_source row. The citation takes the time of the import.
    public required string Source { get; init; }
    public required string Title { get; init; }
    public required string SiteUrl { get; init; }
    public required string Licence { get; init; }
    public required Func<DateTime, string> Citation { get; init; }

    /// The downloaded file is named <FileStem>-<yyyy-MM-dd>.<FileExtension>, unless FindFileName is set.
    /// A folder (ImportsFolder) is named <FileStem>-<yyyy-MM-dd>, and FileExtension is not used.
    public required string FileStem { get; init; }
    public required string FileExtension { get; init; }

    /// For a source of several files kept in one folder: the File setting names a folder, Download
    /// is given the folder's path and fills it, and Read reads the folder.
    public bool ImportsFolder { get; init; }

    /// For a source that names its own files: finds the name of the file to download (it may ask the
    /// source), before Download runs. Null: the name above.
    public Func<CancellationToken, Task<string>>? FindFileName { get; init; }

    /// For a source whose status_source row depends on the file imported (JNCC: the file's own URL,
    /// and the year of its release in the attribution): the row for the file, given the row built
    /// from SiteUrl and Citation. Null: that row.
    public Func<string, StatusSourceInfo, StatusSourceInfo>? SourceForFile { get; init; }

    /// Words in the messages: "No folder for the <DownloadNoun>: ..." and "No <RowsNoun> in <file>."
    public required string DownloadNoun { get; init; }
    public required string RowsNoun { get; init; }

    /// Downloads the source into the file. It prints its own progress.
    public required Func<string, CancellationToken, Task> Download { get; init; }

    /// Reads the file into rows. An exception that IsReadError accepts is reported as "Could not read
    /// <file>" and stops the run; any other exception is not caught.
    public required Func<string, IReadOnlyList<TRow>> Read { get; init; }
    public required Func<Exception, bool> IsReadError { get; init; }

    /// Replaces every row of the source and its status_source row, in one transaction.
    public required Action<StatusListStore, IReadOnlyList<TRow>, DateTime, StatusSourceInfo> Replace { get; init; }

    /// The table of counts: its two column headings and the value each row is counted under.
    public required string GroupColumn { get; init; }
    public required string CountColumn { get; init; }
    public required Func<TRow, string> GroupOf { get; init; }

    /// Prints the lines that follow the table.
    public required Action<IReadOnlyList<TRow>> Summary { get; init; }
}

internal static class StatusListImport {
    public static async Task<int> RunAsync<TRow>(StatusListImportSettings settings, StatusListImportSpec<TRow> spec,
        CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var storePath = paths.ResolveStatusListsPath(settings.StorePath);
        var now = DateTime.UtcNow;

        string file;
        if (settings.File is { } given) {
            file = Path.GetFullPath(given.StartsWith("~/", StringComparison.Ordinal)
                ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), given[2..])
                : given);
            if (spec.ImportsFolder ? !Directory.Exists(file) : !File.Exists(file)) {
                AnsiConsole.MarkupLineInterpolated($"[red]{(spec.ImportsFolder ? "Folder" : "File")} not found:[/] {file}");
                return -1;
            }
        } else {
            var folder = paths.GetStatusListsDownloadDir();
            if (folder is null) {
                AnsiConsole.MarkupLine($"[red]No folder for the {Markup.Escape(spec.DownloadNoun)}:[/] set datastore_dir under [[Datastore]] or status_lists_dir under [[Datasets]] in paths.ini, or give --file.");
                return -1;
            }
            Directory.CreateDirectory(folder);
            try {
                var name = spec.FindFileName is { } find ? await find(cancellationToken).ConfigureAwait(false)
                    : spec.ImportsFolder ? $"{spec.FileStem}-{now:yyyy-MM-dd}"
                    : $"{spec.FileStem}-{now:yyyy-MM-dd}.{spec.FileExtension}";
                file = Path.Combine(folder, name);
                await spec.Download(file, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (ex is HttpRequestException or IOException or JsonException
                                             or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
                return 1;
            }
        }

        IReadOnlyList<TRow> rows;
        try {
            rows = spec.Read(file);
        } catch (Exception ex) when (spec.IsReadError(ex)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read {Path.GetFileName(file)}:[/] {ex.Message}");
            return 1;
        }
        if (rows.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[red]No {spec.RowsNoun} in {Path.GetFileName(file)}.[/] The store was not changed.");
            return 1;
        }

        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        using var store = StatusListStore.Open(storePath);
        var source = new StatusSourceInfo(spec.Source, spec.Title, spec.SiteUrl, spec.Licence, spec.Citation(now),
            Path.GetFileName(file), now, rows.Count);
        spec.Replace(store, rows, now, spec.SourceForFile?.Invoke(file, source) ?? source);

        var table = new Table().Border(TableBorder.Rounded).AddColumn(spec.GroupColumn).AddColumn(new TableColumn(spec.CountColumn).RightAligned());
        foreach (var group in rows.GroupBy(spec.GroupOf).OrderByDescending(g => g.Count())) {
            table.AddRow(Markup.Escape(group.Key), group.Count().ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(table);
        spec.Summary(rows);
        AnsiConsole.MarkupLineInterpolated($"[grey]{(spec.ImportsFolder ? "Folder" : "File")}:[/] {file}");
        return 0;
    }
}
