using System.ComponentModel;
using System.Globalization;
using System.Text;
using System.Text.Json;
using BeastieBot3.Configuration;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses nztcs-import`: downloads the current assessments and the species names of the New
// Zealand Threat Classification System database (about 17 + 23 requests of 1,000 rows), keeps them
// in the status lists folder as nztcs-<date>.json, and replaces the NZTCS assessments in the status
// lists store with them.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses nztcs-import", CommandKind.Mutates,
    "Download the current assessments of the New Zealand Threat Classification System database (nztcs.org.nz, Department of Conservation; about 16,000 assessments; CC BY 4.0) and store them in the status lists store, each with the scientific name of its species record.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the assessments again, keeps them in the status lists folder with the date in the file name, and replaces the stored NZTCS assessments, so assessments that are no longer current are deleted.",
    Examples = new[] {
        "statuses nztcs-import",
        "statuses nztcs-import --file ~/datastore/status-lists/nztcs-2026-10-08.json",
    })]
internal sealed class NztcsImportCommand : AsyncCommand<NztcsImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Status lists store. Default: Datastore:status_lists_sqlite, else status_lists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this file, kept by an earlier run, instead of downloading.")]
        public string? File { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = paths.ResolveStatusListsPath(settings.StorePath);
        var now = DateTime.UtcNow;

        string file;
        if (settings.File is { } given) {
            file = Path.GetFullPath(given.StartsWith("~/", StringComparison.Ordinal)
                ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), given[2..])
                : given);
            if (!System.IO.File.Exists(file)) {
                AnsiConsole.MarkupLineInterpolated($"[red]File not found:[/] {file}");
                return -1;
            }
        } else {
            var folder = paths.GetStatusListsDownloadDir();
            if (folder is null) {
                AnsiConsole.MarkupLine("[red]No folder for the download:[/] set datastore_dir under [[Datastore]] or status_lists_dir under [[Datasets]] in paths.ini, or give --file.");
                return -1;
            }
            Directory.CreateDirectory(folder);
            file = Path.Combine(folder, $"nztcs-{now:yyyy-MM-dd}.json");
            try {
                await Download(file, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (ex is HttpRequestException or IOException or JsonException
                                             or TaskCanceledException && !cancellationToken.IsCancellationRequested) {
                AnsiConsole.MarkupLineInterpolated($"[red]Download failed:[/] {ex.Message}");
                return 1;
            }
        }

        IReadOnlyList<NztcsAssessment> rows;
        try {
            rows = Read(System.IO.File.ReadAllText(file, Encoding.UTF8));
        } catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not read {Path.GetFileName(file)}:[/] {ex.Message}");
            return 1;
        }
        if (rows.Count == 0) {
            AnsiConsole.MarkupLineInterpolated($"[red]No assessments in {Path.GetFileName(file)}.[/] The store was not changed.");
            return 1;
        }

        AnsiConsole.MarkupLine($"[grey]Status lists store:[/] {Markup.Escape(storePath)}");
        using var store = StatusListStore.Open(storePath);
        store.ReplaceNztcs(rows, now, new StatusSourceInfo(StatusSources.Nztcs, NztcsApi.Title, NztcsApi.SiteUrl, NztcsApi.Licence,
            NztcsApi.Citation(now), Path.GetFileName(file), now, rows.Count));

        var table = new Table().Border(TableBorder.Rounded).AddColumn("NZTCS status").AddColumn(new TableColumn("Assessments").RightAligned());
        foreach (var group in rows.GroupBy(r => NztcsApi.StatusText(r.Category, r.Status) ?? "(not assessed)").OrderByDescending(g => g.Count())) {
            table.AddRow(Markup.Escape(group.Key), group.Count().ToString("N0", CultureInfo.InvariantCulture));
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} current NZTCS assessments[/] of {rows.Select(r => r.SpeciesId).Distinct().Count():N0} species records.");
        var noName = rows.Count(r => r.ScientificName is null);
        if (noName > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]{noName:N0} assessments have no scientific name: an informal name (\"sp.\", \"aff.\", a name in quotes), or a title with no name that could be read and no species record.[/]");
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]File:[/] {file}");
        return 0;
    }

    // The kept file: {"assessments": [...], "species": [...]}, each the rows of every page as NZTCS gave them.
    internal static IReadOnlyList<NztcsAssessment> Read(string json) {
        using var document = JsonDocument.Parse(json);
        var speciesNames = new Dictionary<long, string>();
        foreach (var row in document.RootElement.GetProperty("species").EnumerateArray()) {
            if (NztcsApi.ReadSpecies(row) is { } species) {
                speciesNames[species.SpeciesId] = species.ScientificName;
            }
        }
        return document.RootElement.GetProperty("assessments").EnumerateArray()
            .Select(row => NztcsApi.ReadAssessment(row, speciesNames))
            .OfType<NztcsAssessment>()
            .ToList();
    }

    private static async Task Download(string file, CancellationToken cancellationToken) {
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(NatureServeClient.UserAgent);
        var assessments = await DownloadAll(http, NztcsApi.AssessmentSearchUrl, NztcsApi.AssessmentRequest, "assessments", cancellationToken)
            .ConfigureAwait(false);
        var species = await DownloadAll(http, NztcsApi.SpeciesSearchUrl, NztcsApi.SpeciesRequest, "species records", cancellationToken)
            .ConfigureAwait(false);
        var partial = file + ".part";
        await using (var output = System.IO.File.Create(partial)) {
            await using var writer = new Utf8JsonWriter(output);
            writer.WriteStartObject();
            writer.WritePropertyName("assessments");
            writer.WriteStartArray();
            foreach (var row in assessments) {
                row.WriteTo(writer);
            }
            writer.WriteEndArray();
            writer.WritePropertyName("species");
            writer.WriteStartArray();
            foreach (var row in species) {
                row.WriteTo(writer);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        System.IO.File.Move(partial, file, overwrite: true);
    }

    // Every page of one search, until the rows read reach the total NZTCS gives.
    private static async Task<List<JsonElement>> DownloadAll(HttpClient http, string url, Func<int, string> body, string what,
        CancellationToken cancellationToken) {
        var rows = new List<JsonElement>();
        long total = 0;
        for (var page = 1; ; page++) {
            using var content = new StringContent(body(page), Encoding.UTF8, "application/json");
            using var response = await http.PostAsync(url, content, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            var (pageTotal, pageRows) = NztcsApi.ReadPage(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
            total = pageTotal;
            rows.AddRange(pageRows);
            AnsiConsole.MarkupLineInterpolated($"[grey]NZTCS {what}: {rows.Count:N0} of {total:N0}[/]");
            if (pageRows.Count == 0 || rows.Count >= total) {
                break;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
        }
        if (rows.Count < total) {
            throw new IOException($"NZTCS gave {rows.Count:N0} of the {total:N0} {what} it reported.");
        }
        return rows;
    }
}
