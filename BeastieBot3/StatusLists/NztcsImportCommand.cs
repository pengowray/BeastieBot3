using System.ComponentModel;
using System.Text;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses nztcs-import`: downloads the current assessments and the species names of the New
// Zealand Threat Classification System database (about 17 + 23 requests of 1,000 rows), keeps them
// in the status lists folder as nztcs-<date>.json, and replaces the NZTCS assessments in the status
// lists store with them. StatusListImport runs it.

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
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this file, kept by an earlier run, instead of downloading.")]
        public override string? File { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return StatusListImport.RunAsync(settings, Spec(), cancellationToken);
    }

    internal static StatusListImportSpec<NztcsAssessment> Spec() => new() {
        Source = StatusSources.Nztcs,
        Title = NztcsApi.Title,
        SiteUrl = NztcsApi.SiteUrl,
        Licence = NztcsApi.Licence,
        Citation = NztcsApi.Citation,
        FileStem = "nztcs",
        FileExtension = "json",
        DownloadNoun = "download",
        RowsNoun = "assessments",
        Download = Download,
        Read = file => Read(File.ReadAllText(file, Encoding.UTF8)),
        IsReadError = ex => ex is JsonException or KeyNotFoundException or InvalidOperationException,
        Replace = (store, rows, now, source) => store.ReplaceNztcs(rows, now, source),
        GroupColumn = "NZTCS status",
        CountColumn = "Assessments",
        GroupOf = r => NztcsApi.StatusText(r.Category, r.Status) ?? "(not assessed)",
        Summary = rows => {
            AnsiConsole.MarkupLineInterpolated(
                $"[green]Stored {rows.Count:N0} current NZTCS assessments[/] of {rows.Select(r => r.SpeciesId).Distinct().Count():N0} species records.");
            var noName = rows.Count(r => r.ScientificName is null);
            if (noName > 0) {
                AnsiConsole.MarkupLineInterpolated($"[grey]{noName:N0} assessments have no scientific name: an informal name (\"sp.\", \"aff.\", a name in quotes), or a title with no name that could be read and no species record.[/]");
            }
        },
    };

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
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5));
        var assessments = await DownloadAll(http, NztcsApi.AssessmentSearchUrl, NztcsApi.AssessmentRequest, "assessments", cancellationToken)
            .ConfigureAwait(false);
        var species = await DownloadAll(http, NztcsApi.SpeciesSearchUrl, NztcsApi.SpeciesRequest, "species records", cancellationToken)
            .ConfigureAwait(false);
        await StatusListDownload.WriteJsonObjectAsync(file, ("assessments", assessments), ("species", species)).ConfigureAwait(false);
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
