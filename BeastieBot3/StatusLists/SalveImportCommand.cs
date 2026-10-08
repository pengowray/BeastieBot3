using System.ComponentModel;
using System.Text;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses salve-import`: downloads the current national assessments of Brazil's fauna from SALVE
// (ICMBio; about 31 requests of 500 rows), keeps them in the status lists folder as
// salve-<date>.json, and replaces the SALVE assessments in the status lists store with them.
// StatusListImport runs it.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses salve-import", CommandKind.Mutates,
    "Download the current national assessments of Brazil's fauna from SALVE (salve.icmbio.gov.br, ICMBio; about 15,400 species and subspecies; public, with the source cited) and store them in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the assessments again, keeps them in the status lists folder with the date in the file name, and replaces the stored SALVE assessments, so assessments that are no longer current are deleted.",
    Examples = new[] {
        "statuses salve-import",
        "statuses salve-import --file ~/datastore/status-lists/salve-2026-10-08.json",
    })]
internal sealed class SalveImportCommand : AsyncCommand<SalveImportCommand.Settings> {
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--file <PATH>")]
        [Description("Import this file, kept by an earlier run, instead of downloading.")]
        public override string? File { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return StatusListImport.RunAsync(settings, Spec(), cancellationToken);
    }

    internal static StatusListImportSpec<SalveAssessment> Spec() => new() {
        Source = StatusSources.Salve,
        Title = SalveApi.Title,
        SiteUrl = SalveApi.SiteUrl,
        Licence = SalveApi.Licence,
        Citation = SalveApi.Citation,
        FileStem = "salve",
        FileExtension = "json",
        DownloadNoun = "download",
        RowsNoun = "assessments",
        Download = Download,
        Read = file => Read(File.ReadAllText(file, Encoding.UTF8)),
        IsReadError = ex => ex is JsonException,
        Replace = (store, rows, now, source) => store.ReplaceSalve(rows, now, source),
        GroupColumn = "Category",
        CountColumn = "Assessments",
        GroupOf = r => r.PossiblyExtinct ? r.Category + "(PE)" : r.Category,
        Summary = rows => AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} current SALVE assessments[/], {rows.Count(r => r.Doi is not null):N0} with a DOI."),
    };

    // The kept file: the rows of every page as SALVE gave them, in one array.
    internal static IReadOnlyList<SalveAssessment> Read(string json) {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray().Select(SalveApi.ReadAssessment).OfType<SalveAssessment>().ToList();
    }

    private static async Task Download(string file, CancellationToken cancellationToken) {
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5), acceptJson: true);
        var rows = new List<JsonElement>();
        for (var page = 1; ; page++) {
            using var response = await http.GetAsync(SalveApi.PageUrl(page), cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            var pageRows = SalveApi.ReadPage(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
            rows.AddRange(pageRows);
            AnsiConsole.MarkupLineInterpolated($"[grey]SALVE: {rows.Count:N0} assessments read[/]");
            if (pageRows.Count < SalveApi.PageSize) {
                break;
            }
            if (page >= 200) {
                throw new IOException("SALVE gave more than 200 full pages, which is not what it gave before; stopped.");
            }
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
        }
        await StatusListDownload.WriteJsonArrayAsync(file, rows).ConfigureAwait(false);
    }
}
