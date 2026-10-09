using System.ComponentModel;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using Spectre.Console;
using Spectre.Console.Cli;

// `checklists import`: downloads a country checklist from another source (ChecklistSources) into the
// checklists folder, unless it is there already, and imports it into the checklists store, replacing
// that source's rows. `checklists status` lists what is imported.

namespace BeastieBot3.Checklists;

[CommandInfo("checklists import", CommandKind.Mutates,
    "Download a country checklist from another source (mdd: Mammal Diversity Database, wcvp: Kew's World Checklist of Vascular Plants, reptiledb: The Reptile Database, amphibiaweb: AmphibiaWeb; gbif: the counts checklists gbif-fetch downloaded, matched to IUCN species through GBIF's backbone) and import it into the checklists store, for checklists crosscheck.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Replaces the source's rows from the downloaded file. The file is downloaded only when it is not in the checklists folder yet, or with --download.",
    Examples = new[] {
        "checklists import --source mdd",
        "checklists import --source all",
        "checklists import --source wcvp --download",
        "checklists import --source reptiledb --file ~/Downloads/reptiledb.zip",
    })]
internal sealed class ChecklistsImportCommand : AsyncCommand<ChecklistsImportCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--source <KEY>")]
        [Description("mdd, wcvp, reptiledb, amphibiaweb, or all.")]
        public string Source { get; init; } = "all";

        [CommandOption("--file <PATH>")]
        [Description("Import this file instead of the downloaded one (one source only).")]
        public string? File { get; init; }

        [CommandOption("--download")]
        [Description("Download the file again even when it is in the checklists folder.")]
        public bool Download { get; init; }

        [CommandOption("--store <PATH>")]
        [Description("Checklists store. Default: Datastore:checklists_sqlite, else checklists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("--site-db <FILE>")]
        [Description("gbif only: the public site's database, whose species are matched to GBIF's. Default: Datastore:site_sqlite in paths.ini.")]
        public string? SiteDatabase { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = settings.StorePath ?? paths.GetChecklistsPath();
        var folder = paths.GetChecklistsDownloadDir();
        if (storePath is null || folder is null) {
            AnsiConsole.MarkupLine("[red]No datastore folder is set:[/] set datastore_dir under [[Datastore]] in paths.ini, or give --store.");
            return -1;
        }
        var all = settings.Source.Equals("all", StringComparison.OrdinalIgnoreCase);
        var gbif = all || settings.Source.Equals(GbifChecklist.Source, StringComparison.OrdinalIgnoreCase);
        var sources = all
            ? ChecklistSources.All
            : ChecklistSources.Find(settings.Source) is { } one ? [one] : [];
        if (sources.Count == 0 && !gbif) {
            AnsiConsole.MarkupLineInterpolated($"[red]Unknown source[/] {settings.Source}: use mdd, wcvp, reptiledb, amphibiaweb, gbif or all.");
            return -1;
        }
        if (settings.File is not null && sources.Count != 1) {
            AnsiConsole.MarkupLine("[red]--file needs one --source.[/]");
            return -1;
        }
        Directory.CreateDirectory(folder);
        using var store = ChecklistStore.Open(storePath);
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(20) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent());
        var failed = false;
        foreach (var source in sources) {
            var file = settings.File ?? Path.Combine(folder, source.FileName);
            try {
                if (settings.File is null && (settings.Download || !System.IO.File.Exists(file))) {
                    AnsiConsole.MarkupLineInterpolated($"[grey]Downloading {source.Title} from[/] {source.Url}");
                    await Download(http, source.Url, file, cancellationToken);
                }
                foreach (var (extraName, extraUrl) in source.ExtraFiles) {
                    var extra = Path.Combine(Path.GetDirectoryName(file) ?? folder, extraName);
                    if (settings.Download || !System.IO.File.Exists(extra)) {
                        AnsiConsole.MarkupLineInterpolated($"[grey]Downloading[/] {extraUrl}");
                        await Download(http, extraUrl, extra, cancellationToken);
                    }
                }
                AnsiConsole.MarkupLineInterpolated($"[grey]Reading[/] {file}");
                var parse = source.Parse(file);
                store.Replace(source.Key, parse.Version, source.Licence, source.Url, parse.Rows, parse.Synonyms, parse.Names, parse.Species, parse.Infraspecific);
                var taxa = parse.Rows.Select(r => r.ScientificName).Distinct(StringComparer.Ordinal).Count();
                AnsiConsole.MarkupLineInterpolated(
                    $"[green]{source.Title}:[/] {ChecklistStore.Count(parse.Rows.Count)} taxon and area rows for {ChecklistStore.Count(taxa)} taxa, {ChecklistStore.Count(parse.Synonyms.Count)} synonyms for matching, {ChecklistStore.Count(parse.Names.Count(n => n.NameType == ChecklistNameTypes.Common))} English names and {ChecklistStore.Count(parse.Names.Count(n => n.NameType == ChecklistNameTypes.Synonym))} synonyms for the site. Licence: {source.Licence}.");
                if (parse.Infraspecific.Count > 0 || parse.InfraspecificNotRead > 0) {
                    var withSubspecies = parse.Infraspecific.Select(i => i.SpeciesName).Distinct(StringComparer.Ordinal).Count();
                    var notes = parse.Infraspecific.Where(i => i.Note is not null).GroupBy(i => i.Note!).OrderBy(g => g.Key, StringComparer.Ordinal)
                        .Select(g => $"{g.Key} {ChecklistStore.Count(g.Count())}").ToList();
                    AnsiConsole.MarkupLineInterpolated(
                        $"  Subspecies: {ChecklistStore.Count(parse.Infraspecific.Count)} of {ChecklistStore.Count(withSubspecies)} species, {ChecklistStore.Count(parse.InfraspecificNotRead)} entries not read{(notes.Count > 0 ? "; notes: " + string.Join(", ", notes) : "")}.");
                }
            } catch (Exception e) when (e is HttpRequestException or IOException or InvalidDataException or KeyNotFoundException or TaskCanceledException) {
                AnsiConsole.MarkupLineInterpolated($"[red]{source.Title} not imported:[/] {e.Message}");
                failed = true;
            }
        }
        if (gbif) {
            failed |= !await ImportGbif(store, http, folder, ChecklistPaths.ExpandHome(settings.SiteDatabase ?? paths.GetSiteDatabasePath()), all, cancellationToken);
        }
        return failed ? 1 : 0;
    }

    // GBIF: the downloaded counts of the species matched through the backbone. With "all", nothing is
    // done until checklists gbif-fetch has downloaded counts.
    private static async Task<bool> ImportGbif(ChecklistStore store, HttpClient http, string folder, string? sitePath, bool quietWhenEmpty,
        CancellationToken cancellationToken) {
        if (store.GbifFetched().Count == 0) {
            if (!quietWhenEmpty) {
                AnsiConsole.MarkupLine("[red]No GBIF counts downloaded yet.[/] Run checklists gbif-fetch first.");
            }
            return quietWhenEmpty;
        }
        if (sitePath is null || !System.IO.File.Exists(sitePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Site database not found:[/] {sitePath ?? "(not set)"}");
            return false;
        }
        var backbone = Path.Combine(folder, GbifChecklist.BackboneFile);
        try {
            if (!System.IO.File.Exists(backbone)) {
                AnsiConsole.MarkupLineInterpolated($"[grey]Downloading GBIF's backbone names (about 490 MB) from[/] {GbifChecklist.BackboneUrl}");
                await Download(http, GbifChecklist.BackboneUrl, backbone, cancellationToken);
            }
            AnsiConsole.MarkupLine("[grey]Matching IUCN species to GBIF's backbone...[/]");
            using var site = ChecklistPaths.OpenReadOnly(sitePath);
            var rows = GbifChecklist.Rows(store, site, backbone, out var matched, out var iucnSpecies);
            store.Replace(GbifChecklist.Source, $"occurrence counts of {store.GbifFetched().Values.Max():yyyy-MM-dd}",
                "per dataset (CC0, CC BY, CC BY-NC)", "https://api.gbif.org/v1/occurrence/search", rows);
            AnsiConsole.MarkupLineInterpolated(
                $"[green]{GbifChecklist.Title}:[/] {ChecklistStore.Count(matched)} of {ChecklistStore.Count(iucnSpecies)} IUCN species matched to GBIF; {ChecklistStore.Count(rows.Count)} species and country rows.");
            return true;
        } catch (Exception e) when (e is HttpRequestException or IOException or InvalidDataException or TaskCanceledException) {
            AnsiConsole.MarkupLineInterpolated($"[red]GBIF not imported:[/] {e.Message}");
            return false;
        }
    }

    private static async Task Download(HttpClient http, string url, string file, CancellationToken cancellationToken) {
        var partial = file + ".part";
        using (var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken)) {
            response.EnsureSuccessStatusCode();
            await using var output = System.IO.File.Create(partial);
            await response.Content.CopyToAsync(output, cancellationToken);
        }
        System.IO.File.Move(partial, file, overwrite: true);
    }

    // Wikimedia's user agent says who runs the bot; the same one is sent to the checklist sources.
    private static string UserAgent() {
        EnvFileLoader.LoadIfPresent();
        return Environment.GetEnvironmentVariable("WIKIPEDIA_USER_AGENT")?.Trim() is { Length: > 0 } agent
            ? agent
            : "BeastieBot3/0.1 (https://en.wikipedia.org/wiki/User:Beastie_Bot)";
    }
}

[CommandInfo("checklists status", CommandKind.ReadOnly, "List the country checklists imported into the checklists store, with their versions, licences and sizes.",
    Examples = new[] { "checklists status" })]
internal sealed class ChecklistsStatusCommand : Command<ChecklistsStatusCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--store <PATH>")]
        [Description("Checklists store. Default: Datastore:checklists_sqlite, else checklists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var storePath = settings.StorePath ?? settings.CreatePaths().GetChecklistsPath();
        using var store = storePath is null ? null : ChecklistStore.OpenReadOnly(storePath);
        if (store is null) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]No checklists imported yet[/] ({storePath ?? "no datastore folder set"}). Run checklists import.");
            return 0;
        }
        var table = new Table().AddColumns("Source", "Version", "Licence", "Taxa", "Rows", "Subspecies", "Imported");
        foreach (var s in store.Sources()) {
            table.AddRow(Markup.Escape(ChecklistSources.TitleOf(s.Source)), Markup.Escape(s.Version ?? ""), Markup.Escape(s.Licence ?? ""),
                ChecklistStore.Count(s.Taxa), ChecklistStore.Count(s.Rows), ChecklistStore.Count(s.Subspecies), s.ImportedAt?.ToString("yyyy-MM-dd HH:mm") ?? "");
        }
        AnsiConsole.Write(table);
        return 0;
    }
}
