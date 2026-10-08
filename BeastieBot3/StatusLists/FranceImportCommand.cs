using System.ComponentModel;
using System.Globalization;
using CsvHelper;
using Spectre.Console;
using Spectre.Console.Cli;

// `statuses france-import`: downloads PatriNat's BDC Statuts (BDC.zip) and TAXREF (the TAXREF zip)
// into the status lists folder, each kept under its name on PatriNat's server and the date the
// server gives the file (BDC-2025-11-28.zip, TAXREF_v18_2025-2025-11-28.zip; a file already there
// is read again, not downloaded again), and replaces the French statuses, TAXREF names and links in
// the status lists store. StatusListImport runs it.
//
// The files are on PatriNat's temporary download page while INPN is down. When an address stops
// working the command stops before changing the store, and its message names that page and the
// option that takes the file's new address.

namespace BeastieBot3.StatusLists;

[CommandInfo("statuses france-import", CommandKind.Mutates,
    "Download PatriNat's BDC Statuts (statuses of species in France, linked to TAXREF; Licence Ouverte 2.0) and TAXREF, and store the French national and regional red lists and the national, overseas, regional and departmental protection lists in the status lists store, with each listed taxon's TAXREF names and its IUCN, BirdLife, Catalogue of Life and GBIF ids.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run asks PatriNat's server for the date of each file, downloads a file only when the status lists folder has no copy with that date, and replaces the stored French statuses and TAXREF names and links, so rows that the new files do not have are deleted.",
    Examples = new[] {
        "statuses france-import",
        "statuses france-import --file ~/datastore/status-lists/BDC-2025-11-28.zip --taxref-file ~/datastore/status-lists/TAXREF_v18_2025-2025-11-28.zip",
    })]
internal sealed class FranceImportCommand : AsyncCommand<FranceImportCommand.Settings> {
    public sealed class Settings : StatusListImportSettings {
        [CommandOption("--store <PATH>")]
        [Description(StoreDescription)]
        public override string? StorePath { get; init; }

        [CommandOption("--file <PATH>")]
        [Description("Import this BDC zip (BDC.zip from PatriNat, or a copy kept by an earlier run) instead of downloading. Give --taxref-file with it.")]
        public override string? File { get; init; }

        [CommandOption("--taxref-file <PATH>")]
        [Description("The TAXREF zip to read with --file (TAXREF_v18_2025.zip from PatriNat, or a kept copy). Use the TAXREF version that the BDC was built on: TAXREF v18 for BDC_18.")]
        public string? TaxrefFile { get; init; }

        [CommandOption("--bdc-url <URL>")]
        [Description("Address of BDC.zip. Default: " + FranceBdc.DownloadUrl)]
        public string? BdcUrl { get; init; }

        [CommandOption("--taxref-url <URL>")]
        [Description("Address of the TAXREF zip. Default: " + FranceTaxref.DownloadUrl)]
        public string? TaxrefUrl { get; init; }
    }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        if ((settings.File is null) != (settings.TaxrefFile is null)) {
            AnsiConsole.MarkupLine("[red]Give --file and --taxref-file together:[/] the import reads the BDC zip and the TAXREF zip.");
            return Task.FromResult(-1);
        }
        string? taxrefFile = null;
        if (settings.TaxrefFile is { } given) {
            taxrefFile = StatusListImport.FullPath(given);
            if (!System.IO.File.Exists(taxrefFile)) {
                AnsiConsole.MarkupLineInterpolated($"[red]File not found:[/] {taxrefFile}");
                return Task.FromResult(-1);
            }
        }
        return StatusListImport.RunAsync(settings, Spec(taxrefFile, settings.BdcUrl ?? FranceBdc.DownloadUrl, settings.TaxrefUrl ?? FranceTaxref.DownloadUrl),
            cancellationToken);
    }

    /// A file on PatriNat's server: its address, the name it is kept under, and the option that
    /// gives its address.
    private sealed record RemoteFile(string Url, string KeptName, string Option);

    /// <paramref name="taxrefFile"/>: the TAXREF zip given with --taxref-file; null to download it.
    internal static StatusListImportSpec<FranceStatusRow> Spec(string? taxrefFile, string bdcUrl = FranceBdc.DownloadUrl,
        string taxrefUrl = FranceTaxref.DownloadUrl) {
        // Set by the steps of the run: the files on the server (a download only), the TAXREF zip's
        // path, and what was read.
        RemoteFile? bdcRemote = null, taxrefRemote = null;
        var taxrefPath = taxrefFile;
        FranceBdcData? bdc = null;
        FranceTaxrefData? taxref = null;
        return new StatusListImportSpec<FranceStatusRow> {
            Source = StatusSources.France,
            Title = FranceBdc.Title,
            SiteUrl = FranceBdc.DataGouvUrl,
            Licence = FranceBdc.Licence,
            // Replaced by SourceForFile, which knows the version and date of the file read.
            Citation = _ => "",
            FileStem = "BDC",
            FileExtension = "zip",
            FindFileName = async cancellationToken => {
                using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(2));
                bdcRemote = await Probe(http, bdcUrl, "--bdc-url", cancellationToken).ConfigureAwait(false);
                taxrefRemote = await Probe(http, taxrefUrl, "--taxref-url", cancellationToken).ConfigureAwait(false);
                return bdcRemote.KeptName;
            },
            SourceForFile = (_, source) => source with {
                Citation = bdc is { Version: { Length: > 0 } version, CsvDate: { } date } ? FranceBdc.Citation(version, date, bdc.FileCount) : null,
            },
            DownloadNoun = "download",
            RowsNoun = "statuses",
            Download = async (file, cancellationToken) => {
                using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(10));
                await Fetch(http, bdcRemote!, file, cancellationToken).ConfigureAwait(false);
                taxrefPath = Path.Combine(Path.GetDirectoryName(file)!, taxrefRemote!.KeptName);
                await Fetch(http, taxrefRemote, taxrefPath, cancellationToken).ConfigureAwait(false);
            },
            Read = file => {
                AnsiConsole.MarkupLineInterpolated($"[grey]Reading[/] {file}");
                bdc = FranceBdc.ReadZip(file);
                AnsiConsole.MarkupLineInterpolated($"[grey]Reading[/] {taxrefPath}");
                taxref = FranceTaxref.ReadZip(taxrefPath!, bdc.Rows.Select(r => r.CdRef).ToHashSet());
                return bdc.Rows;
            },
            IsReadError = ex => ex is InvalidDataException or CsvHelperException,
            Replace = (store, _, now, source) => store.ReplaceFrance(bdc!, taxref!, now, source, new StatusSourceInfo(StatusSources.Taxref,
                FranceTaxref.Title, FranceTaxref.DataGouvUrl, FranceBdc.Licence, FranceTaxref.Citation(taxref!.Version, taxref.Date, taxref.DataFileCount),
                Path.GetFileName(taxrefPath), now, taxref.Names.Count)),
            GroupColumn = "Status type",
            CountColumn = "Rows",
            GroupOf = r => bdc!.Types.FirstOrDefault(t => t.Code == r.TypeCode) is { } type ? $"{type.Code} ({type.Label})" : r.TypeCode,
            Summary = _ => Summary(bdc!, taxref!),
        };
    }

    private static void Summary(FranceBdcData bdc, FranceTaxrefData taxref) {
        string N(long n) => n.ToString("N0", CultureInfo.InvariantCulture);
        var rows = bdc.Rows;
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {N(rows.Count)} statuses[/] of {N(rows.Select(r => r.CdRef).Distinct().Count())} taxa from BDC version {bdc.Version ?? "?"} ({N(bdc.RowsRead)} rows read; the other status types are not stored), with {N(bdc.Documents.Count)} documents and {N(bdc.Territories.Count)} places.");

        // The national red list by place.
        var territories = bdc.Territories.ToDictionary(t => t.Code, StringComparer.Ordinal);
        var national = rows.Where(r => r.TypeCode == "LRN").ToList();
        var table = new Table().Border(TableBorder.Rounded).Title("National red list (LRN)")
            .AddColumn("Place").AddColumn(new TableColumn("Rows").RightAligned()).AddColumn(new TableColumn("Taxa").RightAligned())
            .AddColumn(new TableColumn("Current rows").RightAligned());
        foreach (var place in national.GroupBy(r => r.TerritoryCode).OrderByDescending(g => g.Count())) {
            var name = territories.TryGetValue(place.Key, out var t) ? t.NameEn ?? t.Name : place.Key;
            table.AddRow(Markup.Escape($"{name} ({place.Key})"), N(place.Count()), N(place.Select(r => r.CdRef).Distinct().Count()),
                N(place.Count(r => r.IsCurrent == true)));
        }
        table.AddRow("All places", N(national.Count), N(national.Select(r => r.CdRef).Distinct().Count()), N(national.Count(r => r.IsCurrent == true)));
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]National red list categories:[/] {string.Join("; ", national.GroupBy(r => r.Code).OrderByDescending(g => g.Count()).Select(g => $"{g.Key} {N(g.Count())}"))}");

        // The current rows of the red lists.
        var years = bdc.Documents.ToDictionary(d => d.CdDoc, d => d.Year);
        int Year(FranceStatusRow row) => row.CdDoc is { } doc && years.TryGetValue(doc, out var year) && year is { } y ? y : int.MinValue;
        foreach (var type in FranceBdc.RedListTypes.Order(StringComparer.Ordinal)) {
            var groups = rows.Where(r => r.TypeCode == type).GroupBy(r => (r.CdRef, r.TerritoryCode, r.Remark.Population ?? "")).ToList();
            var older = groups.Sum(g => g.Count(r => Year(r) < g.Max(Year)));
            var otherName = groups.Sum(g => g.Count(r => r.IsCurrent == false && Year(r) == g.Max(Year)));
            var twoCurrent = groups.Where(g => g.Count(r => r.IsCurrent == true) > 1).ToList();
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]{type} rows that are not current: {N(older)} of an older list for the same taxon, place and population; {N(otherName)} of another name of a taxon that has a row under its accepted name in the same list.[/]");
            if (twoCurrent.Count > 0) {
                var examples = twoCurrent.Take(4).Select(g =>
                    $"{g.First(r => r.IsCurrent == true).Name} ({g.Key.TerritoryCode}: {string.Join(", ", g.Where(r => r.IsCurrent == true).Select(r => r.Code))})");
                AnsiConsole.MarkupLineInterpolated(
                    $"[yellow]{type}: {N(twoCurrent.Count)} taxa have two or more current rows for one place and population (rows for two names that TAXREF now treats as one taxon, two lists of the same year, or one list of several populations whose rows the BDC does not label):[/] {string.Join("; ", examples)}{(twoCurrent.Count > 4 ? " ..." : "")}");
            }
        }
        if (bdc.PopulationsFromTitles > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[grey]{N(bdc.PopulationsFromTitles)} bird rows take their population from their list's title (\"oiseaux nicheurs\": breeding).[/]");
        }
        if (bdc.SentenceRemarks > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]{N(bdc.SentenceRemarks)} red list remarks are sentences and are not stored.[/]");
        }
        if (bdc.Skipped > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {N(bdc.Skipped)} rows with no type, TAXREF id, status code, place or name.[/]");
        }

        // TAXREF.
        var links = taxref.Links.GroupBy(l => l.Source).OrderByDescending(g => g.Count()).Select(g => $"{g.Key} {N(g.Count())}");
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {N(taxref.Names.Count)} TAXREF v{taxref.Version} names[/] of {N(taxref.Names.Select(n => n.CdRef).Distinct().Count())} accepted names, and {N(taxref.Links.Count)} links: {string.Join("; ", links)}.");
        var iucn = taxref.Links.Where(l => l.Source is FranceTaxref.IucnRedList or FranceTaxref.BirdLife).Select(l => l.CdRef).ToHashSet();
        var nationalTaxa = national.Select(r => r.CdRef).Distinct().ToList();
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]National red list taxa with an IUCN taxon id in TAXREF: {N(nationalTaxa.Count(iucn.Contains))} of {N(nationalTaxa.Count)}.[/]");

        if (bdc.Version is { } bdcVersion && bdcVersion != taxref.Version) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]The BDC is version {bdcVersion} and TAXREF is v{taxref.Version}: the BDC's accepted names (CD_REF) are those of TAXREF v{bdcVersion}, so some names and links may not match.[/]");
        }
        var accepted = taxref.Names.ToDictionary(n => n.CdNom, n => n.CdRef);
        var missing = rows.Select(r => r.CdNom).Distinct().Count(n => !accepted.ContainsKey(n));
        var moved = rows.Where(r => accepted.TryGetValue(r.CdNom, out var cdRef) && cdRef != r.CdRef).Select(r => r.CdNom).Distinct().Count();
        if (missing > 0 || moved > 0) {
            AnsiConsole.MarkupLineInterpolated(
                $"[yellow]TAXREF and the BDC differ on {N(missing + moved)} names of the stored statuses: {N(missing)} are not among the TAXREF names read, and {N(moved)} have another accepted name in TAXREF.[/]");
        }
    }

    // Asks the server for a file's type and date (HEAD), to name the kept copy.
    private static async Task<RemoteFile> Probe(HttpClient http, string url, string option, CancellationToken cancellationToken) {
        AnsiConsole.MarkupLineInterpolated($"[grey]Checking[/] {url}");
        HttpResponseMessage response;
        try {
            using var request = new HttpRequestMessage(HttpMethod.Head, url);
            response = await http.SendAsync(request, cancellationToken).ConfigureAwait(false);
        } catch (HttpRequestException ex) {
            throw new IOException(FranceBdc.MissingFileMessage(url, ex.Message, option), ex);
        }
        using (response) {
            if (!response.IsSuccessStatusCode) {
                throw new IOException(FranceBdc.MissingFileMessage(url, $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}", option));
            }
            if (response.Content.Headers.ContentType?.MediaType is { } type && type.Contains("html", StringComparison.OrdinalIgnoreCase)) {
                throw new IOException(FranceBdc.MissingFileMessage(url, $"the server answers with a web page ({type}), not a zip file", option));
            }
            var date = response.Content.Headers.LastModified?.UtcDateTime ?? DateTime.UtcNow;
            var stem = Path.GetFileNameWithoutExtension(new Uri(url).AbsolutePath);
            return new RemoteFile(url, $"{(stem.Length > 0 ? stem : "download")}-{date:yyyy-MM-dd}.zip", option);
        }
    }

    // A file of the same name has the same date on the server, so it is read again rather than
    // downloaded again. A download that is not a zip file is deleted.
    private static async Task Fetch(HttpClient http, RemoteFile remote, string file, CancellationToken cancellationToken) {
        if (System.IO.File.Exists(file)) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Already downloaded:[/] {file}");
            return;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]Downloading[/] {remote.Url}");
        try {
            await StatusListDownload.SaveAsync(http, remote.Url, file, cancellationToken).ConfigureAwait(false);
        } catch (HttpRequestException ex) {
            throw new IOException(FranceBdc.MissingFileMessage(remote.Url, ex.Message, remote.Option), ex);
        }
        if (!IsZip(file)) {
            System.IO.File.Delete(file);
            throw new IOException(FranceBdc.MissingFileMessage(remote.Url, "the file downloaded is not a zip file", remote.Option));
        }
    }

    private static bool IsZip(string file) {
        using var stream = System.IO.File.OpenRead(file);
        Span<byte> start = stackalloc byte[4];
        return stream.Read(start) == 4 && start[0] == (byte)'P' && start[1] == (byte)'K' && start[2] == 3 && start[3] == 4;
    }
}
