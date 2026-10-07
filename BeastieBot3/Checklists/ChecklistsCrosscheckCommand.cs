using System.ComponentModel;
using System.Globalization;
using System.Text;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Web.Endpoints;
using CsvHelper;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `checklists crosscheck`: compares IUCN's countries with each imported checklist (ChecklistCrosscheck)
// and writes a Markdown summary and a CSV of the species whose countries differ, per source.

namespace BeastieBot3.Checklists;

[CommandInfo("checklists crosscheck", CommandKind.ReadOnly,
    "Compare the countries IUCN's latest global assessments record for each species (the public site's database) with the imported country checklists, and write a report of the species whose countries differ. Outputs Markdown and CSV.",
    Examples = new[] { "checklists crosscheck", "checklists crosscheck --source mdd" })]
internal sealed class ChecklistsCrosscheckCommand : Command<ChecklistsCrosscheckCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--source <KEY>")]
        [Description("mdd, wcvp, reptiledb, amphibiaweb, or all (the default): every imported source.")]
        public string Source { get; init; } = "all";

        [CommandOption("--site-db <FILE>")]
        [Description("The public site's database (schema 18 or later, with countries). Default: Datastore:site_sqlite in paths.ini.")]
        public string? SiteDatabase { get; init; }

        [CommandOption("--store <PATH>")]
        [Description("Checklists store. Default: Datastore:checklists_sqlite, else checklists.sqlite in the datastore folder.")]
        public string? StorePath { get; init; }

        [CommandOption("-o|--output <DIR>")]
        [Description("Folder for the report files. Default: Datastore:reports_dir in paths.ini.")]
        public string? OutputDirectory { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var storePath = settings.StorePath ?? paths.GetChecklistsPath();
        using var store = storePath is null ? null : ChecklistStore.OpenReadOnly(storePath);
        if (store is null) {
            AnsiConsole.MarkupLine("[red]No checklists imported yet.[/] Run checklists import.");
            return -1;
        }
        var sitePath = ExpandHome(settings.SiteDatabase ?? paths.GetSiteDatabasePath());
        if (sitePath is null || !File.Exists(sitePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Site database not found:[/] {sitePath ?? "(not set)"}. Build it with site build-db.");
            return -1;
        }
        using var site = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = sitePath, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ConnectionString);
        site.Open();
        using (var check = site.CreateCommand()) {
            check.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'taxon_area'";
            if (Convert.ToInt64(check.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
                AnsiConsole.MarkupLineInterpolated($"[red]The site database has no countries[/] (taxon_area): rebuild it with site build-db (schema 18 or later). {sitePath}");
                return -1;
            }
        }
        var tdwg = ChecklistCrosscheck.ReadTdwg(Path.Combine(RulesPaths.Resolve(paths).SourceRulesDir, "tdwg-level4-iso.csv"));
        var imported = store.Sources().Select(s => s.Source).ToHashSet(StringComparer.Ordinal);
        var keys = settings.Source.Equals("all", StringComparison.OrdinalIgnoreCase)
            ? ChecklistCrosscheck.Groups.Keys.Where(imported.Contains).ToList()
            : [settings.Source.ToLowerInvariant()];
        var results = new List<ChecklistCrosscheckResult>();
        foreach (var key in keys) {
            if (!ChecklistCrosscheck.Groups.ContainsKey(key) || !imported.Contains(key)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {key}:[/] not imported, or not a known source.");
                continue;
            }
            AnsiConsole.MarkupLineInterpolated($"[grey]Comparing with {ChecklistSources.Find(key)?.Title ?? key}...[/]");
            results.Add(ChecklistCrosscheck.Run(key, store, site, tdwg));
        }
        var directory = ReportPathResolver.ResolveDirectory(paths, settings.OutputDirectory, Path.GetDirectoryName(sitePath));
        var stamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
        var md = Path.Combine(directory, $"country-crosscheck-{stamp}.md");
        File.WriteAllText(md, Markdown(results, store.Sources(), stamp), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"[green]Written:[/] {md}");
        foreach (var result in results) {
            var csv = Path.Combine(directory, $"country-crosscheck-{stamp}-{result.Source}.csv");
            WriteCsv(csv, result);
            AnsiConsole.MarkupLineInterpolated($"[green]Written:[/] {csv}");
        }
        PrintSummary(results);
        return 0;
    }

    private static string? ExpandHome(string? path) =>
        path is not null && path.StartsWith('~') ? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), path.TrimStart('~', '/')) : path;

    private static string N(int n) => n.ToString("N0", CultureInfo.InvariantCulture);
    private static string Pct(int n, int of) => of == 0 ? "" : $"{100.0 * n / of:0}%";

    private static (int Agree, int IucnOnly, int SourceOnly, int EndemicBoth, int EndemicIucnOnly, int EndemicSourceOnly) Tally(ChecklistCrosscheckResult r) {
        int agree = 0, iucnOnly = 0, sourceOnly = 0, endemicBoth = 0, endemicIucn = 0, endemicSource = 0;
        foreach (var c in r.Comparisons) {
            if (c.IucnOnly.Count == 0 && c.SourceOnly.Count == 0) agree++;
            if (c.IucnOnly.Count > 0) iucnOnly++;
            if (c.SourceOnly.Count > 0) sourceOnly++;
            var sourceEndemic = c.SingleCountry;
            if (c.IucnEndemic is not null && sourceEndemic == c.IucnEndemic) endemicBoth++;
            else if (c.IucnEndemic is not null) endemicIucn++;
            else if (sourceEndemic is not null && c.IucnNative > 1) endemicSource++;
        }
        return (agree, iucnOnly, sourceOnly, endemicBoth, endemicIucn, endemicSource);
    }

    private static void PrintSummary(IReadOnlyList<ChecklistCrosscheckResult> results) {
        var table = new Table().AddColumns("Source", "IUCN species", "Matched", "Compared", "Same countries", "IUCN lists more", "Source lists more");
        foreach (var r in results) {
            var t = Tally(r);
            table.AddRow(Markup.Escape(ChecklistSources.Find(r.Source)?.Title ?? r.Source), N(r.IucnSpecies), N(r.Matched), N(r.Compared),
                $"{N(t.Agree)} ({Pct(t.Agree, r.Compared)})", N(t.IucnOnly), N(t.SourceOnly));
        }
        AnsiConsole.Write(table);
    }

    private static string Markdown(IReadOnlyList<ChecklistCrosscheckResult> results, IReadOnlyList<ChecklistSourceInfo> sources, string stamp) {
        var sb = new StringBuilder();
        sb.AppendLine("# IUCN countries checked against other checklists");
        sb.AppendLine();
        sb.AppendLine("Each IUCN species (latest global assessment) is compared with the same species in another checklist, matched by name or synonym. "
            + "\"IUCN lists more\": countries IUCN records as native that the checklist does not list at all. "
            + "\"Checklist lists more\": countries the checklist lists as native that IUCN does not record at all. "
            + "Records of uncertain presence, introduced and vagrant records never count as a difference.");
        sb.AppendLine();
        sb.AppendLine("| Checklist | Version | Licence | IUCN species in its group | Matched | Compared | Same countries | IUCN lists more | Checklist lists more |");
        sb.AppendLine("|---|---|---|---:|---:|---:|---:|---:|---:|");
        foreach (var r in results) {
            var t = Tally(r);
            var info = sources.FirstOrDefault(s => s.Source == r.Source);
            sb.AppendLine($"| {ChecklistSources.Find(r.Source)?.Title ?? r.Source} | {info?.Version} | {info?.Licence} | {N(r.IucnSpecies)} | {N(r.Matched)} ({N(r.MatchedBySynonym)} by synonym) | {N(r.Compared)} | {N(t.Agree)} ({Pct(t.Agree, r.Compared)}) | {N(t.IucnOnly)} | {N(t.SourceOnly)} |");
        }
        sb.AppendLine();
        sb.AppendLine("Matched species with no native country on one side are not compared, and nor are IUCN species matched through a synonym to a checklist species that another IUCN species also matches (the checklist lumps them): "
            + string.Join(", ", results.Select(r => $"{N(r.Lumped)} for {ChecklistSources.Find(r.Source)?.Title ?? r.Source}")) + ".");
        sb.AppendLine();
        foreach (var r in results) {
            var t = Tally(r);
            var title = ChecklistSources.Find(r.Source)?.Title ?? r.Source;
            sb.AppendLine($"## {title}");
            sb.AppendLine();
            sb.AppendLine($"- Endemic to one country in both: {N(t.EndemicBoth)}");
            sb.AppendLine($"- Endemic in IUCN, more than one native country in the checklist: {N(t.EndemicIucnOnly)}");
            sb.AppendLine($"- One native country in the checklist, more than one in IUCN: {N(t.EndemicSourceOnly)}");
            sb.AppendLine();
            sb.AppendLine("Countries that differ most often:");
            sb.AppendLine();
            sb.AppendLine("| Country or region | IUCN lists, checklist does not | Checklist lists, IUCN does not |");
            sb.AppendLine("|---|---:|---:|");
            var iucnOnly = r.Comparisons.SelectMany(c => c.IucnOnly).GroupBy(c => c).ToDictionary(g => g.Key, g => g.Count());
            var sourceOnly = r.Comparisons.SelectMany(c => c.SourceOnly).GroupBy(c => c).ToDictionary(g => g.Key, g => g.Count());
            foreach (var country in iucnOnly.Keys.Union(sourceOnly.Keys).OrderByDescending(c => iucnOnly.GetValueOrDefault(c) + sourceOnly.GetValueOrDefault(c)).Take(15)) {
                sb.AppendLine($"| {country} | {N(iucnOnly.GetValueOrDefault(country))} | {N(sourceOnly.GetValueOrDefault(country))} |");
            }
            sb.AppendLine();
            if (r.UnreadPlaces.Count > 0) {
                var top = r.UnreadPlaces.OrderByDescending(p => p.Value).Take(25).Select(p => $"{p.Key} ({N(p.Value)})");
                sb.AppendLine($"Place names in the checklist not read as a country ({N(r.UnreadPlaces.Count)} different names; the most frequent): {string.Join(", ", top)}.");
                sb.AppendLine();
            }
            sb.AppendLine($"Every species that differs: `country-crosscheck-{stamp}-{r.Source}.csv`.");
            sb.AppendLine();
        }
        return sb.ToString();
    }

    private static void WriteCsv(string path, ChecklistCrosscheckResult r) {
        using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (var h in new[] { "iucn_taxon_id", "scientific_name", "checklist_name", "class", "iucn_native_countries", "checklist_native_countries",
                     "iucn_only", "checklist_only", "iucn_endemic_to", "checklist_single_country", "matched_by" }) {
            csv.WriteField(h);
        }
        csv.NextRecord();
        foreach (var c in r.Comparisons.Where(c => c.IucnOnly.Count > 0 || c.SourceOnly.Count > 0)) {
            object?[] fields = [c.TaxonId, c.ScientificName, c.SourceName, c.ClassName, c.IucnNative, c.SourceNative,
                string.Join(" ", c.IucnOnly), string.Join(" ", c.SourceOnly), c.IucnEndemic, c.SingleCountry, c.BySynonym ? "synonym" : "name"];
            foreach (var f in fields) {
                csv.WriteField(f);
            }
            csv.NextRecord();
        }
    }
}
