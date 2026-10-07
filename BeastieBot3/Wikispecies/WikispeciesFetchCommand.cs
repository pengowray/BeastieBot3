using System.ComponentModel;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `wikispecies fetch`: downloads the Wikispecies page of each IUCN taxon (by its scientific name,
// redirects followed) and the taxonavigation templates above it, into the Wikispecies cache (the
// Wikipedia cache's tables in a file of their own), so `site build-db` can show Wikispecies'
// classification beside the others. Round by round: the taxon pages, then the templates their
// Taxonavigation sections call, then the templates those call, until a round finds no new template.
// Downloaded pages are not asked for again (unless --refresh-days).

namespace BeastieBot3.Wikispecies;

[CommandInfo("wikispecies fetch", CommandKind.Mutates,
    "Download the Wikispecies page of each IUCN taxon and the taxonavigation templates above it into the Wikispecies cache, for the species site's comparison of ranks.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Asks only for the pages not downloaded yet. --refresh-days also downloads again those older than that.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] { "wikispecies fetch --status", "wikispecies fetch --limit 1000", "wikispecies fetch" })]
internal sealed class WikispeciesFetchCommand : AsyncCommand<WikispeciesFetchCommand.Settings> {
    public static readonly Uri ActionEndpoint = new("https://species.wikimedia.org/w/api.php");
    public static readonly Uri RestEndpoint = new("https://species.wikimedia.org/api/rest_v1/");

    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Wikispecies cache database. Default: Datastore:wikispecies_cache_sqlite in paths.ini, else wikispecies_cache.sqlite in the datastore folder.")]
        public string? CachePath { get; init; }

        [CommandOption("--database <FILE>")]
        [Description("IUCN Red List database whose taxa are looked up. Default: Datastore:IUCN_sqlite_from_cvs in paths.ini.")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Download at most N pages in this run. 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Also download again the pages downloaded more than this many days ago.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how many pages are known, downloaded and left, and download nothing.")]
        public bool Status { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        string cachePath, iucnPath;
        try {
            cachePath = paths.ResolveWikispeciesCachePath(settings.CachePath);
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase, "--database");
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{ex.Message}[/]");
            return -1;
        }
        var titles = ReadTaxonTitles(iucnPath);
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN taxa (species, subspecies and varieties):[/] {titles.Count:N0}");
        var cache = settings.Status ? WikipediaCacheStore.OpenReadOnly(cachePath) : WikipediaCacheStore.Open(cachePath);
        if (cache is null) {
            AnsiConsole.MarkupLineInterpolated($"No Wikispecies cache yet ({cachePath}). Pages to download: {titles.Count:N0}, then the templates they use.");
            return 0;
        }
        using (cache) {
            DateTime? refreshBefore = settings.RefreshDays is > 0 ? DateTime.UtcNow.AddDays(-settings.RefreshDays.Value) : null;
            var budget = settings.Limit > 0 ? settings.Limit : int.MaxValue;
            using var client = settings.Status ? null : new WikipediaApiClient(WikipediaConfiguration.FromEnvironment() with {
                ActionEndpoint = ActionEndpoint,
                RestEndpoint = RestEndpoint,
            });
            var downloader = client is null ? null : new BatchPageDownloader(cache, client, new WikipediaPageFetcher(cache, client));
            var known = new HashSet<string>(titles, StringComparer.Ordinal);
            var round = titles;
            var level = 0;
            var left = 0;
            var pages = 0;
            while (round.Count > 0) {
                cancellationToken.ThrowIfCancellationRequested();
                var due = round.Where(t => Due(cache.GetDownloadState(t), refreshBefore)).ToList();
                if (downloader is not null && due.Count > 0 && budget > 0) {
                    var take = due.Take(budget).ToList();
                    budget -= take.Count;
                    var description = level == 0 ? "Taxon pages" : $"Templates, level {level}";
                    await downloader.FetchPagesAsync(description, take, WikipediaApiClient.MaxTitlesPerRequest, cancellationToken);
                }
                // Counted after the download: pages not asked for, or whose request failed.
                left += round.Count(t => cache.GetDownloadState(t) is not { Status: WikiPageDownloadStatus.Cached or WikiPageDownloadStatus.Missing });
                // The templates this level's pages call are the next level.
                var next = new List<string>();
                foreach (var title in round) {
                    if (cache.ReadArticleText(title) is not { } page) {
                        continue;
                    }
                    var parsed = level == 0
                        ? WikispeciesTaxonavigation.ParsePage(page.Wikitext, page.Title)
                        : WikispeciesTaxonavigation.ParseTemplate(page.Wikitext);
                    if (level == 0 && parsed is not null) {
                        pages++;
                    }
                    if (parsed?.Parent is { } parent && known.Add(WikispeciesTaxonavigation.TemplateTitle(parent))) {
                        next.Add(WikispeciesTaxonavigation.TemplateTitle(parent));
                    }
                }
                if (level == 0) {
                    AnsiConsole.MarkupLineInterpolated($"[grey]Taxon pages with a Taxonavigation section:[/] {pages:N0}");
                }
                round = next;
                level++;
            }
            AnsiConsole.MarkupLineInterpolated($"Pages and templates known: {known.Count:N0}, over {level:N0} levels. Not downloaded yet: {left:N0}.");
            if (downloader is not null) {
                AnsiConsole.MarkupLineInterpolated(
                    $"Requests: {downloader.Requests:N0}. Saved: [green]{downloader.PagesSaved:N0}[/], with no page: [yellow]{downloader.PagesMissing:N0}[/], failed: [red]{downloader.PagesFailed:N0}[/].");
                if (left > 0) {
                    AnsiConsole.MarkupLine("[grey]Run the command again: the templates above the ones just downloaded are found from them.[/]");
                }
                return downloader.PagesFailed > 0 ? 1 : 0;
            }
            return 0;
        }
    }

    // The Wikispecies titles of the IUCN taxa: subpopulations have their species' name.
    private static List<string> ReadTaxonTitles(string iucnDatabase) {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = iucnDatabase, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT scientificName FROM taxonomy_html WHERE scientificName IS NOT NULL ORDER BY scientificName";
        using var reader = command.ExecuteReader();
        var titles = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (reader.Read()) {
            var title = WikispeciesTaxonavigation.TitleFor(reader.GetString(0));
            if (title.Length > 0 && seen.Add(title)) {
                titles.Add(title);
            }
        }
        return titles;
    }

    private static bool Due((string Status, DateTime? DownloadedAt)? state, DateTime? refreshBefore) =>
        state is null || state.Value.Status is WikiPageDownloadStatus.Pending or WikiPageDownloadStatus.Failed
        || (state.Value.Status == WikiPageDownloadStatus.Cached && refreshBefore is { } cutoff && (state.Value.DownloadedAt is null || state.Value.DownloadedAt < cutoff));
}
