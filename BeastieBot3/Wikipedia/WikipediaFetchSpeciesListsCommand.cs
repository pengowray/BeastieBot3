using System.ComponentModel;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using Spectre.Console;
using Spectre.Console.Cli;

// `wikipedia fetch-species-lists`: finds the English Wikipedia pages that list species and downloads
// their wikitext into the Wikipedia cache, for `wikipedia report-species-lists`.
//
// Pages are found in two ways (SpeciesListSources):
//   1. every article that uses {{IUCN status}} or {{Species table/row}}: "List of" pages and also
//      genus and family articles with a list of their species;
//   2. the "List of" pages in the categories Lists of animals, plants, fungi and organisms and their
//      subcategories, to --category-depth levels below them. Only titles that start "List of" are
//      kept, because the subcategories also hold pages that are not lists.
// Each template and category is a row in species_list_sources with the time it was read, so a
// stopped run carries on with the ones not read yet, and a source is read again after --find-days.
// Pages are then downloaded through the action API, a few per request (list pages are large); only
// pages not downloaded yet, or downloaded before --refresh-days, are asked for.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia fetch-species-lists", CommandKind.Mutates,
    "Find English Wikipedia pages that list species (articles that use {{IUCN status}} or {{Species table/row}}, and the \"List of\" pages in the categories of lists of animals, plants, fungi and organisms) and download their wikitext into the Wikipedia cache, for wikipedia report-species-lists.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Reads again only the templates and categories read more than --find-days ago, and downloads only the pages not downloaded yet. --refresh-days also downloads again the pages downloaded more than that many days ago.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "wikipedia fetch-species-lists --status",
        "wikipedia fetch-species-lists --limit 50",
        "wikipedia fetch-species-lists",
        "wikipedia fetch-species-lists --refresh-days 30",
    })]
internal sealed class WikipediaFetchSpeciesListsCommand : AsyncCommand<WikipediaFetchSpeciesListsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Wikipedia cache database. Default: Datastore:enwiki_cache_sqlite in paths.ini.")]
        public string? CachePath { get; init; }

        [CommandOption("--category-depth <N>")]
        [Description("How many levels of subcategories to read below each starting category. Default: 2.")]
        public int CategoryDepth { get; init; } = 2;

        [CommandOption("--find-days <DAYS>")]
        [Description("Read a template's pages or a category's members again when they were last read more than this many days ago. Default: 30.")]
        public int FindDays { get; init; } = 30;

        [CommandOption("--skip-find")]
        [Description("Download the pages already found, without reading the templates and categories.")]
        public bool SkipFind { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Download at most N pages in this run. 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Also download again the pages downloaded more than this many days ago.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how many templates, categories and pages are left to read and download, and download nothing.")]
        public bool Status { get; init; }
    }

    /// The templates whose articles are read, all of them, whatever their title.
    public static readonly string[] Templates = ["Template:IUCN status", "Template:Species table/row"];

    /// The categories read for "List of" pages, with their subcategories.
    public static readonly string[] Categories = [
        "Category:Lists of animals",
        "Category:Lists of plants",
        "Category:Lists of fungi",
        "Category:Lists of organisms",
    ];

    // List pages are large (up to 2 MB of wikitext), so a request asks for a few at a time.
    private const int TitlesPerRequest = 10;

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        string cachePath;
        try {
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{ex.Message}[/]");
            return -1;
        }
        var cache = settings.Status ? WikipediaCacheStore.OpenReadOnly(cachePath) : WikipediaCacheStore.Open(cachePath);
        if (cache is null) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikipedia cache not found:[/] {cachePath}");
            return -1;
        }
        using (cache) {
            var now = DateTime.UtcNow;
            var foundBefore = now.AddDays(-Math.Max(0, settings.FindDays));
            DateTime? refreshBefore = settings.RefreshDays is > 0 ? now.AddDays(-settings.RefreshDays.Value) : null;
            var depth = Math.Max(0, settings.CategoryDepth);
            if (!settings.Status) {
                foreach (var template in Templates) {
                    cache.AddSpeciesListSource(template, SpeciesListSourceKinds.Template, 0);
                }
                foreach (var category in Categories) {
                    cache.AddSpeciesListSource(category, SpeciesListSourceKinds.Category, 0);
                }
            }

            WritePlan(cache, foundBefore, refreshBefore, depth, settings.SkipFind);
            if (settings.Status) {
                return 0;
            }

            using var client = new WikipediaApiClient(WikipediaConfiguration.FromEnvironment());
            var failed = false;
            try {
                if (!settings.SkipFind) {
                    await FindAsync(cache, client, foundBefore, depth, cancellationToken);
                }
                var toDownload = PagesToDownload(cache, refreshBefore);
                if (settings.Limit > 0 && toDownload.Count > settings.Limit) {
                    toDownload = toDownload.Take(settings.Limit).ToList();
                }
                var downloader = new BatchPageDownloader(cache, client, new WikipediaPageFetcher(cache, client));
                await downloader.FetchPagesAsync("Pages", toDownload, TitlesPerRequest, cancellationToken);
                AnsiConsole.MarkupLineInterpolated(
                    $"Requests: {downloader.Requests:N0}. Pages saved: [green]{downloader.PagesSaved:N0}[/], with no page: [yellow]{downloader.PagesMissing:N0}[/], failed: [red]{downloader.PagesFailed:N0}[/].");
                failed = downloader.PagesFailed > 0;
            } catch (WikipediaApiException ex) {
                AnsiConsole.MarkupLineInterpolated($"[red]Stopped: Wikipedia did not answer {BatchPageDownloader.MaxFailedInARow} requests in a row.[/] {ex.Message}");
                AnsiConsole.MarkupLine("[grey]Run the command again to carry on where it stopped.[/]");
                failed = true;
            }
            WritePlan(cache, foundBefore, refreshBefore, depth, skipFind: false);
            return failed ? 1 : 0;
        }
    }

    // Reads each template and category not read since foundBefore, shallowest first; the
    // subcategories a category adds are read in the same run, down to maxDepth.
    private static async Task FindAsync(WikipediaCacheStore cache, WikipediaApiClient client, DateTime foundBefore, int maxDepth,
        CancellationToken cancellationToken) {
        for (var round = 0; round <= maxDepth + 1; round++) {
            var due = DueSources(cache, foundBefore, maxDepth);
            if (due.Count == 0) {
                return;
            }
            await ProgressConsole.RunAsync($"Templates and categories (level {due.Min(s => s.Depth)})", due.Count, async progress => {
                foreach (var source in due) {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (source.Kind == SpeciesListSourceKinds.Template) {
                        var pages = await client.ListEmbeddedInAsync(source.Name, WikipediaApiClient.ArticleNamespace, cancellationToken);
                        cache.SaveSpeciesListFind(source, pages.Select(p => p.Title).ToList(), [], DateTime.UtcNow);
                    } else {
                        var members = await client.ListCategoryMembersAsync(source.Name, cancellationToken);
                        var lists = members.Where(m => m.Namespace == WikipediaApiClient.ArticleNamespace && IsListTitle(m.Title))
                            .Select(m => m.Title).ToList();
                        var subcategories = source.Depth < maxDepth
                            ? members.Where(m => m.Namespace == WikipediaApiClient.CategoryNamespace).Select(m => m.Title).ToList()
                            : [];
                        cache.SaveSpeciesListFind(source, lists, subcategories, DateTime.UtcNow);
                    }
                    progress.Increment(1);
                }
            }, cancellationToken);
        }
    }

    internal static bool IsListTitle(string title) => title.StartsWith("List of ", StringComparison.Ordinal);

    internal static List<SpeciesListSource> DueSources(WikipediaCacheStore cache, DateTime foundBefore, int maxDepth) {
        var sources = cache.GetSpeciesListSources().Where(s => s.Depth <= maxDepth).ToList();
        var due = sources.Where(s => s.FoundAt is null || s.FoundAt < foundBefore).ToList();
        if (due.Count == 0) {
            return due;
        }
        // One level at a time, so a category's subcategories are known before they are counted.
        var level = due.Min(s => s.Depth);
        return due.Where(s => s.Depth == level).ToList();
    }

    internal static List<string> PagesToDownload(WikipediaCacheStore cache, DateTime? refreshBefore) {
        var titles = new List<string>();
        foreach (var page in cache.GetSpeciesListPages()) {
            var state = cache.GetDownloadState(page.Title);
            if (state is null || state.Value.Status is WikiPageDownloadStatus.Pending or WikiPageDownloadStatus.Failed) {
                titles.Add(page.Title);
            } else if (state.Value.Status == WikiPageDownloadStatus.Cached && refreshBefore is { } cutoff
                       && (state.Value.DownloadedAt is null || state.Value.DownloadedAt < cutoff)) {
                titles.Add(page.Title);
            }
        }
        return titles;
    }

    private static void WritePlan(WikipediaCacheStore cache, DateTime foundBefore, DateTime? refreshBefore, int maxDepth, bool skipFind) {
        var sources = cache.GetSpeciesListSources().Where(s => s.Depth <= maxDepth).ToList();
        var pages = cache.GetSpeciesListPages();
        var downloaded = 0;
        var missing = 0;
        foreach (var page in pages) {
            var state = cache.GetDownloadState(page.Title);
            if (state?.Status == WikiPageDownloadStatus.Cached) {
                downloaded++;
            } else if (state?.Status == WikiPageDownloadStatus.Missing) {
                missing++;
            }
        }
        var toDownload = PagesToDownload(cache, refreshBefore).Count;
        var table = new Table().AddColumns("", "Done", "To do");
        var templates = sources.Where(s => s.Kind == SpeciesListSourceKinds.Template).ToList();
        var categories = sources.Where(s => s.Kind == SpeciesListSourceKinds.Category).ToList();
        // A read-only --status run before the first run has no rows for the starting sources yet.
        var known = sources.Select(s => s.Name).ToHashSet(StringComparer.Ordinal);
        string Due(List<SpeciesListSource> list, string[] starting) => skipFind
            ? "skipped"
            : (list.Count(s => s.FoundAt is null || s.FoundAt < foundBefore) + starting.Count(s => !known.Contains(s))).ToString("N0");
        table.AddRow("Templates read", templates.Count(s => s.FoundAt >= foundBefore).ToString("N0"), Due(templates, Templates));
        table.AddRow($"Categories read (to {maxDepth} levels down)", categories.Count(s => s.FoundAt >= foundBefore).ToString("N0"), Due(categories, Categories));
        table.AddRow("Pages downloaded", downloaded.ToString("N0"), toDownload.ToString("N0"));
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated($"[grey]Pages found:[/] {pages.Count:N0}. [grey]Pages that no longer exist:[/] {missing:N0}.");
    }
}
