using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using BeastieBot3.Col;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;
using BeastieBot3.SiteBuild;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;
using Spectre.Console;
using Spectre.Console.Cli;

// `wikipedia fetch-group-titles`: downloads the English Wikipedia pages named after the groups of
// the public site's tree (IUCN's kingdom to genus plus the CoL placement's groups between them, as
// `site build-db` builds it), and the redirects that point at each article they lead to, so the
// site can find a group by its English name ("Fruit bat" redirects to "Megabat", whose taxobox
// taxon is Pteropodidae).
//
// For each band (kingdom to family, then subfamilies and tribes, then genera):
//   1. pages: the group names not downloaded yet, 50 titles per action API request (no REST HTML),
//      saved through WikipediaPageFetcher like any other page (redirects, categories, taxobox);
//   2. kingdom-qualified titles: for a name that is a disambiguation page, "Morus (plant)" and the
//      like (WikiPageKingdom.QualifiedTitlesFor over the title list), downloaded the same way;
//   3. redirects: for each article reached, the redirects in the article namespace that point at
//      it (prop=redirects), into wiki_incoming_redirects.
// Each step asks only for what is not stored yet, so a stopped run carries on where it left off.
// A name that the all-titles list (`wikipedia titles-dump`) does not have is not asked for.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia fetch-group-titles", CommandKind.Mutates,
    "Download the English Wikipedia pages named after the public site's groups (IUCN's kingdom to genus, with the Catalogue of Life groups between them) and the redirects that point at each article, into the Wikipedia cache. site build-db uses the article titles and redirects as English names that the site's search finds.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Asks only for the names not downloaded yet and the articles whose redirects are not downloaded yet. --refresh-days also downloads again what is older than that.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "wikipedia fetch-group-titles --status",
        "wikipedia fetch-group-titles --limit 20",
        "wikipedia fetch-group-titles",
        "wikipedia fetch-group-titles --refresh-days 180",
    })]
internal sealed class WikipediaFetchGroupTitlesCommand : AsyncCommand<WikipediaFetchGroupTitlesCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Wikipedia cache database. Default: Datastore:enwiki_cache_sqlite in paths.ini.")]
        public string? CachePath { get; init; }

        [CommandOption("--iucn-db <PATH>")]
        [Description("IUCN CSV export database the groups are read from. Default: Datastore:IUCN_sqlite_from_cvs in paths.ini.")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Ask for at most N titles in this run (pages and redirect lists together). 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Also download again the pages and redirect lists downloaded more than this many days ago.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how many titles each step has left, and download nothing.")]
        public bool Status { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        string cachePath, iucnDatabase;
        try {
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
            iucnDatabase = paths.ResolveIucnDatabasePath(settings.IucnDatabase, "--iucn-db");
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{ex.Message}[/]");
            return -1;
        }
        if (!File.Exists(iucnDatabase)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN CSV export not found:[/] {iucnDatabase}");
            return -1;
        }

        var titles = PlanTitles(paths, iucnDatabase, cancellationToken, out var groupCount);
        AnsiConsole.MarkupLineInterpolated($"[grey]Groups:[/] {groupCount:N0}, with {titles.Count:N0} different titles to look up.");

        WikipediaCacheStore? cache = settings.Status ? WikipediaCacheStore.OpenReadOnly(cachePath) : WikipediaCacheStore.Open(cachePath);
        if (cache is null) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikipedia cache not found:[/] {cachePath}");
            return -1;
        }
        using (cache) {
            var refreshBefore = settings.RefreshDays is > 0 ? DateTime.UtcNow.AddDays(-settings.RefreshDays.Value) : (DateTime?)null;
            var survey = new GroupTitleSurvey(cache, refreshBefore);
            if (!survey.HasTitleList) {
                AnsiConsole.MarkupLine("[yellow]No all-titles list is imported (wikipedia titles-dump), so every name is asked for, including names English Wikipedia has no page for.[/]");
            }
            var before = survey.Survey(titles, cancellationToken);
            WritePlan(before, survey.TitleListDate);
            if (settings.Status) {
                return 0;
            }

            var budget = settings.Limit > 0 ? settings.Limit : int.MaxValue;
            using var client = new WikipediaApiClient(WikipediaConfiguration.FromEnvironment());
            var run = new GroupTitleRun(cache, client, new WikipediaPageFetcher(cache, client));
            var failed = false;
            try {
                foreach (var band in Enumerable.Range(0, GroupTitlePlanner.BandNames.Count)) {
                    var bandTitles = titles.Where(t => t.Band == band).ToList();
                    var bandName = GroupTitlePlanner.BandNames[band];
                    if (budget <= 0) {
                        break;
                    }
                    var pages = survey.Survey(bandTitles, cancellationToken).Single();
                    budget -= await run.FetchPagesAsync($"Pages ({bandName})", Take(pages.PagesToDownload, ref budget), cancellationToken);
                    var qualified = survey.Survey(bandTitles, cancellationToken).Single();
                    budget -= await run.FetchPagesAsync($"Kingdom-qualified pages ({bandName})", Take(qualified.QualifiedToDownload, ref budget), cancellationToken);
                    var redirects = survey.Survey(bandTitles, cancellationToken).Single();
                    budget -= await run.FetchRedirectsAsync($"Redirects ({bandName})", Take(redirects.RedirectListsToDownload, ref budget), cancellationToken);
                }
            } catch (WikipediaApiException ex) {
                AnsiConsole.MarkupLineInterpolated($"[red]Stopped: {ex.Message}[/]");
                failed = true;
            }

            var after = survey.Survey(titles, cancellationToken);
            WriteStatus(cache, after, groupCount, iucnDatabase, survey.TitleListDate);
            AnsiConsole.MarkupLineInterpolated(
                $"Requests: {run.Requests:N0}. Pages saved: [green]{run.PagesSaved:N0}[/], with no page: [yellow]{run.PagesMissing:N0}[/], failed: [red]{run.PagesFailed:N0}[/]. Redirect lists saved: [green]{run.RedirectListsSaved:N0}[/] ({run.RedirectsSaved:N0} redirects).");
            WritePlan(after, survey.TitleListDate);
            return failed || run.PagesFailed > 0 ? 1 : 0;
        }
    }

    // `budget` items at most; the rest wait for the next run.
    private static IReadOnlyList<string> Take(IReadOnlyList<string> items, ref int budget) {
        var take = Math.Min(items.Count, budget);
        return take == items.Count ? items : items.Take(take).ToList();
    }

    private static IReadOnlyList<GroupTitle> PlanTitles(PathsService paths, string iucnDatabase, CancellationToken cancellationToken, out int groupCount) {
        var colDatabase = paths.GetColSqlitePath() is { } col && !string.IsNullOrWhiteSpace(col) ? Path.GetFullPath(col) : null;
        var placement = colDatabase is null ? null : TaxonPlacementStore.SidecarPath(colDatabase);
        var tree = SiteGroupTree.Load(iucnDatabase, colDatabase, placement, IucnNotAssignedRules.LoadForPaths(paths), out var warning, cancellationToken);
        if (warning is not null) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{warning}[/]");
        }
        groupCount = tree.Nodes.Count;

        var rulesList = Path.Combine(paths.BaseDirectory, "rules", "rules-list.txt");
        var taxonRulesPath = Path.Combine(paths.BaseDirectory, "rules", "taxon-rules.yml");
        var legacy = File.Exists(rulesList) ? new LegacyTaxaRuleList(rulesList) : LegacyTaxaRuleList.Empty();
        var taxonRules = File.Exists(taxonRulesPath) ? TaxonRulesService.Load(taxonRulesPath) : null;
        var headings = new HeadingFormatter(legacy, taxonRules, storeBackedProvider: null);
        return GroupTitlePlanner.Plan(tree.Nodes, node => headings.ResolveWikilink(node.Name, node.Kingdom));
    }

    private static void WritePlan(IReadOnlyList<GroupTitleSurveyBand> bands, string? titleListDate) {
        // Bands as columns, so the table fits a narrow console.
        var table = new Table().AddColumn("Titles of groups");
        foreach (var band in bands) {
            table.AddColumn(new TableColumn(Markup.Escape(GroupTitlePlanner.BandNames[band.Band])).RightAligned());
        }
        void Row(string label, Func<GroupTitleSurveyBand, long> value) =>
            table.AddRow(new[] { Markup.Escape(label) }.Concat(bands.Select(b => N(value(b)))).ToArray());
        Row("Names", b => b.Titles);
        Row("Downloaded", b => b.Downloaded);
        Row("No page on English Wikipedia", b => b.NoPage);
        Row("Not in the all-titles list, not asked for", b => b.NotInTitleList);
        Row("Pages to download", b => b.PagesToDownload.Count);
        Row("Kingdom-qualified pages to download", b => b.QualifiedToDownload.Count);
        Row("Articles reached", b => b.Articles);
        Row("Articles with their redirects downloaded", b => b.ArticlesWithRedirects);
        Row("Redirect lists to download", b => b.RedirectListsToDownload.Count);
        AnsiConsole.Write(table);
        if (titleListDate is not null && bands.Sum(b => b.NotInTitleList) > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Names not in the all-titles list of {titleListDate} are not asked for. A newer list (wikipedia titles-dump) can add them.[/]");
        }
    }

    private static string N(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static void WriteStatus(WikipediaCacheStore cache, IReadOnlyList<GroupTitleSurveyBand> bands, int groupCount, string iucnDatabase,
        string? titleListDate) {
        var values = new Dictionary<string, string> {
            [GroupTitleStatusKeys.FinishedAt] = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.Groups] = groupCount.ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.Titles] = bands.Sum(b => b.Titles).ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.PagesToDownload] = bands.Sum(b => b.PagesToDownload.Count + b.QualifiedToDownload.Count).ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.NotInTitleList] = bands.Sum(b => b.NotInTitleList).ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.Articles] = bands.Sum(b => b.Articles).ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.RedirectListsToDownload] = bands.Sum(b => b.RedirectListsToDownload.Count).ToString(CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.IucnDatabase] = iucnDatabase,
            [GroupTitleStatusKeys.IucnChangedAt] = (Web.Flows.PublicSiteStateReader.SqliteChangedAt(iucnDatabase) ?? DateTime.MinValue)
                .ToString("O", CultureInfo.InvariantCulture),
            [GroupTitleStatusKeys.TitleListDate] = titleListDate ?? string.Empty,
        };
        cache.SetGroupTitleStatus(values);
    }
}

/// Keys of wiki_group_title_status, written at the end of each `wikipedia fetch-group-titles` run.
internal static class GroupTitleStatusKeys {
    public const string FinishedAt = "finished_at";
    public const string Groups = "groups";
    public const string Titles = "titles";
    public const string PagesToDownload = "pages_to_download";
    public const string NotInTitleList = "not_in_title_list";
    public const string Articles = "articles";
    public const string RedirectListsToDownload = "redirect_lists_to_download";
    public const string IucnDatabase = "iucn_database";
    public const string IucnChangedAt = "iucn_changed_at";
    public const string TitleListDate = "title_list_date";
}

/// What one band of titles has left: lists of normalized titles to ask for, and counts.
internal sealed class GroupTitleSurveyBand {
    public int Band { get; init; }
    public long Titles { get; set; }
    public long Downloaded { get; set; }
    public long NoPage { get; set; }
    public long NotInTitleList { get; set; }
    public long Articles { get; set; }
    public long ArticlesWithRedirects { get; set; }
    public List<string> PagesToDownload { get; } = new();
    public List<string> QualifiedToDownload { get; } = new();
    public List<string> RedirectListsToDownload { get; } = new();
}

/// Works out, from the cache alone, what is left to ask for.
internal sealed class GroupTitleSurvey {
    private readonly WikipediaCacheStore _cache;
    private readonly DateTime? _refreshBefore;

    public GroupTitleSurvey(WikipediaCacheStore cache, DateTime? refreshBefore) {
        _cache = cache;
        _refreshBefore = refreshBefore;
        HasTitleList = cache.HasTitleListRows();
        TitleListDate = HasTitleList ? cache.GetDumpInfo()?.DumpDate : null;
    }

    public bool HasTitleList { get; }
    public string? TitleListDate { get; }

    public IReadOnlyList<GroupTitleSurveyBand> Survey(IReadOnlyList<GroupTitle> titles, CancellationToken cancellationToken) {
        var fetches = _cache.ReadIncomingRedirectFetches();
        var bands = new SortedDictionary<int, GroupTitleSurveyBand>();
        var seenArticles = new HashSet<string>(StringComparer.Ordinal);
        var seenQualified = new HashSet<string>(StringComparer.Ordinal);
        var seenPages = new HashSet<string>(StringComparer.Ordinal);
        foreach (var title in titles) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!bands.TryGetValue(title.Band, out var band)) {
                bands[title.Band] = band = new GroupTitleSurveyBand { Band = title.Band };
            }
            band.Titles++;
            if (!CanAsk(title.NormalizedTitle)) {
                band.NoPage++;
                continue;
            }
            var article = Visit(title.NormalizedTitle, band, fetches, seenArticles, seenPages, countTitle: true);
            if (article is { IsDisambiguation: true }) {
                var candidates = _cache.FindTitlesWithQualifier(article.NormalizedTitle);
                foreach (var kingdom in title.Kingdoms) {
                    foreach (var qualified in WikiPageKingdom.QualifiedTitlesFor(article.NormalizedTitle, kingdom, candidates)) {
                        if (!seenQualified.Add(qualified)) {
                            continue;
                        }
                        var page = _cache.GetPageByNormalizedTitle(qualified);
                        if (page is null || NeedsDownload(page)) {
                            band.QualifiedToDownload.Add(qualified);
                        } else {
                            Visit(qualified, band, fetches, seenArticles, seenPages, countTitle: false);
                        }
                    }
                }
            }
        }
        return bands.Values.ToList();
    }

    // Follows a title to its downloaded article and notes what it still needs. Returns the article,
    // or null when the title has no downloaded article yet.
    private WikiGroupArticle? Visit(string normalized, GroupTitleSurveyBand band, IReadOnlyDictionary<string, DateTime> fetches,
        HashSet<string> seenArticles, HashSet<string> seenPages, bool countTitle) {
        var page = _cache.GetPageByNormalizedTitle(normalized);
        if (page is null || NeedsDownload(page)) {
            if ((page is null || page.DownloadStatus == WikiPageDownloadStatus.Pending) && HasTitleList && !_cache.IsInTitleList(normalized)) {
                if (countTitle) {
                    band.NotInTitleList++;
                }
                return null;
            }
            if (seenPages.Add(normalized)) {
                band.PagesToDownload.Add(normalized);
            }
            return null;
        }
        if (page.DownloadStatus == WikiPageDownloadStatus.Missing) {
            if (countTitle) {
                band.NoPage++;
            }
            return null;
        }
        if (countTitle) {
            band.Downloaded++;
        }
        var article = _cache.ResolveDownloadedArticle(normalized, readTaxobox: false);
        if (article is null) {
            // A redirect whose target is not downloaded yet.
            var target = page.RedirectTarget is { } t ? WikipediaTitleHelper.Normalize(t) : null;
            if (target is not null && seenPages.Add(target)) {
                band.PagesToDownload.Add(target);
            }
            return null;
        }
        if (!article.IsDisambiguation && seenArticles.Add(article.NormalizedTitle)) {
            band.Articles++;
            if (fetches.TryGetValue(article.NormalizedTitle, out var at) && (_refreshBefore is null || at >= _refreshBefore)) {
                band.ArticlesWithRedirects++;
            } else {
                band.RedirectListsToDownload.Add(article.NormalizedTitle);
            }
        }
        return article;
    }

    private bool NeedsDownload(WikiPageSummary page) => page.DownloadStatus switch {
        WikiPageDownloadStatus.Pending or WikiPageDownloadStatus.Failed => true,
        WikiPageDownloadStatus.Cached => _refreshBefore is { } before && page.LastSeenAt is { } seen && seen < before,
        _ => false,
    };

    // Characters MediaWiki titles cannot have; a batch with one of them fails as a whole.
    private static bool CanAsk(string title) => title.IndexOfAny(['|', '#', '<', '>', '[', ']', '{', '}']) < 0;
}

/// Asks Wikipedia for pages and redirect lists in batches and saves what comes back.
internal sealed class GroupTitleRun {
    private readonly WikipediaCacheStore _cache;
    private readonly WikipediaApiClient _client;
    private readonly WikipediaPageFetcher _fetcher;
    private int _failedInARow;

    // After this many failed requests in a row, the run stops: Wikipedia is not answering.
    private const int MaxFailedInARow = 5;

    public GroupTitleRun(WikipediaCacheStore cache, WikipediaApiClient client, WikipediaPageFetcher fetcher) {
        _cache = cache;
        _client = client;
        _fetcher = fetcher;
    }

    public long Requests { get; private set; }
    public long PagesSaved { get; private set; }
    public long PagesMissing { get; private set; }
    public long PagesFailed { get; private set; }
    public long RedirectListsSaved { get; private set; }
    public long RedirectsSaved { get; private set; }

    /// Downloads the pages; returns how many titles it asked for.
    public async Task<int> FetchPagesAsync(string description, IReadOnlyList<string> titles, CancellationToken cancellationToken) {
        if (titles.Count == 0) {
            return 0;
        }
        await ProgressConsole.RunAsync(description, titles.Count, async progress => {
            foreach (var chunk in titles.Chunk(WikipediaApiClient.MaxTitlesPerRequest)) {
                cancellationToken.ThrowIfCancellationRequested();
                var now = DateTime.UtcNow;
                var items = chunk.Select(title => {
                    var row = _cache.UpsertPageCandidate(new WikiPageCandidate(title, title, PageId: null, now, now));
                    return new WikiPageWorkItem(row.PageRowId, title, title, WikiPageDownloadStatus.Pending, null, 0);
                }).ToList();
                var importId = _cache.BeginImport($"enwiki:batch of {chunk.Length} from {chunk[0]}");
                var watch = Stopwatch.StartNew();
                IReadOnlyDictionary<string, WikipediaQueryResult> results;
                try {
                    Requests++;
                    results = await _client.QueryPagesAsync(chunk, cancellationToken).ConfigureAwait(false);
                } catch (WikipediaApiException ex) {
                    foreach (var item in items) {
                        _cache.RecordPageFailure(item.PageRowId, ex.Message, DateTime.UtcNow);
                    }
                    _cache.CompleteImportFailure(importId, ex.Message, (int?)ex.StatusCode, watch.Elapsed);
                    PagesFailed += items.Count;
                    progress.Increment(chunk.Length);
                    if (++_failedInARow >= MaxFailedInARow) {
                        throw;
                    }
                    AnsiConsole.MarkupLineInterpolated($"[yellow]A request failed, carrying on: {ex.Message}[/]");
                    continue;
                }
                _failedInARow = 0;
                long bytes = 0;
                foreach (var item in items) {
                    if (!results.TryGetValue(item.PageTitle, out var result)) {
                        _cache.RecordPageFailure(item.PageRowId, "not in the batch answer", DateTime.UtcNow);
                        PagesFailed++;
                        continue;
                    }
                    bytes += result.PayloadBytes;
                    var outcome = _fetcher.SaveQueried(item, result, importId);
                    if (outcome.Success || outcome.Skipped) {
                        PagesSaved++;
                    } else if (outcome.Missing) {
                        PagesMissing++;
                    } else {
                        PagesFailed++;
                    }
                }
                _cache.CompleteImportSuccess(importId, 200, bytes, watch.Elapsed);
                progress.Increment(chunk.Length);
            }
        }, cancellationToken).ConfigureAwait(false);
        return titles.Count;
    }

    /// Downloads the redirect lists of the articles; returns how many titles it asked for.
    public async Task<int> FetchRedirectsAsync(string description, IReadOnlyList<string> articles, CancellationToken cancellationToken) {
        if (articles.Count == 0) {
            return 0;
        }
        await ProgressConsole.RunAsync(description, articles.Count, async progress => {
            foreach (var chunk in articles.Chunk(WikipediaApiClient.MaxTitlesPerRequest)) {
                cancellationToken.ThrowIfCancellationRequested();
                IReadOnlyDictionary<string, WikipediaIncomingRedirects> results;
                try {
                    Requests++;
                    results = await _client.QueryIncomingRedirectsAsync(chunk, cancellationToken).ConfigureAwait(false);
                } catch (WikipediaApiException ex) {
                    progress.Increment(chunk.Length);
                    if (++_failedInARow >= MaxFailedInARow) {
                        throw;
                    }
                    AnsiConsole.MarkupLineInterpolated($"[yellow]A request failed, carrying on: {ex.Message}[/]");
                    continue;
                }
                _failedInARow = 0;
                var now = DateTime.UtcNow;
                foreach (var title in chunk) {
                    if (!results.TryGetValue(title, out var result)) {
                        continue;
                    }
                    _cache.SaveIncomingRedirects(title, result.Exists ? result.Target : null, result.Redirects, now);
                    RedirectListsSaved++;
                    RedirectsSaved += result.Redirects.Count;
                }
                progress.Increment(chunk.Length);
            }
        }, cancellationToken).ConfigureAwait(false);
        return articles.Count;
    }
}
