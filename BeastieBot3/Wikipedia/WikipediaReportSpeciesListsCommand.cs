using System.ComponentModel;
using System.Diagnostics;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Site;
using BeastieBot3.Site.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Spectre.Console;
using Spectre.Console.Cli;

// `wikipedia report-species-lists`: runs the public site's /update code over every species list in
// the Wikipedia cache and writes a report of the pages that can be updated (SpeciesListReport).
//
// Pages: those `wikipedia fetch-species-lists` found and downloaded, and (--pages groups or all) the
// articles of the site's groups that `wikipedia fetch-group-titles` downloaded, when their wikitext
// has a list or a table (LooksLikeList). Each article is checked once, however many titles lead to it.
// The site database (site build-db) gives the latest global assessments, as on the site.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia report-species-lists", CommandKind.ReadOnly,
    "Compare the species lists on English Wikipedia (downloaded by wikipedia fetch-species-lists, and the genus and family articles downloaded by wikipedia fetch-group-titles) with the latest IUCN Red List assessments in the public site's database, and write a report of the statuses that are out of date or missing and the taxa missing from each list. Outputs Markdown and CSV.",
    Examples = new[] {
        "wikipedia report-species-lists",
        "wikipedia report-species-lists --pages lists",
        "wikipedia report-species-lists --title \"List of mammals of Australia\"",
        "wikipedia report-species-lists --limit 50",
    })]
internal sealed class WikipediaReportSpeciesListsCommand : Command<WikipediaReportSpeciesListsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Wikipedia cache database. Default: Datastore:enwiki_cache_sqlite in paths.ini.")]
        public string? CachePath { get; init; }

        [CommandOption("--site-db <FILE>")]
        [Description("The public site's database. Default: Datastore:site_sqlite in paths.ini.")]
        public string? SiteDatabase { get; init; }

        [CommandOption("--pages <WHICH>")]
        [Description("Which pages to check: lists (pages found by wikipedia fetch-species-lists), groups (genus, family and other group articles with a list) or all. Default: all.")]
        public string Pages { get; init; } = "all";

        [CommandOption("--title <TITLE>")]
        [Description("Check only this page (it must be downloaded).")]
        public string? Title { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Check at most N pages. 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("-o|--output <DIR>")]
        [Description("Folder for the report files. Default: Datastore:reports_dir in paths.ini.")]
        public string? OutputDirectory { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        string cachePath;
        try {
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        } catch (InvalidOperationException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{ex.Message}[/]");
            return -1;
        }
        var sitePath = settings.SiteDatabase ?? paths.GetSiteDatabasePath();
        if (sitePath is null) {
            AnsiConsole.MarkupLine("[red]No site database is set:[/] add site_sqlite to [[Datastore]] in paths.ini, or give --site-db.");
            return -1;
        }
        var which = settings.Pages.Trim().ToLowerInvariant();
        if (which is not ("all" or "lists" or "groups")) {
            AnsiConsole.MarkupLineInterpolated($"[red]--pages must be lists, groups or all, not[/] {settings.Pages}");
            return -1;
        }

        var siteDb = new SiteDatabase(Options.Create(new SiteOptions { DatabasePath = sitePath }), NullLogger<SiteDatabase>.Instance, TimeProvider.System);
        if (siteDb.Snapshot is not { } snapshot) {
            AnsiConsole.MarkupLineInterpolated($"[red]Cannot use the site database[/] {sitePath}: {siteDb.PublicNotReadyReason}. Build it with site build-db.");
            return -1;
        }
        using var cache = WikipediaCacheStore.OpenReadOnly(cachePath);
        if (cache is null) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikipedia cache not found:[/] {cachePath}");
            return -1;
        }

        var plan = PlanPages(cache, which, settings.Title);
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Lists found:[/] {plan.ListsFound:N0} ([grey]not downloaded:[/] {plan.ListsNotDownloaded:N0}). [grey]Group articles with a list:[/] {plan.GroupArticles:N0} of {plan.GroupArticlesRead:N0} read.");
        var pages = plan.Pages;
        if (settings.Limit > 0 && pages.Count > settings.Limit) {
            pages = pages.Take(settings.Limit).ToList();
        }

        var queries = new SiteQueries(siteDb);
        using var statuses = queries.OpenStatusLookup();
        var groups = new SiteListScopeLookup(queries, new SpeciesTableQueries(siteDb), statuses);
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var areas = queries.AreaNames();
        var results = new List<SpeciesListReportRow>();
        var failures = new List<(string Title, string Error)>();
        var watch = Stopwatch.StartNew();
        ProgressConsole.Run("Pages", pages.Count, progress => {
            foreach (var page in pages) {
                cancellationToken.ThrowIfCancellationRequested();
                var text = cache.ReadPageText(page.PageRowId);
                if (text is not null) {
                    try {
                        results.Add(new SpeciesListReportRow(SpeciesListSurvey.Check(text, statuses, groups, today, areas), page.Sources));
                    } catch (Exception ex) when (ex is not OperationCanceledException) {
                        failures.Add((page.Title, ex.Message));
                    }
                }
                progress.Increment(1);
            }
        });
        AnsiConsole.MarkupLineInterpolated($"[grey]Checked {results.Count:N0} pages in {watch.Elapsed.TotalSeconds:N0} s.[/]");
        foreach (var (title, error) in failures) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not check[/] {title}: {error}");
        }

        var directory = ReportPathResolver.ResolveDirectory(paths, settings.OutputDirectory, Path.GetDirectoryName(cachePath));
        var stamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");
        var report = new SpeciesListReport(results, plan, snapshot.IucnRelease, snapshot.Get(Shared.SiteData.SiteDbSchema.MetaKeys.BuiltAtUtc), failures);
        var written = report.Write(directory, $"species-lists-{stamp}");
        foreach (var path in written) {
            AnsiConsole.MarkupLineInterpolated($"[green]Written:[/] {path}");
        }
        report.PrintSummary();
        return failures.Count > 0 ? 1 : 0;
    }

    /// The pages to check, each article once, in title order.
    internal static SpeciesListPlan PlanPages(WikipediaCacheStore cache, string which, string? onlyTitle) {
        var byRow = new Dictionary<long, SpeciesListPlanPage>();
        int listsFound = 0, listsNotDownloaded = 0, groupArticles = 0, groupArticlesRead = 0;
        if (onlyTitle is not null) {
            var article = cache.ResolveDownloadedArticle(onlyTitle, readTaxobox: false);
            if (article is not null) {
                var normalized = WikipediaTitleHelper.Normalize(onlyTitle);
                var sources = cache.GetSpeciesListPages().FirstOrDefault(p => WikipediaTitleHelper.Normalize(p.Title) == normalized)?.Sources ?? [];
                byRow[article.PageRowId] = new SpeciesListPlanPage(article.PageRowId, article.Title, sources);
            } else {
                listsNotDownloaded = 1;
            }
            return new SpeciesListPlan(byRow.Values.ToList(), 1, listsNotDownloaded, 0, 0);
        }
        if (which is "all" or "lists") {
            foreach (var page in cache.GetSpeciesListPages()) {
                listsFound++;
                var article = cache.ResolveDownloadedArticle(page.Title, readTaxobox: false);
                if (article is null) {
                    listsNotDownloaded++;
                    continue;
                }
                if (byRow.TryGetValue(article.PageRowId, out var known)) {
                    byRow[article.PageRowId] = known with { Sources = known.Sources.Concat(page.Sources).Distinct().ToList() };
                } else {
                    byRow[article.PageRowId] = new SpeciesListPlanPage(article.PageRowId, article.Title, page.Sources);
                }
            }
        }
        if (which is "all" or "groups") {
            foreach (var title in cache.GetGroupArticleTitles()) {
                var article = cache.ResolveDownloadedArticle(title, readTaxobox: false);
                if (article is null || byRow.ContainsKey(article.PageRowId)) {
                    continue;
                }
                groupArticlesRead++;
                var text = cache.ReadPageText(article.PageRowId);
                if (text is null || !LooksLikeList(text.Wikitext)) {
                    continue;
                }
                groupArticles++;
                byRow[article.PageRowId] = new SpeciesListPlanPage(article.PageRowId, article.Title, [SpeciesListPlanPage.GroupArticle]);
            }
        }
        var pages = byRow.Values.OrderBy(p => p.Title, StringComparer.Ordinal).ToList();
        return new SpeciesListPlan(pages, listsFound, listsNotDownloaded, groupArticles, groupArticlesRead);
    }

    /// Whether a group article's wikitext has a status template, a wikitable, or at least three
    /// list lines with an italic name: the pages worth running the status updater on.
    internal static bool LooksLikeList(string wikitext) {
        if (wikitext.Contains("{{IUCN status", StringComparison.OrdinalIgnoreCase)
            || wikitext.Contains("{{Species table", StringComparison.OrdinalIgnoreCase)
            || wikitext.Contains("\n{|", StringComparison.Ordinal)) {
            return true;
        }
        var lines = 0;
        foreach (var line in wikitext.AsSpan().EnumerateLines()) {
            if (line.Length > 2 && (line[0] == '*' || line[0] == '#') && line.Contains("''", StringComparison.Ordinal) && ++lines >= 3) {
                return true;
            }
        }
        return false;
    }
}

internal sealed record SpeciesListPlanPage(long PageRowId, string Title, IReadOnlyList<string> Sources) {
    /// The source of a group article (wikipedia fetch-group-titles).
    public const string GroupArticle = "group article";
}

internal sealed record SpeciesListPlan(IReadOnlyList<SpeciesListPlanPage> Pages, int ListsFound, int ListsNotDownloaded,
    int GroupArticles, int GroupArticlesRead);

internal sealed record SpeciesListReportRow(SpeciesListPageResult Result, IReadOnlyList<string> Sources);
