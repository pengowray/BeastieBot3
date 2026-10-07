using System.ComponentModel;
using System.Text.Json;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Taxonomy;
using Spectre.Console;
using Spectre.Console.Cli;

// `wikipedia fetch-taxonomy-templates`: downloads the taxonomy templates (Template:Taxonomy/Felis)
// that the downloaded articles' automatic taxoboxes and speciesboxes start from, and every template
// above them, so `site build-db` can show English Wikipedia's classification beside the others.
// Round by round: the templates the taxoboxes name, then the parents those templates name, until a
// round finds no new template. Downloaded templates are not asked for again (unless --refresh-days).

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia fetch-taxonomy-templates", CommandKind.Mutates,
    "Download the English Wikipedia taxonomy templates (Template:Taxonomy/...) that the downloaded articles' taxoboxes use, and every template above them, into the Wikipedia cache, for the species site's comparison of ranks.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Asks only for the templates not downloaded yet. --refresh-days also downloads again those older than that.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] { "wikipedia fetch-taxonomy-templates --status", "wikipedia fetch-taxonomy-templates --limit 500", "wikipedia fetch-taxonomy-templates" })]
internal sealed class WikipediaFetchTaxonomyTemplatesCommand : AsyncCommand<WikipediaFetchTaxonomyTemplatesCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Wikipedia cache database. Default: Datastore:enwiki_cache_sqlite in paths.ini.")]
        public string? CachePath { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Download at most N templates in this run. 0 or unset: no limit.")]
        public int Limit { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Also download again the templates downloaded more than this many days ago.")]
        public int? RefreshDays { get; init; }

        [CommandOption("--status")]
        [Description("Print how many templates are known, downloaded and left, and download nothing.")]
        public bool Status { get; init; }
    }

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
            DateTime? refreshBefore = settings.RefreshDays is > 0 ? DateTime.UtcNow.AddDays(-settings.RefreshDays.Value) : null;
            var known = new HashSet<string>(StringComparer.Ordinal);
            var round = cache.TaxoboxStarts().Select(TaxonomyTemplates.Title).Where(known.Add).ToList();
            AnsiConsole.MarkupLineInterpolated($"[grey]Templates the taxoboxes start from:[/] {round.Count:N0}");
            var budget = settings.Limit > 0 ? settings.Limit : int.MaxValue;
            using var client = settings.Status ? null : new WikipediaApiClient(WikipediaConfiguration.FromEnvironment());
            var downloader = client is null ? null : new BatchPageDownloader(cache, client, new WikipediaPageFetcher(cache, client));
            var level = 0;
            var left = 0;
            while (round.Count > 0) {
                cancellationToken.ThrowIfCancellationRequested();
                var due = round.Where(t => Due(cache.GetDownloadState(t), refreshBefore)).ToList();
                if (downloader is not null && due.Count > 0 && budget > 0) {
                    var take = due.Take(budget).ToList();
                    budget -= take.Count;
                    await downloader.FetchPagesAsync($"Templates, level {level + 1}", take, WikipediaApiClient.MaxTitlesPerRequest, cancellationToken);
                }
                // Counted after the download: templates not asked for, or whose request failed.
                left += round.Count(t => cache.GetDownloadState(t) is not { Status: WikiPageDownloadStatus.Cached or WikiPageDownloadStatus.Missing });
                // The parents of this level's templates are the next level.
                var next = new List<string>();
                foreach (var title in round) {
                    if (cache.ReadArticleText(title) is not { } page
                        || TaxonomyTemplates.Parse(title[TaxonomyTemplates.Prefix.Length..], page.Wikitext) is not { } template) {
                        continue;
                    }
                    // The parent, and the template a "same as" template takes its rank and name from.
                    foreach (var name in new[] { template.Parent, template.SameAs }) {
                        if (name is not null && known.Add(TaxonomyTemplates.Title(name))) {
                            next.Add(TaxonomyTemplates.Title(name));
                        }
                    }
                }
                round = next;
                level++;
            }
            AnsiConsole.MarkupLineInterpolated($"Templates known: {known.Count:N0}, over {level:N0} levels. Not downloaded yet: {left:N0}.");
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

    private static bool Due((string Status, DateTime? DownloadedAt)? state, DateTime? refreshBefore) =>
        state is null || state.Value.Status is WikiPageDownloadStatus.Pending or WikiPageDownloadStatus.Failed
        || (state.Value.Status == WikiPageDownloadStatus.Cached && refreshBefore is { } cutoff && (state.Value.DownloadedAt is null || state.Value.DownloadedAt < cutoff));
}
