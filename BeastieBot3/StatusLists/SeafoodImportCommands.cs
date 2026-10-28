using System.ComponentModel;
using System.Text;
using System.Text.Json;
using Spectre.Console;
using Spectre.Console.Cli;

// `seafood goodfish-import` and `seafood seafoodwatch-import`: download a seafood guide's ratings, keep
// the download in the status lists folder, and replace that guide's ratings in the status lists store.
// StatusListImport runs both. The names are read with the fixes in
// rules/status-lists/seafood-name-fixes.yml, so after editing that file, import the kept file again
// with --file.

namespace BeastieBot3.StatusLists;

[CommandInfo("seafood goodfish-import", CommandKind.Mutates,
    "Download the ratings of GoodFish, Australia's Sustainable Seafood Guide (goodfish.org.au, Australian Marine Conservation Society; about 210 pages, one request a second) and store them in the status lists store.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads every page again, keeps the download in the status lists folder with the date in the file name, and replaces the stored GoodFish ratings.",
    Examples = new[] {
        "seafood goodfish-import",
        "seafood goodfish-import --file ~/datastore/status-lists/goodfish-2026-10-29.json",
    })]
internal sealed class GoodFishImportCommand : AsyncCommand<GoodFishImportCommand.Settings> {
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
        var fixes = SeafoodNames.LoadFixes(SeafoodNames.FixesPathFor(settings.CreatePaths()), SeafoodGuideKeys.GoodFish);
        return StatusListImport.RunAsync(settings, Spec(fixes), cancellationToken);
    }

    internal static StatusListImportSpec<SeafoodRating> Spec(IReadOnlyDictionary<string, string> fixes) => new() {
        Source = StatusSources.GoodFish,
        Title = GoodFishGuide.Title,
        SiteUrl = GoodFishGuide.SiteUrl,
        Licence = GoodFishGuide.Licence,
        Citation = GoodFishGuide.Citation,
        FileStem = "goodfish",
        FileExtension = "json",
        DownloadNoun = "download",
        RowsNoun = "ratings",
        Download = GoodFishGuide.DownloadAsync,
        Read = file => GoodFishGuide.Read(File.ReadAllText(file, Encoding.UTF8), fixes),
        IsReadError = ex => ex is JsonException or KeyNotFoundException or InvalidOperationException,
        Replace = (store, rows, now, source) => store.ReplaceSeafoodRatings(SeafoodGuideKeys.GoodFish, rows, now, source),
        GroupColumn = "GoodFish rating",
        CountColumn = "Pages",
        GroupOf = r => string.Join(" or ", r.Ratings),
        Summary = rows => SeafoodImportSummary.Print("GoodFish", rows),
    };
}

[CommandInfo("seafood seafoodwatch-import", CommandKind.Mutates,
    "Download the recommendations of Seafood Watch (seafoodwatch.org, Monterey Bay Aquarium; about 1,950, in one request) and store them in the status lists store. Its terms of use allow personal use only.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Each run downloads the recommendations again, keeps the download in the status lists folder with the date in the file name, and replaces the stored Seafood Watch recommendations.",
    Examples = new[] {
        "seafood seafoodwatch-import",
        "seafood seafoodwatch-import --file ~/datastore/status-lists/seafoodwatch-2026-10-29.json",
    })]
internal sealed class SeafoodWatchImportCommand : AsyncCommand<SeafoodWatchImportCommand.Settings> {
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
        var fixes = SeafoodNames.LoadFixes(SeafoodNames.FixesPathFor(settings.CreatePaths()), SeafoodGuideKeys.SeafoodWatch);
        return StatusListImport.RunAsync(settings, Spec(fixes), cancellationToken);
    }

    internal static StatusListImportSpec<SeafoodRating> Spec(IReadOnlyDictionary<string, string> fixes) => new() {
        Source = StatusSources.SeafoodWatch,
        Title = SeafoodWatchGuide.Title,
        SiteUrl = SeafoodWatchGuide.SiteUrl,
        Licence = SeafoodWatchGuide.Licence,
        Citation = SeafoodWatchGuide.Citation,
        FileStem = "seafoodwatch",
        FileExtension = "json",
        DownloadNoun = "download",
        RowsNoun = "recommendations",
        Download = async (file, cancellationToken) => {
            using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(5), acceptJson: true);
            await StatusListDownload.SaveAsync(http, SeafoodWatchGuide.RecommendationsUrl(DateTime.UtcNow), file, cancellationToken)
                .ConfigureAwait(false);
        },
        Read = file => SeafoodWatchGuide.Read(File.ReadAllText(file, Encoding.UTF8), fixes),
        IsReadError = ex => ex is JsonException or KeyNotFoundException or InvalidOperationException,
        Replace = (store, rows, now, source) => store.ReplaceSeafoodRatings(SeafoodGuideKeys.SeafoodWatch, rows, now, source),
        SourceForFile = (file, source) => source with { Version = SeafoodWatchGuide.Version(File.ReadAllText(file, Encoding.UTF8)) ?? source.Version },
        GroupColumn = "Seafood Watch rating",
        CountColumn = "Recommendations",
        GroupOf = r => r.Ratings[0],
        Summary = rows => SeafoodImportSummary.Print("Seafood Watch", rows),
    };
}

internal static class SeafoodImportSummary {
    public static void Print(string guide, IReadOnlyList<SeafoodRating> rows) {
        var names = rows.SelectMany(r => r.Names).ToList();
        var scientific = names.Where(n => n.ScientificName is not null).Select(n => n.ScientificName!).Distinct().Count();
        AnsiConsole.MarkupLineInterpolated(
            $"[green]Stored {rows.Count:N0} {guide} ratings[/] naming {scientific:N0} species or subspecies.");
        var groups = names.Where(n => n.ScientificName is null).Select(n => n.Written).Distinct().Order(StringComparer.Ordinal).ToList();
        if (groups.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]{groups.Count:N0} names are of a genus or group, which the site does not match to a taxon: {string.Join(", ", groups)}[/]");
        }
    }
}
