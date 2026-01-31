using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikipediaEnqueueTaxaCommand : Command<WikipediaEnqueueTaxaCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--iucn-db <PATH>")]
        [Description("Path to the IUCN taxonomy SQLite database. Defaults to Datastore:IUCN_sqlite_from_cvs.")]
        public string? IucnDatabase { get; init; }

        [CommandOption("--ranks <LIST>")]
        [Description("Comma-separated ranks to enqueue (default: class,order,family). Supported: kingdom,phylum,class,order,family,genus.")]
        public string? Ranks { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Maximum number of titles to enqueue (0 = all).")]
        public int Limit { get; init; }

        [CommandOption("--force-refresh")]
        [Description("Re-enqueue existing titles even if they were seen recently.")]
        public bool ForceRefresh { get; init; }

        [CommandOption("--refresh-days <DAYS>")]
        [Description("Refresh titles last seen before the specified number of days.")]
        public int? RefreshDays { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, System.Threading.CancellationToken cancellationToken) {
        _ = context;
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string iucnPath;
        string cachePath;
        try {
            iucnPath = paths.ResolveIucnDatabasePath(settings.IucnDatabase);
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }

        if (!File.Exists(iucnPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN SQLite database not found:[/] {Markup.Escape(iucnPath)}");
            return -2;
        }

        using var iucnConnection = new SqliteConnection($"Data Source={iucnPath};Mode=ReadOnly");
        iucnConnection.Open();

        if (!ObjectExists(iucnConnection, "view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]Missing view view_assessments_html_taxonomy_html in the IUCN database. Re-run the importer to rebuild the view.[/]");
            return -3;
        }

        using var wikiStore = WikipediaCacheStore.Open(cachePath);

        var ranks = ParseRanks(settings.Ranks);
        if (ranks.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No ranks specified; nothing to enqueue.[/]");
            return 0;
        }

        var limit = settings.Limit <= 0 ? int.MaxValue : Math.Clamp(settings.Limit, 1, int.MaxValue);
        var titles = CollectTitles(iucnConnection, ranks, limit, cancellationToken);
        if (titles.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No taxon titles found for the requested ranks.[/]");
            return 0;
        }

        var now = DateTime.UtcNow;
        DateTime? refreshThreshold = null;
        if (settings.RefreshDays.HasValue && settings.RefreshDays.Value > 0) {
            refreshThreshold = now.AddDays(-settings.RefreshDays.Value);
        }

        var inserted = 0;
        var refreshed = 0;
        var skipped = 0;

        foreach (var title in titles) {
            cancellationToken.ThrowIfCancellationRequested();

            var normalized = WikipediaTitleHelper.Normalize(title);
            if (string.IsNullOrWhiteSpace(normalized)) {
                continue;
            }

            var existing = wikiStore.GetPageByNormalizedTitle(normalized);
            var candidate = new WikiPageCandidate(title.Trim(), normalized, PageId: null, now, now);
            if (existing is null) {
                wikiStore.UpsertPageCandidate(candidate);
                inserted++;
                continue;
            }

            var needsRefresh = settings.ForceRefresh;
            if (!needsRefresh && refreshThreshold.HasValue) {
                needsRefresh = !existing.LastSeenAt.HasValue || existing.LastSeenAt.Value < refreshThreshold.Value;
            }

            if (!needsRefresh) {
                skipped++;
                continue;
            }

            wikiStore.DeletePage(existing.PageRowId);
            wikiStore.UpsertPageCandidate(candidate);
            refreshed++;
        }

        AnsiConsole.MarkupLine($"Processed [green]{titles.Count}[/] titles (inserted [green]{inserted}[/], refreshed [grey]{refreshed}[/], skipped [grey]{skipped}[/]).");
        AnsiConsole.MarkupLine("Next step: run [blue]wikipedia fetch-pages[/] to download redirects for the queued titles.");
        return 0;
    }

    private static List<string> CollectTitles(SqliteConnection connection, IReadOnlyList<string> ranks, int limit, System.Threading.CancellationToken cancellationToken) {
        var results = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rank in ranks) {
            if (results.Count >= limit) {
                break;
            }

            var column = GetRankColumn(rank);
            if (column == null) {
                continue;
            }

            using var command = connection.CreateCommand();
            command.CommandText = $@"
SELECT DISTINCT {column}
FROM view_assessments_html_taxonomy_html
WHERE {column} IS NOT NULL AND TRIM({column}) != ''
ORDER BY {column};";

            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (reader.IsDBNull(0)) {
                    continue;
                }

                var value = reader.GetString(0).Trim();
                if (string.IsNullOrWhiteSpace(value)) {
                    continue;
                }

                var normalizedTitle = NormalizeTaxonTitle(value);
                if (string.IsNullOrWhiteSpace(normalizedTitle)) {
                    continue;
                }

                if (IsPlaceholderTitle(normalizedTitle)) {
                    continue;
                }

                if (seen.Add(normalizedTitle)) {
                    results.Add(normalizedTitle);
                    if (results.Count >= limit) {
                        break;
                    }
                }
            }
        }

        return results;
    }

    private static IReadOnlyList<string> ParseRanks(string? ranksRaw) {
        if (string.IsNullOrWhiteSpace(ranksRaw)) {
            return new List<string> { "class", "order", "family" };
        }

        var results = new List<string>();
        foreach (var raw in ranksRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var value = raw.Trim().ToLowerInvariant();
            if (!string.IsNullOrWhiteSpace(value)) {
                results.Add(value);
            }
        }

        return results;
    }

    private static string? GetRankColumn(string rank) => rank switch {
        "kingdom" => "kingdomName",
        "phylum" => "phylumName",
        "class" => "className",
        "order" => "orderName",
        "family" => "familyName",
        "genus" => "genusName",
        _ => null
    };

    private static string NormalizeTaxonTitle(string value) {
        var trimmed = value.Trim();
        if (string.IsNullOrWhiteSpace(trimmed)) {
            return trimmed;
        }

        if (IsAllCaps(trimmed)) {
            return ToTitleCase(trimmed);
        }

        if (trimmed.Contains(' ')) {
            var parts = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            for (var i = 0; i < parts.Length; i++) {
                if (IsAllCaps(parts[i])) {
                    parts[i] = ToTitleCase(parts[i]);
                }
            }
            return string.Join(' ', parts);
        }

        return trimmed;
    }

    private static bool IsAllCaps(string word) {
        if (string.IsNullOrEmpty(word)) return false;
        var hasLetter = false;
        foreach (var c in word) {
            if (!char.IsLetter(c)) continue;
            hasLetter = true;
            if (!char.IsUpper(c)) return false;
        }
        return hasLetter;
    }

    private static string ToTitleCase(string word) {
        if (string.IsNullOrEmpty(word)) return word;
        if (word.Length == 1) return word.ToUpperInvariant();
        return char.ToUpperInvariant(word[0]) + word[1..].ToLowerInvariant();
    }

    private static bool IsPlaceholderTitle(string value) {
        return value.Equals("Not assigned", StringComparison.OrdinalIgnoreCase)
            || value.Equals("Unassigned", StringComparison.OrdinalIgnoreCase)
            || value.Contains("Not assigned", StringComparison.OrdinalIgnoreCase)
            || value.Contains("Unassigned", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ObjectExists(SqliteConnection connection, string name, string type) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = @type AND name = @name LIMIT 1";
        command.Parameters.AddWithValue("@type", type);
        command.Parameters.AddWithValue("@name", name);
        return command.ExecuteScalar() is not null;
    }
}
