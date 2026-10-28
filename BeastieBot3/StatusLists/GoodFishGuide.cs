using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Spectre.Console;

// GoodFish, Australia's Sustainable Seafood Guide (goodfish.org.au, Australian Marine Conservation
// Society). Each rating is a WordPress page of the post type "species" (about 210). The site's REST
// API lists the pages with their dates, but its taxonomies (rating, region, method, Latin name) are
// not in the API, so the download also reads each page and keeps the part that holds them: from the
// title down to the start of the "More information" text.
//
// The kept file is {"entries": [the API's rows], "pages": [{"id": <post id>, "html": "<part of the page>"}]}.
// GoodFish gives no date for an assessment; the API's modified_gmt is when the page last changed.

namespace BeastieBot3.StatusLists;

internal static partial class GoodFishGuide {
    public const string Title = "GoodFish: Australia's Sustainable Seafood Guide";
    public const string SiteUrl = "https://goodfish.org.au/sustainable-seafood-guide/";
    public const string Licence = "No licence published";

    public static string Citation(DateTime accessed) =>
        $"Australian Marine Conservation Society. GoodFish: Australia's Sustainable Seafood Guide. https://goodfish.org.au/ (accessed {accessed:d MMMM yyyy}).";

    public static string ApiPageUrl(int page) =>
        $"https://goodfish.org.au/wp-json/wp/v2/species?per_page=100&page={page}&_fields=id,slug,link,date_gmt,modified_gmt,title,class_list";

    /// The part of a page that Read reads: from the species-meta block to 2,000 characters past the
    /// "More information" heading (its first list names the fisheries). Null when the page has no
    /// species-meta block.
    public static string? KeptPart(string html) {
        var start = html.IndexOf("class=\"species-meta\"", StringComparison.Ordinal);
        if (start < 0) {
            return null;
        }
        var more = html.IndexOf("<strong>More information</strong>", start, StringComparison.Ordinal);
        var end = more < 0 ? Math.Min(html.Length, start + 20_000) : Math.Min(html.Length, more + 2_000);
        return html[start..end];
    }

    /// Reads one page's rating. Null when the page gives no rating.
    public static SeafoodRating? ReadPage(long postId, string url, string? published, string? modified, string part,
        IReadOnlyDictionary<string, string> fixes) {
        var ratings = Classifications().Match(part) is { Success: true } block
            ? RatingItem().Matches(block.Groups[1].Value).Select(m => SeafoodNames.PlainText(m.Groups[1].Value)).OfType<string>().Distinct().ToList()
            : [];
        if (ratings.Count == 0) {
            return null;
        }
        var title = SeafoodNames.PlainText(Heading().Match(part).Groups[1].Value) ?? $"GoodFish page {postId}";
        var latin = LatinNames().Match(part) is { Success: true } l ? SeafoodNames.PlainText(l.Groups[1].Value) : null;
        var method = Method().Match(part) is { Success: true } m ? SeafoodNames.PlainText(m.Groups[1].Value) : null;
        var region = Region().Match(part) is { Success: true } r ? SeafoodNames.PlainText(r.Groups[1].Value) : null;
        var note = Note().Match(part) is { Success: true } n ? SeafoodNames.PlainText(n.Groups[1].Value) : null;
        var fisheries = MoreInformationList().Match(part) is { Success: true } f
            ? ListItem().Matches(f.Groups[1].Value).Select(i => SeafoodNames.PlainText(i.Groups[1].Value)).OfType<string>().ToList()
            : [];
        return new SeafoodRating(
            SeafoodGuideKeys.GoodFish,
            postId.ToString(CultureInfo.InvariantCulture),
            title,
            GroupName: null,
            ratings,
            Color: null,
            Score: null,
            region,
            WaterBody: null,
            method is null ? [] : [method],
            Production(method),
            fisheries,
            note,
            url,
            published,
            modified,
            SeafoodNames.Read(SeafoodNames.Split(latin), fixes));
    }

    /// 'farmed' for "Farmed" and "Australian Farmed", 'wild' for "Wild Caught"; null otherwise.
    public static string? Production(string? method) => method?.ToLowerInvariant() switch {
        { } m when m.Contains("farmed", StringComparison.Ordinal) => "farmed",
        { } m when m.Contains("wild", StringComparison.Ordinal) => "wild",
        _ => null,
    };

    /// The kept file's pages, each read. Pages with no rating are left out.
    public static IReadOnlyList<SeafoodRating> Read(string json, IReadOnlyDictionary<string, string> fixes) {
        using var document = JsonDocument.Parse(json);
        var pages = new Dictionary<long, string>();
        foreach (var page in document.RootElement.GetProperty("pages").EnumerateArray()) {
            if (page.GetProperty("html").GetString() is { } part) {
                pages[page.GetProperty("id").GetInt64()] = part;
            }
        }
        var ratings = new List<SeafoodRating>();
        foreach (var entry in document.RootElement.GetProperty("entries").EnumerateArray()) {
            var id = entry.GetProperty("id").GetInt64();
            if (!pages.TryGetValue(id, out var html)) {
                continue;
            }
            if (ReadPage(id, entry.GetProperty("link").GetString()!, Utc(entry, "date_gmt"), Utc(entry, "modified_gmt"), html, fixes) is { } rating) {
                ratings.Add(rating);
            }
        }
        return SeafoodNames.ExpandAcrossRatings(ratings);
    }

    // WordPress's *_gmt dates have no zone: "2026-08-06T07:24:05", which is UTC.
    private static string? Utc(JsonElement entry, string property) =>
        entry.TryGetProperty(property, out var value) && value.GetString() is { Length: > 0 } text
        && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var at)
            ? at.ToString("O", CultureInfo.InvariantCulture)
            : null;

    /// Downloads the API's list of pages, then each page, one a second.
    public static async Task DownloadAsync(string file, CancellationToken cancellationToken) {
        using var http = StatusListDownload.CreateClient(TimeSpan.FromMinutes(2));
        void Retry(string message) => AnsiConsole.MarkupLineInterpolated($"[yellow]{message}[/]");
        var entries = new List<JsonElement>();
        for (var page = 1; ; page++) {
            string body;
            try {
                body = await StatusListDownload.GetStringAsync(http, ApiPageUrl(page), Retry, cancellationToken).ConfigureAwait(false);
            } catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.BadRequest && page > 1) {
                break; // WordPress answers 400 for a page past the last
            }
            using var document = JsonDocument.Parse(body);
            var rows = document.RootElement.EnumerateArray().Select(e => e.Clone()).ToList();
            entries.AddRange(rows);
            if (rows.Count < 100) {
                break;
            }
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]GoodFish lists {entries.Count:N0} pages.[/]");
        var pages = new List<JsonElement>();
        foreach (var entry in entries) {
            var id = entry.GetProperty("id").GetInt64();
            var html = await StatusListDownload.GetStringAsync(http, entry.GetProperty("link").GetString()!, Retry, cancellationToken).ConfigureAwait(false);
            pages.Add(JsonSerializer.SerializeToElement(new { id, html = KeptPart(html) }));
            if (pages.Count % 25 == 0) {
                AnsiConsole.MarkupLineInterpolated($"[grey]GoodFish pages: {pages.Count:N0} of {entries.Count:N0}[/]");
            }
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        }
        await StatusListDownload.WriteJsonObjectAsync(file, ("entries", entries), ("pages", pages)).ConfigureAwait(false);
    }

    // The first list of ratings (the one at the top of the page).
    [GeneratedRegex(@"<div class=""classifications"">\s*<ul>(.*?)</ul>", RegexOptions.Singleline)]
    private static partial Regex Classifications();

    [GeneratedRegex(@"<li[^>]*class=""[^""]*classification--[^""]*""[^>]*>(.*?)</li>", RegexOptions.Singleline)]
    private static partial Regex RatingItem();

    [GeneratedRegex(@"<h1>(.*?)</h1>", RegexOptions.Singleline)]
    private static partial Regex Heading();

    [GeneratedRegex(@"Latin names?:(.*?)<", RegexOptions.Singleline)]
    private static partial Regex LatinNames();

    [GeneratedRegex(@"<h3 class=""species-block__title grey"">(.*?)</h3>", RegexOptions.Singleline)]
    private static partial Regex Method();

    [GeneratedRegex(@"<h3 class=""species-block__title"">\s*Region:\s*<br\s*/?>(.*?)</h3>", RegexOptions.Singleline)]
    private static partial Regex Region();

    [GeneratedRegex(@"<div class=""note"">.*?<strong>Note:</strong>(.*?)</p>", RegexOptions.Singleline)]
    private static partial Regex Note();

    // The list that starts the "More information" text, when it starts with one: the fisheries.
    [GeneratedRegex(@"<strong>More information</strong></p>\s*<div class=""toggle-content[^""]*"">\s*<ul>(.*?)</ul>", RegexOptions.Singleline)]
    private static partial Regex MoreInformationList();

    [GeneratedRegex(@"<li[^>]*>(.*?)</li>", RegexOptions.Singleline)]
    private static partial Regex ListItem();
}
