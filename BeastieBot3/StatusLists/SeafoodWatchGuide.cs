using System.Globalization;
using System.Text.Json;

// Seafood Watch (seafoodwatch.org, Monterey Bay Aquarium). Its search page reads every
// recommendation from one JSON endpoint, /api/recs (about 1,950 recommendations in 94 groups; it
// answers 404 without the query and requestTime parameters). The kept file is that answer as given.
// Each recommendation has the scientific names (sName, a few with several names joined by commas),
// country and body of water, methods ("Bottom trawls#Wild|Longlines#Wild"), the fishery or stock
// (detail), the rating (buyText: Best Choice, Good Alternative, Avoid, or Certified for a product
// with an eco-certification Seafood Watch recognises) and its colour. Its summary and the groups'
// buying advice are narrative and are not stored. Seafood Watch gives no dates in this answer; each
// recommendation's report has them.

namespace BeastieBot3.StatusLists;

internal static class SeafoodWatchGuide {
    public const string Title = "Seafood Watch";
    public const string SiteUrl = "https://www.seafoodwatch.org/recommendations/search";
    public const string Licence = "All rights reserved (Monterey Bay Aquarium Foundation); terms allow personal use only";
    private const string BaseUrl = "https://www.seafoodwatch.org";

    public static string Citation(DateTime accessed) =>
        $"Monterey Bay Aquarium Seafood Watch. Seafood Recommendations. https://www.seafoodwatch.org/recommendations/search (accessed {accessed:d MMMM yyyy}).";

    public static string RecommendationsUrl(DateTime now) =>
        $"{BaseUrl}/api/recs?query=&requestTime={Uri.EscapeDataString(now.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture))}";

    /// The answer's version (the "version" property), for status_source.version.
    public static string? Version(string json) {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.TryGetProperty("version", out var version) ? version.ToString() : null;
    }

    public static IReadOnlyList<SeafoodRating> Read(string json, IReadOnlyDictionary<string, string> fixes) {
        using var document = JsonDocument.Parse(json);
        var ratings = new List<SeafoodRating>();
        foreach (var group in document.RootElement.GetProperty("result").EnumerateArray()) {
            var groupName = Text(group, "name");
            foreach (var rec in group.GetProperty("recs").EnumerateArray()) {
                if (ReadRecommendation(rec, groupName, fixes) is { } rating) {
                    ratings.Add(rating);
                }
            }
        }
        return SeafoodNames.ExpandAcrossRatings(ratings);
    }

    /// One recommendation; null when it has no id or rating.
    public static SeafoodRating? ReadRecommendation(JsonElement rec, string? groupName, IReadOnlyDictionary<string, string> fixes) {
        var id = rec.TryGetProperty("id", out var idValue) ? idValue.ToString() : null;
        var rating = Text(rec, "buyText");
        if (string.IsNullOrEmpty(id) || rating is null) {
            return null;
        }
        var (methods, production) = Methods(Text(rec, "method"));
        var link = Text(rec, "link");
        return new SeafoodRating(
            SeafoodGuideKeys.SeafoodWatch,
            id,
            Text(rec, "name") ?? Text(rec, "sName") ?? id,
            Text(rec, "groupName") ?? groupName,
            [rating],
            Text(rec, "color"),
            Text(rec, "score"),
            Text(rec, "formatedC"),
            Text(rec, "bow")?.Replace(",", ", ", StringComparison.Ordinal),
            methods,
            production,
            Parts(Text(rec, "detail")),
            Note: null,
            link is null ? SiteUrl : link.StartsWith('/') ? BaseUrl + link : link,
            Published: null,
            Modified: null,
            SeafoodNames.Read(SeafoodNames.Split(Text(rec, "sName")), fixes));
    }

    /// "Bottom trawls#Wild|Pots#Wild" as the methods ("Bottom trawls", "Pots") and the production
    /// ('wild', 'farmed', or 'wild|farmed' when the recommendation has both).
    public static (IReadOnlyList<string> Methods, string? Production) Methods(string? method) {
        var parts = Parts(method).Select(p => p.Split('#', 2)).ToList();
        var methods = parts.Select(p => p[0].Trim()).Where(m => m.Length > 0).Distinct().ToList();
        var kinds = parts.Where(p => p.Length == 2).Select(p => p[1].Trim().ToLowerInvariant()).Where(k => k is "wild" or "farmed").Distinct()
            .OrderBy(k => k == "wild" ? 0 : 1).ToList();
        return (methods, kinds.Count == 0 ? null : string.Join('|', kinds));
    }

    private static IReadOnlyList<string> Parts(string? value) =>
        value is null ? [] : value.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Where(p => p != "N/A").ToList();

    // A string property's text; null when missing, empty or "N/A".
    private static string? Text(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
        && value.GetString()?.Trim() is { Length: > 0 } text && text != "N/A"
            ? text
            : null;
}
