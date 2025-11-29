using System.Text.Json;

namespace BeastieBot3;

internal static class WikidataSitelinkExtractor {
    public static bool TryGetEnwikiTitle(string json, out string? title) => TryGetSitelinkTitle(json, "enwiki", out title);

    public static bool TryGetSitelinkTitle(string json, string siteKey, out string? title) {
        title = null;
        if (string.IsNullOrWhiteSpace(json)) {
            return false;
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (!root.TryGetProperty("entities", out var entities) || entities.ValueKind != JsonValueKind.Object) {
            return false;
        }

        foreach (var entityProperty in entities.EnumerateObject()) {
            if (!entityProperty.Value.TryGetProperty("sitelinks", out var sitelinks)) {
                continue;
            }

            if (!sitelinks.TryGetProperty(siteKey, out var siteEntry)) {
                continue;
            }

            title = siteEntry.TryGetProperty("title", out var titleElement) ? titleElement.GetString() : null;
            return !string.IsNullOrWhiteSpace(title);
        }

        return false;
    }
}
