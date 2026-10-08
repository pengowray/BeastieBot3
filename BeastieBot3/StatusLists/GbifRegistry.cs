using System.Text.Json;

// What `statuses red-lists-import` reads from the GBIF registry about a dataset
// (GET https://api.gbif.org/v1/dataset/<key>, one request per dataset and run): its title, licence,
// pubDate, DOI, GBIF's citation of it and its DWC_ARCHIVE endpoint. The pubDate decides whether the
// archive is downloaded again.

namespace BeastieBot3.StatusLists;

/// Licence: the short name (CC0 1.0, CC BY 4.0, CC BY-NC 4.0), or the registry's value as given when
/// it is another licence. PubDate: yyyy-MM-dd.
internal sealed record GbifDatasetInfo(
    string Key,
    string Title,
    string? Licence,
    string? PubDate,
    string? Doi,
    string? Citation,
    string? ArchiveUrl,
    bool Deleted);

internal static class GbifRegistry {
    public static string DatasetUrl(string key) => $"https://api.gbif.org/v1/dataset/{Uri.EscapeDataString(key)}";

    public static string DatasetPage(string key) => $"https://www.gbif.org/dataset/{Uri.EscapeDataString(key)}";

    public static GbifDatasetInfo Parse(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        string? Text(JsonElement element, string property) =>
            element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && value.GetString() is { Length: > 0 } text
                ? text.Trim()
                : null;
        var archive = root.TryGetProperty("endpoints", out var endpoints) && endpoints.ValueKind == JsonValueKind.Array
            ? endpoints.EnumerateArray().Where(e => Text(e, "type") == "DWC_ARCHIVE").Select(e => Text(e, "url")).FirstOrDefault(u => u is not null)
            : null;
        var citation = root.TryGetProperty("citation", out var cite) && cite.ValueKind == JsonValueKind.Object ? Text(cite, "text") : null;
        var pubDate = Text(root, "pubDate");
        return new GbifDatasetInfo(
            Text(root, "key") ?? "",
            Text(root, "title") ?? "",
            LicenceName(Text(root, "license")),
            pubDate is { Length: >= 10 } ? pubDate[..10] : pubDate,
            Text(root, "doi"),
            citation,
            archive,
            Text(root, "deleted") is not null);
    }

    /// The short name of a Creative Commons licence URL: http://creativecommons.org/licenses/by/4.0/legalcode
    /// is "CC BY 4.0". Another value is returned as given; none is null.
    internal static string? LicenceName(string? url) {
        if (string.IsNullOrWhiteSpace(url)) {
            return null;
        }
        var path = url.Trim().ToLowerInvariant();
        return path.Contains("publicdomain/zero/1.0") ? "CC0 1.0"
            : path.Contains("licenses/by-nc/4.0") ? "CC BY-NC 4.0"
            : path.Contains("licenses/by/4.0") ? "CC BY 4.0"
            : url.Trim();
    }
}
