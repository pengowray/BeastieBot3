using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

// NatureServe Explorer's species search (POST https://explorer.natureserve.org/api/data/speciesSearch,
// documented at https://explorer.natureserve.org/api-docs/): the request body and the reading of a
// page of results. Pure, so tests run it on saved JSON.
//
// The search answers HTTP 500 for any page past its first 10,000 records (page 100 at 100 records a
// page, checked 2026-10-08), so a full download asks for one scientific name prefix at a time
// ("A", then "Aa".."Az" when "A" has more than 10,000 records). The textSearch "startsWith" match
// on scientificName is a prefix of the whole name, case does not matter, and the counts of "A".."Z"
// add up to the total. Within one query the order is fixed (informal group, then name), so pages do
// not overlap.

namespace BeastieBot3.StatusLists;

/// One NatureServe Explorer record, as `statuses natureserve-fetch` stores it.
internal sealed record NatureServeSpecies(
    long ElementGlobalId,
    string UniqueId,
    string? Elcode,
    string ScientificName,
    string? PrimaryCommonName,
    string? PrimaryCommonNameLanguage,
    string? GRank,
    string? RoundedGRank,
    string? ClassificationStatus,
    string? Kingdom,
    string? Phylum,
    string? TaxClass,
    string? TaxOrder,
    string? Family,
    string? Genus,
    string? InformalTaxonomy,
    bool Infraspecies,
    string? UsesaCode,
    string? CosewicCode,
    string? SaraCode,
    string? SaraCodeRaw,
    string? UsNRank,
    string? CaNRank,
    string NsxUrl,
    string? LastModified,
    IReadOnlyList<string> Synonyms);

internal sealed record NatureServePage(long TotalResults, int ResultCount, IReadOnlyList<NatureServeSpecies> Species);

internal static class NatureServeSearch {
    public const string SearchUrl = "https://explorer.natureserve.org/api/data/speciesSearch";
    public const string UnpublishedUrl = "https://explorer.natureserve.org/api/data/unpublishedTaxa";
    public const string SiteUrl = "https://explorer.natureserve.org";
    public const int PageSize = 100;            // the largest page the search allows
    public const int MaxRecordsPerQuery = 10_000;

    /// The species search body for one page of one name prefix ("" for every record), with records
    /// modified since <paramref name="modifiedSinceUtc"/> only when it is given.
    public static string BuildRequest(string prefix, int page, DateTime? modifiedSinceUtc, int pageSize = PageSize) {
        var body = new JsonObject {
            ["criteriaType"] = "species",
            ["textCriteria"] = prefix.Length == 0
                ? new JsonArray()
                : new JsonArray(new JsonObject {
                    ["paramType"] = "textSearch",
                    ["searchToken"] = prefix,
                    ["matchAgainst"] = "scientificName",
                    ["operator"] = "startsWith",
                }),
            ["pagingOptions"] = new JsonObject { ["page"] = page, ["recordsPerPage"] = pageSize },
        };
        if (modifiedSinceUtc is { } since) {
            body["modifiedSince"] = since.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        }
        return body.ToJsonString();
    }

    public static string BuildUnpublishedRequest(DateTime? sinceUtc) {
        var body = new JsonObject { ["criteriaType"] = "unpublished" };
        if (sinceUtc is { } since) {
            body["unpublishedSince"] = since.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        }
        return body.ToJsonString();
    }

    /// The prefixes that replace <paramref name="prefix"/> when it has more than 10,000 records:
    /// "A".."Z" for every record, else the prefix followed by each small letter.
    public static IReadOnlyList<string> Split(string prefix) {
        var letters = prefix.Length == 0 ? "ABCDEFGHIJKLMNOPQRSTUVWXYZ" : "abcdefghijklmnopqrstuvwxyz";
        return letters.Select(c => prefix + c).ToList();
    }

    /// The number of pages needed for <paramref name="total"/> records.
    public static int PageCount(long total) => (int)((total + PageSize - 1) / PageSize);

    public static NatureServePage ParsePage(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        long total = 0;
        if (root.TryGetProperty("resultsSummary", out var summary) && summary.TryGetProperty("totalResults", out var t) && t.ValueKind == JsonValueKind.Number) {
            total = t.GetInt64();
        }
        var species = new List<NatureServeSpecies>();
        var count = 0;
        if (root.TryGetProperty("results", out var results) && results.ValueKind == JsonValueKind.Array) {
            foreach (var result in results.EnumerateArray()) {
                count++;
                if (ParseSpecies(result) is { } s) {
                    species.Add(s);
                }
            }
        }
        return new NatureServePage(total, count, species);
    }

    /// The element global ids of the records unpublished from Explorer.
    public static IReadOnlyList<long> ParseUnpublished(string json) {
        using var document = JsonDocument.Parse(json);
        var ids = new List<long>();
        if (document.RootElement.TryGetProperty("unpublishedRecords", out var records) && records.ValueKind == JsonValueKind.Array) {
            foreach (var record in records.EnumerateArray()) {
                if (record.TryGetProperty("elementGlobalId", out var id) && id.ValueKind == JsonValueKind.Number) {
                    ids.Add(id.GetInt64());
                }
            }
        }
        return ids;
    }

    /// One result, or null when it is not a species record or has no id or name.
    internal static NatureServeSpecies? ParseSpecies(JsonElement result) {
        if (Text(result, "recordType") is { } type && !type.Equals("SPECIES", StringComparison.OrdinalIgnoreCase)) {
            return null;
        }
        if (!result.TryGetProperty("elementGlobalId", out var idElement) || idElement.ValueKind != JsonValueKind.Number
            || Text(result, "scientificName") is not { } name) {
            return null;
        }
        var id = idElement.GetInt64();
        var global = result.TryGetProperty("speciesGlobal", out var g) && g.ValueKind == JsonValueKind.Object ? g : default;
        string? GlobalText(string property) => global.ValueKind == JsonValueKind.Object ? Text(global, property) : null;

        var saraRaw = GlobalText("saraCode");
        var url = Text(result, "nsxUrl") is { } relative
            ? (relative.StartsWith("http", StringComparison.OrdinalIgnoreCase) ? relative : SiteUrl + relative)
            : $"{SiteUrl}/Taxon/ELEMENT_GLOBAL.2.{id}";
        return new NatureServeSpecies(
            ElementGlobalId: id,
            UniqueId: Text(result, "uniqueId") ?? $"ELEMENT_GLOBAL.2.{id}",
            Elcode: Text(result, "elcode"),
            ScientificName: name,
            PrimaryCommonName: Text(result, "primaryCommonName"),
            PrimaryCommonNameLanguage: Text(result, "primaryCommonNameLanguage"),
            GRank: Text(result, "gRank"),
            RoundedGRank: Text(result, "roundedGRank"),
            ClassificationStatus: Text(result, "classificationStatus"),
            Kingdom: GlobalText("kingdom"),
            Phylum: GlobalText("phylum"),
            TaxClass: GlobalText("taxclass"),
            TaxOrder: GlobalText("taxorder"),
            Family: GlobalText("family"),
            Genus: GlobalText("genus"),
            InformalTaxonomy: GlobalText("informalTaxonomy"),
            Infraspecies: global.ValueKind == JsonValueKind.Object && global.TryGetProperty("infraspecies", out var infra) && infra.ValueKind == JsonValueKind.True,
            UsesaCode: GlobalText("usesaCode"),
            CosewicCode: GlobalText("cosewicCode"),
            SaraCode: EnglishPart(saraRaw),
            SaraCodeRaw: saraRaw,
            UsNRank: NationRank(result, "US"),
            CaNRank: NationRank(result, "CA"),
            NsxUrl: url,
            LastModified: Text(result, "lastModified"),
            Synonyms: Strings(global, "synonyms"));
    }

    /// The English part of a bilingual SARA status ("Endangered/En voie de disparition" gives
    /// "Endangered"); the text as given when it has no "/".
    public static string? EnglishPart(string? bilingual) {
        if (string.IsNullOrWhiteSpace(bilingual)) {
            return null;
        }
        var slash = bilingual.IndexOf('/');
        var english = (slash < 0 ? bilingual : bilingual[..slash]).Trim();
        return english.Length == 0 ? null : english;
    }

    private static string? NationRank(JsonElement result, string nationCode) {
        if (!result.TryGetProperty("nations", out var nations) || nations.ValueKind != JsonValueKind.Array) {
            return null;
        }
        foreach (var nation in nations.EnumerateArray()) {
            if (string.Equals(Text(nation, "nationCode"), nationCode, StringComparison.OrdinalIgnoreCase)) {
                return Text(nation, "roundedNRank");
            }
        }
        return null;
    }

    private static IReadOnlyList<string> Strings(JsonElement element, string property) {
        if (element.ValueKind != JsonValueKind.Object || !element.TryGetProperty(property, out var array) || array.ValueKind != JsonValueKind.Array) {
            return Array.Empty<string>();
        }
        var list = new List<string>();
        foreach (var item in array.EnumerateArray()) {
            if (item.ValueKind == JsonValueKind.String && item.GetString()?.Trim() is { Length: > 0 } s && !list.Contains(s, StringComparer.Ordinal)) {
                list.Add(s);
            }
        }
        return list;
    }

    private static string? Text(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && value.GetString()?.Trim() is { Length: > 0 } s
            ? s
            : null;
}
