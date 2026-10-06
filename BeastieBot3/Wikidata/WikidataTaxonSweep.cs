using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

// The query and the result parser of `wikidata sweep-taxa`, which reads a short record of every
// Wikidata item with a taxon name (P225): about 4 million items in October 2026.
//
// The sweep uses the QLever Wikidata endpoint, not the Wikidata Query Service. A page of 100,000
// items, ordered by Q-number after a cursor, takes about 12 seconds on QLever. The Query Service
// computes the numeric id of every taxon item before the cursor filter and the LIMIT can run (see
// WikidataApiClient.BuildTaxonQuery), which for 4 million items does not finish inside its 60-second
// limit at any page size.
//
// One result row per combination of values, so an item with two parent taxa has two rows. Rows are
// ordered by Q-number, so every row of an item is next to the others. A full page can stop part way
// through an item's rows: the command drops the last item of a full page and starts the next page
// before it (TakeComplete).

namespace BeastieBot3.Wikidata;

internal sealed record WikidataSweptTaxon(
    long Qid,
    string TaxonName,
    IReadOnlyList<string> OtherNames,
    long? RankQid,
    IReadOnlyList<long> ParentQids,
    IReadOnlyList<string> ColIds,
    IReadOnlyList<string> IucnTaxonIds,
    string? EnwikiTitle,
    string? LabelEn);

internal sealed record WikidataSweepPage(IReadOnlyList<WikidataSweptTaxon> Items, int RowCount);

internal static class WikidataTaxonSweep {
    public static readonly Uri DefaultEndpoint = new("https://qlever.dev/api/wikidata");

    public const string SpeciesRankQid = "Q7432";
    public const long SpeciesRank = 7432;

    private const string EntityPrefix = "http://www.wikidata.org/entity/Q";
    private const string EnwikiPrefix = "https://en.wikipedia.org/wiki/";

    public static string BuildQuery(long cursor, int limit) => $$"""
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?qid ?name ?rank ?parent ?col ?iucn ?enwiki ?label WHERE {
          ?item wdt:P225 ?name .
          BIND(xsd:integer(STRAFTER(STR(?item), "{{EntityPrefix}}")) AS ?qid)
          FILTER(?qid > {{cursor}})
          OPTIONAL { ?item wdt:P105 ?rank . }
          OPTIONAL { ?item wdt:P171 ?parent . }
          OPTIONAL { ?item wdt:P10585 ?col . }
          OPTIONAL { ?item wdt:P627 ?iucn . }
          OPTIONAL { ?enwiki schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . }
          OPTIONAL { ?item rdfs:label ?label . FILTER(LANG(?label) = "en") }
        }
        ORDER BY ?qid
        LIMIT {{limit}}
        """;

    /// How many items have a taxon name, for the sweep's progress line. QLever answers in about
    /// 2 seconds.
    public const string CountQuery = """
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        SELECT (COUNT(DISTINCT ?item) AS ?total) WHERE { ?item wdt:P225 ?name . }
        """;

    public static long? ParseCount(string json) {
        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("results", out var results)
            || !results.TryGetProperty("bindings", out var bindings)) {
            return null;
        }
        foreach (var binding in bindings.EnumerateArray()) {
            if (long.TryParse(Text(binding, "total"), out var total)) {
                return total;
            }
        }
        return null;
    }

    /// Groups the result rows by item. Items keep the result's order (by Q-number).
    public static WikidataSweepPage Parse(string json) {
        using var document = JsonDocument.Parse(json);
        var items = new List<WikidataSweptTaxon>();
        var rowCount = 0;
        if (!document.RootElement.TryGetProperty("results", out var results)
            || !results.TryGetProperty("bindings", out var bindings)) {
            return new WikidataSweepPage(items, 0);
        }

        Builder? current = null;
        foreach (var binding in bindings.EnumerateArray()) {
            rowCount++;
            if (!long.TryParse(Text(binding, "qid"), out var qid) || Text(binding, "name") is not { } name) {
                continue;
            }
            if (current is null || current.Qid != qid) {
                if (current is not null) {
                    items.Add(current.Build());
                }
                current = new Builder(qid);
            }
            current.Names.Add(name);
            if (EntityNumber(Text(binding, "rank")) is { } rank) {
                current.Ranks.Add(rank);
            }
            if (EntityNumber(Text(binding, "parent")) is { } parent) {
                current.Parents.Add(parent);
            }
            if (Text(binding, "col") is { } col) {
                current.ColIds.Add(col);
            }
            if (Text(binding, "iucn") is { } iucn) {
                current.IucnIds.Add(iucn);
            }
            if (EnwikiTitle(Text(binding, "enwiki")) is { } title) {
                current.EnwikiTitle ??= title;
            }
            if (Text(binding, "label") is { } label) {
                current.Label ??= label;
            }
        }
        if (current is not null) {
            items.Add(current.Build());
        }
        return new WikidataSweepPage(items, rowCount);
    }

    /// The items of a page that are certainly complete. When the page is full, the rows of its last
    /// item may continue on the next page, so that item is left for the next page; unless it is the
    /// only item, which then has more rows than a page and is kept as it is.
    public static IReadOnlyList<WikidataSweptTaxon> TakeComplete(WikidataSweepPage page, int limit) =>
        page.RowCount >= limit && page.Items.Count > 1 ? page.Items.Take(page.Items.Count - 1).ToList() : page.Items;

    /// "Phascolarctos cinereus" from "https://en.wikipedia.org/wiki/Phascolarctos_cinereus".
    public static string? EnwikiTitle(string? url) {
        if (url is null || !url.StartsWith(EnwikiPrefix, StringComparison.Ordinal)) {
            return null;
        }
        var title = Uri.UnescapeDataString(url[EnwikiPrefix.Length..]).Replace('_', ' ');
        return title.Length == 0 ? null : title;
    }

    public static long? EntityNumber(string? uri) =>
        uri is not null && uri.StartsWith(EntityPrefix, StringComparison.Ordinal)
        && long.TryParse(uri.AsSpan(EntityPrefix.Length), out var n) ? n : null;

    private static string? Text(JsonElement binding, string name) =>
        binding.TryGetProperty(name, out var element) && element.TryGetProperty("value", out var value)
        && value.GetString() is { Length: > 0 } text ? text : null;

    private sealed class Builder {
        public Builder(long qid) => Qid = qid;
        public long Qid { get; }
        public SortedSet<string> Names { get; } = new(StringComparer.Ordinal);
        public SortedSet<long> Ranks { get; } = new();
        public SortedSet<long> Parents { get; } = new();
        public SortedSet<string> ColIds { get; } = new(StringComparer.Ordinal);
        public SortedSet<string> IucnIds { get; } = new(StringComparer.Ordinal);
        public string? EnwikiTitle { get; set; }
        public string? Label { get; set; }

        // The first name in ordinal order is the item's name; the rest are kept as other names. An
        // English label is kept only when it is not one of the taxon names, since most taxon items
        // have their scientific name as their English label.
        public WikidataSweptTaxon Build() {
            var name = Names.Min!;
            var label = Label is not null && !Names.Contains(Label, StringComparer.OrdinalIgnoreCase) ? Label : null;
            return new WikidataSweptTaxon(Qid, name, Names.Skip(1).ToList(), Ranks.Count > 0 ? Ranks.Min : null,
                Parents.ToList(), ColIds.ToList(), IucnIds.ToList(), EnwikiTitle, label);
        }
    }
}
