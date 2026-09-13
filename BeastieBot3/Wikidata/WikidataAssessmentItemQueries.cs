using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;

// SPARQL for finding IUCN assessment publication items. Each query was timed against the live
// endpoints (2026-09-13) before being written down here; the ones left out are as instructive:
//
// - `?item wdt:P356 ?doi FILTER(STRSTARTS(?doi, "10.2305/IUCN.UK"))` answers in ~12s on the main
//   graph but times out (504 after 67s) on the scholarly graph, where it has to read every DOI.
//   Scholarly articles live only there since the graph split, so the DOI prefix is found through
//   the search index instead (DoiSearch), which covers both graphs.
// - Searching the whole prefix at once returns ~6,440 items but drops some: deep result pages
//   aren't stable, and 136 items with P1433 = IUCN Red List were missing. Slicing the prefix by
//   release year keeps each result set small (at most ~1,200) and recovered them.
// - A URL scan (`?item wdt:P953 ?url FILTER(STRSTARTS(STR(?url), "https://www.iucnredlist.org/"))`)
//   times out on both graphs; restricted to data set items it takes ~4s on main.
// - `?item wdt:P921 wd:Q32059` (main subject: IUCN Red List) is mostly papers about the Red List,
//   not assessments, so it isn't a route.

namespace BeastieBot3.Wikidata;

internal enum WikidataGraph {
    Main,
    Scholarly,
}

internal static class WikidataAssessmentItemQueries {
    public static readonly Uri ScholarlyEndpoint = new("https://query-scholarly.wikidata.org/sparql");

    public const string RouteEditions = "editions";
    public const string RoutePublishedIn = "published-in";
    public const string RouteDoiSearch = "doi-search";
    public const string RouteDoiScan = "doi-scan";
    public const string RouteDatasetUrl = "dataset-url";

    public static string GraphName(WikidataGraph graph) => graph == WikidataGraph.Scholarly ? "scholarly" : "main";

    public static string RouteTag(string route, WikidataGraph? graph) =>
        graph is null ? route : $"{route}:{GraphName(graph.Value)}";

    private const string Prefixes =
        """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX mwapi: <https://www.mediawiki.org/ontology#API/>

""";

    /// Editions of the IUCN Red List ("The IUCN Red List of Threatened Species 2016.1"). A few
    /// assessment items are published in an edition rather than in Q32059 itself. Main graph, ~2s.
    public static string Editions() =>
        Prefixes + $"SELECT ?item WHERE {{ ?item wdt:P629 wd:{WikidataAssessmentItemTable.RedListQid} }}";

    /// Items published in the Red List or one of its editions, cursor-paged on the numeric id.
    /// Scholarly graph: 6,569 rows in ~6s for Q32059 alone.
    public static string PublishedIn(IReadOnlyCollection<string> publicationQids, long cursor, int limit) {
        var values = string.Join(' ', publicationQids.Select(q => "wd:" + q));
        return Prefixes + $$"""
SELECT ?qid WHERE {
  VALUES ?pub { {{values}} }
  ?item wdt:P1433 ?pub .
  BIND(xsd:integer(STRAFTER(STR(?item), "http://www.wikidata.org/entity/Q")) AS ?qid)
  FILTER(?qid > {{cursor.ToString(CultureInfo.InvariantCulture)}})
}
GROUP BY ?qid
ORDER BY ?qid
LIMIT {{limit.ToString(CultureInfo.InvariantCulture)}}
""";
    }

    /// Search-index prefix match on P356, e.g. "10.2305/IUCN.UK.2016". Case-insensitive; returns
    /// item ids from either graph. ~3-6s per release year.
    public static string DoiSearch(string doiPrefix) {
        var search = "haswbstatement:P356=" + doiPrefix + "*";
        return Prefixes + $$"""
SELECT DISTINCT ?title WHERE {
  SERVICE wikibase:mwapi {
    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                    wikibase:api "Search";
                    mwapi:srsearch "{{EscapeLiteral(search)}}";
                    mwapi:srnamespace "0" .
    ?title wikibase:apiOutput mwapi:title .
  }
}
""";
    }

    /// The prefixes DoiSearch is run with: the 1990s as one slice (~420 items), then one per year
    /// up to next year.
    public static IReadOnlyList<string> DoiSearchSlices(int throughYear) {
        var slices = new List<string> { IucnPrefix + "199" };
        for (var year = 2000; year <= throughYear; year++) {
            slices.Add(IucnPrefix + year.ToString(CultureInfo.InvariantCulture));
        }

        return slices;
    }

    private const string IucnPrefix = "10.2305/IUCN.UK.";

    /// Main graph only (~12s): on the scholarly graph this scan times out.
    public static string DoiScan() => Prefixes + """
SELECT ?item WHERE {
  ?item wdt:P356 ?doi .
  FILTER(STRSTARTS(?doi, "10.2305/IUCN.UK"))
}
""";

    /// Data set items pointing at the Red List site. Main graph, ~4s.
    public static string DatasetUrl() => Prefixes + """
SELECT DISTINCT ?item WHERE {
  ?item wdt:P31 wd:Q1172284 .
  ?item wdt:P953|wdt:P854|wdt:P856|wdt:P973 ?url .
  FILTER(CONTAINS(STR(?url), "iucnredlist.org"))
}
""";

    public static readonly string[] DetailProperties = {
        "P31", "P356", "P1433", "P921", "P577", "P1476", "P50", "P2093", "P953", "P854", "P856",
    };

    /// One row per (item, property, value) for the given items: no OPTIONAL cross products, so a
    /// batch of 250 items is a few thousand rows. Items not in the queried graph return nothing.
    public static string Details(IReadOnlyCollection<string> qids) {
        var items = string.Join(' ', qids.Select(q => "wd:" + q));
        var properties = string.Join(' ', DetailProperties.Select(p => "wdt:" + p)) + " rdfs:label schema:dateModified";
        return Prefixes + $$"""
SELECT ?item ?p ?v WHERE {
  VALUES ?item { {{items}} }
  VALUES ?p { {{properties}} }
  ?item ?p ?v .
  FILTER(?p != rdfs:label || LANG(?v) = "en")
}
""";
    }

    // ------------------------------------------------------------------ parsing

    private const string EntityPrefix = "http://www.wikidata.org/entity/";

    /// Item ids from one result column, whether bound as an entity IRI, a "Q123" title or a
    /// numeric id.
    public static IReadOnlyList<string> ParseItemIds(string json, string variable) {
        var list = new List<string>();
        foreach (var binding in Bindings(json)) {
            if (!binding.TryGetProperty(variable, out var cell) || !cell.TryGetProperty("value", out var raw)) {
                continue;
            }

            if (ToItemId(raw.GetString()) is { } qid) {
                list.Add(qid);
            }
        }

        return list;
    }

    public static IReadOnlyDictionary<string, List<WikidataTriple>> ParseDetails(string json) {
        var result = new Dictionary<string, List<WikidataTriple>>(StringComparer.Ordinal);
        foreach (var binding in Bindings(json)) {
            if (!binding.TryGetProperty("item", out var itemCell)
                || !binding.TryGetProperty("p", out var propertyCell)
                || !binding.TryGetProperty("v", out var valueCell)) {
                continue;
            }

            var qid = ToItemId(itemCell.GetProperty("value").GetString());
            var property = ShortProperty(propertyCell.GetProperty("value").GetString());
            if (qid is null || property is null) {
                continue;
            }

            var rawValue = valueCell.GetProperty("value").GetString() ?? string.Empty;
            var isUri = valueCell.TryGetProperty("type", out var type) && type.GetString() == "uri";
            var value = isUri && rawValue.StartsWith(EntityPrefix, StringComparison.Ordinal)
                ? rawValue[EntityPrefix.Length..]
                : rawValue;
            string? language = valueCell.TryGetProperty("xml:lang", out var lang) ? lang.GetString() : null;

            if (!result.TryGetValue(qid, out var list)) {
                result[qid] = list = new List<WikidataTriple>();
            }

            list.Add(new WikidataTriple(property, value, language));
        }

        return result;
    }

    private static string? ShortProperty(string? iri) => iri switch {
        null => null,
        "http://www.w3.org/2000/01/rdf-schema#label" => "label",
        "http://schema.org/dateModified" => "modified",
        _ when iri.StartsWith("http://www.wikidata.org/prop/direct/", StringComparison.Ordinal) =>
            iri["http://www.wikidata.org/prop/direct/".Length..],
        _ => null,
    };

    internal static string? ToItemId(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var text = value.Trim();
        if (text.StartsWith(EntityPrefix, StringComparison.Ordinal)) {
            text = text[EntityPrefix.Length..];
        }

        if (text.Length > 1 && (text[0] == 'Q' || text[0] == 'q') && text.Skip(1).All(char.IsAsciiDigit)) {
            return "Q" + text[1..];
        }

        return text.All(char.IsAsciiDigit) && text.Length > 0 ? "Q" + text : null;
    }

    private static IEnumerable<JsonElement> Bindings(string json) {
        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("results", out var results)
            || !results.TryGetProperty("bindings", out var bindings)) {
            yield break;
        }

        // Clone so the elements outlive the document once the iterator moves on.
        foreach (var binding in bindings.EnumerateArray()) {
            yield return binding.Clone();
        }
    }

    private static string EscapeLiteral(string value) =>
        new StringBuilder(value).Replace("\\", "\\\\").Replace("\"", "\\\"").ToString();
}
