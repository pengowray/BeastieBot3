using System;

namespace BeastieBot3;

internal sealed record WikidataConfiguration(
    Uri ApiEndpoint,
    Uri SparqlEndpoint,
    string UserAgent,
    TimeSpan Timeout,
    TimeSpan RequestDelay,
    TimeSpan SparqlDelay,
    int SparqlBatchSize
) {
    public static WikidataConfiguration FromEnvironment() {
        EnvFileLoader.LoadIfPresent();

        var apiUrl = ReadUri("WIKIDATA_API_ENDPOINT", "https://www.wikidata.org/w/api.php");
        var sparqlUrl = ReadUri("WIKIDATA_SPARQL_ENDPOINT", "https://query.wikidata.org/sparql");
        var userAgent = Environment.GetEnvironmentVariable("WIKIDATA_USER_AGENT")?.Trim();
        if (string.IsNullOrWhiteSpace(userAgent)) {
            userAgent = "BeastieBot3/1.0 (+https://github.com/pengowray/BeastieBot3)";
        }

        var timeoutSeconds = ReadInt("WIKIDATA_TIMEOUT_SECONDS", 120, min: 5, max: 600);
        var requestDelayMs = ReadInt("WIKIDATA_REQUEST_DELAY_MS", 500, min: 0, max: 10_000);
        var sparqlDelayMs = ReadInt("WIKIDATA_SPARQL_DELAY_MS", 1_000, min: 100, max: 10_000);
        var sparqlBatchSize = ReadInt("WIKIDATA_SPARQL_BATCH_SIZE", 500, min: 50, max: 2_000);

        return new WikidataConfiguration(
            apiUrl,
            sparqlUrl,
            userAgent,
            TimeSpan.FromSeconds(timeoutSeconds),
            TimeSpan.FromMilliseconds(requestDelayMs),
            TimeSpan.FromMilliseconds(sparqlDelayMs),
            sparqlBatchSize
        );
    }

    private static Uri ReadUri(string key, string fallback) {
        var raw = Environment.GetEnvironmentVariable(key) ?? fallback;
        if (!Uri.TryCreate(raw, UriKind.Absolute, out var uri)) {
            throw new InvalidOperationException($"{key} value '{raw}' is not a valid absolute URI.");
        }

        return uri;
    }

    private static int ReadInt(string key, int fallback, int min, int max) {
        var raw = Environment.GetEnvironmentVariable(key);
        if (int.TryParse(raw, out var parsed)) {
            return Math.Clamp(parsed, min, max);
        }

        return fallback;
    }
}
