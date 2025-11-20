using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace BeastieBot3;

internal sealed class WikidataApiClient : IDisposable {
    private readonly HttpClient _apiClient;
    private readonly HttpClient _sparqlClient;
    private readonly WikidataConfiguration _configuration;
    private readonly SemaphoreSlim _apiSemaphore = new(1, 1);
    private readonly SemaphoreSlim _sparqlSemaphore = new(1, 1);
    private DateTime _nextApiAllowed = DateTime.MinValue;
    private DateTime _nextSparqlAllowed = DateTime.MinValue;

    public WikidataApiClient(WikidataConfiguration configuration) {
        _configuration = configuration;

        var apiHandler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
        };

        _apiClient = new HttpClient(apiHandler) {
            BaseAddress = configuration.ApiEndpoint,
            Timeout = configuration.Timeout
        };
        ConfigureDefaultHeaders(_apiClient.DefaultRequestHeaders, configuration.UserAgent, "application/json");

        var sparqlHandler = new SocketsHttpHandler {
            AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
        };

        _sparqlClient = new HttpClient(sparqlHandler) {
            BaseAddress = configuration.SparqlEndpoint,
            Timeout = configuration.Timeout
        };
        ConfigureDefaultHeaders(_sparqlClient.DefaultRequestHeaders, configuration.UserAgent, "application/sparql-results+json");
    }

    public async Task<WikidataApiResponse> GetEntityAsync(string entityId, CancellationToken cancellationToken) {
        await _apiSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            _nextApiAllowed = await EnforceRateLimitAsync(_nextApiAllowed, _configuration.RequestDelay, cancellationToken).ConfigureAwait(false);
            var payload = await SendEntityRequestAsync(entityId, cancellationToken).ConfigureAwait(false);
            return payload;
        }
        finally {
            _apiSemaphore.Release();
        }
    }

    public async Task<IReadOnlyList<WikidataSeedRow>> QueryTaxonSeedsAsync(long cursor, int limit, CancellationToken cancellationToken) {
        await _sparqlSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try {
            _nextSparqlAllowed = await EnforceRateLimitAsync(_nextSparqlAllowed, _configuration.SparqlDelay, cancellationToken).ConfigureAwait(false);
            var query = BuildTaxonQuery(cursor, limit);
            var response = await SendWithRetryAsync(
                _sparqlClient,
                HttpMethod.Post,
                string.Empty,
                () => new FormUrlEncodedContent(new[] { new KeyValuePair<string, string>("query", query) }),
                cancellationToken).ConfigureAwait(false);
            return ParseSeedResponse(response.Body);
        }
        finally {
            _sparqlSemaphore.Release();
        }
    }

    private async Task<WikidataApiResponse> SendEntityRequestAsync(string entityId, CancellationToken cancellationToken) {
        return await SendWithRetryAsync(
            _apiClient,
            HttpMethod.Post,
            string.Empty,
            () => new FormUrlEncodedContent(new Dictionary<string, string> {
                ["action"] = "wbgetentities",
                ["format"] = "json",
                ["formatversion"] = "2",
                ["props"] = "info|labels|descriptions|claims",
                ["ids"] = entityId,
                ["languages"] = "en",
                ["normalize"] = "1"
            }),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<WikidataApiResponse> SendWithRetryAsync(HttpClient client, HttpMethod method, string relativeUrl, Func<HttpContent?>? contentFactory, CancellationToken cancellationToken) {
        var attempt = 0;
        var delay = _configuration.RequestDelay;
        while (true) {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            using var request = new HttpRequestMessage(method, relativeUrl.Length == 0 ? client.BaseAddress : relativeUrl.StartsWith("http", StringComparison.OrdinalIgnoreCase) ? new Uri(relativeUrl) : new Uri(client.BaseAddress!, relativeUrl));
            if (contentFactory is not null) {
                request.Content = contentFactory();
            }

            try {
                var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
                var payload = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                var length = response.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(payload);

                if (response.IsSuccessStatusCode) {
                    return new WikidataApiResponse(request.RequestUri?.ToString() ?? string.Empty, payload, response.StatusCode, length);
                }

                if (!ShouldRetry(response.StatusCode, attempt)) {
                    throw new WikidataApiException(request.RequestUri?.ToString() ?? relativeUrl, response.StatusCode, payload, attempt);
                }

                delay = await DelayAfterFailureAsync(response, delay, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) {
                throw;
            }
            catch (WikidataApiException) {
                throw;
            }
            catch (Exception ex) when (attempt < 5) {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = NextDelay(delay);
                if (attempt >= 5) {
                    throw new WikidataApiException(relativeUrl, null, ex.Message, attempt, ex);
                }
            }
        }
    }

    private static bool ShouldRetry(HttpStatusCode statusCode, int attempt) {
        if (attempt >= 5) {
            return false;
        }

        return statusCode is HttpStatusCode.TooManyRequests
            or HttpStatusCode.RequestTimeout
            or HttpStatusCode.BadGateway
            or HttpStatusCode.ServiceUnavailable
            or HttpStatusCode.GatewayTimeout
            or HttpStatusCode.InternalServerError;
    }

    private async Task<TimeSpan> DelayAfterFailureAsync(HttpResponseMessage response, TimeSpan currentDelay, CancellationToken cancellationToken) {
        if (response.Headers.RetryAfter is { } retryAfter) {
            if (retryAfter.Date.HasValue) {
                var wait = retryAfter.Date.Value - DateTimeOffset.UtcNow;
                if (wait > TimeSpan.Zero) {
                    await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                    return currentDelay;
                }
            }
            else if (retryAfter.Delta.HasValue) {
                await Task.Delay(retryAfter.Delta.Value, cancellationToken).ConfigureAwait(false);
                return currentDelay;
            }
        }

        await Task.Delay(currentDelay, cancellationToken).ConfigureAwait(false);
        return NextDelay(currentDelay);
    }

    private static void ConfigureDefaultHeaders(HttpRequestHeaders headers, string userAgent, string accept) {
        headers.Accept.Clear();
        headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
        headers.UserAgent.ParseAdd(userAgent);
    }

    private static string BuildTaxonQuery(long cursor, int limit) {
        return $@"PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?item ?qid (SUM(?flag141) AS ?hasP141) (SUM(?flag627) AS ?hasP627)
WHERE {{
  {{
    ?item wdt:P141 ?status .
    BIND(1 AS ?flag141)
    BIND(0 AS ?flag627)
  }}
  UNION
  {{
    ?item wdt:P627 ?taxonId .
    BIND(0 AS ?flag141)
    BIND(1 AS ?flag627)
  }}
  ?item wdt:P31 wd:Q16521 .
    BIND(xsd:integer(STRAFTER(STR(?item), ""http://www.wikidata.org/entity/Q"")) AS ?qid)
  FILTER(?qid > {cursor})
}}
GROUP BY ?item ?qid
ORDER BY ?qid
LIMIT {limit}";
    }

    private static IReadOnlyList<WikidataSeedRow> ParseSeedResponse(string json) {
        var list = new List<WikidataSeedRow>();
        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("results", out var results)) {
            return list;
        }

        if (!results.TryGetProperty("bindings", out var bindings)) {
            return list;
        }

        foreach (var binding in bindings.EnumerateArray()) {
            if (!TryReadBinding(binding, "qid", out long numericId)) {
                continue;
            }

            var entityId = "Q" + numericId;
            var hasP141 = TryReadBinding(binding, "hasP141", out long p141) && p141 > 0;
            var hasP627 = TryReadBinding(binding, "hasP627", out long p627) && p627 > 0;
            list.Add(new WikidataSeedRow(numericId, entityId, hasP141, hasP627));
        }

        return list;
    }

    private static bool TryReadBinding(JsonElement binding, string name, out long value) {
        value = 0;
        if (!binding.TryGetProperty(name, out var element)) {
            return false;
        }

        if (!element.TryGetProperty("value", out var raw)) {
            return false;
        }

        var text = raw.GetString();
        return long.TryParse(text, out value);
    }

    private static TimeSpan NextDelay(TimeSpan current) {
        var doubled = TimeSpan.FromMilliseconds(current.TotalMilliseconds * 2);
        var cap = TimeSpan.FromSeconds(30);
        return doubled <= cap ? doubled : cap;
    }

    private static async Task<DateTime> EnforceRateLimitAsync(DateTime nextAllowed, TimeSpan minDelay, CancellationToken cancellationToken) {
        var now = DateTime.UtcNow;
        if (nextAllowed > now) {
            var wait = nextAllowed - now;
            if (wait > TimeSpan.Zero) {
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
            }
        }

        nextAllowed = DateTime.UtcNow + minDelay;
        return nextAllowed;
    }

    public void Dispose() {
        _apiSemaphore.Dispose();
        _sparqlSemaphore.Dispose();
        _apiClient.Dispose();
        _sparqlClient.Dispose();
    }
}

internal sealed record WikidataApiResponse(string Url, string Body, HttpStatusCode StatusCode, long PayloadBytes);

internal sealed record WikidataSeedRow(long NumericId, string EntityId, bool HasP141, bool HasP627);

internal sealed class WikidataApiException : Exception {
    public WikidataApiException(string url, HttpStatusCode? statusCode, string responseBody, int attempt, Exception? inner = null)
        : base($"Wikidata request to {url} failed with status {(int?)statusCode ?? 0} on attempt {attempt}" + (string.IsNullOrWhiteSpace(responseBody) ? string.Empty : $" Body: {responseBody}"), inner) {
        Url = url;
        StatusCode = statusCode;
        ResponseBody = responseBody;
        Attempt = attempt;
    }

    public string Url { get; }
    public HttpStatusCode? StatusCode { get; }
    public string ResponseBody { get; }
    public int Attempt { get; }
}
