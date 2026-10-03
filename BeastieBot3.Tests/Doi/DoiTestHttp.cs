using System.Net;
using BeastieBot3.Iucn.Doi;

namespace BeastieBot3.Tests.Doi;

// A scripted HttpMessageHandler for the DOI tests: answers each request from a function of its URL,
// records every URL asked for, and never touches the network.
internal sealed class StubHandler : HttpMessageHandler {
    private readonly Func<HttpRequestMessage, int, HttpResponseMessage> _answer;

    public StubHandler(Func<HttpRequestMessage, int, HttpResponseMessage> answer) {
        _answer = answer;
    }

    public List<string> Urls { get; } = new();
    public List<string> UserAgents { get; } = new();

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) {
        Urls.Add(request.RequestUri!.AbsoluteUri);
        UserAgents.Add(request.Headers.UserAgent.ToString());
        return Task.FromResult(_answer(request, Urls.Count));
    }

    public static HttpResponseMessage Json(int status, string body, TimeSpan? retryAfter = null) {
        var response = new HttpResponseMessage((HttpStatusCode)status) {
            Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json"),
        };
        if (retryAfter is { } wait) {
            response.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(wait);
        }
        return response;
    }

    // doi.org handle API answers.
    public static HttpResponseMessage Exists(string doi, string url) =>
        Json(200, $$"""{"responseCode":1,"handle":"{{doi}}","values":[{"index":1,"type":"URL","data":{"format":"string","value":"{{url}}"},"ttl":86400,"timestamp":"2025-02-28T07:41:15Z"}]}""");

    public static HttpResponseMessage Missing(string doi) =>
        Json(404, $$"""{"responseCode":100,"handle":"{{doi}}"}""");
}

/// Records waits instead of sleeping.
internal sealed class RecordedDelays {
    public List<TimeSpan> Waits { get; } = new();

    public Task Delay(TimeSpan wait, CancellationToken cancellationToken) {
        Waits.Add(wait);
        return Task.CompletedTask;
    }
}

/// A doi.org lookup from a dictionary of DOIs to page URLs; any other DOI is not found.
internal sealed class FakeLookup : IDoiHandleLookup {
    private readonly Dictionary<string, string?> _existing;
    private readonly HashSet<string> _unexpected;

    public FakeLookup(Dictionary<string, string?> existing, IEnumerable<string>? unexpected = null) {
        _existing = new Dictionary<string, string?>(existing, StringComparer.OrdinalIgnoreCase);
        _unexpected = new HashSet<string>(unexpected ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
    }

    public List<string> Asked { get; } = new();

    public Task<DoiHandleResult> LookupAsync(string doi, CancellationToken cancellationToken) {
        Asked.Add(doi);
        if (_unexpected.Contains(doi)) {
            return Task.FromResult(new DoiHandleResult(doi, DoiHandleStatus.Unexpected, 200, 2, null));
        }
        return Task.FromResult(_existing.TryGetValue(doi, out var url)
            ? new DoiHandleResult(doi, DoiHandleStatus.Exists, 200, 1, url)
            : new DoiHandleResult(doi, DoiHandleStatus.NotFound, 404, 100, null));
    }
}
