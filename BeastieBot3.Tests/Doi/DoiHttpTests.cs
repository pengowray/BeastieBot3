using BeastieBot3.Iucn.Doi;

namespace BeastieBot3.Tests.Doi;

// Pins the doi.org handle API reading and the paced getter's retry rules, over a stub handler
// (no network).
public class DoiHttpTests {
    private const string PolarBear = "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en";

    [Fact]
    public void Interpret_Exists_WithItsUrl() {
        var body = """{"responseCode":1,"handle":"10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en","values":[{"index":1,"type":"URL","data":{"format":"string","value":"https://www.iucnredlist.org/species/22823/14871490"}}]}""";
        var result = DoiHandleClient.Interpret(PolarBear, 200, body);
        Assert.Equal(DoiHandleStatus.Exists, result.Status);
        Assert.Equal(1, result.ResponseCode);
        Assert.Equal("https://www.iucnredlist.org/species/22823/14871490", result.Url);
    }

    [Fact]
    public void Interpret_NotFound() {
        var result = DoiHandleClient.Interpret(PolarBear, 404, """{"responseCode":100,"handle":"x"}""");
        Assert.Equal(DoiHandleStatus.NotFound, result.Status);
        Assert.Null(result.Url);
    }

    [Fact]
    public void Interpret_HandleWithNoUrlValue_Exists() {
        var result = DoiHandleClient.Interpret(PolarBear, 200, """{"responseCode":200,"handle":"x","values":[]}""");
        Assert.Equal(DoiHandleStatus.Exists, result.Status);
        Assert.Null(result.Url);
    }

    [Theory]
    [InlineData(404, "<html>Not Found</html>")]
    [InlineData(404, "")]
    [InlineData(200, """{"responseCode":2,"message":"Error"}""")]
    [InlineData(400, """{"responseCode":100}""")]
    [InlineData(200, """{"responseCode":100}""")]
    public void Interpret_AnythingElse_IsUnexpected_UnlessItIsTheNotFoundCode(int status, string body) {
        var result = DoiHandleClient.Interpret(PolarBear, status, body);
        var expected = status == 200 && body.Contains("100") ? DoiHandleStatus.NotFound : DoiHandleStatus.Unexpected;
        Assert.Equal(expected, result.Status);
    }

    [Fact]
    public async Task Lookup_AsksTheHandleApiForTheUrlValue_WithTheProjectUserAgent() {
        var handler = new StubHandler((_, _) => StubHandler.Exists(PolarBear, "https://www.iucnredlist.org/species/22823/14871490"));
        using var http = new HttpClient(handler);
        http.DefaultRequestHeaders.UserAgent.ParseAdd(IucnResolveDoisCommand.UserAgent);
        var client = new DoiHandleClient(new PoliteHttpGetter(http, TimeSpan.Zero));

        var result = await client.LookupAsync(PolarBear, CancellationToken.None);

        Assert.Equal(DoiHandleStatus.Exists, result.Status);
        Assert.Equal("https://doi.org/api/handles/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en?type=URL", handler.Urls.Single());
        Assert.Contains("BeastieBot3/1.0", handler.UserAgents.Single());
        Assert.DoesNotContain("@", handler.UserAgents.Single());
    }

    [Fact]
    public async Task Getter_429_WaitsForRetryAfter_SlowsDown_AndTriesAgain() {
        var handler = new StubHandler((_, call) => call == 1
            ? StubHandler.Json(429, "{}", TimeSpan.FromSeconds(7))
            : StubHandler.Missing(PolarBear));
        using var http = new HttpClient(handler);
        var delays = new RecordedDelays();
        var getter = new PoliteHttpGetter(http, TimeSpan.FromMilliseconds(300), delays.Delay);

        var result = await getter.GetAsync("https://doi.org/api/handles/x", CancellationToken.None);

        Assert.Equal(404, result.Status);
        Assert.Equal(2, handler.Urls.Count);
        Assert.Contains(TimeSpan.FromSeconds(7), delays.Waits);
        Assert.Equal(TimeSpan.FromMilliseconds(600), getter.MinInterval);
        Assert.Equal(1, getter.RateLimited);
        Assert.Equal(2, getter.Requests);
    }

    [Fact]
    public async Task Getter_429WithoutRetryAfter_WaitsTheRateLimitWait() {
        var handler = new StubHandler((_, call) => call == 1 ? StubHandler.Json(429, "{}") : StubHandler.Json(200, "{}"));
        using var http = new HttpClient(handler);
        var delays = new RecordedDelays();
        var getter = new PoliteHttpGetter(http, TimeSpan.Zero, delays.Delay) { RateLimitWait = TimeSpan.FromSeconds(42) };

        await getter.GetAsync("https://doi.org/x", CancellationToken.None);

        Assert.Contains(TimeSpan.FromSeconds(42), delays.Waits);
        Assert.Equal(TimeSpan.FromMilliseconds(250), getter.MinInterval);
    }

    [Fact]
    public async Task Getter_ServerErrors_BackOffAndTryAgain() {
        var handler = new StubHandler((_, call) => call <= 2 ? StubHandler.Json(503, "") : StubHandler.Json(200, "ok"));
        using var http = new HttpClient(handler);
        var delays = new RecordedDelays();
        var getter = new PoliteHttpGetter(http, TimeSpan.Zero, delays.Delay);

        var result = await getter.GetAsync("https://doi.org/x", CancellationToken.None);

        Assert.Equal(200, result.Status);
        Assert.Equal(new[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) }, delays.Waits);
        Assert.Equal(2, getter.Retried);
    }

    [Fact]
    public async Task Getter_GivesUpAfterMaxAttempts() {
        var handler = new StubHandler((_, _) => StubHandler.Json(502, ""));
        using var http = new HttpClient(handler);
        var getter = new PoliteHttpGetter(http, TimeSpan.Zero, new RecordedDelays().Delay) { MaxAttempts = 3 };

        await Assert.ThrowsAsync<PoliteHttpException>(() => getter.GetAsync("https://doi.org/x", CancellationToken.None));
        Assert.Equal(3, handler.Urls.Count);
    }

    [Fact]
    public async Task Getter_KeepsRequestsApart() {
        var handler = new StubHandler((_, _) => StubHandler.Json(200, "{}"));
        using var http = new HttpClient(handler);
        var delays = new RecordedDelays();
        var getter = new PoliteHttpGetter(http, TimeSpan.FromSeconds(1), delays.Delay);

        await getter.GetAsync("https://doi.org/a", CancellationToken.None);
        await getter.GetAsync("https://doi.org/b", CancellationToken.None);

        // The first request goes at once; the second waits for most of the interval.
        var wait = Assert.Single(delays.Waits);
        Assert.InRange(wait, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1));
    }
}
