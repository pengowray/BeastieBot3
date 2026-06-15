using System;

// Configuration record for IUCN API client. Environment variables:
// - IUCN_API_TOKEN (required): Bearer token for apiv4.iucnredlist.org
// - IUCN_API_BASE_URL (default: https://api.iucnredlist.org)
// - IUCN_API_TIMEOUT_SECONDS (default: 120)
// - IUCN_API_MAX_CONCURRENCY (default: 1)
// - IUCN_API_RETRY_INITIAL_SECONDS (default: 2) / IUCN_API_RETRY_MAX_SECONDS (default: 60) — 5xx/timeout backoff
// - IUCN_API_RATELIMIT_SECONDS (default: 60): how long to wait on a 429 with no Retry-After header
// - IUCN_API_RATELIMIT_MAX_RETRIES (default: 10): how many times to wait-and-retry a rate-limited (429) request
// Calls EnvFileLoader.LoadIfPresent() to support .env files.

namespace BeastieBot3.Configuration;

internal sealed record IucnApiConfiguration(
    Uri BaseUri,
    string Token,
    TimeSpan Timeout,
    int MaxConcurrency,
    TimeSpan InitialDelay,
    TimeSpan MaxDelay,
    TimeSpan RateLimitWait,
    int MaxRateLimitRetries
) {
    public static IucnApiConfiguration FromEnvironment() {
        EnvFileLoader.LoadIfPresent();

        var baseUrl = Environment.GetEnvironmentVariable("IUCN_API_BASE_URL") ?? "https://api.iucnredlist.org";
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var baseUri)) {
            throw new InvalidOperationException($"IUCN_API_BASE_URL '{baseUrl}' is invalid.");
        }

        var token = Environment.GetEnvironmentVariable("IUCN_API_TOKEN");
        if (string.IsNullOrWhiteSpace(token)) {
            throw new InvalidOperationException("IUCN_API_TOKEN environment variable is required to call the IUCN API.");
        }

        var timeoutSeconds = TryParseInt("IUCN_API_TIMEOUT_SECONDS", 120);
        var concurrency = Math.Clamp(TryParseInt("IUCN_API_MAX_CONCURRENCY", 1), 1, 4);
        var initialDelay = TimeSpan.FromSeconds(Math.Clamp(TryParseInt("IUCN_API_RETRY_INITIAL_SECONDS", 2), 1, 30));
        var maxDelay = TimeSpan.FromSeconds(Math.Clamp(TryParseInt("IUCN_API_RETRY_MAX_SECONDS", 60), 5, 300));
        var rateLimitWait = TimeSpan.FromSeconds(Math.Clamp(TryParseInt("IUCN_API_RATELIMIT_SECONDS", 60), 5, 600));
        var maxRateLimitRetries = Math.Clamp(TryParseInt("IUCN_API_RATELIMIT_MAX_RETRIES", 10), 1, 100);

        return new IucnApiConfiguration(
            baseUri,
            token.Trim(),
            TimeSpan.FromSeconds(timeoutSeconds),
            concurrency,
            initialDelay,
            maxDelay,
            rateLimitWait,
            maxRateLimitRetries
        );
    }

    private static int TryParseInt(string key, int fallback) {
        var raw = Environment.GetEnvironmentVariable(key);
        return int.TryParse(raw, out var value) && value > 0 ? value : fallback;
    }
}
