using System;

namespace BeastieBot3;

internal sealed record IucnApiConfiguration(
    Uri BaseUri,
    string Token,
    TimeSpan Timeout,
    int MaxConcurrency,
    TimeSpan InitialDelay,
    TimeSpan MaxDelay
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

        return new IucnApiConfiguration(
            baseUri,
            token.Trim(),
            TimeSpan.FromSeconds(timeoutSeconds),
            concurrency,
            initialDelay,
            maxDelay
        );
    }

    private static int TryParseInt(string key, int fallback) {
        var raw = Environment.GetEnvironmentVariable(key);
        return int.TryParse(raw, out var value) && value > 0 ? value : fallback;
    }
}
