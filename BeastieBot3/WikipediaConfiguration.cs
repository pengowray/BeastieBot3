using System;

namespace BeastieBot3;

internal sealed record WikipediaConfiguration(
    Uri ActionEndpoint,
    Uri RestEndpoint,
    string UserAgent,
    TimeSpan Timeout,
    TimeSpan ActionDelay,
    TimeSpan RestDelay
) {
    public static WikipediaConfiguration FromEnvironment() {
        EnvFileLoader.LoadIfPresent();

        var actionEndpoint = ReadUri("WIKIPEDIA_ACTION_ENDPOINT", "https://en.wikipedia.org/w/api.php");
        var restEndpoint = ReadUri("WIKIPEDIA_REST_ENDPOINT", "https://en.wikipedia.org/api/rest_v1/");
        var userAgent = Environment.GetEnvironmentVariable("WIKIPEDIA_USER_AGENT")?.Trim();
        if (string.IsNullOrWhiteSpace(userAgent)) {
            userAgent = "BeastieBot3/1.0 (+https://github.com/pengowray/BeastieBot3)";
        }

        var timeoutSeconds = ReadInt("WIKIPEDIA_TIMEOUT_SECONDS", 120, 5, 600);
        var actionDelayMs = ReadInt("WIKIPEDIA_ACTION_DELAY_MS", 500, 0, 10_000);
        var restDelayMs = ReadInt("WIKIPEDIA_REST_DELAY_MS", 500, 0, 10_000);

        return new WikipediaConfiguration(
            actionEndpoint,
            restEndpoint,
            userAgent,
            TimeSpan.FromSeconds(timeoutSeconds),
            TimeSpan.FromMilliseconds(actionDelayMs),
            TimeSpan.FromMilliseconds(restDelayMs)
        );
    }

    private static Uri ReadUri(string key, string fallback) {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw)) {
            raw = fallback;
        }

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
