using System;
using System.Collections.Generic;
using System.IO;

namespace BeastieBot3;

/// <summary>
/// Lightweight .env loader so api keys can be provided without editing source.
/// Values in the real environment take precedence over .env entries.
/// </summary>
internal static class EnvFileLoader {
    private static bool _loaded;

    public static void LoadIfPresent(string? explicitPath = null) {
        if (_loaded) {
            return;
        }

        var candidates = new List<string>();
        if (!string.IsNullOrWhiteSpace(explicitPath)) {
            candidates.Add(explicitPath);
        }

        var currentDirectory = Directory.GetCurrentDirectory();
        candidates.Add(Path.Combine(currentDirectory, ".env"));

        var baseDirectory = AppContext.BaseDirectory;
        if (!string.Equals(currentDirectory, baseDirectory, StringComparison.OrdinalIgnoreCase)) {
            candidates.Add(Path.Combine(baseDirectory, ".env"));
        }

        foreach (var path in candidates) {
            if (!File.Exists(path)) {
                continue;
            }

            foreach (var rawLine in File.ReadAllLines(path)) {
                var line = rawLine.Trim();
                if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal)) {
                    continue;
                }

                var separatorIndex = line.IndexOf('=');
                if (separatorIndex <= 0) {
                    continue;
                }

                var key = line[..separatorIndex].Trim();
                var value = line[(separatorIndex + 1)..].Trim();
                if (key.Length == 0 || Environment.GetEnvironmentVariable(key) is { Length: > 0 }) {
                    continue;
                }

                Environment.SetEnvironmentVariable(key, value);
            }

            break; // only load the first matching .env
        }

        _loaded = true;
    }
}
