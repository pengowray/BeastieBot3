using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace BeastieBot3 {
    // Reads path values from an INI file (default: "paths.ini" next to the app).
    // All key/value pairs across all sections are returned as flattened keys like "Section:Key".
    public sealed class IniPathReader {
        private readonly IConfigurationRoot _config;

        public string BaseDirectory { get; }
        public string SourceFilePath { get; }

        public IniPathReader(string? fileName = null, string? baseDirectory = null) {
            BaseDirectory = baseDirectory ?? AppContext.BaseDirectory;
            var iniFile = fileName ?? "paths.ini";

            var builder = new ConfigurationBuilder()
            .SetBasePath(BaseDirectory)
            .AddIniFile(iniFile, optional: true, reloadOnChange: true);

            _config = builder.Build();
            SourceFilePath = Path.Combine(BaseDirectory, iniFile);
        }

        // Get a single normalized path by key (keys are case-insensitive, and can be "Section:Key").
        public string? Get(string key) {
            var raw = _config[key];
            if (string.IsNullOrWhiteSpace(raw)) return null;
            return NormalizePath(raw);
        }

        // Read and normalize all values from the INI file as paths. Keys are flattened (e.g., "Section:Key").
        public IReadOnlyDictionary<string, string> GetAll() {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in _config.AsEnumerable()) {
                if (string.IsNullOrWhiteSpace(kvp.Key)) continue; // skip root
                if (string.IsNullOrWhiteSpace(kvp.Value)) continue; // skip empty

                var normalized = TryNormalize(kvp.Value!, out var path)
                ? path!
                : kvp.Value!; // leave as-is if we can't normalize

                result[kvp.Key] = normalized;
            }
            return result;
        }

        // Optional helper to get all keys within a section as a dictionary of normalized paths.
        public IReadOnlyDictionary<string, string> GetSection(string sectionName) {
            var section = _config.GetSection(sectionName);
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var child in section.AsEnumerable()) {
                if (string.IsNullOrWhiteSpace(child.Key) || string.IsNullOrWhiteSpace(child.Value)) continue;
                // child.Key is full path like "Section:Sub:Key"; preserve the key as-is
                var normalized = TryNormalize(child.Value!, out var path)
                ? path!
                : child.Value!;
                dict[child.Key] = normalized;
            }
            return dict;
        }

        private bool TryNormalize(string value, out string? normalized) {
            try {
                normalized = NormalizePath(value);
                return true;
            } catch {
                normalized = null;
                return false;
            }
        }

        private string NormalizePath(string value) {
            var v = value.Trim();
            v = Environment.ExpandEnvironmentVariables(v);

            // Expand leading tilde to user profile if present
            if (v.Length > 0 && v[0] == '~') {
                var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                if (v.Length == 1) {
                    v = home;
                } else if (v[1] == '/' || v[1] == '\\') {
                    v = Path.Combine(home, v.Substring(2));
                }
            }

            // Make absolute
            if (!Path.IsPathFullyQualified(v)) {
                v = Path.GetFullPath(Path.Combine(BaseDirectory, v));
            }

            return v;
        }
    }
}
