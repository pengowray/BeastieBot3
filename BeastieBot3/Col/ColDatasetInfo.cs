using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace BeastieBot3.Col;

// The handful of identifying fields from a ColDP archive's metadata.yaml
// (key / title / alias / issued / version). Single source of truth for reading
// them: ColImporter uses Parse() when importing, and the web col-version endpoint
// uses ReadFromZip() to learn which release sits in the input folder — so both
// interpret the metadata the same way and can't drift.
internal sealed record ColDatasetInfo(string? Key, string? Title, string? Alias, string? Issued, string? Version) {
    // Human-facing release label, preferring the short alias (e.g. "COL26.5 XR").
    public string? DisplayLabel =>
        !string.IsNullOrWhiteSpace(Alias) ? Alias :
        !string.IsNullOrWhiteSpace(Version) ? Version :
        !string.IsNullOrWhiteSpace(Title) ? Title : null;

    public static ColDatasetInfo Parse(string yamlText) {
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        var map = deserializer.Deserialize<Dictionary<string, object?>>(yamlText) ?? new Dictionary<string, object?>();
        return new ColDatasetInfo(
            Key: ExtractString(map, "key"),
            Title: ExtractString(map, "title"),
            Alias: ExtractString(map, "alias"),
            Issued: ExtractString(map, "issued"),
            Version: ExtractString(map, "version"));
    }

    // Reads metadata.yaml out of a ColDP zip without inflating the whole archive
    // (the zip central directory + one small entry). Returns null on any failure
    // (not a zip, no metadata.yaml, unreadable) — callers treat that as "unknown".
    public static ColDatasetInfo? ReadFromZip(string zipPath) {
        try {
            using var archive = ZipFile.OpenRead(zipPath);
            var entry = archive.Entries.FirstOrDefault(e => e.FullName.Equals("metadata.yaml", StringComparison.OrdinalIgnoreCase));
            if (entry is null) return null;
            using var stream = entry.Open();
            using var reader = new StreamReader(stream, Encoding.UTF8, true);
            return Parse(reader.ReadToEnd());
        } catch {
            return null;
        }
    }

    private static string? ExtractString(IDictionary<string, object?> map, string key) {
        if (!map.TryGetValue(key, out var value) || value is null) {
            return null;
        }

        return value switch {
            string s => s,
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString()
        };
    }
}
