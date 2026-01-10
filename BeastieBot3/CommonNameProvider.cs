using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class CommonNameProvider : IDisposable {
    private readonly SqliteConnection? _wikidataConnection;
    private readonly SqliteConnection? _iucnConnection;
    private readonly Dictionary<string, IReadOnlyList<string>> _wikidataCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<long, string?> _iucnCache = new();

    public CommonNameProvider(string? wikidataPath, string? iucnApiPath) {
        _wikidataConnection = OpenReadOnlyConnection(wikidataPath);
        _iucnConnection = OpenReadOnlyConnection(iucnApiPath);
    }

    public string? GetBestCommonName(IucnTaxonomyRow row, IReadOnlyList<string>? entityIds, IEnumerable<string?>? extraScientificNames = null) {
        if (row is null) {
            throw new ArgumentNullException(nameof(row));
        }

        var disallowed = BuildScientificNameSet(row, extraScientificNames);
        var wikidata = GetWikidataCommonName(entityIds, disallowed);
        if (!string.IsNullOrWhiteSpace(wikidata)) {
            return wikidata;
        }

        var iucn = GetIucnCommonName(row.TaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture), disallowed);
        return string.IsNullOrWhiteSpace(iucn) ? null : iucn;
    }

    public void Dispose() {
        _wikidataConnection?.Dispose();
        _iucnConnection?.Dispose();
    }

    private static SqliteConnection? OpenReadOnlyConnection(string? path) {
        if (string.IsNullOrWhiteSpace(path)) {
            return null;
        }

        var fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath)) {
            return null;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    private string? GetWikidataCommonName(IReadOnlyList<string>? entityIds, HashSet<string> disallowed) {
        if (_wikidataConnection is null || entityIds is null) {
            return null;
        }

        foreach (var entityId in entityIds) {
            if (string.IsNullOrWhiteSpace(entityId)) {
                continue;
            }

            foreach (var candidate in GetWikidataNames(entityId.Trim())) {
                if (IsAcceptableCommonName(candidate, disallowed)) {
                    return candidate;
                }
            }
        }

        return null;
    }

    private IReadOnlyList<string> GetWikidataNames(string entityId) {
        if (_wikidataCache.TryGetValue(entityId, out var cached)) {
            return cached;
        }

        if (_wikidataConnection is null) {
            return Array.Empty<string>();
        }

        using var command = _wikidataConnection.CreateCommand();
        command.CommandText = "SELECT json FROM wikidata_entities WHERE entity_id=@id LIMIT 1";
        command.Parameters.AddWithValue("@id", entityId);
        var json = command.ExecuteScalar() as string;
        if (string.IsNullOrWhiteSpace(json)) {
            _wikidataCache[entityId] = Array.Empty<string>();
            return Array.Empty<string>();
        }

        try {
            var record = WikidataEntityParser.Parse(json);
            var englishNames = record.CommonNames
                .Where(name => name.Language.StartsWith("en", StringComparison.OrdinalIgnoreCase))
                .Select(name => name.Value.Trim())
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            IReadOnlyList<string> result = englishNames.Count == 0 ? Array.Empty<string>() : englishNames;
            _wikidataCache[entityId] = result;
            return result;
        }
        catch (JsonException) {
            _wikidataCache[entityId] = Array.Empty<string>();
            return Array.Empty<string>();
        }
        catch (InvalidOperationException) {
            _wikidataCache[entityId] = Array.Empty<string>();
            return Array.Empty<string>();
        }
    }

    private string? GetIucnCommonName(string? sisIdText, HashSet<string> disallowed) {
        if (_iucnConnection is null || string.IsNullOrWhiteSpace(sisIdText) || !long.TryParse(sisIdText, out var sisId)) {
            return null;
        }

        if (_iucnCache.TryGetValue(sisId, out var cached)) {
            return IsAcceptableCommonName(cached, disallowed) ? cached : null;
        }

        using var command = _iucnConnection.CreateCommand();
        command.CommandText = @"SELECT t.json
FROM taxa_lookup l
JOIN taxa t ON t.id = l.taxa_id
WHERE l.sis_id=@sis
ORDER BY CASE WHEN l.scope='species' THEN 0 ELSE 1 END
LIMIT 1";
        command.Parameters.AddWithValue("@sis", sisId);
        var json = command.ExecuteScalar() as string;
        var preferred = ExtractIucnPreferredName(json);
        _iucnCache[sisId] = preferred;
        return IsAcceptableCommonName(preferred, disallowed) ? preferred : null;
    }

    private static string? ExtractIucnPreferredName(string? json) {
        if (string.IsNullOrWhiteSpace(json)) {
            return null;
        }

        try {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            return ReadPreferredCommonName(root)
                ?? (root.TryGetProperty("taxon", out var taxon) ? ReadPreferredCommonName(taxon) ?? ReadCommonNamesArray(taxon) : null)
                ?? ReadCommonNamesArray(root);
        }
        catch (JsonException) {
            return null;
        }
    }

    private static string? ReadPreferredCommonName(JsonElement element) {
        if (element.ValueKind != JsonValueKind.Object) {
            return null;
        }

        if (element.TryGetProperty("preferred_common_name", out var preferred) && preferred.ValueKind == JsonValueKind.String) {
            var value = preferred.GetString();
            if (!string.IsNullOrWhiteSpace(value)) {
                return value.Trim();
            }
        }

        return null;
    }

    private static string? ReadCommonNamesArray(JsonElement element) {
        if (!element.TryGetProperty("common_names", out var array) || array.ValueKind != JsonValueKind.Array) {
            return null;
        }

        foreach (var entry in array.EnumerateArray()) {
            if (entry.ValueKind != JsonValueKind.Object) {
                continue;
            }

            var language = entry.TryGetProperty("language", out var langElement) ? langElement.GetString() : null;
            if (!string.IsNullOrWhiteSpace(language) && !language.StartsWith("en", StringComparison.OrdinalIgnoreCase)) {
                continue;
            }

            var name = entry.TryGetProperty("name", out var nameElement) ? nameElement.GetString() : null;
            if (!string.IsNullOrWhiteSpace(name)) {
                return name.Trim();
            }
        }

        return null;
    }

    private static HashSet<string> BuildScientificNameSet(IucnTaxonomyRow row, IEnumerable<string?>? extras) {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(string? value) {
            if (!string.IsNullOrWhiteSpace(value)) {
                set.Add(value.Trim());
            }
        }

        Add(row.ScientificNameTaxonomy);
        Add(row.ScientificNameAssessments);
        Add(ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName));

        if (extras is not null) {
            foreach (var extra in extras) {
                Add(extra);
            }
        }

        return set;
    }

    private static bool IsAcceptableCommonName(string? candidate, HashSet<string> disallowed) {
        if (string.IsNullOrWhiteSpace(candidate)) {
            return false;
        }

        var trimmed = candidate.Trim();
        return trimmed.Length > 0 && !disallowed.Contains(trimmed);
    }
}
