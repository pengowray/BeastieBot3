using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;

// Where a Catalogue of Life preferred (accepted) name is already recorded as a synonym on the IUCN
// side. IUCN carries synonyms only in the API cache (taxon.synonyms[]), so this scans that cache
// once and maps each bare synonym name to the accepted IUCN taxa (root_sis_id) that list it. The
// synonym report uses it to surface the sharpest disagreement: IUCN accepts name X with Y among its
// synonyms while CoL accepts Y with X as a synonym, i.e. the two catalogues are reversed.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal enum IucnSynonymMatch {
    Unknown,     // no index available (API cache absent), so the question was not answered
    None,        // the name is not an IUCN synonym
    OtherTaxon,  // an IUCN synonym, but of a different taxon
    SameTaxon,   // an IUCN synonym of this same taxon (direction reversed relative to CoL)
}

internal sealed class IucnSynonymIndex {
    private readonly Dictionary<string, HashSet<long>> _byName;
    private readonly Dictionary<long, List<string>> _byTaxon;

    public int SynonymNameCount => _byName.Count;

    private IucnSynonymIndex(Dictionary<string, HashSet<long>> byName, Dictionary<long, List<string>> byTaxon) {
        _byName = byName;
        _byTaxon = byTaxon;
    }

    // The taxon's own synonyms, as IUCN records them. Read the other way round from Lookup: used to
    // ask whether a taxon whose accepted name is missing from CoL is nevertheless in CoL under one of
    // the names IUCN files as a synonym.
    public IReadOnlyList<string> SynonymsOf(long taxonId) =>
        _byTaxon.TryGetValue(taxonId, out var names) ? names : Array.Empty<string>();

    // Does the CoL accepted name appear among IUCN's synonyms, and if so is it a synonym of this same
    // IUCN taxon (the reversed-direction case) or of a different one?
    public IucnSynonymMatch Lookup(string? colAcceptedName, long iucnTaxonId) {
        var key = Normalize(colAcceptedName);
        if (key.Length == 0 || !_byName.TryGetValue(key, out var taxa)) {
            return IucnSynonymMatch.None;
        }
        return taxa.Contains(iucnTaxonId) ? IucnSynonymMatch.SameTaxon : IucnSynonymMatch.OtherTaxon;
    }

    // Test seam: build directly from (synonym name, accepted taxon id) pairs.
    public static IucnSynonymIndex FromEntries(IEnumerable<(string Name, long TaxonId)> entries) {
        var map = new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        var byTaxon = new Dictionary<long, List<string>>();
        foreach (var (name, taxonId) in entries) {
            Add(map, byTaxon, name, taxonId);
        }
        return new IucnSynonymIndex(map, byTaxon);
    }

    public static IucnSynonymIndex Build(SqliteConnection apiCache, int? limit, CancellationToken ct) {
        var map = new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        var byTaxon = new Dictionary<long, List<string>>();
        const string sql = "SELECT root_sis_id, json FROM taxa";
        using var command = apiCache.CreateCommand();
        command.CommandText = limit is > 0 ? sql + " LIMIT " + limit.Value : sql;
        command.CommandTimeout = 0;

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            ct.ThrowIfCancellationRequested();
            var rootSisId = reader.GetInt64(0);
            if (reader.IsDBNull(1)) {
                continue;
            }
            JsonDocument document;
            try { document = JsonDocument.Parse(reader.GetString(1)); } catch (JsonException) { continue; }
            using (document) {
                var root = document.RootElement;
                if (!root.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                    continue;
                }
                if (!taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                    continue;
                }
                foreach (var synonym in synonyms.EnumerateArray()) {
                    var bare = BareName(synonym);
                    if (bare is not null) {
                        Add(map, byTaxon, bare, rootSisId);
                    }
                }
            }
        }
        return new IucnSynonymIndex(map, byTaxon);
    }

    private static void Add(Dictionary<string, HashSet<long>> map, Dictionary<long, List<string>> byTaxon,
        string name, long taxonId) {
        var key = Normalize(name);
        if (key.Length == 0) {
            return;
        }
        if (!map.TryGetValue(key, out var set)) {
            set = new HashSet<long>();
            map[key] = set;
        }
        set.Add(taxonId);

        // The reverse map keeps the name as written, not the normalised key, because it is looked up
        // in CoL and shown to the reader.
        if (!byTaxon.TryGetValue(taxonId, out var names)) {
            names = new List<string>();
            byTaxon[taxonId] = names;
        }
        var trimmed = name.Trim();
        if (!names.Contains(trimmed, StringComparer.OrdinalIgnoreCase)) {
            names.Add(trimmed);
        }
    }

    // Reconstruct the bare binomial/trinomial from the structured fields (which exclude authorship),
    // so it lines up with CoL's author-free scientificName.
    private static string? BareName(JsonElement synonym) {
        if (synonym.ValueKind != JsonValueKind.Object) {
            return null;
        }
        var genus = Str(synonym, "genus_name");
        var species = Str(synonym, "species_name");
        if (genus is null || species is null) {
            return null;
        }
        var infra = Str(synonym, "infra_name");
        return infra is null ? $"{genus} {species}" : $"{genus} {species} {infra}";
    }

    private static string? Str(JsonElement element, string name) =>
        element.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(prop.GetString())
            ? prop.GetString()!.Trim()
            : null;

    private static string Normalize(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return string.Empty;
        }
        var sb = new StringBuilder(value.Length);
        var previousSpace = false;
        foreach (var ch in value.Trim()) {
            if (char.IsWhiteSpace(ch)) {
                if (!previousSpace) {
                    sb.Append(' ');
                    previousSpace = true;
                }
            } else {
                sb.Append(char.ToLowerInvariant(ch));
                previousSpace = false;
            }
        }
        return sb.ToString();
    }
}
