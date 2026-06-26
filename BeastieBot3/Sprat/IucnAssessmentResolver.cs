using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Sprat;

// Resolves the IUCN Red List internalTaxonId / assessmentId / yearPublished (and the PE/PEW flags) for a
// SPRAT taxon, by joining its scientific name to the IUCN release database (IUCN_<edition>.sqlite). SPRAT
// carries no IUCN assessment id, so without this the Australia lists can only emit the bare
// {{IUCN status|CODE}} badge; with it they emit the full {{IUCN status|CODE|taxonId/assessmentId|1|year=}}
// form (a referenced link to iucnredlist.org), identical to the non-Australia IUCN lists.
//
// Matching cascade (verified ~99.7% coverage of SPRAT's IUCN-status taxa):
//   1. exact taxonomy_html.scientificName = SPRAT scientific_name
//   2. fallback to SPRAT's IUCN_Red_List_Listed_Names column (the IUCN-side accepted name)
// Both use the canonical Global-scope species assessment (scopes LIKE '%Global%'); the BINARY exact match
// stays sargable on idx_taxonomy_html_scientificName. Unresolved taxa fall back to the bare badge.

internal sealed record IucnAssessmentRef(
    long TaxonId, long AssessmentId, string? YearPublished, string? RedlistCategory,
    string? PossiblyExtinct, string? PossiblyExtinctInTheWild);

internal sealed class IucnAssessmentResolver : IDisposable {
    private readonly SqliteConnection _connection;
    private readonly bool _ownsConnection;
    private readonly Dictionary<string, IucnAssessmentRef?> _cache = new(StringComparer.Ordinal);

    public IucnAssessmentResolver(string iucnDatabasePath) {
        if (string.IsNullOrWhiteSpace(iucnDatabasePath) || !File.Exists(iucnDatabasePath)) {
            throw new FileNotFoundException($"IUCN database not found: {iucnDatabasePath}", iucnDatabasePath);
        }
        var builder = new SqliteConnectionStringBuilder {
            DataSource = Path.GetFullPath(iucnDatabasePath), Mode = SqliteOpenMode.ReadOnly,
        };
        _connection = new SqliteConnection(builder.ToString());
        _connection.Open();
        _ownsConnection = true;
    }

    /// <summary>Test seam: resolve over a caller-owned connection (e.g. a shared <c>:memory:</c> DB).</summary>
    internal IucnAssessmentResolver(SqliteConnection connection) {
        _connection = connection;
        _ownsConnection = false;
    }

    /// <summary>
    /// The Global-scope species assessment for a SPRAT taxon, by its scientific name then its IUCN listed
    /// name, or null when neither resolves (the caller then emits the bare badge). Memoized.
    /// </summary>
    public IucnAssessmentRef? Resolve(string? scientificName, string? listedName) {
        var key = (scientificName ?? string.Empty) + "" + (listedName ?? string.Empty);
        if (_cache.TryGetValue(key, out var cached)) {
            return cached;
        }
        var result = Lookup(scientificName);
        if (result is null && !string.IsNullOrWhiteSpace(listedName)) {
            foreach (var candidate in listedName.Split(',')) {
                result = Lookup(candidate);
                if (result is not null) {
                    break;
                }
            }
        }
        _cache[key] = result;
        return result;
    }

    private IucnAssessmentRef? Lookup(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            SELECT t.taxonId, a.assessmentId, a.yearPublished, a.redlistCategory,
                   a.possiblyExtinct, a.possiblyExtinctInTheWild
            FROM taxonomy_html t
            JOIN assessments_html a ON a.taxonId = t.taxonId
            WHERE t.scientificName = @name AND a.scopes LIKE '%Global%'
            ORDER BY CAST(a.yearPublished AS INTEGER) DESC
            LIMIT 1;";
        cmd.Parameters.AddWithValue("@name", name.Trim());
        using var reader = cmd.ExecuteReader();
        if (!reader.Read()) {
            return null;
        }
        return new IucnAssessmentRef(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5));
    }

    public void Dispose() {
        if (_ownsConnection) {
            _connection.Dispose();
        }
    }
}
