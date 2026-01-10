using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class IucnSynonymService : IDisposable {
    private readonly SqliteConnection? _iucnApiConnection;
    private readonly SqliteConnection? _colConnection;
    private readonly ColTaxonRepository? _colRepository;
    private readonly Dictionary<long, IReadOnlyList<string>> _iucnSynonymCache = new();

    public IucnSynonymService(string? iucnApiCachePath, string? colDatabasePath) {
        if (!string.IsNullOrWhiteSpace(iucnApiCachePath) && File.Exists(iucnApiCachePath)) {
            var builder = new SqliteConnectionStringBuilder {
                DataSource = iucnApiCachePath,
                Mode = SqliteOpenMode.ReadOnly
            };

            _iucnApiConnection = new SqliteConnection(builder.ConnectionString);
            _iucnApiConnection.Open();
        }

        if (!string.IsNullOrWhiteSpace(colDatabasePath) && File.Exists(colDatabasePath)) {
            var builder = new SqliteConnectionStringBuilder {
                DataSource = colDatabasePath,
                Mode = SqliteOpenMode.ReadOnly
            };

            _colConnection = new SqliteConnection(builder.ConnectionString);
            _colConnection.Open();
            _colRepository = new ColTaxonRepository(_colConnection);
        }
    }

    public bool HasIucnApiCache => _iucnApiConnection is not null;
    public bool HasColDatabase => _colRepository is not null;

    public IReadOnlyList<TaxonNameCandidate> GetCandidates(IucnTaxonomyRow row, CancellationToken cancellationToken) {
        if (row is null) {
            throw new ArgumentNullException(nameof(row));
        }

        var results = new List<TaxonNameCandidate>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void AddCandidate(string? value, TaxonNameSource source) {
            if (string.IsNullOrWhiteSpace(value)) {
                return;
            }

            var trimmed = value.Trim();
            if (trimmed.Length == 0 || !seen.Add(trimmed)) {
                return;
            }

            results.Add(new TaxonNameCandidate(trimmed, source));
        }

        AddCandidate(row.ScientificNameTaxonomy, TaxonNameSource.IucnTaxonomy);
        AddCandidate(row.ScientificNameAssessments, TaxonNameSource.IucnAssessments);
        AddCandidate(ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName), TaxonNameSource.IucnConstructed);

        foreach (var rank in ScientificNameHelper.BuildInfraRankTokens(row.InfraType)) {
            AddCandidate(ScientificNameHelper.BuildWithRankLabel(row.GenusName, row.SpeciesName, rank, row.InfraName), TaxonNameSource.IucnInfraRanked);
        }

        var sisId = row.TaxonId;
        foreach (var synonym in GetIucnApiSynonyms(sisId, cancellationToken)) {
            AddCandidate(synonym, TaxonNameSource.IucnSynonym);
        }

        if (_colRepository is not null) {
            foreach (var synonym in GetColSynonyms(row, cancellationToken)) {
                AddCandidate(synonym, TaxonNameSource.ColSynonym);
            }
        }

        return results;
    }

    public void Dispose() {
        _iucnApiConnection?.Dispose();
        _colConnection?.Dispose();
    }

    private IReadOnlyList<string> GetIucnApiSynonyms(long sisId, CancellationToken cancellationToken) {
        if (_iucnApiConnection is null) {
            return Array.Empty<string>();
        }

        if (_iucnSynonymCache.TryGetValue(sisId, out var cached)) {
            return cached;
        }

        using var command = _iucnApiConnection.CreateCommand();
        command.CommandText = "SELECT json FROM taxa WHERE root_sis_id=@id LIMIT 1";
        command.Parameters.AddWithValue("@id", sisId);
        cancellationToken.ThrowIfCancellationRequested();
        var json = command.ExecuteScalar() as string;
        var names = ParseIucnSynonyms(json);
        _iucnSynonymCache[sisId] = names;
        return names;
    }

    private static IReadOnlyList<string> ParseIucnSynonyms(string? json) {
        if (string.IsNullOrWhiteSpace(json)) {
            return Array.Empty<string>();
        }

        try {
            using var document = JsonDocument.Parse(json);
            if (!document.RootElement.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                return Array.Empty<string>();
            }

            if (!taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                return Array.Empty<string>();
            }

            var list = new List<string>();
            foreach (var item in synonyms.EnumerateArray()) {
                if (!item.TryGetProperty("name", out var nameElement)) {
                    continue;
                }

                var name = nameElement.GetString();
                if (!string.IsNullOrWhiteSpace(name)) {
                    list.Add(name.Trim());
                }
            }

            return list.Count == 0 ? Array.Empty<string>() : list;
        }
        catch (JsonException) {
            return Array.Empty<string>();
        }
    }

    private IReadOnlyList<string> GetColSynonyms(IucnTaxonomyRow row, CancellationToken cancellationToken) {
        if (_colRepository is null) {
            return Array.Empty<string>();
        }

        var matches = _colRepository.FindByComponents(row.GenusName, row.SpeciesName, row.InfraName, cancellationToken);
        if (matches.Count == 0) {
            return Array.Empty<string>();
        }

        var builder = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(string? value) {
            if (string.IsNullOrWhiteSpace(value)) {
                return;
            }

            builder.Add(value.Trim());
        }

        foreach (var match in matches) {
            if (!LooksSynonym(match.Status)) {
                continue;
            }

            Add(match.ScientificName);
            Add(ScientificNameHelper.BuildFromParts(match.Genus, match.SpecificEpithet, match.InfraspecificEpithet));
            Add(ScientificNameHelper.BuildWithSubgenus(match.Genus, match.Subgenus, match.SpecificEpithet, match.InfraspecificEpithet));
        }

        return builder.Count == 0 ? Array.Empty<string>() : builder.ToList();
    }

    private static bool LooksSynonym(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return false;
        }

        var normalized = status.Trim().ToLowerInvariant();
        return normalized.Contains("synonym", StringComparison.Ordinal);
    }

}

internal sealed record TaxonNameCandidate(string Name, TaxonNameSource Source) {
    public bool IsSynonym => Source is TaxonNameSource.IucnSynonym or TaxonNameSource.ColSynonym;
    public bool IsAlternateMatch => IsSynonym || Source is TaxonNameSource.IucnInfraRanked;
};

internal enum TaxonNameSource {
    IucnTaxonomy,
    IucnAssessments,
    IucnConstructed,
    IucnInfraRanked,
    IucnSynonym,
    ColSynonym
}
