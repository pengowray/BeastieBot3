using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Col;
using BeastieBot3.Taxonomy;

// Aggregates synonyms from two sources: IUCN API cache (taxa.json synonym array)
// and COL database (via ColTaxonRepository). Caches results in memory by sis_id.
// Used by WikipediaListGenerator to include synonym redirects in species lists.
// Opens both Datastore:IUCN_api_cache_sqlite and Datastore:COL_sqlite.

namespace BeastieBot3.Iucn;

internal sealed class IucnSynonymService : IDisposable {
    private readonly SqliteConnection? _iucnApiConnection;
    private readonly SqliteConnection? _colConnection;
    private readonly ColTaxonRepository? _colRepository;
    private readonly ColNameResolver? _colNameResolver;
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
            _colNameResolver = new ColNameResolver(_colRepository);
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

        // Candidates are looked up by name: as a Wikipedia article title, and as a Wikidata taxon
        // name. Neither carries the authority IUCN stores with a synonym ("Eumeces schneideri
        // (Daudin, 1802) [orth. error]"), so the stored form is offered as the bare name instead.
        // Names that are already bare, higher taxa included, come through unchanged.
        void AddCandidate(string? value, TaxonNameSource source) {
            if (string.IsNullOrWhiteSpace(value)) {
                return;
            }

            var trimmed = value.Trim();
            var bare = BareScientificName.Strip(trimmed);
            if (bare.Length == 0) {
                return;
            }

            // Stripping down to the genus alone would offer the genus article for a species, so a
            // name that loses everything below the genus is dropped rather than guessed at.
            if (!string.Equals(bare, trimmed, StringComparison.Ordinal)
                && bare.IndexOf(' ') < 0 && trimmed.IndexOf(' ') >= 0) {
                return;
            }

            if (!seen.Add(bare)) {
                return;
            }

            results.Add(new TaxonNameCandidate(bare, source));
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

        // The article overwhelmingly lives at the CoL ACCEPTED name (when the IUCN name is a CoL
        // synonym), at the clean spelling (when the IUCN name is a formatting slip), or at the way
        // CoL writes the same name (gender agreement after a genus transfer). Offer all three; the
        // matcher validates each against the enwiki cache, so a wrong guess simply fails to match
        // rather than corrupting anything.
        if (_colNameResolver is not null) {
            var primaryName = !string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy)
                ? row.ScientificNameTaxonomy
                : row.ScientificNameAssessments;
            var resolution = _colNameResolver.Resolve(row.GenusName, row.SpeciesName, row.InfraName, primaryName, row.KingdomName, cancellationToken);
            AddCandidate(resolution.AcceptedName, TaxonNameSource.ColAccepted);
            AddCandidate(resolution.CorrectedSpelling, TaxonNameSource.ColCorrected);
            AddCandidate(resolution.VariantName, TaxonNameSource.ColVariant);

            // Second hop, and only for the taxa that need it. The lookup above starts from the IUCN
            // name, so it finds nothing when the two catalogues disagree about the genus outright:
            // IUCN's name is absent from CoL and so are its near spellings. Looking up the taxon's
            // OTHER IUCN names reaches CoL in those cases, and CoL's accepted name for one of them
            // is where the article lives. Gated on NameIsUnknownToCol rather than on "no accepted
            // name": the overwhelming majority of taxa are accepted in CoL under the IUCN name and
            // also return no accepted name, and running the hop for those would add several CoL
            // queries per taxon across the whole Red List for nothing.
            if (resolution.NameIsUnknownToCol) {
                foreach (var synonym in GetIucnApiSynonyms(sisId, cancellationToken)) {
                    var bare = BareScientificName.Strip(synonym.Trim());
                    if (bare.Length == 0 || bare.IndexOf(' ') < 0) {
                        continue;
                    }
                    var viaSynonym = _colNameResolver.Resolve(null, null, null, bare, row.KingdomName, cancellationToken);
                    if (viaSynonym.HasAcceptedName) {
                        AddCandidate(viaSynonym.AcceptedName, TaxonNameSource.ColAcceptedViaSynonym);
                        break;
                    }
                }
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
    // "Not the name IUCN publishes", which is what the matcher records as synonym_used. Every CoL
    // and IUCN-synonym source qualifies; only the four IucnTaxonomy/Assessments/Constructed/
    // InfraRanked renderings of the assessed name itself do not.
    public bool IsSynonym => Source is TaxonNameSource.IucnSynonym or TaxonNameSource.ColSynonym
        or TaxonNameSource.ColAccepted or TaxonNameSource.ColCorrected
        or TaxonNameSource.ColVariant or TaxonNameSource.ColAcceptedViaSynonym;
    public bool IsAlternateMatch => IsSynonym || Source is TaxonNameSource.IucnInfraRanked;
};

internal enum TaxonNameSource {
    IucnTaxonomy,
    IucnAssessments,
    IucnConstructed,
    IucnInfraRanked,
    IucnSynonym,
    ColSynonym,
    // The CoL accepted name (the IUCN name is a CoL synonym of it) and the clean CoL spelling (the
    // IUCN name is a formatting-equivalent slip). Both are resolved by ColNameResolver.
    ColAccepted,
    ColCorrected,
    // The same name as CoL writes it, differing only by Latin gender agreement or a patronym
    // ending, and the CoL accepted name reached through one of the taxon's other IUCN names.
    ColVariant,
    ColAcceptedViaSynonym
}
