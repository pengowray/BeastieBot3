using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Taxonomy;

// Per-taxon Catalogue of Life name resolution shared by the Wikipedia list pipeline: given an IUCN
// name it answers two questions the Red List lists care about:
//
//   AcceptedName      - the IUCN name is a CoL synonym of a different accepted name (resolved via
//                       parentID, since this ColDP schema has no acceptedNameUsageID). Wikipedia
//                       articles live at the accepted name, so this is the candidate that avoids a
//                       redlink on the IUCN name.
//   CorrectedSpelling - the IUCN name has no exact CoL match but a formatting-equivalent near match
//                       (a diacritic, Unicode-encoding, punctuation, or spacing slip). This is a
//                       likely data slip in the IUCN string that can be shown/linked cleanly.
//
// It reuses ColTaxonRepository and ScientificNameDifference (the same building blocks as the audit
// crosscheck) and caches per taxon. Lookups are kingdom-filtered so a plant is never matched to an
// animal homonym. IUCN is the anchor for these lists, so a genuine (non-formatting) spelling
// difference is deliberately NOT offered as a correction.

namespace BeastieBot3.Col;

internal readonly record struct ColNameResolution(
    string? AcceptedName, string? AcceptedColId,
    string? CorrectedSpelling, string? CorrectedColId) {
    public static ColNameResolution None => default;
    public bool HasAcceptedName => !string.IsNullOrWhiteSpace(AcceptedName);
    public bool HasCorrectedSpelling => !string.IsNullOrWhiteSpace(CorrectedSpelling);
}

internal sealed class ColNameResolver : IDisposable {
    private readonly ColTaxonRepository _repo;
    private readonly SqliteConnection? _owned;
    private readonly Dictionary<string, ColNameResolution> _cache = new(StringComparer.Ordinal);

    public ColNameResolver(string colDatabasePath) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = colDatabasePath, Mode = SqliteOpenMode.ReadOnly
        }.ConnectionString);
        connection.Open();
        _owned = connection;
        _repo = new ColTaxonRepository(connection);
    }

    public ColNameResolver(ColTaxonRepository repository) {
        _repo = repository ?? throw new ArgumentNullException(nameof(repository));
    }

    public ColNameResolution Resolve(string? genus, string? species, string? infraName, string? scientificName, string? kingdom, CancellationToken cancellationToken) {
        var name = Decode(!string.IsNullOrWhiteSpace(scientificName)
            ? scientificName
            : ScientificNameHelper.BuildFromParts(genus, species, infraName));
        if (string.IsNullOrWhiteSpace(name)) {
            return ColNameResolution.None;
        }

        var key = $"{(kingdom ?? string.Empty).Trim().ToLowerInvariant()}|{name.Trim().ToLowerInvariant()}";
        if (_cache.TryGetValue(key, out var cached)) {
            return cached;
        }
        var result = Compute(genus, species, infraName, name.Trim(), kingdom, cancellationToken);
        _cache[key] = result;
        return result;
    }

    private ColNameResolution Compute(string? genus, string? species, string? infraName, string name, string? kingdom, CancellationToken ct) {
        var candidates = new List<ColTaxonRecord>();
        candidates.AddRange(_repo.FindByScientificName(name, ct));
        if (candidates.Count == 0 && !string.IsNullOrWhiteSpace(genus) && !string.IsNullOrWhiteSpace(species)) {
            candidates.AddRange(_repo.FindByComponents(genus!, species!, infraName, ct));
        }

        var unique = candidates
            .GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First())
            .Where(c => KingdomMatches(c, kingdom))
            .ToList();

        var primary = ChoosePrimary(unique);
        if (primary is not null) {
            // An exact match: the only correction on offer is the accepted name when CoL treats the
            // IUCN name as a synonym of something else.
            if (IsSynonymStatus(primary.Status) && !string.IsNullOrWhiteSpace(primary.ParentId)) {
                var accepted = _repo.GetById(primary.ParentId, ct);
                var acceptedName = Decode(accepted?.ScientificName);
                if (!string.IsNullOrWhiteSpace(acceptedName) && !NamesEqual(acceptedName!, name)) {
                    return new ColNameResolution(acceptedName, accepted!.Id, null, null);
                }
            }
            return ColNameResolution.None;
        }

        // No exact match: offer a formatting-equivalent near match (a likely data slip) only.
        var near = NearMatch(genus, species, name, kingdom, ct);
        if (near is not null) {
            return new ColNameResolution(null, null, Decode(near.ScientificName), near.Id);
        }
        return ColNameResolution.None;
    }

    private ColTaxonRecord? NearMatch(string? genus, string? species, string name, string? kingdom, CancellationToken ct) {
        var pool = new List<ColTaxonRecord>();
        if (!string.IsNullOrWhiteSpace(genus)) {
            pool.AddRange(_repo.FindByGenericName(genus!, ct));
        }
        if (!string.IsNullOrWhiteSpace(species)) {
            pool.AddRange(_repo.FindBySpecificEpithet(species!, ct));
        }

        ColTaxonRecord? best = null;
        var bestDistance = int.MaxValue;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in pool.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First())) {
            if (!KingdomMatches(candidate, kingdom)) {
                continue;
            }
            var colName = Decode(candidate.ScientificName);
            if (string.IsNullOrWhiteSpace(colName) || !seen.Add(colName!)) {
                continue;
            }
            var diff = ScientificNameDifference.Classify(name, colName!);
            // Only a formatting-equivalent difference is treated as the same name and worth offering;
            // a genuine spelling variant could be a different taxon, so it is left to IUCN.
            if (!diff.IsFormattingEquivalent) {
                continue;
            }
            if (diff.Distance < bestDistance) {
                bestDistance = diff.Distance;
                best = candidate;
            }
        }
        return best;
    }

    private static ColTaxonRecord? ChoosePrimary(IReadOnlyList<ColTaxonRecord> candidates) {
        if (candidates.Count == 0) {
            return null;
        }
        return candidates.FirstOrDefault(c => IsAcceptedStatus(c.Status))
            ?? candidates.FirstOrDefault(c => IsSynonymStatus(c.Status))
            ?? candidates[0];
    }

    private static bool IsAcceptedStatus(string? status) =>
        string.IsNullOrWhiteSpace(status) || status.Trim().ToLowerInvariant().Contains("accepted", StringComparison.Ordinal);

    private static bool IsSynonymStatus(string? status) =>
        !string.IsNullOrWhiteSpace(status) && status.Trim().ToLowerInvariant().Contains("synonym", StringComparison.Ordinal);

    private static bool KingdomMatches(ColTaxonRecord record, string? kingdom) {
        if (string.IsNullOrWhiteSpace(record.Kingdom) || string.IsNullOrWhiteSpace(kingdom)) {
            return true; // cannot verify; do not exclude
        }
        return record.Kingdom.Trim().Equals(kingdom.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private static bool NamesEqual(string a, string b) =>
        string.Equals(a.Trim(), b.Trim(), StringComparison.OrdinalIgnoreCase);

    private static string? Decode(string? value) => string.IsNullOrEmpty(value) ? value : WebUtility.HtmlDecode(value);

    public void Dispose() => _owned?.Dispose();
}
