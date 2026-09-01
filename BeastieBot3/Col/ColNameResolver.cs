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
//   VariantName       - the IUCN name has no exact CoL match, but CoL writes the same name a
//                       different legitimate way: gender agreement after a genus transfer
//                       (Schistura striatus / striata) or a patronym with one -i or two. See
//                       LatinNameVariant for why this is not the same thing as a close spelling.
//
// It reuses ColTaxonRepository and ScientificNameDifference (the same building blocks as the audit
// crosscheck) and caches per taxon. Lookups are kingdom-filtered so a plant is never matched to an
// animal homonym. IUCN is the anchor for these lists, so a genuine (non-formatting) spelling
// difference is deliberately NOT offered as a correction.

namespace BeastieBot3.Col;

internal readonly record struct ColNameResolution(
    string? AcceptedName, string? AcceptedColId,
    string? CorrectedSpelling, string? CorrectedColId,
    string? VariantName = null, string? VariantColId = null,
    bool NameIsInCol = false) {
    public static ColNameResolution None => default;
    public bool HasAcceptedName => !string.IsNullOrWhiteSpace(AcceptedName);
    public bool HasCorrectedSpelling => !string.IsNullOrWhiteSpace(CorrectedSpelling);
    public bool HasVariantName => !string.IsNullOrWhiteSpace(VariantName);

    /// True when no CoL usage carries the IUCN name and nothing resembling it was found either.
    /// Distinguishes "CoL agrees, nothing to offer" from "CoL has never heard of this name", which
    /// otherwise both read as an empty resolution.
    public bool NameIsUnknownToCol => !NameIsInCol && !HasCorrectedSpelling && !HasVariantName;
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

    // The IUCN name is in CoL and there is nothing better to offer. Not the same as None, which
    // means CoL has no record of the name at all.
    private static ColNameResolution Found => new(null, null, null, null, NameIsInCol: true);

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

        if (unique.Count > 0) {
            // The IUCN name is accepted in CoL for this kingdom: nothing to correct.
            if (unique.Any(c => IsAcceptedStatus(c.Status))) {
                return Found;
            }
            // Otherwise CoL treats the name as a synonym. Return the accepted name only when the
            // accepted taxon is in the same kingdom. The kingdom guard is applied to the accepted
            // target rather than the synonym, because CoL synonym rows carry a blank kingdom (so the
            // filter above never excludes them). Every synonym is tried so the result does not depend
            // on row order when a name has synonyms pointing to more than one kingdom.
            foreach (var synonym in unique.Where(c => IsSynonymStatus(c.Status))) {
                if (string.IsNullOrWhiteSpace(synonym.ParentId)) {
                    continue;
                }
                var accepted = _repo.GetById(synonym.ParentId, ct);
                if (accepted is null || !KingdomMatches(accepted, kingdom)) {
                    continue;
                }
                var acceptedName = Decode(accepted.ScientificName);
                if (!string.IsNullOrWhiteSpace(acceptedName) && !NamesEqual(acceptedName!, name)) {
                    return new ColNameResolution(acceptedName, accepted.Id, null, null, NameIsInCol: true);
                }
            }
            return Found;
        }

        // No exact match. Two kinds of CoL name are still the same name and worth offering: a
        // formatting-equivalent spelling (a data slip), and a Latin variant of it (gender agreement
        // or a patronym ending). Either may itself be a CoL synonym, so the accepted name it points
        // at is resolved too and returned alongside; a caller looking for an article wants both.
        var pool = CandidatePool(genus, species, kingdom, ct);

        var near = NearMatch(pool, name);
        if (near is not null) {
            var (acceptedName, acceptedId) = AcceptedBehind(near, kingdom, ct);
            return new ColNameResolution(acceptedName, acceptedId, Decode(near.ScientificName), near.Id);
        }

        var variant = VariantMatch(pool, name);
        if (variant is not null) {
            var (acceptedName, acceptedId) = AcceptedBehind(variant, kingdom, ct);
            return new ColNameResolution(acceptedName, acceptedId, null, null, Decode(variant.ScientificName), variant.Id);
        }
        return ColNameResolution.None;
    }

    // The CoL accepted name a synonym record points at, or nothing when the record is itself
    // accepted (there is no better name to offer) or points outside the kingdom.
    private (string? Name, string? Id) AcceptedBehind(ColTaxonRecord record, string? kingdom, CancellationToken ct) {
        if (!IsSynonymStatus(record.Status) || string.IsNullOrWhiteSpace(record.ParentId)) {
            return (null, null);
        }
        var accepted = _repo.GetById(record.ParentId!, ct);
        if (accepted is null || !KingdomMatches(accepted, kingdom)) {
            return (null, null);
        }
        var name = Decode(accepted.ScientificName);
        return string.IsNullOrWhiteSpace(name) ? (null, null) : (name, accepted.Id);
    }

    // Every CoL usage sharing the genus or the species epithet, deduplicated by id and by name and
    // kingdom-filtered. Read once, because both passes below scan the same set.
    private List<(ColTaxonRecord Record, string Name)> CandidatePool(string? genus, string? species, string? kingdom, CancellationToken ct) {
        var pool = new List<ColTaxonRecord>();
        if (!string.IsNullOrWhiteSpace(genus)) {
            pool.AddRange(_repo.FindByGenericName(genus!, ct));
        }
        if (!string.IsNullOrWhiteSpace(species)) {
            pool.AddRange(_repo.FindBySpecificEpithet(species!, ct));
        }

        var result = new List<(ColTaxonRecord, string)>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in pool.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First())) {
            if (!KingdomMatches(candidate, kingdom)) {
                continue;
            }
            var colName = Decode(candidate.ScientificName);
            if (string.IsNullOrWhiteSpace(colName) || !seen.Add(colName!)) {
                continue;
            }
            result.Add((candidate, colName!));
        }
        return result;
    }

    private static ColTaxonRecord? NearMatch(List<(ColTaxonRecord Record, string Name)> pool, string name) {
        ColTaxonRecord? best = null;
        var bestDistance = int.MaxValue;
        foreach (var (candidate, colName) in pool) {
            var diff = ScientificNameDifference.Classify(name, colName);
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

    // An accepted CoL name beats a CoL synonym when both spell the name the same way, so the caller
    // is handed the name an article is most likely to be filed under.
    private static ColTaxonRecord? VariantMatch(List<(ColTaxonRecord Record, string Name)> pool, string name) {
        ColTaxonRecord? best = null;
        foreach (var (candidate, colName) in pool) {
            if (!LatinNameVariant.SameName(name, colName)) {
                continue;
            }
            if (best is null || (!IsAcceptedStatus(best.Status) && IsAcceptedStatus(candidate.Status))) {
                best = candidate;
            }
        }
        return best;
    }

    // OrdinalIgnoreCase rather than lowercasing first: called once per CoL candidate per taxon, and
    // neither trimming nor case changes whether the word is present.
    private static bool IsAcceptedStatus(string? status) =>
        string.IsNullOrWhiteSpace(status) || status.Contains("accepted", StringComparison.OrdinalIgnoreCase);

    private static bool IsSynonymStatus(string? status) =>
        !string.IsNullOrWhiteSpace(status) && status.Contains("synonym", StringComparison.OrdinalIgnoreCase);

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
