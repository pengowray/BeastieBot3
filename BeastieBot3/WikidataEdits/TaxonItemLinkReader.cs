using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.Data.Sqlite;

// Every way the Wikidata cache ties an IUCN taxon id to an item, read-only:
//   - P627 claims, from wikidata_p627_values (source = 'claim'). On 2026-09-13 those rows matched
//     the P627 values in the entity JSON for all 187,911 downloaded items, and reading them takes
//     well under a second where parsing the JSON takes over a minute. The table does not record
//     rank, so the 7 deprecated P627 statements then in the cache come through as ordinary
//     P627Claim links; WdTaxonItem.IucnTaxonIds has the ranks when that matters.
//   - backfill-iucn's pending matches (wikidata_pending_iucn_matches), one row per taxon at most.
//
// Nothing is resolved here. A taxon can come back linked to several items, or to the same item by
// both a claim and a pending match; the link classifier decides what that means.

namespace BeastieBot3.WikidataEdits;

/// A P627 claim value or pending-match taxon id that is not a plain SIS number
/// (e.g. "40028/22064188", a taxon id with an assessment id appended). Source is "claim" or "pending".
internal sealed record SkippedLinkValue(string Qid, string Value, string Source);

internal sealed record TaxonItemLinkSet(IReadOnlyList<TaxonItemLink> Links, IReadOnlyList<SkippedLinkValue> Skipped) {
    public IReadOnlyDictionary<LinkSource, int> CountBySource() {
        var counts = new Dictionary<LinkSource, int>();
        foreach (var source in Enum.GetValues<LinkSource>()) {
            counts[source] = 0;
        }

        foreach (var link in Links) {
            counts[link.Source]++;
        }

        return counts;
    }
}

internal sealed class TaxonItemLinkReader : IDisposable {
    private readonly SqliteConnection _connection;

    private TaxonItemLinkReader(SqliteConnection connection) {
        _connection = connection;
    }

    public static TaxonItemLinkReader Open(string databasePath) => new(WdTaxonItemReader.OpenReadOnly(databasePath));

    public TaxonItemLinkSet ReadAll() {
        var links = new List<TaxonItemLink>();
        var skipped = new List<SkippedLinkValue>();
        ReadClaims(links, skipped);
        ReadPendingMatches(links, skipped);
        return new TaxonItemLinkSet(links, skipped);
    }

    private void ReadClaims(List<TaxonItemLink> links, List<SkippedLinkValue> skipped) {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT entity_numeric_id, value
            FROM wikidata_p627_values
            WHERE source = 'claim'
            ORDER BY entity_numeric_id, value
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var qid = "Q" + reader.GetInt64(0).ToString(CultureInfo.InvariantCulture);
            var value = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            if (TryParseTaxonId(value, out var taxonId)) {
                links.Add(new TaxonItemLink(taxonId, qid, LinkSource.P627Claim, null));
            }
            else {
                skipped.Add(new SkippedLinkValue(qid, value, "claim"));
            }
        }
    }

    private void ReadPendingMatches(List<TaxonItemLink> links, List<SkippedLinkValue> skipped) {
        using var command = _connection.CreateCommand();
        command.CommandText = """
            SELECT iucn_taxon_id, entity_numeric_id, entity_id, matched_name, match_method, is_synonym
            FROM wikidata_pending_iucn_matches
            ORDER BY entity_numeric_id, iucn_taxon_id
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var value = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var qid = reader.IsDBNull(2) || string.IsNullOrWhiteSpace(reader.GetString(2))
                ? "Q" + reader.GetInt64(1).ToString(CultureInfo.InvariantCulture)
                : reader.GetString(2).Trim();
            var method = reader.IsDBNull(4) ? null : reader.GetString(4);
            var source = SourceForPendingMatch(method, !reader.IsDBNull(5) && reader.GetInt64(5) != 0);
            if (source is null) {
                skipped.Add(new SkippedLinkValue(qid, $"{value} (match_method '{method}')", "pending"));
                continue;
            }

            if (!TryParseTaxonId(value, out var taxonId)) {
                skipped.Add(new SkippedLinkValue(qid, value, "pending"));
                continue;
            }

            var matchedName = reader.IsDBNull(3) ? null : reader.GetString(3);
            links.Add(new TaxonItemLink(taxonId, qid, source.Value, matchedName));
        }
    }

    /// backfill-iucn's match_method + is_synonym as a LinkSource; null for a method it doesn't write.
    /// A synonym flag only splits TaxonName: CachedName and Label keep their source either way.
    internal static LinkSource? SourceForPendingMatch(string? matchMethod, bool isSynonym) {
        switch (matchMethod?.Trim().ToUpperInvariant()) {
            case "TAXONNAME":
                return isSynonym ? LinkSource.SearchTaxonNameSynonym : LinkSource.SearchTaxonName;
            case "CACHEDNAME":
                return LinkSource.CachedName;
            case "LABEL":
                return LinkSource.Label;
            default:
                return null;
        }
    }

    /// A plain positive SIS number: digits only, no sign, no whitespace inside.
    internal static bool TryParseTaxonId(string? value, out long taxonId) {
        taxonId = 0;
        var span = value.AsSpan().Trim();
        return span.Length > 0
            && long.TryParse(span, NumberStyles.None, CultureInfo.InvariantCulture, out taxonId)
            && taxonId > 0;
    }

    public void Dispose() => _connection.Dispose();
}
