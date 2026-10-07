using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Lists;
using Microsoft.Data.Sqlite;

// The queries for the pages of species from the Catalogue of Life and Wikidata that IUCN does not
// have (/col/{id}, /wikidata/{qid}) and for finding them in search.

namespace BeastieBot3.Site.Data;

/// One pair of extra_overlap seen from one side: the other side is an IUCN taxon or another extra
/// species. Likely: the reason makes the pair likely the same species, not only possibly.
public sealed record ExtraPairRow(string Reason, bool Likely, TaxonSummary? Taxon, ExtraSpeciesRow? Extra);

/// An extra species found by search. MatchedName: the name that matched when it is not shown with
/// the result (Wikidata's spelling of the scientific name); null otherwise.
public sealed record ExtraSearchHit(ExtraSpeciesRow Species, string? MatchedName, bool IsExactMatch);

public sealed record ExtraSearchResult(IReadOnlyList<ExtraSearchHit> Hits, long Total) {
    public static readonly ExtraSearchResult Empty = new([], 0);
}

public sealed partial class SiteQueries {
    // Search reads at most this many FTS matches and ranks them in C#. Every extra species has a
    // two-word scientific name, so a search for one finds few rows and an exact match is never cut.
    private const int ExtraSearchCandidates = 500;

    public ExtraSpeciesRow? GetExtraSpeciesByColId(string colId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {ExtraColumns} FROM {ExtraFrom} WHERE e.col_id = @id";
        command.Parameters.AddWithValue("@id", colId);
        return ReadExtras(command).FirstOrDefault();
    }

    /// item: the Wikidata item's number (33609 for Q33609).
    public ExtraSpeciesRow? GetExtraSpeciesByWikidataItem(long item) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {ExtraColumns} FROM {ExtraFrom} WHERE e.wikidata_qid = @item";
        command.Parameters.AddWithValue("@item", item);
        return ReadExtras(command).FirstOrDefault();
    }

    /// The IUCN taxa whose Catalogue of Life id is colId, taxa in the release first.
    public IReadOnlyList<TaxonSummary> GetTaxaWithColId(string colId) {
        using var connection = _db.OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {SummaryColumns} FROM taxon t {SummaryJoin} WHERE t.col_id = @id ORDER BY t.in_release DESC, t.taxon_id";
        command.Parameters.AddWithValue("@id", colId);
        using var reader = command.ExecuteReader();
        var taxa = new List<TaxonSummary>();
        while (reader.Read()) {
            taxa.Add(SummaryAt(reader, 0));
        }
        return taxa;
    }

    /// The IUCN taxa and other extra species that an extra species may be the same as, likely ones first.
    public IReadOnlyList<ExtraPairRow> GetExtraOverlaps(int extraId) {
        var pairs = new List<(string Reason, bool Likely, long? TaxonId, int? ExtraId)>();
        using (var connection = _db.OpenConnection())
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT reason, likely, taxon_id, other_extra_id FROM extra_overlap WHERE extra_id = @id
                UNION ALL
                SELECT reason, likely, NULL, extra_id FROM extra_overlap WHERE other_extra_id = @id
                """;
            command.Parameters.AddWithValue("@id", extraId);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                pairs.Add((reader.GetString(0), reader.GetInt64(1) != 0,
                    reader.IsDBNull(2) ? null : reader.GetInt64(2), reader.IsDBNull(3) ? null : reader.GetInt32(3)));
            }
        }
        var extras = GetExtraSpeciesByIds(pairs.Where(p => p.ExtraId is not null).Select(p => p.ExtraId!.Value).Distinct().ToList());
        var rows = new List<ExtraPairRow>();
        foreach (var (reason, likely, taxonId, otherId) in pairs) {
            if (taxonId is { } tid && GetSummary(tid) is { } taxon) {
                rows.Add(new ExtraPairRow(reason, likely, taxon, null));
            } else if (otherId is { } oid && extras.TryGetValue(oid, out var extra)) {
                rows.Add(new ExtraPairRow(reason, likely, null, extra));
            }
        }
        return [.. rows.OrderByDescending(r => r.Likely)];
    }

    /// The extra species that may be the same as an IUCN taxon, likely ones first.
    public IReadOnlyList<ExtraPairRow> GetExtraOverlapsOfTaxon(long taxonId) {
        var pairs = new List<(int ExtraId, string Reason, bool Likely)>();
        using (var connection = _db.OpenConnection())
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT extra_id, reason, likely FROM extra_overlap WHERE taxon_id = @id";
            command.Parameters.AddWithValue("@id", taxonId);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                pairs.Add((reader.GetInt32(0), reader.GetString(1), reader.GetInt64(2) != 0));
            }
        }
        var extras = GetExtraSpeciesByIds(pairs.Select(p => p.ExtraId).Distinct().ToList());
        return [.. pairs
            .Where(p => extras.ContainsKey(p.ExtraId))
            .Select(p => new ExtraPairRow(p.Reason, p.Likely, null, extras[p.ExtraId]))
            .OrderByDescending(r => r.Likely)
            .ThenBy(r => r.Extra!.ScientificName, StringComparer.Ordinal)];
    }

    /// Extra species whose scientific name, Wikidata name or English name matches the text: exact
    /// matches first, then names that start with the text, then the rest, shorter names first.
    public ExtraSearchResult SearchExtra(string text, int limit, CancellationToken cancellationToken = default) {
        var key = SiteNameKey.Fold(text);
        if (key.Length == 0 || FtsQuery.Build(text) is not { } match) {
            return ExtraSearchResult.Empty;
        }
        cancellationToken.ThrowIfCancellationRequested();
        var ids = new List<int>();
        long total;
        using (var connection = _db.OpenConnection()) {
            using var interrupt = InterruptOnCancel(connection, cancellationToken);
            try {
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT rowid FROM extra_name_fts WHERE extra_name_fts MATCH @match LIMIT @cap";
                command.Parameters.AddWithValue("@match", match);
                command.Parameters.AddWithValue("@cap", ExtraSearchCandidates);
                using (var reader = command.ExecuteReader()) {
                    while (reader.Read()) {
                        ids.Add(reader.GetInt32(0));
                    }
                }
                total = ids.Count;
                if (ids.Count >= ExtraSearchCandidates) {
                    command.CommandText = "SELECT COUNT(*) FROM extra_name_fts WHERE extra_name_fts MATCH @match";
                    total = Convert.ToInt64(command.ExecuteScalar());
                }
            } catch (SqliteException ex) when (ex.SqliteErrorCode == SqliteInterruptCode && cancellationToken.IsCancellationRequested) {
                throw new OperationCanceledException("The search was cancelled.", ex, cancellationToken);
            }
        }
        var rows = GetExtraSpeciesByIds(ids);
        var hits = rows.Values
            .Select(row => Rank(row, key))
            .OrderBy(r => r.Score)
            .ThenBy(r => r.Hit.Species.ScientificName.Length)
            .ThenBy(r => r.Hit.Species.ScientificName, StringComparer.Ordinal)
            .Take(limit)
            .Select(r => r.Hit)
            .ToList();
        return new ExtraSearchResult(hits, total);
    }

    // 0: a name is the text; 1: a name starts with it; 2: a word in a name matched.
    private static (ExtraSearchHit Hit, int Score) Rank(ExtraSpeciesRow row, string key) {
        var names = new[] { row.ScientificName, row.WikidataName, row.CommonNameEn };
        var keys = names.Select(n => n is null ? null : SiteNameKey.Fold(n)).ToArray();
        var best = 2;
        int? matched = null;
        for (var i = 0; i < keys.Length; i++) {
            if (keys[i] is not { } k) {
                continue;
            }
            var score = k == key ? 0 : k.StartsWith(key, StringComparison.Ordinal) ? 1 : 2;
            if (score < best) {
                best = score;
                matched = i;
            }
        }
        // Only Wikidata's spelling needs a note: the scientific and English names are shown anyway.
        var matchedName = matched == 1 ? row.WikidataName : null;
        return (new ExtraSearchHit(row, matchedName, best == 0), best);
    }
}
