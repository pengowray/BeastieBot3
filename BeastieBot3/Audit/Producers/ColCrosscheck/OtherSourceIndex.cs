using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Microsoft.Data.Sqlite;

// What Wikidata and the English Wikipedia hold for a taxon, looked up by IUCN taxon id.
//
// Only the "not found in the Catalogue of Life" report uses this, and only for the few hundred taxa
// on it, so every answer is a small indexed query rather than a table scan. The report's claim is
// "there is no route from this taxon into CoL", and these two sources qualify that claim in two
// ways worth showing:
//
//   Recognised elsewhere - a Wikidata item or a Wikipedia article means the name is in use even
//                          though CoL has no record of it. Nothing there means it is not.
//   Another name         - Wikidata's taxon name, the article's title, and the title a redirect
//                          points at are three names for the taxon that IUCN does not publish. When
//                          one of them is in CoL, that is the missing link between the two
//                          catalogues, and it is usually a genus transfer CoL made and IUCN has not.
//
// Both caches are optional. When one is absent its columns stay blank, and the report says the
// check did not run rather than that nothing was found.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal sealed record OtherSourceHit {
    /// Wikidata item id ("Q140"), or null when no item carries this IUCN taxon id.
    public string? WikidataId { get; init; }
    /// English Wikipedia article title the taxon is matched to, or null when it has none.
    public string? WikipediaTitle { get; init; }
    /// Names for this taxon from either source that IUCN does not publish, best first.
    public IReadOnlyList<string> OtherNames { get; init; } = Array.Empty<string>();

    public static readonly OtherSourceHit None = new();
}

internal sealed class OtherSourceIndex {
    private readonly SqliteConnection? _wikidata;
    private readonly SqliteConnection? _wikipedia;

    private OtherSourceIndex(SqliteConnection? wikidata, SqliteConnection? wikipedia) {
        _wikidata = wikidata;
        _wikipedia = wikipedia;
    }

    public bool HasWikidata => _wikidata is not null;
    public bool HasWikipedia => _wikipedia is not null;
    public bool HasAnything => HasWikidata || HasWikipedia;

    /// Builds an index over whichever caches are present and carry the tables this needs. Returns
    /// null when neither does, so callers can leave the columns out entirely.
    public static OtherSourceIndex? Build(SqliteConnection? wikidata, SqliteConnection? wikipedia) {
        var wd = wikidata is not null
                 && AuditContext.ObjectExists(wikidata, "wikidata_p627_values")
                 && AuditContext.ObjectExists(wikidata, "wikidata_entities")
            ? wikidata : null;
        var wp = wikipedia is not null
                 && AuditContext.ObjectExists(wikipedia, "taxon_wiki_matches")
                 && AuditContext.ObjectExists(wikipedia, "wiki_pages")
            ? wikipedia : null;
        return wd is null && wp is null ? null : new OtherSourceIndex(wd, wp);
    }

    public OtherSourceHit Lookup(long taxonId, string? iucnName, CancellationToken ct) {
        var id = taxonId.ToString(CultureInfo.InvariantCulture);
        var names = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(iucnName)) {
            seen.Add(iucnName!.Trim());   // the name we already know is absent from CoL
        }

        void Offer(string? value) {
            if (string.IsNullOrWhiteSpace(value)) {
                return;
            }
            // Wikipedia titles use spaces in this cache, but a stored redirect target may not.
            var name = value.Replace('_', ' ').Trim();
            // A bare genus is not another name for a species; offering it would pair the taxon with
            // its genus article and read as a match.
            if (name.IndexOf(' ') < 0 || !seen.Add(name)) {
                return;
            }
            names.Add(name);
        }

        string? wikidataId = null;
        if (_wikidata is not null) {
            ct.ThrowIfCancellationRequested();
            wikidataId = Scalar(_wikidata, """
                SELECT e.entity_id FROM wikidata_p627_values p
                JOIN wikidata_entities e ON e.entity_numeric_id = p.entity_numeric_id
                WHERE p.value = @id LIMIT 1
                """, id);
            if (wikidataId is not null && AuditContext.ObjectExists(_wikidata, "wikidata_scientific_names")) {
                foreach (var name in Column(_wikidata, """
                    SELECT s.name FROM wikidata_p627_values p
                    JOIN wikidata_scientific_names s ON s.entity_numeric_id = p.entity_numeric_id
                    WHERE p.value = @id
                    """, id)) {
                    Offer(name);
                }
            }
        }

        string? title = null;
        if (_wikipedia is not null) {
            ct.ThrowIfCancellationRequested();
            using var cmd = _wikipedia.CreateCommand();
            cmd.CommandText = """
                SELECT m.candidate_title, p.page_title, p.redirect_target
                FROM taxon_wiki_matches m
                LEFT JOIN wiki_pages p ON p.id = m.page_row_id
                WHERE m.taxon_source = 'iucn' AND m.taxon_identifier = @id AND m.match_status = 'matched'
                LIMIT 1
                """;
            cmd.Parameters.AddWithValue("@id", id);
            cmd.CommandTimeout = 30;
            using var reader = cmd.ExecuteReader();
            if (reader.Read()) {
                title = Text(reader, 0) ?? Text(reader, 1);
                title = title?.Replace('_', ' ').Trim();
                Offer(title);
                Offer(Text(reader, 2));   // the article is a redirect: its target is the current name
            }
        }

        return new OtherSourceHit {
            WikidataId = wikidataId,
            WikipediaTitle = string.IsNullOrWhiteSpace(title) ? null : title,
            OtherNames = names,
        };
    }

    public static string WikidataUrl(string entityId) => $"https://www.wikidata.org/wiki/{Uri.EscapeDataString(entityId)}";

    public static string WikipediaUrl(string title) =>
        $"https://en.wikipedia.org/wiki/{Uri.EscapeDataString(title.Replace(' ', '_'))}";

    private static string? Scalar(SqliteConnection conn, string sql, string id) {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.CommandTimeout = 30;
        var value = cmd.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static List<string> Column(SqliteConnection conn, string sql, string id) {
        var result = new List<string>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.CommandTimeout = 30;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            var value = Text(reader, 0);
            if (value is not null) {
                result.Add(value);
            }
        }
        return result;
    }

    private static string? Text(SqliteDataReader reader, int i) =>
        reader.IsDBNull(i) || string.IsNullOrWhiteSpace(reader.GetString(i)) ? null : reader.GetString(i).Trim();
}
