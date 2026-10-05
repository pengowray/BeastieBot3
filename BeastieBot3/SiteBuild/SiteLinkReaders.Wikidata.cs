using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;

// Wikidata for `site build-db`: each taxon's item, its IUCN conservation status (P141) statements,
// the items for assessments and their DOIs, and the taxon synonyms (P1420) of the items.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ Wikidata

    public static void ReadWikidata(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        SiteDoiSources dois, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var hasAssessmentItems = WikidataAssessmentItemTable.Exists(connection);
        var deprecated = WikidataIucnReferenceTables.ReadDeprecatedTaxonIds(connection);
        if (deprecated is null) {
            stats.Warnings.Add("The Wikidata cache has no list of IUCN taxon IDs at deprecated rank, so every item that states a taxon's id is taken to state it. To fill the list, run wikidata iucn-assessment-items.");
        }

        var claims = new Dictionary<long, List<(long NumericId, string? Label)>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT p.value, p.entity_numeric_id, e.label_en
                FROM wikidata_p627_values p
                LEFT JOIN wikidata_entities e ON e.entity_numeric_id = p.entity_numeric_id
                WHERE p.source = 'claim'
                """;
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (!long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                    || !taxa.ContainsKey(taxonId)) {
                    continue;
                }
                if (!claims.TryGetValue(taxonId, out var list)) {
                    claims[taxonId] = list = new List<(long, string?)>();
                }
                var numericId = reader.GetInt64(1);
                if (!list.Any(c => c.NumericId == numericId)) {
                    list.Add((numericId, reader.IsDBNull(2) ? null : reader.GetString(2)));
                }
            }
        }

        using var taxonNames = connection.CreateCommand();
        taxonNames.CommandText = "SELECT name FROM wikidata_scientific_names WHERE entity_numeric_id = @id";
        var taxonNameId = taxonNames.Parameters.Add("@id", SqliteType.Integer);
        // The other items stating each taxon's id, by taxon, filled in with their P141 by ReadP141.
        var otherItems = new Dictionary<long, List<(long NumericId, bool Deprecated)>>();
        if (deprecated is not null) {
            stats.QidsP627Deprecated = 0;
        }
        foreach (var (taxonId, items) in claims) {
            var taxon = taxa[taxonId];
            var idText = taxonId.ToString(CultureInfo.InvariantCulture);
            bool IsDeprecated(long numericId) => deprecated?.Contains((numericId, idText)) == true;
            long chosen;
            if (items.Count == 1) {
                chosen = items[0].NumericId;
            } else {
                stats.QidTieBreaks++;
                var candidates = new List<WikidataCandidate>();
                foreach (var (numericId, label) in items) {
                    taxonNameId.Value = numericId;
                    var names = new List<string>();
                    using (var reader = taxonNames.ExecuteReader()) {
                        while (reader.Read()) {
                            names.Add(reader.GetString(0));
                        }
                    }
                    candidates.Add(new WikidataCandidate(numericId, label, names, IsDeprecated(numericId)));
                }
                chosen = SiteBuildRules.ChooseP627Item(candidates, taxon.ScientificName);
                otherItems[taxonId] = items.Where(i => i.NumericId != chosen)
                    .OrderBy(i => i.NumericId)
                    .Select(i => (i.NumericId, IsDeprecated(i.NumericId)))
                    .ToList();
            }
            taxon.WikidataQid = "Q" + chosen.ToString(CultureInfo.InvariantCulture);
            taxon.WikidataQidSource = "p627";
            if (IsDeprecated(chosen)) {
                taxon.WikidataP627Deprecated = true;
                stats.QidsP627Deprecated++;
            }
            stats.QidsFromP627++;
        }

        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT m.iucn_taxon_id, m.entity_numeric_id, e.description_en
                FROM wikidata_pending_iucn_matches m
                LEFT JOIN wikidata_entities e ON e.entity_numeric_id = m.entity_numeric_id
                WHERE m.match_method IN ('TaxonName', 'CachedName') AND m.is_synonym = 0
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (!long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                    || !taxa.TryGetValue(taxonId, out var taxon) || taxon.WikidataQid is not null) {
                    continue;
                }
                // A match by name alone can land on a same-named taxon in another kingdom (the plant
                // Clusia flava on an insect's item). The item's description says what it is.
                if (SiteBuildRules.DescribesAnotherKingdom(reader.IsDBNull(2) ? null : reader.GetString(2), taxon.Kingdom)) {
                    stats.QidsNameMatchOtherKingdom++;
                    continue;
                }
                taxon.WikidataQid = "Q" + reader.GetInt64(1).ToString(CultureInfo.InvariantCulture);
                taxon.WikidataQidSource = "name-match";
                stats.QidsFromNameMatch++;
            }
        }

        ReadP141(connection, taxa, otherItems, hasAssessmentItems, stats, cancellationToken);

        if (!hasAssessmentItems) {
            stats.Warnings.Add("The Wikidata cache has no table of Wikidata items for IUCN assessments, so the build takes no DOIs and no assessment items from Wikidata. To fill the table, run wikidata iucn-assessment-items.");
            return;
        }
        foreach (var row in WikidataAssessmentItemTable.ReadAll(connection)) {
            cancellationToken.ThrowIfCancellationRequested();
            stats.WikidataItems.Add(row);
        }

        // DOIs of Wikidata items for assessments: doi first, then any others in all_dois.
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT assessment_id, doi, all_dois
                FROM wikidata_iucn_assessment_items
                WHERE assessment_id IS NOT NULL
                ORDER BY qid_numeric
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var assessmentId = reader.GetInt64(0);
                var found = new List<string>();
                if (!reader.IsDBNull(1) && SiteBuildRules.NullIfBlank(reader.GetString(1)) is { } mainDoi) {
                    found.Add(mainDoi);
                }
                if (!reader.IsDBNull(2)) {
                    found.AddRange(reader.GetString(2).Split(new[] { ' ', ',', ';', '|', '\n', '\t' },
                        StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
                }
                if (found.Count == 0) {
                    continue;
                }
                if (!dois.Wikidata.TryGetValue(assessmentId, out var list)) {
                    dois.Wikidata[assessmentId] = list = new List<string>();
                }
                foreach (var doi in found) {
                    if (!list.Contains(doi, StringComparer.OrdinalIgnoreCase)) {
                        list.Add(doi);
                    }
                }
            }
        }
    }

    // The IUCN conservation status (P141) statements of each item that states its taxon's IUCN id,
    // and of the other items that state it too, from the index tables the cache fills when it
    // downloads an item (wikidata_p141_statements and wikidata_p141_references), and the day it
    // downloaded the item. Reading every item's JSON instead took 94 seconds for 4.2 GB in October
    // 2026; the index tables take about a second, but record only the first stated in (P248) item
    // of each reference, its IUCN taxon IDs (P627), and no reference URL or retrieved date.
    //
    // A reference cites IUCN when it has an IUCN taxon ID; its stated in is the Red List (Q32059),
    // IUCN (Q48268), an edition of the Red List (wikidata_iucn_red_list_editions) or an assessment's
    // item (wikidata_iucn_assessment_items); or it has a reference URL (P854) on iucnredlist.org or
    // a subdomain. For the URL and for a stated in after the first, the JSON of the items is read,
    // but only for the statements whose references the index shows citing something else or nothing
    // (P141JsonReferences; 87 items in the build of 3 October 2026).
    private static void ReadP141(SqliteConnection connection, IReadOnlyDictionary<long, SiteTaxon> taxa,
        IReadOnlyDictionary<long, List<(long NumericId, bool Deprecated)>> otherItems, bool hasAssessmentItems, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var byItem = new Dictionary<long, List<SiteTaxon>>();
        foreach (var taxon in taxa.Values) {
            if (taxon.WikidataQidSource == "p627" && taxon.WikidataQid is { } qid
                && long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var numericId)) {
                if (!byItem.TryGetValue(numericId, out var list)) {
                    byItem[numericId] = list = new List<SiteTaxon>();
                }
                list.Add(taxon);
            }
        }
        var wanted = new HashSet<long>(byItem.Keys);
        foreach (var list in otherItems.Values) {
            wanted.UnionWith(list.Select(i => i.NumericId));
        }

        var editions = WikidataIucnReferenceTables.ReadEditions(connection);
        stats.RedListEditions = editions?.Count;
        if (editions is null) {
            stats.Warnings.Add("The Wikidata cache has no list of the IUCN Red List's editions, so a P141 reference stated in an edition counts as citing IUCN only when it has an IUCN taxon ID. To fill the list, run wikidata iucn-assessment-items.");
        }
        var iucnSources = new HashSet<long>(WikidataStatusStatement.IucnSourceItems.Concat(editions ?? Enumerable.Empty<string>())
            .Select(q => long.Parse(q.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture)));
        if (hasAssessmentItems) {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT qid_numeric FROM wikidata_iucn_assessment_items";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                iucnSources.Add(reader.GetInt64(0));
            }
        }

        var downloaded = new Dictionary<long, string>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_numeric_id, downloaded_at FROM wikidata_entities WHERE json_downloaded = 1";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var numericId = reader.GetInt64(0);
                if (wanted.Contains(numericId)) {
                    var at = reader.IsDBNull(1) ? null : StoredUtc.Parse(reader.GetString(1));
                    downloaded[numericId] = at?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? string.Empty;
                }
            }
        }

        // One row per (reference, IUCN taxon id); a reference with no IUCN taxon id has an empty one.
        var references = new Dictionary<(long Item, string Statement), ReferenceSummary>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_numeric_id, statement_id, reference_hash, source_qid, iucn_taxon_id FROM wikidata_p141_references";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var key = (reader.GetInt64(0), reader.GetString(1));
                if (!downloaded.ContainsKey(key.Item1)) {
                    continue;
                }
                if (!references.TryGetValue(key, out var summary)) {
                    references[key] = summary = new ReferenceSummary();
                }
                summary.Hashes.Add(reader.GetString(2));
                if (!reader.IsDBNull(3)) {
                    var source = reader.GetInt64(3);
                    summary.StatedIn.Add("Q" + source.ToString(CultureInfo.InvariantCulture));
                    summary.CitesIucn |= iucnSources.Contains(source);
                }
                var taxonId = reader.GetString(4);
                if (taxonId.Length > 0) {
                    summary.TaxonIds.Add(taxonId);
                    summary.CitesIucn = true;
                }
            }
        }

        // Statements with references that the index shows citing something else or nothing: their
        // items' JSON may show an IUCN reference URL or a later stated in.
        var unsure = references.Where(r => !r.Value.CitesIucn && r.Value.Hashes.Count > 0)
            .GroupBy(r => r.Key.Item, r => r.Key.Statement);
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT json FROM wikidata_entities WHERE entity_numeric_id = @id";
            var idParameter = command.Parameters.Add("@id", SqliteType.Integer);
            foreach (var group in unsure) {
                cancellationToken.ThrowIfCancellationRequested();
                idParameter.Value = group.Key;
                var json = command.ExecuteScalar() as string;
                stats.P141ItemsReadAsJson++;
                var found = P141JsonReferences.StatementsCitingIucn(json, group.ToHashSet(StringComparer.Ordinal), iucnSources.Contains);
                foreach (var (statementId, citation) in found) {
                    references[(group.Key, statementId)].CitesIucn = true;
                    if (citation == P141JsonCitation.ReferenceUrl) {
                        stats.P141CitesIucnByUrl++;
                    } else {
                        stats.P141CitesIucnByLaterStatedIn++;
                    }
                }
            }
        }

        var statements = new Dictionary<long, List<WikidataStatusStatement>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_numeric_id, statement_id, status_entity_id, rank FROM wikidata_p141_statements";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var numericId = reader.GetInt64(0);
                if (!downloaded.ContainsKey(numericId)) {
                    continue;
                }
                var statementId = reader.GetString(1);
                if (!statements.TryGetValue(numericId, out var list)) {
                    statements[numericId] = list = new List<WikidataStatusStatement>();
                }
                var summary = references.GetValueOrDefault((numericId, statementId));
                list.Add(new WikidataStatusStatement(statementId, reader.GetString(2), reader.GetString(3),
                    summary?.StatedIn.ToList() ?? [], summary?.TaxonIds.ToList() ?? [], summary?.Hashes.Count ?? 0, summary?.CitesIucn ?? false));
            }
        }

        List<WikidataStatusStatement> StatementsOf(long numericId) =>
            statements.TryGetValue(numericId, out var found)
                ? found.OrderBy(s => RankOrder(s.Rank)).ThenBy(s => s.Id, StringComparer.Ordinal).ToList()
                : [];

        foreach (var (numericId, day) in downloaded) {
            if (!byItem.TryGetValue(numericId, out var linked)) {
                continue;
            }
            var list = StatementsOf(numericId);
            var json = WikidataStatusStatement.ListToJson(list);
            foreach (var taxon in linked) {
                taxon.WikidataP141 = json;
                taxon.WikidataItemDownloaded = day.Length == 0 ? null : day;
                stats.QidsWithP141Known++;
                if (list.Count == 0) {
                    stats.QidsWithNoP141++;
                }
            }
        }

        foreach (var (taxonId, others) in otherItems) {
            var items = others.Select(o => new WikidataOtherTaxonItem("Q" + o.NumericId.ToString(CultureInfo.InvariantCulture), o.Deprecated,
                downloaded.ContainsKey(o.NumericId) ? StatementsOf(o.NumericId) : null)).ToList();
            taxa[taxonId].WikidataOtherItems = WikidataOtherTaxonItem.ListToJson(items);
        }

        static int RankOrder(string rank) => rank switch { "preferred" => 0, "normal" => 1, _ => 2 };
    }

    private sealed class ReferenceSummary {
        public HashSet<string> Hashes { get; } = new(StringComparer.Ordinal);
        public SortedSet<string> StatedIn { get; } = new(StringComparer.Ordinal);
        public SortedSet<string> TaxonIds { get; } = new(StringComparer.Ordinal);
        public bool CitesIucn { get; set; }
    }

    /// The scientific names of the items that each taxon's Wikidata item names as a taxon synonym
    /// (P1420), at normal or preferred rank. Only items the Wikidata cache has downloaded have a
    /// name, and most synonym items are not downloaded. Wikidata states the author of a name as an
    /// item (P405 on P225), so these synonyms have no authority. Reads every cached item whose JSON
    /// mentions P1420 (about 45 seconds over the whole cache).
    public static void ReadWikidataSynonyms(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var byItem = new Dictionary<long, List<SiteTaxon>>();
        foreach (var taxon in taxa.Values) {
            if (taxon.WikidataQid is { Length: > 1 } qid && long.TryParse(qid.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture, out var id)) {
                if (!byItem.TryGetValue(id, out var list)) {
                    byItem[id] = list = new List<SiteTaxon>();
                }
                list.Add(taxon);
            }
        }
        using var connection = OpenReadOnly(path);
        var wanted = new List<(List<SiteTaxon> Taxa, long SynonymItem)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_numeric_id, json FROM wikidata_entities WHERE json LIKE '%\"P1420\"%'";
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (!byItem.TryGetValue(reader.GetInt64(0), out var itemTaxa) || reader.IsDBNull(1)) {
                    continue;
                }
                foreach (var synonymItem in WikidataTaxonSynonyms.ItemsIn(reader.GetString(1))) {
                    wanted.Add((itemTaxa, synonymItem));
                }
            }
        }
        using var names = connection.CreateCommand();
        names.CommandText = "SELECT name FROM wikidata_scientific_names WHERE entity_numeric_id = @id ORDER BY language";
        var idParameter = names.Parameters.Add("@id", SqliteType.Integer);
        foreach (var (itemTaxa, synonymItem) in wanted) {
            stats.WikidataSynonymItems++;
            idParameter.Value = synonymItem;
            if (names.ExecuteScalar() is not string name) {
                continue;
            }
            stats.WikidataSynonymsNamed++;
            foreach (var taxon in itemTaxa) {
                taxon.WikidataSynonyms.Add(new SiteSynonym(name));
            }
        }
    }
}
