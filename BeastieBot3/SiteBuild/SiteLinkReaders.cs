using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Sprat;
using BeastieBot3.Wikidata;
using Microsoft.Data.Sqlite;

// The links and outside identifiers of each taxon, and the outside DOIs, for `site build-db`. Every
// source is opened read-only.
//
//   enwiki_title   enwiki_cache.sqlite taxon_wiki_matches: taxon_source 'iucn', match_status
//                  'matched', the final title after redirects.
//   wikidata_qid   wikidata_cache.sqlite: an item stating the IUCN taxon id (P627 claim) -> 'p627';
//                  several items -> SiteBuildRules.ChooseP627Item. Otherwise an item matched by name
//                  (wikidata_pending_iucn_matches, methods TaxonName and CachedName, not through a
//                  synonym) -> 'name-match'.
//                  An item that states the id only at deprecated rank (wikidata_deprecated_iucn_taxon_ids,
//                  which `wikidata iucn-assessment-items` fills) is chosen only when every item does
//                  (wikidata_p627_deprecated). The other items stating the id go in
//                  wikidata_other_items, with their P141 statements.
//   wikidata_p141  for a 'p627' item the cache has downloaded: its P141 statements with their rank and,
//                  from their references, the stated in (P248) items, the IUCN taxon IDs (P627), how
//                  many there are, and whether one cites IUCN (wikidata_p141_statements and
//                  wikidata_p141_references, plus the Red List editions in
//                  wikidata_iucn_red_list_editions and the assessment items, and the item's JSON for
//                  the few statements the index cannot decide), and the day it was downloaded
//                  (wikidata_item_downloaded).
//   col_id         the CoL placement file's species_match (species only; Accepted, Synonym and
//                  ProvisionallyAccepted give the accepted usage id), keyed by IUCN's own kingdom,
//                  genus and species spelling; otherwise the common names store's CoL cross-reference.
//                  Subpopulations get none: CoL has no subpopulations.
//   SPRAT          sprat.sqlite sprat_species. The profile of the whole taxon: by exact scientific
//                  name, then by each name in IUCN_Red_List_Listed_Names. Profiles of populations:
//                  every row named after the taxon with a population in brackets
//                  (SiteBuildRules.ClassifySpratName). Only an EPBC-listed row gives a status.
//                  The two listed-name columns are named from the report's header text, so a report
//                  may lack them; they then read as empty (SpratTableColumns), with a warning.
//   DOI cache     iucn_doi_cache.sqlite doi_check: DOIs `iucn resolve-dois` found in Crossref's list
//                  of IUCN DOIs or at doi.org; crossref_works: the title Crossref registered for each
//                  DOI, for the name in the titles of new Wikidata items for assessments.
//   DOIs           GBIF's copy of the IUCN checklist (the current global assessment of each taxon)
//                  and Wikidata items for assessments (wikidata_iucn_assessment_items).
//   Wikidata items for assessments: the same table, items that are publications (SiteWikidataItems).
//                  A cache written before the table existed gives none.
//   GBIF citation  The checklist's recommended citation from its eml.xml, and the dataset DOI: the
//                  citation's identifier, else the DOI in the citation text, else 10.15468/0qnb58.

namespace BeastieBot3.SiteBuild;

internal static class SiteLinkReaders {
    public static SqliteConnection OpenReadOnly(string path) => SiteIucnCsvReader.OpenReadOnly(path);

    // ------------------------------------------------------------ English Wikipedia

    public static void ReadWikipedia(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT taxon_identifier, redirect_final_title, normalized_title
            FROM taxon_wiki_matches
            WHERE taxon_source = 'iucn' AND match_status = 'matched'
            """;
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!long.TryParse(reader.GetString(0), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                || !taxa.TryGetValue(taxonId, out var taxon)) {
                continue;
            }
            var title = SiteBuildRules.NullIfBlank(reader.IsDBNull(1) ? null : reader.GetString(1))
                ?? SiteBuildRules.NullIfBlank(reader.IsDBNull(2) ? null : reader.GetString(2));
            if (title is not null) {
                taxon.EnwikiTitle = title;
                stats.EnwikiTitles++;
            }
        }
    }

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
    // (P141JsonReferences; 89 statements in October 2026).
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

    // ------------------------------------------------------------ Catalogue of Life

    /// Sets col_id from the placement file and returns the CoL release it was built from ("COL26.7 XR").
    public static string? ReadColPlacement(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var bySpecies = new Dictionary<(string Kingdom, string Genus, string Species), string>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT kingdom, genus, species, accepted_id
                FROM species_match
                WHERE match_kind IN ('Accepted', 'Synonym', 'ProvisionallyAccepted') AND accepted_id IS NOT NULL AND accepted_id <> ''
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                bySpecies[(reader.GetString(0), reader.GetString(1), reader.GetString(2))] = reader.GetString(3);
            }
        }
        foreach (var taxon in taxa.Values) {
            if (taxon.Kind != SiteTaxonKind.Species || taxon.Kingdom is null || taxon.Genus is null || taxon.SpeciesEpithet is null) {
                continue;
            }
            if (bySpecies.TryGetValue((taxon.Kingdom, taxon.Genus, taxon.SpeciesEpithet), out var colId)) {
                taxon.ColId = colId;
                stats.ColIdsFromPlacement++;
            }
        }

        using var source = connection.CreateCommand();
        source.CommandText = "SELECT col_path FROM placement_source ORDER BY built_at DESC LIMIT 1";
        return SiteBuildRules.ColReleaseFromPath(source.ExecuteScalar() as string);
    }

    public static void ApplyColCrossReferences(IReadOnlyDictionary<long, SiteTaxon> taxa, IReadOnlyDictionary<long, string> crossReferences,
        SiteBuildStats stats) {
        foreach (var (taxonId, colId) in crossReferences) {
            if (taxa.TryGetValue(taxonId, out var taxon) && taxon.ColId is null && taxon.Kind != SiteTaxonKind.Subpopulation) {
                taxon.ColId = colId;
                stats.ColIdsFromCrossReference++;
            }
        }
    }

    // ------------------------------------------------------------ SPRAT

    /// Fills each taxon's EpbcListings; returns the report file the SPRAT database was imported from.
    /// A name shared by several taxa goes to the taxon in the release with the lowest id, else the
    /// lowest id of the others. A database with no sprat_species table, or one without the SPRAT id,
    /// scientific name or EPBC status column, gives no listings and a warning. A missing IUCN listed
    /// names or EPBC listed name column reads as empty, with a warning.
    public static string? ReadSprat(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var columns = SpratTableColumns.Read(connection);
        if (columns is null) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.Table} table, so no SPRAT profiles or EPBC listings were used.");
            return null;
        }
        var missingRequired = new[] { SpratColumns.SpratTaxonId, SpratColumns.ScientificName, SpratColumns.EpbcStatus }
            .Where(c => !columns.Has(c)).ToList();
        if (missingRequired.Count > 0) {
            stats.Warnings.Add($"The SPRAT database {path} has no {string.Join(", ", missingRequired)} column in {SpratColumns.Table}, "
                + "so no SPRAT profiles or EPBC listings were used.");
            return null;
        }
        if (!columns.Has(SpratColumns.IucnListedName)) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.IucnListedName} column, "
                + "so SPRAT profiles were matched by their scientific name only.");
        }
        if (!columns.Has(SpratColumns.EpbcListedName)) {
            stats.Warnings.Add($"The SPRAT database {path} has no {SpratColumns.EpbcListedName} column, "
                + "so each EPBC listing gives SPRAT's scientific name as the listed name.");
        }

        var byName = new Dictionary<string, SiteTaxon>(StringComparer.Ordinal);
        foreach (var taxon in taxa.Values.OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            byName.TryAdd(taxon.ScientificName, taxon);
        }

        // The profile for the whole taxon: an exact scientific name match beats a sense in brackets,
        // which beats a listed-name match; then a profile with an EPBC listing; then the lowest SPRAT id.
        var best = new Dictionary<long, (int Rank, EpbcListing Listing)>();
        var populations = new Dictionary<long, List<EpbcListing>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = $"""
                SELECT {columns.Select(SpratColumns.SpratTaxonId)}, {columns.Select(SpratColumns.ScientificName)},
                       {columns.Select(SpratColumns.EpbcStatus)}, {columns.Select(SpratColumns.IucnListedName)},
                       {columns.Select(SpratColumns.EpbcListedName)}
                FROM {SpratTableColumns.Quote(SpratColumns.Table)}
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (reader.IsDBNull(0) || !long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var spratId)) {
                    continue;
                }
                var epbc = SiteBuildRules.EpbcCode(reader.IsDBNull(2) ? null : reader.GetString(2));
                var scientificName = SiteBuildRules.NullIfBlank(reader.IsDBNull(1) ? null : reader.GetString(1));
                var listedName = SiteBuildRules.NullIfBlank(reader.IsDBNull(4) ? null : reader.GetString(4)) ?? scientificName;

                // A population of a taxon: the taxon's name with the population in brackets.
                SiteTaxon? populationOf = null;
                if (scientificName is not null) {
                    for (var at = scientificName.IndexOf(" (", StringComparison.Ordinal); at > 0;
                         at = scientificName.IndexOf(" (", at + 1, StringComparison.Ordinal)) {
                        if (!byName.TryGetValue(scientificName[..at], out var taxon)) {
                            continue;
                        }
                        var match = SiteBuildRules.ClassifySpratName(scientificName, taxon.ScientificName);
                        if (match.Kind == SpratNameKind.Population) {
                            // The population as the listed name gives it, when that has the same form.
                            var listed = listedName is null ? match : SiteBuildRules.ClassifySpratName(listedName, taxon.ScientificName);
                            var population = listed.Kind == SpratNameKind.Population ? listed.Population! : match.Population!;
                            if (!populations.TryGetValue(taxon.TaxonId, out var list)) {
                                populations[taxon.TaxonId] = list = new List<EpbcListing>();
                            }
                            list.Add(new EpbcListing(spratId, listedName ?? scientificName, epbc, EpbcAppliesTo.Population, population));
                            populationOf = taxon;
                        } else if (match.Kind == SpratNameKind.Taxon) {
                            Consider(taxon, 2);
                        } else if (match.Kind == SpratNameKind.NotPopulation) {
                            stats.SpratBracketsNotPopulation++;
                        }
                        break;
                    }
                }

                // The profile of the whole taxon: by its scientific name, then by each IUCN name it lists.
                var names = new List<(string Name, bool Exact)>();
                if (scientificName is not null) {
                    names.Add((scientificName, true));
                }
                if (!reader.IsDBNull(3)) {
                    names.AddRange(reader.GetString(3).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(n => (n, false)));
                }
                foreach (var (name, exact) in names) {
                    if (!byName.TryGetValue(name, out var taxon) || taxon == populationOf) {
                        continue;
                    }
                    Consider(taxon, exact ? 0 : 4);
                    break;
                }

                void Consider(SiteTaxon taxon, int baseRank) {
                    var rank = baseRank + (epbc is null ? 1 : 0);
                    if (!best.TryGetValue(taxon.TaxonId, out var current) || rank < current.Rank
                        || (rank == current.Rank && spratId < current.Listing.SpratTaxonId)) {
                        best[taxon.TaxonId] = (rank, new EpbcListing(spratId, listedName ?? scientificName ?? string.Empty, epbc,
                            EpbcAppliesTo.Taxon, null));
                    }
                }
            }
        }
        foreach (var (taxonId, (_, listing)) in best) {
            taxa[taxonId].EpbcListings.Add(listing);
            stats.SpratMatched++;
            if (listing.Status is not null) {
                stats.EpbcStatuses++;
            }
        }
        foreach (var (taxonId, list) in populations) {
            var taxon = taxa[taxonId];
            foreach (var listing in list.OrderBy(l => l.Population, StringComparer.OrdinalIgnoreCase).ThenBy(l => l.SpratTaxonId)) {
                if (taxon.EpbcListings.Any(l => l.SpratTaxonId == listing.SpratTaxonId)) {
                    continue;
                }
                taxon.EpbcListings.Add(listing);
                stats.SpratPopulationProfiles++;
                if (listing.Status is not null) {
                    stats.EpbcPopulationListings++;
                }
            }
        }

        // `sprat import` records the report file in import_metadata.
        if (DelimitedTableImporter.GetTableColumns(connection, "import_metadata")?.Contains("filename") != true) {
            return null;
        }
        using var file = connection.CreateCommand();
        file.CommandText = "SELECT filename FROM import_metadata ORDER BY id DESC LIMIT 1";
        return file.ExecuteScalar() is string fileName ? Path.GetFileName(fileName.Trim()) : null;
    }

    // ------------------------------------------------------------ DOIs from `iucn resolve-dois`

    /// Reads `iucn resolve-dois`'s cache (table doi_check: assessment_id, taxon_id, doi, checked_at,
    /// candidates_tried; doi NULL when no candidate resolved) into dois.Resolved. A file without the
    /// table, or with a table the build cannot read, gives no DOIs and a warning.
    public static void ReadDoiCache(string path, SiteDoiSources dois, SiteBuildStats stats, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'doi_check'";
            if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0) {
                stats.Warnings.Add($"The DOI cache {path} has no doi_check table, so no DOIs from `iucn resolve-dois` were used.");
                return;
            }
        }
        ReadCrossrefTitles(connection, path, dois, stats, cancellationToken);
        try {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT assessment_id, doi, checked_at FROM doi_check";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                stats.DoiChecksRead++;
                if (!reader.IsDBNull(2) && StoredUtc.Parse(reader.GetString(2)) is { } checkedAt
                    && (stats.DoiCheckedTo is null || checkedAt > stats.DoiCheckedTo)) {
                    stats.DoiCheckedTo = checkedAt;
                }
                if (reader.IsDBNull(0) || reader.IsDBNull(1) || SiteBuildRules.NullIfBlank(reader.GetString(1)) is not { } doi) {
                    continue;
                }
                stats.DoiChecksWithDoi++;
                dois.Resolved[reader.GetInt64(0)] = doi;
            }
        } catch (SqliteException ex) {
            dois.Resolved.Clear();
            stats.DoiChecksRead = 0;
            stats.DoiChecksWithDoi = 0;
            stats.DoiCheckedTo = null;
            stats.Warnings.Add($"The DOI cache {path} could not be read, so no DOIs from `iucn resolve-dois` were used: {ex.Message}");
        }
    }

    // The titles Crossref registered for IUCN DOIs (crossref_works.title), which `iucn resolve-dois`
    // stores from October 2026. A cache without them gives none, and a warning.
    private static void ReadCrossrefTitles(SqliteConnection connection, string path, SiteDoiSources dois, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'crossref_works'";
            if (Convert.ToInt64(exists.ExecuteScalar(), CultureInfo.InvariantCulture) == 0 || !Iucn.Doi.IucnDoiCacheStore.HasCrossrefTitles(connection)) {
                stats.Warnings.Add($"The DOI cache {path} has no titles from Crossref, so new Wikidata items use IUCN's citation name, which for an older assessment may be newer than the name in the assessment's title. To add the titles, run iucn resolve-dois --refresh-crossref.");
                return;
            }
        }
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT doi, assessment_id, title FROM crossref_works WHERE title IS NOT NULL";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            dois.CrossrefTitles[reader.GetString(0)] = (reader.GetInt64(1), reader.GetString(2));
        }
    }

    // ------------------------------------------------------------ GBIF

    public static GbifChecklistInfo ReadGbif(string zipPath, IReadOnlyDictionary<long, SiteTaxon> taxa,
        SiteDoiSources dois, CancellationToken cancellationToken) {
        var checklist = GbifIucnChecklistReader.Read(zipPath, cancellationToken);
        foreach (var (taxonId, taxon) in checklist.Taxa) {
            if (taxa.ContainsKey(taxonId) && taxon.Doi is not null) {
                dois.Gbif[taxonId] = (taxon.AssessmentId, taxon.Doi);
            }
        }
        return GbifChecklistInfo.From(checklist.Summary.Dataset);
    }
}

/// What the site shows about GBIF's copy of the IUCN checklist: its version, publication date,
/// recommended citation and dataset DOI (without a resolver prefix).
internal sealed record GbifChecklistInfo(string? Version, string? Published, string? Citation, string Doi) {
    public static GbifChecklistInfo From(GbifIucnDatasetInfo dataset) => new(
        dataset.RedListVersion ?? dataset.VersionText,
        dataset.PubDate,
        dataset.Citation,
        Iucn.Gbif.IucnDoi.Extract(dataset.CitationIdentifier) ?? Iucn.Gbif.IucnDoi.Extract(dataset.Citation) ?? GbifIucnChecklistFiles.DatasetDoi);
}
