using System.Globalization;
using BeastieBot3.Iucn.Gbif;
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
//   col_id         the CoL placement file's species_match (species only; Accepted, Synonym and
//                  ProvisionallyAccepted give the accepted usage id), keyed by IUCN's own kingdom,
//                  genus and species spelling; otherwise the common names store's CoL cross-reference.
//                  Subpopulations get none: CoL has no subpopulations.
//   SPRAT          sprat.sqlite sprat_species, by exact scientific name, then by each name in
//                  IUCN_Red_List_Listed_Names. Only an EPBC-listed row gives a status.
//   DOIs           GBIF's copy of the IUCN checklist (the current global assessment of each taxon)
//                  and Wikidata items for assessments (wikidata_iucn_assessment_items).

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
        foreach (var (taxonId, items) in claims) {
            var taxon = taxa[taxonId];
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
                    candidates.Add(new WikidataCandidate(numericId, label, names));
                }
                chosen = SiteBuildRules.ChooseP627Item(candidates, taxon.ScientificName);
            }
            taxon.WikidataQid = "Q" + chosen.ToString(CultureInfo.InvariantCulture);
            taxon.WikidataQidSource = "p627";
            stats.QidsFromP627++;
        }

        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT iucn_taxon_id, entity_numeric_id
                FROM wikidata_pending_iucn_matches
                WHERE match_method IN ('TaxonName', 'CachedName') AND is_synonym = 0
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (!long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var taxonId)
                    || !taxa.TryGetValue(taxonId, out var taxon) || taxon.WikidataQid is not null) {
                    continue;
                }
                taxon.WikidataQid = "Q" + reader.GetInt64(1).ToString(CultureInfo.InvariantCulture);
                taxon.WikidataQidSource = "name-match";
                stats.QidsFromNameMatch++;
            }
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

    /// Sets sprat_taxon_id and epbc_status; returns the report file the SPRAT database was imported from.
    public static string? ReadSprat(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var byName = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var taxon in taxa.Values) {
            byName.TryAdd(taxon.ScientificName, taxon.TaxonId);
        }

        // Best SPRAT row per IUCN taxon: an exact scientific name match beats a listed-name match,
        // then a row with an EPBC listing, then the lowest SPRAT id.
        var best = new Dictionary<long, (int Rank, long SpratId, string? Epbc)>();
        using var connection = OpenReadOnly(path);
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT sprat_taxon_id, scientific_name, epbc_status, IUCN_Red_List_Listed_Names FROM sprat_species";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (reader.IsDBNull(0) || !long.TryParse(reader.GetString(0).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var spratId)) {
                    continue;
                }
                var epbc = SiteBuildRules.EpbcCode(reader.IsDBNull(2) ? null : reader.GetString(2));
                var names = new List<(string Name, bool Exact)>();
                if (!reader.IsDBNull(1)) {
                    names.Add((reader.GetString(1).Trim(), true));
                }
                if (!reader.IsDBNull(3)) {
                    names.AddRange(reader.GetString(3).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(n => (n, false)));
                }
                foreach (var (name, exact) in names) {
                    if (!byName.TryGetValue(name, out var taxonId)) {
                        continue;
                    }
                    var rank = (exact ? 0 : 2) + (epbc is null ? 1 : 0);
                    if (!best.TryGetValue(taxonId, out var current) || rank < current.Rank
                        || (rank == current.Rank && spratId < current.SpratId)) {
                        best[taxonId] = (rank, spratId, epbc);
                    }
                    break;
                }
            }
        }
        foreach (var (taxonId, match) in best) {
            var taxon = taxa[taxonId];
            taxon.SpratTaxonId = match.SpratId;
            taxon.EpbcStatus = match.Epbc;
            stats.SpratMatched++;
            if (match.Epbc is not null) {
                stats.EpbcStatuses++;
            }
        }

        using var file = connection.CreateCommand();
        file.CommandText = "SELECT filename FROM import_metadata ORDER BY id DESC LIMIT 1";
        return file.ExecuteScalar() is string fileName ? Path.GetFileName(fileName.Trim()) : null;
    }

    // ------------------------------------------------------------ GBIF

    public static (string? Version, string? Published) ReadGbif(string zipPath, IReadOnlyDictionary<long, SiteTaxon> taxa,
        SiteDoiSources dois, CancellationToken cancellationToken) {
        var checklist = GbifIucnChecklistReader.Read(zipPath, cancellationToken);
        foreach (var (taxonId, taxon) in checklist.Taxa) {
            if (taxa.ContainsKey(taxonId) && taxon.Doi is not null) {
                dois.Gbif[taxonId] = (taxon.AssessmentId, taxon.Doi);
            }
        }
        var dataset = checklist.Summary.Dataset;
        return (dataset.RedListVersion ?? dataset.VersionText, dataset.PubDate);
    }
}
