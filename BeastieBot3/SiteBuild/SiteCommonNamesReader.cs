using BeastieBot3.CommonNames;
using Microsoft.Data.Sqlite;

// Reads the common names store (common_names.sqlite) for `site build-db`, read-only:
//   - every English common name of each IUCN taxon, with its source mapped to the site's sources
//     (wikipedia_title and wikipedia_taxobox -> wikipedia, wikidata_label and wikidata -> wikidata,
//     col -> col, iucn -> iucn);
//   - the best English name, chosen exactly as the Wikipedia lists choose it
//     (CommonNameStore.ChooseBest over the same candidates GetBestCommonNameForTaxon reads, with
//     ambiguous names skipped) and capitalised with the store's caps rules;
//   - Catalogue of Life synonyms (synonym_type 'synonym'; 'ambiguous_synonym' rows and the
//     constructed rank variants are left out; IUCN's synonyms come from the API records instead);
//   - Catalogue of Life ids from taxon_cross_references, the fallback for col_id.
// The store's taxa are keyed by primary_source 'iucn' and primary_source_id = IUCN taxon id.

namespace BeastieBot3.SiteBuild;

internal static class SiteCommonNamesReader {
    public static void Read(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats,
        Dictionary<long, string> colCrossReferences, CancellationToken cancellationToken) {
        using var store = CommonNameStore.OpenReadOnly(path);
        var ambiguous = store.GetAmbiguousNames("en");
        var capsRules = store.GetAllCapsRules();

        using var connection = SiteIucnCsvReader.OpenReadOnly(path);

        // English names, grouped by taxon.
        var candidates = new Dictionary<long, List<CommonNameCandidate>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT t.primary_source_id, cn.raw_name, cn.normalized_name, cn.source, cn.is_preferred
                FROM common_names cn
                JOIN taxa t ON t.id = cn.taxon_id
                WHERE cn.language = 'en' AND t.primary_source = 'iucn'
                ORDER BY cn.id
                """;
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (!long.TryParse(reader.GetString(0), out var taxonId) || !taxa.TryGetValue(taxonId, out var taxon)) {
                    continue;
                }
                var raw = reader.GetString(1);
                var source = reader.GetString(3);
                var preferred = reader.GetInt64(4) == 1;
                if (!candidates.TryGetValue(taxonId, out var list)) {
                    candidates[taxonId] = list = new List<CommonNameCandidate>();
                }
                list.Add(new CommonNameCandidate(raw, reader.GetString(2), source, preferred));
                if (SiteSource(source) is { } siteSource) {
                    // Only IUCN's own main name is marked preferred on the site.
                    taxon.EnglishNames.Add((raw, siteSource, siteSource == SiteNameSource.Iucn && preferred));
                }
            }
        }

        foreach (var (taxonId, list) in candidates) {
            var best = CommonNameStore.ChooseBest(list, ambiguous);
            if (best is null) {
                continue;
            }
            var name = CommonNameNormalizer.ApplyCapitalization(SiteBuildRules.CleanName(best.RawName), capsRules);
            if (!string.IsNullOrWhiteSpace(name)) {
                taxa[taxonId].CommonNameEn = name.Trim();
                stats.CommonNameEn++;
            }
        }

        // Catalogue of Life synonyms.
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT t.primary_source_id, s.original_name
                FROM scientific_name_synonyms s
                JOIN taxa t ON t.id = s.taxon_id
                WHERE s.source = 'col' AND s.synonym_type = 'synonym' AND t.primary_source = 'iucn'
                ORDER BY s.id
                """;
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (long.TryParse(reader.GetString(0), out var taxonId) && taxa.TryGetValue(taxonId, out var taxon)) {
                    taxon.ColSynonyms.Add(reader.GetString(1));
                }
            }
        }

        // Catalogue of Life ids; the first one recorded when a taxon has several.
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT t.primary_source_id, x.source_identifier
                FROM taxon_cross_references x
                JOIN taxa t ON t.id = x.taxon_id
                WHERE x.source = 'col' AND t.primary_source = 'iucn'
                ORDER BY x.id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (long.TryParse(reader.GetString(0), out var taxonId) && taxa.ContainsKey(taxonId)
                    && SiteBuildRules.NullIfBlank(reader.GetString(1)) is { } colId) {
                    colCrossReferences.TryAdd(taxonId, colId);
                }
            }
        }
    }

    /// The site's source for a common names store source; null for a source the site does not show.
    public static string? SiteSource(string storeSource) => storeSource.Trim().ToLowerInvariant() switch {
        "wikipedia_title" or "wikipedia_taxobox" => SiteNameSource.Wikipedia,
        "wikidata_label" or "wikidata" => SiteNameSource.Wikidata,
        "col" => SiteNameSource.Col,
        "iucn" => SiteNameSource.Iucn,
        _ => null,
    };
}
