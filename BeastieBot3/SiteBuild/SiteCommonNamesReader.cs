using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists.Legacy;
using Microsoft.Data.Sqlite;

// Reads the common names store (common_names.sqlite) for `site build-db`, read-only:
//   - every English common name of each IUCN taxon, with its source mapped to the site's sources
//     (wikipedia_title and wikipedia_taxobox -> wikipedia, wikidata_label and wikidata -> wikidata,
//     col -> col, iucn -> iucn);
//   - the best English name, chosen by CommonNameChooser as the Wikipedia lists choose it: a
//     manual override in rules-list.txt ("Panthera leo = lion") first, else the best of the
//     taxon's names (names ambiguous for the taxon skipped, by the store's taxa.id, as
//     AmbiguousNames decides) capitalised with the store's caps rules; and dropped when the lists
//     would drop it (CommonNameChooser.IsUnusable: the scientific name again, or a working name);
//   - Catalogue of Life synonyms (synonym_type 'synonym'; 'ambiguous_synonym' rows and the
//     constructed rank variants are left out; IUCN's synonyms come from the API records instead);
//   - Catalogue of Life ids from taxon_cross_references, the fallback for col_id.
// The store's taxa are keyed by primary_source 'iucn' and primary_source_id = IUCN taxon id.

namespace BeastieBot3.SiteBuild;

internal static class SiteCommonNamesReader {
    public static void Read(string path, IReadOnlyDictionary<long, SiteTaxon> taxa, LegacyTaxaRuleList? overrides,
        SiteBuildStats stats, Dictionary<long, string> colCrossReferences, CancellationToken cancellationToken) {
        using var store = CommonNameStore.OpenReadOnly(path);
        var chooser = CommonNameChooser.ForStore(store, overrides, tidyRawName: SiteBuildRules.CleanName);

        using var connection = SiteIucnCsvReader.OpenReadOnly(path);

        // English names, grouped by IUCN taxon id, with the store's taxa.id the ambiguity verdicts use.
        var candidates = new Dictionary<long, (long StoreTaxonId, List<CommonNameCandidate> Names)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT t.primary_source_id, cn.raw_name, cn.normalized_name, cn.source, cn.is_preferred, t.id
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
                if (!candidates.TryGetValue(taxonId, out var entry)) {
                    candidates[taxonId] = entry = (reader.GetInt64(5), new List<CommonNameCandidate>());
                }
                entry.Names.Add(new CommonNameCandidate(raw, reader.GetString(2), source, preferred));
                if (SiteSource(source) is { } siteSource) {
                    // Only IUCN's own main name is marked preferred on the site.
                    taxon.EnglishNames.Add((raw, siteSource, siteSource == SiteNameSource.Iucn && preferred));
                }
            }
        }

        foreach (var taxon in taxa.Values) {
            var subject = new CommonNameSubject(taxon.ScientificName, taxon.ScientificName, taxon.Genus, taxon.SpeciesEpithet);
            Func<string?>? storeName = candidates.TryGetValue(taxon.TaxonId, out var entry)
                ? () => chooser.FromStore(entry.StoreTaxonId, entry.Names)?.DisplayName
                : null;
            var choice = chooser.Choose(subject, storeName);
            switch (choice.Kind) {
                case CommonNameChoiceKind.None:
                    continue;
                case CommonNameChoiceKind.Unusable:
                    // The lists show no common name when the best one is the scientific name again
                    // or a working name ("sp. nov.", an authority with a year); neither does the site.
                    stats.CommonNameEnUnusable++;
                    continue;
            }
            taxon.CommonNameEn = choice.Name!.Trim();
            stats.CommonNameEn++;
            if (choice.Kind == CommonNameChoiceKind.Rules) {
                stats.CommonNameEnFromRules++;
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
