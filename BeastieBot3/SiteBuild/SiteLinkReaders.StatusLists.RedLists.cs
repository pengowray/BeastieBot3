using System.Globalization;
using System.Text.RegularExpressions;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // A list that replaces another for the taxa both cover: Luxembourg's bryophyte update of 2008 over
    // the list of 2003.
    private static readonly Dictionary<string, string> RedListReplaces = new(StringComparer.Ordinal) {
        ["lu-bryophytes-2008"] = "lu-bryophytes-2003",
    };

    // National and subnational red lists from GBIF (`statuses red-lists-import`): per list, each taxon
    // with a status goes to the site taxon with its canonical name in its kingdom, else to the taxon of
    // one of the list's synonyms for it or its accepted name, else of an IUCN synonym; one row per site
    // taxon and list. A taxon with several statuses in one list (Ecuador's birds: the mainland and the
    // Galápagos) gets one row per status, with the locality as the area it applies to. Each list is an
    // other_status_list row, named in English with its year. Returns the lists.
    // skipCountries: countries whose lists another source gives (France's, from the BDC Statuts).
    private static List<OtherStatusList> ReadRedLists(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        IReadOnlyCollection<string> skipCountries, CancellationToken cancellationToken) {
        var datasets = new Dictionary<string, OtherStatusList>(StringComparer.Ordinal);
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT dataset_key, list_name, list_name_en, list_year, publisher, country_code, region, licence, citation, fetched_at, gbif_dataset_key
                FROM red_list_dataset
                ORDER BY dataset_key
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var key = reader.GetString(0);
                var original = reader.GetString(1);
                var english = Text(reader, 2) ?? original;
                var year = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
                var name = year is { } y && !english.Contains(y.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal) ? $"{english} ({y})" : english;
                var fetched = Text(reader, 9) is { } at && StoredUtc.Parse(at) is { } utc ? utc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) : null;
                datasets[key] = new OtherStatusList("redlist:" + key, OtherStatusSystems.NationalRedList, reader.GetString(5), Text(reader, 6),
                    name, original == english ? null : original, 0, reader.GetString(4), Text(reader, 7), LicenceUrl(Text(reader, 7)),
                    Text(reader, 8), "https://www.gbif.org/dataset/" + reader.GetString(10), year?.ToString(CultureInfo.InvariantCulture), fetched);
            }
        }
        var taxa = new Dictionary<(string Dataset, string Id), RedListTaxon>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT dataset_key, taxon_id, seq, canonical_name, accepted_name, kingdom, threat_status, iucn_code, status_label, locality, url
                FROM red_list_taxon
                WHERE canonical_name IS NOT NULL
                ORDER BY dataset_key, taxon_id, seq
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var key = (reader.GetString(0), reader.GetString(1));
                if (!taxa.TryGetValue(key, out var taxon)) {
                    taxa[key] = taxon = new RedListTaxon(key.Item1, key.Item2, reader.GetString(3), Text(reader, 5), new List<string>(), new List<RedListStatus>());
                    if (Text(reader, 4) is { } accepted && StatusLists.RedListArchiveReader.CanonicalName(accepted, null, null) is { } acceptedName) {
                        taxon.OtherNames.Add(acceptedName);
                    }
                }
                taxon.Statuses.Add(new RedListStatus(reader.GetString(6), Text(reader, 7), Text(reader, 8), Text(reader, 9), Text(reader, 10)));
            }
        }
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT dataset_key, accepted_taxon_id, canonical_name FROM red_list_synonym WHERE canonical_name IS NOT NULL";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (taxa.TryGetValue((reader.GetString(0), reader.GetString(1)), out var taxon)) {
                    taxon.OtherNames.Add(reader.GetString(2));
                }
            }
        }
        stats.RedListStatuses = taxa.Values.Sum(t => t.Statuses.Count);

        var lists = new Dictionary<string, OtherStatusList>(StringComparer.Ordinal);
        var covered = new Dictionary<string, HashSet<long>>(StringComparer.Ordinal);
        // A list that replaces another is read first, so the other knows which taxa to leave out.
        var order = taxa.Values.GroupBy(t => t.Dataset).OrderBy(g => RedListReplaces.ContainsKey(g.Key) ? 0 : 1).ThenBy(g => g.Key, StringComparer.Ordinal);
        foreach (var dataset in order) {
            if (!datasets.TryGetValue(dataset.Key, out var list) || (list.Country is { } country && skipCountries.Contains(country))) {
                continue;
            }
            var replacedBy = RedListReplaces.FirstOrDefault(p => p.Value == dataset.Key).Key;
            var matches = StatusListMatcher.OnePerTaxon(dataset, index, t => StatusListNameIndex.Kingdom(t.Kingdom), t => t.Name, t => t.OtherNames);
            var taken = covered[dataset.Key] = new HashSet<long>();
            foreach (var (taxon, record, _) in matches) {
                if (replacedBy is not null && covered.GetValueOrDefault(replacedBy)?.Contains(taxon.TaxonId) == true) {
                    continue;
                }
                taken.Add(taxon.TaxonId);
                var several = record.Statuses.Count > 1;
                foreach (var status in record.Statuses) {
                    var (text, own) = RedListStatusText(status);
                    var population = several ? status.Locality : list.Region;
                    taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.NationalRedList, text, status.IucnCode,
                        SiteBuildRules.OtherListedName(record.Name, taxon.ScientificName), population, OtherStatusSources.RedLists,
                        record.Dataset + ":" + record.TaxonId, RedListUrl(record, status), null, ListKey: list.ListKey, Qualifier: own));
                    stats.RedListRows++;
                }
                lists.TryAdd(list.ListKey, list);
            }
            stats.RedListMatched += taken.Count;
        }
        return lists.Values.ToList();
    }

    // The status as the site shows it, and the list's own code or word when it is not that text. A
    // status that is an IUCN code ("VU") is the category's English name; any other ("CR*", Germany's
    // "3", Ukraine's "вразливий") is the list's English label ("Critically Endangered (presumed extinct)",
    // "Threatened (Gefährdet)"), else the category's name, else the status as written.
    private static (string Text, string? Own) RedListStatusText(RedListStatus status) {
        var iucn = status.IucnCode is { } code ? IucnCategoryName(code) : null;
        if (iucn is not null && status.ThreatStatus == status.IucnCode) {
            return (iucn, null);
        }
        var text = status.Label ?? iucn ?? status.ThreatStatus;
        return (text, string.Equals(status.ThreatStatus, text, StringComparison.OrdinalIgnoreCase) ? null : status.ThreatStatus);
    }

    private static string? IucnCategoryName(string code) => code switch {
        "EX" => "Extinct",
        "EW" => "Extinct in the Wild",
        "RE" => "Regionally Extinct",
        "CR" => "Critically Endangered",
        "EN" => "Endangered",
        "VU" => "Vulnerable",
        "NT" => "Near Threatened",
        "LC" => "Least Concern",
        "DD" => "Data Deficient",
        "NA" => "Not Applicable",
        _ => null,
    };

    // The taxon's page at the publisher when the archive gives one; Sweden's taxa at Artfakta by their
    // Dyntaxa id; else none (the note links the dataset).
    private static string? RedListUrl(RedListTaxon taxon, RedListStatus status) {
        if (status.Url is { } url && url.StartsWith("http", StringComparison.Ordinal)) {
            return url;
        }
        var dyntaxa = Regex.Match(taxon.TaxonId, @"^urn:lsid:dyntaxa\.se:Taxon:(\d+)$");
        return dyntaxa.Success ? "https://artfakta.se/taxa/" + dyntaxa.Groups[1].Value : null;
    }

    private static string? LicenceUrl(string? licence) => licence switch {
        "CC0 1.0" => "https://creativecommons.org/publicdomain/zero/1.0/",
        "CC BY 4.0" => "https://creativecommons.org/licenses/by/4.0/",
        "CC BY-NC 4.0" => "https://creativecommons.org/licenses/by-nc/4.0/",
        _ => null,
    };

    private sealed record RedListTaxon(string Dataset, string TaxonId, string Name, string? Kingdom, List<string> OtherNames,
        List<RedListStatus> Statuses);

    private sealed record RedListStatus(string ThreatStatus, string? IucnCode, string? Label, string? Locality, string? Url);
}
