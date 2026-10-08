using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The status lists store for `site build-db`: NatureServe's global ranks with the COSEWIC and SARA
// statuses it records (`statuses natureserve-fetch`), and the US Endangered Species Act listings in
// ECOS (`statuses ecos-import`).

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ status lists

    /// Adds each matched taxon's NatureServe, COSEWIC, SARA and ESA rows to its OtherStatuses;
    /// returns the dates the store's two sources were last downloaded (yyyy-MM-dd, null when the store
    /// has no rows of that source). A store without a source's tables gives no rows of that source.
    public static (string? NatureServeFetched, string? EcosFetched) ReadStatusLists(string path,
        IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var index = new StatusListNameIndex(taxa.Values);
        string? natureServe = null, ecos = null;
        if (DelimitedTableImporter.GetTableColumns(connection, "natureserve_species") is not null) {
            ReadNatureServe(connection, index, stats, cancellationToken);
            natureServe = SourceFetched(connection, OtherStatusSources.NatureServe);
        } else {
            stats.Warnings.Add($"The status lists store {path} has no NatureServe records: run statuses natureserve-fetch.");
        }
        if (DelimitedTableImporter.GetTableColumns(connection, "ecos_listing") is not null) {
            ReadEcos(connection, index, stats, cancellationToken);
            ecos = SourceFetched(connection, OtherStatusSources.Ecos);
        } else {
            stats.Warnings.Add($"The status lists store {path} has no ECOS listings: run statuses ecos-import.");
        }
        return (natureServe, ecos);
    }

    // NatureServe: each record goes to the taxon with its scientific name, else the one taxon that one
    // of its NatureServe synonyms names, else the one taxon whose IUCN synonyms include its name. A
    // taxon gets one record: Standard records are tried before Provisional and Nonstandard ones, and
    // a match by name before any match by a synonym.
    private static void ReadNatureServe(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var records = new List<NatureServeRecord>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT element_global_id, scientific_name, kingdom, g_rank, rounded_g_rank, cosewic_code, sara_code, nsx_url
                FROM natureserve_species
                ORDER BY CASE classification_status WHEN 'Standard' THEN 0 ELSE 1 END, element_global_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                records.Add(new NatureServeRecord(reader.GetInt64(0), reader.GetString(1), Text(reader, 2), Text(reader, 3),
                    Text(reader, 4), Text(reader, 5), Text(reader, 6), reader.GetString(7)));
            }
        }
        var synonyms = new Dictionary<long, List<string>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT element_global_id, name FROM natureserve_synonym";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var id = reader.GetInt64(0);
                if (!synonyms.TryGetValue(id, out var list)) {
                    synonyms[id] = list = new List<string>();
                }
                list.Add(reader.GetString(1));
            }
        }
        stats.NatureServeRecords = records.Count;

        var matched = new Dictionary<long, NatureServeRecord>();
        var unmatched = new List<NatureServeRecord>();
        foreach (var record in records) {
            var kingdom = StatusListNameIndex.Kingdom(record.Kingdom);
            if (index.Find(kingdom, record.ScientificName) is { } taxon) {
                if (matched.TryAdd(taxon.TaxonId, record)) {
                    stats.NatureServeByName++;
                }
            } else {
                unmatched.Add(record);
            }
        }
        foreach (var record in unmatched) {
            var kingdom = StatusListNameIndex.Kingdom(record.Kingdom);
            var taxon = synonyms.TryGetValue(record.ElementGlobalId, out var names)
                ? names.Select(n => index.Find(kingdom, n)).FirstOrDefault(t => t is not null && !matched.ContainsKey(t.TaxonId))
                : null;
            if (taxon is not null) {
                matched[taxon.TaxonId] = record;
                stats.NatureServeBySynonym++;
            } else if (index.FindByIucnSynonym(kingdom, record.ScientificName) is { } bySynonym && !matched.ContainsKey(bySynonym.TaxonId)) {
                matched[bySynonym.TaxonId] = record;
                stats.NatureServeByIucnSynonym++;
            }
        }

        foreach (var (taxonId, record) in matched) {
            var taxon = index.Taxon(taxonId);
            var sourceId = record.ElementGlobalId.ToString(CultureInfo.InvariantCulture);
            var listedName = SiteBuildRules.OtherListedName(record.ScientificName, taxon.ScientificName);
            if (OtherStatusSystems.NatureServeRankMeaning(record.RoundedGRank) is not null) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.NatureServeGlobal, record.GRank ?? record.RoundedGRank!,
                    record.RoundedGRank, listedName, null, OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.NatureServeRanks++;
            }
            if (OtherStatusSystems.CosewicLabel(record.CosewicCode) is { } cosewic) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Cosewic, cosewic, null, listedName, null,
                    OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.CosewicStatuses++;
            }
            if (SiteBuildRules.NullIfBlank(record.SaraCode) is { } sara) {
                taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Sara, sara, null, listedName, null,
                    OtherStatusSources.NatureServe, sourceId, record.Url, null));
                stats.SaraStatuses++;
            }
        }
    }

    // ECOS: each listing goes to the taxon with its scientific name, else the one taxon that another
    // name its brackets give names ("Papasula (=Sula) abbotti" gives Sula abbotti), else the one taxon
    // whose IUCN synonyms include its name. A taxon can get several listings: one per population.
    private static void ReadEcos(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var names = new Dictionary<long, List<string>>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = "SELECT entity_id, name FROM ecos_name";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var id = reader.GetInt64(0);
                if (!names.TryGetValue(id, out var list)) {
                    names[id] = list = new List<string>();
                }
                list.Add(reader.GetString(1));
            }
        }
        using var listings = connection.CreateCommand();
        listings.CommandText = """
            SELECT entity_id, scientific_name, kingdom, status, entity_description, listing_date, url
            FROM ecos_listing
            ORDER BY entity_id
            """;
        using var row = listings.ExecuteReader();
        while (row.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            stats.EcosListings++;
            var entityId = row.GetInt64(0);
            var scientificName = row.GetString(1);
            var kingdom = StatusListNameIndex.Kingdom(Text(row, 2));
            var taxon = index.Find(kingdom, scientificName)
                ?? (names.TryGetValue(entityId, out var others) ? others.Select(n => index.Find(kingdom, n)).FirstOrDefault(t => t is not null) : null)
                ?? index.FindByIucnSynonym(kingdom, scientificName);
            if (taxon is null) {
                continue;
            }
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Esa, SiteBuildRules.CollapseWhitespace(row.GetString(3)), null,
                SiteBuildRules.OtherListedName(scientificName, taxon.ScientificName), SiteBuildRules.EcosAppliesTo(Text(row, 4)),
                OtherStatusSources.Ecos, entityId.ToString(CultureInfo.InvariantCulture), row.GetString(6), Text(row, 5)));
            stats.EcosMatched++;
        }
    }

    // status_source.fetched_at as yyyy-MM-dd; null when the source has no row.
    private static string? SourceFetched(SqliteConnection connection, string source) {
        if (DelimitedTableImporter.GetTableColumns(connection, "status_source") is null) {
            return null;
        }
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT fetched_at FROM status_source WHERE source = @source";
        command.Parameters.AddWithValue("@source", source);
        return command.ExecuteScalar() is string fetched && StoredUtc.Parse(fetched) is { } at
            ? at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;
    }

    private static string? Text(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : SiteBuildRules.NullIfBlank(reader.GetString(ordinal));

    private sealed record NatureServeRecord(long ElementGlobalId, string ScientificName, string? Kingdom, string? GRank,
        string? RoundedGRank, string? CosewicCode, string? SaraCode, string Url);
}

/// Finds the taxon of a scientific name in one kingdom, for the status lists. Names are compared
/// with spaces collapsed and without rank markers ("ssp.", "subsp.", "var."), so NatureServe's
/// "Lithobates areolatus circulosus" finds IUCN's "Lithobates areolatus ssp. circulosus". A taxon in
/// the release is found before an old IUCN id with the same name; a name that two taxa in the release
/// have (or, when none is in the release, two other taxa) finds none. Subpopulations, which have
/// their species' scientific name, are never found.
internal sealed class StatusListNameIndex {
    private readonly Dictionary<(string Kingdom, string Name), SiteTaxon?> _byName = new();
    private readonly Dictionary<(string Kingdom, string Name), SiteTaxon?> _byIucnSynonym = new();
    private readonly Dictionary<long, SiteTaxon> _taxa = new();

    public StatusListNameIndex(IEnumerable<SiteTaxon> taxa) {
        // Taxa in the release first: a later taxon with the same key only makes the key ambiguous
        // when it is in the release too.
        var released = new HashSet<(string, string)>();
        var releasedSynonyms = new HashSet<(string, string)>();
        foreach (var taxon in taxa.Where(t => t.Kind != SiteTaxonKind.Subpopulation).OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            _taxa[taxon.TaxonId] = taxon;
            var kingdom = taxon.Kingdom?.Trim().ToUpperInvariant() ?? string.Empty;
            Add(_byName, released, (kingdom, Key(taxon.ScientificName)), taxon);
            foreach (var synonym in taxon.IucnSynonyms) {
                Add(_byIucnSynonym, releasedSynonyms, (kingdom, Key(synonym.Name)), taxon);
            }
        }
    }

    private static void Add(Dictionary<(string, string), SiteTaxon?> map, HashSet<(string, string)> released,
        (string, string) key, SiteTaxon taxon) {
        if (!map.TryGetValue(key, out var existing)) {
            map[key] = taxon;
            if (taxon.InRelease) {
                released.Add(key);
            }
        } else if (existing is not null && existing.TaxonId != taxon.TaxonId && (taxon.InRelease || !released.Contains(key))) {
            map[key] = null;
        }
    }

    public SiteTaxon Taxon(long taxonId) => _taxa[taxonId];

    /// The taxon with this scientific name in this kingdom (IUCN's spelling: "ANIMALIA"); with no
    /// kingdom, the one taxon with the name in any kingdom.
    public SiteTaxon? Find(string? kingdom, string name) => Find(_byName, kingdom, name);

    /// The one taxon whose IUCN synonyms include this name.
    public SiteTaxon? FindByIucnSynonym(string? kingdom, string name) => Find(_byIucnSynonym, kingdom, name);

    private static SiteTaxon? Find(Dictionary<(string Kingdom, string Name), SiteTaxon?> map, string? kingdom, string name) {
        var key = Key(name);
        if (kingdom is not null) {
            return map.GetValueOrDefault((kingdom, key));
        }
        SiteTaxon? found = null;
        foreach (var k in Kingdoms) {
            if (map.TryGetValue((k, key), out var taxon)) {
                if (taxon is null || found is not null) {
                    return null;
                }
                found = taxon;
            }
        }
        return found;
    }

    private static readonly string[] Kingdoms = { "ANIMALIA", "PLANTAE", "FUNGI", "CHROMISTA", "PROTISTA", "BACTERIA" };

    /// IUCN's kingdom for a status list's kingdom ("Animalia", "Animal", "Plant"); null when unknown.
    public static string? Kingdom(string? kingdom) => kingdom?.Trim().ToUpperInvariant() switch {
        "ANIMALIA" or "ANIMAL" => "ANIMALIA",
        "PLANTAE" or "PLANT" => "PLANTAE",
        "FUNGI" or "FUNGUS" => "FUNGI",
        "CHROMISTA" => "CHROMISTA",
        _ => null,
    };

    internal static string Key(string name) =>
        string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries).Where(w => w is not ("ssp." or "subsp." or "var.")));
}
