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
    public static (string? NatureServeFetched, string? EcosFetched, string? NztcsFetched) ReadStatusLists(string path,
        IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var index = new StatusListNameIndex(taxa.Values);
        string? natureServe = null, ecos = null, nztcs = null;
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
        if (DelimitedTableImporter.GetTableColumns(connection, "salve_assessment") is not null) {
            ReadSalve(connection, index, stats, cancellationToken);
            stats.SalveFetched = SourceFetched(connection, OtherStatusSources.Salve);
        } else {
            stats.Warnings.Add($"The status lists store {path} has no SALVE assessments: run statuses salve-import.");
        }
        if (DelimitedTableImporter.GetTableColumns(connection, "nztcs_assessment") is not null) {
            ReadNztcs(connection, index, stats, cancellationToken);
            nztcs = SourceFetched(connection, OtherStatusSources.Nztcs);
        } else {
            stats.Warnings.Add($"The status lists store {path} has no NZTCS assessments: run statuses nztcs-import.");
        }
        return (natureServe, ecos, nztcs);
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

        var matches = StatusListMatcher.OnePerTaxon(records, index, r => StatusListNameIndex.Kingdom(r.Kingdom),
            r => r.ScientificName, r => synonyms.GetValueOrDefault(r.ElementGlobalId));
        stats.NatureServeByName = matches.Count(m => m.Kind == StatusListMatchKind.Name);
        stats.NatureServeBySynonym = matches.Count(m => m.Kind == StatusListMatchKind.OtherName);
        stats.NatureServeByIucnSynonym = matches.Count(m => m.Kind == StatusListMatchKind.IucnSynonym);

        foreach (var (taxon, record, _) in matches) {
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

    // NZTCS: each current assessment with a scientific name (the store leaves informal names out)
    // goes to the one taxon with that name in any kingdom (NZTCS gives no kingdom), else the one
    // taxon whose IUCN synonyms include it. A taxon gets one assessment: matches by name first.
    private static void ReadNztcs(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var rows = new List<(long Id, string Name, string Status, string? Report)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT assessment_id, scientific_name, category, status, report_name
                FROM nztcs_assessment
                WHERE scientific_name IS NOT NULL
                ORDER BY assessment_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                if (StatusLists.NztcsApi.StatusText(Text(reader, 2), Text(reader, 3)) is { } status) {
                    rows.Add((reader.GetInt64(0), reader.GetString(1), status, Text(reader, 4)));
                }
            }
        }
        stats.NztcsAssessments = rows.Count;
        var matches = StatusListMatcher.OnePerTaxon(rows, index, _ => null, r => r.Name);
        foreach (var (taxon, row, _) in matches) {
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Nztcs, row.Status, null,
                SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), null, OtherStatusSources.Nztcs,
                row.Id.ToString(CultureInfo.InvariantCulture), StatusLists.NztcsApi.AssessmentUrl(row.Id), null, row.Report));
        }
        stats.NztcsMatched = matches.Count;
    }

    // SALVE: each current assessment goes to the animal taxon with its name, else the one animal
    // taxon whose IUCN synonyms include it; one assessment per taxon, matches by name first.
    private static void ReadSalve(SqliteConnection connection, StatusListNameIndex index, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        var rows = new List<(string Id, string Name, string Code, string Status, string? AssessedOn, string? Doi)>();
        using (var command = connection.CreateCommand()) {
            command.CommandText = """
                SELECT ficha_id, scientific_name, category, possibly_extinct, assessed_on, doi
                FROM salve_assessment
                ORDER BY ficha_id
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var possiblyExtinct = reader.GetInt64(3) == 1;
                if (OtherStatusSystems.SalveLabel(reader.GetString(2), possiblyExtinct) is { } status) {
                    var code = reader.GetString(2).ToUpperInvariant() + (possiblyExtinct && reader.GetString(2) == "CR" ? "(PE)" : "");
                    rows.Add((reader.GetString(0), reader.GetString(1), code, status, Text(reader, 4), Text(reader, 5)));
                }
            }
        }
        stats.SalveAssessments = rows.Count;
        var matches = StatusListMatcher.OnePerTaxon(rows, index, _ => "ANIMALIA", r => r.Name);
        foreach (var (taxon, row, _) in matches) {
            taxon.OtherStatuses.Add(new OtherStatus(OtherStatusSystems.Salve, row.Status, row.Code,
                SiteBuildRules.OtherListedName(row.Name, taxon.ScientificName), null, OtherStatusSources.Salve, row.Id,
                StatusLists.SalveApi.AssessmentUrl(row.Id, row.Doi), row.AssessedOn));
        }
        stats.SalveMatched = matches.Count;
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
