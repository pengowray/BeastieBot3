using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// The status lists store for `site build-db`: NatureServe's global ranks with the COSEWIC and SARA
// statuses it records (`statuses natureserve-fetch`), the US Endangered Species Act listings in
// ECOS (`statuses ecos-import`), the New Zealand Threat Classification System assessments
// (`statuses nztcs-import`) and SALVE's assessments of Brazil's fauna (`statuses salve-import`).
// The reader of each source is in SiteLinkReaders.StatusLists.<Source>.cs.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    // ------------------------------------------------------------ status lists

    /// Adds each matched taxon's NatureServe, COSEWIC, SARA, ESA, SALVE and NZTCS rows to its
    /// OtherStatuses. Returns the dates NatureServe, ECOS and NZTCS were last downloaded, and sets
    /// stats.SalveFetched to SALVE's (yyyy-MM-dd; null when the store's status_source table has no
    /// row for the source). A store without a source's tables gives no rows of that source.
    public static (string? NatureServeFetched, string? EcosFetched, string? NztcsFetched) ReadStatusLists(string path,
        IReadOnlyDictionary<long, SiteTaxon> taxa, SiteBuildStats stats, List<OtherStatusList> lists, CancellationToken cancellationToken) {
        using var connection = OpenReadOnly(path);
        var index = new StatusListNameIndex(taxa.Values);
        string? natureServe = null, ecos = null, nztcs = null;
        if (DelimitedTableImporter.GetTableColumns(connection, "cites_listing") is not null) {
            ReadCites(connection, index, taxa.Values, stats, cancellationToken);
        } else {
            stats.Warnings.Add($"The status lists store {path} has no CITES listings: run statuses cites-import.");
        }
        if (DelimitedTableImporter.GetTableColumns(connection, "red_list_taxon") is not null) {
            lists.AddRange(ReadRedLists(connection, index, stats, cancellationToken));
        } else {
            stats.Warnings.Add($"The status lists store {path} has no national red lists: run statuses red-lists-import.");
        }
        if (DelimitedTableImporter.GetTableColumns(connection, "jncc_designation") is not null) {
            lists.AddRange(ReadJncc(connection, index, stats, cancellationToken));
        } else {
            stats.Warnings.Add($"The status lists store {path} has no JNCC designations: run statuses jncc-import.");
        }
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
}
