using Microsoft.Data.Sqlite;

namespace BeastieBot3.Site.Data;

/// Column reads shared by the site's queries. Import with `using static`.
internal static class ReaderValues {
    /// The column's text, or null when it is NULL.
    public static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
