using System;
using System.Collections.Generic;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The columns of an imported sprat_species table, read once with PRAGMA table_info. A SPRAT report
// may lack some columns: SpratImporter names a column it does not map (SpratColumns.HeaderMap) from
// the report's own header text, so a header IUCN renames or drops changes the table. Readers select
// such a column through Select, which gives NULL when the table does not have it. A missing column
// cannot be selected by name: Microsoft.Data.Sqlite's SQLite is built with SQLITE_DQS=0, so a
// double-quoted name that is not a column fails the statement with "no such column" (older builds
// read it as a string literal instead). Used by SpratListQueryService and by `site build-db`.

namespace BeastieBot3.Sprat;

internal sealed class SpratTableColumns {
    private readonly IReadOnlySet<string> _columns;

    private SpratTableColumns(IReadOnlySet<string> columns) => _columns = columns;

    /// A table with no columns: Has is always false and Select always gives NULL.
    public static SpratTableColumns None { get; } = new(new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    /// The columns of SpratColumns.Table, or null when the database has no such table.
    public static SpratTableColumns? Read(SqliteConnection connection) =>
        DelimitedTableImporter.GetTableColumns(connection, SpratColumns.Table) is { } columns ? new SpratTableColumns(columns) : null;

    /// Whether the table has the column (letter case ignored, as SQLite does).
    public bool Has(string column) => _columns.Contains(column);

    /// The column's name quoted for SQL, or NULL when the table does not have it.
    public string Select(string column) => Has(column) ? Quote(column) : "NULL";

    public static string Quote(string identifier) => DelimitedTableImporter.QuoteIdentifier(identifier);
}
