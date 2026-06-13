using System;
using System.Data;
using System.IO;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;

// Derived "projection" SQLite store that re-shapes the IUCN API cache (raw JSON)
// into the same relational form as the CSV-imported main DB, so Wikipedia list /
// chart generation can run on either dataset unchanged. Produces:
//   import_metadata (with redlist_version, read by IucnListQueryService/IucnChartDataBuilder)
//   taxonomy_html, assessments_html  (CSV-compatible column names)
//   view_assessments_html_taxonomy_html  (via the shared IucnViewBuilder)
// Built by `iucn api project-view` from the latest cached assessments.
// Follows the standard Store.Open() pattern (private ctor + static factory, WAL).

namespace BeastieBot3.Iucn;

internal sealed class IucnApiProjectionStore : SqliteStore {
    private IucnApiProjectionStore(SqliteConnection connection) : base(connection) {
    }

    public static IucnApiProjectionStore Open(string databasePath) {
        var connection = OpenConnection(databasePath, foreignKeys: false);
        var store = new IucnApiProjectionStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS import_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    redlist_version TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT
);
CREATE TABLE IF NOT EXISTS taxonomy_html (
    import_id INTEGER NOT NULL,
    taxonId INTEGER NOT NULL,
    scientificName TEXT,
    kingdomName TEXT, phylumName TEXT, className TEXT, orderName TEXT,
    familyName TEXT, genusName TEXT, speciesName TEXT
);
CREATE TABLE IF NOT EXISTS assessments_html (
    import_id INTEGER NOT NULL,
    taxonId INTEGER NOT NULL,
    assessmentId INTEGER NOT NULL,
    scientificName TEXT,
    redlistCategory TEXT,
    yearPublished TEXT,
    systems TEXT,
    possiblyExtinct TEXT,
    possiblyExtinctInTheWild TEXT,
    scopes TEXT,
    infraType TEXT, infraName TEXT, infraAuthority TEXT,
    subpopulationName TEXT, authority TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_proj_taxonomy_uniq_taxonId ON taxonomy_html(taxonId);
CREATE INDEX IF NOT EXISTS idx_proj_taxonomy_hierarchy ON taxonomy_html(kingdomName,phylumName,className,orderName,familyName,genusName,speciesName);
CREATE INDEX IF NOT EXISTS idx_proj_assessments_taxonId ON assessments_html(taxonId);
CREATE UNIQUE INDEX IF NOT EXISTS idx_proj_assessments_uniq_assessmentId ON assessments_html(assessmentId);
CREATE INDEX IF NOT EXISTS idx_proj_assessments_category ON assessments_html(redlistCategory);
CREATE INDEX IF NOT EXISTS idx_proj_assessments_scopes ON assessments_html(scopes);
";
        cmd.ExecuteNonQuery();
    }

    /// <summary>Clears all projected rows so a build starts from a clean slate.</summary>
    public void ResetData() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM assessments_html; DELETE FROM taxonomy_html; DELETE FROM import_metadata;";
        cmd.ExecuteNonQuery();
    }

    public long InsertImport(string filename, string redlistVersion) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "INSERT INTO import_metadata(filename, redlist_version, started_at) VALUES (@f, @v, @s); SELECT last_insert_rowid();";
        cmd.Parameters.AddWithValue("@f", filename);
        cmd.Parameters.AddWithValue("@v", redlistVersion);
        cmd.Parameters.AddWithValue("@s", DateTime.UtcNow.ToString("O"));
        return (long)(cmd.ExecuteScalar() ?? 0L);
    }

    public void CompleteImport(long importId) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "UPDATE import_metadata SET ended_at=@e WHERE id=@id";
        cmd.Parameters.AddWithValue("@e", DateTime.UtcNow.ToString("O"));
        cmd.Parameters.AddWithValue("@id", importId);
        cmd.ExecuteNonQuery();
    }

    public void BuildView() =>
        IucnViewBuilder.RecreateJoinView(_connection, "view_assessments_html_taxonomy_html", "assessments_html", "taxonomy_html");

    /// <summary>Opens a batched writer (single transaction + prepared statements).</summary>
    public ProjectionWriter BeginWrite() => new(_connection);

    // Streaming writer: one transaction, prepared INSERT OR IGNORE statements so the
    // unique indexes on taxonId / assessmentId dedupe automatically.
    public sealed class ProjectionWriter : IDisposable {
        private readonly SqliteTransaction _tx;
        private readonly SqliteCommand _taxonomy;
        private readonly SqliteCommand _assessment;
        private bool _committed;

        internal ProjectionWriter(SqliteConnection connection) {
            _tx = connection.BeginTransaction();

            _taxonomy = connection.CreateCommand();
            _taxonomy.Transaction = _tx;
            _taxonomy.CommandText = @"INSERT OR IGNORE INTO taxonomy_html
(import_id, taxonId, scientificName, kingdomName, phylumName, className, orderName, familyName, genusName, speciesName)
VALUES (@import,@taxonId,@sci,@kingdom,@phylum,@class,@order,@family,@genus,@species)";
            AddParams(_taxonomy, "@import", "@taxonId", "@sci", "@kingdom", "@phylum", "@class", "@order", "@family", "@genus", "@species");

            _assessment = connection.CreateCommand();
            _assessment.Transaction = _tx;
            _assessment.CommandText = @"INSERT OR IGNORE INTO assessments_html
(import_id, taxonId, assessmentId, scientificName, redlistCategory, yearPublished, systems, possiblyExtinct, possiblyExtinctInTheWild, scopes, infraType, infraName, infraAuthority, subpopulationName, authority)
VALUES (@import,@taxonId,@assessmentId,@sci,@cat,@year,@systems,@pe,@pew,@scopes,@infraType,@infraName,@infraAuthority,@subpop,@authority)";
            AddParams(_assessment, "@import", "@taxonId", "@assessmentId", "@sci", "@cat", "@year", "@systems", "@pe", "@pew", "@scopes", "@infraType", "@infraName", "@infraAuthority", "@subpop", "@authority");
        }

        private static void AddParams(SqliteCommand cmd, params string[] names) {
            foreach (var n in names) cmd.Parameters.Add(n, SqliteType.Text);
        }

        public void AddTaxonomy(long importId, long taxonId, string? scientificName,
            string? kingdom, string? phylum, string? klass, string? order, string? family, string? genus, string? species) {
            _taxonomy.Parameters["@import"].Value = importId;
            _taxonomy.Parameters["@taxonId"].Value = taxonId;
            _taxonomy.Parameters["@sci"].Value = (object?)scientificName ?? DBNull.Value;
            // kingdom/genus/species are read non-null by IucnListQueryService.ReadRecord,
            // so coalesce to '' (never NULL). kingdom..family are UPPERCASE to match the
            // sargable equality filters in TaxonFilterSql.NormalizeValue.
            _taxonomy.Parameters["@kingdom"].Value = UpperOrEmpty(kingdom);
            _taxonomy.Parameters["@phylum"].Value = Upper(phylum);
            _taxonomy.Parameters["@class"].Value = Upper(klass);
            _taxonomy.Parameters["@order"].Value = Upper(order);
            _taxonomy.Parameters["@family"].Value = Upper(family);
            _taxonomy.Parameters["@genus"].Value = (object?)(genus?.Trim()) ?? string.Empty;
            _taxonomy.Parameters["@species"].Value = (object?)(species?.Trim()) ?? string.Empty;
            _taxonomy.ExecuteNonQuery();
        }

        public void AddAssessment(long importId, ProjectedAssessment a, string redlistCategoryText) {
            _assessment.Parameters["@import"].Value = importId;
            _assessment.Parameters["@taxonId"].Value = a.TaxonId ?? 0L;
            _assessment.Parameters["@assessmentId"].Value = a.AssessmentId;
            _assessment.Parameters["@sci"].Value = (object?)a.ScientificName ?? DBNull.Value;
            _assessment.Parameters["@cat"].Value = (object?)redlistCategoryText ?? DBNull.Value;
            _assessment.Parameters["@year"].Value = (object?)a.YearPublished ?? DBNull.Value;
            _assessment.Parameters["@systems"].Value = (object?)a.Systems ?? DBNull.Value;
            _assessment.Parameters["@pe"].Value = a.PossiblyExtinct;
            _assessment.Parameters["@pew"].Value = a.PossiblyExtinctInTheWild;
            _assessment.Parameters["@scopes"].Value = (object?)a.Scopes ?? DBNull.Value;
            _assessment.Parameters["@infraType"].Value = (object?)a.InfraType ?? DBNull.Value;
            _assessment.Parameters["@infraName"].Value = (object?)a.InfraName ?? DBNull.Value;
            _assessment.Parameters["@infraAuthority"].Value = (object?)a.InfraAuthority ?? DBNull.Value;
            _assessment.Parameters["@subpop"].Value = (object?)a.SubpopulationName ?? DBNull.Value;
            _assessment.Parameters["@authority"].Value = (object?)a.Authority ?? DBNull.Value;
            _assessment.ExecuteNonQuery();
        }

        private static object Upper(string? value) =>
            string.IsNullOrWhiteSpace(value) ? (object)DBNull.Value : value.Trim().ToUpperInvariant();

        private static object UpperOrEmpty(string? value) =>
            string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim().ToUpperInvariant();

        public void Commit() {
            _tx.Commit();
            _committed = true;
        }

        public void Dispose() {
            if (!_committed) {
                try { _tx.Rollback(); } catch { /* connection may already be closed */ }
            }
            _taxonomy.Dispose();
            _assessment.Dispose();
            _tx.Dispose();
        }
    }
}
