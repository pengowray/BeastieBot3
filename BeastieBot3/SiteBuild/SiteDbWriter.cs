using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// Writes the site database (SiteDbSchema.Ddl) into a new file. Fast while building: no journal, no
// fsync, one transaction, and the secondary indexes in the DDL dropped until the rows are in.
// Finish() recreates them from the DDL's own statements, so the finished schema is the DDL exactly,
// rebuilds the full-text index, runs ANALYZE, switches the file to rollback-journal mode (the site
// opens it read-only, and a WAL-mode file cannot be opened read-only in a folder the service
// cannot write) and compacts it with VACUUM.

namespace BeastieBot3.SiteBuild;

internal sealed class SiteDbWriter : IDisposable {
    private static readonly Regex IndexStatement = new(
        @"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?<name>\w+)\s+ON\s+[^;]+;",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private readonly SqliteConnection _connection;
    private SqliteTransaction? _transaction;
    private readonly List<string> _deferredIndexes = new();
    private readonly List<(string Key, long TaxonId, long NameId)> _nameKeys = new();
    private readonly SqliteCommand _taxon;
    private readonly SqliteCommand _assessment;
    private readonly SqliteCommand _name;
    private readonly SqliteCommand _replacedBy;
    private long _nextNameId = 1;
    private bool _finished;

    public long NameCount => _nextNameId - 1;

    private SiteDbWriter(SqliteConnection connection) {
        _connection = connection;
        Execute("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY; PRAGMA cache_size = -262144;");
        Execute(SiteDbSchema.Ddl);
        foreach (Match match in IndexStatement.Matches(SiteDbSchema.Ddl)) {
            _deferredIndexes.Add(match.Value);
            Execute($"DROP INDEX {match.Groups["name"].Value};");
        }
        _transaction = _connection.BeginTransaction();

        _taxon = Prepare("""
            INSERT INTO taxon (taxon_id, scientific_name, kind, kingdom, phylum, class_name, order_name, family, genus,
                species_epithet, infra_rank, infra_name, subpopulation_name, authority, parent_taxon_id, common_name_en,
                enwiki_title, wikidata_qid, wikidata_qid_source, col_id, sprat_taxon_id, epbc_status, latest_global_assessment_id)
            VALUES (@taxon_id, @scientific_name, @kind, @kingdom, @phylum, @class_name, @order_name, @family, @genus,
                @species_epithet, @infra_rank, @infra_name, @subpopulation_name, @authority, @parent_taxon_id, @common_name_en,
                @enwiki_title, @wikidata_qid, @wikidata_qid_source, @col_id, @sprat_taxon_id, @epbc_status, @latest_global_assessment_id)
            """,
            "@taxon_id", "@scientific_name", "@kind", "@kingdom", "@phylum", "@class_name", "@order_name", "@family", "@genus",
            "@species_epithet", "@infra_rank", "@infra_name", "@subpopulation_name", "@authority", "@parent_taxon_id", "@common_name_en",
            "@enwiki_title", "@wikidata_qid", "@wikidata_qid_source", "@col_id", "@sprat_taxon_id", "@epbc_status", "@latest_global_assessment_id");
        _assessment = Prepare("""
            INSERT INTO assessment (assessment_id, taxon_id, scope, is_latest, category, possibly_extinct,
                possibly_extinct_in_the_wild, criteria, criteria_version, year_published, assessment_date, population_trend, citation_json)
            VALUES (@assessment_id, @taxon_id, @scope, @is_latest, @category, @possibly_extinct,
                @possibly_extinct_in_the_wild, @criteria, @criteria_version, @year_published, @assessment_date, @population_trend, @citation_json)
            """,
            "@assessment_id", "@taxon_id", "@scope", "@is_latest", "@category", "@possibly_extinct",
            "@possibly_extinct_in_the_wild", "@criteria", "@criteria_version", "@year_published", "@assessment_date", "@population_trend", "@citation_json");
        _name = Prepare("""
            INSERT INTO name (name_id, taxon_id, name, name_type, language, source, is_preferred)
            VALUES (@name_id, @taxon_id, @name, @name_type, @language, @source, @is_preferred)
            """,
            "@name_id", "@taxon_id", "@name", "@name_type", "@language", "@source", "@is_preferred");
        _replacedBy = Prepare("UPDATE assessment SET replaced_by_assessment_id = @replaced_by WHERE assessment_id = @assessment_id",
            "@replaced_by", "@assessment_id");
    }

    /// Creates the file, replacing any file already at the path.
    public static SiteDbWriter Create(string path) {
        DeleteDatabaseFiles(path);
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false,
        }.ConnectionString);
        connection.Open();
        return new SiteDbWriter(connection);
    }

    /// Deletes a database file and any journal files beside it.
    public static void DeleteDatabaseFiles(string path) {
        foreach (var file in new[] { path, path + "-journal", path + "-wal", path + "-shm" }) {
            if (File.Exists(file)) {
                File.Delete(file);
            }
        }
    }

    public void AddTaxon(SiteTaxon t) {
        Bind(_taxon, t.TaxonId, t.ScientificName, t.Kind, t.Kingdom, t.Phylum, t.ClassName, t.OrderName, t.Family, t.Genus,
            t.SpeciesEpithet, t.InfraRank, t.InfraName, t.SubpopulationName, t.Authority, t.ParentTaxonId, t.CommonNameEn,
            t.EnwikiTitle, t.WikidataQid, t.WikidataQidSource, t.ColId, t.SpratTaxonId, t.EpbcStatus, t.LatestGlobalAssessmentId);
        _taxon.ExecuteNonQuery();
    }

    public void AddAssessment(SiteAssessment a) {
        Bind(_assessment, a.AssessmentId, a.TaxonId, a.Scope, a.IsLatest ? 1 : 0, a.Category, a.PossiblyExtinct ? 1 : 0,
            a.PossiblyExtinctInTheWild ? 1 : 0, a.Criteria, a.CriteriaVersion, a.YearPublished, a.AssessmentDate,
            a.PopulationTrend, a.CitationJson);
        _assessment.ExecuteNonQuery();
    }

    /// Sets replaced_by_assessment_id on an assessment already written.
    public void SetReplacedBy(long assessmentId, long replacedBy) {
        Bind(_replacedBy, replacedBy, assessmentId);
        _replacedBy.ExecuteNonQuery();
    }

    /// Writes one name and remembers its folded key for name_key.
    public void AddName(long taxonId, SiteName name) {
        var nameId = _nextNameId++;
        Bind(_name, nameId, taxonId, name.Name, name.NameType, name.Language, name.Source, name.IsPreferred ? 1 : 0);
        _name.ExecuteNonQuery();
        var key = SiteNameKey.Fold(name.Name);
        if (key.Length > 0) {
            _nameKeys.Add((key, taxonId, nameId));
        }
    }

    public void SetMeta(string key, string? value) {
        if (value is null) {
            return;
        }
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = "INSERT OR REPLACE INTO meta (key, value) VALUES (@key, @value)";
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", value);
        command.ExecuteNonQuery();
    }

    /// Writes name_key, commits, builds the indexes and the full-text index, and compacts the file.
    /// Each step is reported to onStep so the command can time it.
    public void Finish(Action<string, Action> step) {
        step("Writing the exact-name lookup table", () => {
            _nameKeys.Sort((a, b) => {
                var byKey = string.CompareOrdinal(a.Key, b.Key);
                return byKey != 0 ? byKey : a.NameId.CompareTo(b.NameId);
            });
            using var command = Prepare("INSERT INTO name_key (key, taxon_id, name_id) VALUES (@key, @taxon_id, @name_id)",
                "@key", "@taxon_id", "@name_id");
            foreach (var (key, taxonId, nameId) in _nameKeys) {
                Bind(command, key, taxonId, nameId);
                command.ExecuteNonQuery();
            }
            _nameKeys.Clear();
            _transaction!.Commit();
            _transaction.Dispose();
            _transaction = null;
        });
        step("Creating indexes", () => {
            foreach (var statement in _deferredIndexes) {
                Execute(statement);
            }
        });
        step("Building the search index", () => Execute("INSERT INTO name_fts(name_fts) VALUES('rebuild');"));
        step("Running ANALYZE", () => Execute("ANALYZE;"));
        step("Compacting the file (VACUUM)", () => {
            Execute("PRAGMA journal_mode = DELETE;");
            Execute("VACUUM;");
        });
        var mode = Scalar("PRAGMA journal_mode;");
        if (!string.Equals(mode, "delete", StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidOperationException($"The site database is in journal mode '{mode}', not 'delete'.");
        }
        _finished = true;
    }

    public string? Scalar(string sql) {
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        command.CommandTimeout = 0;
        return command.ExecuteScalar()?.ToString();
    }

    private void Execute(string sql) {
        using var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        command.CommandTimeout = 0;
        command.ExecuteNonQuery();
    }

    private SqliteCommand Prepare(string sql, params string[] parameters) {
        var command = _connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        foreach (var parameter in parameters) {
            command.Parameters.Add(new SqliteParameter { ParameterName = parameter });
        }
        command.Prepare();
        return command;
    }

    private static void Bind(SqliteCommand command, params object?[] values) {
        for (var i = 0; i < values.Length; i++) {
            command.Parameters[i].Value = values[i] ?? DBNull.Value;
        }
    }

    public void Dispose() {
        _taxon.Dispose();
        _assessment.Dispose();
        _name.Dispose();
        _replacedBy.Dispose();
        if (!_finished && _transaction is not null) {
            try {
                _transaction.Rollback();
            } catch (SqliteException) {
                // With journal_mode=OFF a rollback may not be possible; the file is deleted anyway.
            }
        }
        _transaction?.Dispose();
        _connection.Dispose();
    }
}
