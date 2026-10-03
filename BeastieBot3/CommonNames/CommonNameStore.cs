using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;
using BeastieBot3.Taxonomy;

// Unified SQLite store aggregating vernacular names from IUCN, Wikidata, Wikipedia, and COL.
// Schema: taxa (sis_id, scientific_name), common_names (name, language, source, taxon_id),
// caps_rules (capitalization overrides from caps.txt). Decides which taxon may use a common name
// that several taxa have (AmbiguousNames). Used by CommonNameAggregateCommand to import, CommonNameReportCommand for
// analysis, and CommonNameChooser (through StoreBackedCommonNameProvider and SiteCommonNamesReader)
// to choose the English name the Wikipedia lists and the species site show.

namespace BeastieBot3.CommonNames;

/// <summary>
/// SQLite store for unified common names from all sources (IUCN, Wikidata, Wikipedia, COL).
/// Supports disambiguation, ambiguous-name detection, and capitalization rules.
/// </summary>
internal sealed class CommonNameStore : SqliteStore {
    // Cache for the ambiguity rule's verdicts (expensive to compute, rarely changes)
    private AmbiguousNames? _cachedAmbiguousNames;
    private string? _cachedAmbiguousNamesLanguage;

    private CommonNameStore(SqliteConnection connection) : base(connection) {
    }

    public static CommonNameStore Open(string databasePath) {
        var connection = OpenConnection(databasePath);
        var store = new CommonNameStore(connection);
        store.EnsureSchema();
        return store;
    }

    /// <summary>
    /// Opens an existing store for reading only: no folder is created, no WAL pragma is set and no
    /// schema work is done, so a reader such as `site build-db` cannot change a store other
    /// commands are using. Any write through it fails.
    /// </summary>
    public static CommonNameStore OpenReadOnly(string databasePath) {
        if (!File.Exists(databasePath)) {
            throw new FileNotFoundException($"Common names store not found: {databasePath}", databasePath);
        }
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        return new CommonNameStore(connection);
    }

    /// <summary>
    /// Test/advanced seam (R5): build a store over a caller-owned, already-open connection — e.g. a
    /// shared <c>:memory:</c> SQLite connection — so the store can be exercised without a file.
    /// </summary>
    internal static CommonNameStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new CommonNameStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            -- Core taxa table: unified view of taxa from all sources
            CREATE TABLE IF NOT EXISTS taxa (
                id INTEGER PRIMARY KEY,
                -- Canonical scientific name (normalized: lowercase, single spaces, no rank markers)
                canonical_name TEXT NOT NULL,
                -- Original scientific name as provided by primary source
                original_name TEXT NOT NULL,
                -- Taxonomic rank: kingdom, phylum, class, order, family, genus, species, subspecies, variety, form
                rank TEXT NOT NULL,
                -- Kingdom for disambiguation (Animalia, Plantae, Fungi, etc.)
                kingdom TEXT,
                -- Extinction status
                is_extinct INTEGER NOT NULL DEFAULT 0,
                is_fossil INTEGER NOT NULL DEFAULT 0,
                -- Validity: 'valid', 'synonym', 'uncertain', 'invalid'
                validity_status TEXT NOT NULL DEFAULT 'valid',
                -- Primary source and identifier
                primary_source TEXT NOT NULL,
                primary_source_id TEXT NOT NULL,
                -- Timestamps
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(primary_source, primary_source_id)
            );
            CREATE INDEX IF NOT EXISTS idx_taxa_canonical ON taxa(canonical_name);
            CREATE INDEX IF NOT EXISTS idx_taxa_kingdom ON taxa(kingdom);
            CREATE INDEX IF NOT EXISTS idx_taxa_validity ON taxa(validity_status);

            -- Scientific name synonyms (including subgenus variations)
            CREATE TABLE IF NOT EXISTS scientific_name_synonyms (
                id INTEGER PRIMARY KEY,
                taxon_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
                -- Normalized synonym for matching
                normalized_name TEXT NOT NULL,
                -- Original form of the synonym
                original_name TEXT NOT NULL,
                -- Source of this synonym: 'iucn', 'col', 'wikidata', 'constructed' (for subgenus variants)
                source TEXT NOT NULL,
                -- Type: 'synonym', 'basionym', 'subgenus_variant', 'rank_variant'
                synonym_type TEXT NOT NULL DEFAULT 'synonym',
                created_at TEXT NOT NULL,
                UNIQUE(taxon_id, normalized_name, source)
            );
            CREATE INDEX IF NOT EXISTS idx_synonyms_normalized ON scientific_name_synonyms(normalized_name);

            -- Common names from all sources
            CREATE TABLE IF NOT EXISTS common_names (
                id INTEGER PRIMARY KEY,
                taxon_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
                -- Original common name as provided
                raw_name TEXT NOT NULL,
                -- Normalized for comparison (lowercase, no punctuation except hyphens stripped too)
                normalized_name TEXT NOT NULL,
                -- Language code (ISO 639-1)
                language TEXT NOT NULL DEFAULT 'en',
                -- Source: 'iucn', 'wikidata', 'wikipedia_title', 'wikipedia_taxobox', 'col'
                source TEXT NOT NULL,
                -- Identifier within source (assessment_id, entity_id, page_id, etc.)
                source_identifier TEXT,
                -- Is this the preferred name from the source?
                is_preferred INTEGER NOT NULL DEFAULT 0,
                -- Timestamps
                created_at TEXT NOT NULL,
                UNIQUE(taxon_id, normalized_name, source, language)
            );
            CREATE INDEX IF NOT EXISTS idx_common_names_normalized ON common_names(normalized_name);
            CREATE INDEX IF NOT EXISTS idx_common_names_taxon ON common_names(taxon_id);
            CREATE INDEX IF NOT EXISTS idx_common_names_language ON common_names(language);

            -- Cross-reference: links taxa across sources (for synonym matching)
            CREATE TABLE IF NOT EXISTS taxon_cross_references (
                id INTEGER PRIMARY KEY,
                taxon_id INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
                -- External source and its identifier
                source TEXT NOT NULL,
                source_identifier TEXT NOT NULL,
                -- Match confidence: 'exact', 'synonym', 'fuzzy', 'manual'
                match_type TEXT NOT NULL DEFAULT 'exact',
                created_at TEXT NOT NULL,
                UNIQUE(taxon_id, source, source_identifier)
            );
            CREATE INDEX IF NOT EXISTS idx_xref_source ON taxon_cross_references(source, source_identifier);

            -- Written only by the removed `common-names detect-conflicts` command; nothing reads it.
            -- Kept so stores that still hold its rows open and purge as before (PurgeSource
            -- empties it). Ambiguous names are worked out from common_names when they are needed
            -- (QueryAmbiguousNames).
            CREATE TABLE IF NOT EXISTS common_name_conflicts (
                id INTEGER PRIMARY KEY,
                -- The ambiguous normalized name
                normalized_name TEXT NOT NULL,
                -- Type: 'ambiguous' (multiple valid taxa), 'caps_mismatch', 'cross_source_mismatch'
                conflict_type TEXT NOT NULL,
                -- First taxon
                taxon_id_a INTEGER NOT NULL REFERENCES taxa(id) ON DELETE CASCADE,
                common_name_id_a INTEGER REFERENCES common_names(id) ON DELETE SET NULL,
                -- Second taxon (NULL for caps_mismatch within same taxon)
                taxon_id_b INTEGER REFERENCES taxa(id) ON DELETE CASCADE,
                common_name_id_b INTEGER REFERENCES common_names(id) ON DELETE SET NULL,
                detected_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_conflicts_normalized ON common_name_conflicts(normalized_name);
            -- SQLite scans a child table once per parent row deleted unless the foreign-key column
            -- is indexed. Without these, deleting names or taxa (see PurgeSource) scans the whole
            -- conflict table for every single row removed.
            CREATE INDEX IF NOT EXISTS idx_conflicts_name_a ON common_name_conflicts(common_name_id_a);
            CREATE INDEX IF NOT EXISTS idx_conflicts_name_b ON common_name_conflicts(common_name_id_b);
            CREATE INDEX IF NOT EXISTS idx_conflicts_taxon_a ON common_name_conflicts(taxon_id_a);
            CREATE INDEX IF NOT EXISTS idx_conflicts_taxon_b ON common_name_conflicts(taxon_id_b);

            -- Capitalization rules (from caps.txt)
            CREATE TABLE IF NOT EXISTS caps_rules (
                id INTEGER PRIMARY KEY,
                -- Lowercase version of the word for lookup
                lowercase_word TEXT NOT NULL UNIQUE,
                -- Correct capitalized form
                correct_form TEXT NOT NULL,
                -- Example names from caps.txt (optional, for reference)
                examples TEXT,
                -- Source: 'caps_txt', 'manual', 'inferred'
                source TEXT NOT NULL DEFAULT 'caps_txt',
                created_at TEXT NOT NULL
            );
            -- (lowercase_word already has a UNIQUE index from the column constraint; no extra index needed.)

            -- When each source was last re-imported from scratch (aggregate --replace), so
            -- `common-names sources` can say which sources still hold names their upstream
            -- data has dropped since.
            CREATE TABLE IF NOT EXISTS source_replacements (
                source TEXT PRIMARY KEY,
                replaced_at TEXT NOT NULL,
                removed_common_names INTEGER NOT NULL DEFAULT 0,
                removed_synonyms INTEGER NOT NULL DEFAULT 0,
                removed_cross_references INTEGER NOT NULL DEFAULT 0,
                removed_taxa INTEGER NOT NULL DEFAULT 0
            );

            -- Import tracking
            CREATE TABLE IF NOT EXISTS import_runs (
                id INTEGER PRIMARY KEY,
                -- Type: 'taxa_iucn', 'taxa_col', 'common_names_iucn', 'common_names_wikidata', etc.
                import_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                records_processed INTEGER DEFAULT 0,
                records_added INTEGER DEFAULT 0,
                records_updated INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                notes TEXT
            );
            """;
        command.ExecuteNonQuery();
    }

    #region Taxa Operations

    public long InsertOrUpdateTaxon(
        string canonicalName,
        string originalName,
        string rank,
        string? kingdom,
        bool isExtinct,
        bool isFossil,
        string validityStatus,
        string primarySource,
        string primarySourceId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO taxa (canonical_name, original_name, rank, kingdom, is_extinct, is_fossil, 
                              validity_status, primary_source, primary_source_id, created_at, updated_at)
            VALUES (@canonical, @original, @rank, @kingdom, @extinct, @fossil, 
                    @validity, @source, @sourceId, @now, @now)
            ON CONFLICT(primary_source, primary_source_id) DO UPDATE SET
                canonical_name = excluded.canonical_name,
                original_name = excluded.original_name,
                rank = excluded.rank,
                kingdom = COALESCE(excluded.kingdom, taxa.kingdom),
                is_extinct = excluded.is_extinct,
                is_fossil = excluded.is_fossil,
                validity_status = excluded.validity_status,
                updated_at = excluded.updated_at
            RETURNING id;
            """;
        var now = DateTime.UtcNow.ToString("O");
        command.Parameters.AddWithValue("@canonical", canonicalName);
        command.Parameters.AddWithValue("@original", originalName);
        command.Parameters.AddWithValue("@rank", rank);
        command.Parameters.AddWithValue("@kingdom", kingdom ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@extinct", isExtinct ? 1 : 0);
        command.Parameters.AddWithValue("@fossil", isFossil ? 1 : 0);
        command.Parameters.AddWithValue("@validity", validityStatus);
        command.Parameters.AddWithValue("@source", primarySource);
        command.Parameters.AddWithValue("@sourceId", primarySourceId);
        command.Parameters.AddWithValue("@now", now);
        return (long)(command.ExecuteScalar() ?? 0L);
    }

    public long? FindTaxonBySourceId(string source, string sourceId) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT id FROM taxa WHERE primary_source = @source AND primary_source_id = @sourceId";
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@sourceId", sourceId);
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (long)result;
    }

    /// <summary>
    /// Resolves a previously-recorded cross-source identity ((source, sourceIdentifier) →
    /// taxon) via taxon_cross_references. This is the cheapest dedup probe: a source row
    /// whose external id was matched on an earlier run maps straight back to its taxon
    /// without re-running the name/synonym fan-out. Backed by idx_xref_source.
    /// </summary>
    public long? FindTaxonByCrossReference(string source, string sourceIdentifier) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT taxon_id FROM taxon_cross_references WHERE source=@source AND source_identifier=@id LIMIT 1";
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@id", sourceIdentifier);
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (long)result;
    }

    public long GetCrossReferenceCount() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM taxon_cross_references";
        return Convert.ToInt64(command.ExecuteScalar() ?? 0L);
    }

    /// <summary>Common-name row counts grouped by source (e.g. iucn, wikidata, wikidata_label,
    /// wikipedia, col), ordered by count desc. Backs the <c>common-names sources</c> diagnostic.</summary>
    public IReadOnlyList<(string Source, int Count)> GetCommonNameCountsBySource() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT source, COUNT(*) FROM common_names GROUP BY source ORDER BY COUNT(*) DESC";
        using var reader = command.ExecuteReader();
        var results = new List<(string, int)>();
        while (reader.Read()) {
            results.Add((reader.GetString(0), reader.GetInt32(1)));
        }
        return results;
    }

    public long? FindTaxonByCanonicalName(string canonicalName, string? kingdom = null) {
        using var command = _connection.CreateCommand();
        if (kingdom != null) {
            command.CommandText = "SELECT id FROM taxa WHERE canonical_name = @name AND kingdom = @kingdom AND validity_status = 'valid' LIMIT 1";
            command.Parameters.AddWithValue("@kingdom", kingdom);
        } else {
            command.CommandText = "SELECT id FROM taxa WHERE canonical_name = @name AND validity_status = 'valid' LIMIT 1";
        }
        command.Parameters.AddWithValue("@name", canonicalName);
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (long)result;
    }

    /// <summary>
    /// Find a taxon by scientific name, checking canonical name first, then synonyms.
    /// </summary>
    public long? FindTaxonByScientificName(string scientificName) {
        // First try canonical name (normalized)
        var normalized = ScientificNameNormalizer.Normalize(scientificName);
        if (normalized == null) return null;

        var taxonId = FindTaxonByCanonicalName(normalized);
        if (taxonId.HasValue) return taxonId;

        // Try synonyms
        return FindTaxonBySynonym(normalized);
    }

    /// <summary>
    /// Find a taxon by scientific name with optional kingdom filtering.
    /// </summary>
    public long? FindTaxonByScientificName(string scientificName, string? kingdom) {
        var normalized = ScientificNameNormalizer.Normalize(scientificName);
        if (normalized == null) return null;

        var taxonId = FindTaxonByCanonicalName(normalized, kingdom);
        if (taxonId.HasValue) return taxonId;

        return FindTaxonBySynonym(normalized, kingdom);
    }

    #endregion

    #region Synonym Operations

    public void InsertSynonym(long taxonId, string normalizedName, string originalName, string source, string synonymType = "synonym") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT OR IGNORE INTO scientific_name_synonyms 
                (taxon_id, normalized_name, original_name, source, synonym_type, created_at)
            VALUES (@taxonId, @normalized, @original, @source, @type, @now);
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@normalized", normalizedName);
        command.Parameters.AddWithValue("@original", originalName);
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@type", synonymType);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    public long? FindTaxonBySynonym(string normalizedName) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT t.id FROM taxa t
            JOIN scientific_name_synonyms s ON s.taxon_id = t.id
            WHERE s.normalized_name = @name AND t.validity_status = 'valid'
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@name", normalizedName);
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (long)result;
    }

    public long? FindTaxonBySynonym(string normalizedName, string? kingdom) {
        using var command = _connection.CreateCommand();
        if (!string.IsNullOrWhiteSpace(kingdom)) {
            command.CommandText =
                """
                SELECT t.id FROM taxa t
                JOIN scientific_name_synonyms s ON s.taxon_id = t.id
                WHERE s.normalized_name = @name AND t.validity_status = 'valid' AND t.kingdom = @kingdom
                LIMIT 1;
                """;
            command.Parameters.AddWithValue("@kingdom", kingdom);
        } else {
            command.CommandText =
                """
                SELECT t.id FROM taxa t
                JOIN scientific_name_synonyms s ON s.taxon_id = t.id
                WHERE s.normalized_name = @name AND t.validity_status = 'valid'
                LIMIT 1;
                """;
        }
        command.Parameters.AddWithValue("@name", normalizedName);
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (long)result;
    }

    /// <summary>
    /// Returns true when the two taxa share any scientific name — i.e. one's canonical
    /// name or a recorded synonym matches the other's canonical name or a synonym. Both
    /// <c>taxa.canonical_name</c> and <c>scientific_name_synonyms.normalized_name</c> are
    /// stored already normalized (via <see cref="Taxonomy.ScientificNameNormalizer"/>), so
    /// the equality join is consistent. Used by conflict detection to avoid recording two
    /// rows that are really name-level synonyms of each other as an ambiguous-name conflict.
    /// </summary>
    public bool AreSynonyms(long taxonIdA, long taxonIdB) {
        if (taxonIdA == taxonIdB) {
            return true;
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            WITH names_a(name) AS (
                SELECT canonical_name FROM taxa WHERE id = @a
                UNION
                SELECT normalized_name FROM scientific_name_synonyms WHERE taxon_id = @a
            ),
            names_b(name) AS (
                SELECT canonical_name FROM taxa WHERE id = @b
                UNION
                SELECT normalized_name FROM scientific_name_synonyms WHERE taxon_id = @b
            )
            SELECT 1 FROM names_a JOIN names_b ON names_a.name = names_b.name LIMIT 1;
            """;
        command.Parameters.AddWithValue("@a", taxonIdA);
        command.Parameters.AddWithValue("@b", taxonIdB);
        return command.ExecuteScalar() != null;
    }

    #endregion

    #region Common Name Operations

    public long InsertCommonName(
        long taxonId,
        string rawName,
        string normalizedName,
        string language,
        string source,
        string? sourceIdentifier,
        bool isPreferred) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO common_names
                (taxon_id, raw_name, normalized_name, language, source, source_identifier, is_preferred, created_at)
            VALUES (@taxonId, @raw, @normalized, @lang, @source, @sourceId, @preferred, @now)
            ON CONFLICT(taxon_id, normalized_name, source, language) DO UPDATE SET
                raw_name = excluded.raw_name,
                is_preferred = MAX(common_names.is_preferred, excluded.is_preferred)
            RETURNING id;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@raw", rawName);
        command.Parameters.AddWithValue("@normalized", normalizedName);
        command.Parameters.AddWithValue("@lang", language);
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@sourceId", sourceIdentifier ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@preferred", isPreferred ? 1 : 0);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        return (long)(command.ExecuteScalar() ?? 0L);
    }

    public IReadOnlyList<CommonNameRecord> GetCommonNamesByNormalized(string normalizedName, string? language = null) {
        using var command = _connection.CreateCommand();
        if (language != null) {
            command.CommandText =
                """
                SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name,
                       cn.language, cn.source, cn.source_identifier, cn.is_preferred,
                       t.canonical_name, t.kingdom, t.validity_status, t.is_extinct, t.is_fossil
                FROM common_names cn
                JOIN taxa t ON t.id = cn.taxon_id
                WHERE cn.normalized_name = @name AND cn.language = @lang
                ORDER BY cn.is_preferred DESC, cn.source;
                """;
            command.Parameters.AddWithValue("@lang", language);
        } else {
            command.CommandText =
                """
                SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name,
                       cn.language, cn.source, cn.source_identifier, cn.is_preferred,
                       t.canonical_name, t.kingdom, t.validity_status, t.is_extinct, t.is_fossil
                FROM common_names cn
                JOIN taxa t ON t.id = cn.taxon_id
                WHERE cn.normalized_name = @name
                ORDER BY cn.is_preferred DESC, cn.source;
                """;
        }
        command.Parameters.AddWithValue("@name", normalizedName);

        var results = new List<CommonNameRecord>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(new CommonNameRecord(
                Id: reader.GetInt64(0),
                TaxonId: reader.GetInt64(1),
                RawName: reader.GetString(2),
                NormalizedName: reader.GetString(3),
                Language: reader.GetString(4),
                Source: reader.GetString(5),
                SourceIdentifier: reader.IsDBNull(6) ? null : reader.GetString(6),
                IsPreferred: reader.GetInt32(7) == 1,
                TaxonCanonicalName: reader.GetString(8),
                TaxonKingdom: reader.IsDBNull(9) ? null : reader.GetString(9),
                TaxonValidityStatus: reader.GetString(10),
                TaxonIsExtinct: reader.GetInt32(11) == 1,
                TaxonIsFossil: reader.GetInt32(12) == 1
            ));
        }
        return results;
    }

    public IReadOnlyList<string> GetDistinctNormalizedCommonNames(string language = "en") {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT normalized_name FROM common_names WHERE language = @lang ORDER BY normalized_name";
        command.Parameters.AddWithValue("@lang", language);
        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    /// <summary>
    /// Get all distinct raw common names for a language. More efficient for caps checking
    /// since we only need the raw names, not full records.
    /// </summary>
    public IReadOnlyList<string> GetDistinctRawCommonNames(string language = "en", int? limit = null) {
        using var command = _connection.CreateCommand();
        var sql = "SELECT DISTINCT raw_name FROM common_names WHERE language = @lang";
        if (limit.HasValue) {
            sql += $" LIMIT {limit.Value}";
        }
        command.CommandText = sql;
        command.Parameters.AddWithValue("@lang", language);
        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    /// <summary>
    /// Source priority for common name selection.
    /// Lower numbers = higher priority. Wikipedia sources are preferred as they match existing article titles.
    /// Order: wikipedia_title, wikipedia_taxobox, wikidata_label, iucn (preferred), iucn (other), wikidata (aliases), col.
    /// </summary>
    internal static int GetSourcePriority(string source, bool isPreferred) {
        return source.ToLowerInvariant() switch {
            "wikipedia_title" => 1,
            "wikipedia_taxobox" => 2,
            "wikidata_label" => 3,
            "iucn" => isPreferred ? 4 : 5,
            "wikidata" => 6,
            "col" => 7,
            _ => 99
        };
    }

    /// <summary>
    /// The best of a taxon's common names in <paramref name="language"/>, by the one ranking
    /// (<see cref="CommonNameChooser.ChooseBest"/>): null when the taxon has none or every one is
    /// ambiguous for it. With <paramref name="allowAmbiguous"/> no name is skipped as ambiguous.
    /// The name is not capitalised; <see cref="CommonNameChooser.FromStore"/> does that.
    /// </summary>
    public CommonNameResult? GetBestCommonNameForTaxon(long taxonId, string language = "en", bool allowAmbiguous = false) {
        var candidates = GetCommonNamesForTaxon(taxonId, language);
        if (candidates.Count == 0) {
            return null;
        }
        return CommonNameChooser.ChooseBest(taxonId, ToCandidates(candidates),
            allowAmbiguous ? AmbiguousNames.None : GetAmbiguousNamesSet(language));
    }

    /// <summary>The fields of each record the ranking reads.</summary>
    internal static IEnumerable<CommonNameCandidate> ToCandidates(IEnumerable<CommonNameRecord> records) =>
        records.Select(c => new CommonNameCandidate(c.RawName, c.NormalizedName, c.Source, c.IsPreferred));

    /// <summary>
    /// Get all common names for a specific taxon.
    /// </summary>
    public IReadOnlyList<CommonNameRecord> GetCommonNamesForTaxon(long taxonId, string language = "en") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name,
                   cn.language, cn.source, cn.source_identifier, cn.is_preferred,
                   t.canonical_name, t.kingdom, t.validity_status, t.is_extinct, t.is_fossil
            FROM common_names cn
            JOIN taxa t ON t.id = cn.taxon_id
            WHERE cn.taxon_id = @taxonId AND cn.language = @lang
            ORDER BY cn.is_preferred DESC, cn.source;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@lang", language);

        var results = new List<CommonNameRecord>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(new CommonNameRecord(
                Id: reader.GetInt64(0),
                TaxonId: reader.GetInt64(1),
                RawName: reader.GetString(2),
                NormalizedName: reader.GetString(3),
                Language: reader.GetString(4),
                Source: reader.GetString(5),
                SourceIdentifier: reader.IsDBNull(6) ? null : reader.GetString(6),
                IsPreferred: reader.GetInt32(7) == 1,
                TaxonCanonicalName: reader.GetString(8),
                TaxonKingdom: reader.IsDBNull(9) ? null : reader.GetString(9),
                TaxonValidityStatus: reader.GetString(10),
                TaxonIsExtinct: reader.GetInt32(11) == 1,
                TaxonIsFossil: reader.GetInt32(12) == 1
            ));
        }
        return results;
    }

    /// <summary>
    /// The ambiguity rule's verdicts for <paramref name="language"/> (see <see cref="QueryAmbiguousNames"/>).
    /// Result is cached for efficiency when doing repeated lookups.
    /// </summary>
    private AmbiguousNames GetAmbiguousNamesSet(string language = "en") {
        // Return cached value if available for same language
        if (_cachedAmbiguousNames != null && _cachedAmbiguousNamesLanguage == language) {
            return _cachedAmbiguousNames;
        }

        var verdicts = QueryAmbiguousNames(language);

        // Cache the result
        _cachedAmbiguousNames = verdicts;
        _cachedAmbiguousNamesLanguage = language;

        return verdicts;
    }

    /// <summary>
    /// Reads every <paramref name="language"/> common name of the valid, non-fossil taxa, in any
    /// kingdom, and applies the one ambiguity rule to them (<see cref="AmbiguousNames"/>). List
    /// generation and `site build-db` skip a name that is ambiguous for the taxon
    /// (<see cref="CommonNameChooser.ChooseBest"/>), and `common-names report --report ambiguous` lists the shared
    /// names with the taxon that keeps each one (<see cref="GetAmbiguousCommonNames"/>), so all
    /// three read this method and cannot drift apart. Source priority comes from
    /// <see cref="GetSourcePriority"/>, so the names are grouped here rather than in SQL.
    /// With <paramref name="kingdom"/>, only that kingdom's taxa are counted, so a name shared by
    /// a plant and an animal is not shared within either kingdom. The kingdom is upper-cased
    /// before binding, because taxa store it as IUCN writes it ("PLANTAE") and the report's
    /// --kingdom help suggests "Plantae".
    /// Junk names (<see cref="CommonNameQuality"/>) are left out, and a repairable name counts
    /// under its repaired name's key (<see cref="CommonNameChooser.UsableName"/>), the key the
    /// chooser compares it by.
    /// </summary>
    private AmbiguousNames QueryAmbiguousNames(string language, string? kingdom = null) {
        using var command = _connection.CreateCommand();
        var kingdomFilter = kingdom != null ? "AND t.kingdom = @kingdom" : "";
        command.CommandText = $@"
            SELECT c.normalized_name, c.taxon_id, t.canonical_name, c.source, c.is_preferred, c.raw_name, t.kingdom
            FROM common_names c
            JOIN taxa t ON c.taxon_id = t.id
            WHERE c.language = @lang
              AND t.validity_status = 'valid'
              AND t.is_fossil = 0
              {kingdomFilter};
        ";
        command.Parameters.AddWithValue("@lang", language);
        if (kingdom != null) {
            command.Parameters.AddWithValue("@kingdom", kingdom.Trim().ToUpperInvariant());
        }

        var holdings = new List<NameHolding>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var source = reader.GetString(3);
            var preferred = reader.GetInt32(4) == 1;
            var candidate = new CommonNameCandidate(reader.GetString(5), reader.GetString(0), source, preferred);
            if (CommonNameChooser.UsableName(candidate, language) is not { } usable) {
                continue;
            }
            holdings.Add(new NameHolding(
                NormalizedName: usable.NormalizedName,
                TaxonId: reader.GetInt64(1),
                CanonicalName: reader.GetString(2),
                Priority: GetSourcePriority(source, preferred),
                Kingdom: reader.IsDBNull(6) ? null : reader.GetString(6)));
        }
        return AmbiguousNames.Build(holdings);
    }

    /// <summary>
    /// The cached ambiguity rule's verdicts for the given language.
    /// </summary>
    public AmbiguousNames GetAmbiguousNames(string language = "en") {
        return GetAmbiguousNamesSet(language);
    }

    /// <summary>
    /// Get all scientific names (canonical + synonyms) for a taxon.
    /// </summary>
    public IReadOnlyList<string> GetScientificNamesForTaxon(long taxonId) {
        var results = new List<string>();

        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT canonical_name, original_name FROM taxa WHERE id = @id";
            command.Parameters.AddWithValue("@id", taxonId);
            using var reader = command.ExecuteReader();
            if (reader.Read()) {
                if (!reader.IsDBNull(0)) results.Add(reader.GetString(0));
                if (!reader.IsDBNull(1)) results.Add(reader.GetString(1));
            }
        }

        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT original_name FROM scientific_name_synonyms WHERE taxon_id = @id";
            command.Parameters.AddWithValue("@id", taxonId);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (!reader.IsDBNull(0)) results.Add(reader.GetString(0));
            }
        }

        return results;
    }

    /// <summary>
    /// How many species the store has in <paramref name="genus"/>: the distinct first two words of
    /// the valid, non-fossil taxa whose canonical name is the genus followed by more words. A
    /// subspecies counts as its species, and a working name ("pristoceuthophilus sp. nov.") as one
    /// species. `common-names aggregate` uses it to tell a genus with one species from a larger one.
    /// </summary>
    public int CountSpeciesInGenus(string genus) {
        var lower = genus.Trim().ToLowerInvariant();
        if (lower.Length == 0) {
            return 0;
        }
        using var command = _connection.CreateCommand();
        // A range on the canonical name index: '!' is the character after the space.
        command.CommandText =
            """
            SELECT canonical_name FROM taxa
            WHERE canonical_name >= @from AND canonical_name < @to
              AND validity_status = 'valid' AND is_fossil = 0;
            """;
        command.Parameters.AddWithValue("@from", lower + " ");
        command.Parameters.AddWithValue("@to", lower + "!");
        var species = new HashSet<string>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var words = reader.GetString(0).Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (words.Length >= 2) {
                species.Add(words[1]);
            }
        }
        return species.Count;
    }

    /// <summary>
    /// A taxon's scientific names as stored, normalised: taxa.canonical_name and every
    /// scientific_name_synonyms.normalized_name. <see cref="ScientificNameCheck"/> compares a
    /// candidate common name with them.
    /// </summary>
    public TaxonScientificNames GetTaxonScientificNames(long taxonId) {
        string? canonical;
        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT canonical_name FROM taxa WHERE id = @id";
            command.Parameters.AddWithValue("@id", taxonId);
            canonical = command.ExecuteScalar() as string;
        }

        var synonyms = new List<string>();
        using (var command = _connection.CreateCommand()) {
            command.CommandText = "SELECT DISTINCT normalized_name FROM scientific_name_synonyms WHERE taxon_id = @id";
            command.Parameters.AddWithValue("@id", taxonId);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                synonyms.Add(reader.GetString(0));
            }
        }
        return new TaxonScientificNames(canonical, synonyms);
    }

    /// <summary>
    /// The word sets <see cref="ScientificNameCheck"/> uses: genus names and epithets from every
    /// canonical name and synonym in the store, and the words of the English common names from IUCN
    /// and the Catalogue of Life. Names from Wikipedia and Wikidata labels are left out, because the
    /// check decides which of those are stored, so using them would make one run depend on the last.
    /// </summary>
    public NameWordSets LoadNameWordSets() =>
        NameWordSets.Build(
            ReadStrings("SELECT canonical_name FROM taxa UNION ALL SELECT normalized_name FROM scientific_name_synonyms"),
            ReadStrings("SELECT raw_name FROM common_names WHERE language = 'en' AND source IN ('iucn', 'col')"));

    private IEnumerable<string> ReadStrings(string sql) {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (!reader.IsDBNull(0)) {
                yield return reader.GetString(0);
            }
        }
    }

    /// <summary>
    /// Get all common names for a specific taxon across all languages.
    /// </summary>
    public IReadOnlyList<CommonNameRecord> GetCommonNamesForTaxonAllLanguages(long taxonId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name,
                   cn.language, cn.source, cn.source_identifier, cn.is_preferred,
                   t.canonical_name, t.kingdom, t.validity_status, t.is_extinct, t.is_fossil
            FROM common_names cn
            JOIN taxa t ON t.id = cn.taxon_id
            WHERE cn.taxon_id = @taxonId
            ORDER BY cn.language, cn.is_preferred DESC, cn.source;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);

        var results = new List<CommonNameRecord>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(new CommonNameRecord(
                Id: reader.GetInt64(0),
                TaxonId: reader.GetInt64(1),
                RawName: reader.GetString(2),
                NormalizedName: reader.GetString(3),
                Language: reader.GetString(4),
                Source: reader.GetString(5),
                SourceIdentifier: reader.IsDBNull(6) ? null : reader.GetString(6),
                IsPreferred: reader.GetInt32(7) == 1,
                TaxonCanonicalName: reader.GetString(8),
                TaxonKingdom: reader.IsDBNull(9) ? null : reader.GetString(9),
                TaxonValidityStatus: reader.GetString(10),
                TaxonIsExtinct: reader.GetInt32(11) == 1,
                TaxonIsFossil: reader.GetInt32(12) == 1
            ));
        }
        return results;
    }

    /// <summary>
    /// Clears the cached ambiguous names set. Call this after modifying common name data.
    /// </summary>
    public void InvalidateAmbiguousNamesCache() {
        _cachedAmbiguousNames = null;
        _cachedAmbiguousNamesLanguage = null;
    }

    /// <summary>
    /// The match_type of a Wikipedia cross-reference for a page that `common-names aggregate`
    /// decided is about another taxon matched to the same page (WikipediaPageMatch); the taxon
    /// takes no names from it, and <see cref="GetWikipediaArticleTitle"/> does not link it. Other
    /// matched pages are recorded as "exact".
    /// </summary>
    internal const string OtherTaxonsPageMatch = "other_taxon_page";

    /// <summary>
    /// The title of the Wikipedia article matched to a taxon: the page its name came from
    /// (<see cref="GetWikipediaNamePage"/>), else, for English, the page it is matched to
    /// (<see cref="GetMatchedWikipediaPage"/>). The lists use
    /// StoreBackedCommonNameProvider, which links the taxon's own scientific name instead of the
    /// matched page when Wikipedia has a page or a redirect with that name.
    /// </summary>
    public string? GetWikipediaArticleTitle(long taxonId, string language = "en") =>
        GetWikipediaNamePage(taxonId, language)
        ?? (string.Equals(language, "en", StringComparison.OrdinalIgnoreCase) ? GetMatchedWikipediaPage(taxonId) : null);

    /// <summary>
    /// The title of the page a taxon's wikipedia_title or wikipedia_taxobox name came from
    /// (source_identifier), or null. Not the name itself: a title name has its disambiguation
    /// removed ("Jack Dempsey" from "Jack Dempsey (fish)"), and a taxobox name is the infobox's name
    /// field ("Red mullet" on "Mullus barbatus", "Sunda slow loris{sfn|Groves|2005|p=122}"), so
    /// neither is a link target.
    /// </summary>
    public string? GetWikipediaNamePage(long taxonId, string language = "en") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT COALESCE(cn.source_identifier, cn.raw_name)
            FROM common_names cn
            WHERE cn.taxon_id = @taxonId
              AND cn.language = @lang
              AND cn.source IN ('wikipedia_title', 'wikipedia_taxobox')
            ORDER BY
              CASE cn.source
                WHEN 'wikipedia_title' THEN 1
                WHEN 'wikipedia_taxobox' THEN 2
              END,
              cn.is_preferred DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@lang", language);
        return command.ExecuteScalar() as string;
    }

    /// <summary>
    /// The English Wikipedia page a taxon is matched to (its "exact" Wikipedia cross-reference), or
    /// null. It is the page the match ended on after following redirects: "Crenimugil buchanani"
    /// for Moolgarda buchanani, but also the genus page "Leucoraja" for Leucoraja wallacei, whose
    /// own name is a redirect to it. A page about another taxon (<see cref="OtherTaxonsPageMatch"/>)
    /// is not used. English Wikipedia is the only one `common-names aggregate` reads.
    /// </summary>
    public string? GetMatchedWikipediaPage(long taxonId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT source_identifier FROM taxon_cross_references
            WHERE taxon_id = @taxonId AND source = 'wikipedia' AND match_type = 'exact'
            ORDER BY id DESC
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        return command.ExecuteScalar() as string;
    }

    #endregion

    #region Cross-Reference Operations

    public void InsertCrossReference(long taxonId, string source, string sourceIdentifier, string matchType = "exact") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT OR IGNORE INTO taxon_cross_references 
                (taxon_id, source, source_identifier, match_type, created_at)
            VALUES (@taxonId, @source, @sourceId, @matchType, @now);
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@sourceId", sourceIdentifier);
        command.Parameters.AddWithValue("@matchType", matchType);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    #endregion

    #region Source Purge

    /// <summary>
    /// Every row a given aggregation source writes, by table. `common-names aggregate --replace`
    /// deletes exactly these before re-importing that source, so the hub mirrors the current
    /// release instead of a union of every release ever aggregated. Wikipedia and Wikidata write
    /// their common names under more specific tags than the source name, which is why this is an
    /// explicit map rather than a match on the source string.
    /// `constructed` synonyms are deliberately absent: `common-names init` mints those from the
    /// hub's own names, so no aggregation source owns them.
    /// MintsTaxa is false for IUCN: its taxa are the skeleton `common-names init` seeds, not
    /// --create-missing additions, so a purge of IUCN must never delete taxa.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, SourceRowTags> PurgeableSources =
        new Dictionary<string, SourceRowTags>(StringComparer.OrdinalIgnoreCase) {
            ["iucn"] = new(CommonNames: new[] { "iucn" }, Synonyms: new[] { "iucn" }, CrossReferences: new[] { "iucn" }, MintsTaxa: false),
            ["wikidata"] = new(CommonNames: new[] { "wikidata", "wikidata_label" }, Synonyms: Array.Empty<string>(), CrossReferences: new[] { "wikidata" }, MintsTaxa: true),
            ["wikipedia"] = new(CommonNames: new[] { "wikipedia_title", "wikipedia_taxobox" }, Synonyms: Array.Empty<string>(), CrossReferences: new[] { "wikipedia" }, MintsTaxa: true),
            ["col"] = new(CommonNames: new[] { "col" }, Synonyms: new[] { "col" }, CrossReferences: new[] { "col" }, MintsTaxa: true),
        };

    internal readonly record struct SourceRowTags(
        IReadOnlyList<string> CommonNames,
        IReadOnlyList<string> Synonyms,
        IReadOnlyList<string> CrossReferences,
        bool MintsTaxa);

    /// <summary>Counts removed by <see cref="PurgeSource"/>.</summary>
    public readonly record struct PurgeCounts(int CommonNames, int Synonyms, int CrossReferences, int Taxa, int Conflicts) {
        public int Total => CommonNames + Synonyms + CrossReferences + Taxa + Conflicts;
    }

    /// <summary>
    /// Removes everything one aggregation source contributed, so the next import of that source
    /// leaves the hub matching the source's current contents. Taxa are only removed when the
    /// purged source both minted them (--create-missing) and nothing else references them any
    /// more; the IUCN-anchored skeleton and any taxon another source still names is untouched.
    /// </summary>
    /// <param name="includeSynonyms">
    /// False leaves the source's synonyms in place. IUCN synonyms are only re-imported when
    /// `aggregate --include-synonyms` is given, so purging them on a run that will not rewrite
    /// them would drop them for good.
    /// </param>
    public PurgeCounts PurgeSource(string source, bool includeSynonyms = true) {
        if (!PurgeableSources.TryGetValue(source, out var tags)) {
            throw new ArgumentException($"Unknown aggregation source '{source}'.", nameof(source));
        }

        using var transaction = _connection.BeginTransaction();
        // Rows left by the removed `common-names detect-conflicts` command point at individual
        // common-name rows, so the purge empties the table rather than leave them half-empty.
        var conflicts = DeleteAll(transaction, "common_name_conflicts");
        var names = DeleteByTag(transaction, "common_names", "source", tags.CommonNames);
        var synonyms = includeSynonyms
            ? DeleteByTag(transaction, "scientific_name_synonyms", "source", tags.Synonyms)
            : 0;
        var xrefs = DeleteByTag(transaction, "taxon_cross_references", "source", tags.CrossReferences);
        var taxa = tags.MintsTaxa ? DeleteOrphanedTaxa(transaction, source) : 0;
        RecordReplacement(transaction, source, names, synonyms, xrefs, taxa);
        transaction.Commit();

        InvalidateAmbiguousNamesCache();
        return new PurgeCounts(names, synonyms, xrefs, taxa, conflicts);
    }

    private void RecordReplacement(SqliteTransaction transaction, string source, int names, int synonyms, int xrefs, int taxa) {
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO source_replacements
                (source, replaced_at, removed_common_names, removed_synonyms, removed_cross_references, removed_taxa)
            VALUES (@source, @now, @names, @synonyms, @xrefs, @taxa)
            ON CONFLICT(source) DO UPDATE SET
                replaced_at = excluded.replaced_at,
                removed_common_names = excluded.removed_common_names,
                removed_synonyms = excluded.removed_synonyms,
                removed_cross_references = excluded.removed_cross_references,
                removed_taxa = excluded.removed_taxa;
            """;
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("@names", names);
        command.Parameters.AddWithValue("@synonyms", synonyms);
        command.Parameters.AddWithValue("@xrefs", xrefs);
        command.Parameters.AddWithValue("@taxa", taxa);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// When each source was last re-imported from scratch, keyed by the aggregate --source name.
    /// A source that is absent has only ever been added to.
    /// </summary>
    public IReadOnlyDictionary<string, SourceReplacement> GetSourceReplacements() {
        var results = new Dictionary<string, SourceReplacement>(StringComparer.OrdinalIgnoreCase);
        // A store written before the table was added, opened read-only (OpenReadOnly does no
        // schema work), has no source_replacements table: no source has been replaced in it.
        using (var exists = _connection.CreateCommand()) {
            exists.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'source_replacements'";
            if (exists.ExecuteScalar() is null) {
                return results;
            }
        }
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT source, replaced_at, removed_common_names, removed_synonyms,
                   removed_cross_references, removed_taxa
            FROM source_replacements;
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            // Stored as a UTC "O" string; RoundtripKind keeps it UTC instead of shifting it by
            // the machine's offset.
            if (!DateTime.TryParse(reader.GetString(1), null,
                    System.Globalization.DateTimeStyles.RoundtripKind, out var replacedAt)) {
                continue;
            }
            // Conflicts are not stored per replacement (they are cleared wholesale, not per
            // source), so this count is always 0 here — read the individual fields, not Total.
            results[reader.GetString(0)] = new SourceReplacement(
                reader.GetString(0), replacedAt,
                new PurgeCounts(reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4), reader.GetInt32(5), 0));
        }
        return results;
    }

    public readonly record struct SourceReplacement(string Source, DateTime ReplacedAt, PurgeCounts Removed);

    private int DeleteAll(SqliteTransaction transaction, string table) {
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"DELETE FROM {table};";
        return command.ExecuteNonQuery();
    }

    private int DeleteByTag(SqliteTransaction transaction, string table, string column, IReadOnlyList<string> tags) {
        if (tags.Count == 0) {
            return 0;
        }
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        var placeholders = new List<string>(tags.Count);
        for (var i = 0; i < tags.Count; i++) {
            placeholders.Add("@t" + i);
            command.Parameters.AddWithValue("@t" + i, tags[i]);
        }
        command.CommandText = $"DELETE FROM {table} WHERE {column} IN ({string.Join(", ", placeholders)});";
        return command.ExecuteNonQuery();
    }

    // A taxon this source minted is dead weight once the purge leaves it with no names, no
    // synonyms and no cross-references from any source. Anything another source still points at
    // survives, so this can never thin the hub below what the other sources describe.
    // Only called for sources that mint taxa (--create-missing). For IUCN, primary_source
    // "iucn" marks the init-seeded skeleton instead, and nothing writes an "iucn"
    // cross-reference, so this query would delete every IUCN species left without names.
    private int DeleteOrphanedTaxa(SqliteTransaction transaction, string source) {
        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            DELETE FROM taxa
            WHERE primary_source = @source
              AND id NOT IN (SELECT taxon_id FROM common_names)
              AND id NOT IN (SELECT taxon_id FROM scientific_name_synonyms)
              AND id NOT IN (SELECT taxon_id FROM taxon_cross_references);
            """;
        command.Parameters.AddWithValue("@source", source);
        return command.ExecuteNonQuery();
    }

    #endregion

    #region Caps Rules Operations

    public void InsertCapsRule(string lowercaseWord, string correctForm, string? examples = null, string source = "caps_txt") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO caps_rules (lowercase_word, correct_form, examples, source, created_at)
            VALUES (@lower, @correct, @examples, @source, @now)
            ON CONFLICT(lowercase_word) DO UPDATE SET
                correct_form = excluded.correct_form,
                examples = COALESCE(excluded.examples, caps_rules.examples),
                source = excluded.source;
            """;
        command.Parameters.AddWithValue("@lower", lowercaseWord);
        command.Parameters.AddWithValue("@correct", correctForm);
        command.Parameters.AddWithValue("@examples", examples ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@source", source);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    public string? GetCorrectCapitalization(string lowercaseWord) {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT correct_form FROM caps_rules WHERE lowercase_word = @word";
        command.Parameters.AddWithValue("@word", lowercaseWord.ToLowerInvariant());
        var result = command.ExecuteScalar();
        return result == null || result == DBNull.Value ? null : (string)result;
    }

    /// <summary>
    /// Load all caps rules into memory for efficient batch lookups.
    /// </summary>
    public Dictionary<string, string> GetAllCapsRules() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT lowercase_word, correct_form FROM caps_rules";
        var results = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results[reader.GetString(0)] = reader.GetString(1);
        }
        return results;
    }

    public int GetCapsRuleCount() {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM caps_rules";
        return Convert.ToInt32(command.ExecuteScalar());
    }

    #endregion

    #region Import Tracking

    public long BeginImportRun(string importType) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO import_runs (import_type, started_at, status)
            VALUES (@type, @now, 'running')
            RETURNING id;
            """;
        command.Parameters.AddWithValue("@type", importType);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        return (long)(command.ExecuteScalar() ?? 0L);
    }

    public void CompleteImportRun(long runId, int processed, int added, int updated, int errors, string? notes = null) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            UPDATE import_runs SET
                ended_at = @now,
                records_processed = @processed,
                records_added = @added,
                records_updated = @updated,
                errors = @errors,
                status = 'completed',
                notes = @notes
            WHERE id = @id;
            """;
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.Parameters.AddWithValue("@processed", processed);
        command.Parameters.AddWithValue("@added", added);
        command.Parameters.AddWithValue("@updated", updated);
        command.Parameters.AddWithValue("@errors", errors);
        command.Parameters.AddWithValue("@notes", notes ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@id", runId);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Every run of one import type, newest first. The notes hold each run's options (for
    /// example "language=en; cleared first"); a run that never finished has none.
    /// </summary>
    public IReadOnlyList<ImportRunRecord> GetImportRuns(string importType) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT id, status, started_at, ended_at, notes
            FROM import_runs
            WHERE import_type = @type
            ORDER BY id DESC;
            """;
        command.Parameters.AddWithValue("@type", importType);

        var results = new List<ImportRunRecord>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(new ImportRunRecord(
                reader.GetInt64(0),
                reader.GetString(1),
                StoredUtc.Parse(reader.GetString(2)),
                reader.IsDBNull(3) ? null : StoredUtc.Parse(reader.GetString(3)),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }
        return results;
    }

    /// <summary>
    /// Gets a summary of the most recent import run for each import type.
    /// </summary>
    public IReadOnlyList<ImportRunSummary> GetImportRunSummaries() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT 
                import_type,
                MAX(ended_at) as last_run,
                SUM(CASE WHEN status = 'completed' THEN records_added ELSE 0 END) as total_added,
                MAX(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as has_completed
            FROM import_runs
            GROUP BY import_type
            ORDER BY import_type;
            """;

        var results = new List<ImportRunSummary>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var importType = reader.GetString(0);
            var lastRun = reader.IsDBNull(1) ? (DateTime?)null : DateTime.Parse(reader.GetString(1));
            var totalAdded = reader.GetInt32(2);
            var hasCompleted = reader.GetInt32(3) == 1;
            results.Add(new ImportRunSummary(importType, lastRun, totalAdded, hasCompleted));
        }
        return results;
    }

    #endregion

    #region Statistics

    public (int TaxaCount, int SynonymCount, int CommonNameCount) GetStatistics() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT 
                (SELECT COUNT(*) FROM taxa),
                (SELECT COUNT(*) FROM scientific_name_synonyms),
                (SELECT COUNT(*) FROM common_names);
            """;
        using var reader = command.ExecuteReader();
        if (reader.Read()) {
            return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
        }
        return (0, 0, 0);
    }

    /// <summary>
    /// Get normalized names that are IUCN-preferred for multiple distinct taxa (efficient SQL query).
    /// </summary>
    public IReadOnlyList<string> GetIucnPreferredConflictNames(int? limit, string? kingdom = null) {
        using var command = _connection.CreateCommand();
        var kingdomFilter = kingdom != null ? "AND t.kingdom = @kingdom" : "";
        var limitClause = limit.HasValue ? "LIMIT @limit" : "";
        command.CommandText = $@"
            SELECT c.normalized_name
            FROM common_names c
            JOIN taxa t ON c.taxon_id = t.id
            WHERE c.source = 'iucn' 
              AND c.is_preferred = 1 
              AND c.language = 'en'
              AND t.validity_status = 'valid'
              AND t.is_fossil = 0
              {kingdomFilter}
            GROUP BY c.normalized_name
            HAVING COUNT(DISTINCT c.taxon_id) > 1
            ORDER BY COUNT(DISTINCT c.taxon_id) DESC
            {limitClause};
        ";
        if (limit.HasValue) {
            command.Parameters.AddWithValue("@limit", limit.Value);
        }
        if (kingdom != null) {
            command.Parameters.AddWithValue("@kingdom", kingdom.Trim().ToUpperInvariant());
        }

        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    /// <summary>
    /// Get normalized names from Wikipedia sources that map to multiple distinct taxa.
    /// </summary>
    public IReadOnlyList<string> GetWikipediaAmbiguousNames(int? limit, string? kingdom = null) {
        using var command = _connection.CreateCommand();
        var kingdomFilter = kingdom != null ? "AND t.kingdom = @kingdom" : "";
        var limitClause = limit.HasValue ? "LIMIT @limit" : "";
        command.CommandText = $@"
            SELECT c.normalized_name
            FROM common_names c
            JOIN taxa t ON c.taxon_id = t.id
            WHERE c.source IN ('wikipedia_title', 'wikipedia_taxobox')
              AND c.language = 'en'
              AND t.validity_status = 'valid'
              AND t.is_fossil = 0
              {kingdomFilter}
            GROUP BY c.normalized_name
            HAVING COUNT(DISTINCT c.taxon_id) > 1
            ORDER BY COUNT(DISTINCT c.taxon_id) DESC
            {limitClause};
        ";
        if (limit.HasValue) {
            command.Parameters.AddWithValue("@limit", limit.Value);
        }
        if (kingdom != null) {
            command.Parameters.AddWithValue("@kingdom", kingdom.Trim().ToUpperInvariant());
        }

        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    /// <summary>
    /// The ambiguity rule over English names, for the report: <see cref="AmbiguousNames.Names"/>
    /// lists the shared names, most-shared first, and <see cref="AmbiguousNames.KeptBy"/> names
    /// the taxon that may use each one. Without <paramref name="kingdom"/> these are the verdicts
    /// list generation uses (<see cref="QueryAmbiguousNames"/>).
    /// </summary>
    public AmbiguousNames GetAmbiguousCommonNames(string? kingdom = null) =>
        QueryAmbiguousNames("en", kingdom);

    #endregion
}

public record CommonNameRecord(
    long Id,
    long TaxonId,
    string RawName,
    string NormalizedName,
    string Language,
    string Source,
    string? SourceIdentifier,
    bool IsPreferred,
    string TaxonCanonicalName,
    string? TaxonKingdom,
    string TaxonValidityStatus,
    bool TaxonIsExtinct,
    bool TaxonIsFossil
);
/// <summary>
/// One row of import_runs. <see cref="Status"/> is 'completed' for a run that finished and
/// 'running' for one that is still running or was interrupted.
/// </summary>
public record ImportRunRecord(
    long Id,
    string Status,
    DateTime? StartedAt,
    DateTime? EndedAt,
    string? Notes
);

/// <summary>
/// Summary of import runs for a specific import type.
/// </summary>
public record ImportRunSummary(
    string ImportType,
    DateTime? LastRun,
    int TotalAdded,
    bool HasCompleted
);

/// <summary>
/// One common name offered to <see cref="CommonNameChooser.ChooseBest"/>: the fields the ranking reads.
/// </summary>
internal readonly record struct CommonNameCandidate(string RawName, string NormalizedName, string Source, bool IsPreferred);

/// <summary>
/// Result of a best common name lookup.
/// </summary>
/// <param name="RawName">The original common name as stored.</param>
/// <param name="DisplayName">The name to display (with capitalization corrections applied).</param>
/// <param name="NormalizedName">Lowercase normalized form for comparison.</param>
/// <param name="Source">Source of this name (wikipedia_title, wikidata, iucn, etc.).</param>
/// <param name="IsPreferred">Whether this is marked as a preferred name from its source.</param>
/// <param name="IsAmbiguous">Whether another taxon keeps this name or ties for it (<see cref="AmbiguousNames"/>); always false with allowAmbiguous=true.</param>
public record CommonNameResult(
    string RawName,
    string DisplayName,
    string NormalizedName,
    string Source,
    bool IsPreferred,
    bool IsAmbiguous
);
