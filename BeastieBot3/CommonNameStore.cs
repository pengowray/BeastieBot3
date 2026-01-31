using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

/// <summary>
/// SQLite store for unified common names from all sources (IUCN, Wikidata, Wikipedia, COL).
/// Supports disambiguation, conflict detection, and capitalization rules.
/// </summary>
internal sealed class CommonNameStore : IDisposable {
    private readonly SqliteConnection _connection;
    
    // Cache for ambiguous names set (expensive to compute, rarely changes)
    private HashSet<string>? _cachedAmbiguousNames;
    private string? _cachedAmbiguousNamesLanguage;

    private CommonNameStore(SqliteConnection connection) {
        _connection = connection;
    }

    public static CommonNameStore Open(string databasePath) {
        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directory)) {
            Directory.CreateDirectory(directory);
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };

        var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();

        using (var pragma = connection.CreateCommand()) {
            pragma.CommandText = "PRAGMA journal_mode = WAL;";
            pragma.ExecuteNonQuery();
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            pragma.ExecuteNonQuery();
        }

        var store = new CommonNameStore(connection);
        store.EnsureSchema();
        return store;
    }

    public void Dispose() => _connection.Dispose();

    public void EnsureSchema() {
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
                -- Display name with corrected capitalization
                display_name TEXT,
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

            -- Detected common name conflicts
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
                -- Resolution: NULL (unresolved), 'prefer_a', 'prefer_b', 'reject_both', 'manual'
                resolution TEXT,
                resolution_notes TEXT,
                -- Timestamps
                detected_at TEXT NOT NULL,
                resolved_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_conflicts_normalized ON common_name_conflicts(normalized_name);
            CREATE INDEX IF NOT EXISTS idx_conflicts_unresolved ON common_name_conflicts(resolution) WHERE resolution IS NULL;

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
            CREATE INDEX IF NOT EXISTS idx_caps_lowercase ON caps_rules(lowercase_word);

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

    #endregion

    #region Common Name Operations

    public long InsertCommonName(
        long taxonId,
        string rawName,
        string normalizedName,
        string? displayName,
        string language,
        string source,
        string? sourceIdentifier,
        bool isPreferred) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO common_names 
                (taxon_id, raw_name, normalized_name, display_name, language, source, source_identifier, is_preferred, created_at)
            VALUES (@taxonId, @raw, @normalized, @display, @lang, @source, @sourceId, @preferred, @now)
            ON CONFLICT(taxon_id, normalized_name, source, language) DO UPDATE SET
                raw_name = excluded.raw_name,
                display_name = COALESCE(excluded.display_name, common_names.display_name),
                is_preferred = MAX(common_names.is_preferred, excluded.is_preferred)
            RETURNING id;
            """;
        command.Parameters.AddWithValue("@taxonId", taxonId);
        command.Parameters.AddWithValue("@raw", rawName);
        command.Parameters.AddWithValue("@normalized", normalizedName);
        command.Parameters.AddWithValue("@display", displayName ?? (object)DBNull.Value);
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
                SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name, cn.display_name, 
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
                SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name, cn.display_name, 
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
                DisplayName: reader.IsDBNull(4) ? null : reader.GetString(4),
                Language: reader.GetString(5),
                Source: reader.GetString(6),
                SourceIdentifier: reader.IsDBNull(7) ? null : reader.GetString(7),
                IsPreferred: reader.GetInt32(8) == 1,
                TaxonCanonicalName: reader.GetString(9),
                TaxonKingdom: reader.IsDBNull(10) ? null : reader.GetString(10),
                TaxonValidityStatus: reader.GetString(11),
                TaxonIsExtinct: reader.GetInt32(12) == 1,
                TaxonIsFossil: reader.GetInt32(13) == 1
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
    /// Get the best non-ambiguous common name for a taxon.
    /// Returns null if no suitable name is found or all names are ambiguous.
    /// </summary>
    /// <param name="taxonId">The taxon ID to look up.</param>
    /// <param name="language">Language code (default: en).</param>
    /// <param name="allowAmbiguous">If true, return ambiguous names anyway (useful for display with disambiguation).</param>
    /// <returns>The best common name result, or null if none found.</returns>
    public CommonNameResult? GetBestCommonNameForTaxon(long taxonId, string language = "en", bool allowAmbiguous = false) {
        // Get all common names for this taxon
        var candidates = GetCommonNamesForTaxon(taxonId, language);
        if (candidates.Count == 0) {
            return null;
        }

        // Get set of ambiguous normalized names (names that refer to multiple taxa)
        var ambiguousNames = allowAmbiguous ? new HashSet<string>() : GetAmbiguousNamesSet(language);

        // Sort by: source priority ASC, is_preferred DESC (within source), raw_name ASC (for determinism)
        var sorted = candidates
            .OrderBy(c => GetSourcePriority(c.Source, c.IsPreferred))
            .ThenByDescending(c => c.IsPreferred)
            .ThenBy(c => c.RawName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        // Find first non-ambiguous name
        foreach (var candidate in sorted) {
            var isAmbiguous = ambiguousNames.Contains(candidate.NormalizedName);
            if (!isAmbiguous || allowAmbiguous) {
                return new CommonNameResult(
                    RawName: candidate.RawName,
                    DisplayName: candidate.DisplayName ?? candidate.RawName,
                    NormalizedName: candidate.NormalizedName,
                    Source: candidate.Source,
                    IsPreferred: candidate.IsPreferred,
                    IsAmbiguous: isAmbiguous
                );
            }
        }

        return null;
    }

    /// <summary>
    /// Get all common names for a specific taxon.
    /// </summary>
    public IReadOnlyList<CommonNameRecord> GetCommonNamesForTaxon(long taxonId, string language = "en") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name, cn.display_name, 
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
                DisplayName: reader.IsDBNull(4) ? null : reader.GetString(4),
                Language: reader.GetString(5),
                Source: reader.GetString(6),
                SourceIdentifier: reader.IsDBNull(7) ? null : reader.GetString(7),
                IsPreferred: reader.GetInt32(8) == 1,
                TaxonCanonicalName: reader.GetString(9),
                TaxonKingdom: reader.IsDBNull(10) ? null : reader.GetString(10),
                TaxonValidityStatus: reader.GetString(11),
                TaxonIsExtinct: reader.GetInt32(12) == 1,
                TaxonIsFossil: reader.GetInt32(13) == 1
            ));
        }
        return results;
    }

    /// <summary>
    /// Get the set of normalized names that are ambiguous (used by multiple valid taxa).
    /// Result is cached for efficiency when doing repeated lookups.
    /// </summary>
    private HashSet<string> GetAmbiguousNamesSet(string language = "en") {
        // Return cached value if available for same language
        if (_cachedAmbiguousNames != null && _cachedAmbiguousNamesLanguage == language) {
            return _cachedAmbiguousNames;
        }

        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.normalized_name
            FROM common_names cn
            JOIN taxa t ON cn.taxon_id = t.id
            WHERE cn.language = @lang
              AND t.validity_status = 'valid'
              AND t.is_fossil = 0
            GROUP BY cn.normalized_name
            HAVING COUNT(DISTINCT cn.taxon_id) > 1;
            """;
        command.Parameters.AddWithValue("@lang", language);

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            set.Add(reader.GetString(0));
        }
        
        // Cache the result
        _cachedAmbiguousNames = set;
        _cachedAmbiguousNamesLanguage = language;
        
        return set;
    }

    /// <summary>
    /// Get the cached set of ambiguous names (normalized) for the given language.
    /// </summary>
    public IReadOnlySet<string> GetAmbiguousNames(string language = "en") {
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
    /// Get all common names for a specific taxon across all languages.
    /// </summary>
    public IReadOnlyList<CommonNameRecord> GetCommonNamesForTaxonAllLanguages(long taxonId) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.id, cn.taxon_id, cn.raw_name, cn.normalized_name, cn.display_name, 
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
                DisplayName: reader.IsDBNull(4) ? null : reader.GetString(4),
                Language: reader.GetString(5),
                Source: reader.GetString(6),
                SourceIdentifier: reader.IsDBNull(7) ? null : reader.GetString(7),
                IsPreferred: reader.GetInt32(8) == 1,
                TaxonCanonicalName: reader.GetString(9),
                TaxonKingdom: reader.IsDBNull(10) ? null : reader.GetString(10),
                TaxonValidityStatus: reader.GetString(11),
                TaxonIsExtinct: reader.GetInt32(12) == 1,
                TaxonIsFossil: reader.GetInt32(13) == 1
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
    /// Batch lookup: get best common names for multiple taxa at once.
    /// More efficient than calling GetBestCommonNameForTaxon repeatedly.
    /// </summary>
    public Dictionary<long, CommonNameResult> GetBestCommonNamesForTaxa(
        IEnumerable<long> taxonIds, 
        string language = "en", 
        bool allowAmbiguous = false) {
        
        var idList = taxonIds.ToList();
        if (idList.Count == 0) {
            return new Dictionary<long, CommonNameResult>();
        }

        // Pre-load ambiguous names set once
        var ambiguousNames = allowAmbiguous ? new HashSet<string>() : GetAmbiguousNamesSet(language);
        
        // Query all common names for these taxa
        var placeholders = string.Join(",", idList.Select((_, i) => $"@id{i}"));
        using var command = _connection.CreateCommand();
        command.CommandText = $@"
            SELECT cn.taxon_id, cn.raw_name, cn.normalized_name, cn.display_name, 
                   cn.source, cn.is_preferred
            FROM common_names cn
            JOIN taxa t ON t.id = cn.taxon_id
            WHERE cn.taxon_id IN ({placeholders}) 
              AND cn.language = @lang
              AND t.validity_status = 'valid'
            ORDER BY cn.taxon_id, cn.is_preferred DESC;
        ";
        command.Parameters.AddWithValue("@lang", language);
        for (int i = 0; i < idList.Count; i++) {
            command.Parameters.AddWithValue($"@id{i}", idList[i]);
        }

        // Group by taxon_id
        var byTaxon = new Dictionary<long, List<(string RawName, string NormalizedName, string? DisplayName, string Source, bool IsPreferred)>>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var taxonId = reader.GetInt64(0);
            if (!byTaxon.TryGetValue(taxonId, out var list)) {
                list = new List<(string, string, string?, string, bool)>();
                byTaxon[taxonId] = list;
            }
            list.Add((
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetString(4),
                reader.GetInt32(5) == 1
            ));
        }

        // Select best name for each taxon
        var results = new Dictionary<long, CommonNameResult>();
        foreach (var (taxonId, candidates) in byTaxon) {
            var sorted = candidates
                .OrderBy(c => GetSourcePriority(c.Source, c.IsPreferred))
                .ThenByDescending(c => c.IsPreferred)
                .ThenBy(c => c.RawName, StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var candidate in sorted) {
                var isAmbiguous = ambiguousNames.Contains(candidate.NormalizedName);
                if (!isAmbiguous || allowAmbiguous) {
                    results[taxonId] = new CommonNameResult(
                        RawName: candidate.RawName,
                        DisplayName: candidate.DisplayName ?? candidate.RawName,
                        NormalizedName: candidate.NormalizedName,
                        Source: candidate.Source,
                        IsPreferred: candidate.IsPreferred,
                        IsAmbiguous: isAmbiguous
                    );
                    break;
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Get the Wikipedia article title for a taxon.
    /// Returns the raw_name from wikipedia_title or wikipedia_taxobox sources.
    /// </summary>
    public string? GetWikipediaArticleTitle(long taxonId, string language = "en") {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT cn.raw_name
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
        
        var result = command.ExecuteScalar();
        return result as string;
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

    #region Conflict Operations

    public void InsertConflict(
        string normalizedName,
        string conflictType,
        long taxonIdA,
        long? commonNameIdA,
        long? taxonIdB,
        long? commonNameIdB) {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO common_name_conflicts 
                (normalized_name, conflict_type, taxon_id_a, common_name_id_a, taxon_id_b, common_name_id_b, detected_at)
            VALUES (@name, @type, @taxonA, @cnA, @taxonB, @cnB, @now);
            """;
        command.Parameters.AddWithValue("@name", normalizedName);
        command.Parameters.AddWithValue("@type", conflictType);
        command.Parameters.AddWithValue("@taxonA", taxonIdA);
        command.Parameters.AddWithValue("@cnA", commonNameIdA ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@taxonB", taxonIdB ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@cnB", commonNameIdB ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@now", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    public void ClearConflicts() {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM common_name_conflicts";
        command.ExecuteNonQuery();
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

    public (int TaxaCount, int SynonymCount, int CommonNameCount, int ConflictCount) GetStatistics() {
        using var command = _connection.CreateCommand();
        command.CommandText =
            """
            SELECT 
                (SELECT COUNT(*) FROM taxa),
                (SELECT COUNT(*) FROM scientific_name_synonyms),
                (SELECT COUNT(*) FROM common_names),
                (SELECT COUNT(*) FROM common_name_conflicts);
            """;
        using var reader = command.ExecuteReader();
        if (reader.Read()) {
            return (reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3));
        }
        return (0, 0, 0, 0);
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
            command.Parameters.AddWithValue("@kingdom", kingdom);
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
            command.Parameters.AddWithValue("@kingdom", kingdom);
        }

        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    /// <summary>
    /// Get normalized names that map to multiple distinct taxa (general ambiguity check).
    /// </summary>
    public IReadOnlyList<string> GetAmbiguousCommonNames(int? limit, string? kingdom = null) {
        using var command = _connection.CreateCommand();
        var kingdomFilter = kingdom != null ? "AND t.kingdom = @kingdom" : "";
        var limitClause = limit.HasValue ? "LIMIT @limit" : "";
        command.CommandText = $@"
            SELECT c.normalized_name
            FROM common_names c
            JOIN taxa t ON c.taxon_id = t.id
            WHERE c.language = 'en'
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
            command.Parameters.AddWithValue("@kingdom", kingdom);
        }

        var results = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            results.Add(reader.GetString(0));
        }
        return results;
    }

    #endregion
}

public record CommonNameRecord(
    long Id,
    long TaxonId,
    string RawName,
    string NormalizedName,
    string? DisplayName,
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
/// Summary of import runs for a specific import type.
/// </summary>
public record ImportRunSummary(
    string ImportType,
    DateTime? LastRun,
    int TotalAdded,
    bool HasCompleted
);

/// <summary>
/// Result of a best common name lookup.
/// </summary>
/// <param name="RawName">The original common name as stored.</param>
/// <param name="DisplayName">The name to display (with capitalization corrections applied).</param>
/// <param name="NormalizedName">Lowercase normalized form for comparison.</param>
/// <param name="Source">Source of this name (wikipedia_title, wikidata, iucn, etc.).</param>
/// <param name="IsPreferred">Whether this is marked as a preferred name from its source.</param>
/// <param name="IsAmbiguous">Whether this name refers to multiple taxa (returned when allowAmbiguous=true).</param>
public record CommonNameResult(
    string RawName,
    string DisplayName,
    string NormalizedName,
    string Source,
    bool IsPreferred,
    bool IsAmbiguous
);