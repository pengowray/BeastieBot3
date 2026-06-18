using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Col;
using BeastieBot3.Taxonomy;

// Enriches IUCN species with additional ranks from COL's NameUsage table.
// IUCN has limited taxonomy (class/order/family); COL provides suborder,
// superfamily, infraorder, etc. Enables list groupings like separating
// snakes (Serpentes) from lizards within Squamata. Uses ColTaxonRepository
// to query COL parent chain. Called by WikipediaListGenerator.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Enriches IUCN species records with additional taxonomic ranks from Catalogue of Life.
/// This enables finer-grained grouping in Wikipedia lists (e.g., separating snakes from lizards).
/// </summary>
internal sealed class ColTaxonomyEnricher : IDisposable {
    private readonly ColTaxonRepository _colRepository;
    private readonly SqliteConnection _connection;
    private readonly bool _ownsConnection;
    private readonly Dictionary<string, EnrichedTaxonomy> _cache = new(StringComparer.OrdinalIgnoreCase);

    // Optional persistent cache so the multi-GB COL database is hit at most once per taxon, ever:
    // resolved enrichment is stored in a small sidecar SQLite next to the COL DB and reused on every
    // subsequent run. Versioned to the COL file (length+mtime) so it auto-rebuilds when COL changes.
    private SqliteConnection? _cacheConn;
    private SqliteCommand? _cacheSelect;
    private SqliteCommand? _cacheInsert;

    public ColTaxonomyEnricher(string colDatabasePath) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = colDatabasePath,
            Mode = SqliteOpenMode.ReadOnly
        };
        _connection = new SqliteConnection(builder.ConnectionString);
        _connection.Open();
        _colRepository = new ColTaxonRepository(_connection);
        _ownsConnection = true;
        TryOpenPersistentCache(colDatabasePath);
    }

    public ColTaxonomyEnricher(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _colRepository = new ColTaxonRepository(_connection);
        _ownsConnection = false;
    }

    /// <summary>
    /// Enriches a collection of IUCN species records with COL taxonomy data.
    /// Returns a new collection of EnrichedSpeciesRecord with additional rank fields.
    /// </summary>
    public IReadOnlyList<EnrichedSpeciesRecord> Enrich(
        IEnumerable<IucnSpeciesRecord> records, 
        CancellationToken cancellationToken = default) {
        
        var results = new List<EnrichedSpeciesRecord>();
        
        foreach (var record in records) {
            cancellationToken.ThrowIfCancellationRequested();
            var enriched = EnrichSingle(record, cancellationToken);
            results.Add(enriched);
        }
        
        return results;
    }

    /// <summary>
    /// Enriches a single IUCN species record with COL taxonomy data.
    /// </summary>
    public EnrichedSpeciesRecord EnrichSingle(IucnSpeciesRecord record, CancellationToken cancellationToken = default) {
        // Build cache key from scientific name components
        var cacheKey = BuildCacheKey(record);

        if (_cache.TryGetValue(cacheKey, out var cached)) {
            return CreateEnrichedRecord(record, cached);
        }

        // Persistent (cross-run) cache, before touching the big COL DB.
        if (TryReadPersistent(cacheKey, out var persisted)) {
            _cache[cacheKey] = persisted;
            return CreateEnrichedRecord(record, persisted);
        }

        // Try to find in COL
        var colMatch = FindColMatch(record, cancellationToken);
        var taxonomy = colMatch != null
            ? ExtractTaxonomy(colMatch)
            : new EnrichedTaxonomy();

        _cache[cacheKey] = taxonomy;
        WritePersistent(cacheKey, colMatch != null, taxonomy);
        return CreateEnrichedRecord(record, taxonomy);
    }

    // ---- persistent enrichment cache (sidecar SQLite next to the COL DB) ----

    private void TryOpenPersistentCache(string colDatabasePath) {
        try {
            var cachePath = colDatabasePath + ".enrich-cache.sqlite";
            var version = ColVersionStamp(colDatabasePath);
            var conn = new SqliteConnection(new SqliteConnectionStringBuilder {
                DataSource = cachePath, Mode = SqliteOpenMode.ReadWriteCreate
            }.ConnectionString);
            conn.Open();
            using (var pragma = conn.CreateCommand()) {
                // WAL + NORMAL: durable enough for a rebuildable cache, no fsync per insert.
                pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
                pragma.ExecuteNonQuery();
            }
            EnsureCacheSchema(conn, version);

            _cacheSelect = conn.CreateCommand();
            _cacheSelect.CommandText = "SELECT subkingdom, subphylum, subclass, suborder, superfamily, subfamily, tribe, subtribe, subgenus, col_id, col_scientific_name FROM col_enrichment WHERE key = @k";
            _cacheSelect.Parameters.Add("@k", SqliteType.Text);

            _cacheInsert = conn.CreateCommand();
            _cacheInsert.CommandText = "INSERT OR REPLACE INTO col_enrichment (key, found, subkingdom, subphylum, subclass, suborder, superfamily, subfamily, tribe, subtribe, subgenus, col_id, col_scientific_name) " +
                "VALUES (@k,@found,@subkingdom,@subphylum,@subclass,@suborder,@superfamily,@subfamily,@tribe,@subtribe,@subgenus,@colid,@colsci)";
            _cacheInsert.Parameters.Add("@k", SqliteType.Text);
            _cacheInsert.Parameters.Add("@found", SqliteType.Integer);
            foreach (var col in new[] { "@subkingdom", "@subphylum", "@subclass", "@suborder", "@superfamily", "@subfamily", "@tribe", "@subtribe", "@subgenus", "@colid", "@colsci" }) {
                _cacheInsert.Parameters.Add(col, SqliteType.Text);
            }

            _cacheConn = conn;
        } catch {
            // Best-effort cache; on any failure fall back to direct COL queries.
            _cacheConn = null;
            _cacheSelect = null;
            _cacheInsert = null;
        }
    }

    private static void EnsureCacheSchema(SqliteConnection conn, string version) {
        using (var meta = conn.CreateCommand()) {
            meta.CommandText = "CREATE TABLE IF NOT EXISTS cache_meta (k TEXT PRIMARY KEY, v TEXT)";
            meta.ExecuteNonQuery();
        }
        string? stored;
        using (var get = conn.CreateCommand()) {
            get.CommandText = "SELECT v FROM cache_meta WHERE k='col_version'";
            stored = get.ExecuteScalar() as string;
        }
        if (stored != version) {
            // COL DB changed (or first run) — drop any stale enrichment.
            using var drop = conn.CreateCommand();
            drop.CommandText = "DROP TABLE IF EXISTS col_enrichment";
            drop.ExecuteNonQuery();
        }
        using (var create = conn.CreateCommand()) {
            create.CommandText = "CREATE TABLE IF NOT EXISTS col_enrichment (" +
                "key TEXT PRIMARY KEY, found INTEGER NOT NULL, subkingdom TEXT, subphylum TEXT, subclass TEXT, " +
                "suborder TEXT, superfamily TEXT, subfamily TEXT, tribe TEXT, subtribe TEXT, subgenus TEXT, col_id TEXT, col_scientific_name TEXT)";
            create.ExecuteNonQuery();
        }
        using (var setv = conn.CreateCommand()) {
            setv.CommandText = "INSERT OR REPLACE INTO cache_meta (k,v) VALUES ('col_version', @v)";
            setv.Parameters.AddWithValue("@v", version);
            setv.ExecuteNonQuery();
        }
    }

    private static string ColVersionStamp(string colDatabasePath) {
        var fi = new FileInfo(colDatabasePath);
        return $"{fi.Length}:{fi.LastWriteTimeUtc.Ticks}";
    }

    private bool TryReadPersistent(string key, out EnrichedTaxonomy taxonomy) {
        taxonomy = new EnrichedTaxonomy();
        if (_cacheSelect == null) {
            return false;
        }
        _cacheSelect.Parameters["@k"].Value = key;
        using var reader = _cacheSelect.ExecuteReader();
        if (!reader.Read()) {
            return false;
        }
        string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
        taxonomy = new EnrichedTaxonomy {
            Subkingdom = S(0), Subphylum = S(1), Subclass = S(2), Suborder = S(3),
            Superfamily = S(4), Subfamily = S(5), Tribe = S(6), Subtribe = S(7),
            Subgenus = S(8), ColId = S(9), ColScientificName = S(10),
        };
        return true;
    }

    private void WritePersistent(string key, bool found, EnrichedTaxonomy t) {
        if (_cacheInsert == null) {
            return;
        }
        try {
            var p = _cacheInsert.Parameters;
            p["@k"].Value = key;
            p["@found"].Value = found ? 1 : 0;
            p["@subkingdom"].Value = (object?)t.Subkingdom ?? DBNull.Value;
            p["@subphylum"].Value = (object?)t.Subphylum ?? DBNull.Value;
            p["@subclass"].Value = (object?)t.Subclass ?? DBNull.Value;
            p["@suborder"].Value = (object?)t.Suborder ?? DBNull.Value;
            p["@superfamily"].Value = (object?)t.Superfamily ?? DBNull.Value;
            p["@subfamily"].Value = (object?)t.Subfamily ?? DBNull.Value;
            p["@tribe"].Value = (object?)t.Tribe ?? DBNull.Value;
            p["@subtribe"].Value = (object?)t.Subtribe ?? DBNull.Value;
            p["@subgenus"].Value = (object?)t.Subgenus ?? DBNull.Value;
            p["@colid"].Value = (object?)t.ColId ?? DBNull.Value;
            p["@colsci"].Value = (object?)t.ColScientificName ?? DBNull.Value;
            _cacheInsert.ExecuteNonQuery();
        } catch {
            // Best-effort; a failed cache write just means we'll re-query COL next time.
        }
    }

    private ColTaxonRecord? FindColMatch(IucnSpeciesRecord record, CancellationToken cancellationToken) {
        // Try by components first (more precise)
        var matches = _colRepository.FindByComponents(
            record.GenusName, 
            record.SpeciesName, 
            record.InfraName, 
            cancellationToken);

        // Filter to accepted names in the same kingdom
        var accepted = matches
            .Where(m => IsAccepted(m) && KingdomMatches(m, record.KingdomName))
            .ToList();

        if (accepted.Count > 0) {
            return accepted[0];
        }

        // Fall back to scientific name search
        var scientificName = record.ScientificNameTaxonomy 
            ?? record.ScientificNameAssessments 
            ?? ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);

        if (string.IsNullOrWhiteSpace(scientificName)) {
            return null;
        }

        matches = _colRepository.FindByScientificName(scientificName, cancellationToken);
        accepted = matches
            .Where(m => IsAccepted(m) && KingdomMatches(m, record.KingdomName))
            .ToList();

        return accepted.Count > 0 ? accepted[0] : null;
    }

    private static bool IsAccepted(ColTaxonRecord record) {
        return string.IsNullOrWhiteSpace(record.Status) 
            || record.Status.Equals("accepted", StringComparison.OrdinalIgnoreCase);
    }

    private static bool KingdomMatches(ColTaxonRecord colRecord, string iucnKingdom) {
        if (string.IsNullOrWhiteSpace(colRecord.Kingdom) || string.IsNullOrWhiteSpace(iucnKingdom)) {
            return true; // Can't verify, assume match
        }
        return colRecord.Kingdom.Equals(iucnKingdom, StringComparison.OrdinalIgnoreCase);
    }

    private static EnrichedTaxonomy ExtractTaxonomy(ColTaxonRecord col) {
        return new EnrichedTaxonomy {
            Subkingdom = col.Subkingdom,
            Subphylum = col.Subphylum,
            Superclass = null, // COL doesn't have this in standard export
            Subclass = col.Subclass,
            Infraclass = null,
            Superorder = null,
            Suborder = col.Suborder,
            Infraorder = null,
            Parvorder = null,
            Superfamily = col.Superfamily,
            Subfamily = col.Subfamily,
            Tribe = col.Tribe,
            Subtribe = col.Subtribe,
            Subgenus = col.Subgenus,
            ColId = col.Id,
            ColScientificName = col.ScientificName
        };
    }

    private static string BuildCacheKey(IucnSpeciesRecord record) {
        var parts = new[] {
            record.GenusName?.ToLowerInvariant(),
            record.SpeciesName?.ToLowerInvariant(),
            record.InfraName?.ToLowerInvariant()
        };
        return string.Join("|", parts.Where(p => !string.IsNullOrWhiteSpace(p)));
    }

    private static EnrichedSpeciesRecord CreateEnrichedRecord(IucnSpeciesRecord iucn, EnrichedTaxonomy col) {
        return new EnrichedSpeciesRecord(
            // Original IUCN fields
            TaxonId: iucn.TaxonId,
            AssessmentId: iucn.AssessmentId,
            RedlistCategory: iucn.RedlistCategory,
            StatusCode: iucn.StatusCode,
            ScientificNameAssessments: iucn.ScientificNameAssessments,
            ScientificNameTaxonomy: iucn.ScientificNameTaxonomy,
            KingdomName: iucn.KingdomName,
            PhylumName: iucn.PhylumName,
            ClassName: iucn.ClassName,
            OrderName: iucn.OrderName,
            FamilyName: iucn.FamilyName,
            GenusName: iucn.GenusName,
            SpeciesName: iucn.SpeciesName,
            InfraType: iucn.InfraType,
            InfraName: iucn.InfraName,
            SubpopulationName: iucn.SubpopulationName,
            Scopes: iucn.Scopes,
            Authority: iucn.Authority,
            InfraAuthority: iucn.InfraAuthority,
            PossiblyExtinct: iucn.PossiblyExtinct,
            PossiblyExtinctInTheWild: iucn.PossiblyExtinctInTheWild,
            YearPublished: iucn.YearPublished,
            // COL-enriched fields
            Subkingdom: col.Subkingdom,
            Subphylum: col.Subphylum,
            Superclass: col.Superclass,
            Subclass: col.Subclass,
            Infraclass: col.Infraclass,
            Superorder: col.Superorder,
            Suborder: col.Suborder,
            Infraorder: col.Infraorder,
            Parvorder: col.Parvorder,
            Superfamily: col.Superfamily,
            Subfamily: col.Subfamily,
            Tribe: col.Tribe,
            Subtribe: col.Subtribe,
            Subgenus: col.Subgenus,
            ColId: col.ColId,
            ColScientificName: col.ColScientificName
        );
    }

    public void Dispose() {
        _cacheSelect?.Dispose();
        _cacheInsert?.Dispose();
        _cacheConn?.Dispose();
        if (_ownsConnection) {
            _connection.Dispose();
        }
    }
}

/// <summary>
/// Additional taxonomy fields from COL that aren't in IUCN.
/// </summary>
internal sealed class EnrichedTaxonomy {
    public string? Subkingdom { get; init; }
    public string? Subphylum { get; init; }
    public string? Superclass { get; init; }
    public string? Subclass { get; init; }
    public string? Infraclass { get; init; }
    public string? Superorder { get; init; }
    public string? Suborder { get; init; }
    public string? Infraorder { get; init; }
    public string? Parvorder { get; init; }
    public string? Superfamily { get; init; }
    public string? Subfamily { get; init; }
    public string? Tribe { get; init; }
    public string? Subtribe { get; init; }
    public string? Subgenus { get; init; }
    public string? ColId { get; init; }
    public string? ColScientificName { get; init; }
}

/// <summary>
/// IUCN species record enriched with additional COL taxonomy data.
/// </summary>
internal sealed record EnrichedSpeciesRecord(
    // Original IUCN fields
    long TaxonId,
    long AssessmentId,
    string RedlistCategory,
    string StatusCode,
    string? ScientificNameAssessments,
    string? ScientificNameTaxonomy,
    string KingdomName,
    string? PhylumName,
    string? ClassName,
    string? OrderName,
    string? FamilyName,
    string GenusName,
    string SpeciesName,
    string? InfraType,
    string? InfraName,
    string? SubpopulationName,
    string? Scopes,
    string? Authority,
    string? InfraAuthority,
    string? PossiblyExtinct,
    string? PossiblyExtinctInTheWild,
    string? YearPublished,
    // COL-enriched fields
    string? Subkingdom,
    string? Subphylum,
    string? Superclass,
    string? Subclass,
    string? Infraclass,
    string? Superorder,
    string? Suborder,
    string? Infraorder,
    string? Parvorder,
    string? Superfamily,
    string? Subfamily,
    string? Tribe,
    string? Subtribe,
    string? Subgenus,
    string? ColId,
    string? ColScientificName
) {
    /// <summary>
    /// Convert back to IucnSpeciesRecord for compatibility.
    /// </summary>
    public IucnSpeciesRecord ToIucnRecord() => new(
        TaxonId, AssessmentId, RedlistCategory, StatusCode,
        ScientificNameAssessments, ScientificNameTaxonomy,
        KingdomName, PhylumName, ClassName, OrderName, FamilyName,
        GenusName, SpeciesName, InfraType, InfraName, SubpopulationName,
        Scopes, Authority, InfraAuthority, PossiblyExtinct, PossiblyExtinctInTheWild, YearPublished
    );
}
