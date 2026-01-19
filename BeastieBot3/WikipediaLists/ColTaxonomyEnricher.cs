using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;

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

    public ColTaxonomyEnricher(string colDatabasePath) {
        var builder = new SqliteConnectionStringBuilder {
            DataSource = colDatabasePath,
            Mode = SqliteOpenMode.ReadOnly
        };
        _connection = new SqliteConnection(builder.ConnectionString);
        _connection.Open();
        _colRepository = new ColTaxonRepository(_connection);
        _ownsConnection = true;
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

        // Try to find in COL
        var colMatch = FindColMatch(record, cancellationToken);
        var taxonomy = colMatch != null 
            ? ExtractTaxonomy(colMatch) 
            : new EnrichedTaxonomy();
        
        _cache[cacheKey] = taxonomy;
        return CreateEnrichedRecord(record, taxonomy);
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
        Authority, InfraAuthority, PossiblyExtinct, PossiblyExtinctInTheWild, YearPublished
    );
}
