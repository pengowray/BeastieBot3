using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Data.Sqlite;
using BeastieBot3.Infrastructure;
using BeastieBot3.WikipediaLists;

// Reads the imported SPRAT table and produces renderer-ready IucnSpeciesRecord rows for the Australia
// threatened-species lists. SPRAT is the species universe (every taxon notable under an Australian
// system); this service applies the taxonomy filter in SQL, then in C# decides membership and the
// section-driving status. Phase-1 membership = threatened (CR/EN/VU) under EPBC OR IUCN; the
// section is driven by the EPBC status, falling back to the IUCN status for the handful of taxa that
// are IUCN-threatened but not EPBC-listed. The full multi-system status (EPBC, IUCN, and the eight
// state/territory acts) is rendered as an inline annotation on every line.

namespace BeastieBot3.Sprat;

internal sealed class SpratListQueryService : IDisposable {
    private readonly SqliteConnection _connection;
    private readonly bool _ownsConnection;
    // The system columns that actually exist in this DB. Guards against SQLite's quirk where a
    // double-quoted identifier that matches no column is silently treated as a string literal — a
    // missing/renamed status column would otherwise inject its own name into the annotation.
    private readonly IReadOnlyList<SpratColumns.ListingSystem> _systems;

    public SpratListQueryService(string databasePath) {
        if (string.IsNullOrWhiteSpace(databasePath)) {
            throw new ArgumentException("SPRAT database path was not provided.", nameof(databasePath));
        }
        var fullPath = Path.GetFullPath(databasePath);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"SPRAT SQLite database not found: {fullPath}. Run 'sprat import' first.", fullPath);
        }
        var builder = new SqliteConnectionStringBuilder { DataSource = fullPath, Mode = SqliteOpenMode.ReadOnly };
        _connection = new SqliteConnection(builder.ToString());
        _connection.Open();
        _ownsConnection = true;
        _systems = ResolveAvailableSystems(_connection);
    }

    private SpratListQueryService(SqliteConnection connection) {
        _connection = connection;
        _systems = ResolveAvailableSystems(connection);
    }

    /// <summary>Test seam: query over a caller-owned connection (e.g. a shared <c>:memory:</c> DB).</summary>
    internal static SpratListQueryService OpenFromConnection(SqliteConnection connection) => new(connection);

    private static IReadOnlyList<SpratColumns.ListingSystem> ResolveAvailableSystems(SqliteConnection connection) {
        var existing = DelimitedTableImporter.GetTableColumns(connection, SpratColumns.Table)
            ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        return SpratColumns.Systems.Where(s => existing.Contains(s.Column)).ToList();
    }

    public string GetDatasetVersion() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT redlist_version FROM import_metadata WHERE ended_at IS NOT NULL ORDER BY id DESC LIMIT 1";
        return cmd.ExecuteScalar()?.ToString() ?? "unknown";
    }

    /// <summary>
    /// Qualifying threatened taxa for a group, each as an IucnSpeciesRecord whose StatusCode is the
    /// primary Australian status (CR/EN/VU) that drives its section, with the SPRAT vernacular and the
    /// multi-system annotation attached. <paramref name="limit"/> caps the returned (post-membership) set.
    /// </summary>
    public IReadOnlyList<IucnSpeciesRecord> Query(SpratTaxonFilter filter, int? limit = null) {
        // Fixed taxonomy/identity columns (ordinals 0..8), then the available per-system status
        // columns in annotation order.
        var taxonomyCols = new[] {
            SpratColumns.SpratTaxonId, SpratColumns.ScientificName, SpratColumns.CommonName,
            SpratColumns.Kingdom, SpratColumns.Phylum, SpratColumns.ClassName,
            SpratColumns.OrderName, SpratColumns.Family, SpratColumns.Genus,
        };
        const int sysOffset = 9;
        var selectCols = taxonomyCols.Concat(_systems.Select(s => s.Column)).ToList();

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(string.Join(", ", selectCols.Select(Quote)));
        sql.Append(" FROM ").Append(Quote(SpratColumns.Table));
        var parameters = new List<SqliteParameter>();
        AppendWhere(sql, filter, parameters);
        sql.Append(" ORDER BY ")
           .Append($"{Quote(SpratColumns.OrderName)}, {Quote(SpratColumns.Family)}, {Quote(SpratColumns.Genus)}, {Quote(SpratColumns.ScientificName)}");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql.ToString();
        foreach (var p in parameters) {
            cmd.Parameters.Add(p);
        }

        var epbcIdx = IndexOfSystem("epbc");

        var results = new List<IucnSpeciesRecord>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            // Short code per available system, in _systems order.
            var codes = new string?[_systems.Count];
            for (var i = 0; i < _systems.Count; i++) {
                codes[i] = AustralianStatus.ShortCode(GetString(reader, sysOffset + i));
            }

            // Section-driving status, which also decides membership (null = no qualifying status under
            // any system). The EPBC threatened category takes priority (the national headline);
            // otherwise the most-severe qualifying status across IUCN and the state/territory acts.
            var epbcCode = epbcIdx >= 0 ? codes[epbcIdx] : null;
            var primaryCode = epbcCode is "CR" or "EN" or "VU"
                ? epbcCode
                : AustralianStatus.MostSevereQualifyingCode(codes);
            if (primaryCode is null) {
                continue;
            }

            results.Add(BuildRecord(reader, primaryCode, sysOffset));
            if (limit is > 0 && results.Count >= limit.Value) {
                break;
            }
        }
        return results;
    }

    private IucnSpeciesRecord BuildRecord(SqliteDataReader reader, string primaryCode, int sysOffset) {
        var idRaw = GetString(reader, 0);
        var scientific = GetString(reader, 1);
        var commonRaw = GetString(reader, 2);
        var kingdom = GetString(reader, 3);
        var phylum = GetString(reader, 4);
        var className = GetString(reader, 5);
        var orderName = GetString(reader, 6);
        var family = GetString(reader, 7);
        var genusCol = GetString(reader, 8);

        var (genus, species, infraType, infraName) = ParseName(scientific, genusCol, kingdom);
        var annotation = BuildAnnotation(reader, sysOffset);
        var descriptor = IucnRedlistStatus.Describe(primaryCode);
        long.TryParse(idRaw, out var taxonId);

        return new IucnSpeciesRecord(
            TaxonId: taxonId,
            AssessmentId: 0,
            RedlistCategory: descriptor.Category,
            StatusCode: primaryCode,
            ScientificNameAssessments: null,
            ScientificNameTaxonomy: scientific,
            KingdomName: kingdom ?? string.Empty,
            PhylumName: phylum,
            ClassName: className,
            OrderName: orderName,
            FamilyName: family,
            GenusName: genus ?? string.Empty,
            SpeciesName: species ?? string.Empty,
            InfraType: infraType,
            InfraName: infraName,
            SubpopulationName: null,
            Scopes: null,
            Authority: null,
            InfraAuthority: null,
            PossiblyExtinct: null,
            PossiblyExtinctInTheWild: null,
            YearPublished: null,
            CommonNameOverride: CleanCommonName(commonRaw),
            StatusAnnotation: annotation);
    }

    // "EPBC: CR; IUCN: CR; WA: CR" — every available system with a non-blank status, in order.
    private string? BuildAnnotation(SqliteDataReader reader, int sysOffset) {
        var parts = new List<string>();
        for (var i = 0; i < _systems.Count; i++) {
            var code = AustralianStatus.ShortCode(GetString(reader, sysOffset + i));
            if (code is not null) {
                parts.Add($"{_systems[i].Label}: {code}");
            }
        }
        return parts.Count > 0 ? string.Join("; ", parts) : null;
    }

    /// <summary>
    /// Parses a SPRAT scientific name into (genus, species, infraType, infraName). Handles the
    /// explicit botanical rank markers ("var.", "subsp."/"ssp.", "f.") and the bare zoological
    /// trinomial ("Acanthiza iredalei hedleyi" → subspecies). The SPRAT Genus column is authoritative
    /// for the genus. Done here rather than via ScientificNameNormalizer.ParseScientificName because
    /// that helper's word-boundary regex doesn't match a standalone "var."/"subsp." token.
    /// </summary>
    internal static (string? Genus, string? Species, string? InfraType, string? InfraName) ParseName(
        string? scientific, string? genusColumn, string? kingdom) {
        var tokens = (scientific ?? string.Empty).Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var genus = !string.IsNullOrWhiteSpace(genusColumn) ? genusColumn
            : tokens.Length > 0 ? tokens[0] : null;
        var species = tokens.Length > 1 ? tokens[1] : null;

        for (var i = 2; i < tokens.Length; i++) {
            var marker = NormalizeRankMarker(tokens[i]);
            if (marker is not null) {
                var infra = i + 1 < tokens.Length ? tokens[i + 1] : null;
                return (genus, species, marker, infra);
            }
        }

        // No explicit marker: a lowercase third epithet on an animal is a subspecies (zoology has no
        // var.); plants without a marker (informal "sp." phrase names) stay at species rank.
        var isAnimal = string.Equals(kingdom, "Animalia", StringComparison.OrdinalIgnoreCase);
        if (isAnimal && tokens.Length >= 3 && IsEpithet(tokens[2])) {
            return (genus, species, "ssp.", tokens[2]);
        }
        return (genus, species, null, null);
    }

    private static string? NormalizeRankMarker(string token) {
        var t = token.ToLowerInvariant().TrimEnd('.');
        return t switch {
            "var" => "var.",
            "subsp" or "ssp" or "subspecies" => "subsp.",
            "f" or "fo" or "forma" or "form" => "f.",
            "subf" or "subforma" => "subf.",
            "nothosubsp" => "nothosubsp.",
            "nothovar" => "nothovar.",
            _ => null,
        };
    }

    private static bool IsEpithet(string token) =>
        token.Length > 1 && char.IsLower(token[0]) && !token.Contains('.') && !token.Contains('(');

    // SPRAT often lists several vernaculars ("Ploughshare Wattle, Dog's Tooth Wattle"); take the first.
    // Generic group labels used in place of a true vernacular are rejected (see IsGenericDescriptor).
    private static string? CleanCommonName(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return null;
        }
        var first = raw.Split(',')[0].Trim();
        return string.IsNullOrWhiteSpace(first) || IsGenericDescriptor(first) ? null : first;
    }

    private static readonly System.Text.RegularExpressions.Regex IndefiniteArticlePhrase =
        new(@"^an?\s", System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    /// <summary>
    /// True when a SPRAT "common name" is really a generic group label rather than a true vernacular —
    /// e.g. "a shrub", "an orchid", "fern", "land snail", "a camaenid land snail". SPRAT writes these
    /// all-lower-case (real vernaculars are Title Case) or as an indefinite-article phrase, so taxa
    /// carrying one should fall back to their scientific name (or a hub-resolved name).
    /// </summary>
    internal static bool IsGenericDescriptor(string name) {
        var trimmed = name.Trim();
        if (trimmed.Length == 0) {
            return false;
        }
        if (IndefiniteArticlePhrase.IsMatch(trimmed)) {
            return true;
        }
        // Entirely lower-case (has letters, none upper-case) → a descriptor, not a proper name.
        var hasLetter = false;
        foreach (var ch in trimmed) {
            if (char.IsLetter(ch)) {
                if (char.IsUpper(ch)) {
                    return false;
                }
                hasLetter = true;
            }
        }
        return hasLetter;
    }

    private static void AppendWhere(StringBuilder sql, SpratTaxonFilter filter, List<SqliteParameter> parameters) {
        var clauses = new List<string>();
        if (!string.IsNullOrWhiteSpace(filter.Kingdom)) {
            var p = new SqliteParameter("@kingdom", filter.Kingdom);
            parameters.Add(p);
            clauses.Add($"{Quote(SpratColumns.Kingdom)} = {p.ParameterName}");
        }
        if (filter.Classes is { Count: > 0 }) {
            var names = new List<string>();
            for (var i = 0; i < filter.Classes.Count; i++) {
                var p = new SqliteParameter($"@class{i}", filter.Classes[i]);
                parameters.Add(p);
                names.Add(p.ParameterName);
            }
            clauses.Add($"{Quote(SpratColumns.ClassName)} IN ({string.Join(", ", names)})");
        }
        if (filter.ExcludeClasses is { Count: > 0 }) {
            var names = new List<string>();
            for (var i = 0; i < filter.ExcludeClasses.Count; i++) {
                var p = new SqliteParameter($"@xclass{i}", filter.ExcludeClasses[i]);
                parameters.Add(p);
                names.Add(p.ParameterName);
            }
            clauses.Add($"({Quote(SpratColumns.ClassName)} IS NULL OR {Quote(SpratColumns.ClassName)} NOT IN ({string.Join(", ", names)}))");
        }
        if (filter.Orders is { Count: > 0 }) {
            var names = new List<string>();
            for (var i = 0; i < filter.Orders.Count; i++) {
                var p = new SqliteParameter($"@order{i}", filter.Orders[i]);
                parameters.Add(p);
                names.Add(p.ParameterName);
            }
            clauses.Add($"{Quote(SpratColumns.OrderName)} IN ({string.Join(", ", names)})");
        }
        if (filter.ExcludeOrders is { Count: > 0 }) {
            var names = new List<string>();
            for (var i = 0; i < filter.ExcludeOrders.Count; i++) {
                var p = new SqliteParameter($"@xorder{i}", filter.ExcludeOrders[i]);
                parameters.Add(p);
                names.Add(p.ParameterName);
            }
            clauses.Add($"({Quote(SpratColumns.OrderName)} IS NULL OR {Quote(SpratColumns.OrderName)} NOT IN ({string.Join(", ", names)}))");
        }
        if (filter.ExcludePhyla is { Count: > 0 }) {
            var names = new List<string>();
            for (var i = 0; i < filter.ExcludePhyla.Count; i++) {
                var p = new SqliteParameter($"@xphylum{i}", filter.ExcludePhyla[i]);
                parameters.Add(p);
                names.Add(p.ParameterName);
            }
            clauses.Add($"({Quote(SpratColumns.Phylum)} IS NULL OR {Quote(SpratColumns.Phylum)} NOT IN ({string.Join(", ", names)}))");
        }
        if (clauses.Count > 0) {
            sql.Append(" WHERE ").Append(string.Join(" AND ", clauses));
        }
    }

    private int IndexOfSystem(string key) {
        for (var i = 0; i < _systems.Count; i++) {
            if (_systems[i].Key == key) {
                return i;
            }
        }
        return -1;
    }

    private static string? GetString(SqliteDataReader reader, int ordinal) {
        if (reader.IsDBNull(ordinal)) {
            return null;
        }
        var s = reader.GetString(ordinal);
        return string.IsNullOrWhiteSpace(s) ? null : s;
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    public void Dispose() {
        if (_ownsConnection) {
            _connection.Dispose();
        }
    }
}
