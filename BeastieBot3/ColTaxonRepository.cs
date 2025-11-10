using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class ColTaxonRepository {
    private readonly SqliteConnection _connection;
    private readonly ReadOnlyDictionary<string, string> _columnLookup;
    private readonly string _selectClause;
    private readonly Dictionary<string, IReadOnlyList<ColTaxonRecord>> _scientificNameCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IReadOnlyList<ColTaxonRecord>> _componentsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ColTaxonRecord> _idCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyList<ColTaxonRecord>> _parentChainCache = new(StringComparer.Ordinal);

    public ColTaxonRepository(SqliteConnection connection) {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _columnLookup = LoadColumnMap();
        _selectClause = BuildSelectClause();
    }

    public IReadOnlyList<ColTaxonRecord> FindByScientificName(string scientificName, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(scientificName)) {
            return Array.Empty<ColTaxonRecord>();
        }

        var key = scientificName.Trim();
        if (_scientificNameCache.TryGetValue(key, out var cached)) {
            return cached;
        }

        using var command = _connection.CreateCommand();
        command.CommandText = $"{_selectClause}\nFROM nameusage\nWHERE scientificName = @name COLLATE NOCASE\nAND scientificName IS NOT NULL";
        command.Parameters.AddWithValue("@name", scientificName.Trim());
        command.CommandTimeout = 0;
        var results = Execute(command, cancellationToken);
        var readOnly = ToReadOnly(results);
        _scientificNameCache[key] = readOnly;
        return readOnly;
    }

    public IReadOnlyList<ColTaxonRecord> FindByComponents(string genus, string species, string? infraEpithet, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return Array.Empty<ColTaxonRecord>();
        }

        var key = BuildComponentsKey(genus, species, infraEpithet);
        if (_componentsCache.TryGetValue(key, out var cached)) {
            return cached;
        }

        var whereBuilder = new StringBuilder();
        whereBuilder.Append("genericName = @genus COLLATE NOCASE\n  AND specificEpithet = @species COLLATE NOCASE");

        var parameters = new List<SqliteParameter> {
            new("@genus", genus.Trim()),
            new("@species", species.Trim())
        };

        if (!string.IsNullOrWhiteSpace(infraEpithet)) {
            whereBuilder.Append("\n  AND infraspecificEpithet = @infra COLLATE NOCASE");
            parameters.Add(new SqliteParameter("@infra", infraEpithet.Trim()));
        } else {
            whereBuilder.Append("\n  AND (infraspecificEpithet IS NULL OR infraspecificEpithet = '')");
        }

        using var command = _connection.CreateCommand();
        command.CommandText = $"{_selectClause}\nFROM nameusage\nWHERE {whereBuilder}\n  AND scientificName IS NOT NULL";
        foreach (var parameter in parameters) {
            command.Parameters.Add(parameter);
        }
        command.CommandTimeout = 0;
        var results = Execute(command, cancellationToken);
        var readOnly = ToReadOnly(results);
        _componentsCache[key] = readOnly;
        return readOnly;
    }

    public ColTaxonRecord? GetById(string? id, CancellationToken cancellationToken) {
        if (string.IsNullOrWhiteSpace(id)) {
            return null;
        }

        var trimmed = id.Trim();
        if (_idCache.TryGetValue(trimmed, out var cached)) {
            return cached;
        }

        using var command = _connection.CreateCommand();
        command.CommandText = $"{_selectClause}\nFROM nameusage\nWHERE id = @id\nLIMIT 1";
        command.Parameters.AddWithValue("@id", trimmed);
        command.CommandTimeout = 0;

        var list = Execute(command, cancellationToken);
        if (list.Count == 0) {
            return null;
        }

        var record = list[0];
        _idCache[record.Id] = record;
        return record;
    }

    public IReadOnlyList<ColTaxonRecord> GetParentChain(ColTaxonRecord record, CancellationToken cancellationToken) {
        if (record is null) {
            throw new ArgumentNullException(nameof(record));
        }

        if (_parentChainCache.TryGetValue(record.Id, out var cached)) {
            return cached;
        }

        var chain = new List<ColTaxonRecord>();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var current = record;

        while (current is not null && visited.Add(current.Id)) {
            cancellationToken.ThrowIfCancellationRequested();
            chain.Add(current);

            if (string.IsNullOrWhiteSpace(current.ParentId)) {
                break;
            }

            current = GetById(current.ParentId, cancellationToken);
        }

        chain.Reverse();
        var readOnly = ToReadOnly(chain);
        CacheParentChain(chain, readOnly);
        return readOnly;
    }

    private List<ColTaxonRecord> Execute(SqliteCommand command, CancellationToken cancellationToken) {
        var results = new List<ColTaxonRecord>();
        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        if (!reader.HasRows) {
            return results;
        }

        var ordinals = new Ordinals(reader);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            var id = Convert.ToString(reader.GetValue(ordinals.Id), CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(id)) {
                continue;
            }

            var scientificName = reader.IsDBNull(ordinals.ScientificName) ? string.Empty : reader.GetString(ordinals.ScientificName);
            if (scientificName.Length == 0) {
                continue;
            }

            var record = new ColTaxonRecord(
                id,
                scientificName,
                ReadString(reader, ordinals.Authorship),
                ReadString(reader, ordinals.Status),
                ReadString(reader, ordinals.Rank),
                ReadString(reader, ordinals.AcceptedNameUsageId),
                ReadString(reader, ordinals.ParentId),
                ReadString(reader, ordinals.Kingdom),
                ReadString(reader, ordinals.Subkingdom),
                ReadString(reader, ordinals.Phylum),
                ReadString(reader, ordinals.Subphylum),
                ReadString(reader, ordinals.Class),
                ReadString(reader, ordinals.Subclass),
                ReadString(reader, ordinals.Order),
                ReadString(reader, ordinals.Suborder),
                ReadString(reader, ordinals.Superfamily),
                ReadString(reader, ordinals.Family),
                ReadString(reader, ordinals.Subfamily),
                ReadString(reader, ordinals.Tribe),
                ReadString(reader, ordinals.Subtribe),
                ReadString(reader, ordinals.Genus),
                ReadString(reader, ordinals.Subgenus),
                ReadString(reader, ordinals.SpecificEpithet),
                ReadString(reader, ordinals.InfraspecificEpithet)
            );

            results.Add(record);
            _idCache[record.Id] = record;
        }

        return results;
    }

    private static string? ReadString(SqliteDataReader reader, int? ordinal) {
        if (!ordinal.HasValue || reader.IsDBNull(ordinal.Value)) {
            return null;
        }

        return reader.GetString(ordinal.Value);
    }

    private static string BuildComponentsKey(string genus, string species, string? infraEpithet) {
        var builder = new StringBuilder();
        builder.Append(genus.Trim().ToLowerInvariant());
        builder.Append('|');
        builder.Append(species.Trim().ToLowerInvariant());
        builder.Append('|');
        if (!string.IsNullOrWhiteSpace(infraEpithet)) {
            builder.Append(infraEpithet.Trim().ToLowerInvariant());
        }

        return builder.ToString();
    }

    private static IReadOnlyList<ColTaxonRecord> ToReadOnly(List<ColTaxonRecord> records) {
        if (records.Count == 0) {
            return Array.Empty<ColTaxonRecord>();
        }

        return Array.AsReadOnly(records.ToArray());
    }

    private void CacheParentChain(List<ColTaxonRecord> chain, IReadOnlyList<ColTaxonRecord> readOnly) {
        if (chain.Count == 0) {
            return;
        }

        for (var i = 0; i < chain.Count; i++) {
            var record = chain[i];
            if (_parentChainCache.ContainsKey(record.Id)) {
                continue;
            }

            if (i == chain.Count - 1) {
                _parentChainCache[record.Id] = readOnly;
            } else {
                var slice = chain.Take(i + 1).ToList();
                _parentChainCache[record.Id] = ToReadOnly(slice);
            }
        }
    }

    private ReadOnlyDictionary<string, string> LoadColumnMap() {
        using var command = _connection.CreateCommand();
        command.CommandText = "PRAGMA table_info(nameusage);";
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            if (reader.IsDBNull(1)) {
                continue;
            }

            var name = reader.GetString(1);
            if (!string.IsNullOrWhiteSpace(name)) {
                map[name] = name;
            }
        }

        return new ReadOnlyDictionary<string, string>(map);
    }

    private string BuildSelectClause() {
        var columns = new List<string> {
            "id",
            MapColumn("scientificName", "scientificName")
        };

        columns.Add(MapColumnWithFallback("authorship", new[] { "authorship", "scientificNameAuthorship" }));
        columns.Add(MapColumn("status", "status"));
        columns.Add(MapColumn("rank", "rank"));
        columns.Add(MapColumn("acceptedNameUsageID", "acceptedNameUsageID", "acceptedNameUsageId", "acceptedNameUsage"));
        columns.Add(MapColumn("parentID", "parentID", "parentId"));
    columns.Add(MapColumn("kingdom", "kingdom"));
    columns.Add(MapColumn("subkingdom", "subkingdom", "subKingdom"));
    columns.Add(MapColumn("phylum", "phylum", "division"));
    columns.Add(MapColumn("subphylum", "subphylum", "subPhylum"));
    columns.Add(MapColumn("class", "class"));
    columns.Add(MapColumn("subclass", "subclass", "subClass"));
    columns.Add(MapColumn("order", "order"));
    columns.Add(MapColumn("suborder", "suborder", "subOrder"));
    columns.Add(MapColumn("superfamily", "superfamily", "superFamily"));
    columns.Add(MapColumn("family", "family"));
    columns.Add(MapColumn("subfamily", "subfamily", "subFamily"));
    columns.Add(MapColumn("tribe", "tribe"));
    columns.Add(MapColumn("subtribe", "subtribe", "subTribe"));
    columns.Add(MapColumn("genericName", "genericName", "genus"));
    columns.Add(MapColumn("subgenus", "subgenus", "infragenericEpithet"));
    columns.Add(MapColumn("specificEpithet", "specificEpithet", "species"));
    columns.Add(MapColumn("infraspecificEpithet", "infraspecificEpithet", "infraspecies"));

        return "SELECT\n  " + string.Join(",\n  ", columns);
    }

    private string MapColumn(string alias, params string[] candidates) {
        foreach (var candidate in candidates) {
            if (_columnLookup.ContainsKey(candidate)) {
                return Quote(candidate) + " AS " + Quote(alias);
            }
        }

        return "NULL AS " + Quote(alias);
    }

    private string MapColumnWithFallback(string alias, IReadOnlyList<string> candidates) {
        return MapColumn(alias, candidates.ToArray());
    }

    private static string Quote(string identifier) {
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private sealed class Ordinals {
        public Ordinals(SqliteDataReader reader) {
            Id = reader.GetOrdinal("id");
            ScientificName = reader.GetOrdinal("scientificName");
            Authorship = TryGetOrdinal(reader, "authorship");
            Status = TryGetOrdinal(reader, "status");
            Rank = TryGetOrdinal(reader, "rank");
            AcceptedNameUsageId = TryGetOrdinal(reader, "acceptedNameUsageID");
            ParentId = TryGetOrdinal(reader, "parentID");
            Kingdom = TryGetOrdinal(reader, "kingdom");
            Subkingdom = TryGetOrdinal(reader, "subkingdom");
            Phylum = TryGetOrdinal(reader, "phylum");
            Subphylum = TryGetOrdinal(reader, "subphylum");
            Class = TryGetOrdinal(reader, "class");
            Subclass = TryGetOrdinal(reader, "subclass");
            Order = TryGetOrdinal(reader, "order");
            Suborder = TryGetOrdinal(reader, "suborder");
            Superfamily = TryGetOrdinal(reader, "superfamily");
            Family = TryGetOrdinal(reader, "family");
            Subfamily = TryGetOrdinal(reader, "subfamily");
            Tribe = TryGetOrdinal(reader, "tribe");
            Subtribe = TryGetOrdinal(reader, "subtribe");
            Genus = TryGetOrdinal(reader, "genericName");
            Subgenus = TryGetOrdinal(reader, "subgenus");
            SpecificEpithet = TryGetOrdinal(reader, "specificEpithet");
            InfraspecificEpithet = TryGetOrdinal(reader, "infraspecificEpithet");
        }

        public int Id { get; }
        public int ScientificName { get; }
        public int? Authorship { get; }
        public int? Status { get; }
        public int? Rank { get; }
        public int? AcceptedNameUsageId { get; }
        public int? ParentId { get; }
        public int? Kingdom { get; }
        public int? Subkingdom { get; }
        public int? Phylum { get; }
        public int? Subphylum { get; }
        public int? Class { get; }
        public int? Subclass { get; }
        public int? Order { get; }
        public int? Suborder { get; }
        public int? Superfamily { get; }
        public int? Family { get; }
        public int? Subfamily { get; }
        public int? Tribe { get; }
        public int? Subtribe { get; }
        public int? Genus { get; }
        public int? Subgenus { get; }
        public int? SpecificEpithet { get; }
        public int? InfraspecificEpithet { get; }

        private static int? TryGetOrdinal(SqliteDataReader reader, string column) {
            try {
                return reader.GetOrdinal(column);
            } catch (IndexOutOfRangeException) {
                return null;
            }
        }
    }
}

internal sealed record ColTaxonRecord(
    string Id,
    string ScientificName,
    string? Authorship,
    string? Status,
    string? Rank,
    string? AcceptedNameUsageId,
    string? ParentId,
    string? Kingdom,
    string? Subkingdom,
    string? Phylum,
    string? Subphylum,
    string? Class,
    string? Subclass,
    string? Order,
    string? Suborder,
    string? Superfamily,
    string? Family,
    string? Subfamily,
    string? Tribe,
    string? Subtribe,
    string? Genus,
    string? Subgenus,
    string? SpecificEpithet,
    string? InfraspecificEpithet
);
