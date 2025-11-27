using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace BeastieBot3;

internal sealed class WikidataIucnFreshnessAnalyzer {
    private readonly SqliteConnection _iucnConnection;
    private readonly SqliteConnection _wikidataConnection;
    private readonly bool _includeSubpopulations;

    public WikidataIucnFreshnessAnalyzer(SqliteConnection iucnConnection, SqliteConnection wikidataConnection, bool includeSubpopulations) {
        _iucnConnection = iucnConnection ?? throw new ArgumentNullException(nameof(iucnConnection));
        _wikidataConnection = wikidataConnection ?? throw new ArgumentNullException(nameof(wikidataConnection));
        _includeSubpopulations = includeSubpopulations;
    }

    public WikidataIucnFreshnessStats Execute(CancellationToken cancellationToken) {
        var taxa = LoadIucnTaxa(cancellationToken);
        LoadIucnStatuses(taxa, cancellationToken);

        var stats = new WikidataIucnFreshnessStats {
            TotalIucnTaxa = taxa.Count
        };

        var (entitiesWithP627, entityToTaxa, taxonHasP627) = LoadP627Matches(taxa, cancellationToken);
        stats.WikidataEntitiesWithP627 = entitiesWithP627.Count;
        stats.IucnTaxaWithDirectP627 = taxonHasP627.Count;

        var entityNames = LoadEntityNames(entitiesWithP627, cancellationToken);
        var entityMetadata = LoadEntityMetadata(entitiesWithP627, cancellationToken);
        ComputeNameMatches(taxa, entityToTaxa, entityNames, entityMetadata, stats);

        ProcessP141References(taxa, entitiesWithP627, entityMetadata, stats, cancellationToken);

        return stats;
    }

    private Dictionary<string, IucnTaxonRecord> LoadIucnTaxa(CancellationToken cancellationToken) {
        using var command = _iucnConnection.CreateCommand();
        command.CommandText = "SELECT internalTaxonId, scientificName, genusName, speciesName, infraName, infraType, subpopulationName FROM taxonomy_html";
        command.CommandTimeout = 0;

        var taxa = new Dictionary<string, IucnTaxonRecord>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            var taxonId = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrWhiteSpace(taxonId)) {
                continue;
            }

            var subpopulationName = reader.IsDBNull(6) ? null : reader.GetString(6);
            var infraType = reader.IsDBNull(5) ? null : reader.GetString(5);
            if (!_includeSubpopulations && IsPopulationOrRegional(subpopulationName, infraType)) {
                continue;
            }

            var scientificName = reader.IsDBNull(1) ? null : reader.GetString(1);
            var genus = reader.IsDBNull(2) ? null : reader.GetString(2);
            var species = reader.IsDBNull(3) ? null : reader.GetString(3);
            var infraName = reader.IsDBNull(4) ? null : reader.GetString(4);

            var canonicalName = scientificName;
            if (string.IsNullOrWhiteSpace(canonicalName)) {
                canonicalName = ScientificNameHelper.BuildFromParts(genus, species, infraName);
            }

            var normalized = ScientificNameHelper.Normalize(canonicalName);
            var comparable = TaxonNameComparer.NormalizeForExactMatch(canonicalName);
            taxa[taxonId.Trim()] = new IucnTaxonRecord(taxonId.Trim(), canonicalName?.Trim(), normalized, comparable, infraType, subpopulationName);
        }

        return taxa;
    }

    private void LoadIucnStatuses(Dictionary<string, IucnTaxonRecord> taxa, CancellationToken cancellationToken) {
        using var command = _iucnConnection.CreateCommand();
        command.CommandText = "SELECT internalTaxonId, redlistCategory, redlist_version, yearPublished FROM assessments_html";
        command.CommandTimeout = 0;

        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            var taxonId = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (string.IsNullOrWhiteSpace(taxonId)) {
                continue;
            }

            if (!taxa.TryGetValue(taxonId.Trim(), out var taxon)) {
                continue;
            }

            var category = reader.IsDBNull(1) ? null : reader.GetString(1);
            var version = reader.IsDBNull(2) ? null : reader.GetString(2);
            var yearText = reader.IsDBNull(3) ? null : reader.GetString(3);
            var versionKey = RedlistVersionKey.From(version, yearText);
            if (taxon.LatestVersion is not null && taxon.LatestVersion.Value.CompareTo(versionKey) >= 0) {
                continue;
            }

            taxon.LatestVersion = versionKey;
            taxon.LatestRedlistVersion = version;
            taxon.LatestCategoryRaw = category;
            taxon.LatestCategoryCode = IucnCategoryMapper.Normalize(category);
        }
    }

    private (HashSet<long> EntitiesWithP627, Dictionary<long, List<string>> EntityToTaxa, HashSet<string> TaxaWithP627) LoadP627Matches(
        Dictionary<string, IucnTaxonRecord> taxa,
        CancellationToken cancellationToken) {
        var entitiesWithP627 = new HashSet<long>();
        var entityToTaxa = new Dictionary<long, List<string>>();
        var taxonHasP627 = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        using var command = _wikidataConnection.CreateCommand();
        command.CommandText = "SELECT entity_numeric_id, value FROM wikidata_p627_values WHERE source='claim'";
        command.CommandTimeout = 0;

        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            var entityId = reader.GetInt64(0);
            var value = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (string.IsNullOrWhiteSpace(value)) {
                continue;
            }

            var taxonId = value.Trim();
            if (!taxa.ContainsKey(taxonId)) {
                continue;
            }

            entitiesWithP627.Add(entityId);
            taxonHasP627.Add(taxonId);

            if (!entityToTaxa.TryGetValue(entityId, out var list)) {
                list = new List<string>();
                entityToTaxa[entityId] = list;
            }

            if (!list.Any(existing => existing.Equals(taxonId, StringComparison.OrdinalIgnoreCase))) {
                list.Add(taxonId);
            }
        }

        return (entitiesWithP627, entityToTaxa, taxonHasP627);
    }

    private Dictionary<long, EntityNameCollection> LoadEntityNames(IReadOnlyCollection<long> entityIds, CancellationToken cancellationToken) {
        var result = new Dictionary<long, EntityNameCollection>();
        if (entityIds.Count == 0) {
            return result;
        }

        const int chunkSize = 500;
        var ids = entityIds.ToArray();
        for (var offset = 0; offset < ids.Length; offset += chunkSize) {
            cancellationToken.ThrowIfCancellationRequested();
            var chunk = ids.Skip(offset).Take(chunkSize).ToArray();
            using var command = _wikidataConnection.CreateCommand();
            command.CommandText = $"SELECT entity_numeric_id, name FROM wikidata_scientific_names WHERE entity_numeric_id IN ({string.Join(",", chunk.Select((_, i) => $"@p{i}"))})";
            for (var i = 0; i < chunk.Length; i++) {
                command.Parameters.AddWithValue($"@p{i}", chunk[i]);
            }

            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read()) {
                var entityId = reader.GetInt64(0);
                var name = reader.IsDBNull(1) ? null : reader.GetString(1);
                if (string.IsNullOrWhiteSpace(name)) {
                    continue;
                }

                if (!result.TryGetValue(entityId, out var collection)) {
                    collection = new EntityNameCollection();
                    result[entityId] = collection;
                }

                collection.DisplayNames.Add(name.Trim());
                var normalized = TaxonNameComparer.NormalizeForExactMatch(name);
                if (!string.IsNullOrWhiteSpace(normalized)) {
                    collection.NormalizedNames.Add(normalized);
                }
            }
        }

        return result;
    }

    private Dictionary<long, WikidataEntityMetadata> LoadEntityMetadata(IReadOnlyCollection<long> entityIds, CancellationToken cancellationToken) {
        var result = new Dictionary<long, WikidataEntityMetadata>();
        if (entityIds.Count == 0) {
            return result;
        }

        const int chunkSize = 500;
        var ids = entityIds.ToArray();
        for (var offset = 0; offset < ids.Length; offset += chunkSize) {
            cancellationToken.ThrowIfCancellationRequested();
            var chunk = ids.Skip(offset).Take(chunkSize).ToArray();
            using var command = _wikidataConnection.CreateCommand();
            command.CommandText = $"SELECT entity_numeric_id, entity_id, label_en FROM wikidata_entities WHERE entity_numeric_id IN ({string.Join(",", chunk.Select((_, i) => $"@p{i}"))})";
            for (var i = 0; i < chunk.Length; i++) {
                command.Parameters.AddWithValue($"@p{i}", chunk[i]);
            }

            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read()) {
                var numericId = reader.GetInt64(0);
                var entityId = reader.GetString(1);
                var label = reader.IsDBNull(2) ? null : reader.GetString(2);
                result[numericId] = new WikidataEntityMetadata(numericId, entityId, label);
            }
        }

        return result;
    }

    private void ComputeNameMatches(
        Dictionary<string, IucnTaxonRecord> taxa,
        Dictionary<long, List<string>> entityToTaxa,
        Dictionary<long, EntityNameCollection> entityNames,
        Dictionary<long, WikidataEntityMetadata> entityMetadata,
        WikidataIucnFreshnessStats stats) {
        foreach (var (entityId, taxonIds) in entityToTaxa) {
            entityNames.TryGetValue(entityId, out var nameCollection);
            var normalizedNames = nameCollection?.NormalizedNames ?? new HashSet<string>(StringComparer.Ordinal);

            var matched = false;
            foreach (var taxonId in taxonIds) {
                if (!taxa.TryGetValue(taxonId, out var taxon)) {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(taxon.ComparableName)) {
                    continue;
                }

                if (normalizedNames.Contains(taxon.ComparableName!)) {
                    matched = true;
                    break;
                }
            }

            if (matched) {
                stats.WikidataEntitiesWithExactNameMatch++;
                continue;
            }

            if (normalizedNames.Count == 0) {
                stats.WikidataEntitiesMissingScientificName++;
            }
            else {
                stats.WikidataEntitiesUsingSynonymName++;
            }

            if (stats.NameMismatchSamples.Count >= WikidataIucnFreshnessStats.MaxSamples) {
                continue;
            }

            entityMetadata.TryGetValue(entityId, out var metadata);
            var sample = new NameMismatchSample(
                metadata?.EntityId ?? $"Q{entityId}",
                metadata?.Label,
                taxonIds.Select(id => (
                    id,
                    taxa.TryGetValue(id, out var taxon) ? taxon.CanonicalName : null)).ToList(),
                nameCollection?.DisplayNames ?? new List<string>());
            stats.NameMismatchSamples.Add(sample);
        }
    }

    private void ProcessP141References(
        Dictionary<string, IucnTaxonRecord> taxa,
        HashSet<long> entitiesWithP627,
        Dictionary<long, WikidataEntityMetadata> entityMetadata,
        WikidataIucnFreshnessStats stats,
        CancellationToken cancellationToken) {
        using var command = _wikidataConnection.CreateCommand();
        command.CommandText = @"SELECT r.entity_numeric_id, e.entity_id, r.statement_id, r.reference_hash, r.source_qid, r.iucn_taxon_id, s.status_entity_id
FROM wikidata_p141_references r
JOIN wikidata_p141_statements s ON s.entity_numeric_id = r.entity_numeric_id AND s.statement_id = r.statement_id
JOIN wikidata_entities e ON e.entity_numeric_id = r.entity_numeric_id";
        command.CommandTimeout = 0;

        var referenceRecords = new List<P141ReferenceRecord>();
        var entityIdsNeedingJson = new HashSet<long>();
        using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();

            var entityNumericId = reader.GetInt64(0);
            var statementId = reader.GetString(2);
            var referenceHash = reader.GetString(3);
            long? sourceQid = reader.IsDBNull(4) ? null : reader.GetInt64(4);
            var taxonId = reader.IsDBNull(5) ? null : reader.GetString(5);
            var statusEntityId = reader.IsDBNull(6) ? null : reader.GetString(6);

            if (string.IsNullOrWhiteSpace(taxonId) || string.IsNullOrWhiteSpace(statusEntityId)) {
                continue;
            }

            taxonId = taxonId.Trim();
            if (!taxa.ContainsKey(taxonId)) {
                continue;
            }

            stats.TaxaWithP141.Add(taxonId);
            stats.EntitiesWithP141.Add(entityNumericId);
            stats.TotalP141References++;

            if (!entitiesWithP627.Contains(entityNumericId)) {
                stats.EntitiesWithP141ButMissingP627.Add(entityNumericId);
            }

            entityIdsNeedingJson.Add(entityNumericId);
            referenceRecords.Add(new P141ReferenceRecord(entityNumericId, statementId, referenceHash, sourceQid, taxonId, statusEntityId));
        }

        if (referenceRecords.Count == 0) {
            return;
        }

        PopulateRetrievedYears(referenceRecords, entityIdsNeedingJson, cancellationToken);
        PopulateSourceLabels(referenceRecords, stats, cancellationToken);
        BuildP141Histograms(referenceRecords, taxa, entityMetadata, entitiesWithP627, stats);
    }

    private void PopulateRetrievedYears(IReadOnlyList<P141ReferenceRecord> records, HashSet<long> entityIds, CancellationToken cancellationToken) {
        if (entityIds.Count == 0) {
            return;
        }

        var lookup = new Dictionary<ReferenceKey, List<P141ReferenceRecord>>();
        foreach (var record in records) {
            if (!lookup.TryGetValue(record.Key, out var list)) {
                list = new List<P141ReferenceRecord>();
                lookup[record.Key] = list;
            }

            list.Add(record);
        }

        const int chunkSize = 200;
        var ids = entityIds.ToArray();
        for (var offset = 0; offset < ids.Length; offset += chunkSize) {
            cancellationToken.ThrowIfCancellationRequested();
            var chunk = ids.Skip(offset).Take(chunkSize).ToArray();
            using var command = _wikidataConnection.CreateCommand();
            command.CommandText = $"SELECT entity_numeric_id, json FROM wikidata_entities WHERE json IS NOT NULL AND entity_numeric_id IN ({string.Join(",", chunk.Select((_, i) => $"@p{i}"))})";
            for (var i = 0; i < chunk.Length; i++) {
                command.Parameters.AddWithValue($"@p{i}", chunk[i]);
            }

            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var entityId = reader.GetInt64(0);
                if (reader.IsDBNull(1)) {
                    continue;
                }

                var json = reader.GetString(1);
                try {
                    using var document = JsonDocument.Parse(json);
                    var root = document.RootElement;
                    if (!root.TryGetProperty("entities", out var entitiesElement) || entitiesElement.ValueKind != JsonValueKind.Object) {
                        continue;
                    }

                    foreach (var entityProperty in entitiesElement.EnumerateObject()) {
                        if (entityProperty.Value.ValueKind != JsonValueKind.Object) {
                            continue;
                        }

                        if (!entityProperty.Value.TryGetProperty("claims", out var claims) || claims.ValueKind != JsonValueKind.Object) {
                            continue;
                        }

                        if (!claims.TryGetProperty("P141", out var statements) || statements.ValueKind != JsonValueKind.Array) {
                            continue;
                        }

                        foreach (var statement in statements.EnumerateArray()) {
                            if (!statement.TryGetProperty("id", out var statementIdElement)) {
                                continue;
                            }

                            var statementId = statementIdElement.GetString();
                            if (string.IsNullOrWhiteSpace(statementId)) {
                                continue;
                            }

                            if (!statement.TryGetProperty("references", out var references) || references.ValueKind != JsonValueKind.Array) {
                                continue;
                            }

                            foreach (var reference in references.EnumerateArray()) {
                                var referenceHash = reference.TryGetProperty("hash", out var hashElement)
                                    ? hashElement.GetString()
                                    : null;
                                if (string.IsNullOrWhiteSpace(referenceHash)) {
                                    continue;
                                }

                                var key = new ReferenceKey(entityId, statementId, referenceHash);
                                if (!lookup.TryGetValue(key, out var list)) {
                                    continue;
                                }

                                var extractedYear = ExtractRetrievedYear(reference);
                                foreach (var record in list) {
                                    record.RetrievedYear ??= extractedYear;
                                }
                            }
                        }
                    }
                }
                catch (JsonException) {
                    // Ignore malformed JSON for this entity.
                }
            }
        }
    }

    private void PopulateSourceLabels(IReadOnlyList<P141ReferenceRecord> records, WikidataIucnFreshnessStats stats, CancellationToken cancellationToken) {
        var sourceIds = records
            .Where(r => r.SourceQid.HasValue)
            .Select(r => r.SourceQid!.Value)
            .Distinct()
            .ToArray();

        if (sourceIds.Length == 0) {
            return;
        }

        const int chunkSize = 200;
        for (var offset = 0; offset < sourceIds.Length; offset += chunkSize) {
            cancellationToken.ThrowIfCancellationRequested();
            var chunk = sourceIds.Skip(offset).Take(chunkSize).ToArray();
            using var command = _wikidataConnection.CreateCommand();
            command.CommandText = $"SELECT entity_numeric_id, entity_id, label_en FROM wikidata_entities WHERE entity_numeric_id IN ({string.Join(",", chunk.Select((_, i) => $"@p{i}"))})";
            for (var i = 0; i < chunk.Length; i++) {
                command.Parameters.AddWithValue($"@p{i}", chunk[i]);
            }

            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read()) {
                var numericId = reader.GetInt64(0);
                var entityId = reader.GetString(1);
                var label = reader.IsDBNull(2) ? null : reader.GetString(2);
                stats.SourceLabels[numericId] = label ?? entityId;
            }
        }
    }

    private void BuildP141Histograms(
        IReadOnlyList<P141ReferenceRecord> references,
        Dictionary<string, IucnTaxonRecord> taxa,
        Dictionary<long, WikidataEntityMetadata> entityMetadata,
        HashSet<long> entitiesWithP627,
        WikidataIucnFreshnessStats stats) {
        var missingP627Entities = new Dictionary<long, EntityMissingP627SampleBuilder>();

        foreach (var reference in references) {
            if (reference.SourceQid.HasValue) {
                stats.SourceCounts.TryGetValue(reference.SourceQid.Value, out var count);
                stats.SourceCounts[reference.SourceQid.Value] = count + 1;
            }
            else {
                stats.P141ReferencesMissingSource++;
            }

            if (reference.RetrievedYear.HasValue) {
                stats.RetrievedYearCounts.TryGetValue(reference.RetrievedYear.Value, out var yearCount);
                stats.RetrievedYearCounts[reference.RetrievedYear.Value] = yearCount + 1;
            }
            else {
                stats.ReferencesMissingRetrievedYear++;
            }

            if (!entitiesWithP627.Contains(reference.EntityNumericId)) {
                if (!missingP627Entities.TryGetValue(reference.EntityNumericId, out var builder)) {
                    entityMetadata.TryGetValue(reference.EntityNumericId, out var metadata);
                    builder = new EntityMissingP627SampleBuilder(metadata?.EntityId ?? $"Q{reference.EntityNumericId}", metadata?.Label);
                    missingP627Entities[reference.EntityNumericId] = builder;
                }

                builder.AddTaxon(reference.IucnTaxonId);
            }

            if (!taxa.TryGetValue(reference.IucnTaxonId, out var taxon)) {
                stats.StatusAgreement.UnknownIucnCategory++;
                continue;
            }

            var iucnCode = taxon.LatestCategoryCode;
            if (string.IsNullOrWhiteSpace(iucnCode)) {
                stats.StatusAgreement.UnknownIucnCategory++;
                continue;
            }

            var wikiCode = IucnCategoryMapper.MapStatusQid(reference.StatusEntityId);
            if (string.IsNullOrWhiteSpace(wikiCode)) {
                stats.StatusAgreement.UnmappedWikidataStatus++;
                continue;
            }

            stats.StatusAgreement.TotalComparisons++;
            if (string.Equals(iucnCode, wikiCode, StringComparison.OrdinalIgnoreCase)) {
                stats.StatusAgreement.Matches++;
            }
            else {
                stats.StatusAgreement.Mismatches++;
                if (stats.StatusAgreement.MismatchSamples.Count < WikidataIucnFreshnessStats.MaxSamples) {
                    entityMetadata.TryGetValue(reference.EntityNumericId, out var metadata);
                    stats.StatusAgreement.MismatchSamples.Add(new StatusMismatchSample(
                        reference.IucnTaxonId,
                        taxon.LatestCategoryRaw ?? iucnCode,
                        wikiCode,
                        reference.StatusEntityId,
                        metadata?.EntityId ?? $"Q{reference.EntityNumericId}",
                        metadata?.Label));
                }
            }
        }

        foreach (var builder in missingP627Entities.Values.OrderBy(b => b.EntityId).Take(WikidataIucnFreshnessStats.MaxSamples)) {
            stats.MissingP627Samples.Add(builder.Build());
        }
    }

    private static bool IsPopulationOrRegional(string? subpopulation, string? infraType) {
        if (!string.IsNullOrWhiteSpace(subpopulation)) {
            return true;
        }

        if (string.IsNullOrWhiteSpace(infraType)) {
            return false;
        }

        var normalized = infraType.Trim();
        return normalized.IndexOf("population", StringComparison.OrdinalIgnoreCase) >= 0
            || normalized.IndexOf("subpopulation", StringComparison.OrdinalIgnoreCase) >= 0
            || normalized.IndexOf("regional", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static int? ExtractRetrievedYear(JsonElement reference) {
        if (!reference.TryGetProperty("snaks", out var snaks) || snaks.ValueKind != JsonValueKind.Object) {
            return null;
        }

        if (!snaks.TryGetProperty("P813", out var retrievedSnaks) || retrievedSnaks.ValueKind != JsonValueKind.Array) {
            return null;
        }

        foreach (var snak in retrievedSnaks.EnumerateArray()) {
            if (!TryGetDataValue(snak, out var value)) {
                continue;
            }

            if (value.ValueKind != JsonValueKind.Object) {
                continue;
            }

            if (!value.TryGetProperty("time", out var timeElement)) {
                continue;
            }

            var timeText = timeElement.GetString();
            if (TryParseWikidataYear(timeText, out var year)) {
                return year;
            }
        }

        return null;
    }

    private static bool TryGetDataValue(JsonElement snak, out JsonElement value) {
        value = default;
        if (!snak.TryGetProperty("datavalue", out var dataValue) || dataValue.ValueKind != JsonValueKind.Object) {
            return false;
        }

        if (!dataValue.TryGetProperty("value", out value)) {
            return false;
        }

        return true;
    }

    private static bool TryParseWikidataYear(string? value, out int year) {
        year = 0;
        if (string.IsNullOrWhiteSpace(value)) {
            return false;
        }

        var text = value.Trim();
        if (text.Length < 5) {
            return false;
        }

        if (text[0] is '+' or '-') {
            text = text[1..];
        }

        var dashIndex = text.IndexOf('-');
        if (dashIndex > 0) {
            text = text[..dashIndex];
        }

        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out year);
    }
}

internal sealed class WikidataIucnFreshnessStats {
    public const int MaxSamples = 12;

    public long TotalIucnTaxa { get; set; }
    public long IucnTaxaWithDirectP627 { get; set; }
    public long WikidataEntitiesWithP627 { get; set; }
    public long WikidataEntitiesWithExactNameMatch { get; set; }
    public long WikidataEntitiesUsingSynonymName { get; set; }
    public long WikidataEntitiesMissingScientificName { get; set; }
    public List<NameMismatchSample> NameMismatchSamples { get; } = new();

    public HashSet<string> TaxaWithP141 { get; } = new(StringComparer.OrdinalIgnoreCase);
    public long TotalP141References { get; set; }
    public HashSet<long> EntitiesWithP141 { get; } = new();
    public HashSet<long> EntitiesWithP141ButMissingP627 { get; } = new();
    public List<EntityMissingP627Sample> MissingP627Samples { get; } = new();

    public Dictionary<long, long> SourceCounts { get; } = new();
    public long P141ReferencesMissingSource { get; set; }
    public Dictionary<long, string> SourceLabels { get; } = new();

    public Dictionary<int, long> RetrievedYearCounts { get; } = new();
    public long ReferencesMissingRetrievedYear { get; set; }

    public StatusAgreementStats StatusAgreement { get; } = new();
}

internal sealed record NameMismatchSample(
    string EntityId,
    string? Label,
    IReadOnlyList<(string TaxonId, string? Name)> Taxa,
    IReadOnlyList<string> WikidataNames);

internal sealed record EntityMissingP627Sample(string EntityId, string? Label, IReadOnlyList<string> TaxonIds);

internal sealed class EntityNameCollection {
    public HashSet<string> NormalizedNames { get; } = new(StringComparer.Ordinal);
    public List<string> DisplayNames { get; } = new();
}

internal sealed class EntityMissingP627SampleBuilder {
    private readonly HashSet<string> _taxonIds = new(StringComparer.OrdinalIgnoreCase);

    public EntityMissingP627SampleBuilder(string entityId, string? label) {
        EntityId = entityId;
        Label = label;
    }

    public string EntityId { get; }
    public string? Label { get; }

    public void AddTaxon(string taxonId) {
        if (!string.IsNullOrWhiteSpace(taxonId)) {
            _taxonIds.Add(taxonId);
        }
    }

    public EntityMissingP627Sample Build() => new(EntityId, Label, _taxonIds.OrderBy(id => id, StringComparer.OrdinalIgnoreCase).ToList());
}

internal sealed record StatusMismatchSample(
    string TaxonId,
    string? IucnCategory,
    string WikidataCode,
    string StatusQid,
    string EntityId,
    string? EntityLabel);

internal sealed class StatusAgreementStats {
    public long TotalComparisons { get; set; }
    public long Matches { get; set; }
    public long Mismatches { get; set; }
    public long UnknownIucnCategory { get; set; }
    public long UnmappedWikidataStatus { get; set; }
    public List<StatusMismatchSample> MismatchSamples { get; } = new();
}

internal sealed record WikidataEntityMetadata(long NumericId, string EntityId, string? Label);

internal sealed class IucnTaxonRecord {
    public IucnTaxonRecord(string taxonId, string? canonicalName, string? normalizedName, string? comparableName, string? infraType, string? subpopulationName) {
        TaxonId = taxonId;
        CanonicalName = canonicalName;
        NormalizedName = normalizedName;
        ComparableName = comparableName;
        InfraType = infraType;
        SubpopulationName = subpopulationName;
    }

    public string TaxonId { get; }
    public string? CanonicalName { get; }
    public string? NormalizedName { get; }
    public string? ComparableName { get; }
    public string? InfraType { get; }
    public string? SubpopulationName { get; }
    public string? LatestRedlistVersion { get; set; }
    public string? LatestCategoryRaw { get; set; }
    public string? LatestCategoryCode { get; set; }
    public RedlistVersionKey? LatestVersion { get; set; }
}

internal readonly record struct RedlistVersionKey(int Year, int Release, int PublishedYear, string? Raw) : IComparable<RedlistVersionKey> {
    public int CompareTo(RedlistVersionKey other) {
        var cmp = Year.CompareTo(other.Year);
        if (cmp != 0) {
            return cmp;
        }

        cmp = Release.CompareTo(other.Release);
        if (cmp != 0) {
            return cmp;
        }

        cmp = PublishedYear.CompareTo(other.PublishedYear);
        if (cmp != 0) {
            return cmp;
        }

        return string.Compare(Raw, other.Raw, StringComparison.OrdinalIgnoreCase);
    }

    public static RedlistVersionKey From(string? version, string? yearText) {
        var year = ParseInt(version?.Split('-').FirstOrDefault()) ?? ParseInt(yearText) ?? 0;
        var release = 0;
        if (!string.IsNullOrWhiteSpace(version)) {
            var parts = version.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (parts.Length > 1) {
                release = ParseInt(parts[1]) ?? 0;
            }
        }

        var yearPublished = ParseInt(yearText) ?? year;
        return new RedlistVersionKey(year, release, yearPublished, version);
    }

    private static int? ParseInt(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        return int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
            ? result
            : null;
    }
}

internal sealed class P141ReferenceRecord {
    public P141ReferenceRecord(long entityNumericId, string statementId, string referenceHash, long? sourceQid, string iucnTaxonId, string statusEntityId) {
        EntityNumericId = entityNumericId;
        StatementId = statementId;
        ReferenceHash = referenceHash;
        SourceQid = sourceQid;
        IucnTaxonId = iucnTaxonId;
        StatusEntityId = statusEntityId;
    }

    public long EntityNumericId { get; }
    public string StatementId { get; }
    public string ReferenceHash { get; }
    public long? SourceQid { get; }
    public string IucnTaxonId { get; }
    public string StatusEntityId { get; }
    public int? RetrievedYear { get; set; }

    public ReferenceKey Key => new(EntityNumericId, StatementId, ReferenceHash);
}

internal readonly record struct ReferenceKey(long EntityNumericId, string StatementId, string ReferenceHash);

internal static class TaxonNameComparer {
    private static readonly HashSet<string> IgnoredTokens = new(StringComparer.OrdinalIgnoreCase) {
        "subsp.", "ssp.", "subsp", "ssp", "subspecies", "var.", "var", "variety"
    };

    public static string? NormalizeForExactMatch(string? value) {
        var normalized = ScientificNameHelper.Normalize(value);
        if (string.IsNullOrWhiteSpace(normalized)) {
            return normalized;
        }

        var tokens = normalized.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var filtered = tokens.Where(token => !IgnoredTokens.Contains(token)).ToArray();
        return filtered.Length == 0 ? null : string.Join(' ', filtered);
    }
}

internal static class IucnCategoryMapper {
    private static readonly Dictionary<string, string> CategoryMap = new(StringComparer.OrdinalIgnoreCase) {
        ["least concern"] = "LC",
        ["near threatened"] = "NT",
        ["vulnerable"] = "VU",
        ["endangered"] = "EN",
        ["critically endangered"] = "CR",
        ["extinct"] = "EX",
        ["extinct in the wild"] = "EW",
        ["data deficient"] = "DD",
        ["not evaluated"] = "NE",
        ["not applicable"] = "NA",
        ["regionally extinct"] = "RE",
        ["lower risk/least concern"] = "LC",
        ["lower risk/near threatened"] = "NT",
        ["lower risk/conservation dependent"] = "LR/cd"
    };

    private static readonly Dictionary<string, string> StatusQidMap = new(StringComparer.OrdinalIgnoreCase) {
        ["Q211005"] = "LC",
        ["Q719675"] = "NT",
        ["Q278113"] = "VU",
        ["Q96377276"] = "EN",
        ["Q219127"] = "CR",
        ["Q239509"] = "EW",
        ["Q237350"] = "EX",
        ["Q3245245"] = "DD"
    };

    public static string? Normalize(string? category) {
        if (string.IsNullOrWhiteSpace(category)) {
            return null;
        }

        return CategoryMap.TryGetValue(category.Trim(), out var code) ? code : category.Trim();
    }

    public static string? MapStatusQid(string statusEntityId) {
        if (string.IsNullOrWhiteSpace(statusEntityId)) {
            return null;
        }

        return StatusQidMap.TryGetValue(statusEntityId.Trim(), out var code) ? code : null;
    }
}