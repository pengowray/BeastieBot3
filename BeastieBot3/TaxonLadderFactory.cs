using System;
using System.Collections.Generic;
using System.Linq;

namespace BeastieBot3;

internal static class TaxonLadderFactory {
    public static TaxonLadder FromIucn(IucnTaxonomyRow row) {
        if (row is null) {
            throw new ArgumentNullException(nameof(row));
        }

        var nodes = new List<TaxonLadderNode>();
        AddIfPresent(nodes, "kingdom", NormalizeProperCase(row.KingdomName));
        AddIfPresent(nodes, "phylum", NormalizeProperCase(row.PhylumName));
        AddIfPresent(nodes, "class", NormalizeProperCase(row.ClassName));
        AddIfPresent(nodes, "order", NormalizeProperCase(row.OrderName));
        AddIfPresent(nodes, "family", NormalizeProperCase(row.FamilyName));
        AddIfPresent(nodes, "genus", NormalizeScientific(row.GenusName));

        var speciesName = BuildSpeciesName(row.GenusName, row.SpeciesName);
        AddIfPresent(nodes, "species", speciesName);

        if (!string.IsNullOrWhiteSpace(row.InfraName)) {
            var infraRank = NormalizeInfraRank(row.InfraType);
            var infraLabel = BuildInfraName(speciesName, row.InfraType, row.InfraName);
            AddIfPresent(nodes, infraRank, infraLabel);
        }

        return new TaxonLadder("IUCN", nodes);
    }

    public static TaxonLadder FromCol(string sourceLabel, ColTaxonRecord record) {
        return FromColClassification(sourceLabel, record);
    }

    public static TaxonLadder FromColClassification(string sourceLabel, ColTaxonRecord record) {
        if (record is null) {
            throw new ArgumentNullException(nameof(record));
        }

        if (string.IsNullOrWhiteSpace(sourceLabel)) {
            sourceLabel = "COL";
        }

    var nodes = new List<TaxonLadderNode>();
    AddIfPresent(nodes, "kingdom", NormalizeProperCase(record.Kingdom));
    AddIfPresent(nodes, "subkingdom", NormalizeProperCase(record.Subkingdom));
    AddIfPresent(nodes, "phylum", NormalizeProperCase(record.Phylum));
    AddIfPresent(nodes, "subphylum", NormalizeProperCase(record.Subphylum));
    AddIfPresent(nodes, "class", NormalizeProperCase(record.Class));
    AddIfPresent(nodes, "subclass", NormalizeProperCase(record.Subclass));
    AddIfPresent(nodes, "order", NormalizeProperCase(record.Order));
    AddIfPresent(nodes, "suborder", NormalizeProperCase(record.Suborder));
    AddIfPresent(nodes, "superfamily", NormalizeProperCase(record.Superfamily));
    AddIfPresent(nodes, "family", NormalizeProperCase(record.Family));
    AddIfPresent(nodes, "subfamily", NormalizeProperCase(record.Subfamily));
    AddIfPresent(nodes, "tribe", NormalizeProperCase(record.Tribe));
    AddIfPresent(nodes, "subtribe", NormalizeProperCase(record.Subtribe));
        AddIfPresent(nodes, "genus", NormalizeScientific(record.Genus));
        AddIfPresent(nodes, "subgenus", NormalizeScientific(record.Subgenus));

        var speciesName = BuildSpeciesName(record.Genus, record.SpecificEpithet) ?? NormalizeScientific(record.ScientificName);
        if (!string.IsNullOrWhiteSpace(speciesName)) {
            AddIfPresent(nodes, "species", speciesName);
        }

        if (!string.IsNullOrWhiteSpace(record.InfraspecificEpithet)) {
            var infraRank = NormalizeInfraRank(record.Rank);
            var infraLabel = BuildColInfraName(speciesName, record.Rank, record.InfraspecificEpithet);
            AddIfPresent(nodes, infraRank, infraLabel);
        }

        return new TaxonLadder(sourceLabel, nodes);
    }

    public static TaxonLadder FromColLineage(string sourceLabel, IReadOnlyList<ColTaxonRecord> chain) {
        if (string.IsNullOrWhiteSpace(sourceLabel)) {
            throw new ArgumentException("Source label is required.", nameof(sourceLabel));
        }

        if (chain is null) {
            throw new ArgumentNullException(nameof(chain));
        }

        var nodes = new List<TaxonLadderNode>();
        for (var i = 0; i < chain.Count; i++) {
            var record = chain[i];
            if (record is null) {
                continue;
            }

            var rank = NormalizeLineageRank(record.Rank, i);
            var name = NormalizeScientific(record.ScientificName);
            if (string.IsNullOrWhiteSpace(name)) {
                continue;
            }

            AddIfPresent(nodes, rank, name);
        }

        return new TaxonLadder(sourceLabel, nodes);
    }

    private static void AddIfPresent(ICollection<TaxonLadderNode> nodes, string rank, string? value) {
        if (string.IsNullOrWhiteSpace(rank) || string.IsNullOrWhiteSpace(value)) {
            return;
        }

        nodes.Add(new TaxonLadderNode(rank, value.Trim()));
    }

    private static string? NormalizeProperCase(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return null;
        }

        if (trimmed.Length == 1) {
            return trimmed.ToUpperInvariant();
        }

        var first = char.ToUpperInvariant(trimmed[0]);
        var rest = trimmed.Substring(1).ToLowerInvariant();
        return first + rest;
    }

    private static string? NormalizeScientific(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }

        return value.Trim();
    }

    private static string? BuildSpeciesName(string? genus, string? speciesEpithet) {
        var trimmedGenus = NormalizeScientific(genus);
        var trimmedEpithet = NormalizeScientific(speciesEpithet);

        if (string.IsNullOrEmpty(trimmedGenus) && string.IsNullOrEmpty(trimmedEpithet)) {
            return null;
        }

        if (string.IsNullOrEmpty(trimmedEpithet)) {
            return trimmedGenus;
        }

        if (string.IsNullOrEmpty(trimmedGenus)) {
            return trimmedEpithet;
        }

        return trimmedGenus + " " + trimmedEpithet;
    }

    private static string NormalizeInfraRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return "infraspecies";
        }

        var trimmed = rank.Trim();
        if (trimmed.Length == 0) {
            return "infraspecies";
        }

        if (trimmed.Equals("subspecies (plantae)", StringComparison.OrdinalIgnoreCase)) {
            return "subspecies";
        }

        if (trimmed.Equals("species", StringComparison.OrdinalIgnoreCase)) {
            return "species";
        }

        var lower = trimmed.ToLowerInvariant();
        return lower.Contains("subsp", StringComparison.Ordinal) ? "subspecies" : lower;
    }

    private static string? BuildInfraName(string? baseName, string? infraType, string? infraName) {
        var epithet = NormalizeScientific(infraName);
        if (string.IsNullOrEmpty(epithet)) {
            return null;
        }

        var marker = MapInfraMarker(infraType);
        if (string.IsNullOrWhiteSpace(baseName)) {
            return string.IsNullOrEmpty(marker) ? epithet : marker + " " + epithet;
        }

        if (string.IsNullOrEmpty(marker)) {
            return baseName + " " + epithet;
        }

        return baseName + " " + marker + " " + epithet;
    }

    private static string? BuildColInfraName(string? baseName, string? rank, string? infraEpithet) {
        var epithet = NormalizeScientific(infraEpithet);
        if (string.IsNullOrEmpty(epithet)) {
            return null;
        }

        var marker = MapInfraMarker(rank);
        if (string.IsNullOrWhiteSpace(baseName)) {
            return string.IsNullOrEmpty(marker) ? epithet : marker + " " + epithet;
        }

        if (string.IsNullOrEmpty(marker)) {
            return baseName + " " + epithet;
        }

        return baseName + " " + marker + " " + epithet;
    }

    private static string? MapInfraMarker(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return null;
        }

        var normalized = rank.Trim().ToLowerInvariant();
        return normalized switch {
            "subspecies" or "subspecies (plantae)" or "ssp" or "subsp" => "subsp.",
            "variety" or "var" => "var.",
            "form" or "f" => "f.",
            _ => normalized
        };
    }

    private static string NormalizeLineageRank(string? rank, int depth) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return depth == 0 ? "root" : "unranked";
        }

        var trimmed = rank.Trim();
        if (trimmed.Length == 0) {
            return depth == 0 ? "root" : "unranked";
        }

        if (trimmed.Equals("division", StringComparison.OrdinalIgnoreCase)) {
            return "phylum";
        }

        return trimmed.ToLowerInvariant();
    }
}
