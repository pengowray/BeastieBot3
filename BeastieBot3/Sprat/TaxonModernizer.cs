using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace BeastieBot3.Sprat;

// Applies the curated taxonomy modernizations in rules/taxon-modernization.yml to SPRAT order names:
// safe 1:1 renames/typos and the family-conditional structural splits. It is a pure mapping — every
// change it makes is also recorded into a ModernizationLog so the command can emit an audit report
// and a CSV-recommendations report. Debatable names are intentionally absent from the config and so
// flow through unchanged (to be flagged by the report's runtime detectors instead).

/// <summary>A single applied modernization (one taxon, one field), for the audit/CSV reports.</summary>
internal readonly record struct ModernizationChange(
    string Group,
    long TaxonId,
    string ScientificName,
    string Field,
    string From,
    string To,
    string Kind,
    string? EpbcListedAs);

/// <summary>Accumulates every modernization applied across all generated lists in one run.</summary>
internal sealed class ModernizationLog {
    private readonly List<ModernizationChange> _changes = new();
    public IReadOnlyList<ModernizationChange> Changes => _changes;
    public void Record(ModernizationChange change) => _changes.Add(change);
}

/// <summary>The result of modernizing one order value: the new name plus the rule's provenance.</summary>
internal sealed record OrderModernization(string From, string To, string Kind, string? EpbcListedAs);

internal sealed class TaxonModernizer {
    // from-name (trimmed, case-insensitive) → rename rule.
    private readonly IReadOnlyDictionary<string, OrderRule> _orderRenames;
    // obsolete order (case-insensitive) → (family case-insensitive → modern order).
    private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> _orderByFamily;

    private TaxonModernizer(
        IReadOnlyDictionary<string, OrderRule> orderRenames,
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> orderByFamily) {
        _orderRenames = orderRenames;
        _orderByFamily = orderByFamily;
    }

    /// <summary>An engine that applies no changes (used when the config is absent).</summary>
    public static TaxonModernizer Empty() => new(
        new Dictionary<string, OrderRule>(StringComparer.OrdinalIgnoreCase),
        new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase));

    public static TaxonModernizer Load(string path) {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) {
            return Empty();
        }

        var deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        using var reader = File.OpenText(path);
        var root = deserializer.Deserialize<Root>(reader);
        if (root is null) {
            return Empty();
        }

        var renames = new Dictionary<string, OrderRule>(StringComparer.OrdinalIgnoreCase);
        foreach (var rule in root.Orders ?? new List<OrderRule>()) {
            if (!string.IsNullOrWhiteSpace(rule.From) && !string.IsNullOrWhiteSpace(rule.To)) {
                renames[rule.From!.Trim()] = rule;
            }
        }

        var byFamily = new Dictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (order, families) in root.OrderByFamily ?? new Dictionary<string, Dictionary<string, string>>()) {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var (family, modernOrder) in families) {
                if (!string.IsNullOrWhiteSpace(family) && !string.IsNullOrWhiteSpace(modernOrder)) {
                    map[family.Trim()] = modernOrder.Trim();
                }
            }
            byFamily[order.Trim()] = map;
        }

        return new TaxonModernizer(renames, byFamily);
    }

    /// <summary>
    /// The modern form of <paramref name="order"/> given its <paramref name="family"/>, or null when
    /// no rule applies (leave the value unchanged). Family-conditional splits take precedence over a
    /// simple rename so an order listed under both is resolved by family.
    /// </summary>
    public OrderModernization? ModernizeOrder(string? order, string? family) {
        if (string.IsNullOrWhiteSpace(order)) {
            return null;
        }
        var from = order.Trim();

        if (_orderByFamily.TryGetValue(from, out var familyMap)
            && !string.IsNullOrWhiteSpace(family)
            && familyMap.TryGetValue(family.Trim(), out var modernOrder)
            && !string.Equals(modernOrder, from, StringComparison.OrdinalIgnoreCase)) {
            return new OrderModernization(from, modernOrder, "structural", EpbcListedAs: null);
        }

        if (_orderRenames.TryGetValue(from, out var rule)
            && !string.Equals(rule.To, from, StringComparison.OrdinalIgnoreCase)) {
            return new OrderModernization(from, rule.To!.Trim(), rule.Kind ?? "rename", rule.EpbcListedAs);
        }

        return null;
    }

    private sealed class Root {
        public List<OrderRule>? Orders { get; set; }
        public Dictionary<string, Dictionary<string, string>>? OrderByFamily { get; set; }
    }

    internal sealed class OrderRule {
        public string? From { get; set; }
        public string? To { get; set; }
        public string? Kind { get; set; }
        public string? EpbcListedAs { get; set; }
    }
}
