using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// Loads taxon-rules.yml (via YamlDotNet) and provides rule lookup.
// Rules specify exclusions and alternate names per taxon, optionally
// scoped to specific list IDs. Used by WikipediaListGenerator to filter
// species and customize display. Supports regex patterns for taxon matching.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Loads and manages taxon rules from YAML configuration.
/// Provides exclusion checking and rule lookup with list-specific overrides.
/// </summary>
internal sealed class TaxonRulesService {
    private readonly Dictionary<string, TaxonRule> _rules;
    private readonly List<Regex> _globalExclusionPatterns;
    private readonly Dictionary<string, VirtualGroupConfig> _virtualGroups;

    public TaxonRulesService(TaxonRulesConfig config) {
        _rules = new Dictionary<string, TaxonRule>(
            config.Taxa ?? new Dictionary<string, TaxonRule>(),
            StringComparer.OrdinalIgnoreCase);
        
        _virtualGroups = new Dictionary<string, VirtualGroupConfig>(
            config.VirtualGroups ?? new Dictionary<string, VirtualGroupConfig>(),
            StringComparer.OrdinalIgnoreCase);
        
        _globalExclusionPatterns = new List<Regex>();
        foreach (var pattern in config.GlobalExclusions ?? new List<string>()) {
            try {
                _globalExclusionPatterns.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled));
            } catch (ArgumentException) {
                // Skip invalid patterns
            }
        }
    }

    /// <summary>
    /// Load rules from a YAML file.
    /// </summary>
    public static TaxonRulesService Load(string yamlPath) {
        if (!File.Exists(yamlPath)) {
            return new TaxonRulesService(new TaxonRulesConfig());
        }

        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();

        var yaml = File.ReadAllText(yamlPath);
        var config = deserializer.Deserialize<TaxonRulesConfig>(yaml) ?? new TaxonRulesConfig();
        return new TaxonRulesService(config);
    }

    /// <summary>
    /// Check if a taxon should be excluded from a specific list.
    /// </summary>
    public bool ShouldExclude(string taxonName, string? listId = null) {
        if (string.IsNullOrWhiteSpace(taxonName)) {
            return false;
        }

        // Check global exclusion patterns
        foreach (var pattern in _globalExclusionPatterns) {
            if (pattern.IsMatch(taxonName)) {
                return true;
            }
        }

        // Check taxon-specific rules
        if (!_rules.TryGetValue(taxonName, out var rule)) {
            return false;
        }

        // Check list-specific override first
        if (!string.IsNullOrWhiteSpace(listId) && 
            rule.ListOverrides?.TryGetValue(listId, out var listOverride) == true) {
            return listOverride.Exclude;
        }

        return rule.Exclude;
    }

    /// <summary>
    /// Get the rule for a taxon, with list-specific overrides applied.
    /// </summary>
    public TaxonRule? GetRule(string taxonName, string? listId = null) {
        if (string.IsNullOrWhiteSpace(taxonName)) {
            return null;
        }

        if (!_rules.TryGetValue(taxonName, out var rule)) {
            return null;
        }

        // If no list-specific override, return the base rule
        if (string.IsNullOrWhiteSpace(listId) || 
            rule.ListOverrides?.TryGetValue(listId, out var listOverride) != true ||
            listOverride == null) {
            return rule;
        }

        // Merge list-specific overrides with base rule
        return new TaxonRule {
            CommonName = listOverride.CommonName ?? rule.CommonName,
            CommonPlural = rule.CommonPlural,
            Adjective = rule.Adjective,
            Wikilink = listOverride.Wikilink ?? rule.Wikilink,
            MainArticle = rule.MainArticle,
            Blurb = rule.Blurb,
            Comprises = rule.Comprises,
            ForceSplit = rule.ForceSplit,
            UseVirtualGroups = rule.UseVirtualGroups,
            Exclude = listOverride.Exclude
        };
    }

    /// <summary>
    /// Check if a taxon should force-split into lower ranks.
    /// </summary>
    public bool ShouldForceSplit(string taxonName) {
        if (string.IsNullOrWhiteSpace(taxonName)) {
            return false;
        }

        return _rules.TryGetValue(taxonName, out var rule) && rule.ForceSplit;
    }

    /// <summary>
    /// Get the main article link for a taxon heading.
    /// </summary>
    public string? GetMainArticle(string taxonName) {
        if (string.IsNullOrWhiteSpace(taxonName)) {
            return null;
        }

        if (!_rules.TryGetValue(taxonName, out var rule)) {
            return null;
        }

        return rule.MainArticle;
    }

    /// <summary>
    /// Get the wikilink override for a taxon.
    /// </summary>
    public string? GetWikilink(string taxonName, string? listId = null) {
        var rule = GetRule(taxonName, listId);
        return rule?.Wikilink;
    }

    /// <summary>
    /// Check if a taxon should use virtual groups.
    /// </summary>
    public bool ShouldUseVirtualGroups(string taxonName) {
        if (string.IsNullOrWhiteSpace(taxonName)) {
            return false;
        }

        return _rules.TryGetValue(taxonName, out var rule) && rule.UseVirtualGroups;
    }

    /// <summary>
    /// Check if virtual groups are defined for a parent taxon.
    /// </summary>
    public bool HasVirtualGroups(string parentTaxon) {
        return _virtualGroups.ContainsKey(parentTaxon);
    }

    /// <summary>
    /// Get the virtual groups for a parent taxon.
    /// </summary>
    public VirtualGroupConfig? GetVirtualGroups(string parentTaxon) {
        return _virtualGroups.TryGetValue(parentTaxon, out var config) ? config : null;
    }

    /// <summary>
    /// Resolve which virtual group a taxon belongs to under <paramref name="parentTaxon"/>.
    /// <paramref name="cladeNames"/> are the names of the Catalogue of Life nodes between the parent
    /// and the taxon's family, broad to narrow (e.g. Serpentes, Alethinophidia). Matching runs in
    /// three passes, each over the groups in order: a group's <c>clades</c> (and <c>superfamilies</c>,
    /// which are path nodes too) against those names, then its <c>families</c> against the family,
    /// then the default group. Returns null if the parent has no groups or nothing matches.
    /// </summary>
    public VirtualGroupResolution? ResolveVirtualGroup(string parentTaxon, string? family, IReadOnlyList<string> cladeNames) {
        if (!_virtualGroups.TryGetValue(parentTaxon, out var config) || config.Groups.Count == 0) {
            return null;
        }

        if (cladeNames.Count > 0) {
            foreach (var group in config.Groups) {
                foreach (var clade in group.Clades.Concat(group.Superfamilies)) {
                    for (var i = 0; i < cladeNames.Count; i++) {
                        if (string.Equals(clade, cladeNames[i], StringComparison.OrdinalIgnoreCase)) {
                            return new VirtualGroupResolution(group, i);
                        }
                    }
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(family)) {
            foreach (var group in config.Groups) {
                if (group.Families.Any(f => string.Equals(f, family.Trim(), StringComparison.OrdinalIgnoreCase))) {
                    return new VirtualGroupResolution(group, -1);
                }
            }
        }

        var defaultGroup = config.Groups.FirstOrDefault(g => g.Default);
        return defaultGroup is null ? null : new VirtualGroupResolution(defaultGroup, -1);
    }
}

/// <summary>The virtual group a taxon belongs to.</summary>
/// <param name="Group">The matched group.</param>
/// <param name="CladeIndex">Index in the clade names of the node that matched, or -1 when the group
/// matched by family or as the default.</param>
internal sealed record VirtualGroupResolution(VirtualGroup Group, int CladeIndex);
