using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

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
    /// Resolve which virtual group a record belongs to, based on its family/superfamily/clade.
    /// Returns null if no virtual groups are defined or no match is found.
    /// </summary>
    public VirtualGroup? ResolveVirtualGroup(string parentTaxon, string? family, string? superfamily, string? clade) {
        if (!_virtualGroups.TryGetValue(parentTaxon, out var config) || config.Groups.Count == 0) {
            return null;
        }

        VirtualGroup? defaultGroup = null;

        foreach (var group in config.Groups) {
            if (group.Default) {
                defaultGroup = group;
                continue;
            }

            // Check superfamilies
            if (!string.IsNullOrEmpty(superfamily) && group.Superfamilies.Count > 0) {
                foreach (var sf in group.Superfamilies) {
                    if (string.Equals(sf, superfamily, StringComparison.OrdinalIgnoreCase)) {
                        return group;
                    }
                }
            }

            // Check families
            if (!string.IsNullOrEmpty(family) && group.Families.Count > 0) {
                foreach (var f in group.Families) {
                    if (string.Equals(f, family, StringComparison.OrdinalIgnoreCase)) {
                        return group;
                    }
                }
            }

            // Check clades
            if (!string.IsNullOrEmpty(clade) && group.Clades.Count > 0) {
                foreach (var c in group.Clades) {
                    if (string.Equals(c, clade, StringComparison.OrdinalIgnoreCase)) {
                        return group;
                    }
                }
            }
        }

        // Return default group if no match found
        return defaultGroup;
    }
}
