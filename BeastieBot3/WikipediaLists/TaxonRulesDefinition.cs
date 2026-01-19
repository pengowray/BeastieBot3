using System.Collections.Generic;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// YAML-based taxon rules configuration.
/// Loaded from taxon-rules.yml alongside the legacy rules-list.txt.
/// </summary>
internal sealed class TaxonRulesConfig {
    /// <summary>
    /// Rules keyed by taxon name (scientific name or common name).
    /// </summary>
    public Dictionary<string, TaxonRule> Taxa { get; init; } = new();
    
    /// <summary>
    /// Global exclusion patterns (regex).
    /// Taxa matching any pattern are excluded from all lists.
    /// </summary>
    public List<string> GlobalExclusions { get; init; } = new();
    
    /// <summary>
    /// Virtual group definitions keyed by parent taxon name.
    /// Used to organize taxa into logical groupings (e.g., Squamata → Snakes, Lizards).
    /// </summary>
    public Dictionary<string, VirtualGroupConfig> VirtualGroups { get; init; } = new();
}

/// <summary>
/// Virtual group configuration for a parent taxon.
/// </summary>
internal sealed class VirtualGroupConfig {
    /// <summary>
    /// Ordered list of groups. First matching group wins.
    /// </summary>
    public List<VirtualGroup> Groups { get; init; } = new();
}

/// <summary>
/// A virtual group that organizes taxa by family/superfamily/clade membership.
/// </summary>
internal sealed class VirtualGroup {
    /// <summary>
    /// Display name for the group heading.
    /// </summary>
    public string Name { get; init; } = string.Empty;
    
    /// <summary>
    /// Common name (singular).
    /// </summary>
    public string? CommonName { get; init; }
    
    /// <summary>
    /// Common name (plural).
    /// </summary>
    public string? CommonPlural { get; init; }
    
    /// <summary>
    /// Main article to link under the heading.
    /// </summary>
    public string? MainArticle { get; init; }
    
    /// <summary>
    /// Superfamilies that belong to this group.
    /// </summary>
    public List<string> Superfamilies { get; init; } = new();
    
    /// <summary>
    /// Families that belong to this group.
    /// </summary>
    public List<string> Families { get; init; } = new();
    
    /// <summary>
    /// Unranked clades that belong to this group (e.g., Iguania).
    /// </summary>
    public List<string> Clades { get; init; } = new();
    
    /// <summary>
    /// Whether this is the default/fallback group for unmatched taxa.
    /// </summary>
    public bool Default { get; init; }
}

/// <summary>
/// Rules for a specific taxon.
/// </summary>
internal sealed class TaxonRule {
    /// <summary>
    /// Common name (singular).
    /// </summary>
    public string? CommonName { get; init; }
    
    /// <summary>
    /// Common name (plural).
    /// </summary>
    public string? CommonPlural { get; init; }
    
    /// <summary>
    /// Adjective form (e.g., "mammalian" for Mammalia).
    /// </summary>
    public string? Adjective { get; init; }
    
    /// <summary>
    /// Override the default wikilink target.
    /// Used when the taxon name leads to a disambiguation page.
    /// </summary>
    public string? Wikilink { get; init; }
    
    /// <summary>
    /// Main article to link under the heading.
    /// </summary>
    public string? MainArticle { get; init; }
    
    /// <summary>
    /// Descriptive blurb to show under the heading.
    /// </summary>
    public string? Blurb { get; init; }
    
    /// <summary>
    /// Text describing what this taxon comprises.
    /// Shown in grey under the heading.
    /// </summary>
    public string? Comprises { get; init; }
    
    /// <summary>
    /// Whether to force-split into lower ranks even if there are few items.
    /// </summary>
    public bool ForceSplit { get; init; }
    
    /// <summary>
    /// Whether to use virtual groups for this taxon instead of rank-based splitting.
    /// </summary>
    public bool UseVirtualGroups { get; init; }
    
    /// <summary>
    /// Whether to exclude this taxon from lists.
    /// </summary>
    public bool Exclude { get; init; }
    
    /// <summary>
    /// List-specific overrides. Key is list ID.
    /// </summary>
    public Dictionary<string, TaxonListOverride>? ListOverrides { get; init; }
}

/// <summary>
/// List-specific overrides for a taxon.
/// </summary>
internal sealed class TaxonListOverride {
    /// <summary>
    /// Whether to exclude this taxon from this specific list.
    /// </summary>
    public bool Exclude { get; init; }
    
    /// <summary>
    /// Override common name for this list.
    /// </summary>
    public string? CommonName { get; init; }
    
    /// <summary>
    /// Override wikilink for this list.
    /// </summary>
    public string? Wikilink { get; init; }
}
