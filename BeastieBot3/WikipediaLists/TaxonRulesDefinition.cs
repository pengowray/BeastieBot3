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
