using System.Collections.Generic;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaListConfig {
    public WikipediaListDefaults Defaults { get; init; } = new();
    public List<WikipediaListDefinition> Lists { get; init; } = new();
}

internal sealed class WikipediaListDefaults {
    public string? HeaderTemplate { get; init; }
    public string? FooterTemplate { get; init; }
    public List<GroupingLevelDefinition>? Grouping { get; init; }
    public DisplayPreferences Display { get; init; } = new();
}

internal sealed class WikipediaListDefinition {
    public string Id { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public string? Description { get; init; }
    public string OutputFile { get; init; } = string.Empty;
    public TemplateSettings Templates { get; init; } = new();
    public List<TaxonFilterDefinition> Filters { get; init; } = new();
    public List<WikipediaSectionDefinition> Sections { get; init; } = new();
    public List<GroupingLevelDefinition>? Grouping { get; init; }
    public DisplayPreferences? Display { get; init; }
    
    /// <summary>
    /// Custom family-based grouping that replaces the normal taxonomy grouping.
    /// Used for paraphyletic groups like marine mammals where the grouping
    /// doesn't follow normal taxonomic ranks.
    /// </summary>
    public List<CustomGroupDefinition>? CustomGroups { get; init; }
}

/// <summary>
/// A custom group for family-based grouping.
/// Used when taxonomic grouping doesn't fit (e.g., marine mammals).
/// </summary>
internal sealed class CustomGroupDefinition {
    /// <summary>
    /// Display name for the group heading.
    /// </summary>
    public string Name { get; init; } = string.Empty;
    
    /// <summary>
    /// Common name (singular) for species in this group.
    /// </summary>
    public string? CommonName { get; init; }
    
    /// <summary>
    /// Common name (plural) for the group heading.
    /// </summary>
    public string? CommonPlural { get; init; }
    
    /// <summary>
    /// Main article to link under the heading.
    /// </summary>
    public string? MainArticle { get; init; }
    
    /// <summary>
    /// Families that belong to this group (case-insensitive match).
    /// </summary>
    public List<string> Families { get; init; } = new();
    
    /// <summary>
    /// Whether this is the default/fallback group for unmatched taxa.
    /// </summary>
    public bool Default { get; init; }
}

internal sealed class TemplateSettings {
    public string? Header { get; init; }
    public string? Footer { get; init; }
}

internal sealed class TaxonFilterDefinition {
    /// <summary>
    /// Taxonomic rank to filter on (kingdom, phylum, class, order, family, genus).
    /// Mutually exclusive with System filter.
    /// </summary>
    public string Rank { get; init; } = string.Empty;
    
    /// <summary>
    /// Single value to match for the rank filter.
    /// </summary>
    public string Value { get; init; } = string.Empty;
    
    /// <summary>
    /// Multiple values to match with OR logic. If provided, takes precedence over single Value.
    /// </summary>
    public List<string>? Values { get; init; }
    
    /// <summary>
    /// System tag filter (e.g., "Marine", "Freshwater", "Terrestrial").
    /// Uses LIKE matching on the IUCN systems field.
    /// Mutually exclusive with Rank filter.
    /// </summary>
    public string? System { get; init; }
}

internal sealed class WikipediaSectionDefinition {
    public string Key { get; init; } = string.Empty;
    public string Heading { get; init; } = string.Empty;
    public string? Description { get; init; }
    public List<SectionStatusDefinition> Statuses { get; init; } = new();
    public bool HideHeading { get; init; }
}

internal sealed class SectionStatusDefinition {
    public string Code { get; init; } = string.Empty;
    public string? Label { get; init; }
}

internal sealed record GroupingLevelDefinition {
    public string Level { get; init; } = string.Empty;
    public string? Label { get; init; }
    public bool AlwaysDisplay { get; init; }
    public string? UnknownLabel { get; init; }
    /// <summary>
    /// Minimum number of items required for a group to have its own heading.
    /// Groups with fewer items are merged into an "Other" bucket.
    /// Default is 1 (no merging).
    /// </summary>
    public int MinItems { get; init; } = 1;
    /// <summary>
    /// Label for the "Other" bucket when small groups are merged.
    /// Defaults to "Other {Label}" if not specified.
    /// </summary>
    public string? OtherLabel { get; init; }
}

internal sealed class DisplayPreferences {
    public bool PreferCommonNames { get; init; } = true;
    public bool ItalicizeScientific { get; init; } = true;
    public bool IncludeStatusTemplate { get; init; } = true;
    public bool IncludeStatusLabel { get; init; } = true;
    /// <summary>
    /// Whether to group subspecies under their parent species.
    /// When true, subspecies are indented under a parent species heading.
    /// </summary>
    public bool GroupSubspecies { get; init; } = false;
}
