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
    /// <summary>
    /// Minimum number of small groups required before merging into "Other".
    /// Default is 0 (no minimum; any small groups can be merged when MinItems applies).
    /// </summary>
    public int MinGroupsForOther { get; init; } = 0;
    /// <summary>
    /// Whether to show rank label in headings (e.g., "Family: [[Familyidae]]").
    /// Default is false (just show "[[Familyidae]]").
    /// </summary>
    public bool ShowRankLabel { get; init; } = false;
}

/// <summary>
/// Species listing style for Wikipedia output.
/// </summary>
internal enum ListingStyle {
    /// <summary>
    /// Style B: Common name focus (default). Shows common name first, scientific name in parentheses.
    /// Example: [[Western gorilla]] (''Gorilla gorilla'')
    /// </summary>
    CommonNameFocus,
    
    /// <summary>
    /// Style A: Scientific name focus. Shows scientific name first, common name after comma.
    /// Best for plants and invertebrates.
    /// Example: ''[[Pinus radiata]]'', Monterey pine
    /// </summary>
    ScientificNameFocus,
    
    /// <summary>
    /// Style C: Common name only. Shows only common name (falls back to scientific if unavailable).
    /// Best for mammals, birds, bats, sharks where all species have unambiguous common names.
    /// Example: [[Western gorilla]]
    /// </summary>
    CommonNameOnly
}

internal enum InfraspecificDisplayMode {
    /// <summary>
    /// Use separate sections for Species, Subspecies, Varieties, and Stocks/Populations.
    /// </summary>
    SeparateSections,
    /// <summary>
    /// Group subspecies/varieties/populations under their parent species as sub-bullets.
    /// </summary>
    GroupedUnderSpecies
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
    
    /// <summary>
    /// Listing style for species entries.
    /// Default is CommonNameFocus (Style B).
    /// </summary>
    public ListingStyle ListingStyle { get; init; } = ListingStyle.CommonNameFocus;

    /// <summary>
    /// How to display infraspecific taxa (subspecies, varieties, and populations).
    /// Default is SeparateSections for backward compatibility.
    /// </summary>
    public InfraspecificDisplayMode InfraspecificDisplayMode { get; init; } = InfraspecificDisplayMode.SeparateSections;
    
    /// <summary>
    /// Whether to separate subspecies, varieties, and subpopulations into their own sections.
    /// When true, adds "Species", "Subspecies", "Varieties", "Stocks and populations" subheadings.
    /// Default is false for backward compatibility.
    /// </summary>
    public bool SeparateInfraspecificSections { get; init; } = false;
    
    /// <summary>
    /// Whether to filter out regional assessments (subpopulations) from the list.
    /// Default is false to include all assessments (backward compatible).
    /// Set to true to show only global assessments.
    /// </summary>
    public bool ExcludeRegionalAssessments { get; init; } = false;
    
    /// <summary>
    /// Whether to include family annotation for items in "Other" bucket.
    /// Example: "[[Species]] (Family: [[Familyidae]])"
    /// Default is false.
    /// </summary>
    public bool IncludeFamilyInOtherBucket { get; init; } = false;
}

