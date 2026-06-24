using System.Collections.Generic;

// YAML deserialization model for list configuration. Key properties:
// - filter: taxonomy (class, order, family) and status (CR, EN, VU, etc.)
// - grouping: hierarchy levels (order, family, genus) with sort options
// - display: show_status, show_trend, link_species, scientific_name_format
// - virtual_groups: custom groupings (e.g., "Marine mammals")
// Loaded by WikipediaListDefinitionLoader, consumed by WikipediaListGenerator.

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Root YAML model for <c>wikipedia-lists.yml</c>. Contains global defaults and the list of
/// Wikipedia list definitions to generate.
/// </summary>
internal sealed class WikipediaListConfig {
    public WikipediaListDefaults Defaults { get; init; } = new();
    public List<WikipediaListDefinition> Lists { get; init; } = new();
}

/// <summary>
/// Global defaults applied to all lists unless overridden at the list level.
/// Includes header/footer templates, grouping hierarchy, display preferences, and auto-split config.
/// </summary>
internal sealed class WikipediaListDefaults {
    public string? HeaderTemplate { get; init; }
    public string? FooterTemplate { get; init; }
    public List<GroupingLevelDefinition>? Grouping { get; init; }
    public DisplayPreferences Display { get; init; } = new();
    public AutoSplitConfig? AutoSplit { get; init; }
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
    /// <summary>
    /// List-level display overrides, already merged from preset → taxa-group → list by the loader.
    /// Nullable per field (see <see cref="DisplayPreferencesConfig"/>); resolved against the global
    /// <see cref="WikipediaListDefaults.Display"/> baseline at generation time.
    /// </summary>
    public DisplayPreferencesConfig? Display { get; init; }

    /// <summary>
    /// Source taxa-group id this list was expanded from (e.g. "mammals", "marine-mammals").
    /// Null for full/explicit definitions that don't reference a taxa_group.
    /// Used by the <c>--taxa-group</c> generation filter. Parsing the <see cref="Id"/> would be
    /// unreliable because group and preset names can themselves contain hyphens.
    /// </summary>
    public string? TaxaGroup { get; init; }

    /// <summary>
    /// Source preset id (threat-status grouping) this list was expanded from (e.g. "cr", "ex",
    /// "threatened"). Null for full/explicit definitions that don't reference a preset.
    /// Used by the <c>--status</c> generation filter.
    /// </summary>
    public string? Preset { get; init; }

    /// <summary>
    /// Adjective form of the taxa group name (e.g., "mammalian", "amphibian").
    /// Used in intro text like "mammalian species".
    /// </summary>
    public string? TaxaAdjective { get; init; }

    /// <summary>
    /// Lowercase taxa group name (e.g., "mammals", "amphibians").
    /// </summary>
    public string? TaxaNameLower { get; init; }

    /// <summary>
    /// Human-readable status text (e.g., "critically endangered", "vulnerable").
    /// </summary>
    public string? StatusText { get; init; }

    /// <summary>
    /// Wiki-linked status text (e.g., "[[Critically endangered species|critically endangered]]").
    /// </summary>
    public string? StatusWikiLink { get; init; }

    /// <summary>
    /// Pre-rendered <c>[[Category:...]]</c> footer lines (with per-category sort keys), derived by the
    /// loader from the list's status + the taxa group's curated category names. Empty for lists with
    /// no resolvable categories.
    /// </summary>
    public List<string> Categories { get; init; } = new();

    /// <summary>
    /// Advisory per-page renderable-row budget from the taxa group's <c>size_budget.max_entries</c>;
    /// the impact preview flags pages over this. Null = no budget declared.
    /// </summary>
    public int? SizeBudgetMaxEntries { get; init; }

    /// <summary>
    /// Custom family-based grouping that replaces the normal taxonomy grouping.
    /// Used for paraphyletic groups like marine mammals where the grouping
    /// doesn't follow normal taxonomic ranks.
    /// </summary>
    public List<CustomGroupDefinition>? CustomGroups { get; init; }

    /// <summary>
    /// Auto-split configuration for this list. Overrides defaults if specified.
    /// </summary>
    public AutoSplitConfig? AutoSplit { get; init; }

    /// <summary>
    /// Resolved phylogenetic child lists this list summarizes and links down to (e.g.
    /// invertebrates-cr → insects-cr, gastropods-cr). Empty for ordinary leaf lists. Populated by a
    /// post-expansion pass in the loader and only for children that actually generate (same preset).
    /// A non-empty value makes this a "parent" list (summary table + bare-bones child sections).
    /// </summary>
    public List<ChildListLink> SubLists { get; } = new();

    /// <summary>
    /// Resolved non-phylogenetic cross-reference lists (e.g. mammals → marine-mammals). Rendered as a
    /// plain "Related lists" bullet block, NOT as nested phylogenetic sub-lists or count rows.
    /// </summary>
    public List<ChildListLink> SeeAlso { get; } = new();
}

/// <summary>
/// How a child reference is rendered under a parent list.
/// </summary>
internal enum GroupingKind {
    /// <summary>Phylogenetic decomposition: gets a summary-table row + a bare-bones summary section.</summary>
    Phylogenetic,
    /// <summary>Non-phylogenetic cross-reference: a plain bullet link under "Related lists".</summary>
    SeeAlso
}

/// <summary>
/// A resolved link from a parent list to one already-generated child list. The child remains its own
/// independently-generated file; the parent only summarizes counts and links to it. Built by the
/// loader's post-expansion pass, so <see cref="WikiTitle"/>/<see cref="Filters"/> come from the
/// child's own expanded definition (no second generation pass, no DB hit).
/// </summary>
/// <param name="Id">Child list id, e.g. "insects-cr".</param>
/// <param name="DisplayName">Child taxa-group display name, e.g. "Insects".</param>
/// <param name="Adjective">Child taxa-group adjective for prose, e.g. "insect" (may be empty).</param>
/// <param name="WikiTitle">Article title for [[...]] links, derived from the child OutputFile.</param>
/// <param name="Filters">Child taxa-group filters — used to derive the child rank/value for the summary table.</param>
/// <param name="Kind">Phylogenetic (table row + section) or SeeAlso (bullet link).</param>
internal sealed record ChildListLink(
    string Id,
    string DisplayName,
    string Adjective,
    string WikiTitle,
    List<TaxonFilterDefinition> Filters,
    GroupingKind Kind);

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
    /// Values to EXCLUDE for this rank (NULL-safe NOT IN). Combinable with Value/Values:
    /// the include clause (if any) is applied first, then matching rows in Exclude are removed.
    /// A filter row with only Rank + Exclude (no Value/Values) emits just the exclusion —
    /// e.g. Invertebrates = {rank: kingdom, value: Animalia}, {rank: phylum, exclude: [Chordata]}.
    /// Rows whose column is NULL are KEPT (NULL NOT IN (...) would otherwise drop them).
    /// </summary>
    public List<string>? Exclude { get; init; }

    /// <summary>
    /// System tag filter (e.g., "Marine", "Freshwater", "Terrestrial").
    /// Uses LIKE matching on the IUCN systems field.
    /// Mutually exclusive with Rank filter.
    /// </summary>
    public string? System { get; init; }

    /// <summary>
    /// Multiple system tags matched with OR logic — e.g. aquatic mammals = {systems: [Marine,
    /// Freshwater]}. Emits <c>(systems LIKE '%Marine%' OR systems LIKE '%Freshwater%')</c>. Takes
    /// precedence over the single <see cref="System"/>. Mutually exclusive with Rank.
    /// </summary>
    public List<string>? Systems { get; init; }
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
    /// Regional assessments are never included in main lists; default is true to enforce global-only output.
    /// </summary>
    public bool ExcludeRegionalAssessments { get; init; } = true;
    
    /// <summary>
    /// Whether to include family annotation for items in "Other" bucket.
    /// Example: "[[Species]] (Family: [[Familyidae]])"
    /// Default is false.
    /// </summary>
    public bool IncludeFamilyInOtherBucket { get; init; } = false;
}

/// <summary>
/// Override-layer view of <see cref="DisplayPreferences"/> where every field is nullable, so an
/// unset YAML key (null) is distinguishable from one explicitly set to its default value. The
/// per-list / taxa-group / preset <c>display:</c> blocks deserialize into this. Layers are stacked
/// with <see cref="Merge"/> (the upper layer wins per field) and then collapsed to a concrete
/// <see cref="DisplayPreferences"/> by <see cref="ResolveAgainst"/> against the global baseline.
/// Keeping these nullable is what lets a list override a global default of <c>true</c> back to
/// <c>false</c> — the old non-nullable model could only ever OR booleans upward (so a base-true
/// could never be turned off) and unconditionally clobbered the other half with the deserializer's
/// own defaults (so a base value could never be inherited).
/// </summary>
internal sealed class DisplayPreferencesConfig {
    public bool? PreferCommonNames { get; init; }
    public bool? ItalicizeScientific { get; init; }
    public bool? IncludeStatusTemplate { get; init; }
    public bool? IncludeStatusLabel { get; init; }
    public bool? GroupSubspecies { get; init; }
    public ListingStyle? ListingStyle { get; init; }
    public InfraspecificDisplayMode? InfraspecificDisplayMode { get; init; }
    public bool? SeparateInfraspecificSections { get; init; }
    public bool? ExcludeRegionalAssessments { get; init; }
    public bool? IncludeFamilyInOtherBucket { get; init; }

    /// <summary>
    /// Stack <paramref name="over"/> on top of <paramref name="under"/>: each field set in
    /// <paramref name="over"/> wins, otherwise the value falls through to <paramref name="under"/>.
    /// Either side may be null (a missing layer); returns null only when both are null.
    /// </summary>
    public static DisplayPreferencesConfig? Merge(DisplayPreferencesConfig? under, DisplayPreferencesConfig? over) {
        if (under is null) return over;
        if (over is null) return under;
        return new DisplayPreferencesConfig {
            PreferCommonNames = over.PreferCommonNames ?? under.PreferCommonNames,
            ItalicizeScientific = over.ItalicizeScientific ?? under.ItalicizeScientific,
            IncludeStatusTemplate = over.IncludeStatusTemplate ?? under.IncludeStatusTemplate,
            IncludeStatusLabel = over.IncludeStatusLabel ?? under.IncludeStatusLabel,
            GroupSubspecies = over.GroupSubspecies ?? under.GroupSubspecies,
            ListingStyle = over.ListingStyle ?? under.ListingStyle,
            InfraspecificDisplayMode = over.InfraspecificDisplayMode ?? under.InfraspecificDisplayMode,
            SeparateInfraspecificSections = over.SeparateInfraspecificSections ?? under.SeparateInfraspecificSections,
            ExcludeRegionalAssessments = over.ExcludeRegionalAssessments ?? under.ExcludeRegionalAssessments,
            IncludeFamilyInOtherBucket = over.IncludeFamilyInOtherBucket ?? under.IncludeFamilyInOtherBucket,
        };
    }

    /// <summary>
    /// Collapse to a concrete <see cref="DisplayPreferences"/>, taking each unset (null) field from
    /// <paramref name="defaults"/> (the global resolved baseline). Fields set on this config win.
    /// </summary>
    public DisplayPreferences ResolveAgainst(DisplayPreferences defaults) => new DisplayPreferences {
        PreferCommonNames = PreferCommonNames ?? defaults.PreferCommonNames,
        ItalicizeScientific = ItalicizeScientific ?? defaults.ItalicizeScientific,
        IncludeStatusTemplate = IncludeStatusTemplate ?? defaults.IncludeStatusTemplate,
        IncludeStatusLabel = IncludeStatusLabel ?? defaults.IncludeStatusLabel,
        GroupSubspecies = GroupSubspecies ?? defaults.GroupSubspecies,
        ListingStyle = ListingStyle ?? defaults.ListingStyle,
        InfraspecificDisplayMode = InfraspecificDisplayMode ?? defaults.InfraspecificDisplayMode,
        SeparateInfraspecificSections = SeparateInfraspecificSections ?? defaults.SeparateInfraspecificSections,
        ExcludeRegionalAssessments = ExcludeRegionalAssessments ?? defaults.ExcludeRegionalAssessments,
        IncludeFamilyInOtherBucket = IncludeFamilyInOtherBucket ?? defaults.IncludeFamilyInOtherBucket,
    };
}

/// <summary>
/// Configuration for automatic section splitting of large groups.
/// When a leaf group exceeds the threshold, the tree builder tries CoL-enriched
/// intermediate ranks to insert finer-grained headings.
/// </summary>
internal sealed class AutoSplitConfig {
    /// <summary>
    /// Whether auto-split is enabled. Default is true when this config is present.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Minimum number of items in a group to trigger auto-split.
    /// Default is 30.
    /// </summary>
    public int Threshold { get; init; } = 30;

    /// <summary>
    /// All meaningful (non-Other/Unknown) groups must have at least this many items.
    /// When 4+ meaningful groups exist, one group may be below this threshold.
    /// Default is 10.
    /// </summary>
    public int MinGroupSize { get; init; } = 10;

    /// <summary>
    /// Minimum items for a sub-group to get its own heading in auto-split.
    /// Groups below this are lumped into "Other {rank}".
    /// Default is 3 (headings with 1-2 species are noise).
    /// </summary>
    public int MinItemsPerGroup { get; init; } = 3;

    /// <summary>
    /// Maximum fraction (0.0-1.0) of items allowed in Other+Unknown groups combined.
    /// If exceeded, the split is rejected as not informative.
    /// Default is 0.6 (reject if &gt;60% of items are residual).
    /// </summary>
    public double MaxOtherFraction { get; init; } = 0.6;

    /// <summary>
    /// Maximum number of heading groups allowed after lumping.
    /// Prevents walls of headings even when groups are above MinItemsPerGroup.
    /// Default is 15.
    /// </summary>
    public int MaxGroups { get; init; } = 15;

    /// <summary>
    /// Maximum auto-split nesting depth (additional heading levels inserted).
    /// Prevents heading level cap violations from deep recursive splitting.
    /// Default is 1 (one level of auto-split headings).
    /// </summary>
    public int MaxDepth { get; init; } = 1;

    /// <summary>
    /// Minimum number of meaningful (non-Other/Unknown) groups required to accept a split.
    /// Default is 3.
    /// </summary>
    public int MinMeaningfulGroups { get; init; } = 3;

    /// <summary>
    /// When true, reject any split that produces groups with "Unknown" in the label.
    /// Unknown groupings create confusion for editors. Default is true.
    /// </summary>
    public bool RejectUnknownGroups { get; init; } = true;
}

