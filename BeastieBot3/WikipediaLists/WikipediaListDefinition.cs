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
}

internal sealed class TemplateSettings {
    public string? Header { get; init; }
    public string? Footer { get; init; }
}

internal sealed class TaxonFilterDefinition {
    public string Rank { get; init; } = string.Empty;
    public string Value { get; init; } = string.Empty;
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
}

internal sealed class DisplayPreferences {
    public bool PreferCommonNames { get; init; } = true;
    public bool ItalicizeScientific { get; init; } = true;
    public bool IncludeStatusTemplate { get; init; } = true;
    public bool IncludeStatusLabel { get; init; } = true;
}
