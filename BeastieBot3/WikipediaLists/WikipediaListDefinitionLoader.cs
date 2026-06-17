using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// Loads list configuration from YAML (via YamlDotNet). File structure:
// defaults: (header_template, footer_template, grouping)
// lists: [{id, title, filter: {taxonomy, status}, grouping, display}]
// Uses underscored_naming convention. Validates required fields.
// Called by WikipediaListCommand with --config parameter.

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaListDefinitionLoader {
    private readonly IDeserializer _deserializer;

    public WikipediaListDefinitionLoader() {
        _deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
    }

    public WikipediaListConfig Load(string filePath) {
        if (string.IsNullOrWhiteSpace(filePath)) {
            throw new ArgumentException("Configuration path was not provided.", nameof(filePath));
        }

        var fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"Wikipedia list config not found: {fullPath}", fullPath);
        }

        var directory = Path.GetDirectoryName(fullPath) ?? ".";

        // Load supporting files if they exist
        var taxaGroups = LoadTaxaGroups(directory);
        var listPresets = LoadListPresets(directory);

        using var reader = File.OpenText(fullPath);
        var rawConfig = _deserializer.Deserialize<WikipediaListConfigRaw>(reader);
        if (rawConfig is null) {
            throw new InvalidOperationException($"Unable to parse Wikipedia list config at {fullPath}.");
        }

        // Expand the raw config using taxa groups and presets
        return ExpandConfig(rawConfig, taxaGroups, listPresets);
    }

    private Dictionary<string, TaxaGroupDefinition> LoadTaxaGroups(string directory) {
        var path = Path.Combine(directory, "taxa-groups.yml");
        if (!File.Exists(path)) return new();

        using var reader = File.OpenText(path);
        var file = _deserializer.Deserialize<TaxaGroupsFile>(reader);
        return file?.Groups ?? new();
    }

    private Dictionary<string, ListPresetDefinition> LoadListPresets(string directory) {
        var path = Path.Combine(directory, "list-presets.yml");
        if (!File.Exists(path)) return new();

        using var reader = File.OpenText(path);
        var file = _deserializer.Deserialize<ListPresetsFile>(reader);
        return file?.Presets ?? new();
    }

    private WikipediaListConfig ExpandConfig(
        WikipediaListConfigRaw raw,
        Dictionary<string, TaxaGroupDefinition> taxaGroups,
        Dictionary<string, ListPresetDefinition> presets) {

        var expandedLists = new List<WikipediaListDefinition>();
        // Track (group, preset) per expanded list id so the post-expansion pass can resolve a parent's
        // children to the actual {child}-{preset} ids — only for those that genuinely generated.
        var groupPresetById = new Dictionary<string, (string Group, string Preset)>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawList in raw.Lists) {
            // Multi-preset syntax: taxa_group + presets array
            if (!string.IsNullOrEmpty(rawList.TaxaGroup) && rawList.Presets is { Count: > 0 }) {
                foreach (var presetName in rawList.Presets) {
                    var syntheticRaw = new WikipediaListDefinitionRaw {
                        Id = $"{rawList.TaxaGroup}-{presetName}",
                        TaxaGroup = rawList.TaxaGroup,
                        Preset = presetName,
                        Templates = rawList.Templates,
                        Grouping = rawList.Grouping,
                        Display = rawList.Display,
                        CustomGroups = rawList.CustomGroups,
                    };
                    var expanded = ExpandFromReference(syntheticRaw, taxaGroups, presets);
                    if (expanded != null) {
                        expandedLists.Add(expanded);
                        groupPresetById[expanded.Id] = (rawList.TaxaGroup!, presetName);
                    }
                }
            }
            // Single preset syntax: taxa_group + preset
            else if (!string.IsNullOrEmpty(rawList.TaxaGroup) && !string.IsNullOrEmpty(rawList.Preset)) {
                var expanded = ExpandFromReference(rawList, taxaGroups, presets);
                if (expanded != null) {
                    expandedLists.Add(expanded);
                    groupPresetById[expanded.Id] = (rawList.TaxaGroup!, rawList.Preset!);
                }
            }
            else {
                // Already a full definition, just convert
                expandedLists.Add(ConvertToDefinition(rawList));
            }
        }

        ResolveChildLinks(expandedLists, groupPresetById, taxaGroups);

        return new WikipediaListConfig {
            Defaults = raw.Defaults ?? new WikipediaListDefaults(),
            Lists = expandedLists
        };
    }

    /// <summary>
    /// Post-expansion pass: attach each parent list's resolved child/see-also links. A link is added
    /// only when the corresponding <c>{childGroup}-{preset}</c> list actually exists in the expanded
    /// set — so a parent like <c>invertebrates-ew</c> won't emit a dangling link to a never-generated
    /// <c>insects-ew</c>. Title/filters come from the child's already-expanded definition (no re-gen).
    /// </summary>
    private static void ResolveChildLinks(
        List<WikipediaListDefinition> lists,
        Dictionary<string, (string Group, string Preset)> groupPresetById,
        Dictionary<string, TaxaGroupDefinition> taxaGroups) {

        var byId = new Dictionary<string, WikipediaListDefinition>(StringComparer.OrdinalIgnoreCase);
        foreach (var list in lists) byId[list.Id] = list;

        foreach (var def in lists) {
            if (!groupPresetById.TryGetValue(def.Id, out var gp)) continue;
            if (!taxaGroups.TryGetValue(gp.Group, out var group)) continue;

            AddChildLinks(def.SubLists, group.Children, GroupingKind.Phylogenetic, gp.Preset, byId, taxaGroups, def.Id);
            AddChildLinks(def.SeeAlso, group.SeeAlso, GroupingKind.SeeAlso, gp.Preset, byId, taxaGroups, def.Id);
        }
    }

    private static void AddChildLinks(
        List<ChildListLink> target,
        List<string>? childGroupNames,
        GroupingKind kind,
        string preset,
        Dictionary<string, WikipediaListDefinition> byId,
        Dictionary<string, TaxaGroupDefinition> taxaGroups,
        string parentId) {

        if (childGroupNames is null) return;

        foreach (var childGroupName in childGroupNames) {
            if (!taxaGroups.TryGetValue(childGroupName, out var childGroup)) {
                Console.Error.WriteLine($"Warning: child group '{childGroupName}' referenced by '{parentId}' not found in taxa-groups.yml");
                continue;
            }

            var childId = $"{childGroupName}-{preset}";
            if (!byId.TryGetValue(childId, out var childDef)) {
                // The child group exists but has no list for this preset (e.g. invertebrates-ew but
                // insects defines no 'ew'). Skip rather than emit a dangling wikilink.
                continue;
            }

            target.Add(new ChildListLink(
                Id: childId,
                DisplayName: childGroup.Name ?? childGroupName,
                Adjective: childGroup.Adjective ?? string.Empty,
                WikiTitle: DeriveWikiTitle(childDef.OutputFile),
                Filters: childDef.Filters,
                Kind: kind));
        }
    }

    /// <summary>Derive a Wikipedia article title from an output filename
    /// (e.g. "List_of_critically_endangered_insects.wikitext" → "List of critically endangered insects").</summary>
    private static string DeriveWikiTitle(string outputFile) {
        var name = Path.GetFileNameWithoutExtension(outputFile);
        return name.Replace('_', ' ');
    }

    private WikipediaListDefinition? ExpandFromReference(
        WikipediaListDefinitionRaw raw,
        Dictionary<string, TaxaGroupDefinition> taxaGroups,
        Dictionary<string, ListPresetDefinition> presets) {

        if (!taxaGroups.TryGetValue(raw.TaxaGroup!, out var taxaGroup)) {
            Console.Error.WriteLine($"Warning: Unknown taxa_group '{raw.TaxaGroup}' in list '{raw.Id}'");
            return null;
        }

        if (!presets.TryGetValue(raw.Preset!, out var preset)) {
            Console.Error.WriteLine($"Warning: Unknown preset '{raw.Preset}' in list '{raw.Id}'");
            return null;
        }

        // Build template variables
        var vars = new Dictionary<string, string> {
            ["taxa_name"] = taxaGroup.Name ?? raw.TaxaGroup!,
            ["taxa_name_lower"] = (taxaGroup.Name ?? raw.TaxaGroup!).ToLowerInvariant(),
            ["taxa_slug"] = ToSlug(taxaGroup.Name ?? raw.TaxaGroup!),
        };

        // Use explicit values from the list definition, or expand from templates
        var title = raw.Title ?? ExpandTemplate(preset.TitleTemplate, vars);
        var description = raw.Description ?? ExpandTemplate(preset.DescriptionTemplate, vars);
        var outputFile = raw.OutputFile ?? ExpandTemplate(preset.OutputTemplate, vars);

        // Stack the layers low→high: preset (under) → taxa-group → list (over). Per-field nulls fall
        // through, so each layer only overrides the keys it actually sets; the global defaults
        // baseline is applied later in the generator (DisplayPreferencesConfig.ResolveAgainst).
        var mergedDisplay = DisplayPreferencesConfig.Merge(preset.Display, taxaGroup.Display);
        mergedDisplay = DisplayPreferencesConfig.Merge(mergedDisplay, raw.Display);

        return new WikipediaListDefinition {
            Id = raw.Id,
            Title = title ?? $"List of {vars["taxa_name_lower"]}",
            Description = description,
            OutputFile = outputFile ?? $"{raw.Id}.wikitext",
            Templates = new TemplateSettings {
                Header = raw.Templates?.Header ?? preset.Templates?.Header,
                Footer = raw.Templates?.Footer ?? preset.Templates?.Footer,
            },
            Filters = raw.Filters ?? taxaGroup.Filters ?? new(),
            Sections = raw.Sections ?? preset.Sections ?? new(),
            Grouping = raw.Grouping,
            Display = mergedDisplay,
            CustomGroups = raw.CustomGroups ?? taxaGroup.CustomGroups,
            TaxaGroup = raw.TaxaGroup,
            Preset = raw.Preset,
            TaxaAdjective = taxaGroup.Adjective,
            TaxaNameLower = vars["taxa_name_lower"],
            StatusText = preset.StatusText,
            StatusWikiLink = preset.StatusWikiLink,
        };
    }

    private static WikipediaListDefinition ConvertToDefinition(WikipediaListDefinitionRaw raw) {
        return new WikipediaListDefinition {
            Id = raw.Id,
            Title = raw.Title ?? string.Empty,
            Description = raw.Description,
            OutputFile = raw.OutputFile ?? $"{raw.Id}.wikitext",
            Templates = raw.Templates ?? new(),
            Filters = raw.Filters ?? new(),
            Sections = raw.Sections ?? new(),
            Grouping = raw.Grouping,
            Display = raw.Display,
            CustomGroups = raw.CustomGroups,
        };
    }

    private static string? ExpandTemplate(string? template, Dictionary<string, string> vars) {
        if (string.IsNullOrEmpty(template)) return null;

        var result = template;
        foreach (var (key, value) in vars) {
            result = result.Replace($"{{{key}}}", value);
        }
        return result;
    }

    private static string ToSlug(string name) {
        // Convert "Ray-finned fishes" -> "ray-finned_fishes"
        return Regex.Replace(name.ToLowerInvariant(), @"[^a-z0-9]+", "_").Trim('_');
    }
}

// ==================== Raw YAML structures (before expansion) ====================

internal sealed class WikipediaListConfigRaw {
    public WikipediaListDefaults? Defaults { get; init; }
    public List<WikipediaListDefinitionRaw> Lists { get; init; } = new();
}

internal sealed class WikipediaListDefinitionRaw {
    public string Id { get; init; } = string.Empty;

    // Shorthand references
    public string? TaxaGroup { get; init; }
    public string? Preset { get; init; }
    public List<string>? Presets { get; init; }  // Multiple presets: generates one list per preset

    // Explicit values (override templates if provided)
    public string? Title { get; init; }
    public string? Description { get; init; }
    public string? OutputFile { get; init; }
    public TemplateSettings? Templates { get; init; }
    public List<TaxonFilterDefinition>? Filters { get; init; }
    public List<WikipediaSectionDefinition>? Sections { get; init; }
    public List<GroupingLevelDefinition>? Grouping { get; init; }
    public DisplayPreferencesConfig? Display { get; init; }
    
    /// <summary>
    /// Custom family-based grouping (for paraphyletic groups like marine mammals).
    /// </summary>
    public List<CustomGroupDefinition>? CustomGroups { get; init; }
}

// ==================== Supporting file structures ====================

internal sealed class TaxaGroupsFile {
    public Dictionary<string, TaxaGroupDefinition> Groups { get; init; } = new();
}

internal sealed class TaxaGroupDefinition {
    public string? Name { get; init; }
    /// <summary>
    /// Adjective form of the group name for use in prose (e.g., "mammalian", "amphibian").
    /// </summary>
    public string? Adjective { get; init; }
    public List<TaxonFilterDefinition>? Filters { get; init; }

    /// <summary>
    /// Phylogenetic child group names (e.g. invertebrates → [insects, gastropods, ...]). For each
    /// preset of this group, the loader attaches a <see cref="ChildListLink"/> to the matching
    /// <c>{child}-{preset}</c> list IF it exists, making this group's lists "parent" lists.
    /// </summary>
    public List<string>? Children { get; init; }

    /// <summary>
    /// Non-phylogenetic cross-reference group names (e.g. mammals → [marine-mammals]). Rendered as a
    /// plain "Related lists" bullet block, never as count rows or nested phylogenetic sub-lists.
    /// </summary>
    public List<string>? SeeAlso { get; init; }
    /// <summary>
    /// Custom family-based grouping for paraphyletic groups.
    /// When defined, these groups replace the normal taxonomic grouping.
    /// </summary>
    public List<CustomGroupDefinition>? CustomGroups { get; init; }
    /// <summary>
    /// Default display preferences for this taxa group.
    /// Can be overridden at the list level.
    /// </summary>
    public DisplayPreferencesConfig? Display { get; init; }
}

internal sealed class ListPresetsFile {
    public Dictionary<string, ListPresetDefinition> Presets { get; init; } = new();
}

internal sealed class ListPresetDefinition {
    public string? Name { get; init; }
    /// <summary>
    /// Human-readable status text (e.g., "critically endangered").
    /// </summary>
    public string? StatusText { get; init; }
    /// <summary>
    /// Wiki-linked status text (e.g., "[[Critically endangered species|critically endangered]]").
    /// </summary>
    public string? StatusWikiLink { get; init; }
    public string? TitleTemplate { get; init; }
    public string? DescriptionTemplate { get; init; }
    public string? OutputTemplate { get; init; }
    public TemplateSettings? Templates { get; init; }
    public List<WikipediaSectionDefinition>? Sections { get; init; }
    public DisplayPreferencesConfig? Display { get; init; }
}

// Display-preference layering now lives on DisplayPreferencesConfig (Merge + ResolveAgainst);
// the old lossy OR/!=default heuristic merger was removed in favour of per-field null-coalescing.
