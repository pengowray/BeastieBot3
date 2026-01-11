using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

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
                    };
                    var expanded = ExpandFromReference(syntheticRaw, taxaGroups, presets);
                    if (expanded != null) {
                        expandedLists.Add(expanded);
                    }
                }
            }
            // Single preset syntax: taxa_group + preset
            else if (!string.IsNullOrEmpty(rawList.TaxaGroup) && !string.IsNullOrEmpty(rawList.Preset)) {
                var expanded = ExpandFromReference(rawList, taxaGroups, presets);
                if (expanded != null) {
                    expandedLists.Add(expanded);
                }
            }
            else {
                // Already a full definition, just convert
                expandedLists.Add(ConvertToDefinition(rawList));
            }
        }

        return new WikipediaListConfig {
            Defaults = raw.Defaults ?? new WikipediaListDefaults(),
            Lists = expandedLists
        };
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
            Display = raw.Display,
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
    public DisplayPreferences? Display { get; init; }
}

// ==================== Supporting file structures ====================

internal sealed class TaxaGroupsFile {
    public Dictionary<string, TaxaGroupDefinition> Groups { get; init; } = new();
}

internal sealed class TaxaGroupDefinition {
    public string? Name { get; init; }
    public List<TaxonFilterDefinition>? Filters { get; init; }
}

internal sealed class ListPresetsFile {
    public Dictionary<string, ListPresetDefinition> Presets { get; init; } = new();
}

internal sealed class ListPresetDefinition {
    public string? Name { get; init; }
    public string? TitleTemplate { get; init; }
    public string? DescriptionTemplate { get; init; }
    public string? OutputTemplate { get; init; }
    public TemplateSettings? Templates { get; init; }
    public List<WikipediaSectionDefinition>? Sections { get; init; }
}
