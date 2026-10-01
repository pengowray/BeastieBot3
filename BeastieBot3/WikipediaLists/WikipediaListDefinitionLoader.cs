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

        // Load supporting files if they exist. A group's sub_groups block becomes ordinary groups and
        // list entries here (TaxaSubGroups), so everything below sees them like hand-written ones.
        var expandedGroups = TaxaSubGroups.Expand(LoadTaxaGroups(directory));
        foreach (var warning in expandedGroups.Warnings) {
            Console.Error.WriteLine($"Warning: {warning}");
        }
        var taxaGroups = expandedGroups.Groups;
        var listPresets = LoadListPresets(directory);

        using var reader = File.OpenText(fullPath);
        var parsed = _deserializer.Deserialize<WikipediaListConfigRaw>(reader);
        if (parsed is null) {
            throw new InvalidOperationException($"Unable to parse Wikipedia list config at {fullPath}.");
        }
        var rawConfig = new WikipediaListConfigRaw {
            Defaults = parsed.Defaults,
            Lists = TaxaSubGroups.InsertListEntries(parsed.Lists, expandedGroups.Lists),
        };

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
        // Groups whose list entry uses category_split, so a warning's fix names category_split, not presets.
        var categorySplitGroups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var rawList in raw.Lists) {
            // The category_split shorthand, when set, determines the preset fan-out (overriding `presets`).
            var effectivePresets = ResolveEffectivePresets(rawList);
            if (!string.IsNullOrEmpty(rawList.TaxaGroup) && !string.IsNullOrWhiteSpace(rawList.CategorySplit)) {
                categorySplitGroups.Add(rawList.TaxaGroup);
            }
            // Multi-preset syntax: taxa_group + presets array (or category_split)
            if (!string.IsNullOrEmpty(rawList.TaxaGroup) && effectivePresets is { Count: > 0 }) {
                foreach (var presetName in effectivePresets) {
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

        var childLinkNotes = ResolveChildLinks(expandedLists, groupPresetById, categorySplitGroups, taxaGroups);

        return new WikipediaListConfig {
            Defaults = raw.Defaults ?? new WikipediaListDefaults(),
            Lists = expandedLists,
            ChildLinkNotes = childLinkNotes,
        };
    }

    /// <summary>The preset whose single page lists every category (see list-presets.yml).</summary>
    internal const string AllStatusPreset = "all-status";

    /// <summary>
    /// Post-expansion pass: attach each parent list's resolved child/see-also links, and return one
    /// <see cref="ChildLinkNote"/> per (list, sub-group) pair saying what was linked. A link is added only
    /// when the list it points to exists in the expanded set, so a parent like <c>invertebrates-ew</c>
    /// won't emit a dangling link to a never-generated <c>insects-ew</c>. Title/filters come from the
    /// child's already-expanded definition (no re-gen).
    /// </summary>
    private static List<ChildLinkNote> ResolveChildLinks(
        List<WikipediaListDefinition> lists,
        Dictionary<string, (string Group, string Preset)> groupPresetById,
        IReadOnlySet<string> categorySplitGroups,
        Dictionary<string, TaxaGroupDefinition> taxaGroups) {

        var byId = new Dictionary<string, WikipediaListDefinition>(StringComparer.OrdinalIgnoreCase);
        foreach (var list in lists) byId[list.Id] = list;
        var listIds = new HashSet<string>(byId.Keys, StringComparer.OrdinalIgnoreCase);
        var groupsWithLists = new HashSet<string>(groupPresetById.Values.Select(v => v.Group), StringComparer.OrdinalIgnoreCase);
        var entries = new ChildEntryKinds(groupsWithLists, categorySplitGroups);
        var notes = new List<ChildLinkNote>();

        foreach (var def in lists) {
            if (!groupPresetById.TryGetValue(def.Id, out var gp)) continue;
            if (!taxaGroups.TryGetValue(gp.Group, out var group)) continue;

            // An all-status list stands in for a sub-group's missing preset list only on a page that
            // already links at least one sub-group list for its own preset. That completes a parent page
            // (plants-threatened also links conifers-all-status) without turning an ordinary list into a
            // parent page (plants-nt, where no sub-group has an nt list, stays as it is).
            var hasPresetChild = group.Children?.Any(c => listIds.Contains($"{c}-{gp.Preset}")) == true;
            AddChildLinks(def, gp, group.Children, GroupingKind.Phylogenetic, allowAllStatus: hasPresetChild,
                byId, listIds, entries, taxaGroups, notes);
            // A see-also link is a bullet under "Related lists" and does not change the page layout, so
            // its all-status stand-in needs no such condition.
            AddChildLinks(def, gp, group.SeeAlso, GroupingKind.SeeAlso, allowAllStatus: true,
                byId, listIds, entries, taxaGroups, notes);
        }

        return notes;
    }

    // Which taxa groups have a list entry in wikipedia-lists.yml, and which of those use category_split.
    private sealed record ChildEntryKinds(IReadOnlySet<string> WithLists, IReadOnlySet<string> CategorySplit);

    private static void AddChildLinks(
        WikipediaListDefinition parent,
        (string Group, string Preset) parentGroupPreset,
        List<string>? childGroupNames,
        GroupingKind kind,
        bool allowAllStatus,
        Dictionary<string, WikipediaListDefinition> byId,
        IReadOnlySet<string> listIds,
        ChildEntryKinds entries,
        Dictionary<string, TaxaGroupDefinition> taxaGroups,
        List<ChildLinkNote> notes) {

        if (childGroupNames is null) return;
        var target = kind == GroupingKind.SeeAlso ? parent.SeeAlso : parent.SubLists;
        var (parentGroup, preset) = parentGroupPreset;

        foreach (var childGroupName in childGroupNames) {
            if (!taxaGroups.TryGetValue(childGroupName, out var childGroup)) {
                notes.Add(new ChildLinkNote(parent.Id, parentGroup, preset, childGroupName, null, kind,
                    ChildLinkOutcome.UnknownGroup, null, ChildHasLists: false));
                continue;
            }

            var (outcome, childId) = ResolveChildLink(childGroupName, preset, listIds, allowAllStatus);
            notes.Add(new ChildLinkNote(parent.Id, parentGroup, preset, childGroupName, childGroup.Name, kind,
                outcome, childId, entries.WithLists.Contains(childGroupName),
                ChildUsesCategorySplit: entries.CategorySplit.Contains(childGroupName)));
            if (childId is null) continue;

            var childDef = byId[childId];
            target.Add(new ChildListLink(
                Id: childDef.Id,
                DisplayName: childGroup.Name ?? childGroupName,
                Adjective: childGroup.Adjective ?? string.Empty,
                WikiTitle: DeriveWikiTitle(childDef.OutputFile),
                Filters: childDef.Filters,
                Kind: kind));
        }
    }

    /// <summary>
    /// The list a parent links for one sub-group: the sub-group's list for the parent's preset
    /// (<c>{child}-{preset}</c>); failing that, when <paramref name="allowAllStatus"/>, its all-status
    /// list; otherwise none.
    /// </summary>
    internal static (ChildLinkOutcome Outcome, string? ListId) ResolveChildLink(
        string childGroup, string preset, IReadOnlySet<string> listIds, bool allowAllStatus) {
        var presetId = $"{childGroup}-{preset}";
        if (listIds.Contains(presetId)) {
            return (ChildLinkOutcome.Linked, presetId);
        }
        var allStatusId = $"{childGroup}-{AllStatusPreset}";
        if (allowAllStatus && listIds.Contains(allStatusId)) {
            return (ChildLinkOutcome.LinkedAllStatus, allStatusId);
        }
        return (ChildLinkOutcome.NoList, null);
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

        // Build template variables. A group named by a scientific name (an order such as
        // "Malpighiales") sets keep_name_case, so its title and file name keep the capital letter.
        var taxaName = taxaGroup.Name ?? raw.TaxaGroup!;
        var vars = new Dictionary<string, string> {
            ["taxa_name"] = taxaName,
            ["taxa_name_lower"] = taxaGroup.KeepNameCase ? taxaName : taxaName.ToLowerInvariant(),
            ["taxa_slug"] = ToSlug(taxaName, taxaGroup.KeepNameCase),
            // Singular attributive form ("mammalian", "bird", "plant") so descriptions read
            // "Mammalian taxa …" rather than the ungrammatical plural "Mammals taxa …".
            ["taxa_adjective"] = taxaGroup.Adjective ?? (taxaGroup.Name ?? raw.TaxaGroup!).ToLowerInvariant(),
        };

        // Use explicit values from the list definition, or expand from templates
        var title = raw.Title ?? ExpandTemplate(preset.TitleTemplate, vars);
        var description = raw.Description ?? ExpandTemplate(preset.DescriptionTemplate, vars);
        // Preset descriptions start with the (lowercase) singular adjective; capitalize the leading
        // letter so the sentence reads "Plant taxa …" not "plant taxa …". Explicit raw descriptions
        // are left as authored.
        if (raw.Description is null && !string.IsNullOrEmpty(description)) {
            description = char.ToUpperInvariant(description[0]) + description[1..];
        }
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
            Categories = BuildCategoryLines(taxaGroup, preset, vars),
            SizeBudgetMaxEntries = taxaGroup.SizeBudget?.MaxEntries,
        };
    }

    // IUCN statuses that have a "IUCN Red List <status> species" maintenance category on en-wiki.
    // Combined ("threatened") and legacy ("conservation dependent") presets have no single such
    // category, so they get no universal status category.
    private static readonly HashSet<string> IucnStatusCategoryStatuses = new(StringComparer.OrdinalIgnoreCase) {
        "critically endangered", "endangered", "vulnerable", "near threatened",
        "least concern", "data deficient", "extinct",
    };

    /// <summary>
    /// Builds the <c>[[Category:...]]</c> footer lines for a preset-expanded list, mirroring the
    /// en-wikipedia convention: a universal "IUCN Red List {status} species" category plus whichever
    /// curated taxa-group categories are configured, each with a per-category sort key (no DEFAULTSORT).
    /// </summary>
    private static List<string> BuildCategoryLines(
        TaxaGroupDefinition taxaGroup, ListPresetDefinition preset, Dictionary<string, string> vars) {
        var lines = new List<string>();
        var statusText = preset.StatusText;
        if (string.IsNullOrWhiteSpace(statusText)) {
            return lines;
        }

        var taxaName = vars["taxa_name"];
        var taxaLower = vars["taxa_name_lower"];
        var statusCap = char.ToUpperInvariant(statusText[0]) + statusText[1..];

        // Universal status category, sorted to the front of the category with "*<TaxaName>".
        if (IucnStatusCategoryStatuses.Contains(statusText)) {
            lines.Add($"[[Category:IUCN Red List {statusText} species|*{taxaName}]]");
        }

        var cats = taxaGroup.Categories;
        if (cats is not null) {
            if (!string.IsNullOrWhiteSpace(cats.KingdomList)) {
                lines.Add($"[[Category:{cats.KingdomList}|{statusCap} {taxaLower}]]");
            }
            if (!string.IsNullOrWhiteSpace(cats.List)) {
                lines.Add($"[[Category:{cats.List}|{statusCap} {taxaLower}]]");
            }
            if (!string.IsNullOrWhiteSpace(cats.Conservation)) {
                lines.Add($"[[Category:{cats.Conservation}]]");
            }
        }

        return lines;
    }

    // Resolve the effective preset list for a raw list entry: the category_split shorthand wins when
    // set, otherwise the explicit `presets` array.
    internal static List<string>? ResolveEffectivePresets(WikipediaListDefinitionRaw raw) {
        if (string.IsNullOrWhiteSpace(raw.CategorySplit)) {
            return raw.Presets;
        }
        switch (raw.CategorySplit.Trim().ToLowerInvariant()) {
            case "separate":
                return new List<string> { "cr", "en", "vu", "nt", "dd", "lc", "ew", "ex" };
            case "combined-threatened":
            case "combined":
                return new List<string> { "threatened", "nt", "dd", "lc", "ew", "ex" };
            // "merged" goes one step further than combined-threatened: it ALSO folds the two
            // extinction pages (ex + ew) into a single "extinct-combined" page. Net: one threatened
            // page, one extinction page, and separate nt/dd/lc. This is the "fewest reader-friendly
            // pages" option (CR/EN/VU together, EX/EW/PE/PEW together).
            case "merged":
                return new List<string> { "extinct-combined", "threatened", "nt", "dd", "lc" };
            case "all-status":
            case "single":
                return new List<string> { "all-status" };
            default:
                Console.Error.WriteLine($"Warning: unknown category_split '{raw.CategorySplit}' for taxa_group '{raw.TaxaGroup}'; using explicit presets.");
                return raw.Presets;
        }
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

    private static string ToSlug(string name, bool keepCase = false) {
        // Convert "Ray-finned fishes" -> "ray_finned_fishes"; with keepCase, "Malpighiales" -> "Malpighiales"
        var text = keepCase ? name : name.ToLowerInvariant();
        return Regex.Replace(text, @"[^A-Za-z0-9]+", "_").Trim('_');
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

    // Tuning shorthand: when set, determines which preset pages this group fans out to (overrides
    // `presets`). Values: "separate" (one page per category: cr/en/vu/nt/dd/lc/ew/ex),
    // "combined-threatened" (one "threatened" page instead of cr/en/vu, + nt/dd/lc/ew/ex),
    // "merged" (threatened page + ONE extinct-combined page for ex/ew/pe/pew, + nt/dd/lc),
    // "all-status" (a single page covering all categories). Lets a group be retuned from
    // thin per-category slices toward fewer combined pages without hand-editing the preset list.
    public string? CategorySplit { get; init; }

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
    /// Keep the capital letters of <see cref="Name"/> in list titles and file names. For groups named
    /// by a scientific name, such as the order "Malpighiales" ("List of threatened Malpighiales").
    /// </summary>
    public bool KeepNameCase { get; init; }

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
    public List<string>? Children { get; set; }

    /// <summary>
    /// Sub-groups defined by one value each at one rank (e.g. the largest orders of dicots), with the
    /// presets they get lists for. Expanded into ordinary groups, children and list entries by
    /// <see cref="TaxaSubGroups.Expand"/>.
    /// </summary>
    public SubGroupsDefinition? SubGroups { get; init; }

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

    /// <summary>
    /// Curated en-wikipedia category names for this group's list articles. Only the parts that exist
    /// on en-wiki are filled in; lists still always get the universal "IUCN Red List {status} species"
    /// category derived from the preset status.
    /// </summary>
    public TaxaCategoryDefinition? Categories { get; init; }

    /// <summary>
    /// Optional "stay under this size" budget for this group's pages, used by the impact preview /
    /// structure metrics to flag oversized lists (e.g. plants need splitting). Advisory only.
    /// </summary>
    public SizeBudgetDefinition? SizeBudget { get; init; }
}

/// <summary>Target size budget for a taxa group's list pages (advisory; drives the impact preview).</summary>
internal sealed class SizeBudgetDefinition {
    /// <summary>Flag a page whose renderable-row (bullet) count exceeds this.</summary>
    public int? MaxEntries { get; init; }
    /// <summary>Flag a page whose raw wikitext byte size exceeds this (en.wiki ~2,000,000).</summary>
    public long? MaxBytes { get; init; }
}

/// <summary>
/// Curated en-wikipedia category base names for a taxa group, used to build the list-article footer.
/// Each maps to one <c>[[Category:...]]</c> line; the loader appends the per-category sort key.
/// </summary>
internal sealed class TaxaCategoryDefinition {
    /// <summary>e.g. "Lists of animals by conservation status" → sort key "{Status} {taxa}".</summary>
    public string? KingdomList { get; init; }
    /// <summary>e.g. "Lists of mammals" → sort key "{Status} {taxa}".</summary>
    public string? List { get; init; }
    /// <summary>e.g. "Mammal conservation" (no sort key).</summary>
    public string? Conservation { get; init; }
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
