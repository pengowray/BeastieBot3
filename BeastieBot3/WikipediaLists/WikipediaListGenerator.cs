using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BeastieBot3.WikipediaLists.Legacy;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaListGenerator {
    private readonly IucnListQueryService _queryService;
    private readonly WikipediaTemplateRenderer _templateRenderer;
    private readonly LegacyTaxaRuleList _legacyRules;
    private readonly TaxonRulesService? _taxonRules;
    private readonly CommonNameProvider? _commonNameProvider;
    private readonly StoreBackedCommonNameProvider? _storeBackedProvider;
    private readonly ColTaxonomyEnricher? _colEnricher;

    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        CommonNameProvider? commonNameProvider,
        TaxonRulesService? taxonRules = null) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = commonNameProvider;
        _storeBackedProvider = null;
        _colEnricher = null;
        _taxonRules = taxonRules;
    }

    /// <summary>
    /// Constructor using the new store-backed common name provider with pre-aggregated names.
    /// </summary>
    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        StoreBackedCommonNameProvider? storeBackedProvider,
        ColTaxonomyEnricher? colEnricher = null,
        TaxonRulesService? taxonRules = null) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = null;
        _storeBackedProvider = storeBackedProvider;
        _colEnricher = colEnricher;
        _taxonRules = taxonRules;
    }

    public WikipediaListResult Generate(
        WikipediaListDefinition definition,
        WikipediaListDefaults defaults,
        string outputDirectory,
        int? limit) {
        var statusDescriptors = CollectStatusDescriptors(definition);
        var records = _queryService.QuerySpecies(definition, statusDescriptors, limit);
        
        // Apply exclusion rules if taxon rules are configured
        if (_taxonRules != null) {
            records = records.Where(r => !ShouldExcludeRecord(r, definition.Id)).ToList();
        }
        
        var sections = PrepareSections(definition);
        foreach (var section in sections) {
            section.Records.AddRange(records.Where(record => section.StatusSet.Contains(record.StatusCode)));
        }

        var totalCount = sections.Sum(section => section.Records.Count);
        var datasetVersion = _queryService.GetDatasetVersion();

        var scopeLabel = BuildScopeLabel(definition);
        var sectionSummary = string.Join("; ", sections.Select(section => $"{section.Definition.Heading} ({section.Records.Count})"));

        var context = new Dictionary<string, object?> {
            ["title"] = definition.Title,
            ["description"] = definition.Description,
            ["scope_label"] = scopeLabel,
            ["dataset_version"] = datasetVersion,
            ["generated_at"] = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ["total_entries"] = totalCount,
            ["sections_summary"] = sectionSummary
        };

        var headerTemplate = definition.Templates.Header ?? defaults.HeaderTemplate;
        var footerTemplate = definition.Templates.Footer ?? defaults.FooterTemplate;

        var builder = new StringBuilder();
        builder.AppendLine(_templateRenderer.Render(headerTemplate, context).TrimEnd());
        builder.AppendLine();

        var grouping = (IReadOnlyList<GroupingLevelDefinition>)(definition.Grouping
            ?? defaults.Grouping
            ?? new List<GroupingLevelDefinition>());
        var display = definition.Display ?? defaults.Display ?? new DisplayPreferences();

        var totalHeadingCount = 0;
        foreach (var section in sections) {
            if (section.Records.Count == 0) {
                continue;
            }

            if (!section.Definition.HideHeading) {
                builder.AppendLine($"== {section.Definition.Heading} ==");
                totalHeadingCount++;
            }

            if (!string.IsNullOrWhiteSpace(section.Definition.Description)) {
                builder.AppendLine(section.Definition.Description);
                builder.AppendLine();
            }

            var (sectionBody, sectionHeadingCount) = BuildSectionBody(
                section.Records, grouping, display, section.StatusContext, definition.CustomGroups);
            totalHeadingCount += sectionHeadingCount;
            builder.AppendLine(sectionBody);
            builder.AppendLine();
        }

        builder.AppendLine(_templateRenderer.Render(footerTemplate, context).TrimEnd());
        builder.AppendLine();

        Directory.CreateDirectory(outputDirectory);
        var outputPath = Path.Combine(outputDirectory, definition.OutputFile);
        File.WriteAllText(outputPath, builder.ToString());

        return new WikipediaListResult(outputPath, totalCount, totalHeadingCount, datasetVersion);
    }

    private static readonly Dictionary<string, int> RankOrder = new(StringComparer.OrdinalIgnoreCase) {
        ["kingdom"] = 1,
        ["phylum"] = 2,
        ["class"] = 3,
        ["order"] = 4,
        ["family"] = 5,
        ["genus"] = 6,
        ["species"] = 7
    };

    private static string BuildScopeLabel(WikipediaListDefinition definition) {
        if (definition.Filters.Count == 0) {
            return "global";
        }

        var ordered = definition.Filters
            .OrderBy(filter => RankOrder.GetValueOrDefault(filter.Rank?.Trim().ToLowerInvariant() ?? "", 99))
            .Select(filter => filter.Value.Trim())
            .ToList();

        return string.Join(" › ", ordered);
    }

    private static List<SectionRuntime> PrepareSections(WikipediaListDefinition definition) {
        var list = new List<SectionRuntime>();
        foreach (var section in definition.Sections) {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var status in section.Statuses) {
                if (!string.IsNullOrWhiteSpace(status.Code)) {
                    set.Add(status.Code.Trim());
                }
            }

            list.Add(new SectionRuntime(section, set));
        }
        return list;
    }

    private static IReadOnlyList<RedlistStatusDescriptor> CollectStatusDescriptors(WikipediaListDefinition definition) {
        var map = new Dictionary<string, RedlistStatusDescriptor>(StringComparer.OrdinalIgnoreCase);
        foreach (var section in definition.Sections) {
            foreach (var status in section.Statuses) {
                if (string.IsNullOrWhiteSpace(status.Code)) {
                    continue;
                }

                if (!IucnRedlistStatus.TryGetDescriptor(status.Code, out var descriptor)) {
                    throw new InvalidOperationException($"Unknown IUCN status code '{status.Code}' referenced by list '{definition.Id}'.");
                }

                map[descriptor.Code] = descriptor;
            }
        }

        return map.Values.ToList();
    }

    private (string Body, int HeadingCount) BuildSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records, 
        IReadOnlyList<GroupingLevelDefinition> grouping, 
        DisplayPreferences display, 
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups = null,
        int startHeading = 3) {
        
        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        // If custom groups are defined, use custom grouping instead of taxonomic grouping
        if (customGroups != null && customGroups.Count > 0) {
            return BuildCustomGroupedSectionBody(records, customGroups, grouping, display, statusContext, startHeading);
        }

        if (grouping.Count == 0) {
            return (string.Join(Environment.NewLine, records.Select(record => FormatSpeciesLine(record, display, statusContext))), 0);
        }

        // Check if we need COL enrichment:
        // 1. Any grouping level uses COL-specific ranks
        // 2. Any taxon uses virtual groups (which rely on COL superfamily/family)
        var needsEnrichment = _colEnricher != null && 
            (grouping.Any(g => IsColEnrichedRank(g.Level)) || HasVirtualGroupsInGrouping(grouping));
        
        if (needsEnrichment) {
            return BuildEnrichedSectionBody(records, grouping, display, statusContext);
        }

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<IucnSpeciesRecord>(
                level.Label ?? level.Level,
                BuildSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel,
                level.MinItems,
                level.OtherLabel))
            .ToList();

        Func<string, bool>? shouldSkip = _taxonRules != null 
            ? taxon => _taxonRules.ShouldForceSplit(taxon) 
            : null;
        var tree = TaxonomyTreeBuilder.Build(records, levels, shouldSkip);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, tree, startHeading, display, statusContext, ref headingCount);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    /// <summary>
    /// Check if any taxa in the grouping hierarchy might use virtual groups.
    /// </summary>
    private bool HasVirtualGroupsInGrouping(IReadOnlyList<GroupingLevelDefinition> grouping) {
        if (_taxonRules == null) {
            return false;
        }

        // Check if any grouping level might have virtual groups defined
        foreach (var level in grouping) {
            var levelName = level.Level.ToLowerInvariant();
            // Order level is the most likely to have virtual groups
            if (levelName == "order") {
                // Check if we have any virtual groups defined for any order
                // (Squamata, Artiodactyla, Cetartiodactyla, Carnivora, etc.)
                if (_taxonRules.HasAnyVirtualGroups()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static readonly HashSet<string> ColEnrichedRanks = new(StringComparer.OrdinalIgnoreCase) {
        "subkingdom", "subphylum", "superclass", "subclass", "infraclass",
        "superorder", "suborder", "infraorder", "parvorder",
        "superfamily", "subfamily", "tribe", "subtribe", "subgenus"
    };

    private static bool IsColEnrichedRank(string level) => ColEnrichedRanks.Contains(level);

    /// <summary>
    /// Build section body using custom family-based groups instead of taxonomic hierarchy.
    /// Used for paraphyletic groups like marine mammals.
    /// </summary>
    private (string Body, int HeadingCount) BuildCustomGroupedSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records, 
        IReadOnlyList<CustomGroupDefinition> customGroups,
        IReadOnlyList<GroupingLevelDefinition> subGrouping,
        DisplayPreferences display, 
        string? statusContext,
        int startHeading = 3) {
        
        var builder = new StringBuilder();
        var headingCount = 0;

        // Group records by custom group based on family
        var groupedRecords = new Dictionary<CustomGroupDefinition, List<IucnSpeciesRecord>>();
        CustomGroupDefinition? defaultGroup = null;
        List<IucnSpeciesRecord>? unmatchedRecords = null;

        // Initialize groups
        foreach (var group in customGroups) {
            groupedRecords[group] = new List<IucnSpeciesRecord>();
            if (group.Default) {
                defaultGroup = group;
            }
        }

        // Assign records to groups
        foreach (var record in records) {
            var matchedGroup = FindMatchingCustomGroup(record, customGroups);
            if (matchedGroup != null) {
                groupedRecords[matchedGroup].Add(record);
            } else if (defaultGroup != null) {
                groupedRecords[defaultGroup].Add(record);
            } else {
                unmatchedRecords ??= new List<IucnSpeciesRecord>();
                unmatchedRecords.Add(record);
            }
        }

        // Build remaining grouping levels (skip first level since custom groups replace it)
        var remainingGrouping = subGrouping.Count > 1 
            ? subGrouping.Skip(1).ToList() 
            : new List<GroupingLevelDefinition>();

        // Render each custom group
        foreach (var group in customGroups) {
            var groupRecords = groupedRecords[group];
            if (groupRecords.Count == 0) {
                continue;
            }

            // Group heading at startHeading level
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            var displayName = !string.IsNullOrWhiteSpace(group.CommonPlural)
                ? Uppercase(group.CommonPlural)!
                : group.Name;
            builder.AppendLine($"{headingMarkup} {displayName} {headingMarkup}");
            headingCount++;

            if (!string.IsNullOrWhiteSpace(group.MainArticle)) {
                builder.AppendLine($"{{{{main|{group.MainArticle}}}}}");
            }

            // Render records with remaining grouping (e.g., by family) at next heading level
            if (remainingGrouping.Count > 0) {
                var (groupBody, groupHeadingCount) = BuildSectionBody(
                    groupRecords, remainingGrouping, display, statusContext, 
                    customGroups: null, startHeading: headingLevel + 1);
                headingCount += groupHeadingCount;
                builder.AppendLine(groupBody);
            } else {
                // No sub-grouping, just output records
                foreach (var record in groupRecords.OrderBy(r => r.ScientificNameTaxonomy ?? r.ScientificNameAssessments)) {
                    builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
                }
            }
            builder.AppendLine();
        }

        // Handle any unmatched records
        if (unmatchedRecords != null && unmatchedRecords.Count > 0) {
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            builder.AppendLine($"{headingMarkup} Other {headingMarkup}");
            headingCount++;
            foreach (var record in unmatchedRecords.OrderBy(r => r.ScientificNameTaxonomy ?? r.ScientificNameAssessments)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
            }
            builder.AppendLine();
        }

        return (builder.ToString().TrimEnd(), headingCount);
    }

    /// <summary>
    /// Find which custom group a record belongs to based on family membership.
    /// </summary>
    private static CustomGroupDefinition? FindMatchingCustomGroup(
        IucnSpeciesRecord record, 
        IReadOnlyList<CustomGroupDefinition> customGroups) {
        
        var family = record.FamilyName;
        if (string.IsNullOrWhiteSpace(family)) {
            return null;
        }

        // Check non-default groups first (in order)
        foreach (var group in customGroups.Where(g => !g.Default)) {
            if (group.Families.Any(f => f.Equals(family, StringComparison.OrdinalIgnoreCase))) {
                return group;
            }
        }

        return null; // Let caller assign to default group
    }

    private (string Body, int HeadingCount) BuildEnrichedSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records, 
        IReadOnlyList<GroupingLevelDefinition> grouping, 
        DisplayPreferences display, 
        string? statusContext) {
        
        // Enrich records with COL taxonomy
        var enrichedRecords = _colEnricher!.Enrich(records, CancellationToken.None);

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<EnrichedSpeciesRecord>(
                level.Label ?? level.Level,
                BuildEnrichedSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel,
                level.MinItems,
                level.OtherLabel))
            .ToList();

        Func<string, bool>? shouldSkip = _taxonRules != null 
            ? taxon => _taxonRules.ShouldForceSplit(taxon) 
            : null;
        var tree = TaxonomyTreeBuilder.Build(enrichedRecords, levels, shouldSkip);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendEnrichedTree(builder, tree, startHeading: 3, display, statusContext, ref headingCount);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    private void AppendEnrichedTree(
        StringBuilder builder, 
        TaxonomyTreeNode<EnrichedSpeciesRecord> node, 
        int startHeading, 
        DisplayPreferences display, 
        string? statusContext, 
        ref int headingCount) {
        
        foreach (var child in node.Children) {
            var taxonName = child.Value;
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            var heading = FormatHeading(taxonName, child.Label, GetKingdomName(child));
            builder.AppendLine($"{headingMarkup} {heading.Text} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.MainLink)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }

            // Check if this taxon uses virtual groups
            if (!string.IsNullOrWhiteSpace(taxonName) &&
                _taxonRules != null && 
                _taxonRules.ShouldUseVirtualGroups(taxonName) && 
                _taxonRules.HasVirtualGroups(taxonName)) {
                // Render virtual groups instead of normal children
                AppendVirtualGroups(builder, child, taxonName, headingLevel + 1, display, statusContext, ref headingCount);
            } else {
                AppendEnrichedTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount);
            }
        }

        // Convert enriched records to IUCN records for output
        var iucnRecords = node.Items.Select(r => r.ToIucnRecord()).ToList();
        if (display.GroupSubspecies) {
            AppendItemsWithSubspeciesGrouping(builder, iucnRecords, display, statusContext);
        } else {
            foreach (var record in iucnRecords) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
            }
        }
    }

    /// <summary>
    /// Appends items grouped by virtual groups (e.g., Snakes, Lizards, Worm lizards for Squamata).
    /// </summary>
    private void AppendVirtualGroups(
        StringBuilder builder,
        TaxonomyTreeNode<EnrichedSpeciesRecord> parentNode,
        string parentTaxon,
        int headingLevel,
        DisplayPreferences display,
        string? statusContext,
        ref int headingCount) {
        
        // Collect all enriched records from this node and its descendants
        var allRecords = CollectAllEnrichedRecords(parentNode);

        // Group by virtual group
        var groupedRecords = new Dictionary<VirtualGroup, List<EnrichedSpeciesRecord>>();
        VirtualGroup? defaultGroup = null;
        List<EnrichedSpeciesRecord>? unmatchedRecords = null;

        foreach (var record in allRecords) {
            var virtualGroup = _taxonRules!.ResolveVirtualGroup(
                parentTaxon, 
                record.FamilyName, 
                record.Superfamily, 
                clade: null); // TODO: We don't have clade in enriched record yet

            if (virtualGroup == null) {
                // No match, collect for later
                unmatchedRecords ??= new List<EnrichedSpeciesRecord>();
                unmatchedRecords.Add(record);
            } else if (virtualGroup.Default) {
                defaultGroup = virtualGroup;
                if (!groupedRecords.ContainsKey(virtualGroup)) {
                    groupedRecords[virtualGroup] = new List<EnrichedSpeciesRecord>();
                }
                groupedRecords[virtualGroup].Add(record);
            } else {
                if (!groupedRecords.ContainsKey(virtualGroup)) {
                    groupedRecords[virtualGroup] = new List<EnrichedSpeciesRecord>();
                }
                groupedRecords[virtualGroup].Add(record);
            }
        }

        // Add unmatched records to the default group
        if (unmatchedRecords != null && defaultGroup != null && groupedRecords.ContainsKey(defaultGroup)) {
            groupedRecords[defaultGroup].AddRange(unmatchedRecords);
            unmatchedRecords = null;
        }

        // Render each virtual group as a heading
        var virtualGroupConfig = _taxonRules!.GetVirtualGroups(parentTaxon);
        if (virtualGroupConfig != null) {
            foreach (var vg in virtualGroupConfig.Groups) {
                if (!groupedRecords.TryGetValue(vg, out var records) || records.Count == 0) {
                    continue;
                }

                var headingMarkup = new string('=', Math.Min(headingLevel, 6));
                var groupHeading = FormatVirtualGroupHeading(vg);
                builder.AppendLine($"{headingMarkup} {groupHeading.Text} {headingMarkup}");
                headingCount++;
                if (!string.IsNullOrWhiteSpace(groupHeading.MainLink)) {
                    builder.AppendLine($"{{{{main|{groupHeading.MainLink}}}}}");
                }

                // Sort and output records for this group, grouped by family
                var recordsByFamily = records
                    .GroupBy(r => r.FamilyName ?? "Unknown")
                    .OrderBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                // If multiple families, add family subheadings
                if (recordsByFamily.Count > 1) {
                    foreach (var familyGroup in recordsByFamily) {
                        var familyHeadingLevel = Math.Min(headingLevel + 1, 6);
                        var familyHeadingMarkup = new string('=', familyHeadingLevel);
                        var familyHeading = FormatHeading(familyGroup.Key, "family", GetKingdomName(familyGroup));
                        builder.AppendLine($"{familyHeadingMarkup} {familyHeading.Text} {familyHeadingMarkup}");
                        headingCount++;
                        if (!string.IsNullOrWhiteSpace(familyHeading.MainLink)) {
                            builder.AppendLine($"{{{{main|{familyHeading.MainLink}}}}}");
                        }

                        OutputEnrichedRecords(builder, familyGroup.ToList(), display, statusContext);
                    }
                } else {
                    // Single family, no extra heading needed
                    OutputEnrichedRecords(builder, records, display, statusContext);
                }
            }
        }

        // Handle any remaining unmatched records (shouldn't happen if default group is defined)
        if (unmatchedRecords != null && unmatchedRecords.Count > 0) {
            var headingMarkup = new string('=', Math.Min(headingLevel, 6));
            builder.AppendLine($"{headingMarkup} Other {headingMarkup}");
            headingCount++;
            OutputEnrichedRecords(builder, unmatchedRecords, display, statusContext);
        }
    }

    /// <summary>
    /// Collects all enriched records from a node and all its descendants.
    /// </summary>
    private static List<EnrichedSpeciesRecord> CollectAllEnrichedRecords(TaxonomyTreeNode<EnrichedSpeciesRecord> node) {
        var result = new List<EnrichedSpeciesRecord>();
        CollectRecordsRecursive(node, result);
        return result;
    }

    private static void CollectRecordsRecursive(TaxonomyTreeNode<EnrichedSpeciesRecord> node, List<EnrichedSpeciesRecord> result) {
        result.AddRange(node.Items);
        foreach (var child in node.Children) {
            CollectRecordsRecursive(child, result);
        }
    }

    /// <summary>
    /// Output enriched records (converted to IUCN records for compatibility).
    /// </summary>
    private void OutputEnrichedRecords(
        StringBuilder builder,
        IReadOnlyList<EnrichedSpeciesRecord> records,
        DisplayPreferences display,
        string? statusContext) {
        
        var iucnRecords = records.Select(r => r.ToIucnRecord()).ToList();
        if (display.GroupSubspecies) {
            AppendItemsWithSubspeciesGrouping(builder, iucnRecords, display, statusContext);
        } else {
            foreach (var record in iucnRecords) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
            }
        }
    }

    /// <summary>
    /// Format a virtual group heading.
    /// </summary>
    private static HeadingInfo FormatVirtualGroupHeading(VirtualGroup group) {
        var displayName = !string.IsNullOrWhiteSpace(group.CommonPlural) 
            ? Uppercase(group.CommonPlural) 
            : !string.IsNullOrWhiteSpace(group.CommonName)
                ? Uppercase(group.CommonName)
                : group.Name;
        
        return new HeadingInfo(displayName!, group.MainArticle);
    }

    private static Func<IucnSpeciesRecord, string?> BuildSelector(string level) => level.ToLowerInvariant() switch {
        "kingdom" => record => record.KingdomName,
        "phylum" => record => record.PhylumName,
        "class" => record => record.ClassName,
        "order" => record => record.OrderName,
        "family" => record => record.FamilyName,
        "genus" => record => record.GenusName,
        _ => _ => null
    };

    /// <summary>
    /// Build a selector for enriched records that includes COL's additional ranks.
    /// </summary>
    private static Func<EnrichedSpeciesRecord, string?> BuildEnrichedSelector(string level) => level.ToLowerInvariant() switch {
        // Standard IUCN ranks
        "kingdom" => record => record.KingdomName,
        "phylum" => record => record.PhylumName,
        "class" => record => record.ClassName,
        "order" => record => record.OrderName,
        "family" => record => record.FamilyName,
        "genus" => record => record.GenusName,
        // COL-enriched intermediate ranks
        "subkingdom" => record => record.Subkingdom,
        "subphylum" => record => record.Subphylum,
        "superclass" => record => record.Superclass,
        "subclass" => record => record.Subclass,
        "infraclass" => record => record.Infraclass,
        "superorder" => record => record.Superorder,
        "suborder" => record => record.Suborder,
        "infraorder" => record => record.Infraorder,
        "parvorder" => record => record.Parvorder,
        "superfamily" => record => record.Superfamily,
        "subfamily" => record => record.Subfamily,
        "tribe" => record => record.Tribe,
        "subtribe" => record => record.Subtribe,
        "subgenus" => record => record.Subgenus,
        _ => _ => null
    };

    private void AppendTree(StringBuilder builder, TaxonomyTreeNode<IucnSpeciesRecord> node, int startHeading, DisplayPreferences display, string? statusContext, ref int headingCount) {
        foreach (var child in node.Children) {
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            var heading = FormatHeading(child.Value, child.Label, GetKingdomName(child));
            builder.AppendLine($"{headingMarkup} {heading.Text} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.MainLink)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            AppendTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount);
        }

        if (display.GroupSubspecies) {
            AppendItemsWithSubspeciesGrouping(builder, node.Items, display, statusContext);
        } else {
            foreach (var record in node.Items) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
            }
        }
    }

    /// <summary>
    /// Appends species with subspecies grouped under their parent species.
    /// </summary>
    private void AppendItemsWithSubspeciesGrouping(
        StringBuilder builder,
        IReadOnlyList<IucnSpeciesRecord> items,
        DisplayPreferences display,
        string? statusContext) {
        
        // Separate species and subspecies
        var species = new List<IucnSpeciesRecord>();
        var subspeciesGroups = new Dictionary<string, List<IucnSpeciesRecord>>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in items) {
            if (IsSubspecies(record)) {
                var parentKey = GetParentSpeciesKey(record);
                if (!subspeciesGroups.TryGetValue(parentKey, out var list)) {
                    list = new List<IucnSpeciesRecord>();
                    subspeciesGroups[parentKey] = list;
                }
                list.Add(record);
            } else {
                species.Add(record);
            }
        }

        // Output species, inserting subspecies underneath if present
        var processedSubspeciesGroups = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in species) {
            var speciesKey = GetParentSpeciesKey(record);
            builder.AppendLine(FormatSpeciesLine(record, display, statusContext));
            
            // Check if this species has subspecies
            if (subspeciesGroups.TryGetValue(speciesKey, out var subs)) {
                foreach (var sub in subs.OrderBy(s => s.InfraName, StringComparer.OrdinalIgnoreCase)) {
                    builder.AppendLine(FormatSubspeciesLine(sub, display, statusContext));
                }
                processedSubspeciesGroups.Add(speciesKey);
            }
        }

        // Output any subspecies whose parent species isn't in the list
        foreach (var (key, subs) in subspeciesGroups) {
            if (processedSubspeciesGroups.Contains(key)) {
                continue;
            }

            // Create a parent species heading for orphan subspecies
            var firstSub = subs[0];
            var parentName = $"''{firstSub.GenusName} {firstSub.SpeciesName}''";
            builder.AppendLine($"* {parentName}");
            
            foreach (var sub in subs.OrderBy(s => s.InfraName, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(FormatSubspeciesLine(sub, display, statusContext));
            }
        }
    }

    private static bool IsSubspecies(IucnSpeciesRecord record) {
        return !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
    }

    private static string GetParentSpeciesKey(IucnSpeciesRecord record) {
        return $"{record.GenusName?.ToLowerInvariant()}|{record.SpeciesName?.ToLowerInvariant()}";
    }

    private string FormatSubspeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? statusContext) {
        // Indented subspecies line
        var line = FormatSpeciesLine(record, display, statusContext);
        // Add extra indentation (** instead of *)
        if (line.StartsWith("* ")) {
            return "*" + line;
        }
        return line;
    }

    private readonly record struct HeadingInfo(string Text, string? MainLink);

    private HeadingInfo FormatHeading(string? raw, string? rank = null, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return new HeadingInfo("Unassigned", null);
        }

        if (IsOtherOrUnknownHeading(raw)) {
            return new HeadingInfo(raw.Trim(), null);
        }

        // Apply title case to the raw taxon name for display
        var displayName = ToTitleCase(raw);

        // Check new YAML rules first (they take precedence)
        var yamlMainArticle = _taxonRules?.GetMainArticle(raw);
        var yamlRule = _taxonRules?.GetRule(raw);
        
        // YAML rules take precedence for common names
        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonPlural)) {
            var mainLink = yamlMainArticle ?? displayName;
            return new HeadingInfo(Uppercase(yamlRule.CommonPlural)!, mainLink);
        }

        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonName)) {
            var mainLink = yamlMainArticle ?? displayName;
            return new HeadingInfo(Uppercase(yamlRule.CommonName)!, mainLink);
        }

        // Fall back to legacy rules for common names
        var rules = _legacyRules.Get(raw);
        if (!string.IsNullOrWhiteSpace(rules?.CommonPlural)) {
            var mainLink = yamlMainArticle ?? displayName;
            return new HeadingInfo(Uppercase(rules!.CommonPlural)!, mainLink);
        }

        if (!string.IsNullOrWhiteSpace(rules?.CommonName)) {
            var mainLink = yamlMainArticle ?? displayName;
            return new HeadingInfo(Uppercase(rules!.CommonName)!, mainLink);
        }

        // Store-backed common names for higher taxa (if available)
        if (_storeBackedProvider is not null) {
            var storeName = _storeBackedProvider.GetBestCommonNameByScientificName(raw, kingdom);
            if (!string.IsNullOrWhiteSpace(storeName)) {
                var mainLink = yamlMainArticle ?? _storeBackedProvider.GetWikipediaArticleTitleByScientificName(raw, kingdom);
                return new HeadingInfo(Uppercase(storeName)!, mainLink);
            }

            // Fallback: Wikipedia redirect target (e.g., Araneae -> Spider)
            var redirectTitle = _storeBackedProvider.GetWikipediaRedirectTitleByScientificName(raw);
            if (!string.IsNullOrWhiteSpace(redirectTitle) && !redirectTitle.Equals(raw, StringComparison.OrdinalIgnoreCase)) {
                var redirectDisplayName = CommonNameNormalizer.RemoveDisambiguationSuffix(redirectTitle);
                if (!CommonNameNormalizer.LooksLikeScientificName(redirectDisplayName, null, null)) {
                    return new HeadingInfo(Uppercase(redirectDisplayName)!, redirectTitle);
                }
            }
        }

        // Check for wikilink overrides
        if (!string.IsNullOrWhiteSpace(yamlRule?.Wikilink)) {
            return new HeadingInfo(displayName, yamlRule.Wikilink);
        }

        if (!string.IsNullOrWhiteSpace(rules?.Wikilink)) {
            return new HeadingInfo(displayName, rules!.Wikilink);
        }

        // If we have a main article from YAML, use it
        if (!string.IsNullOrWhiteSpace(yamlMainArticle)) {
            return new HeadingInfo(displayName, yamlMainArticle);
        }

        return new HeadingInfo(displayName, null);
    }

    private static bool IsOtherOrUnknownHeading(string raw) {
        var trimmed = raw.Trim();
        return trimmed.StartsWith("Other ", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("Unknown ", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Other", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("Unknown", StringComparison.OrdinalIgnoreCase);
    }

    private static string? GetKingdomName(TaxonomyTreeNode<IucnSpeciesRecord> node) {
        if (node.Items.Count > 0) {
            return node.Items[0].KingdomName;
        }
        foreach (var child in node.Children) {
            var value = GetKingdomName(child);
            if (!string.IsNullOrWhiteSpace(value)) {
                return value;
            }
        }
        return null;
    }

    private static string? GetKingdomName(TaxonomyTreeNode<EnrichedSpeciesRecord> node) {
        if (node.Items.Count > 0) {
            return node.Items[0].KingdomName;
        }
        foreach (var child in node.Children) {
            var value = GetKingdomName(child);
            if (!string.IsNullOrWhiteSpace(value)) {
                return value;
            }
        }
        return null;
    }

    private static string? GetKingdomName(IEnumerable<EnrichedSpeciesRecord> records) {
        foreach (var record in records) {
            if (!string.IsNullOrWhiteSpace(record.KingdomName)) {
                return record.KingdomName;
            }
        }
        return null;
    }

    /// <summary>
    /// Converts a taxonomic name to title case (e.g., "ARTIODACTYLA" → "Artiodactyla").
    /// </summary>
    private static string ToTitleCase(string value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return value;
        }

        return char.ToUpperInvariant(value[0]) + value[1..].ToLowerInvariant();
    }

    private string FormatSpeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? listStatusContext) {
        var descriptor = IucnRedlistStatus.Describe(record.StatusCode);
        var builder = new StringBuilder();
        builder.Append("* ");

        builder.Append(BuildNameFragment(record, display));

        // Add special indicator for PE/PEW if not redundant with list context
        var specialLabel = GetSpecialStatusLabel(record.StatusCode, listStatusContext);
        if (!string.IsNullOrWhiteSpace(specialLabel)) {
            builder.Append(" (");
            builder.Append(specialLabel);
            builder.Append(')');
        }

        // Append subpopulation name if present
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
            builder.Append(" (subpopulation: ");
            builder.Append(record.SubpopulationName);
            builder.Append(')');
        }

        // Add IUCN status template at end: {{IUCN status|XX|taxonId/assessmentId|1|year=YYYY}}
        if (display.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(BuildIucnStatusTemplate(record, descriptor));
        }

        return builder.ToString();
    }

    private static string BuildIucnStatusTemplate(IucnSpeciesRecord record, RedlistStatusDescriptor descriptor) {
        // Build the status code, accounting for PE/PEW flags from database
        var statusCode = GetWikipediaStatusCode(descriptor.Code, record.PossiblyExtinct, record.PossiblyExtinctInTheWild);
        var builder = new StringBuilder();
        builder.Append("{{IUCN status|");
        builder.Append(statusCode);
        builder.Append('|');
        builder.Append(record.TaxonId);
        builder.Append('/');
        builder.Append(record.AssessmentId);
        builder.Append("|1"); // 1 = make link visible

        // Add year for non-extinct statuses
        if (!IsExtinctStatus(descriptor.Code) && !string.IsNullOrWhiteSpace(record.YearPublished)) {
            builder.Append("|year=");
            builder.Append(record.YearPublished);
        }

        builder.Append("}}");
        return builder.ToString();
    }

    /// <summary>
    /// Maps IUCN status codes to Wikipedia template codes.
    /// Uses PE/PEW database flags for CR species to produce CR(PE) or CR(PEW).
    /// Maps legacy LR/* codes to their modern equivalents, except LR/cd which has no exact equivalent.
    /// </summary>
    private static string GetWikipediaStatusCode(string code, string? possiblyExtinct, string? possiblyExtinctInTheWild) {
        var normalized = code.ToUpperInvariant();

        // For CR species, check PE/PEW flags from database
        if (normalized == "CR" || normalized == "CRITICALLY ENDANGERED") {
            if (string.Equals(possiblyExtinct, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PE)";
            }
            if (string.Equals(possiblyExtinctInTheWild, "true", StringComparison.OrdinalIgnoreCase)) {
                return "CR(PEW)";
            }
            return "CR";
        }

        // Map legacy/alternative codes
        // Note: LR/cd is a valid Wikipedia template code, don't map it to NT
        return normalized switch {
            "CR(PE)" or "PE" => "CR(PE)",
            "CR(PEW)" or "PEW" => "CR(PEW)",
            "LR/CD" or "CD" => "LR/cd",
            "LR/NT" => "LR/nt", //"NT",
            "LR/LC" => "LR/lc", //"LC"",
            _ => normalized
        };
    }

    private static bool IsExtinctStatus(string code) => code.ToUpperInvariant() switch {
        "EX" or "EW" => true,
        _ => false
    };

    private static string? GetSpecialStatusLabel(string statusCode, string? listStatusContext) {
        // Don't add redundant labels when the list is specifically for that status
        var code = statusCode.ToUpperInvariant();
        var context = listStatusContext?.ToUpperInvariant() ?? string.Empty;

        // PE/PEW always need indicator except on dedicated PE lists
        if (code is "CR(PE)" or "PE") {
            // If context contains CR(PE) or PE, suppress the label
            if (context.Contains("CR(PE)") || (context.Contains("PE") && !context.Contains("PEW"))) return null;
            return "possibly\u00A0extinct"; // non-breaking space
        }

        if (code is "CR(PEW)" or "PEW") {
            if (context.Contains("CR(PEW)") || context.Contains("PEW")) return null;
            return "possibly extinct in the wild";
        }

        // EW indicator only needed if not on an EW-specific list
        if (code == "EW") {
            if (context.Contains("EW")) return null;
            return "extinct in the wild";
        }

        return null;
    }

    private string BuildNameFragment(IucnSpeciesRecord record, DisplayPreferences display) {
        var commonName = ResolveCommonName(record);
        var scientific = ResolveScientificName(record);
        var rawScientific = scientific; // Keep unformatted version for links
        
        if (display.ItalicizeScientific && !string.IsNullOrWhiteSpace(scientific)) {
            scientific = $"''{scientific}''";
        }

        if (!string.IsNullOrWhiteSpace(commonName) && display.PreferCommonNames) {
            // Try to get Wikipedia article link
            var articleTitle = ResolveWikipediaArticle(record);
            
            if (!string.IsNullOrWhiteSpace(articleTitle)) {
                // We have a Wikipedia article
                // Collapse [[X|X]] to [[X]] when article title matches common name
                var link = string.Equals(articleTitle, commonName, StringComparison.Ordinal)
                    ? $"[[{commonName}]]"
                    : $"[[{articleTitle}|{commonName}]]";
                return $"{link} ({scientific})";
            }
            
            // No Wikipedia article - use scientific name as link target
            // [[Scientific Name|Common Name]] (''Scientific Name'')
            if (!string.IsNullOrWhiteSpace(rawScientific)) {
                return $"[[{rawScientific}|{commonName}]] ({scientific})";
            }
            
            // Fallback: just link the common name
            return $"[[{commonName}]] ({scientific})";
        }

        return scientific ?? record.GenusName;
    }

    /// <summary>
    /// Resolve the Wikipedia article title for a record.
    /// </summary>
    private string? ResolveWikipediaArticle(IucnSpeciesRecord record) {
        // Try store-backed provider first (has Wikipedia source data)
        if (_storeBackedProvider is not null) {
            return _storeBackedProvider.GetWikipediaArticleTitle(record);
        }
        
        return null;
    }

    private string? ResolveCommonName(IucnSpeciesRecord record) {
        // First check legacy rules (highest priority - manual overrides)
        var taxaRules = _legacyRules.Get(record.ScientificNameTaxonomy ?? record.ScientificNameAssessments ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(taxaRules?.CommonName)) {
            return Uppercase(taxaRules!.CommonName);
        }

        // Try the new store-backed provider if available
        if (_storeBackedProvider is not null) {
            return _storeBackedProvider.GetBestCommonName(record);
        }

        // Fall back to legacy provider
        if (_commonNameProvider is null) {
            return null;
        }

        var row = record.ToTaxonomyRow();
        return _commonNameProvider.GetBestCommonName(row, entityIds: null);
    }

    private static string? ResolveScientificName(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.ScientificNameTaxonomy)) {
            return record.ScientificNameTaxonomy;
        }

        if (!string.IsNullOrWhiteSpace(record.ScientificNameAssessments)) {
            return record.ScientificNameAssessments;
        }

        var withRank = ScientificNameHelper.BuildWithRankLabel(record.GenusName, record.SpeciesName, record.InfraType, record.InfraName);
        if (!string.IsNullOrWhiteSpace(withRank)) {
            return withRank;
        }

        return ScientificNameHelper.BuildFromParts(record.GenusName, record.SpeciesName, record.InfraName);
    }

    private static string? Uppercase(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return value;
        }

        return value.Length == 1
            ? value.ToUpperInvariant()
            : char.ToUpperInvariant(value[0]) + value[1..];
    }

    /// <summary>
    /// Check if a record should be excluded based on taxon rules.
    /// </summary>
    private bool ShouldExcludeRecord(IucnSpeciesRecord record, string? listId) {
        if (_taxonRules == null) {
            return false;
        }

        // Build full scientific name for pattern matching
        var scientificName = ResolveScientificName(record);
        
        // Check global exclusion patterns
        if (!string.IsNullOrWhiteSpace(scientificName) && _taxonRules.ShouldExclude(scientificName)) {
            return true;
        }

        // Check list-specific exclusions for higher taxa
        var higherTaxa = new[] {
            record.KingdomName,
            record.PhylumName,
            record.ClassName,
            record.OrderName,
            record.FamilyName,
            record.GenusName
        }.Where(t => !string.IsNullOrWhiteSpace(t));

        foreach (var taxon in higherTaxa) {
            var rule = _taxonRules.GetRule(taxon!, listId);
            if (rule?.Exclude == true) {
                return true;
            }
        }

        return false;
    }

    private sealed class SectionRuntime {
        public SectionRuntime(WikipediaSectionDefinition definition, HashSet<string> statusSet) {
            Definition = definition;
            StatusSet = statusSet;
            // Build context string from status codes for suppressing redundant labels
            StatusContext = string.Join(",", statusSet.Select(s => s.ToUpperInvariant()));
        }

        public WikipediaSectionDefinition Definition { get; }
        public HashSet<string> StatusSet { get; }
        public string StatusContext { get; }
        public List<IucnSpeciesRecord> Records { get; } = new();
    }
}

internal sealed record WikipediaListResult(string OutputPath, int TotalEntries, int HeadingCount, string DatasetVersion);

internal static class IucnSpeciesRecordExtensions {
    public static IucnTaxonomyRow ToTaxonomyRow(this IucnSpeciesRecord record) {
        return new IucnTaxonomyRow(
            record.AssessmentId,
            record.TaxonId,
            record.ScientificNameAssessments,
            record.ScientificNameTaxonomy,
            record.KingdomName,
            record.PhylumName,
            record.ClassName,
            record.OrderName,
            record.FamilyName,
            record.GenusName,
            record.SpeciesName,
            record.InfraType,
            record.InfraName,
            record.SubpopulationName,
            record.Authority,
            record.InfraAuthority
        );
    }
}
