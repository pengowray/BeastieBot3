using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using static BeastieBot3.WikipediaLists.RecordClassification;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.HeadingFormatter;
using static BeastieBot3.WikipediaLists.TaxonGroupingHelper;
using static BeastieBot3.WikipediaLists.SpeciesLineFormatter;

namespace BeastieBot3.WikipediaLists;

// The taxonomy-tree rendering engine: turns a section's records into wikitext, choosing among the
// flat / custom-group / COL-enriched / virtual-group / infraspecific-partition paths, building the
// TaxonomyTreeBuilder tree, and walking it to emit headings + species lines. Extracted from
// WikipediaListGenerator (R2 carve-up) so the generator is a thin orchestrator. Holds the COL enricher
// and taxon rules it needs, plus the line + heading formatters it delegates leaf/heading rendering to.
internal sealed class SectionBodyRenderer {
    private readonly ColTaxonomyEnricher? _colEnricher;
    private readonly TaxonRulesService? _taxonRules;
    private readonly SpeciesLineFormatter _lineFormatter;
    private readonly HeadingFormatter _headingFormatter;

    // Minimum bullet count before a section is wrapped in {{div col}} columns. Two- to four-item
    // families read better as a plain bulleted list than as a sparse multi-column block.
    private const int DivColMinItems = 5;

    public SectionBodyRenderer(
        ColTaxonomyEnricher? colEnricher,
        TaxonRulesService? taxonRules,
        SpeciesLineFormatter lineFormatter,
        HeadingFormatter headingFormatter) {
        _colEnricher = colEnricher;
        _taxonRules = taxonRules;
        _lineFormatter = lineFormatter ?? throw new ArgumentNullException(nameof(lineFormatter));
        _headingFormatter = headingFormatter ?? throw new ArgumentNullException(nameof(headingFormatter));
    }

    public (string Body, int HeadingCount) BuildSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups = null,
        int startHeading = 3,
        AutoSplitConfig? autoSplit = null) {

        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        // Filter out regional assessments if requested
        // Regional assessments are excluded from main lists to keep output global-only
        var filteredRecords = records.Where(r => !IsRegionalAssessment(r)).ToList();

        if (filteredRecords.Count == 0) {
            return ("''No taxa currently listable (all filtered as regional assessments).''", 0);
        }

        var infraspecificMode = ResolveInfraspecificMode(display);

        // If separating infraspecific sections is enabled, partition and render each section
        if (infraspecificMode == InfraspecificDisplayMode.SeparateSections && display.SeparateInfraspecificSections) {
            return BuildInfraspecificSections(filteredRecords, grouping, display, statusContext, customGroups, startHeading, autoSplit);
        }

        // If custom groups are defined, use custom grouping instead of taxonomic grouping
        if (customGroups != null && customGroups.Count > 0) {
            return BuildCustomGroupedSectionBody(filteredRecords, customGroups, grouping, display, statusContext, startHeading);
        }

        if (grouping.Count == 0) {
            return (BuildFlatListBody(filteredRecords, display, statusContext), 0);
        }

        // Check if we need COL enrichment:
        // 1. Any grouping level uses COL-specific ranks
        // 2. Any taxon uses virtual groups (which rely on COL superfamily/family)
        // 3. Auto-split is enabled (needs COL intermediate ranks as candidates)
        var needsEnrichment = _colEnricher != null &&
            (grouping.Any(g => IsColEnrichedRank(g.Level)) || HasVirtualGroupsInGrouping(grouping)
             || (autoSplit != null && autoSplit.Enabled));

        if (needsEnrichment) {
            return BuildEnrichedSectionBody(filteredRecords, grouping, display, statusContext, startHeading, autoSplit);
        }

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<IucnSpeciesRecord>(
                level.Label ?? level.Level,
                BuildSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel,
                level.MinItems,
                level.OtherLabel,
                level.MinGroupsForOther))
            .ToList();

        // Build auto-split options for non-enriched path (limited to genus)
        var autoSplitOptions = BuildAutoSplitOptionsIucn(autoSplit, grouping);

        Func<string, bool>? shouldSkip = _taxonRules != null
            ? taxon => _taxonRules.ShouldForceSplit(taxon)
            : null;
        var tree = TaxonomyTreeBuilder.Build(filteredRecords, levels, shouldSkip, autoSplitOptions);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, tree, startHeading, display, statusContext, ref headingCount, grouping, groupingIndex: 0, otherContext: null, parentTaxon: null);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    /// <summary>
    /// Build section body with infraspecific taxa (subspecies, varieties, populations)
    /// rendered within each taxonomy heading rather than as separate global sections.
    /// Delegates to the normal taxonomy tree path with a flag that triggers per-node partitioning.
    /// </summary>
    private (string Body, int HeadingCount) BuildInfraspecificSections(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups,
        int startHeading,
        AutoSplitConfig? autoSplit = null) {

        // Create a display settings copy that signals per-node infraspecific partitioning
        // SeparateInfraspecificSections = false prevents re-entering this method,
        // while InfraspecificDisplayMode stays SeparateSections so AppendTree knows
        // to partition items within each leaf node.
        var innerDisplay = new DisplayPreferences {
            PreferCommonNames = display.PreferCommonNames,
            ItalicizeScientific = display.ItalicizeScientific,
            IncludeStatusTemplate = display.IncludeStatusTemplate,
            IncludeStatusLabel = display.IncludeStatusLabel,
            GroupSubspecies = false,
            ListingStyle = display.ListingStyle,
            InfraspecificDisplayMode = InfraspecificDisplayMode.SeparateSections,
            SeparateInfraspecificSections = false,  // Prevent recursion back here
            ExcludeRegionalAssessments = false,     // Already filtered above
            IncludeFamilyInOtherBucket = display.IncludeFamilyInOtherBucket
        };

        // Pass ALL records (species + infraspecific) through the normal tree path
        return BuildSectionBodyCore(records, grouping, innerDisplay, statusContext, customGroups, startHeading, autoSplit);
    }

    /// <summary>
    /// Core section body building logic (without infraspecific section separation).
    /// </summary>
    private (string Body, int HeadingCount) BuildSectionBodyCore(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups = null,
        int startHeading = 3,
        AutoSplitConfig? autoSplit = null) {

        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        // If custom groups are defined, use custom grouping instead of taxonomic grouping
        if (customGroups != null && customGroups.Count > 0) {
            return BuildCustomGroupedSectionBody(records, customGroups, grouping, display, statusContext, startHeading);
        }

        if (grouping.Count == 0) {
            return (BuildFlatListBody(records, display, statusContext), 0);
        }

        // Check if we need COL enrichment:
        // 1. Any grouping level uses COL-specific ranks
        // 2. Any taxon uses virtual groups (which rely on COL superfamily/family)
        // 3. Auto-split is enabled (needs COL intermediate ranks as candidates)
        var needsEnrichment = _colEnricher != null &&
            (grouping.Any(g => IsColEnrichedRank(g.Level)) || HasVirtualGroupsInGrouping(grouping)
             || (autoSplit != null && autoSplit.Enabled));

        if (needsEnrichment) {
            return BuildEnrichedSectionBody(records, grouping, display, statusContext, startHeading, autoSplit);
        }

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<IucnSpeciesRecord>(
                level.Label ?? level.Level,
                BuildSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel,
                level.MinItems,
                level.OtherLabel,
                level.MinGroupsForOther))
            .ToList();

        // Build auto-split options for non-enriched path (limited to genus)
        var autoSplitOptions = BuildAutoSplitOptionsIucn(autoSplit, grouping);

        Func<string, bool>? shouldSkip = _taxonRules != null
            ? taxon => _taxonRules.ShouldForceSplit(taxon)
            : null;
        var tree = TaxonomyTreeBuilder.Build(records, levels, shouldSkip, autoSplitOptions);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, tree, startHeading, display, statusContext, ref headingCount, grouping, groupingIndex: 0, otherContext: null, parentTaxon: null);
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
                builder.AppendLine(BuildFlatListBody(groupRecords, display, statusContext));
            }
            builder.AppendLine();
        }

        // Handle any unmatched records
        if (unmatchedRecords != null && unmatchedRecords.Count > 0) {
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            builder.AppendLine($"{headingMarkup} Other {headingMarkup}");
            headingCount++;
            builder.AppendLine(BuildFlatListBody(unmatchedRecords, display, statusContext));
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
        string? statusContext,
        int startHeading = 3,
        AutoSplitConfig? autoSplit = null) {

        // Enrich records with COL taxonomy
        var enrichedRecords = _colEnricher!.Enrich(records, CancellationToken.None);

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<EnrichedSpeciesRecord>(
                level.Label ?? level.Level,
                BuildEnrichedSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel,
                level.MinItems,
                level.OtherLabel,
                level.MinGroupsForOther))
            .ToList();

        // Build auto-split options with COL intermediate rank candidates
        var autoSplitOptions = BuildAutoSplitOptionsEnriched(autoSplit, grouping);

        Func<string, bool>? shouldSkip = _taxonRules != null
            ? taxon => _taxonRules.ShouldForceSplit(taxon)
            : null;
        var tree = TaxonomyTreeBuilder.Build(enrichedRecords, levels, shouldSkip, autoSplitOptions);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendEnrichedTree(builder, tree, startHeading, display, statusContext, ref headingCount, grouping, groupingIndex: 0, otherContext: null, parentTaxon: null);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    private void AppendEnrichedTree(
        StringBuilder builder,
        TaxonomyTreeNode<EnrichedSpeciesRecord> node,
        int startHeading,
        DisplayPreferences display,
        string? statusContext,
        ref int headingCount) {
        AppendEnrichedTree(builder, node, startHeading, display, statusContext, ref headingCount, grouping: null, groupingIndex: 0, otherContext: null, parentTaxon: null);
    }

    private void AppendEnrichedTree(
        StringBuilder builder, 
        TaxonomyTreeNode<EnrichedSpeciesRecord> node, 
        int startHeading, 
        DisplayPreferences display, 
        string? statusContext, 
        ref int headingCount,
        IReadOnlyList<GroupingLevelDefinition>? grouping,
        int groupingIndex,
        OtherBucketContext? otherContext,
        string? parentTaxon) {
        
        foreach (var child in node.Children) {
            // Rule 7: Skip empty headings (no items and no children)
            if (child.ItemCount == 0) {
                continue;
            }

            var taxonName = child.Value;
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);

            // Get grouping configuration for current level
            var currentGrouping = grouping != null && groupingIndex < grouping.Count
                ? grouping[groupingIndex]
                : null;
            var heading = _headingFormatter.FormatHeading(taxonName, child.Label, GetKingdomName(child));
            var headingText = heading.Text;
            if (IsOtherOrUnknownHeading(taxonName ?? string.Empty) &&
                currentGrouping?.Level.Equals("family", StringComparison.OrdinalIgnoreCase) == true &&
                !string.IsNullOrWhiteSpace(parentTaxon)) {
                headingText = $"Other {ToTitleCase(parentTaxon)}";
            }

            builder.AppendLine($"{headingMarkup} {headingText} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.CommonNameSentence)) {
                builder.AppendLine(heading.CommonNameSentence);
            } else if (!string.IsNullOrWhiteSpace(heading.MainLink) && !IsOtherOrUnknownHeading(headingText)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            if (!string.IsNullOrWhiteSpace(heading.Description)) {
                builder.AppendLine(heading.Description);
            }

            // Detect if this is an "Other" bucket
            var isOtherBucket = IsOtherOrUnknownHeading(taxonName ?? "");
            var childOtherContext = isOtherBucket && display.IncludeFamilyInOtherBucket
                ? BuildEnrichedOtherContext(child)
                : otherContext;

            // Check if this taxon uses virtual groups
            if (!string.IsNullOrWhiteSpace(taxonName) &&
                _taxonRules != null &&
                _taxonRules.ShouldUseVirtualGroups(taxonName) &&
                _taxonRules.HasVirtualGroups(taxonName)) {
                // Render virtual groups instead of normal children
                AppendVirtualGroups(builder, child, taxonName, headingLevel + 1, display, statusContext, ref headingCount, grouping, groupingIndex + 1, childOtherContext);
            } else {
                AppendEnrichedTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount, grouping, groupingIndex + 1, childOtherContext, parentTaxon: taxonName);
            }
        }

        // Convert enriched records to IUCN records for output
        var iucnRecords = node.Items.Select(r => r.ToIucnRecord()).ToList();
        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, iucnRecords, display, statusContext, otherContext);
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, iucnRecords, display, statusContext, otherContext);
        } else {
            if (iucnRecords.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (iucnRecords.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
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
        ref int headingCount,
        IReadOnlyList<GroupingLevelDefinition>? grouping,
        int groupingIndex,
        OtherBucketContext? otherContext) {
        
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
                        var familyHeading = _headingFormatter.FormatHeading(familyGroup.Key, "family", GetKingdomName(familyGroup));
                        builder.AppendLine($"{familyHeadingMarkup} {familyHeading.Text} {familyHeadingMarkup}");
                        headingCount++;
                        if (!string.IsNullOrWhiteSpace(familyHeading.CommonNameSentence)) {
                            builder.AppendLine(familyHeading.CommonNameSentence);
                        } else if (!string.IsNullOrWhiteSpace(familyHeading.MainLink)) {
                            builder.AppendLine($"{{{{main|{familyHeading.MainLink}}}}}");
                        }

                        OutputEnrichedRecords(builder, familyGroup.ToList(), display, statusContext, otherContext);
                    }
                } else {
                    // Single family, no extra heading needed
                    OutputEnrichedRecords(builder, records, display, statusContext, otherContext);
                }
            }
        }

        // Handle any remaining unmatched records (shouldn't happen if default group is defined)
        if (unmatchedRecords != null && unmatchedRecords.Count > 0) {
            var headingMarkup = new string('=', Math.Min(headingLevel, 6));
            builder.AppendLine($"{headingMarkup} Other {headingMarkup}");
            headingCount++;
            // Create an Other context for these unmatched records
            var unmatchedOtherContext = display.IncludeFamilyInOtherBucket 
                ? new OtherBucketContext(true) 
                : otherContext;
            OutputEnrichedRecords(builder, unmatchedRecords, display, statusContext, unmatchedOtherContext);
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

    private static void CollectRecordsRecursive<T>(TaxonomyTreeNode<T> node, List<T> result) {
        result.AddRange(node.Items);
        foreach (var child in node.Children) {
            CollectRecordsRecursive(child, result);
        }
    }

    /// <summary>
    /// Builds an OtherBucketContext for an enriched "Other" node, capturing the rank label
    /// and each record's value for that rank (e.g., Subfamily name) so parenthetical
    /// annotations show the correct rank instead of always "Family".
    /// </summary>
    private static OtherBucketContext BuildEnrichedOtherContext(TaxonomyTreeNode<EnrichedSpeciesRecord> node) {
        var rankLabel = node.Label ?? "Family";
        var selector = BuildEnrichedSelector(rankLabel.ToLowerInvariant());
        var map = new Dictionary<long, string>();
        foreach (var record in CollectAllEnrichedRecords(node)) {
            var value = selector(record);
            if (!string.IsNullOrWhiteSpace(value)) {
                map[record.TaxonId] = value;
            }
        }
        return new OtherBucketContext(true, rankLabel, map);
    }

    /// <summary>
    /// Builds an OtherBucketContext for an IUCN-only "Other" node.
    /// </summary>
    private static OtherBucketContext BuildIucnOtherContext(TaxonomyTreeNode<IucnSpeciesRecord> node) {
        var rankLabel = node.Label ?? "Family";
        var selector = BuildSelector(rankLabel.ToLowerInvariant());
        var map = new Dictionary<long, string>();
        var records = new List<IucnSpeciesRecord>();
        CollectRecordsRecursive(node, records);
        foreach (var record in records) {
            var value = selector(record);
            if (!string.IsNullOrWhiteSpace(value)) {
                map[record.TaxonId] = value;
            }
        }
        return new OtherBucketContext(true, rankLabel, map);
    }

    /// <summary>
    /// Output enriched records (converted to IUCN records for compatibility).
    /// </summary>
    private void OutputEnrichedRecords(
        StringBuilder builder,
        IReadOnlyList<EnrichedSpeciesRecord> records,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext = null) {
        
        var iucnRecords = records.Select(r => r.ToIucnRecord()).ToList();
        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, iucnRecords, display, statusContext, otherContext);
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, iucnRecords, display, statusContext, otherContext);
        } else {
            if (iucnRecords.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (iucnRecords.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }
    }



    private void AppendTree(StringBuilder builder, TaxonomyTreeNode<IucnSpeciesRecord> node, int startHeading, DisplayPreferences display, string? statusContext, ref int headingCount) {
        AppendTree(builder, node, startHeading, display, statusContext, ref headingCount, grouping: null, groupingIndex: 0, otherContext: null, parentTaxon: null);
    }

    private void AppendTree(
        StringBuilder builder, 
        TaxonomyTreeNode<IucnSpeciesRecord> node, 
        int startHeading, 
        DisplayPreferences display, 
        string? statusContext, 
        ref int headingCount,
        IReadOnlyList<GroupingLevelDefinition>? grouping,
        int groupingIndex,
        OtherBucketContext? otherContext,
        string? parentTaxon) {
        
        foreach (var child in node.Children) {
            // Rule 7: Skip empty headings (no items and no children)
            if (child.ItemCount == 0) {
                continue;
            }

            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);

            // Get grouping configuration for current level
            var currentGrouping = grouping != null && groupingIndex < grouping.Count
                ? grouping[groupingIndex]
                : null;
            var heading = _headingFormatter.FormatHeading(child.Value, child.Label, GetKingdomName(child));
            var headingText = heading.Text;
            if (IsOtherOrUnknownHeading(child.Value ?? string.Empty) &&
                currentGrouping?.Level.Equals("family", StringComparison.OrdinalIgnoreCase) == true &&
                !string.IsNullOrWhiteSpace(parentTaxon)) {
                headingText = $"Other {ToTitleCase(parentTaxon)}";
            }

            builder.AppendLine($"{headingMarkup} {headingText} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.CommonNameSentence)) {
                builder.AppendLine(heading.CommonNameSentence);
            } else if (!string.IsNullOrWhiteSpace(heading.MainLink) && !IsOtherOrUnknownHeading(headingText)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            if (!string.IsNullOrWhiteSpace(heading.Description)) {
                builder.AppendLine(heading.Description);
            }

            // Detect if this is an "Other" bucket
            var isOtherBucket = IsOtherOrUnknownHeading(child.Value ?? "");
            var childOtherContext = isOtherBucket && display.IncludeFamilyInOtherBucket
                ? BuildIucnOtherContext(child)
                : otherContext;

            AppendTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount, grouping, groupingIndex + 1, childOtherContext, parentTaxon: child.Value);
        }

        if (node.Items.Count == 0) {
            return;
        }

        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, node.Items, display, statusContext, otherContext);
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, node.Items, display, statusContext, otherContext);
        } else {
            if (node.Items.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(node.Items, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (node.Items.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }
    }

    /// <summary>
    /// Partitions items within a single taxonomy node into species, subspecies, varieties,
    /// and populations. Each partition gets its own {{div col}} wrapper and bold sub-heading.
    /// This produces the per-family subspecies grouping rather than one global section.
    /// </summary>
    private void AppendPartitionedItems(
        StringBuilder builder,
        IReadOnlyList<IucnSpeciesRecord> items,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext) {

        var species = new List<IucnSpeciesRecord>();
        var subspecies = new List<IucnSpeciesRecord>();
        var varieties = new List<IucnSpeciesRecord>();
        var populations = new List<IucnSpeciesRecord>();

        foreach (var record in items) {
            if (IsRegionalAssessment(record)) {
                populations.Add(record);
                continue;
            }

            var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? "";
            if (!string.IsNullOrWhiteSpace(infraType) && !string.IsNullOrWhiteSpace(record.InfraName)) {
                if (infraType.Contains("var")) {
                    varieties.Add(record);
                } else if (infraType.Contains("ssp") || infraType.Contains("subsp")) {
                    subspecies.Add(record);
                } else {
                    subspecies.Add(record);
                }
                continue;
            }

            species.Add(record);
        }

        var hasInfraspecific = subspecies.Count > 0 || varieties.Count > 0 || populations.Count > 0;

        // Species items
        if (species.Count > 0) {
            if (species.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(species, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (species.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }

        // Subspecies
        if (subspecies.Count > 0) {
            if (species.Count > 0) {
                builder.AppendLine();
            }
            builder.AppendLine("'''Subspecies'''");
            builder.AppendLine();
            if (subspecies.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(subspecies, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (subspecies.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }

        // Varieties
        if (varieties.Count > 0) {
            builder.AppendLine();
            builder.AppendLine("'''Varieties'''");
            builder.AppendLine();
            if (varieties.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(varieties, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (varieties.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }

        // Stocks and populations
        if (populations.Count > 0) {
            builder.AppendLine();
            builder.AppendLine("'''Stocks and populations'''");
            builder.AppendLine();
            if (populations.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(populations, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (populations.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }

        // If only infraspecific taxa exist (no species), still render them
        if (species.Count == 0 && !hasInfraspecific) {
            // Shouldn't happen since we checked node.Items.Count > 0 above,
            // but guard anyway
        }
    }

    private string BuildFlatListBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext = null) {
        var builder = new StringBuilder();
        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, records, display, statusContext, otherContext);
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, records, display, statusContext, otherContext);
        } else {
            if (records.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(records, display, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (records.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
        }
        return builder.ToString().TrimEnd();
    }

    /// <summary>
    /// Appends species with subspecies/varieties/populations grouped under their parent species.
    /// Uses abbreviated genus for infraspecific sub-bullets.
    /// </summary>
    private void AppendItemsWithInfraspecificGrouping(
        StringBuilder builder,
        IReadOnlyList<IucnSpeciesRecord> items,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext = null) {
        var species = new List<IucnSpeciesRecord>();
        var subspeciesGroups = new Dictionary<string, List<IucnSpeciesRecord>>(StringComparer.OrdinalIgnoreCase);
        var varietyGroups = new Dictionary<string, List<IucnSpeciesRecord>>(StringComparer.OrdinalIgnoreCase);
        var populationGroups = new Dictionary<string, List<IucnSpeciesRecord>>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in items) {
            var parentKey = GetParentSpeciesKey(record);
            if (IsRegionalAssessment(record)) {
                if (!populationGroups.TryGetValue(parentKey, out var list)) {
                    list = new List<IucnSpeciesRecord>();
                    populationGroups[parentKey] = list;
                }
                list.Add(record);
                continue;
            }

            if (IsVariety(record)) {
                if (!varietyGroups.TryGetValue(parentKey, out var list)) {
                    list = new List<IucnSpeciesRecord>();
                    varietyGroups[parentKey] = list;
                }
                list.Add(record);
                continue;
            }

            if (IsSubspecies(record) || IsInfraspecific(record)) {
                if (!subspeciesGroups.TryGetValue(parentKey, out var list)) {
                    list = new List<IucnSpeciesRecord>();
                    subspeciesGroups[parentKey] = list;
                }
                list.Add(record);
                continue;
            }

            species.Add(record);
        }

        if (items.Count >= DivColMinItems) builder.AppendLine("{{div col|colwidth=30em}}");

        var processedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in OrderRecordsForOutput(species, display, otherContext)) {
            var speciesKey = GetParentSpeciesKey(record);
            builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            AppendInfraspecificSubitems(builder, speciesKey, subspeciesGroups, varietyGroups, populationGroups, display, statusContext, otherContext);
            processedKeys.Add(speciesKey);
        }

        var orphanKeys = subspeciesGroups.Keys
            .Concat(varietyGroups.Keys)
            .Concat(populationGroups.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(key => !processedKeys.Contains(key))
            .OrderBy(key => GetRepresentativeFamilyName(key, subspeciesGroups, varietyGroups, populationGroups), StringComparer.OrdinalIgnoreCase)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var key in orphanKeys) {
            var parentHeading = BuildParentSpeciesHeadingLine(key);
            builder.AppendLine(parentHeading);
            AppendInfraspecificSubitems(builder, key, subspeciesGroups, varietyGroups, populationGroups, display, statusContext, otherContext);
        }

        if (items.Count >= DivColMinItems) builder.AppendLine("{{div col end}}");
    }

    private static string GetRepresentativeFamilyName(
        string speciesKey,
        Dictionary<string, List<IucnSpeciesRecord>> subspeciesGroups,
        Dictionary<string, List<IucnSpeciesRecord>> varietyGroups,
        Dictionary<string, List<IucnSpeciesRecord>> populationGroups) {
        if (subspeciesGroups.TryGetValue(speciesKey, out var subspecies) && subspecies.Count > 0) {
            return subspecies[0].FamilyName ?? string.Empty;
        }

        if (varietyGroups.TryGetValue(speciesKey, out var varieties) && varieties.Count > 0) {
            return varieties[0].FamilyName ?? string.Empty;
        }

        if (populationGroups.TryGetValue(speciesKey, out var populations) && populations.Count > 0) {
            return populations[0].FamilyName ?? string.Empty;
        }

        return string.Empty;
    }

    private void AppendInfraspecificSubitems(
        StringBuilder builder,
        string speciesKey,
        Dictionary<string, List<IucnSpeciesRecord>> subspeciesGroups,
        Dictionary<string, List<IucnSpeciesRecord>> varietyGroups,
        Dictionary<string, List<IucnSpeciesRecord>> populationGroups,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext) {
        if (subspeciesGroups.TryGetValue(speciesKey, out var subspecies)) {
            foreach (var sub in subspecies.OrderBy(s => ResolveScientificName(s) ?? string.Empty, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(IndentSubBullet(_lineFormatter.FormatInfraspecificLine(sub, display, statusContext, otherContext)));
            }
        }

        if (varietyGroups.TryGetValue(speciesKey, out var varieties)) {
            foreach (var variety in varieties.OrderBy(s => ResolveScientificName(s) ?? string.Empty, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(IndentSubBullet(_lineFormatter.FormatInfraspecificLine(variety, display, statusContext, otherContext)));
            }
        }

        if (populationGroups.TryGetValue(speciesKey, out var populations)) {
            foreach (var population in populations.OrderBy(s => ResolveScientificName(s) ?? string.Empty, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(IndentSubBullet(_lineFormatter.FormatSpeciesLine(population, display, statusContext, otherContext)));
            }
        }
    }

    private static string IndentSubBullet(string line) {
        return line.StartsWith("* ", StringComparison.Ordinal) ? "*" + line : line;
    }

    private static string BuildParentSpeciesHeadingLine(string speciesKey) {
        var parts = speciesKey.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 2) {
            return "* ''[[Unknown]]''";
        }

        var genus = ToTitleCase(parts[0]);
        var species = parts[1].ToLowerInvariant();
        return $"* ''[[{genus} {species}]]''";
    }

    private static InfraspecificDisplayMode ResolveInfraspecificMode(DisplayPreferences display) {
        if (display.InfraspecificDisplayMode != InfraspecificDisplayMode.SeparateSections) {
            return display.InfraspecificDisplayMode;
        }

        if (!display.SeparateInfraspecificSections && display.GroupSubspecies) {
            return InfraspecificDisplayMode.GroupedUnderSpecies;
        }

        return InfraspecificDisplayMode.SeparateSections;
    }

    private IEnumerable<IucnSpeciesRecord> OrderRecordsForOutput(
        IEnumerable<IucnSpeciesRecord> records,
        DisplayPreferences display,
        OtherBucketContext? otherContext) {
        // Common-name-focused lists (Style B/C) are sorted by the visible vernacular label so the
        // displayed order is alphabetical to the reader; scientific-focus lists (Style A) keep the
        // incoming scientific-name order. The sort key is materialised once per record so the common
        // name is resolved only once even though OrderBy compares it repeatedly.
        var sortByCommonName = display.ListingStyle is ListingStyle.CommonNameOnly or ListingStyle.CommonNameFocus;

        if (otherContext is { IsInOtherBucket: true }) {
            return records
                .Select(r => (record: r, rank: otherContext.GetRankValue(r) ?? string.Empty, label: SortLabel(r, sortByCommonName)))
                .OrderBy(t => t.rank, StringComparer.OrdinalIgnoreCase)
                .ThenBy(t => t.label, StringComparer.OrdinalIgnoreCase)
                .Select(t => t.record);
        }

        if (sortByCommonName) {
            return records
                .Select(r => (record: r, label: SortLabel(r, preferCommonName: true)))
                .OrderBy(t => t.label, StringComparer.OrdinalIgnoreCase)
                .Select(t => t.record);
        }

        return records;
    }

    // The label a record sorts under: the displayed vernacular name when requested and available,
    // otherwise the scientific name (which is what the bullet falls back to showing).
    private string SortLabel(IucnSpeciesRecord record, bool preferCommonName) {
        if (preferCommonName) {
            var common = _lineFormatter.ResolveDisplayCommonName(record);
            if (!string.IsNullOrWhiteSpace(common)) {
                return common;
            }
        }
        return ResolveScientificName(record) ?? string.Empty;
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
}
