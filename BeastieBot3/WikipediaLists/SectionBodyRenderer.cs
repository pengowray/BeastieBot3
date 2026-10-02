using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using static BeastieBot3.WikipediaLists.RecordClassification;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.HeadingFormatter;
using static BeastieBot3.WikipediaLists.TaxonGroupingHelper;
using static BeastieBot3.WikipediaLists.SpeciesLineFormatter;

namespace BeastieBot3.WikipediaLists;

/// <summary>
/// Settings for the heading tree of one section: auto-split, intermediate layers, and where to
/// record their decisions.
/// </summary>
internal sealed record SectionTreeOptions {
    public AutoSplitConfig? AutoSplit { get; init; }
    public IntermediateGroupsConfig? IntermediateGroups { get; init; }
    public IAutoSplitDiagnostics? Diagnostics { get; init; }

    /// <summary>Start of every path in the diagnostics, e.g. the status section heading.</summary>
    public string? SectionLabel { get; init; }
}

// The taxonomy-tree rendering engine: turns a section's records into wikitext. Custom groups (marine
// mammals) and flat lists have their own small paths; everything else goes through one tree:
// TaxonomyTreeBuilder groups the records (configured levels, Catalogue of Life layers from the
// placement, curated virtual groups, auto-split), then AppendTree writes headings and species lines.
// Extracted from WikipediaListGenerator (R2 carve-up) so the generator is a thin orchestrator.
internal sealed class SectionBodyRenderer {
    private readonly ITaxonPlacement? _placement;
    private readonly TaxonRulesService? _taxonRules;
    private readonly SpeciesLineFormatter _lineFormatter;
    private readonly HeadingFormatter _headingFormatter;

    // Minimum bullet count before a section is wrapped in {{div col}} columns. Two- to four-item
    // families read better as a plain bulleted list than as a sparse multi-column block.
    private const int DivColMinItems = 5;

    // MediaWiki's deepest heading level.
    private const int DeepestHeading = 6;

    public SectionBodyRenderer(
        ITaxonPlacement? placement,
        TaxonRulesService? taxonRules,
        SpeciesLineFormatter lineFormatter,
        HeadingFormatter headingFormatter) {
        _placement = placement;
        _taxonRules = taxonRules;
        _lineFormatter = lineFormatter ?? throw new ArgumentNullException(nameof(lineFormatter));
        _headingFormatter = headingFormatter ?? throw new ArgumentNullException(nameof(headingFormatter));
    }

    /// <summary>The number of heading levels from <paramref name="startHeading"/> down to H6.</summary>
    public static int HeadingLevelsFrom(int startHeading) => Math.Max(0, DeepestHeading - startHeading + 1);

    public (string Body, int HeadingCount) BuildSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups = null,
        int startHeading = 3,
        SectionTreeOptions? tree = null) {

        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        // Filter out regional assessments unless the list opts to keep them (e.g. the SPRAT Australia
        // lists, which surface EPBC population listings under a "Populations" sub-section). IUCN lists
        // default to ExcludeRegionalAssessments=true, so their output is unchanged.
        var filteredRecords = display.ExcludeRegionalAssessments
            ? records.Where(r => !IsRegionalAssessment(r)).ToList()
            : records.ToList();

        if (filteredRecords.Count == 0) {
            return ("''No taxa currently listable (all filtered as regional assessments).''", 0);
        }

        return RenderBody(filteredRecords, grouping, PerNodeDisplay(display), statusContext, customGroups, startHeading, tree ?? new SectionTreeOptions());
    }

    /// <summary>
    /// With separate infraspecific sections, subspecies, varieties and populations are not split off
    /// into sections of their own: every record goes through the tree, and each node's items are
    /// partitioned under bold sub-labels. The returned copy says so to the item renderer.
    /// </summary>
    private static DisplayPreferences PerNodeDisplay(DisplayPreferences display) {
        if (ResolveInfraspecificMode(display) != InfraspecificDisplayMode.SeparateSections || !display.SeparateInfraspecificSections) {
            return display;
        }

        return new DisplayPreferences {
            PreferCommonNames = display.PreferCommonNames,
            ItalicizeScientific = display.ItalicizeScientific,
            IncludeStatusTemplate = display.IncludeStatusTemplate,
            IncludeStatusLabel = display.IncludeStatusLabel,
            GroupSubspecies = false,
            ListingStyle = display.ListingStyle,
            InfraspecificDisplayMode = InfraspecificDisplayMode.SeparateSections,
            SeparateInfraspecificSections = false,
            ExcludeRegionalAssessments = false,     // Already filtered
            IncludeFamilyInOtherBucket = display.IncludeFamilyInOtherBucket
        };
    }

    private (string Body, int HeadingCount) RenderBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups,
        int startHeading,
        SectionTreeOptions tree) {

        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        if (customGroups != null && customGroups.Count > 0) {
            return BuildCustomGroupedSectionBody(records, customGroups, grouping, display, statusContext, startHeading, tree);
        }

        if (grouping.Count == 0) {
            return (BuildFlatListBody(records, display, statusContext), 0);
        }

        var options = new TaxonomyTreeOptions<IucnSpeciesRecord> {
            ShouldSkipGroup = _taxonRules != null ? taxon => _taxonRules.ShouldForceSplit(taxon) : null,
            AutoSplit = BuildAutoSplitOptions(tree.AutoSplit, grouping, _placement),
            Intermediate = _placement != null ? BuildIntermediateOptions(tree.IntermediateGroups) : null,
            VirtualGroups = BuildVirtualGroupOptions(_taxonRules),
            HeadingLevels = HeadingLevelsFrom(startHeading),
            Diagnostics = tree.Diagnostics,
            RootLabel = tree.SectionLabel,
        };
        var root = TaxonomyTreeBuilder.Build(records, BuildLevels(grouping, _placement), options);

        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, root, startHeading, display, statusContext, ref headingCount, otherContext: null);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    /// <summary>
    /// Build section body using custom family-based groups instead of taxonomic hierarchy.
    /// Used for paraphyletic groups like marine mammals. Within each group the remaining grouping
    /// levels go through the normal tree, without auto-split or intermediate layers.
    /// </summary>
    private (string Body, int HeadingCount) BuildCustomGroupedSectionBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<CustomGroupDefinition> customGroups,
        IReadOnlyList<GroupingLevelDefinition> subGrouping,
        DisplayPreferences display,
        string? statusContext,
        int startHeading,
        SectionTreeOptions tree) {

        var builder = new StringBuilder();
        var headingCount = 0;

        // Group records by custom group based on family
        var groupedRecords = new Dictionary<CustomGroupDefinition, List<IucnSpeciesRecord>>();
        CustomGroupDefinition? defaultGroup = null;
        List<IucnSpeciesRecord>? unmatchedRecords = null;

        foreach (var group in customGroups) {
            groupedRecords[group] = new List<IucnSpeciesRecord>();
            if (group.Default) {
                defaultGroup = group;
            }
        }

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
        var innerTree = tree with { AutoSplit = null, IntermediateGroups = null };

        var headingLevel = Math.Min(startHeading, DeepestHeading);
        var headingMarkup = new string('=', headingLevel);
        foreach (var group in customGroups) {
            var groupRecords = groupedRecords[group];
            if (groupRecords.Count == 0) {
                continue;
            }

            var displayName = !string.IsNullOrWhiteSpace(group.CommonPlural)
                ? Uppercase(group.CommonPlural)!
                : group.Name;
            builder.AppendLine($"{headingMarkup} {displayName} {headingMarkup}");
            headingCount++;

            if (!string.IsNullOrWhiteSpace(group.MainArticle)) {
                builder.AppendLine($"{{{{main|{group.MainArticle}}}}}");
            }

            if (remainingGrouping.Count > 0) {
                var (groupBody, groupHeadingCount) = RenderBody(
                    groupRecords, remainingGrouping, display, statusContext,
                    customGroups: null, startHeading: headingLevel + 1, innerTree);
                headingCount += groupHeadingCount;
                builder.AppendLine(groupBody);
            } else {
                builder.AppendLine(BuildFlatListBody(groupRecords, display, statusContext));
            }
            builder.AppendLine();
        }

        if (unmatchedRecords != null && unmatchedRecords.Count > 0) {
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

        foreach (var group in customGroups.Where(g => !g.Default)) {
            if (group.Families.Any(f => f.Equals(family, StringComparison.OrdinalIgnoreCase))) {
                return group;
            }
        }

        return null;
    }

    /// <summary>
    /// Writes a node's own items, then a heading and the subtree for each child. Items come first
    /// because wikitext gives everything after a heading to that heading: an item written after the
    /// last child section would read as part of it.
    /// </summary>
    private void AppendTree(
        StringBuilder builder,
        TaxonomyTreeNode<IucnSpeciesRecord> node,
        int childHeading,
        DisplayPreferences display,
        string? statusContext,
        ref int headingCount,
        OtherBucketContext? otherContext) {

        if (node.Items.Count > 0) {
            var itemsContext = node.LumpedKey is { } lumpedKey && display.IncludeFamilyInOtherBucket
                ? BuildOtherContext(node, node.LumpedLabel ?? lumpedKey, lumpedKey)
                : otherContext;
            AppendItems(builder, node.Items, display, statusContext, itemsContext);
        }

        foreach (var child in node.Children) {
            if (child.ItemCount == 0) {
                continue;
            }

            // The tree builder keeps the tree within the heading budget; the clamp is a guard only.
            var headingMarkup = new string('=', Math.Min(childHeading, DeepestHeading));
            var heading = FormatNodeHeading(child, node);
            builder.AppendLine($"{headingMarkup} {heading.Text} {headingMarkup}");
            headingCount++;

            var isResidual = child.Kind != TreeNodeKind.Virtual && IsOtherOrUnknownHeading(child.Value ?? string.Empty);
            if (!string.IsNullOrWhiteSpace(heading.CommonNameSentence)) {
                builder.AppendLine(heading.CommonNameSentence);
            } else if (!string.IsNullOrWhiteSpace(heading.MainLink) && !isResidual) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            if (!string.IsNullOrWhiteSpace(heading.Description)) {
                builder.AppendLine(heading.Description);
            }

            var childOtherContext = isResidual && display.IncludeFamilyInOtherBucket
                ? BuildOtherContext(child)
                : otherContext;
            AppendTree(builder, child, childHeading + 1, display, statusContext, ref headingCount, childOtherContext);
        }
    }

    /// <summary>
    /// The heading for a tree node:
    /// <list type="bullet">
    ///   <item>Virtual group: the group's common plural with its {{main}} link.</item>
    ///   <item>Catalogue of Life node: "Suborder Serpentes", or just "Cetacea" when its rank is not shown.</item>
    ///   <item>Level and auto-split values: "Family Felidae". A family-level "Other" bucket under a
    ///         named parent reads "Other {parent}" ("Other Testudines", "Other snakes").</item>
    /// </list>
    /// </summary>
    private HeadingInfo FormatNodeHeading(TaxonomyTreeNode<IucnSpeciesRecord> child, TaxonomyTreeNode<IucnSpeciesRecord> parent) {
        switch (child.Kind) {
            case TreeNodeKind.Virtual: {
                var group = FindVirtualGroup(child);
                return group != null
                    ? _headingFormatter.FormatVirtualGroupHeading(group)
                    : new HeadingInfo(child.Value ?? "Other", null);
            }
            case TreeNodeKind.Intermediate:
                return _headingFormatter.FormatHeading(child.Value, ColRankOrder.DisplayName(child.Key), GetKingdomName(child), showRank: child.ShowRank);
        }

        var heading = _headingFormatter.FormatHeading(child.Value, child.Label, GetKingdomName(child));
        if (string.Equals(child.Key, "family", StringComparison.OrdinalIgnoreCase)
            && IsOtherOrUnknownHeading(child.Value ?? string.Empty)
            && ParentName(parent) is { } parentName) {
            heading = heading with { Text = $"Other {parentName}" };
        }
        return heading;
    }

    /// <summary>The parent's name for an "Other {parent}" heading, or null for the root.</summary>
    private string? ParentName(TaxonomyTreeNode<IucnSpeciesRecord> parent) {
        if (string.IsNullOrWhiteSpace(parent.Value)) {
            return null;
        }

        if (parent.Kind == TreeNodeKind.Virtual) {
            var group = FindVirtualGroup(parent);
            return group != null
                ? _headingFormatter.FormatVirtualGroupHeading(group).Text.ToLowerInvariant()
                : null;
        }

        return ToTitleCase(parent.Value);
    }

    private VirtualGroup? FindVirtualGroup(TaxonomyTreeNode<IucnSpeciesRecord> node) {
        if (_taxonRules is null || string.IsNullOrWhiteSpace(node.VirtualOwner)) {
            return null;
        }

        return _taxonRules.GetVirtualGroups(node.VirtualOwner)?.Groups
            .FirstOrDefault(g => string.Equals(g.Name, node.Value, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The annotation context for an "Other" bucket: each record's value at the bucket's rank, so
    /// lines can read "(Family: X)" or "(subfamily: Y)". CoL ranks are read from the placement.
    /// </summary>
    private OtherBucketContext BuildOtherContext(TaxonomyTreeNode<IucnSpeciesRecord> node) =>
        BuildOtherContext(node, node.Label ?? "Family", node.Key ?? (node.Label ?? "Family").ToLowerInvariant());

    private OtherBucketContext BuildOtherContext(TaxonomyTreeNode<IucnSpeciesRecord> node, string rankLabel, string rankKey) {
        var selector = BuildSelector(rankKey, _placement);
        var map = new Dictionary<long, string>();
        foreach (var record in CollectRecords(node)) {
            var value = selector(record);
            if (!string.IsNullOrWhiteSpace(value)) {
                map[record.TaxonId] = value;
            }
        }
        return new OtherBucketContext(true, rankLabel, map);
    }

    private static List<IucnSpeciesRecord> CollectRecords(TaxonomyTreeNode<IucnSpeciesRecord> node) {
        var result = new List<IucnSpeciesRecord>();
        Collect(node);
        return result;

        void Collect(TaxonomyTreeNode<IucnSpeciesRecord> current) {
            result.AddRange(current.Items);
            foreach (var child in current.Children) {
                Collect(child);
            }
        }
    }

    private string BuildFlatListBody(
        IReadOnlyList<IucnSpeciesRecord> records,
        DisplayPreferences display,
        string? statusContext) {
        var builder = new StringBuilder();
        AppendItems(builder, records, display, statusContext, otherContext: null);
        return builder.ToString().TrimEnd();
    }

    /// <summary>Writes species lines in the display's infraspecific mode.</summary>
    private void AppendItems(
        StringBuilder builder,
        IReadOnlyList<IucnSpeciesRecord> items,
        DisplayPreferences display,
        string? statusContext,
        OtherBucketContext? otherContext) {
        if (ResolveInfraspecificMode(display) == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, items, display, statusContext, otherContext);
        } else {
            AppendPartitionedItems(builder, items, display, statusContext, otherContext);
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

        // Populations (EPBC population listings / IUCN subpopulations)
        if (populations.Count > 0) {
            builder.AppendLine();
            builder.AppendLine("'''Populations'''");
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
}
