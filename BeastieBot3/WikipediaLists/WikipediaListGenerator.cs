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
        var display = MergeDisplayPreferences(defaults.Display, definition.Display);

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

        // Filter out regional assessments if requested
        // Regional assessments are excluded from main lists to keep output global-only
        var filteredRecords = records.Where(r => !IsRegionalAssessment(r)).ToList();

        if (filteredRecords.Count == 0) {
            return ("''No taxa currently listable (all filtered as regional assessments).''", 0);
        }

        var infraspecificMode = ResolveInfraspecificMode(display);

        // If separating infraspecific sections is enabled, partition and render each section
        if (infraspecificMode == InfraspecificDisplayMode.SeparateSections && display.SeparateInfraspecificSections) {
            return BuildInfraspecificSections(filteredRecords, grouping, display, statusContext, customGroups, startHeading);
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
        var needsEnrichment = _colEnricher != null && 
            (grouping.Any(g => IsColEnrichedRank(g.Level)) || HasVirtualGroupsInGrouping(grouping));
        
        if (needsEnrichment) {
            return BuildEnrichedSectionBody(filteredRecords, grouping, display, statusContext);
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

        Func<string, bool>? shouldSkip = _taxonRules != null 
            ? taxon => _taxonRules.ShouldForceSplit(taxon) 
            : null;
        var tree = TaxonomyTreeBuilder.Build(filteredRecords, levels, shouldSkip);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, tree, startHeading, display, statusContext, ref headingCount, grouping, groupingIndex: 0, otherContext: null, parentTaxon: null);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    /// <summary>
    /// Build section body with separate sections for Species, Subspecies, Varieties, and Stocks/Populations.
    /// </summary>
    private (string Body, int HeadingCount) BuildInfraspecificSections(
        IReadOnlyList<IucnSpeciesRecord> records,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        string? statusContext,
        IReadOnlyList<CustomGroupDefinition>? customGroups,
        int startHeading) {
        
        // Partition records by type
        var species = new List<IucnSpeciesRecord>();
        var subspecies = new List<IucnSpeciesRecord>();
        var varieties = new List<IucnSpeciesRecord>();
        var populations = new List<IucnSpeciesRecord>();

        foreach (var record in records) {
            // Check for non-global assessments first (regional scopes or subpopulations)
            if (IsRegionalAssessment(record)) {
                populations.Add(record);
                continue;
            }

            // Check for infrarank
            var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? "";
            if (!string.IsNullOrWhiteSpace(infraType) && !string.IsNullOrWhiteSpace(record.InfraName)) {
                if (infraType.Contains("var")) {
                    varieties.Add(record);
                } else if (infraType.Contains("ssp") || infraType.Contains("subsp")) {
                    subspecies.Add(record);
                } else {
                    // Other infranks (form, etc.) go to subspecies section
                    subspecies.Add(record);
                }
                continue;
            }

            species.Add(record);
        }

        var builder = new StringBuilder();
        var headingCount = 0;

        // Create a display settings copy with infraspecific grouping disabled to avoid recursion
        var innerDisplay = new DisplayPreferences {
            PreferCommonNames = display.PreferCommonNames,
            ItalicizeScientific = display.ItalicizeScientific,
            IncludeStatusTemplate = display.IncludeStatusTemplate,
            IncludeStatusLabel = display.IncludeStatusLabel,
            GroupSubspecies = false,
            ListingStyle = display.ListingStyle,
            InfraspecificDisplayMode = InfraspecificDisplayMode.SeparateSections,
            SeparateInfraspecificSections = false,  // Prevent recursion
            ExcludeRegionalAssessments = false,     // Already filtered
            IncludeFamilyInOtherBucket = display.IncludeFamilyInOtherBucket
        };

        void AppendSectionHeader(string label) {
            builder.AppendLine($"'''{label}'''");
            builder.AppendLine("{{div col|colwidth=30em}}");
        }

        void AppendSectionFooter() {
            builder.AppendLine("{{div col end}}");
        }

        // Render Species section (only add heading if other sections exist)
        if (species.Count > 0) {
            var needsSectionHeading = subspecies.Count > 0 || varieties.Count > 0 || populations.Count > 0;
            if (needsSectionHeading) {
                AppendSectionHeader("Species");
                headingCount++;
            }
            var (speciesBody, speciesHeadingCount) = BuildSectionBodyCore(species, grouping, innerDisplay, statusContext, customGroups, startHeading + (subspecies.Count > 0 || varieties.Count > 0 || populations.Count > 0 ? 1 : 0));
            headingCount += speciesHeadingCount;
            builder.AppendLine(speciesBody);
            if (needsSectionHeading) {
                AppendSectionFooter();
            }
            if (subspecies.Count > 0 || varieties.Count > 0 || populations.Count > 0) {
                builder.AppendLine();
            }
        }

        // Render Subspecies section
        if (subspecies.Count > 0) {
            AppendSectionHeader("Subspecies");
            headingCount++;
            var (subBody, subHeadingCount) = BuildSectionBodyCore(subspecies, grouping, innerDisplay, statusContext, customGroups, startHeading + 1);
            headingCount += subHeadingCount;
            builder.AppendLine(subBody);
            AppendSectionFooter();
            if (varieties.Count > 0 || populations.Count > 0) {
                builder.AppendLine();
            }
        }

        // Render Varieties section
        if (varieties.Count > 0) {
            AppendSectionHeader("Varieties");
            headingCount++;
            var (varBody, varHeadingCount) = BuildSectionBodyCore(varieties, grouping, innerDisplay, statusContext, customGroups, startHeading + 1);
            headingCount += varHeadingCount;
            builder.AppendLine(varBody);
            AppendSectionFooter();
            if (populations.Count > 0) {
                builder.AppendLine();
            }
        }

        // Render Stocks and populations section
        if (populations.Count > 0) {
            AppendSectionHeader("Stocks and populations");
            headingCount++;
            var (popBody, popHeadingCount) = BuildSectionBodyCore(populations, grouping, innerDisplay, statusContext, customGroups, startHeading + 1);
            headingCount += popHeadingCount;
            builder.AppendLine(popBody);
            AppendSectionFooter();
        }

        return (builder.ToString().TrimEnd(), headingCount);
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
        int startHeading = 3) {
        
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
                level.OtherLabel,
                level.MinGroupsForOther))
            .ToList();

        Func<string, bool>? shouldSkip = _taxonRules != null 
            ? taxon => _taxonRules.ShouldForceSplit(taxon) 
            : null;
        var tree = TaxonomyTreeBuilder.Build(records, levels, shouldSkip);
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
                level.OtherLabel,
                level.MinGroupsForOther))
            .ToList();

        Func<string, bool>? shouldSkip = _taxonRules != null 
            ? taxon => _taxonRules.ShouldForceSplit(taxon) 
            : null;
        var tree = TaxonomyTreeBuilder.Build(enrichedRecords, levels, shouldSkip);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendEnrichedTree(builder, tree, startHeading: 3, display, statusContext, ref headingCount, grouping, groupingIndex: 0, otherContext: null, parentTaxon: null);
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
            var taxonName = child.Value;
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            
            // Get grouping configuration for current level
            var currentGrouping = grouping != null && groupingIndex < grouping.Count 
                ? grouping[groupingIndex] 
                : null;
            var showRankLabel = currentGrouping?.ShowRankLabel ?? false;
            
            var heading = FormatHeading(taxonName, child.Label, GetKingdomName(child), showRankLabel);
            var headingText = heading.Text;
            if (IsOtherOrUnknownHeading(taxonName ?? string.Empty) &&
                currentGrouping?.Level.Equals("family", StringComparison.OrdinalIgnoreCase) == true &&
                !string.IsNullOrWhiteSpace(parentTaxon)) {
                headingText = $"Other {ToTitleCase(parentTaxon)}";
            }

            builder.AppendLine($"{headingMarkup} {headingText} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.MainLink) && !IsOtherOrUnknownHeading(headingText)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }

            // Detect if this is an "Other" bucket
            var isOtherBucket = IsOtherOrUnknownHeading(taxonName ?? "");
            var childOtherContext = isOtherBucket && display.IncludeFamilyInOtherBucket 
                ? new OtherBucketContext(true) 
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
        } else {
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
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
                        var familyHeading = FormatHeading(familyGroup.Key, "family", GetKingdomName(familyGroup));
                        builder.AppendLine($"{familyHeadingMarkup} {familyHeading.Text} {familyHeadingMarkup}");
                        headingCount++;
                        if (!string.IsNullOrWhiteSpace(familyHeading.MainLink)) {
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
        string? statusContext,
        OtherBucketContext? otherContext = null) {
        
        var iucnRecords = records.Select(r => r.ToIucnRecord()).ToList();
        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, iucnRecords, display, statusContext, otherContext);
        } else {
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
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
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            
            // Get grouping configuration for current level
            var currentGrouping = grouping != null && groupingIndex < grouping.Count 
                ? grouping[groupingIndex] 
                : null;
            var showRankLabel = currentGrouping?.ShowRankLabel ?? false;
            
            var heading = FormatHeading(child.Value, child.Label, GetKingdomName(child), showRankLabel);
            var headingText = heading.Text;
            if (IsOtherOrUnknownHeading(child.Value ?? string.Empty) &&
                currentGrouping?.Level.Equals("family", StringComparison.OrdinalIgnoreCase) == true &&
                !string.IsNullOrWhiteSpace(parentTaxon)) {
                headingText = $"Other {ToTitleCase(parentTaxon)}";
            }

            builder.AppendLine($"{headingMarkup} {headingText} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.MainLink) && !IsOtherOrUnknownHeading(headingText)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            
            // Detect if this is an "Other" bucket
            var isOtherBucket = IsOtherOrUnknownHeading(child.Value ?? "");
            var childOtherContext = isOtherBucket && display.IncludeFamilyInOtherBucket 
                ? new OtherBucketContext(true) 
                : otherContext;
            
            AppendTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount, grouping, groupingIndex + 1, childOtherContext, parentTaxon: child.Value);
        }

        var infraspecificMode = ResolveInfraspecificMode(display);
        if (infraspecificMode == InfraspecificDisplayMode.GroupedUnderSpecies) {
            AppendItemsWithInfraspecificGrouping(builder, node.Items, display, statusContext, otherContext);
        } else {
            foreach (var record in OrderRecordsForOutput(node.Items, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
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
        string? statusContext,
        OtherBucketContext? otherContext = null) {
        
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
            builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            
            // Check if this species has subspecies
            if (subspeciesGroups.TryGetValue(speciesKey, out var subs)) {
                foreach (var sub in subs.OrderBy(s => s.InfraName, StringComparer.OrdinalIgnoreCase)) {
                    builder.AppendLine(FormatSubspeciesLine(sub, display, statusContext, otherContext));
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
                builder.AppendLine(FormatSubspeciesLine(sub, display, statusContext, otherContext));
            }
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
        } else {
            foreach (var record in OrderRecordsForOutput(records, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
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

        var processedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in OrderRecordsForOutput(species, otherContext)) {
            var speciesKey = GetParentSpeciesKey(record);
            builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
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
                builder.AppendLine(IndentSubBullet(FormatInfraspecificLine(sub, display, statusContext, otherContext)));
            }
        }

        if (varietyGroups.TryGetValue(speciesKey, out var varieties)) {
            foreach (var variety in varieties.OrderBy(s => ResolveScientificName(s) ?? string.Empty, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(IndentSubBullet(FormatInfraspecificLine(variety, display, statusContext, otherContext)));
            }
        }

        if (populationGroups.TryGetValue(speciesKey, out var populations)) {
            foreach (var population in populations.OrderBy(s => ResolveScientificName(s) ?? string.Empty, StringComparer.OrdinalIgnoreCase)) {
                builder.AppendLine(IndentSubBullet(FormatSpeciesLine(population, display, statusContext, otherContext)));
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

    private static bool IsSubspecies(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(record.InfraName) && (infraType.Contains("subsp") || infraType.Contains("ssp"));
    }

    private static bool IsVariety(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(record.InfraName) && infraType.Contains("var");
    }

    private static bool IsInfraspecific(IucnSpeciesRecord record) {
        return !string.IsNullOrWhiteSpace(record.InfraName) && !string.IsNullOrWhiteSpace(record.InfraType);
    }

    private static bool IsRegionalAssessment(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
            return true;
        }

        var scopes = record.Scopes;
        if (string.IsNullOrWhiteSpace(scopes)) {
            return false;
        }

        var parts = scopes.Split(new[] { ',', ';', '&' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var hasGlobalScope = parts.Any(part => part.Contains("global", StringComparison.OrdinalIgnoreCase));
        if (hasGlobalScope) {
            return false;
        }

        return parts.Length > 0;
    }

    private static string? GetRegionalScopeLabel(IucnSpeciesRecord record) {
        if (!IsRegionalAssessment(record)) {
            return null;
        }

        if (string.IsNullOrWhiteSpace(record.Scopes)) {
            return null;
        }

        var parts = record.Scopes
            .Split(new[] { ',', ';', '&' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(part => !part.Contains("global", StringComparison.OrdinalIgnoreCase))
            .ToList();

        return parts.Count == 0 ? null : string.Join(", ", parts);
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

    private static IEnumerable<IucnSpeciesRecord> OrderRecordsForOutput(
        IEnumerable<IucnSpeciesRecord> records,
        OtherBucketContext? otherContext) {
        var ordered = records;
        if (otherContext is { IsInOtherBucket: true }) {
            ordered = ordered
                .OrderBy(r => r.FamilyName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ThenBy(r => ResolveScientificName(r) ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        }
        return ordered;
    }

    private static string GetParentSpeciesKey(IucnSpeciesRecord record) {
        return $"{record.GenusName?.ToLowerInvariant()}|{record.SpeciesName?.ToLowerInvariant()}";
    }

    private string FormatSubspeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? statusContext, OtherBucketContext? otherContext = null) {
        // Indented subspecies line
        var line = FormatSpeciesLine(record, display, statusContext, otherContext);
        // Add extra indentation (** instead of *)
        if (line.StartsWith("* ")) {
            return "*" + line;
        }
        return line;
    }

    private string FormatInfraspecificLine(IucnSpeciesRecord record, DisplayPreferences display, string? listStatusContext, OtherBucketContext? otherContext = null) {
        var descriptor = IucnRedlistStatus.Describe(record.StatusCode);
        var builder = new StringBuilder();
        builder.Append("* ");

        var commonName = ResolveCommonName(record);
        var articleTitle = ResolveWikipediaArticle(record);
        var infraLink = BuildInfraspecificLink(record, articleTitle, abbreviateGenus: true);
        if (!string.IsNullOrWhiteSpace(infraLink)) {
            builder.Append(infraLink);
            if (!string.IsNullOrWhiteSpace(commonName)) {
                builder.Append(", ");
                builder.Append(commonName);
            }
        } else {
            builder.Append(BuildNameFragment(record, display));
        }

        var specialLabel = GetSpecialStatusLabel(record.StatusCode, listStatusContext);
        if (!string.IsNullOrWhiteSpace(specialLabel)) {
            builder.Append(" (");
            builder.Append(specialLabel);
            builder.Append(')');
        }

        var scopeLabel = GetRegionalScopeLabel(record);
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName) || !string.IsNullOrWhiteSpace(scopeLabel)) {
            builder.Append(" (");
            if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                builder.Append("subpopulation: ");
                builder.Append(record.SubpopulationName);
            }

            if (!string.IsNullOrWhiteSpace(scopeLabel)) {
                if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                    builder.Append("; ");
                }
                builder.Append("scope: ");
                builder.Append(scopeLabel);
            }

            builder.Append(')');
        }

        if (display.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(BuildIucnStatusTemplate(record, descriptor));
        }

        if (otherContext is { IsInOtherBucket: true } && !string.IsNullOrWhiteSpace(record.FamilyName)) {
            var familyName = ToTitleCase(record.FamilyName);
            var shouldLink = otherContext.ShouldLinkFamily(familyName);
            if (shouldLink) {
                builder.Append($" (Family: [[{familyName}]])");
            } else {
                builder.Append($" (Family: {familyName})");
            }
        }

        return builder.ToString();
    }

    private readonly record struct HeadingInfo(string Text, string? MainLink);
    
    /// <summary>
    /// Context for items within an "Other" bucket, tracking which families need annotation.
    /// </summary>
    private sealed class OtherBucketContext {
        private readonly HashSet<string> _linkedFamilies = new(StringComparer.OrdinalIgnoreCase);
        
        public bool IsInOtherBucket { get; }
        
        public OtherBucketContext(bool isInOtherBucket) {
            IsInOtherBucket = isInOtherBucket;
        }
        
        /// <summary>
        /// Returns true if this is the first occurrence of the family and it should be linked.
        /// </summary>
        public bool ShouldLinkFamily(string family) {
            if (string.IsNullOrWhiteSpace(family)) return false;
            return _linkedFamilies.Add(family);
        }
    }

    private HeadingInfo FormatHeading(string? raw, string? rank = null, string? kingdom = null, bool showRankLabel = false) {
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
            var headingText = FormatHeadingText(displayName, rank, showRankLabel, isScientificName: true);
            return new HeadingInfo(headingText, yamlRule.Wikilink);
        }

        if (!string.IsNullOrWhiteSpace(rules?.Wikilink)) {
            var headingText = FormatHeadingText(displayName, rank, showRankLabel, isScientificName: true);
            return new HeadingInfo(headingText, rules!.Wikilink);
        }

        // If we have a main article from YAML, use it
        if (!string.IsNullOrWhiteSpace(yamlMainArticle)) {
            var headingText = FormatHeadingText(displayName, rank, showRankLabel, isScientificName: true);
            return new HeadingInfo(headingText, yamlMainArticle);
        }

        // Scientific name only - apply rank label formatting
        var finalText = FormatHeadingText(displayName, rank, showRankLabel, isScientificName: true);
        return new HeadingInfo(finalText, null);
    }
    
    /// <summary>
    /// Formats heading text, optionally adding rank label prefix.
    /// </summary>
    private static string FormatHeadingText(string displayName, string? rank, bool showRankLabel, bool isScientificName) {
        if (!showRankLabel || string.IsNullOrWhiteSpace(rank) || !isScientificName) {
            return displayName;
        }
        
        // Capitalize the rank for display (e.g., "family" -> "Family")
        var capitalizedRank = char.ToUpperInvariant(rank[0]) + rank.Substring(1).ToLowerInvariant();
        return $"{capitalizedRank} {displayName}";
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

    private string FormatSpeciesLine(IucnSpeciesRecord record, DisplayPreferences display, string? listStatusContext, OtherBucketContext? otherContext = null) {
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
        var scopeLabel = GetRegionalScopeLabel(record);
        if (!string.IsNullOrWhiteSpace(record.SubpopulationName) || !string.IsNullOrWhiteSpace(scopeLabel)) {
            builder.Append(" (");
            if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                builder.Append("subpopulation: ");
                builder.Append(record.SubpopulationName);
            }

            if (!string.IsNullOrWhiteSpace(scopeLabel)) {
                if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
                    builder.Append("; ");
                }
                builder.Append("scope: ");
                builder.Append(scopeLabel);
            }

            builder.Append(')');
        }

        // Add IUCN status template at end: {{IUCN status|XX|taxonId/assessmentId|1|year=YYYY}}
        if (display.IncludeStatusTemplate) {
            builder.Append(' ');
            builder.Append(BuildIucnStatusTemplate(record, descriptor));
        }
        
        // Add family annotation for "Other" bucket items
        if (otherContext is { IsInOtherBucket: true } && !string.IsNullOrWhiteSpace(record.FamilyName)) {
            var familyName = ToTitleCase(record.FamilyName);
            var shouldLink = otherContext.ShouldLinkFamily(familyName);
            if (shouldLink) {
                builder.Append($" (Family: [[{familyName}]])");
            } else {
                builder.Append($" (Family: {familyName})");
            }
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
        var articleTitle = ResolveWikipediaArticle(record);
        var rawScientific = ResolveScientificName(record);
        var formattedScientific = FormatScientificNameForDisplay(record, display.ItalicizeScientific);
        
        return display.ListingStyle switch {
            ListingStyle.ScientificNameFocus => BuildScientificNameFocusFragment(commonName, articleTitle, rawScientific, formattedScientific, record),
            ListingStyle.CommonNameOnly => BuildCommonNameOnlyFragment(commonName, articleTitle, rawScientific, formattedScientific, record),
            _ => BuildCommonNameFocusFragment(commonName, articleTitle, rawScientific, formattedScientific, record),  // Default: CommonNameFocus
        };
    }

    /// <summary>
    /// Style A: Scientific name focus. Shows scientific name first, common name after comma.
    /// Examples:
    /// - ''[[Pinus radiata]]'', Monterey pine
    /// - ''[[Scientific name]]''
    /// - ''[[Wikilink|Scientific name]]'', Common name
    /// </summary>
    private string BuildScientificNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        // For infraspecific taxa with var./subsp., use special formatting
        var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
        var infraLink = hasInfrarank ? BuildInfraspecificLink(record, articleTitle) : null;
        
        if (!string.IsNullOrWhiteSpace(infraLink)) {
            if (!string.IsNullOrWhiteSpace(commonName)) {
                return $"{infraLink}, {commonName}";
            }
            return infraLink;
        }
        
        // Standard species formatting
        var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
        
        if (string.IsNullOrWhiteSpace(linkTarget)) {
            return formattedScientific;
        }
        
        // Use ''[[X]]'' format when link target matches scientific name
        if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
            var linkedScientific = $"''[[{rawScientific}]]''";
            if (!string.IsNullOrWhiteSpace(commonName)) {
                return $"{linkedScientific}, {commonName}";
            }
            return linkedScientific;
        }
        
        // Article uses common name as title, so use [[Wikilink|Scientific name]]
        var linkedWithPipe = $"[[{linkTarget}|{formattedScientific}]]";
        if (!string.IsNullOrWhiteSpace(commonName)) {
            return $"{linkedWithPipe}, {commonName}";
        }
        return linkedWithPipe;
    }

    /// <summary>
    /// Style B: Common name focus (default). Shows common name first, scientific name in parentheses.
    /// Examples:
    /// - [[Common name]] (''Scientific name'')
    /// - [[Wikilink|Common name]] (''Scientific name'')
    /// - ''[[Scientific name]]'' (fallback when no common name)
    /// </summary>
    private string BuildCommonNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            // Fallback to scientific name only
            var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
            if (!string.IsNullOrWhiteSpace(linkTarget)) {
                if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
                    return $"''[[{linkTarget}]]''";
                }
                return $"[[{linkTarget}|{formattedScientific}]]";
            }
            return formattedScientific;
        }
        
        // We have a common name
        var commonLinkTarget = ResolveLinkTargetForCommonName(articleTitle, rawScientific, commonName);
        
        if (string.IsNullOrWhiteSpace(commonLinkTarget)) {
            // No link target available, just use common name
            return $"[[{commonName}]] ({formattedScientific})";
        }
        
        // Build the link
        string linkedCommonName;
        if (string.Equals(commonLinkTarget, commonName, StringComparison.Ordinal)) {
            linkedCommonName = $"[[{commonName}]]";
        } else {
            linkedCommonName = $"[[{commonLinkTarget}|{commonName}]]";
        }
        
        return $"{linkedCommonName} ({formattedScientific})";
    }

    /// <summary>
    /// Style C: Common name only. Shows only common name (falls back to scientific if unavailable).
    /// Examples:
    /// - [[Common name]]
    /// - [[Wikilink|Common name]]
    /// - ''[[Scientific name]]'' (fallback when no common name)
    /// </summary>
    private string BuildCommonNameOnlyFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            // Fallback to scientific name
            var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
            if (!string.IsNullOrWhiteSpace(linkTarget)) {
                if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
                    return $"''[[{linkTarget}]]''";
                }
                return $"[[{linkTarget}|{formattedScientific}]]";
            }
            return formattedScientific;
        }
        
        // We have a common name - show only common name
        var commonLinkTarget = ResolveLinkTargetForCommonName(articleTitle, rawScientific, commonName);
        
        if (string.IsNullOrWhiteSpace(commonLinkTarget)) {
            return $"[[{commonName}]]";
        }
        
        if (string.Equals(commonLinkTarget, commonName, StringComparison.Ordinal)) {
            return $"[[{commonName}]]";
        }
        
        return $"[[{commonLinkTarget}|{commonName}]]";
    }

    /// <summary>
    /// Builds a properly formatted link for subspecies/varieties with correct italicization.
    /// For infraspecific taxa, we need [[link|''Genus species'' subsp. ''subspecies'']] format.
    /// For animals, the rank marker is hidden.
    /// </summary>
    private static string? BuildInfraspecificLink(IucnSpeciesRecord record, string? articleTitle, bool abbreviateGenus = false) {
        if (string.IsNullOrWhiteSpace(record.InfraName)) {
            return null;
        }
        
        var displayText = BuildInfraspecificDisplayText(record, abbreviateGenus);
        var fullScientific = BuildScientificNameForLink(record);
        if (string.IsNullOrWhiteSpace(displayText) || string.IsNullOrWhiteSpace(fullScientific)) {
            return null;
        }

        var linkTarget = !string.IsNullOrWhiteSpace(articleTitle) ? articleTitle : fullScientific;

        if (!abbreviateGenus && string.Equals(linkTarget, fullScientific, StringComparison.OrdinalIgnoreCase) &&
            !RequiresRankMarker(record)) {
            return $"''[[{fullScientific}]]''";
        }

        return $"[[{linkTarget}|{displayText}]]";
    }

    /// <summary>
    /// Format a scientific name for display (with italics if requested).
    /// </summary>
    private static string FormatScientificNameForDisplay(IucnSpeciesRecord record, bool italicize) {
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            var formatted = BuildInfraspecificDisplayText(record, abbreviateGenus: false, stripItalics: !italicize);
            if (!string.IsNullOrWhiteSpace(formatted)) {
                return formatted;
            }
        }

        var scientific = BuildScientificNameForDisplay(record);
        if (string.IsNullOrWhiteSpace(scientific)) {
            return record.GenusName ?? "";
        }
        
        return italicize ? $"''{scientific}''" : scientific;
    }

    private static string? BuildScientificNameForDisplay(IucnSpeciesRecord record) {
        if (!string.IsNullOrWhiteSpace(record.InfraName)) {
            return BuildInfraspecificDisplayText(record, abbreviateGenus: false, stripItalics: true);
        }

        return ResolveScientificName(record);
    }

    private static string BuildScientificNameForLink(IucnSpeciesRecord record) {
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return ResolveScientificName(record) ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(record.InfraName)) {
            return $"{genus} {species}";
        }

        var rankMarker = ResolveInfraspecificRankMarker(record);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            return $"{genus} {species} {rankMarker} {record.InfraName?.Trim()}".Replace("  ", " ");
        }

        return $"{genus} {species} {record.InfraName?.Trim()}".Replace("  ", " ");
    }

    private static string? BuildInfraspecificDisplayText(
        IucnSpeciesRecord record,
        bool abbreviateGenus,
        bool stripItalics = false) {
        var genus = record.GenusName?.Trim();
        var species = record.SpeciesName?.Trim();
        var infraName = record.InfraName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species) || string.IsNullOrWhiteSpace(infraName)) {
            return null;
        }

        if (abbreviateGenus) {
            genus = genus.Length > 0 ? $"{genus[0]}." : genus;
        }

        var rankMarker = ResolveInfraspecificRankMarker(record);
        if (!string.IsNullOrWhiteSpace(rankMarker)) {
            var head = stripItalics ? $"{genus} {species}" : $"''{genus} {species}''";
            var tail = stripItalics ? infraName : $"''{infraName}''";
            return $"{head} {rankMarker} {tail}";
        }

        return stripItalics
            ? $"{genus} {species} {infraName}"
            : $"''{genus} {species} {infraName}''";
    }

    private static string? ResolveInfraspecificRankMarker(IucnSpeciesRecord record) {
        var infraType = record.InfraType?.Trim().ToLowerInvariant() ?? string.Empty;
        var kingdom = record.KingdomName?.ToUpperInvariant() ?? string.Empty;

        if (infraType.Contains("var")) {
            return "var.";
        }

        if (infraType.Contains("subsp") || infraType.Contains("ssp")) {
            return kingdom == "ANIMALIA" ? null : "subsp.";
        }

        if (!string.IsNullOrWhiteSpace(infraType)) {
            return infraType.EndsWith(".") ? infraType : infraType + ".";
        }

        return null;
    }

    private static bool RequiresRankMarker(IucnSpeciesRecord record) {
        return !string.IsNullOrWhiteSpace(ResolveInfraspecificRankMarker(record));
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

    private string ResolveLinkTarget(IucnSpeciesRecord record, string? articleTitle, string? rawScientific) {
        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return articleTitle;
        }

        if (!string.IsNullOrWhiteSpace(rawScientific)) {
            return rawScientific;
        }

        var built = BuildScientificNameForLink(record);
        if (!string.IsNullOrWhiteSpace(built)) {
            return built;
        }

        return record.GenusName ?? record.SpeciesName ?? string.Empty;
    }

    private static string ResolveLinkTargetForCommonName(string? articleTitle, string? rawScientific, string commonName) {
        if (!string.IsNullOrWhiteSpace(articleTitle)) {
            return articleTitle;
        }

        if (!string.IsNullOrWhiteSpace(rawScientific)) {
            return rawScientific;
        }

        return commonName;
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

    /// <summary>
    /// Merges display preferences, with override values taking precedence over base values.
    /// This allows taxa groups to specify just the settings they want to change while
    /// inheriting all other settings from defaults.
    /// </summary>
    private static DisplayPreferences MergeDisplayPreferences(DisplayPreferences? basePrefs, DisplayPreferences? overridePrefs) {
        // Start with defaults if no base
        basePrefs ??= new DisplayPreferences();
        
        // If no override, just use base
        if (overridePrefs == null) {
            return basePrefs;
        }

        // Merge: override takes precedence for explicitly set values
        // Since we can't tell if a value was explicitly set or just defaulted in YAML,
        // we use a heuristic: non-default values in override take precedence
        return new DisplayPreferences {
            PreferCommonNames = overridePrefs.PreferCommonNames,
            ItalicizeScientific = overridePrefs.ItalicizeScientific,
            IncludeStatusTemplate = overridePrefs.IncludeStatusTemplate,
            IncludeStatusLabel = overridePrefs.IncludeStatusLabel,
            GroupSubspecies = overridePrefs.GroupSubspecies || basePrefs.GroupSubspecies,
            ListingStyle = overridePrefs.ListingStyle != ListingStyle.CommonNameFocus 
                ? overridePrefs.ListingStyle 
                : basePrefs.ListingStyle,
            InfraspecificDisplayMode = overridePrefs.InfraspecificDisplayMode != InfraspecificDisplayMode.SeparateSections
                ? overridePrefs.InfraspecificDisplayMode
                : basePrefs.InfraspecificDisplayMode,
            SeparateInfraspecificSections = overridePrefs.SeparateInfraspecificSections || basePrefs.SeparateInfraspecificSections,
            ExcludeRegionalAssessments = overridePrefs.ExcludeRegionalAssessments || basePrefs.ExcludeRegionalAssessments,
            IncludeFamilyInOtherBucket = overridePrefs.IncludeFamilyInOtherBucket || basePrefs.IncludeFamilyInOtherBucket,
        };
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
