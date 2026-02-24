using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BeastieBot3.WikipediaLists.Legacy;
using BeastieBot3.CommonNames;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

// Main engine for generating Wikipedia species list wikitext. Workflow:
// 1. IucnListQueryService fetches matching species from IUCN database
// 2. ColTaxonomyEnricher adds COL ranks for grouping
// 3. StoreBackedCommonNameProvider resolves vernacular names
// 4. TaxonRulesService applies exclusions/overrides
// 5. TaxonomyTreeBuilder groups by taxonomy hierarchy
// 6. WikipediaTemplateRenderer outputs final wikitext
// Orchestrated by WikipediaListCommand.

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

        // Classify records by rank
        var allRecords = sections.SelectMany(s => s.Records).ToList();
        var speciesCount = allRecords.Count(r => !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName));
        var subspeciesCount = allRecords.Count(IsSubspecies);
        var varietyCount = allRecords.Count(IsVariety);
        var subpopCount = allRecords.Count(r => !string.IsNullOrWhiteSpace(r.SubpopulationName));

        var taxaAdj = definition.TaxaAdjective ?? "";
        var taxaNameLower = definition.TaxaNameLower ?? "";
        var statusText = definition.StatusText ?? "";
        var statusWikiLink = definition.StatusWikiLink ?? "";

        var isExtinct = statusText == "extinct" || statusText == "extinct in the wild";
        var hasIntroMetadata = !string.IsNullOrEmpty(definition.TaxaAdjective) && !string.IsNullOrEmpty(definition.StatusText);

        // Compute percentage of evaluated species (skip for extinct lists and lists without intro metadata)
        string? percentageText = null;
        if (hasIntroMetadata && !isExtinct) {
            var evaluatedTotal = _queryService.CountEvaluatedSpecies(definition.Filters);
            if (evaluatedTotal > 0 && speciesCount > 0) {
                percentageText = $"{FormatPercentage(speciesCount, evaluatedTotal)} of all evaluated {taxaAdj} species are listed as {statusText}.";
            }
        }

        // Build pre-rendered intro paragraphs (skip when no intro metadata, suppress subpops for extinct)
        string? subspeciesParagraph = null;
        string? subpopulationParagraph = null;
        string? threatenedContext = null;
        string? ddInfo = null;
        string? notesParagraph = null;
        if (hasIntroMetadata) {
            subspeciesParagraph = BuildSubspeciesParagraph(subspeciesCount, varietyCount, taxaAdj, statusText);
            subpopulationParagraph = isExtinct ? null : BuildSubpopulationParagraph(subpopCount, taxaNameLower, statusText);
            threatenedContext = BuildThreatenedContext(definition, taxaAdj, taxaNameLower);
            ddInfo = BuildDataDeficientInfo(definition, taxaAdj);
            notesParagraph = BuildNotesParagraph(speciesCount, subspeciesCount, varietyCount, subpopCount, taxaAdj, statusText);
        }

        // CR-specific: possibly extinct counts
        string? peText = null;
        if (statusText == "critically endangered") {
            var peCount = allRecords.Count(r => r.StatusCode == "CR(PE)" && !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName));
            var pewCount = allRecords.Count(r => r.StatusCode == "CR(PEW)" && !IsInfraspecific(r) && string.IsNullOrWhiteSpace(r.SubpopulationName));
            var combinedPe = peCount + pewCount;
            if (combinedPe > 0) {
                if (peCount > 0 && pewCount == 0) {
                    peText = $", including {NewspaperNumber(peCount)} which are tagged as ''possibly extinct''";
                } else {
                    peText = $", including {NewspaperNumber(combinedPe)} which are tagged as ''possibly extinct'' or ''possibly extinct in the wild''";
                }
            } else {
                peText = ", none of which are tagged as ''possibly extinct''";
            }
        }

        var context = new Dictionary<string, object?> {
            ["title"] = definition.Title,
            ["description"] = definition.Description,
            ["scope_label"] = scopeLabel,
            ["dataset_version"] = datasetVersion,
            ["generated_at"] = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ["total_entries"] = totalCount,
            ["sections_summary"] = sectionSummary,
            // Intro text variables
            ["species_count"] = NewspaperNumber(speciesCount),
            ["taxa_adjective"] = string.IsNullOrEmpty(taxaAdj) ? null : taxaAdj,
            ["taxa_name_lower"] = taxaNameLower,
            ["status_text"] = statusText,
            ["status_wiki_link"] = statusWikiLink,
            ["percentage_text"] = percentageText,
            ["pe_text"] = peText,
            ["subspecies_paragraph"] = subspeciesParagraph,
            ["subpopulation_paragraph"] = subpopulationParagraph,
            ["threatened_context"] = threatenedContext,
            ["dd_info"] = ddInfo,
            ["notes_paragraph"] = notesParagraph,
            ["simple_intro"] = hasIntroMetadata ? null : "1",
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

            var autoSplitConfig = ResolveAutoSplitConfig(definition, defaults);
            var (sectionBody, sectionHeadingCount) = BuildSectionBody(
                section.Records, grouping, display, section.StatusContext, definition.CustomGroups,
                autoSplit: autoSplitConfig);
            totalHeadingCount += sectionHeadingCount;
            builder.AppendLine(sectionBody);
            builder.AppendLine();
        }

        builder.AppendLine(_templateRenderer.Render(footerTemplate, context).TrimEnd());
        builder.AppendLine();

        Directory.CreateDirectory(outputDirectory);
        var outputPath = Path.Combine(outputDirectory, definition.OutputFile);
        var content = builder.ToString();
        File.WriteAllText(outputPath, content);

        // Collect structural metrics from the generated wikitext
        var metrics = new ListStructureMetrics {
            ListId = definition.Id,
            FileName = Path.GetFileName(outputPath),
            TotalTaxa = totalCount,
            HeadingCount = totalHeadingCount
        };
        WikitextMetricsCollector.CollectFromWikitext(content, metrics);
        WikitextMetricsCollector.DetectProblems(metrics);

        return new WikipediaListResult(outputPath, totalCount, totalHeadingCount, datasetVersion, metrics);
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
            return BuildEnrichedSectionBody(filteredRecords, grouping, display, statusContext, autoSplit);
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
            return BuildEnrichedSectionBody(records, grouping, display, statusContext, autoSplit);
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
        string? statusContext,
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
            var heading = FormatHeading(taxonName, child.Label, GetKingdomName(child));
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
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, iucnRecords, display, statusContext, otherContext);
        } else {
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col end}}");
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
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, iucnRecords, display, statusContext, otherContext);
        } else {
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col end}}");
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

    // Rank hierarchy from broadest to narrowest, used for auto-split candidate selection.
    // Only ranks that are useful for section splitting (not kingdom/phylum/class which are too broad).
    private static readonly string[] RankHierarchy = {
        "order", "suborder", "infraorder", "parvorder", "superfamily",
        "family", "subfamily", "tribe", "subtribe", "subgenus", "genus"
    };

    /// <summary>
    /// Resolve auto-split config: list-level overrides defaults.
    /// </summary>
    private static AutoSplitConfig? ResolveAutoSplitConfig(
        WikipediaListDefinition definition, WikipediaListDefaults defaults) {
        return definition.AutoSplit ?? defaults.AutoSplit;
    }

    /// <summary>
    /// Build auto-split options for non-enriched (IUCN-only) records.
    /// Candidates are limited to family and genus (the only ranks available without COL).
    /// </summary>
    private static AutoSplitOptions<IucnSpeciesRecord>? BuildAutoSplitOptionsIucn(
        AutoSplitConfig? config,
        IReadOnlyList<GroupingLevelDefinition> definedLevels,
        IAutoSplitDiagnostics? diagnostics = null) {
        if (config == null || !config.Enabled) {
            return null;
        }

        var definedRanks = new HashSet<string>(
            definedLevels.Select(l => l.Level.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);

        var candidates = new List<TaxonomyTreeLevel<IucnSpeciesRecord>>();
        var iucnRanks = new[] { "order", "family", "genus" };

        var lastDefinedIndex = -1;
        for (int i = 0; i < iucnRanks.Length; i++) {
            if (definedRanks.Contains(iucnRanks[i])) {
                lastDefinedIndex = i;
            }
        }

        // Add ranks below the last defined rank that aren't already defined
        for (int i = lastDefinedIndex + 1; i < iucnRanks.Length; i++) {
            var rank = iucnRanks[i];
            if (!definedRanks.Contains(rank)) {
                var otherLabel = GetOtherLabel(rank);
                candidates.Add(new TaxonomyTreeLevel<IucnSpeciesRecord>(
                    rank, BuildSelector(rank),
                    UnknownLabel: otherLabel,
                    MinItems: config.MinItemsPerGroup,
                    OtherLabel: otherLabel,
                    MinGroupsForOther: 2));
            }
        }

        if (candidates.Count == 0) {
            return null;
        }

        return new AutoSplitOptions<IucnSpeciesRecord>(
            config.Threshold, config.MinGroupSize, candidates,
            MaxOtherFraction: config.MaxOtherFraction,
            MaxGroups: config.MaxGroups,
            MaxDepth: config.MaxDepth,
            MinMeaningfulGroups: config.MinMeaningfulGroups,
            RejectUnknownGroups: config.RejectUnknownGroups,
            Diagnostics: diagnostics);
    }

    /// <summary>
    /// Build auto-split options for COL-enriched records.
    /// Candidates are all COL intermediate ranks below the last defined grouping level.
    /// </summary>
    private static AutoSplitOptions<EnrichedSpeciesRecord>? BuildAutoSplitOptionsEnriched(
        AutoSplitConfig? config,
        IReadOnlyList<GroupingLevelDefinition> definedLevels,
        IAutoSplitDiagnostics? diagnostics = null) {
        if (config == null || !config.Enabled) {
            return null;
        }

        var definedRanks = new HashSet<string>(
            definedLevels.Select(l => l.Level.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);

        // Find the position of the last defined rank in the hierarchy
        var lastDefinedIndex = -1;
        for (int i = 0; i < RankHierarchy.Length; i++) {
            if (definedRanks.Contains(RankHierarchy[i])) {
                lastDefinedIndex = i;
            }
        }

        // Add ranks below the last defined rank that aren't already defined
        var candidates = new List<TaxonomyTreeLevel<EnrichedSpeciesRecord>>();
        for (int i = lastDefinedIndex + 1; i < RankHierarchy.Length; i++) {
            var rank = RankHierarchy[i];
            if (!definedRanks.Contains(rank)) {
                var otherLabel = GetOtherLabel(rank);
                candidates.Add(new TaxonomyTreeLevel<EnrichedSpeciesRecord>(
                    rank, BuildEnrichedSelector(rank),
                    UnknownLabel: otherLabel,
                    MinItems: config.MinItemsPerGroup,
                    OtherLabel: otherLabel,
                    MinGroupsForOther: 2));
            }
        }

        if (candidates.Count == 0) {
            return null;
        }

        return new AutoSplitOptions<EnrichedSpeciesRecord>(
            config.Threshold, config.MinGroupSize, candidates,
            MaxOtherFraction: config.MaxOtherFraction,
            MaxGroups: config.MaxGroups,
            MaxDepth: config.MaxDepth,
            MinMeaningfulGroups: config.MinMeaningfulGroups,
            RejectUnknownGroups: config.RejectUnknownGroups,
            Diagnostics: diagnostics);
    }

    private static string GetOtherLabel(string rank) => rank.ToLowerInvariant() switch {
        "subfamily" => "Other subfamilies",
        "superfamily" => "Other superfamilies",
        "family" => "Other families",
        "subgenus" => "Other subgenera",
        "genus" => "Other genera",
        "tribe" => "Other tribes",
        "subtribe" => "Other subtribes",
        "suborder" => "Other suborders",
        "infraorder" => "Other infraorders",
        "parvorder" => "Other parvorders",
        "order" => "Other orders",
        _ => $"Other {rank}"
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
            var heading = FormatHeading(child.Value, child.Label, GetKingdomName(child));
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

            // Detect if this is an "Other" bucket
            var isOtherBucket = IsOtherOrUnknownHeading(child.Value ?? "");
            var childOtherContext = isOtherBucket && display.IncludeFamilyInOtherBucket
                ? new OtherBucketContext(true)
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
            if (node.Items.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(node.Items, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (node.Items.Count >= 3) builder.AppendLine("{{div col end}}");
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
            if (species.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(species, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (species.Count >= 3) builder.AppendLine("{{div col end}}");
        }

        // Subspecies
        if (subspecies.Count > 0) {
            if (species.Count > 0) {
                builder.AppendLine();
            }
            builder.AppendLine("'''Subspecies'''");
            builder.AppendLine();
            if (subspecies.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(subspecies, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (subspecies.Count >= 3) builder.AppendLine("{{div col end}}");
        }

        // Varieties
        if (varieties.Count > 0) {
            builder.AppendLine();
            builder.AppendLine("'''Varieties'''");
            builder.AppendLine();
            if (varieties.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(varieties, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (varieties.Count >= 3) builder.AppendLine("{{div col end}}");
        }

        // Stocks and populations
        if (populations.Count > 0) {
            builder.AppendLine();
            builder.AppendLine("'''Stocks and populations'''");
            builder.AppendLine();
            if (populations.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(populations, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (populations.Count >= 3) builder.AppendLine("{{div col end}}");
        }

        // If only infraspecific taxa exist (no species), still render them
        if (species.Count == 0 && !hasInfraspecific) {
            // Shouldn't happen since we checked node.Items.Count > 0 above,
            // but guard anyway
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
        } else if (infraspecificMode == InfraspecificDisplayMode.SeparateSections) {
            AppendPartitionedItems(builder, records, display, statusContext, otherContext);
        } else {
            if (records.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(records, otherContext)) {
                builder.AppendLine(FormatSpeciesLine(record, display, statusContext, otherContext));
            }
            if (records.Count >= 3) builder.AppendLine("{{div col end}}");
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

        if (items.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");

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

        if (items.Count >= 3) builder.AppendLine("{{div col end}}");
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

    private readonly record struct HeadingInfo(string Text, string? MainLink, string? CommonNameSentence = null);
    
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

    private HeadingInfo FormatHeading(string? raw, string? rank = null, string? kingdom = null) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return new HeadingInfo("Unassigned", null);
        }

        if (IsOtherOrUnknownHeading(raw)) {
            return new HeadingInfo(raw.Trim(), null);
        }

        // Apply title case to the raw taxon name for display
        var displayName = ToTitleCase(raw);

        // --- Heading text is always the scientific name with rank label ---
        var headingText = FormatHeadingText(displayName, rank, showRankLabel: true, isScientificName: true);

        // --- Resolve common name from all sources (for sentence, not heading) ---
        string? commonName = null;
        var yamlRule = _taxonRules?.GetRule(raw);
        var legacyRules = _legacyRules.Get(raw);

        // Priority: YAML CommonPlural > YAML CommonName > Legacy CommonPlural > Legacy CommonName
        if (!string.IsNullOrWhiteSpace(yamlRule?.CommonPlural))
            commonName = yamlRule.CommonPlural;
        else if (!string.IsNullOrWhiteSpace(yamlRule?.CommonName))
            commonName = yamlRule.CommonName;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.CommonPlural))
            commonName = legacyRules.CommonPlural;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.CommonName))
            commonName = legacyRules.CommonName;
        else if (_storeBackedProvider is not null) {
            // Store-backed common names for higher taxa
            var storeName = _storeBackedProvider.GetBestCommonNameByScientificName(raw, kingdom);
            if (!string.IsNullOrWhiteSpace(storeName)) {
                commonName = storeName;
            } else {
                // Fallback: Wikipedia redirect target (e.g., Araneae -> Spider)
                var redirectTitle = _storeBackedProvider.GetWikipediaRedirectTitleByScientificName(raw);
                if (!string.IsNullOrWhiteSpace(redirectTitle) && !redirectTitle.Equals(raw, StringComparison.OrdinalIgnoreCase)) {
                    var cleaned = CommonNameNormalizer.RemoveDisambiguationSuffix(redirectTitle);
                    if (!CommonNameNormalizer.LooksLikeScientificName(cleaned, null, null)) {
                        commonName = cleaned;
                    }
                }
            }
        }

        // --- Resolve wikilink target for the sentence ---
        string? wikilinkTarget = null;
        if (!string.IsNullOrWhiteSpace(yamlRule?.Wikilink))
            wikilinkTarget = yamlRule.Wikilink;
        else if (!string.IsNullOrWhiteSpace(legacyRules?.Wikilink))
            wikilinkTarget = legacyRules.Wikilink;
        else {
            var yamlMainArticle = _taxonRules?.GetMainArticle(raw);
            if (!string.IsNullOrWhiteSpace(yamlMainArticle))
                wikilinkTarget = yamlMainArticle;
            else if (_storeBackedProvider is not null)
                wikilinkTarget = _storeBackedProvider.GetWikipediaArticleTitleByScientificName(raw, kingdom);
        }

        // --- Build common name sentence ---
        var sentence = BuildCommonNameSentence(displayName, rank, commonName, wikilinkTarget);

        return new HeadingInfo(headingText, null, sentence);
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

    /// <summary>
    /// Builds a descriptive sentence showing the common name for a taxon.
    /// Example: "Members of the [[Sminthidae]] family are called birch mice."
    /// </summary>
    private static string? BuildCommonNameSentence(
        string scientificName, string? rank, string? commonNameOrPlural, string? wikilinkOverride) {
        if (string.IsNullOrWhiteSpace(commonNameOrPlural)) {
            return null;
        }

        // Build wikilink expression
        string wikilink;
        if (!string.IsNullOrWhiteSpace(wikilinkOverride) &&
            !wikilinkOverride.Equals(scientificName, StringComparison.OrdinalIgnoreCase)) {
            wikilink = $"[[{wikilinkOverride}|{scientificName}]]";
        } else {
            wikilink = $"[[{scientificName}]]";
        }

        // Build sentence with or without rank
        if (!string.IsNullOrWhiteSpace(rank)) {
            var lowerRank = rank.ToLowerInvariant();
            return $"Members of the {wikilink} {lowerRank} are called {commonNameOrPlural}.";
        }

        return $"Members of {wikilink} are called {commonNameOrPlural}.";
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

        // For infraspecific taxa, use properly formatted name for link targets.
        // This ensures animal subspecies omit "ssp." and plants include "subsp."/"var.".
        var linkScientific = !string.IsNullOrWhiteSpace(record.InfraName)
            ? BuildScientificNameForLink(record)
            : rawScientific;

        return display.ListingStyle switch {
            ListingStyle.ScientificNameFocus => BuildScientificNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, record),
            ListingStyle.CommonNameOnly => BuildCommonNameOnlyFragment(commonName, articleTitle, linkScientific, formattedScientific, record),
            _ => BuildCommonNameFocusFragment(commonName, articleTitle, linkScientific, formattedScientific, record),  // Default: CommonNameFocus
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
    /// Scientific name must always be explicitly visible — never hidden inside a link.
    /// Examples:
    /// - [[Common name]] (''Scientific name'')
    /// - [[Wikilink|Common name]] (''Scientific name'')
    /// - [[Scientific name|Article title]] (''Scientific name'')  (fallback when no common name but article exists with different title)
    /// - ''[[Scientific name]]'' (fallback when no common name and no distinct article)
    /// </summary>
    private string BuildCommonNameFocusFragment(string? commonName, string? articleTitle, string? rawScientific, string formattedScientific, IucnSpeciesRecord record) {
        if (string.IsNullOrWhiteSpace(commonName)) {
            // For infraspecific taxa, use specialized formatting with proper rank markers
            var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
            if (hasInfrarank) {
                var infraLink = BuildInfraspecificLink(record, articleTitle);
                if (!string.IsNullOrWhiteSpace(infraLink)) {
                    return infraLink;
                }
            }

            // Fallback: no common name resolved
            var linkTarget = ResolveLinkTarget(record, articleTitle, rawScientific);
            if (!string.IsNullOrWhiteSpace(linkTarget)) {
                if (string.Equals(linkTarget, rawScientific, StringComparison.OrdinalIgnoreCase)) {
                    return $"''[[{linkTarget}]]''";
                }

                if (!string.IsNullOrWhiteSpace(rawScientific)) {
                    return $"[[{rawScientific}|{linkTarget}]] ({formattedScientific})";
                }
                return $"[[{linkTarget}]] ({formattedScientific})";
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
            // For infraspecific taxa, use specialized formatting with proper rank markers
            var hasInfrarank = !string.IsNullOrWhiteSpace(record.InfraType) && !string.IsNullOrWhiteSpace(record.InfraName);
            if (hasInfrarank) {
                var infraLink = BuildInfraspecificLink(record, articleTitle);
                if (!string.IsNullOrWhiteSpace(infraLink)) {
                    return infraLink;
                }
            }

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

    // ==================== Intro text helpers ====================

    private static string NewspaperNumber(int number) {
        var words = new[] { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };
        if (number >= 0 && number <= 10) return words[number];
        if (number >= 10000) return number.ToString("N0");
        return number.ToString();
    }

    private static string FormatPercentage(int count, int total) {
        var ratio = (double)count / total;
        if (ratio > 0.1) return ratio.ToString("P0");
        if (ratio > 0.01) return ratio.ToString("P1");
        return ratio.ToString("P2");
    }

    private static string? BuildSubspeciesParagraph(int subspeciesCount, int varietyCount, string? taxaAdj, string? statusText) {
        if (subspeciesCount == 0 && varietyCount == 0) return null;
        var total = subspeciesCount + varietyCount;
        string what;
        if (subspeciesCount > 0 && varietyCount > 0)
            what = $"{NewspaperNumber(subspeciesCount)} subspecies and {NewspaperNumber(varietyCount)} varieties";
        else if (varietyCount > 0)
            what = $"{NewspaperNumber(varietyCount)} {taxaAdj} varieties";
        else
            what = $"{NewspaperNumber(subspeciesCount)} {taxaAdj} subspecies";

        if (!string.IsNullOrEmpty(statusText))
            return $"The IUCN also lists {what} as {statusText}.";
        return $"The IUCN has also evaluated {what}.";
    }

    private static string? BuildSubpopulationParagraph(int subpopCount, string? taxaNameLower, string? statusText) {
        if (subpopCount == 0) {
            if (!string.IsNullOrEmpty(statusText))
                return $"No subpopulations of {taxaNameLower} have been evaluated as {statusText} by the IUCN.";
            return null;
        }
        var have = subpopCount == 1 ? "has" : "have";
        var subpops = subpopCount == 1 ? "a subpopulation" : "subpopulations";
        if (!string.IsNullOrEmpty(statusText))
            return $"Of the subpopulations of {taxaNameLower} evaluated by the IUCN, {NewspaperNumber(subpopCount)} {have} been assessed as {statusText}.";
        return $"Of the subpopulations of {taxaNameLower} evaluated by the IUCN, {NewspaperNumber(subpopCount)} {have} been assessed.";
    }

    private string? BuildThreatenedContext(WikipediaListDefinition definition, string? taxaAdj, string? taxaNameLower) {
        var statusText = definition.StatusText;
        if (statusText == "endangered") {
            var crCount = _queryService.CountSpeciesByStatus(definition.Filters, "CR");
            var enCount = _queryService.CountSpeciesByStatus(definition.Filters, "EN");
            var combined = crCount + enCount;
            var crListTitle = $"List of critically endangered {taxaNameLower}";
            return "For a species to be considered endangered by the IUCN it must meet certain quantitative criteria which are designed to classify taxa facing \"a very high risk of extinction\". "
                + "An even higher risk is faced by ''critically endangered'' species, which meet the quantitative criteria for endangered species. "
                + $"[[{crListTitle}|Critically endangered {taxaNameLower}]] are listed separately. "
                + $"There are {NewspaperNumber(combined)} {taxaAdj} species which are endangered or critically endangered.";
        }

        if (statusText == "vulnerable") {
            var crListTitle = $"List of critically endangered {taxaNameLower}";
            var enListTitle = $"List of endangered {taxaNameLower}";
            return "For a species to be assessed as vulnerable to extinction the best available evidence must meet quantitative criteria set by the IUCN designed to reflect \"a high risk of extinction in the wild\". "
                + $"''Endangered'' and ''critically endangered'' species also meet the quantitative criteria of ''vulnerable'' species, and are listed separately. See: [[{enListTitle}]], [[{crListTitle}]]. "
                + "Vulnerable, endangered and critically endangered species are collectively referred to as ''[[threatened species]]'' by the IUCN.";
        }

        return null;
    }

    private string? BuildDataDeficientInfo(WikipediaListDefinition definition, string? taxaAdj) {
        var statusText = definition.StatusText;
        // Only show DD info for threatened statuses
        if (statusText != "critically endangered" && statusText != "endangered" && statusText != "vulnerable" && statusText != "threatened")
            return null;

        var ddCount = _queryService.CountSpeciesByStatus(definition.Filters, "DD");
        if (ddCount == 0) return null;

        var evaluatedTotal = _queryService.CountEvaluatedSpecies(definition.Filters);
        var ddPercent = evaluatedTotal > 0 ? FormatPercentage(ddCount, evaluatedTotal) : "";

        return $"Additionally {NewspaperNumber(ddCount)} {taxaAdj} species ({ddPercent} of those evaluated) are listed as [[data deficient]], meaning there is insufficient information for a full assessment of conservation status. "
            + "As these species typically have small distributions and/or populations, they are intrinsically likely to be threatened, according to the IUCN."
            + "<ref>{{cite web|title=Limitations of the Data|url=http://www.iucnredlist.org/initiatives/mammals/description/limitations|website=The IUCN Red List of Threatened Species|publisher=Union for Conservation of Nature and Natural Resources (IUCN)|accessdate=11 January 2016|archive-date=7 October 2018|archive-url=https://web.archive.org/web/20181007170630/http://www.iucnredlist.org/initiatives/mammals/description/limitations|url-status=live}}</ref>"
            + " While the category of ''data deficient'' indicates that no assessment of extinction risk has been made for the taxa, the IUCN notes that it may be appropriate to give them \"the same degree of attention as threatened taxa, at least until their status can be assessed.\""
            + "<ref>{{cite web|title=2001 Categories & Criteria (version 3.1)|url=http://www.iucnredlist.org/static/categories_criteria_3_1|website=The IUCN Red List of Threatened Species|publisher=Union for Conservation of Nature and Natural Resources (IUCN)|accessdate=11 January 2016|archive-date=8 October 2008|archive-url=https://web.archive.org/web/20081008002903/http://www.iucnredlist.org/static/categories_criteria_3_1|url-status=live}}</ref>";
    }

    private static string BuildNotesParagraph(int speciesCount, int subspeciesCount, int varietyCount, int subpopCount, string? taxaAdj, string? statusText) {
        var whats = "species";
        if (subspeciesCount > 0 && varietyCount > 0)
            whats = "species, subspecies and varieties";
        else if (subspeciesCount > 0)
            whats = "species and subspecies";

        var statusPhrase = !string.IsNullOrEmpty(statusText) ? $"{statusText} " : "";
        var note = $"This is a complete list of {statusPhrase}{taxaAdj} {whats} as evaluated by the IUCN.";

        if (statusText == "critically endangered")
            note += " Species considered possibly extinct by the IUCN are marked as such.";

        if (subpopCount > 0 && !string.IsNullOrEmpty(statusText))
            note += $" {char.ToUpperInvariant(whats[0])}{whats[1..]} which have {statusText} subpopulations (or stocks) are indicated.";

        note += " Where possible common names for taxa are given while links point to the scientific name used by the IUCN.";

        return note;
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

internal sealed record WikipediaListResult(
    string OutputPath,
    int TotalEntries,
    int HeadingCount,
    string DatasetVersion,
    ListStructureMetrics? Metrics = null);

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
