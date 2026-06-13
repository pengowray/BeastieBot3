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
using static BeastieBot3.WikipediaLists.RecordClassification;
using static BeastieBot3.WikipediaLists.ProseFormat;
using static BeastieBot3.WikipediaLists.ParentSummaryTableBuilder;
using static BeastieBot3.WikipediaLists.SpeciesLineFormatter;

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
    // Per-child status-count aggregator for parent lists (summary table + count sentences).
    // Optional: when null, parent lists degrade gracefully (no summary table).
    private readonly IucnChartDataBuilder? _chartData;
    // Builds the English-prose intro + mustache template context (counts, percentages, paragraphs).
    private readonly IntroProseBuilder _introProse;
    // Renders one record to a wikitext bullet line in the selected listing style (owns name resolution).
    private readonly SpeciesLineFormatter _lineFormatter;

    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        CommonNameProvider? commonNameProvider,
        TaxonRulesService? taxonRules = null,
        IucnChartDataBuilder? chartData = null) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = commonNameProvider;
        _storeBackedProvider = null;
        _colEnricher = null;
        _taxonRules = taxonRules;
        _chartData = chartData;
        _introProse = new IntroProseBuilder(_queryService);
        _lineFormatter = new SpeciesLineFormatter(_legacyRules, _storeBackedProvider, _commonNameProvider);
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
        TaxonRulesService? taxonRules = null,
        IucnChartDataBuilder? chartData = null) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = null;
        _storeBackedProvider = storeBackedProvider;
        _colEnricher = colEnricher;
        _taxonRules = taxonRules;
        _chartData = chartData;
        _introProse = new IntroProseBuilder(_queryService);
        _lineFormatter = new SpeciesLineFormatter(_legacyRules, _storeBackedProvider, _commonNameProvider);
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
        var datasetYear = datasetVersion.IndexOf('-') is var dash and >= 0
            ? datasetVersion[..dash]
            : datasetVersion;

        var scopeLabel = BuildScopeLabel(definition);
        var sectionSummary = string.Join("; ", sections.Select(section => $"{section.Definition.Heading} ({section.Records.Count})"));

        var allRecords = sections.SelectMany(s => s.Records).ToList();
        var context = _introProse.BuildContext(
            definition, allRecords, totalCount, scopeLabel, sectionSummary, datasetVersion, datasetYear);

        var headerTemplate = definition.Templates.Header ?? defaults.HeaderTemplate;
        var footerTemplate = definition.Templates.Footer ?? defaults.FooterTemplate;

        var builder = new StringBuilder();
        builder.AppendLine(_templateRenderer.Render(headerTemplate, context).TrimEnd());
        builder.AppendLine();

        var grouping = (IReadOnlyList<GroupingLevelDefinition>)(definition.Grouping
            ?? defaults.Grouping
            ?? new List<GroupingLevelDefinition>());
        var display = MergeDisplayPreferences(defaults.Display, definition.Display);

        // A parent list (one with resolved phylogenetic children) renders a summary table + bare-bones
        // child sections instead of the flat species body. Requires the count aggregator.
        var isParent = definition.SubLists.Count > 0 && _chartData != null;
        var parentTableEmitted = false;

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

            int sectionHeadingCount;
            string sectionBody;
            var autoSplitConfig = ResolveAutoSplitConfig(definition, defaults);
            if (isParent) {
                (sectionBody, sectionHeadingCount) = BuildParentSectionBody(
                    section, definition, grouping, display, autoSplitConfig, datasetYear,
                    startHeading: 3, includeTable: !parentTableEmitted);
                parentTableEmitted = true;
            } else {
                (sectionBody, sectionHeadingCount) = BuildSectionBody(
                    section.Records, grouping, display, section.StatusContext, definition.CustomGroups,
                    autoSplit: autoSplitConfig);
            }
            totalHeadingCount += sectionHeadingCount;
            builder.AppendLine(sectionBody);
            builder.AppendLine();
        }

        // Non-phylogenetic cross-references (e.g. marine mammals under mammals) render once as a plain
        // bullet block — never as count rows or nested phylogenetic sub-lists.
        if (isParent && definition.SeeAlso.Count > 0) {
            builder.AppendLine(BuildRelatedListsBlock(definition.SeeAlso));
            builder.AppendLine();
            totalHeadingCount++;
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
            HeadingCount = totalHeadingCount,
            IsParent = isParent
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

        var parts = definition.Filters
            .OrderBy(filter => RankOrder.GetValueOrDefault(filter.Rank?.Trim().ToLowerInvariant() ?? "", 99))
            .Select(FilterScopeLabel)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToList();

        return parts.Count > 0 ? string.Join(" › ", parts!) : "global";
    }

    /// <summary>
    /// A human-readable breadcrumb segment for one filter. Handles System tags, multi-value (Values)
    /// includes, and exclude-only filters so virtual parents (Fish = several classes; Invertebrates =
    /// Animalia minus Chordata) and System-filter groups (marine mammals) never render blank segments.
    /// </summary>
    private static string? FilterScopeLabel(TaxonFilterDefinition filter) {
        if (!string.IsNullOrWhiteSpace(filter.System)) {
            return filter.System.Trim();
        }
        if (filter.Values is { Count: > 0 }) {
            var joined = string.Join("/", filter.Values.Select(v => v.Trim()).Where(v => v.Length > 0));
            return string.IsNullOrWhiteSpace(joined) ? null : joined;
        }
        var value = filter.Value?.Trim();
        var hasExclude = filter.Exclude is { Count: > 0 };
        if (!string.IsNullOrWhiteSpace(value)) {
            return hasExclude ? $"{value} (excl. {string.Join(", ", filter.Exclude!)})" : value;
        }
        return hasExclude ? $"excl. {string.Join(", ", filter.Exclude!)}" : null;
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

    // ==================== Parent (nested) list rendering ====================

    /// <summary>
    /// Render a PARENT list section: a full EX..DD summary table of child sub-taxa (once per list) plus
    /// a bare-bones summary block per phylogenetic child (heading + {{main}} + a status-scoped count
    /// sentence). The parent links to and summarizes its children; it never re-queries or inlines their
    /// species. Counts come from the canonical <see cref="IucnChartDataBuilder.BuildChildBreakdown"/>.
    /// </summary>
    private (string Body, int HeadingCount) BuildParentSectionBody(
        SectionRuntime section,
        WikipediaListDefinition definition,
        IReadOnlyList<GroupingLevelDefinition> grouping,
        DisplayPreferences display,
        AutoSplitConfig? autoSplit,
        string datasetYear,
        int startHeading,
        bool includeTable) {

        var sb = new StringBuilder();
        var headingCount = 0;

        // Determine the rank that distinguishes children (e.g. "class") and each child's value at it.
        var childRank = DeriveChildRank(definition.SubLists);
        var linkByValue = new Dictionary<string, ChildListLink>(StringComparer.Ordinal);
        var orderedKeys = new List<string>();
        foreach (var link in definition.SubLists) {
            var val = ChildDiscriminatingValue(link, childRank);
            if (val != null && !linkByValue.ContainsKey(val)) {
                linkByValue[val] = link;
                orderedKeys.Add(val);
            }
        }

        // One GROUP BY scan over the parent scope, grouped by the child rank column. Curated children
        // are seeded so zero-count children still appear; other classes/orders under the parent appear too.
        Dictionary<string, IReadOnlyList<StatusCount>>? breakdown = null;
        if (childRank != null && _chartData != null) {
            breakdown = _chartData.BuildChildBreakdown(definition.Filters, childRank, orderedKeys);
        }

        if (includeTable && breakdown is { Count: > 0 }) {
            sb.AppendLine(BuildChildSummaryTable(breakdown, orderedKeys, linkByValue));
            sb.AppendLine();
        }

        var statusText = !string.IsNullOrWhiteSpace(definition.StatusText)
            ? definition.StatusText
            : section.Definition.Heading.ToLowerInvariant();

        // One bare-bones block per phylogenetic child, scoped to THIS section's statuses.
        foreach (var link in definition.SubLists) {
            var val = ChildDiscriminatingValue(link, childRank);
            IReadOnlyList<StatusCount>? row = null;
            if (val != null && breakdown != null) breakdown.TryGetValue(val, out row);
            var n = row != null ? SectionStatusTotal(row, section.StatusSet) : 0;
            if (n == 0) continue; // omit empty child SECTIONS — the child still appears as a table row

            var markup = new string('=', Math.Min(startHeading, 6));
            sb.AppendLine($"{markup} {link.DisplayName} {markup}");
            headingCount++;
            sb.AppendLine($"{{{{main|{link.WikiTitle}}}}}");
            var adj = string.IsNullOrWhiteSpace(link.Adjective) ? string.Empty : link.Adjective + " ";
            // Append a possibly-extinct breakdown only when this section actually covers CR(PE)/CR(PEW)
            // and the counts are nonzero.
            var pe = section.StatusSet.Contains("CR(PE)") ? RowCount(row!, "CR(PE)") : 0;
            var pew = section.StatusSet.Contains("CR(PEW)") ? RowCount(row!, "CR(PEW)") : 0;
            sb.AppendLine($"As of {datasetYear}, the IUCN Red List lists {NewspaperNumber(n)} {statusText} {adj}species{PossiblyExtinctClause(pe, pew)}.");
            sb.AppendLine();
        }

        // Orphan sub-taxa: parent-scope taxa with NO dedicated sub-list. Each gets its own heading
        // (a peer of the linked children, e.g. === Cephalopoda ===) with a flat species list beneath —
        // no "Other" wrapper heading and no deeper order/family nesting at the top level. Nothing is
        // dropped, so the sub-lists + orphan sections together cover the whole parent scope.
        if (childRank != null) {
            var selector = BuildSelector(childRank);
            var childValueSet = new HashSet<string>(orderedKeys, StringComparer.Ordinal);
            var orphanGroups = section.Records
                .Where(r => !IsRegionalAssessment(r))
                .Where(r => {
                    var v = TaxonFilterSql.NormalizeValue(childRank, selector(r));
                    return v == null || !childValueSet.Contains(v);
                })
                .GroupBy(r => selector(r) ?? string.Empty)
                .OrderByDescending(g => g.Count())
                .ThenBy(g => g.Key, StringComparer.Ordinal);

            var markup = new string('=', Math.Min(startHeading, 6));
            foreach (var grp in orphanGroups) {
                var heading = string.IsNullOrWhiteSpace(grp.Key) ? "Unassigned" : ToTitleCase(grp.Key);
                sb.AppendLine($"{markup} {heading} {markup}");
                headingCount++;
                // Reuse the standard list-generation path so an orphan class splits into orders/families
                // (and auto-splits) exactly when a normal list would — no bespoke rendering for these.
                var (body, hc) = BuildSectionBody(
                    grp.ToList(), grouping, display, section.StatusContext, definition.CustomGroups,
                    startHeading: Math.Min(startHeading, 6) + 1, autoSplit: autoSplit);
                headingCount += hc;
                sb.AppendLine(body);
                sb.AppendLine();
            }
        }

        return (sb.ToString().TrimEnd(), headingCount);
    }

    /// <summary>The finest rank-based single-value filter the children carry (e.g. "class" for insects/gastropods).</summary>
    private static string? DeriveChildRank(IReadOnlyList<ChildListLink> children) {
        string? best = null;
        var bestOrder = -1;
        foreach (var link in children) {
            foreach (var f in link.Filters) {
                if (!string.IsNullOrWhiteSpace(f.System) || string.IsNullOrWhiteSpace(f.Value)) continue;
                var rank = f.Rank?.Trim().ToLowerInvariant();
                if (rank == null || !RankOrder.TryGetValue(rank, out var ord)) continue;
                if (ord > bestOrder) { bestOrder = ord; best = rank; }
            }
        }
        return best;
    }

    /// <summary>A child's normalized value at the parent's child rank (e.g. "INSECTA"), matching the breakdown keys.</summary>
    private static string? ChildDiscriminatingValue(ChildListLink link, string? childRank) {
        if (childRank == null) return null;
        foreach (var f in link.Filters) {
            if (!string.IsNullOrWhiteSpace(f.System)) continue;
            if (!string.Equals(f.Rank?.Trim(), childRank, StringComparison.OrdinalIgnoreCase)) continue;
            if (string.IsNullOrWhiteSpace(f.Value)) continue;
            return TaxonFilterSql.NormalizeValue(childRank, f.Value);
        }
        return null;
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
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
            if (iucnRecords.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(iucnRecords, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
    /// Each candidate level sets <c>UnknownLabel = OtherLabel</c> so that species missing
    /// a rank value route into "Other {rank}" instead of "Unknown {rank}" — this prevents
    /// the RejectUnknownGroups gate from blocking otherwise good splits.
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
                    MinGroupsForOther: 3));
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
    /// Candidates are all COL intermediate ranks below the last defined grouping level
    /// (subfamily, tribe, subtribe, subgenus, genus).
    /// Each candidate level sets <c>UnknownLabel = OtherLabel</c> so that species missing
    /// a rank value route into "Other {rank}" instead of "Unknown {rank}".
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
                    MinGroupsForOther: 3));
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
            if (node.Items.Count >= 3) builder.AppendLine("{{div col|colwidth=30em}}");
            foreach (var record in OrderRecordsForOutput(node.Items, otherContext)) {
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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
            builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
            
            // Check if this species has subspecies
            if (subspeciesGroups.TryGetValue(speciesKey, out var subs)) {
                foreach (var sub in subs.OrderBy(s => s.InfraName, StringComparer.OrdinalIgnoreCase)) {
                    builder.AppendLine(_lineFormatter.FormatSubspeciesLine(sub, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSubspeciesLine(sub, display, statusContext, otherContext));
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
                builder.AppendLine(_lineFormatter.FormatSpeciesLine(record, display, statusContext, otherContext));
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

    private static IEnumerable<IucnSpeciesRecord> OrderRecordsForOutput(
        IEnumerable<IucnSpeciesRecord> records,
        OtherBucketContext? otherContext) {
        var ordered = records;
        if (otherContext is { IsInOtherBucket: true }) {
            ordered = ordered
                .OrderBy(r => otherContext.GetRankValue(r) ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ThenBy(r => ResolveScientificName(r) ?? string.Empty, StringComparer.OrdinalIgnoreCase);
        }
        return ordered;
    }

    private readonly record struct HeadingInfo(string Text, string? MainLink, string? CommonNameSentence = null, string? Description = null);
    
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

        // --- Revived comprises/blurb grey-text line (legacy TaxonHeaderBlurb.GrayText) ---
        var description = FormatTaxonDescription(yamlRule);

        return new HeadingInfo(headingText, null, sentence, description);
    }

    /// <summary>
    /// Build the optional descriptive line under a heading from a taxon rule's <c>blurb</c>/<c>comprises</c>.
    /// <c>blurb</c> is emitted as authored (already a sentence, e.g. "Includes tree frogs and allies");
    /// <c>comprises</c> becomes an italic "Comprises X." line. Returns null when neither is authored.
    /// </summary>
    private static string? FormatTaxonDescription(TaxonRule? rule) {
        if (rule is null) return null;
        if (!string.IsNullOrWhiteSpace(rule.Blurb)) {
            return rule.Blurb.Trim();
        }
        if (!string.IsNullOrWhiteSpace(rule.Comprises)) {
            return $"''Comprises {rule.Comprises.Trim()}.''";
        }
        return null;
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
