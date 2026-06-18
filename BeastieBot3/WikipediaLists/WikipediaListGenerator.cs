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
using static BeastieBot3.WikipediaLists.HeadingFormatter;
using static BeastieBot3.WikipediaLists.TaxonGroupingHelper;

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
    // Builds section headings (scientific-name line + common-name sentence + main link + blurb).
    private readonly HeadingFormatter _headingFormatter;
    // Renders a section's records to wikitext (tree building + heading/line emission).
    private readonly SectionBodyRenderer _renderer;

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
        _headingFormatter = new HeadingFormatter(_legacyRules, _taxonRules, _storeBackedProvider);
        _renderer = new SectionBodyRenderer(_colEnricher, _taxonRules, _lineFormatter, _headingFormatter);
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
        _headingFormatter = new HeadingFormatter(_legacyRules, _taxonRules, _storeBackedProvider);
        _renderer = new SectionBodyRenderer(_colEnricher, _taxonRules, _lineFormatter, _headingFormatter);
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
        // Resolve the list's per-field display overrides against the global defaults baseline.
        var display = definition.Display?.ResolveAgainst(defaults.Display) ?? defaults.Display;

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
                (sectionBody, sectionHeadingCount) = _renderer.BuildSectionBody(
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
                // Use the common plural for the orphan class heading when a rule supplies one
                // (e.g. Diplopoda → Millipedes), matching the curated child sections; otherwise the
                // title-cased scientific name.
                var heading = string.IsNullOrWhiteSpace(grp.Key)
                    ? "Unassigned"
                    : _headingFormatter.ResolveHigherTaxonCommonName(grp.Key) ?? ToTitleCase(grp.Key);
                sb.AppendLine($"{markup} {heading} {markup}");
                headingCount++;
                // Reuse the standard list-generation path so an orphan class splits into orders/families
                // (and auto-splits) exactly when a normal list would — no bespoke rendering for these.
                var (body, hc) = _renderer.BuildSectionBody(
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
