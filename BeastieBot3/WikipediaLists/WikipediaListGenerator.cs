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
    private readonly CommonNameProvider? _commonNameProvider;
    private readonly StoreBackedCommonNameProvider? _storeBackedProvider;
    private readonly ColTaxonomyEnricher? _colEnricher;

    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        CommonNameProvider? commonNameProvider) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = commonNameProvider;
        _storeBackedProvider = null;
        _colEnricher = null;
    }

    /// <summary>
    /// Constructor using the new store-backed common name provider with pre-aggregated names.
    /// </summary>
    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        StoreBackedCommonNameProvider? storeBackedProvider,
        ColTaxonomyEnricher? colEnricher = null) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = null;
        _storeBackedProvider = storeBackedProvider;
        _colEnricher = colEnricher;
    }

    public WikipediaListResult Generate(
        WikipediaListDefinition definition,
        WikipediaListDefaults defaults,
        string outputDirectory,
        int? limit) {
        var statusDescriptors = CollectStatusDescriptors(definition);
        var records = _queryService.QuerySpecies(definition, statusDescriptors, limit);
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

            var (sectionBody, sectionHeadingCount) = BuildSectionBody(section.Records, grouping, display, section.StatusContext);
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

    private (string Body, int HeadingCount) BuildSectionBody(IReadOnlyList<IucnSpeciesRecord> records, IReadOnlyList<GroupingLevelDefinition> grouping, DisplayPreferences display, string? statusContext) {
        if (records.Count == 0) {
            return ("''No taxa currently listable.''", 0);
        }

        if (grouping.Count == 0) {
            return (string.Join(Environment.NewLine, records.Select(record => FormatSpeciesLine(record, display, statusContext))), 0);
        }

        // Check if we need COL enrichment (any grouping level uses COL-specific ranks)
        var needsEnrichment = _colEnricher != null && grouping.Any(g => IsColEnrichedRank(g.Level));
        
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

        var tree = TaxonomyTreeBuilder.Build(records, levels);
        var builder = new StringBuilder();
        var headingCount = 0;
        AppendTree(builder, tree, startHeading: 3, display, statusContext, ref headingCount);
        return (builder.ToString().TrimEnd(), headingCount);
    }

    private static readonly HashSet<string> ColEnrichedRanks = new(StringComparer.OrdinalIgnoreCase) {
        "subkingdom", "subphylum", "superclass", "subclass", "infraclass",
        "superorder", "suborder", "infraorder", "parvorder",
        "superfamily", "subfamily", "tribe", "subtribe", "subgenus"
    };

    private static bool IsColEnrichedRank(string level) => ColEnrichedRanks.Contains(level);

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

        var tree = TaxonomyTreeBuilder.Build(enrichedRecords, levels);
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
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            var heading = FormatHeading(child.Value);
            builder.AppendLine($"{headingMarkup} {heading.Text} {headingMarkup}");
            headingCount++;
            if (!string.IsNullOrWhiteSpace(heading.MainLink)) {
                builder.AppendLine($"{{{{main|{heading.MainLink}}}}}");
            }
            AppendEnrichedTree(builder, child, headingLevel + 1, display, statusContext, ref headingCount);
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
            var heading = FormatHeading(child.Value);
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

    private HeadingInfo FormatHeading(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return new HeadingInfo("Unassigned", null);
        }

        var rules = _legacyRules.Get(raw);
        if (!string.IsNullOrWhiteSpace(rules?.CommonPlural)) {
            return new HeadingInfo(Uppercase(rules!.CommonPlural)!, ToTitleCase(raw));
        }

        if (!string.IsNullOrWhiteSpace(rules?.CommonName)) {
            return new HeadingInfo(Uppercase(rules!.CommonName)!, ToTitleCase(raw));
        }

        if (!string.IsNullOrWhiteSpace(rules?.Wikilink)) {
            return new HeadingInfo(raw, rules!.Wikilink);
        }

        return new HeadingInfo(raw, null);
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
        if (display.ItalicizeScientific && !string.IsNullOrWhiteSpace(scientific)) {
            scientific = $"''{scientific}''";
        }

        if (!string.IsNullOrWhiteSpace(commonName) && display.PreferCommonNames) {
            return $"[[{commonName}]] ({scientific})";
        }

        return scientific ?? record.GenusName;
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
