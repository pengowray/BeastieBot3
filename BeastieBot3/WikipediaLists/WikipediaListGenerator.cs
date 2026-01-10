using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using BeastieBot3.WikipediaLists.Legacy;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaListGenerator {
    private readonly IucnListQueryService _queryService;
    private readonly WikipediaTemplateRenderer _templateRenderer;
    private readonly LegacyTaxaRuleList _legacyRules;
    private readonly CommonNameProvider? _commonNameProvider;

    public WikipediaListGenerator(
        IucnListQueryService queryService,
        WikipediaTemplateRenderer templateRenderer,
        LegacyTaxaRuleList legacyRules,
        CommonNameProvider? commonNameProvider) {
        _queryService = queryService ?? throw new ArgumentNullException(nameof(queryService));
        _templateRenderer = templateRenderer ?? throw new ArgumentNullException(nameof(templateRenderer));
        _legacyRules = legacyRules ?? throw new ArgumentNullException(nameof(legacyRules));
        _commonNameProvider = commonNameProvider;
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
        var datasetVersion = "unknown"; // Version now stored in import_metadata, not per-row

        var scopeLabel = BuildScopeLabel(definition);
        var sectionSummary = string.Join("; ", sections.Select(section => $"{section.Definition.Heading} ({section.Records.Count})"));

        var context = new TemplateContext {
            Title = definition.Title,
            Description = definition.Description,
            ScopeLabel = scopeLabel,
            DatasetVersion = datasetVersion,
            GeneratedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm 'UTC'", CultureInfo.InvariantCulture),
            TotalEntries = totalCount,
            SectionsSummary = sectionSummary
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

        foreach (var section in sections) {
            if (section.Records.Count == 0) {
                continue;
            }

            if (!section.Definition.HideHeading) {
                builder.AppendLine($"== {section.Definition.Heading} ==");
            }

            if (!string.IsNullOrWhiteSpace(section.Definition.Description)) {
                builder.AppendLine(section.Definition.Description);
                builder.AppendLine();
            }

            builder.AppendLine(BuildSectionBody(section.Records, grouping, display));
            builder.AppendLine();
        }

        builder.AppendLine(_templateRenderer.Render(footerTemplate, context).TrimEnd());
        builder.AppendLine();

        Directory.CreateDirectory(outputDirectory);
        var outputPath = Path.Combine(outputDirectory, definition.OutputFile);
        File.WriteAllText(outputPath, builder.ToString());

        return new WikipediaListResult(outputPath, totalCount, datasetVersion);
    }

    private static string BuildScopeLabel(WikipediaListDefinition definition) {
        if (definition.Filters.Count == 0) {
            return "global";
        }

        var ordered = definition.Filters
            .OrderBy(filter => filter.Rank, StringComparer.OrdinalIgnoreCase)
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

    private string BuildSectionBody(IReadOnlyList<IucnSpeciesRecord> records, IReadOnlyList<GroupingLevelDefinition> grouping, DisplayPreferences display) {
        if (records.Count == 0) {
            return "''No taxa currently listable.''";
        }

        if (grouping.Count == 0) {
            return string.Join(Environment.NewLine, records.Select(record => FormatSpeciesLine(record, display)));
        }

        var levels = grouping
            .Select(level => new TaxonomyTreeLevel<IucnSpeciesRecord>(
                level.Label ?? level.Level,
                BuildSelector(level.Level),
                level.AlwaysDisplay,
                level.UnknownLabel))
            .ToList();

        var tree = TaxonomyTreeBuilder.Build(records, levels);
        var builder = new StringBuilder();
        AppendTree(builder, tree, startHeading: 3, display);
        return builder.ToString().TrimEnd();
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

    private void AppendTree(StringBuilder builder, TaxonomyTreeNode<IucnSpeciesRecord> node, int startHeading, DisplayPreferences display) {
        foreach (var child in node.Children) {
            var headingLevel = Math.Min(startHeading, 6);
            var headingMarkup = new string('=', headingLevel);
            builder.AppendLine($"{headingMarkup} {FormatHeading(child.Value)} {headingMarkup}");
            AppendTree(builder, child, headingLevel + 1, display);
        }

        foreach (var record in node.Items) {
            builder.AppendLine(FormatSpeciesLine(record, display));
        }
    }

    private string FormatHeading(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return "Unassigned";
        }

        var rules = _legacyRules.Get(raw);
        if (!string.IsNullOrWhiteSpace(rules?.CommonPlural)) {
            return $"{Uppercase(rules!.CommonPlural)} ({raw})";
        }

        if (!string.IsNullOrWhiteSpace(rules?.CommonName)) {
            return $"{Uppercase(rules!.CommonName)} ({raw})";
        }

        if (!string.IsNullOrWhiteSpace(rules?.Wikilink)) {
            return $"[[{rules!.Wikilink}|{raw}]]";
        }

        return raw;
    }

    private string FormatSpeciesLine(IucnSpeciesRecord record, DisplayPreferences display) {
        var descriptor = IucnRedlistStatus.Describe(record.StatusCode);
        var builder = new StringBuilder();
        builder.Append("* ");
        if (display.IncludeStatusTemplate && !string.IsNullOrWhiteSpace(descriptor.TemplateName)) {
            builder.Append("{{");
            builder.Append(descriptor.TemplateName);
            builder.Append("}} ");
        }

        builder.Append(BuildNameFragment(record, display));

        if (display.IncludeStatusLabel && !string.IsNullOrWhiteSpace(descriptor.Label)) {
            builder.Append(" – ");
            builder.Append(descriptor.Label);
        }

        if (!string.IsNullOrWhiteSpace(record.SubpopulationName)) {
            builder.Append(" (subpopulation: ");
            builder.Append(record.SubpopulationName);
            builder.Append(')');
        }

        return builder.ToString();
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
        var taxaRules = _legacyRules.Get(record.ScientificNameTaxonomy ?? record.ScientificNameAssessments ?? string.Empty);
        if (!string.IsNullOrWhiteSpace(taxaRules?.CommonName)) {
            return Uppercase(taxaRules!.CommonName);
        }

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
        }

        public WikipediaSectionDefinition Definition { get; }
        public HashSet<string> StatusSet { get; }
        public List<IucnSpeciesRecord> Records { get; } = new();
    }
}

internal sealed record WikipediaListResult(string OutputPath, int TotalEntries, string DatasetVersion);

internal sealed class TemplateContext {
    public string Title { get; init; } = string.Empty;
    public string? Description { get; init; }
    public string ScopeLabel { get; init; } = string.Empty;
    public string DatasetVersion { get; init; } = string.Empty;
    public string GeneratedAt { get; init; } = string.Empty;
    public int TotalEntries { get; init; }
    public string SectionsSummary { get; init; } = string.Empty;
}

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
