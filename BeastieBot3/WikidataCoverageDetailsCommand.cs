using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class WikidataCoverageDetailsCommand : AsyncCommand<WikidataCoverageReportSettings> {
    public override Task<int> ExecuteAsync(CommandContext context, WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(WikidataCoverageReportSettings settings, CancellationToken cancellationToken) {
        var exitCode = WikidataCoverageAnalysis.TryExecute(settings, cancellationToken, out var analysisResult);
        if (exitCode != 0 || analysisResult is null) {
            return exitCode;
        }

        RenderDetails(analysisResult, settings);
        return 0;
    }

    private static void RenderDetails(WikidataCoverageAnalysisResult result, WikidataCoverageReportSettings settings) {
        var stats = result.Stats;
        var (synonymPath, unmatchedPath) = ResolveOutputPaths(settings);

        using var commonNames = new CommonNameProvider(result.WikidataDatabasePath, result.IucnApiCachePath);
        var synonymMarkdown = BuildSynonymDocument(result, commonNames);
        var unmatchedMarkdown = BuildUnmatchedDocument(result, commonNames);

        WriteMarkdownFile(synonymPath, synonymMarkdown);
        WriteMarkdownFile(unmatchedPath, unmatchedMarkdown);

        AnsiConsole.MarkupLine($"[grey]IUCN DB:[/] {Markup.Escape(result.IucnDatabasePath)}");
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(result.WikidataDatabasePath)}");
        AnsiConsole.MarkupLine($"[grey]Rows considered:[/] {stats.Total:N0}");
        AnsiConsole.MarkupLine($"[green]Synonym matches saved to:[/] {Markup.Escape(synonymPath)}");
        AnsiConsole.MarkupLine($"[green]Unmatched taxa saved to:[/] {Markup.Escape(unmatchedPath)}");
    }

    private static string BuildSynonymDocument(WikidataCoverageAnalysisResult result, CommonNameProvider commonNameProvider) {
        var builder = CreateDocumentBuilder(result);
        RenderSynonymSection(result.Stats, builder, commonNameProvider);
        return builder.ToString();
    }

    private static string BuildUnmatchedDocument(WikidataCoverageAnalysisResult result, CommonNameProvider commonNameProvider) {
        var builder = CreateDocumentBuilder(result);
        RenderUnmatchedSection(result.Stats, builder, commonNameProvider);
        return builder.ToString();
    }

    private static MarkdownDocumentBuilder CreateDocumentBuilder(WikidataCoverageAnalysisResult result) {
        var builder = new MarkdownDocumentBuilder();
        builder.AppendLine($"IUCN DB: {result.IucnDatabasePath}");
        builder.AppendLine($"Wikidata cache: {result.WikidataDatabasePath}");
        builder.AppendLine($"Rows considered: {result.Stats.Total:N0}");
        builder.AppendLine();
        return builder;
    }

    private static void RenderSynonymSection(CoverageStats stats, MarkdownDocumentBuilder builder, CommonNameProvider commonNameProvider) {
        var total = stats.SynonymDetails.Count;
        builder.WriteHeading(1, $"Synonym matches ({total})");
        if (total == 0) {
            builder.AppendLine("_No synonym-only matches were found._");
            builder.AppendLine();
            return;
        }

        var levels = new List<TaxonomyTreeLevel<SynonymCoverageItem>> {
            new("Kingdom", item => item.Row.KingdomName, AlwaysDisplay: true, UnknownLabel: "Unknown kingdom"),
            new("Phylum", item => item.Row.PhylumName),
            new("Class", item => item.Row.ClassName),
            new("Order", item => item.Row.OrderName),
            new("Family", item => item.Row.FamilyName),
            new("Genus", item => item.Row.GenusName)
        };

        var tree = TaxonomyTreeBuilder.Build(stats.SynonymDetails, levels);
        RenderTaxonomyTree(tree, level: 2,
            item => FormatSynonymLeaf(item, commonNameProvider),
            item => item.Row.ScientificNameTaxonomy ?? item.Row.ScientificNameAssessments ?? item.Row.SpeciesName ?? item.Candidate.Name,
            builder);
        builder.AppendLine();
    }

    private static void RenderUnmatchedSection(CoverageStats stats, MarkdownDocumentBuilder builder, CommonNameProvider commonNameProvider) {
        var total = stats.UnmatchedDetails.Count;
        builder.WriteHeading(1, $"Unmatched taxa ({total})");
        if (total == 0) {
            builder.AppendLine("_All taxa were matched via P627 or scientific names._");
            builder.AppendLine();
            return;
        }

        var levels = new List<TaxonomyTreeLevel<IucnTaxonomyRow>> {
            new("Kingdom", row => row.KingdomName, AlwaysDisplay: true, UnknownLabel: "Unknown kingdom"),
            new("Phylum", row => row.PhylumName),
            new("Class", row => row.ClassName),
            new("Order", row => row.OrderName),
            new("Family", row => row.FamilyName),
            new("Genus", row => row.GenusName)
        };

        var tree = TaxonomyTreeBuilder.Build(stats.UnmatchedDetails, levels);
        RenderTaxonomyTree(tree, level: 2,
            row => FormatUnmatchedLeaf(row, commonNameProvider),
            row => row.ScientificNameTaxonomy ?? row.ScientificNameAssessments ?? row.SpeciesName,
            builder);
        builder.AppendLine();
    }

    private static void RenderTaxonomyTree<T>(TaxonomyTreeNode<T> node, int level, Func<T, string> formatItem, Func<T, string?>? sortKey, MarkdownDocumentBuilder builder) {
        foreach (var child in node.Children) {
            var label = child.Label is null ? child.Value ?? "" : $"{child.Label}: {child.Value}";
            var heading = string.IsNullOrWhiteSpace(label) ? "(unspecified)" : label.Trim();
            builder.WriteHeading(level, $"{heading} [{child.ItemCount}]");
            RenderTaxonomyTree(child, level + 1, formatItem, sortKey, builder);
        }

        if (node.Items.Count == 0) {
            return;
        }

        var orderedItems = sortKey is null
            ? node.Items
            : node.Items.OrderBy(item => sortKey(item) ?? string.Empty, StringComparer.OrdinalIgnoreCase).ToList();

        foreach (var item in orderedItems) {
            builder.WriteListItem(level, formatItem(item));
        }
    }

    private static string FormatSynonymLeaf(SynonymCoverageItem item, CommonNameProvider commonNameProvider) {
        var displayName = item.Row.ScientificNameTaxonomy
            ?? item.Row.ScientificNameAssessments
            ?? ScientificNameHelper.BuildFromParts(item.Row.GenusName, item.Row.SpeciesName, item.Row.InfraName)
            ?? item.Candidate.Name;

        var pieces = new List<string> {
            $"**{EscapeMarkdown(displayName)}**",
            $"synonym: *{EscapeMarkdown(item.Candidate.Name)}* ({item.Candidate.Source})"
        };

        var commonName = commonNameProvider.GetBestCommonName(item.Row, item.EntityIds, new[] { item.Candidate.Name });
        if (!string.IsNullOrWhiteSpace(commonName)) {
            pieces.Add($"common name: {EscapeMarkdown(commonName)}");
        }

        var qids = FormatQidLinks(item.EntityIds);
        if (!string.IsNullOrWhiteSpace(qids)) {
            pieces.Add($"Wikidata: {qids}");
        }

        var iucnUrl = BuildIucnAssessmentUrl(item.Row);
        if (!string.IsNullOrWhiteSpace(item.Row.RedlistVersion)) {
            pieces.Add($"Red List {EscapeMarkdown(item.Row.RedlistVersion)}");
        }
        if (!string.IsNullOrWhiteSpace(iucnUrl)) {
            pieces.Add(iucnUrl);
        }
        return string.Join(" - ", pieces);
    }

    private static string FormatUnmatchedLeaf(IucnTaxonomyRow row, CommonNameProvider commonNameProvider) {
        var displayName = row.ScientificNameTaxonomy
            ?? row.ScientificNameAssessments
            ?? ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName)
            ?? row.InternalTaxonId
            ?? "Unknown taxon";

        var pieces = new List<string> { $"**{EscapeMarkdown(displayName)}**" };

        var commonName = commonNameProvider.GetBestCommonName(row, null);
        if (!string.IsNullOrWhiteSpace(commonName)) {
            pieces.Add($"common name: {EscapeMarkdown(commonName)}");
        }

        var link = BuildIucnAssessmentUrl(row);
        if (!string.IsNullOrWhiteSpace(row.InternalTaxonId)) {
            pieces.Add($"ID {EscapeMarkdown(row.InternalTaxonId)}");
        }
        if (!string.IsNullOrWhiteSpace(row.RedlistVersion)) {
            pieces.Add($"Red List {EscapeMarkdown(row.RedlistVersion)}");
        }
        if (!string.IsNullOrWhiteSpace(link)) {
            pieces.Add(link);
        }
        return string.Join(" - ", pieces);
    }

    private static string FormatQidLinks(IReadOnlyList<string> entityIds) {
        if (entityIds.Count == 0) {
            return string.Empty;
        }

        var parts = new List<string>(entityIds.Count);
        foreach (var id in entityIds) {
            if (string.IsNullOrWhiteSpace(id)) {
                continue;
            }

            var trimmed = id.Trim();
            var url = $"https://www.wikidata.org/wiki/{trimmed}";
            parts.Add($"[{EscapeMarkdown(trimmed)}]({url})");
        }

        return string.Join(", ", parts);
    }

    private static string BuildIucnAssessmentUrl(IucnTaxonomyRow row) {
        var speciesId = row.InternalTaxonId?.Trim() ?? string.Empty;
        var assessmentId = row.AssessmentId?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(speciesId) || string.IsNullOrWhiteSpace(assessmentId)) {
            return string.Empty;
        }

        return $"https://www.iucnredlist.org/species/{speciesId}/{assessmentId}";
    }

    private static (string SynonymPath, string UnmatchedPath) ResolveOutputPaths(WikidataCoverageReportSettings settings) {
        var baseDirectory = string.IsNullOrWhiteSpace(settings.OutputDirectory)
            ? Environment.CurrentDirectory
            : settings.OutputDirectory;
        baseDirectory = Path.GetFullPath(baseDirectory);
        var timestamp = DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss");

        var synonymPath = !string.IsNullOrWhiteSpace(settings.SynonymOutputPath)
            ? Path.GetFullPath(settings.SynonymOutputPath)
            : Path.Combine(baseDirectory, $"wikidata-coverage-synonyms-{timestamp}.md");

        var unmatchedPath = !string.IsNullOrWhiteSpace(settings.UnmatchedOutputPath)
            ? Path.GetFullPath(settings.UnmatchedOutputPath)
            : Path.Combine(baseDirectory, $"wikidata-coverage-unmatched-{timestamp}.md");

        return (synonymPath, unmatchedPath);
    }

    private static void WriteMarkdownFile(string path, string content) {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(path, content, Encoding.UTF8);
    }

    private static string EscapeMarkdown(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return string.Empty;
        }

        var builder = new StringBuilder(value.Length * 2);
        foreach (var ch in value) {
            if (ch is '[' or ']' or '(' or ')' or '*' or '_' or '`') {
                builder.Append('\\');
            }
            builder.Append(ch);
        }

        return builder.ToString();
    }

    private sealed class MarkdownDocumentBuilder {
        private readonly StringBuilder _builder = new();

        public void AppendLine(string line = "") => _builder.AppendLine(line);

        public void WriteHeading(int level, string text) {
            var headingLevel = Math.Clamp(level, 1, 6);
            var prefix = new string('#', headingLevel);
            _builder.Append(prefix).Append(' ').AppendLine(text);
        }

        public void WriteListItem(int level, string text) {
            var indent = level > 1 ? " " : string.Empty;
            _builder.Append(indent).Append("- ").AppendLine(text);
        }

        public override string ToString() => _builder.ToString();
    }
}
