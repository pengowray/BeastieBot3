using System;
using System.Collections.Generic;
using System.Linq;
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

        RenderDetails(analysisResult);
        return 0;
    }

    private static void RenderDetails(WikidataCoverageAnalysisResult result) {
        var stats = result.Stats;
        AnsiConsole.MarkupLine($"[grey]IUCN DB:[/] {Markup.Escape(result.IucnDatabasePath)}");
        AnsiConsole.MarkupLine($"[grey]Wikidata cache:[/] {Markup.Escape(result.WikidataDatabasePath)}");
        AnsiConsole.MarkupLine($"[grey]Rows considered:[/] {stats.Total}");
        AnsiConsole.WriteLine();

        RenderSynonymSection(stats);
        AnsiConsole.WriteLine();
        RenderUnmatchedSection(stats);
    }

    private static void RenderSynonymSection(CoverageStats stats) {
        var total = stats.SynonymDetails.Count;
        WriteSectionHeading($"Synonym matches ({total})", level: 1);
        if (total == 0) {
            AnsiConsole.MarkupLine("[grey]No synonym-only matches were found.[/]");
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
        RenderTaxonomyTree(tree, level: 2, FormatSynonymLeaf, item => item.Row.ScientificNameTaxonomy ?? item.Row.ScientificNameAssessments ?? item.Row.SpeciesName ?? item.Candidate.Name);
    }

    private static void RenderUnmatchedSection(CoverageStats stats) {
        var total = stats.UnmatchedDetails.Count;
        WriteSectionHeading($"Unmatched taxa ({total})", level: 1);
        if (total == 0) {
            AnsiConsole.MarkupLine("[grey]All taxa were matched via P627 or scientific names.[/]");
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
        RenderTaxonomyTree(tree, level: 2, FormatUnmatchedLeaf, row => row.ScientificNameTaxonomy ?? row.ScientificNameAssessments ?? row.SpeciesName);
    }

    private static void RenderTaxonomyTree<T>(TaxonomyTreeNode<T> node, int level, Func<T, string> formatItem, Func<T, string?>? sortKey) {
        foreach (var child in node.Children) {
            var label = child.Label is null ? child.Value ?? "" : $"{child.Label}: {child.Value}";
            var heading = string.IsNullOrWhiteSpace(label) ? "(unspecified)" : label.Trim();
            WriteSectionHeading($"{heading} [{child.ItemCount}]", level);
            RenderTaxonomyTree(child, level + 1, formatItem, sortKey);
        }

        if (node.Items.Count == 0) {
            return;
        }

        var orderedItems = sortKey is null
            ? node.Items
            : node.Items.OrderBy(item => sortKey(item) ?? string.Empty, StringComparer.OrdinalIgnoreCase).ToList();

        var indent = new string(' ', Math.Clamp(level, 1, 6) * 2);
        foreach (var item in orderedItems) {
            AnsiConsole.MarkupLine($"{indent}- {formatItem(item)}");
        }
    }

    private static string FormatSynonymLeaf(SynonymCoverageItem item) {
        var displayName = item.Row.ScientificNameTaxonomy
            ?? item.Row.ScientificNameAssessments
            ?? ScientificNameHelper.BuildFromParts(item.Row.GenusName, item.Row.SpeciesName, item.Row.InfraName)
            ?? item.Candidate.Name;

        var pieces = new List<string> {
            $"[green]{Markup.Escape(displayName)}[/]",
            $"synonym: [italic]{Markup.Escape(item.Candidate.Name)}[/] ({item.Candidate.Source})"
        };

        var qids = FormatQidLinks(item.EntityIds);
        if (!string.IsNullOrWhiteSpace(qids)) {
            pieces.Add($"QIDs: {qids}");
        }

        pieces.Add($"[link={Markup.Escape(BuildIucnAssessmentUrl(item.Row))}]IUCN {Markup.Escape(item.Row.RedlistVersion)}[/]");
        return string.Join(" - ", pieces);
    }

    private static string FormatUnmatchedLeaf(IucnTaxonomyRow row) {
        var displayName = row.ScientificNameTaxonomy
            ?? row.ScientificNameAssessments
            ?? ScientificNameHelper.BuildFromParts(row.GenusName, row.SpeciesName, row.InfraName)
            ?? row.InternalTaxonId
            ?? "Unknown taxon";

        var link = BuildIucnAssessmentUrl(row);
        var id = row.InternalTaxonId ?? "?";
        var version = row.RedlistVersion ?? "?";
        return $"[yellow]{Markup.Escape(displayName)}[/] (ID {Markup.Escape(id)}, {Markup.Escape(version)}) - [link={Markup.Escape(link)}]IUCN assessment[/]";
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
            var url = $"https://www.wikidata.org/wiki/{Markup.Escape(trimmed)}";
            parts.Add($"[link={url}]{Markup.Escape(trimmed)}[/]");
        }

        return string.Join(", ", parts);
    }

    private static string BuildIucnAssessmentUrl(IucnTaxonomyRow row) {
        var speciesId = row.InternalTaxonId?.Trim() ?? string.Empty;
        var assessmentId = row.AssessmentId?.Trim() ?? string.Empty;
        return $"https://www.iucnredlist.org/species/{speciesId}/{assessmentId}";
    }

    private static void WriteSectionHeading(string text, int level) {
        var headingLevel = Math.Clamp(level, 1, 6);
        var prefix = new string('#', headingLevel);
        AnsiConsole.MarkupLine($"[bold]{prefix} {Markup.Escape(text)}[/]");
    }
}
