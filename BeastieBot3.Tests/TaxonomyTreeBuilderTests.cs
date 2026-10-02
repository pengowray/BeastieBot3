using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Taxonomy;

namespace BeastieBot3.Tests;

// Pins the list tree builder's grouping rules: a level with one value gets no heading, small groups
// are lumped into an "Other" bucket that sorts last, a one-item residual bucket is not given a
// heading, and auto-split accepts or rejects a finer rank by its gates.
public class TaxonomyTreeBuilderTests {
    internal sealed record Sp(string Order, string Family, string Genus, string Name);

    private static List<Sp> Family(string order, string family, int count, string? genus = null) =>
        Enumerable.Range(1, count)
            .Select(i => new Sp(order, family, genus ?? family + "us", $"{family}-{i}"))
            .ToList();

    private static TaxonomyTreeLevel<Sp> OrderLevel() => new("Order", s => s.Order);

    private static TaxonomyTreeLevel<Sp> FamilyLevel(int minItems = 1, int minGroupsForOther = 0) =>
        new("Family", s => s.Family, MinItems: minItems, MinGroupsForOther: minGroupsForOther);

    [Fact]
    public void LevelWithOneValue_GetsNoHeading() {
        var items = Family("CARNIVORA", "FELIDAE", 3).Concat(Family("CARNIVORA", "CANIDAE", 2)).ToList();

        var tree = TaxonomyTreeBuilder.Build(items, new[] { OrderLevel(), FamilyLevel() });

        Assert.Equal(new[] { "CANIDAE", "FELIDAE" }, tree.Children.Select(c => c.Value));
        Assert.All(tree.Children, c => Assert.Equal("Family", c.Label));
    }

    [Fact]
    public void AlwaysDisplay_KeepsTheHeadingForOneValue() {
        var items = Family("CARNIVORA", "FELIDAE", 3);
        var levels = new[] { OrderLevel() with { AlwaysDisplay = true }, FamilyLevel() };

        var tree = TaxonomyTreeBuilder.Build(items, levels);

        var order = Assert.Single(tree.Children);
        Assert.Equal("CARNIVORA", order.Value);
        Assert.Equal(3, order.ItemCount);
    }

    [Fact]
    public void SmallGroups_AreLumpedIntoOther_WhichSortsLast() {
        var items = Family("X", "ZETIDAE", 6)
            .Concat(Family("X", "ALPHIDAE", 6))
            .Concat(Family("X", "BETIDAE", 1))
            .Concat(Family("X", "GAMMIDAE", 2))
            .Concat(Family("X", "DELTIDAE", 1))
            .ToList();

        var tree = TaxonomyTreeBuilder.Build(items, new[] { FamilyLevel(minItems: 5, minGroupsForOther: 3) });

        Assert.Equal(new[] { "ALPHIDAE", "ZETIDAE", "Other families" }, tree.Children.Select(c => c.Value));
        Assert.Equal(4, tree.Children[2].ItemCount);
    }

    [Fact]
    public void FewerSmallGroupsThanTheMinimum_JoinAnOtherBucketThatExists_WhichSortsLast() {
        // Two small groups are below MinGroupsForOther = 3, but the blank family already makes an
        // "Other families" bucket, so they join it instead of keeping one- and two-item headings.
        var items = Family("X", "ZETIDAE", 6)
            .Concat(Family("X", "BETIDAE", 1))
            .Concat(Family("X", "GAMMIDAE", 2))
            .Concat(Family("X", "", 6))
            .ToList();
        var level = FamilyLevel(minItems: 5, minGroupsForOther: 3) with { UnknownLabel = "Other families" };

        var tree = TaxonomyTreeBuilder.Build(items, new[] { level });

        Assert.Equal(
            new[] { "ZETIDAE", "Other families" },
            tree.Children.Select(c => c.Value));
        Assert.Equal(9, tree.Children.Single(c => c.Value == "Other families").ItemCount);
    }

    [Fact]
    public void FewerSmallGroupsThanTheMinimum_AreNotLumped_WhenNoOtherBucketExists() {
        var items = Family("X", "ZETIDAE", 6)
            .Concat(Family("X", "BETIDAE", 1))
            .Concat(Family("X", "GAMMIDAE", 2))
            .ToList();
        var level = FamilyLevel(minItems: 5, minGroupsForOther: 3);

        var tree = TaxonomyTreeBuilder.Build(items, new[] { level });

        Assert.Equal(new[] { "BETIDAE", "GAMMIDAE", "ZETIDAE" }, tree.Children.Select(c => c.Value));
    }

    [Fact]
    public void OneItemResidualBucket_IsPlacedOnTheParent() {
        var items = Family("X", "ALPHIDAE", 3)
            .Concat(Family("X", "BETIDAE", 3))
            .Append(new Sp("X", "", "Nullus", "blank-1"))
            .ToList();
        var level = FamilyLevel() with { UnknownLabel = "Other families" };

        var tree = TaxonomyTreeBuilder.Build(items, new[] { level });

        Assert.Equal(new[] { "ALPHIDAE", "BETIDAE" }, tree.Children.Select(c => c.Value));
        Assert.Equal("blank-1", Assert.Single(tree.Items).Name);
    }

    private static AutoSplitOptions<Sp> GenusSplit() => new(
        Threshold: 30,
        MinGroupSize: 10,
        CandidateLevels: new[] {
            new TaxonomyTreeLevel<Sp>("genus", s => s.Genus,
                UnknownLabel: "Other genera", MinItems: 3, OtherLabel: "Other genera", MinGroupsForOther: 3),
        });

    [Fact]
    public void AutoSplit_AcceptsWellSizedGenera() {
        var items = Family("X", "BIGIDAE", 12, genus: "Alpha")
            .Concat(Family("X", "BIGIDAE", 11, genus: "Beta"))
            .Concat(Family("X", "BIGIDAE", 10, genus: "Gamma"))
            .ToList();
        var diagnostics = new AutoSplitDiagnosticCollector();

        var tree = TaxonomyTreeBuilder.Build(items, new[] { FamilyLevel() },
            new TaxonomyTreeOptions<Sp> { AutoSplit = GenusSplit(), Diagnostics = diagnostics });

        Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, tree.Children.Select(c => c.Value));
        Assert.All(tree.Children, c => Assert.Equal("genus", c.Label));
        var accepted = Assert.Single(diagnostics.Decisions, d => d.Outcome == "accepted");
        Assert.Equal("genus", accepted.CandidateRank);
        Assert.Equal(33, accepted.ItemCount);
    }

    [Fact]
    public void AutoSplit_RejectsTinyGenera_AndLeavesOneSection() {
        var items = Enumerable.Range(1, 40)
            .Select(i => new Sp("X", "BIGIDAE", $"Genus{i}", $"sp-{i}"))
            .ToList();
        var diagnostics = new AutoSplitDiagnosticCollector();

        var tree = TaxonomyTreeBuilder.Build(items, new[] { FamilyLevel() },
            new TaxonomyTreeOptions<Sp> { AutoSplit = GenusSplit(), Diagnostics = diagnostics });

        Assert.Empty(tree.Children);
        Assert.Equal(40, tree.Items.Count);
        Assert.DoesNotContain(diagnostics.Decisions, d => d.Outcome == "accepted");
        // Every attempt ends with one record for the whole attempt.
        Assert.Single(diagnostics.Decisions, d => d.CandidateRank == "(all)");
    }

    [Fact]
    public void AutoSplit_NeedsAHeadingLevelLeft() {
        var items = Family("X", "BIGIDAE", 12, genus: "Alpha")
            .Concat(Family("X", "BIGIDAE", 11, genus: "Beta"))
            .Concat(Family("X", "BIGIDAE", 10, genus: "Gamma"))
            .Concat(Family("X", "OTHERIDAE", 3))
            .ToList();
        var diagnostics = new AutoSplitDiagnosticCollector();

        // One heading level: the family headings use it, so the genus split cannot be shown.
        var tree = TaxonomyTreeBuilder.Build(items, new[] { FamilyLevel() },
            new TaxonomyTreeOptions<Sp> { AutoSplit = GenusSplit(), Diagnostics = diagnostics, HeadingLevels = 1 });

        var big = tree.Children.Single(c => c.Value == "BIGIDAE");
        Assert.Empty(big.Children);
        Assert.Equal(33, big.Items.Count);
        Assert.Contains(diagnostics.Decisions, d => d.Outcome == "rejected:heading_depth");
    }
}
