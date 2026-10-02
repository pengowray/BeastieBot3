using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;
using static BeastieBot3.Tests.ListTreeFixtures;

namespace BeastieBot3.Tests;

// Pins the wikitext the section renderer writes from the tree: heading text for CoL nodes shown by
// name only, virtual-group headings with {{main}}, a node's own items before its child headings,
// the "Other {parent}" heading, and no heading deeper than H6.
public sealed class SectionRenderingTests : IDisposable {
    private readonly string _rulesPath = Path.GetTempFileName();

    public void Dispose() => File.Delete(_rulesPath);

    private static TaxonRulesService Rules() => new(new TaxonRulesConfig {
        Taxa = new Dictionary<string, TaxonRule> {
            ["Cetacea"] = new() { CommonPlural = "cetaceans" },
            ["Artiodactyla"] = new() { UseVirtualGroups = true },
        },
        VirtualGroups = new Dictionary<string, VirtualGroupConfig> {
            ["Artiodactyla"] = new() {
                Groups = new List<VirtualGroup> {
                    new() { Name = "Cetaceans", CommonPlural = "cetaceans", MainArticle = "Cetacea", Clades = new() { "Cetacea" } },
                    new() { Name = "Even-toed ungulates", CommonPlural = "even-toed ungulates", MainArticle = "Even-toed ungulate", Default = true },
                },
            },
        },
    });

    private HeadingFormatter Headings(TaxonRulesService? rules) => new(new LegacyTaxaRuleList(_rulesPath), rules, storeBackedProvider: null);

    private SectionBodyRenderer Renderer(ITaxonPlacement? placement, TaxonRulesService? rules) => new(
        placement, rules,
        new SpeciesLineFormatter(new LegacyTaxaRuleList(_rulesPath), storeBackedProvider: null, commonNameProvider: null),
        Headings(rules));

    private static readonly DisplayPreferences Display = new() { ListingStyle = ListingStyle.ScientificNameFocus };

    private static readonly SectionTreeOptions Layers = new() { IntermediateGroups = new IntermediateGroupsConfig() };

    private static string[] Lines(string body) => body.Split('\n').Select(l => l.TrimEnd('\r')).ToArray();

    [Fact]
    public void NameOnlyHeading_HasNoRankWord() {
        var heading = Headings(Rules()).FormatHeading("Cetacea", "order", kingdom: null, showRank: false);

        Assert.Equal("Cetacea", heading.Text);
        Assert.Equal("Members of [[Cetacea]] are called cetaceans.", heading.CommonNameSentence);
    }

    [Fact]
    public void RankedHeading_KeepsTheRankWord() {
        var heading = Headings(Rules()).FormatHeading("CETACEA", "order", kingdom: null);

        Assert.Equal("Order Cetacea", heading.Text);
        Assert.Equal("Members of the [[Cetacea]] order are called cetaceans.", heading.CommonNameSentence);
    }

    [Fact]
    public void CoLLayersAndVirtualGroups_RenderWithinH6() {
        var records = ArtiodactylaRecords();
        records.AddRange(Family(Mammalia, Artiodactyla, "MONODONTIDAE", 4));
        var placement = new TaxonPlacementIndex(ArtiodactylaPlacement().Paths.Append(
            OrderToFamily(Mammalia, Artiodactyla, "MONODONTIDAE", NameOnly("Cetacea", "order"), Suborder("Odontoceti"))));

        var (body, headingCount) = Renderer(placement, Rules())
            .BuildSectionBody(records, DefaultGrouping(), Display, "LC", tree: Layers);
        var lines = Lines(body);

        Assert.Contains("=== Order Artiodactyla ===", lines);
        Assert.Contains("==== Cetaceans ====", lines);
        Assert.Contains("{{main|Cetacea}}", lines);
        Assert.Contains("===== Suborder Odontoceti =====", lines);
        Assert.Contains("====== Family Delphinidae ======", lines);
        Assert.Contains("===== Family Bovidae =====", lines);
        Assert.DoesNotContain(lines, l => l.StartsWith("=======", StringComparison.Ordinal));
        Assert.Equal(lines.Count(l => l.StartsWith("=", StringComparison.Ordinal)), headingCount);
    }

    [Fact]
    public void NodeItems_RenderBeforeChildHeadings() {
        var grouping = new[] {
            new GroupingLevelDefinition { Level = "order", Label = "Order" },
            new GroupingLevelDefinition { Level = "family", Label = "Family", UnknownLabel = "Other families" },
        };
        var records = Family(Reptilia, "TESTUDINES", "TESTUDINIDAE", 3)
            .Concat(Family(Reptilia, "TESTUDINES", "EMYDIDAE", 3))
            .Append(Rec(Reptilia, "TESTUDINES", "", "Lonely", "turtle"))
            .Concat(Family(Reptilia, "CROCODYLIA", "CROCODYLIDAE", 2))
            .ToList();

        var (body, _) = Renderer(placement: null, rules: null)
            .BuildSectionBody(records, grouping, Display, "LC");
        var lines = Lines(body).ToList();

        var order = lines.IndexOf("=== Order Testudines ===");
        var lonely = lines.FindIndex(l => l.Contains("Lonely turtle", StringComparison.Ordinal));
        var firstFamily = lines.IndexOf("==== Family Emydidae ====");
        Assert.True(order >= 0 && lonely > order && firstFamily > lonely, body);
    }

    [Fact]
    public void FamilyOtherBucket_IsNamedAfterItsOrder_AndPluralOnASingleOrderPage() {
        var records = Family(Reptilia, "TESTUDINES", "TESTUDINIDAE", 6)
            .Concat(Family(Reptilia, "TESTUDINES", "EMYDIDAE", 1))
            .Concat(Family(Reptilia, "TESTUDINES", "CHELIDAE", 1))
            .Concat(Family(Reptilia, "TESTUDINES", "PELOMEDUSIDAE", 1))
            .ToList();
        var crocodiles = Family(Reptilia, "CROCODYLIA", "CROCODYLIDAE", 2);

        var (twoOrders, _) = Renderer(placement: null, rules: null)
            .BuildSectionBody(records.Concat(crocodiles).ToList(), DefaultGrouping(), Display, "LC");
        var (oneOrder, _) = Renderer(placement: null, rules: null)
            .BuildSectionBody(records, DefaultGrouping(), Display, "LC");

        Assert.Contains("==== Other Testudines ====", Lines(twoOrders));
        Assert.Contains("=== Other families ===", Lines(oneOrder));
    }
}
