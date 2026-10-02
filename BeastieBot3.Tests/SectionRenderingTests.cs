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

    [Fact]
    public void FamiliesAllMergedIntoOneBucket_StillNameEachSpeciesFamily() {
        // Every Carnivora family is below min_items, so all go into one "Other" bucket, which gets no
        // heading because it is the only group. Each species still shows its family.
        var records = Family(Mammalia, "CARNIVORA", "FELIDAE", 2)
            .Concat(Family(Mammalia, "CARNIVORA", "CANIDAE", 2))
            .Concat(Family(Mammalia, "CARNIVORA", "URSIDAE", 1))
            .Concat(Family(Mammalia, "PRIMATES", "HOMINIDAE", 6))
            .ToList();
        var display = new DisplayPreferences { ListingStyle = ListingStyle.ScientificNameFocus, IncludeFamilyInOtherBucket = true };

        var (body, _) = Renderer(placement: null, rules: null).BuildSectionBody(records, DefaultGrouping(), display, "LC");
        var lines = Lines(body);

        var felid = Assert.Single(lines, l => l.Contains("Felus species1", StringComparison.Ordinal));
        Assert.Contains("Family", felid);
        Assert.Contains("Felidae", felid);
        Assert.DoesNotContain(lines, l => l.Contains("Other Carnivora", StringComparison.Ordinal));
    }

    [Fact]
    public void NotAssignedOrderAndFamily_GetNoHeading_AndAreNotLumpedIntoOther() {
        // IUCN's "NOT ASSIGNED" values with no rule: no "Order Not assigned" or "Family Not assigned"
        // heading, and the species are not merged into an "Other" bucket with real small families.
        var records = Family("ACTINOPTERYGII", "NOT ASSIGNED", "POMACENTRIDAE", 6)
            .Concat(Family("ACTINOPTERYGII", "CYPRINIFORMES", "CYPRINIDAE", 6))
            .Concat(Family("ACTINOPTERYGII", "CYPRINIFORMES", "NOT ASSIGNED", 2))
            .Concat(Family("ACTINOPTERYGII", "CYPRINIFORMES", "BALITORIDAE", 1))
            .Concat(Family("ACTINOPTERYGII", "CYPRINIFORMES", "COBITIDAE", 1))
            .ToList();
        var display = new DisplayPreferences { ListingStyle = ListingStyle.ScientificNameFocus, IncludeFamilyInOtherBucket = true };

        var (body, _) = Renderer(placement: null, rules: null).BuildSectionBody(records, DefaultGrouping(), display, "LC");
        var lines = Lines(body);

        Assert.True(lines.Contains("=== Family Pomacentridae ==="), body);
        Assert.DoesNotContain(lines, l => l.StartsWith("=", StringComparison.Ordinal) && l.Contains("assigned", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("=== Family Pomacentridae ===", lines);
        Assert.Contains(lines, l => l.Contains("Not assigned species1", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(lines, l => l.Contains("Family: Not", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void SmallFamiliesPlusNotAssigned_GetNoOtherHeading_AndKeepFamilyNotes() {
        // Every real Russulales family is small, and some species have no IUCN family: no lone
        // "Other Russulales" heading, the species sit under the order with their family named.
        var records = Family("AGARICOMYCETES", "RUSSULALES", "ALBATRELLACEAE", 1)
            .Concat(Family("AGARICOMYCETES", "RUSSULALES", "STEREACEAE", 1))
            .Concat(Family("AGARICOMYCETES", "RUSSULALES", "NOT ASSIGNED", 2))
            .Concat(Family("AGARICOMYCETES", "AGARICALES", "AGARICACEAE", 6))
            .ToList();
        var display = new DisplayPreferences { ListingStyle = ListingStyle.ScientificNameFocus, IncludeFamilyInOtherBucket = true };

        var (body, _) = Renderer(placement: null, rules: null).BuildSectionBody(records, DefaultGrouping(), display, "LC");
        var lines = Lines(body);

        Assert.False(lines.Any(l => l.StartsWith("=", StringComparison.Ordinal) && l.Contains("Other", StringComparison.Ordinal)), body);
        Assert.Contains(lines, l => l.Contains("Albatrellaceae species1", StringComparison.Ordinal) && l.Contains("Family:", StringComparison.Ordinal));
        Assert.DoesNotContain(lines, l => l.Contains("assigned", StringComparison.OrdinalIgnoreCase) && l.Contains("Family:", StringComparison.Ordinal));
    }

    [Fact]
    public void FamiliesWithNoIucnOrder_EachGetAHeading_NotAnOtherBucketAmongTheOrders() {
        var records = Family("LECANOROMYCETES", "NOT ASSIGNED", "SARRAMEANACEAE", 1)
            .Concat(Family("LECANOROMYCETES", "NOT ASSIGNED", "TRAPELIACEAE", 1))
            .Concat(Family("LECANOROMYCETES", "NOT ASSIGNED", "MYCOPORACEAE", 1))
            .Concat(Family("LECANOROMYCETES", "LECANORALES", "LECANORACEAE", 6))
            .Concat(Family("LECANOROMYCETES", "PELTIGERALES", "PELTIGERACEAE", 6))
            .ToList();

        var (body, _) = Renderer(placement: null, rules: null).BuildSectionBody(records, DefaultGrouping(), Display, "LC");
        var lines = Lines(body);

        Assert.True(lines.Contains("=== Family Trapeliaceae ==="), body);
        Assert.DoesNotContain(lines, l => l.StartsWith("=", StringComparison.Ordinal) && l.Contains("Other", StringComparison.Ordinal));
    }
}
