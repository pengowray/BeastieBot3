using BeastieBot3.Audit;
using BeastieBot3.Audit.Model;
using Xunit;

namespace BeastieBot3.Tests;

// Pins the friendly group vocabulary the audit tables show in place of raw Class and Family:
// the coarse tier, the order-level detail, and what happens when the ladder is missing or new.
public class TaxonGroupTests {
    private static string Label(string? kingdom, string? cls, string? order = null) =>
        TaxonGroups.For(kingdom, null, cls, order).Label;

    [Theory]
    [InlineData("ANIMALIA", "AVES", "Birds")]
    [InlineData("ANIMALIA", "REPTILIA", "Reptiles")]
    [InlineData("ANIMALIA", "CHONDRICHTHYES", "Fish: Sharks and rays")]
    [InlineData("PLANTAE", "CYCADOPSIDA", "Plants: Cycads")]
    [InlineData("PLANTAE", "MAGNOLIOPSIDA", "Plants: Dicotyledons")]
    [InlineData("ANIMALIA", "CLITELLATA", "Other invertebrates: Earthworms and leeches")]
    public void ClassAloneGivesTheGroup(string kingdom, string cls, string expected) =>
        Assert.Equal(expected, Label(kingdom, cls));

    [Theory]
    [InlineData("MAMMALIA", "CHIROPTERA", "Mammals: Bats")]
    [InlineData("MAMMALIA", "RODENTIA", "Mammals: Rodents")]
    [InlineData("AMPHIBIA", "GYMNOPHIONA", "Amphibians: Caecilians")]
    [InlineData("INSECTA", "LEPIDOPTERA", "Insects: Butterflies and moths")]
    public void CuratedOrdersAddDetail(string cls, string order, string expected) =>
        Assert.Equal(expected, Label("ANIMALIA", cls, order));

    [Fact]
    public void AnUncuratedOrderLeavesTheClassLabelAlone() =>
        Assert.Equal("Birds", Label("ANIMALIA", "AVES", "PASSERIFORMES"));

    [Fact]
    public void ClassLookupIsCaseInsensitive() =>
        Assert.Equal("Mammals: Bats", Label("Animalia", "Mammalia", "Chiroptera"));

    // "NOT ASSIGNED" is a real value in the export and means missing; it must never read as a name.
    [Theory]
    [InlineData("NOT ASSIGNED")]
    [InlineData("")]
    [InlineData(null)]
    public void MissingClassFallsBackToTheKingdom(string? cls) =>
        Assert.Equal("Fungi", Label("FUNGI", cls));

    [Fact]
    public void MissingOrderIsIgnoredRatherThanShown() =>
        Assert.Equal("Mammals", Label("ANIMALIA", "MAMMALIA", "NOT ASSIGNED"));

    // A class added in a later release still lands in the right kingdom and keeps its own name.
    [Fact]
    public void UnknownClassKeepsItsNameUnderTheKingdom() =>
        Assert.Equal("Plants: Newopsida", Label("PLANTAE", "NEWOPSIDA"));

    [Fact]
    public void NothingAtAllDescribesTheRecord() =>
        Assert.Equal(TaxonGroups.NoTaxonomy, Label(null, null));

    // An unrecognised chordate class is a vertebrate; it must not fall into an invertebrate bucket.
    [Fact]
    public void UnknownChordateClassStaysOutOfTheInvertebrates() =>
        Assert.Equal("Other animals: Cephalaspidomorphus",
            TaxonGroups.For("ANIMALIA", "CHORDATA", "CEPHALASPIDOMORPHUS", null).Label);

    [Fact]
    public void UnknownNonChordateAnimalClassIsAnInvertebrate() =>
        Assert.Equal("Other invertebrates: Newozoa",
            TaxonGroups.For("ANIMALIA", "NEWOPHORA", "NEWOZOA", null).Label);

    // Only "NOT ASSIGNED" is hidden: a real kingdom name is a name.
    [Fact]
    public void UnrecognisedKingdomKeepsItsName() =>
        Assert.Equal("Protozoa: Ciliophora", TaxonGroups.For("PROTOZOA", null, "CILIOPHORA", null).Label);

    [Theory]
    [InlineData("THEOCOSTRACA")]
    [InlineData("THECOSTRACA")]
    public void BothSpellingsOfThecostracaAreBarnacles(string cls) =>
        Assert.Equal("Crustaceans: Barnacles", Label("ANIMALIA", cls));

    [Fact]
    public void TheRetiredLampreyClassStillResolves() =>
        Assert.Equal("Fish: Lampreys", Label("ANIMALIA", "CEPHALASPIDOMORPHI"));

    [Fact]
    public void CountsUseTheCoarseTierAndListTheBiggestFirst() {
        var findings = new[] {
            Finding("ANIMALIA", "MAMMALIA", "CHIROPTERA"),
            Finding("ANIMALIA", "MAMMALIA", "RODENTIA"),
            Finding("ANIMALIA", "MAMMALIA", null),
            Finding("ANIMALIA", "REPTILIA", null),
            Finding("PLANTAE", "CYCADOPSIDA", null),
        };
        Assert.Equal("Mammals (3), Reptiles (1), Plants (1)", TaxonGroups.CountLine(findings));
    }

    // A bare group label means "not one of the labelled orders", so it belongs after them.
    [Fact]
    public void TheResidualSortsAfterTheLabelledOrders() {
        var bats = TaxonGroups.SortKey(Finding("ANIMALIA", "MAMMALIA", "CHIROPTERA"));
        var other = TaxonGroups.SortKey(Finding("ANIMALIA", "MAMMALIA", "CARNIVORA"));
        Assert.True(string.CompareOrdinal(bats, other) < 0);
    }

    [Fact]
    public void SortKeyKeepsOneGroupTogether() {
        var hagfish = TaxonGroups.SortKey(Finding("ANIMALIA", "MYXINI", null));
        var lamprey = TaxonGroups.SortKey(Finding("ANIMALIA", "PETROMYZONTI", null));
        var insect = TaxonGroups.SortKey(Finding("ANIMALIA", "INSECTA", null));
        Assert.True(string.CompareOrdinal(hagfish, insect) < 0);
        Assert.True(string.CompareOrdinal(lamprey, insect) < 0);
    }

    private static AuditFinding Finding(string? kingdom, string? cls, string? order) =>
        new() { Kingdom = kingdom, Class = cls, Order = order };
}
