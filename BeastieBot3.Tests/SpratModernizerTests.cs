using System.IO;
using BeastieBot3.Sprat;

namespace BeastieBot3.Tests;

// Pins the TaxonModernizer order-remap logic: simple 1:1 renames, family-conditional splits, and the
// no-op pass-through, loaded from an inline YAML config (independent of the shipped rules file).
public class SpratModernizerTests {
    private static TaxonModernizer LoadFrom(string yaml) {
        var path = Path.Combine(Path.GetTempPath(), $"modernize_{System.Guid.NewGuid():N}.yml");
        File.WriteAllText(path, yaml);
        try {
            return TaxonModernizer.Load(path);
        } finally {
            File.Delete(path);
        }
    }

    private const string Config = """
        orders:
          - { from: Diprotodonta, to: Diprotodontia, kind: typo, epbc_listed_as: Diprotodonta }
          - { from: Pinnipedia, to: Carnivora, kind: structural }
        order_by_family:
          Polyprotodonta:
            Dasyuridae: Dasyuromorphia
            Peramelidae: Peramelemorphia
        """;

    [Fact]
    public void SimpleRename_AppliesAndCarriesProvenance() {
        var m = LoadFrom(Config);

        var diprot = m.ModernizeOrder("Diprotodonta", "Macropodidae");
        Assert.NotNull(diprot);
        Assert.Equal("Diprotodontia", diprot!.To);
        Assert.Equal("typo", diprot.Kind);
        Assert.Equal("Diprotodonta", diprot.EpbcListedAs);

        var pinniped = m.ModernizeOrder("Pinnipedia", "Otariidae");
        Assert.Equal("Carnivora", pinniped!.To);
        Assert.Equal("structural", pinniped.Kind);
        Assert.Null(pinniped.EpbcListedAs);
    }

    [Fact]
    public void FamilyConditionalSplit_RoutesByFamily() {
        var m = LoadFrom(Config);

        Assert.Equal("Dasyuromorphia", m.ModernizeOrder("Polyprotodonta", "Dasyuridae")!.To);
        Assert.Equal("Peramelemorphia", m.ModernizeOrder("Polyprotodonta", "Peramelidae")!.To);
        Assert.Equal("structural", m.ModernizeOrder("Polyprotodonta", "Dasyuridae")!.Kind);
        // An unmapped family under a split order is left unchanged (no rule applies).
        Assert.Null(m.ModernizeOrder("Polyprotodonta", "Macropodidae"));
    }

    [Fact]
    public void OrderNotes_LoadFromConfig() {
        var m = LoadFrom("""
            order_notes:
              Diprotodontia:
                ref_name: afd-diprotodontia
                note: "Follows the [[Australian Faunal Directory]]."
                reference: "{{cite web |title=Order DIPROTODONTIA |url=https://example.org}}"
            """);
        Assert.True(m.OrderNotes.ContainsKey("Diprotodontia"));
        var note = m.OrderNotes["Diprotodontia"];
        Assert.Equal("afd-diprotodontia", note.RefName);
        Assert.Contains("Australian Faunal Directory", note.Note);
        Assert.Contains("cite web", note.Reference);
        // A note missing required fields is skipped.
        Assert.Empty(TaxonModernizer.Empty().OrderNotes);
    }

    [Fact]
    public void NoRule_ReturnsNull() {
        var m = LoadFrom(Config);
        Assert.Null(m.ModernizeOrder("Rodentia", "Muridae"));   // valid order, no rule
        Assert.Null(m.ModernizeOrder("", "Muridae"));            // blank
        Assert.Null(TaxonModernizer.Empty().ModernizeOrder("Diprotodonta", "Macropodidae"));
    }
}
