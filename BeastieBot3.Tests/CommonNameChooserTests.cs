using System;
using System.IO;
using BeastieBot3.CommonNames;
using BeastieBot3.WikipediaLists.Legacy;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins CommonNameChooser, the one place the Wikipedia lists and `site build-db` choose a taxon's
// English common name: rules-list.txt first, then the store's best name (ambiguous names skipped,
// caps rules applied), then the unusable check, which leaves the taxon with no name rather than
// trying the next one.
public sealed class CommonNameChooserTests : IDisposable {
    private readonly string _rulesPath = Path.Combine(Path.GetTempPath(), $"chooser-rules-{Guid.NewGuid():N}.txt");

    public void Dispose() {
        if (File.Exists(_rulesPath)) {
            File.Delete(_rulesPath);
        }
    }

    private static CommonNameStore OpenInMemory() {
        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        return CommonNameStore.OpenFromConnection(conn);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, string sourceId) =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", "ANIMALIA",
            isExtinct: false, isFossil: false, validityStatus: "valid",
            primarySource: "iucn", primarySourceId: sourceId);

    private static void AddName(CommonNameStore store, long taxon, string raw, string source, bool preferred = false) =>
        store.InsertCommonName(taxon, raw, CommonNameNormalizer.NormalizeForMatching(raw)!, "en", source, null, preferred);

    private LegacyTaxaRuleList Rules(params string[] lines) {
        File.WriteAllLines(_rulesPath, lines);
        return new LegacyTaxaRuleList(_rulesPath);
    }

    private static CommonNameSubject Subject(string scientificName) {
        var parts = scientificName.Split(' ');
        return new CommonNameSubject(scientificName, scientificName, parts[0], parts.Length > 1 ? parts[1] : null);
    }

    private static Func<string?> StoreName(CommonNameChooser chooser, CommonNameStore store, long taxon) =>
        () => chooser.FromStore(taxon, CommonNameStore.ToCandidates(store.GetCommonNamesForTaxon(taxon)))?.DisplayName;

    [Fact]
    public void RulesListName_IsUsed_EvenWhenAnotherTaxonKeepsIt() {
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        AddName(store, lion, "Mountain lion", "wikipedia_title", preferred: true);
        AddName(store, cougar, "Mountain lion", "col");
        AddName(store, cougar, "Cougar", "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store, Rules("Puma concolor = mountain lion"));

        var choice = chooser.Choose(Subject("Puma concolor"), StoreName(chooser, store, cougar));

        Assert.Equal(CommonNameChoiceKind.Rules, choice.Kind);
        Assert.Equal("Mountain lion", choice.Name);
    }

    [Fact]
    public void StoreName_SkipsAmbiguousNames_AndIsCapitalised() {
        using var store = OpenInMemory();
        var lion = AddTaxon(store, "panthera leo", "1");
        var cougar = AddTaxon(store, "puma concolor", "2");
        AddName(store, lion, "Mountain lion", "wikipedia_title", preferred: true);
        AddName(store, cougar, "Mountain Lion", "iucn", preferred: true);
        AddName(store, cougar, "Red Tiger", "iucn");
        var chooser = CommonNameChooser.ForStore(store);

        var choice = chooser.Choose(Subject("Puma concolor"), StoreName(chooser, store, cougar));

        Assert.Equal(CommonNameChoiceKind.Store, choice.Kind);
        Assert.Equal("Red tiger", choice.Name);
    }

    [Fact]
    public void UnusableBestName_GivesNoName_WithoutTryingTheNextOne() {
        // The lists show only the scientific name when the best name is the scientific name again.
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "aus bus", "1");
        AddName(store, taxon, "Aus bus", "wikipedia_title", preferred: true);
        AddName(store, taxon, "Bus fish", "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        var choice = chooser.Choose(Subject("Aus bus"), StoreName(chooser, store, taxon));

        Assert.Equal(CommonNameChoiceKind.Unusable, choice.Kind);
        Assert.True(choice.Found);
        Assert.Null(choice.Name);
    }

    [Fact]
    public void NoRulesAndNoStoreName_IsNone() {
        var chooser = CommonNameChooser.RulesOnly(null);

        var choice = chooser.Choose(Subject("Aus bus"), storeName: null);

        Assert.Equal(CommonNameChoiceKind.None, choice.Kind);
        Assert.False(choice.Found);
    }

    [Theory]
    [InlineData("Aus bus sp. nov. 'Kimberley'")]
    [InlineData("Aus spp. complex")]
    [InlineData("Aus bus Smith, 1901")]
    [InlineData("Aus bus non Jones")]
    [InlineData("aus bus")]
    [InlineData("Aus bus (Smith)")]
    [InlineData("  ")]
    public void IsUnusable_RejectsWorkingNamesAuthoritiesAndTheScientificName(string name) {
        Assert.True(CommonNameChooser.IsUnusable(name, "Aus bus", "Aus", "bus"));
    }

    [Theory]
    [InlineData("Bus fish")]
    [InlineData("Cassin's 17-year cicada")]
    public void IsUnusable_KeepsOrdinaryNames(string name) {
        Assert.False(CommonNameChooser.IsUnusable(name, "Aus bus", "Aus", "bus"));
    }
}
