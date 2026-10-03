using System;
using System.Collections.Generic;
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

    [Fact]
    public void JunkNames_AreSkipped_AndTheNextNameIsUsed() {
        // 2026 data: Argia percellulata has "Calvert, 1902" from Wikidata, an author citation.
        using var store = OpenInMemory();
        var damselfly = AddTaxon(store, "argia percellulata", "1");
        AddName(store, damselfly, "Calvert, 1902", "wikidata_label");
        AddName(store, damselfly, "Mexican Dancer", "col");
        var chooser = CommonNameChooser.ForStore(store);

        var choice = chooser.Choose(Subject("Argia percellulata"), StoreName(chooser, store, damselfly));

        Assert.Equal("Mexican dancer", choice.Name);
    }

    [Fact]
    public void RepairableNames_AreUsedRepaired() {
        using var store = OpenInMemory();
        var loris = AddTaxon(store, "nycticebus coucang", "1");
        store.InsertCommonName(loris, "Sunda slow loris{sfn|Groves|2005|p=122}", "sundaslowlorissfngroves2005p122",
            "en", "wikipedia_taxobox", null, false);
        AddName(store, loris, "Greater Slow Loris", "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        var best = chooser.FromStore(loris, CommonNameStore.ToCandidates(store.GetCommonNamesForTaxon(loris)));

        Assert.NotNull(best);
        Assert.Equal("Sunda slow loris", best!.DisplayName);
        Assert.Equal("sundaslowloris", best.NormalizedName);
        Assert.Equal("Sunda slow loris{sfn|Groves|2005|p=122}", best.RawName);
    }

    [Fact]
    public void ScientificNameWithASubgenus_IsSkipped() {
        // 2026 data: Holothuria lessoni's Wikidata label is "Holothuria (Metriatyla) lessoni", which
        // the lists showed as its common name ahead of IUCN's "Golden Sandfish".
        using var store = OpenInMemory();
        var sandfish = AddTaxon(store, "holothuria lessoni", "1");
        AddName(store, sandfish, "Holothuria (Metriatyla) lessoni", "wikidata_label");
        AddName(store, sandfish, "Golden Sandfish", "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        var choice = chooser.Choose(Subject("Holothuria lessoni"),
            () => chooser.FromStore(sandfish, CommonNameStore.ToCandidates(store.GetCommonNamesForTaxon(sandfish)), "Holothuria lessoni")?.DisplayName);

        Assert.Equal("Golden sandfish", choice.Name);
    }

    [Fact]
    public void ANameInBracketsAfterTheGenus_IsKept_WhenItIsNotTheTaxonsScientificName() {
        using var store = OpenInMemory();
        var phalarope = AddTaxon(store, "phalaropus fulicarius", "1");
        AddName(store, phalarope, "Grey (Red) Phalarope", "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        var best = chooser.FromStore(phalarope, CommonNameStore.ToCandidates(store.GetCommonNamesForTaxon(phalarope)), "Phalaropus fulicarius");

        Assert.Equal("Grey (Red) Phalarope", best!.RawName);
    }

    [Theory]
    [InlineData("Large Palau flying fox")]
    [InlineData("Lake Mackay hare-wallaby")]
    [InlineData("Black Sea bottlenose dolphin")]
    [InlineData("Banded martin")]
    public void AWikipediaTitle_KeepsTheTitlesCapitals(string title) {
        // 2026 data: the caps rules have no rule for "Palau", "Mackay" or "Sea", so the lists showed
        // "Large palau flying fox"; a rule capitalises "martin" (the surname), so they showed
        // "Banded Martin" for the bird.
        using var store = OpenInMemory();
        store.InsertCapsRule("martin", "Martin");
        var taxon = AddTaxon(store, "aus bus", "1");
        AddName(store, taxon, title, "wikipedia_title", preferred: true);
        AddName(store, taxon, title.ToUpperInvariant(), "iucn", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        Assert.Equal(title, StoreName(chooser, store, taxon)());
    }

    [Fact]
    public void AWikipediaTitle_GetsAFirstCapital() {
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "aus bus", "1");
        AddName(store, taxon, "eastern Mud turtle", "wikipedia_title", preferred: true);
        var chooser = CommonNameChooser.ForStore(store);

        Assert.Equal("Eastern Mud turtle", StoreName(chooser, store, taxon)());
    }

    [Fact]
    public void ATaxoboxName_StillGetsTheCapsRules() {
        // About 590 taxobox names are in title case ("White Ash", "Nodding Yucca").
        using var store = OpenInMemory();
        var taxon = AddTaxon(store, "fraxinus americana", "1");
        AddName(store, taxon, "White Ash", "wikipedia_taxobox");
        var chooser = CommonNameChooser.ForStore(store);

        Assert.Equal("White ash", StoreName(chooser, store, taxon)());
    }

    [Fact]
    public void ANameFromAnotherSource_TakesTheCapitalsOfTheTaxonsWikipediaTitle() {
        // The IUCN name is chosen here because the title is not; it is the title in other capitals.
        var iucn = new CommonNameResult("Large Palau Flying Fox", "Large Palau Flying Fox", "largepalauflyingfox",
            "iucn", IsPreferred: true, IsAmbiguous: false);
        var candidates = new[] {
            new CommonNameCandidate("Large Palau flying fox", "largepalauflyingfox", "wikipedia_title", true),
            new CommonNameCandidate("Large Palau Flying Fox", "largepalauflyingfox", "iucn", true),
        };
        var lowerAfterFirst = (string name) => CommonNameNormalizer.ApplyCapitalization(name, new Dictionary<string, string>());

        Assert.Equal("Large Palau flying fox", CommonNameChooser.DisplayCasing(iucn, candidates, lowerAfterFirst));
        Assert.Equal("Palau fruit bat", CommonNameChooser.DisplayCasing(iucn with { DisplayName = "Palau Fruit Bat" },
            candidates, lowerAfterFirst));
    }

    [Fact]
    public void ArticleTitle_IsThePageTheNameCameFrom() {
        // 2026 data: "Jack Dempsey" is the title name of the page "Jack Dempsey (fish)", and
        // "Red mullet" is the taxobox name on the page "Mullus barbatus"; the lists linked the
        // name ([[Jack Dempsey]] is the boxer).
        using var store = OpenInMemory();
        var cichlid = AddTaxon(store, "rocio octofasciata", "1");
        var mullet = AddTaxon(store, "mullus barbatus", "2");
        store.InsertCommonName(cichlid, "Jack Dempsey", "jackdempsey", "en", "wikipedia_title", "Jack Dempsey (fish)", true);
        store.InsertCommonName(mullet, "Red mullet", "redmullet", "en", "wikipedia_taxobox", "Mullus barbatus", false);

        Assert.Equal("Jack Dempsey (fish)", store.GetWikipediaArticleTitle(cichlid));
        Assert.Equal("Mullus barbatus", store.GetWikipediaArticleTitle(mullet));
    }

    [Fact]
    public void ArticleTitle_WithoutAWikipediaName_IsThePageTheTaxonIsMatchedTo() {
        // The article "Crenimugil buchanani" is a scientific name, so it is no common name of
        // Moolgarda buchanani, and Wikipedia has no page "Moolgarda buchanani". The Maui dolphin is
        // matched to the page about Hector's dolphin, which is not its own article.
        using var store = OpenInMemory();
        var mullet = AddTaxon(store, "moolgarda buchanani", "1");
        store.InsertCrossReference(mullet, "wikipedia", "Crenimugil buchanani");
        var subspecies = AddTaxon(store, "cephalorhynchus hectori maui", "2");
        store.InsertCrossReference(subspecies, "wikipedia", "Hector's dolphin", CommonNameStore.OtherTaxonsPageMatch);
        var named = AddTaxon(store, "rocio octofasciata", "3");
        store.InsertCrossReference(named, "wikipedia", "Cichlasoma octofasciatum");
        store.InsertCommonName(named, "Jack Dempsey", "jackdempsey", "en", "wikipedia_title", "Jack Dempsey (fish)", true);

        Assert.Equal("Crenimugil buchanani", store.GetWikipediaArticleTitle(mullet));
        Assert.Null(store.GetWikipediaArticleTitle(subspecies));
        Assert.Equal("Jack Dempsey (fish)", store.GetWikipediaArticleTitle(named));
        Assert.Null(store.GetWikipediaArticleTitle(mullet, "fr"));
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
