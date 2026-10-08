using BeastieBot3.CommonNames;
using BeastieBot3.Wikipedia;
using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins StoreBackedCommonNameProvider.IsScientificTitle: which redirect targets of a group's
// scientific name are scientific names, so the group gets no English name from them. The cases are
// from the site database of October 2026, where 168 groups had another group's scientific name as
// their English name.
public class GroupRedirectTitleTests {
    // Genus names and how many IUCN and Catalogue of Life English names use each as a word.
    private static readonly NameWordSets Words = new(
        ["paspalum", "drepana", "komarekiona", "gorilla", "caracara", "hylocitrea", "ficus", "rana"],
        [],
        new Dictionary<string, int> {
            ["paspalum"] = 29, ["gorilla"] = 20, ["caracara"] = 25, ["hylocitrea"] = 4, ["ficus"] = 3, ["rana"] = 8,
            ["spider"] = 1131, ["mantis"] = 88,
        });

    [Theory]
    // The group's own name with a bracketed word.
    [InlineData("Contia", "Contia (snake)", "Contia tenuis")]
    // A family, subfamily or superfamily name.
    [InlineData("Pseudomyrmecini", "Pseudomyrmecinae", "Pseudomyrmecini")]
    [InlineData("Stylephoriformes", "Stylephoridae", "Stylephorus chordatus")]
    [InlineData("Tischeriidae", "Tischerioidea", "Tischeriidae")]
    // A genus redirecting to another genus: never an English name, even when English names use the word.
    [InlineData("Thrasya", "Paspalum", "Paspalum")]
    [InlineData("Watsonalla", "Drepana (moth)", "Drepana")]
    [InlineData("Aquarana", "Rana (genus)", "Rana")]
    // A group above genus redirecting to its genus, or to its genus's one species, when English names
    // do not use the genus name as a word (or fewer than 10 do).
    [InlineData("Komarekionidae", "Komarekiona", "Komarekiona eatoni")]
    [InlineData("Hylocitreidae", "Hylocitrea", "Hylocitrea<ref name=\"IOC\">{{cite web|title=Waxwings}}</ref>")]
    [InlineData("Ficeae", "Ficus (plant)", "Ficus (plant)")]
    public void ScientificNames(string group, string redirectTarget, string taxobox) =>
        Assert.True(StoreBackedCommonNameProvider.IsScientificTitle(group, redirectTarget, Article(redirectTarget, taxobox), () => Words));

    [Theory]
    // English names: the article's taxon is another taxon than the title names.
    [InlineData("Araneae", "Spider", "Araneae")]
    [InlineData("Mantodea", "Mantis", "Mantodea")]
    // A group above genus redirecting to a genus whose name English names use as a word.
    [InlineData("Gorillini", "Gorilla", "Gorilla")]
    [InlineData("Polyborinae", "Caracara (subfamily)", "Caracara")]
    public void EnglishNames(string group, string redirectTarget, string taxobox) =>
        Assert.False(StoreBackedCommonNameProvider.IsScientificTitle(group, redirectTarget, Article(redirectTarget, taxobox), () => Words));

    [Theory]
    [InlineData("Sepioidea", "Sepiida")]
    [InlineData("Amylocorticiaceae", "Amylocorticiales")]
    public void AnOrderNameIsScientificEvenWhenTheTargetIsNotDownloaded(string group, string redirectTarget) =>
        Assert.True(StoreBackedCommonNameProvider.IsScientificTitle(group, redirectTarget, null, () => Words));

    [Fact]
    public void ATargetThatIsNotDownloadedIsKept() =>
        Assert.False(StoreBackedCommonNameProvider.IsScientificTitle("Cetartiodactyla", "Even-toed ungulate", null, () => Words));

    private static WikiGroupArticle Article(string title, string taxobox) =>
        new(1, title, title.ToLowerInvariant(), false, taxobox, false);
}
