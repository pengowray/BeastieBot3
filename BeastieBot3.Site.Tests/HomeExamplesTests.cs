using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Tests;

public sealed class HomeExamplesTests {
    // Every pool name as if the database had it, threatened unless listed in leastConcern.
    private static Dictionary<string, ExampleTaxon> Available(params string[] leastConcern) =>
        HomeExamples.AllNames.Distinct().ToDictionary(n => n, n => new ExampleTaxon(n, "Name of " + n, leastConcern.Contains(n) ? "LC" : "EN"));

    private static string Scientific(HomeExample e, IReadOnlyDictionary<string, ExampleTaxon> available) =>
        e.Italic ? e.Text : available.Values.Single(t => t.CommonName == e.Text).ScientificName;

    [Fact]
    public void MegafaunaFirstAPlantAlwaysAndNeverMoreThanFive() {
        var available = Available();
        for (var seed = 0; seed < 500; seed++) {
            var names = HomeExamples.Pick(new Random(seed), available).Select(e => Scientific(e, available)).ToList();
            Assert.Equal(HomeExamples.Count, names.Count);
            Assert.Equal(names.Count, names.Distinct().Count());
            Assert.Contains(names[0], HomeExamples.Megafauna);
            Assert.Contains(names, HomeExamples.Plants.Contains);
        }
    }

    [Fact]
    public void ABatInAboutHalfAndAtLeastOneScientificName() {
        var available = Available();
        var withBat = 0;
        for (var seed = 0; seed < 1000; seed++) {
            var examples = HomeExamples.Pick(new Random(seed), available);
            Assert.Contains(examples, e => e.Italic);
            if (examples.Any(e => HomeExamples.Bats.Contains(Scientific(e, available)))) {
                withBat++;
            }
        }
        Assert.InRange(withBat, 430, 570);
    }

    [Fact]
    public void ThreatenedSpeciesArePickedMoreOften() {
        var available = Available("Ursus arctos");
        var brownBear = 0;
        var tiger = 0;
        for (var seed = 0; seed < 4000; seed++) {
            var first = Scientific(HomeExamples.Pick(new Random(seed), available)[0], available);
            brownBear += first == "Ursus arctos" ? 1 : 0;
            tiger += first == "Panthera tigris" ? 1 : 0;
        }
        Assert.True(tiger > 2 * brownBear, $"tiger {tiger}, brown bear {brownBear}");
    }

    [Fact]
    public void NamesTheDatabaseDoesNotHaveAreLeftOut() {
        var available = new Dictionary<string, ExampleTaxon> {
            ["Panthera leo"] = new("Panthera leo", "Lion", "VU"),
            ["Lipotes vexillifer"] = new("Lipotes vexillifer", null, "CR(PE)"),
        };
        var examples = HomeExamples.Pick(new Random(1), available);
        Assert.Equal(2, examples.Count);
        Assert.Contains(new HomeExample("Lipotes vexillifer", true), examples);
        Assert.Empty(HomeExamples.Pick(new Random(1), new Dictionary<string, ExampleTaxon>()));
    }
}
