using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// site build-db leaves out a Wikidata item matched by name alone when its description names a group
// in another kingdom. The cases are from the 2026-1 build (Clusia flava matched to an insect's item).
public class WikidataNameMatchKingdomTests {
    [Theory]
    [InlineData("species of insect", "PLANTAE", true)]
    [InlineData("species of crustacean", "PLANTAE", true)]
    [InlineData("species of mollusc", "PLANTAE", true)]
    [InlineData("species of fungus", "ANIMALIA", true)]
    [InlineData("species of plant", "ANIMALIA", true)]
    [InlineData("species of plant", "PLANTAE", false)]
    [InlineData("species of tree frog", "ANIMALIA", false)]
    [InlineData("species of tree squirrel common throughout Eurasia", "ANIMALIA", false)]
    [InlineData("genus of insects", "PLANTAE", true)]
    [InlineData("species of bird", "ANIMALIA", false)]
    [InlineData("Wikimedia disambiguation page", "PLANTAE", false)]
    [InlineData(null, "PLANTAE", false)]
    [InlineData("species of insect", null, false)]
    public void DescribesAnotherKingdom(string? description, string? kingdom, bool expected) =>
        Assert.Equal(expected, SiteBuildRules.DescribesAnotherKingdom(description, kingdom));
}
