using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// Pins which P141 statements `site build-db` finds citing IUCN in an item's JSON, for the references
// the cache's index cannot decide. The first case is Q571449 (Arctocephalus gazella, taxon 2058) as
// the cache held it on 13 September 2026, cut down; the others are made up.
public class P141JsonReferencesTests {
    private const string PreferredEndangered = "Q571449$d351c0cb-4173-2e87-3086-98f1b0871894";
    private const string NormalLeastConcern = "q571449$4D114D83-9B72-49BB-A754-8520B542576A";

    private const string ArctocephalusGazella = """
        {"entities":{"Q571449":{"id":"Q571449","claims":{"P141":[
          {"id":"q571449$4D114D83-9B72-49BB-A754-8520B542576A","rank":"normal","mainsnak":{"datavalue":{"value":{"id":"Q211005"}}},
           "qualifiers":{"P582":[{"datavalue":{"value":{"time":"+2026-04-00T00:00:00Z","precision":10}}}]},
           "references":[{"hash":"a","snaks":{"P248":[{"datavalue":{"value":{"id":"Q115962546"}}}],"P627":[{"datavalue":{"value":"2058"}}]}}]},
          {"id":"Q571449$d351c0cb-4173-2e87-3086-98f1b0871894","rank":"preferred","mainsnak":{"datavalue":{"value":{"id":"Q96377276"}}},
           "qualifiers":{"P580":[{"datavalue":{"value":{"time":"+2026-04-00T00:00:00Z","precision":10}}}]},
           "references":[{"hash":"b","snaks":{"P854":[{"datavalue":{"value":"https://nc.iucnredlist.org/redlist/content/attachment_files/Arctocephalus_gazella_Pre-Publication_Assessment_RLTS_T2058A293563664_en.pdf"}}]}}]}
        ]}}}}
        """;

    private static bool NoSource(long _) => false;

    [Fact]
    public void ReferenceUrlOnNcIucnRedList_CitesIucn() {
        var found = P141JsonReferences.StatementsCitingIucn(ArctocephalusGazella, new HashSet<string> { PreferredEndangered }, NoSource);
        Assert.Equal(P141JsonCitation.ReferenceUrl, Assert.Single(found, f => f.Key == PreferredEndangered).Value);
    }

    [Fact]
    public void OnlyTheStatementsAskedFor_AndIdsCompareExactly() {
        // The least concern statement's id starts with a lower-case "q"; an upper-case one is another id.
        var asked = new HashSet<string> { NormalLeastConcern.Replace('q', 'Q') };
        Assert.Empty(P141JsonReferences.StatementsCitingIucn(ArctocephalusGazella, asked, _ => true));
    }

    [Fact]
    public void StatedIn_AnyOfAReferencesItems() {
        var found = P141JsonReferences.StatementsCitingIucn(ArctocephalusGazella, new HashSet<string> { NormalLeastConcern },
            item => item == 115962546);
        Assert.Equal(P141JsonCitation.StatedIn, found[NormalLeastConcern]);
    }

    [Theory]
    [InlineData("""{"entities":{"Q1":{"claims":{"P141":[{"id":"Q1$x","references":[{"snaks":{"P854":[{"datavalue":{"value":"https://www.example.org/iucnredlist.org"}}]}}]}]}}}}""")]
    [InlineData("""{"entities":{"Q1":{"claims":{"P141":[{"id":"Q1$x","references":[{"snaks":{"P854":[{"snaktype":"somevalue"}]}}]}]}}}}""")]
    [InlineData("""{"entities":{"Q1":{"claims":{"P141":[{"id":"Q1$x"}]}}}}""")]
    [InlineData("""{"entities":{"Q1":{"claims":{}}}}""")]
    [InlineData("{}")]
    [InlineData("not json")]
    [InlineData("")]
    [InlineData(null)]
    public void NothingCitingIucn(string? json) {
        Assert.Empty(P141JsonReferences.StatementsCitingIucn(json, new HashSet<string> { "Q1$x" }, NoSource));
    }
}
