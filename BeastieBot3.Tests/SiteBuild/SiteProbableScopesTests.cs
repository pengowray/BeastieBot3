using BeastieBot3.Iucn;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// rules/iucn-probable-scopes.yml and what site build-db takes from it.
public class SiteProbableScopesTests {
    private const string Yaml = """
        scopes:
          - scope: United Arab Emirates
            kind: national
            evidence: The citation credits the UAE National Red List Workshop.
            assessments:
              280430982: Balaenoptera edeni_new
              279062402: Felis lybica
          - scope: Persian Gulf
            kind: regional
            evidence: The locations are the countries around the Persian Gulf.
            assessments:
              280279533: Lethrinus nebulosus
        """;

    [Fact]
    public void FromYaml_ReadsEachAssessmentsGroup() {
        var scopes = IucnProbableScopes.FromYaml(Yaml);
        Assert.Equal(3, scopes.Count);
        Assert.Equal(new IucnProbableScope(280279533, "Persian Gulf", "regional", "The locations are the countries around the Persian Gulf.", "Lethrinus nebulosus"),
            scopes.For(280279533));
        Assert.Null(scopes.For(1));
    }

    [Theory]
    [InlineData("scopes:\n  - scope: Greece\n    kind: country\n    evidence: x\n    assessments:\n      1: A b\n", "kind 'country'")]
    [InlineData("scopes:\n  - scope: Greece\n    kind: national\n    assessments:\n      1: A b\n", "no evidence")]
    [InlineData("scopes:\n  - {scope: A, kind: national, evidence: x, assessments: {1: A b}}\n  - {scope: B, kind: national, evidence: y, assessments: {1: A b}}\n", "assessment 1 is in two groups")]
    public void FromYaml_RefusesABadFile(string yaml, string message) =>
        Assert.Contains(message, Assert.Throws<InvalidOperationException>(() => IucnProbableScopes.FromYaml(yaml)).Message);

    // The file in the repository reads, and every group's evidence is there.
    [Fact]
    public void TheRulesFileReads() {
        var scopes = IucnProbableScopes.Load(Path.Combine(AppContext.BaseDirectory, "rules", IucnProbableScopes.FileName));
        Assert.NotNull(scopes.SourcePath);
        Assert.Equal(51, scopes.Count);
        Assert.Equal("United Arab Emirates", scopes.For(280430982)!.Scope);
    }

    [Fact]
    public void Match_TakesTheDatabasesAssessmentsWithNoScopeAndWarnsAboutTheRest() {
        var scopes = IucnProbableScopes.FromYaml(Yaml);
        var (rows, warnings) = SiteProbableScopes.Match(scopes,
            noScope: [(280430982, "Balaenoptera edeni_new"), (299807856, "Grammomys cometes")],
            listed: [(280430982, "", "Balaenoptera edeni_new"), (280279533, "Persian Gulf", "Lethrinus nebulosus")]);

        Assert.Equal([280430982L], rows.Select(r => r.AssessmentId));
        Assert.Equal(2, warnings.Count);
        Assert.Contains("1 assessments with no scope have no probable scope in rules/iucn-probable-scopes.yml: 299807856 (Grammomys cometes).", warnings);
        Assert.Contains("Assessment 280279533 (Lethrinus nebulosus) now has the scope Persian Gulf; take it out of rules/iucn-probable-scopes.yml.", warnings);
    }

    [Fact]
    public void Match_WarnsWhenTheFileNamesAnotherTaxon() {
        var (_, warnings) = SiteProbableScopes.Match(IucnProbableScopes.FromYaml(Yaml),
            noScope: [(279062402, "Felis silvestris")], listed: [(279062402, "", "Felis silvestris")]);
        Assert.Equal(["rules/iucn-probable-scopes.yml names assessment 279062402 Felis lybica, but it is an assessment of Felis silvestris."], warnings);
    }
}
