using BeastieBot3.Iucn;

// Which rows of rules/iucn-probable-scopes.yml go into the site database's probable_scope table, and
// what the build warns about: an assessment with no scope that the file does not cover (new ones
// come with each release), an entry whose assessment now has a scope, and an entry whose name is not
// the taxon's. Entries for assessments the site does not have (IUCN's unpublished drafts, which the
// file lists for the audit site) are left out without a warning.

namespace BeastieBot3.SiteBuild;

internal static class SiteProbableScopes {
    private const string File = "rules/" + IucnProbableScopes.FileName;

    /// noScope: the assessments in the site database with no scope, with their taxon's name. listed:
    /// the file's assessments that are in the database, with their scope and taxon's name.
    public static (List<IucnProbableScope> Rows, List<string> Warnings) Match(IucnProbableScopes rules,
        IReadOnlyList<(long Id, string TaxonName)> noScope, IReadOnlyList<(long Id, string Scope, string TaxonName)> listed) {
        var rows = new List<IucnProbableScope>();
        var warnings = new List<string>();
        var missing = new List<string>();
        foreach (var (id, name) in noScope.OrderBy(a => a.Id)) {
            if (rules.For(id) is { } guess) {
                rows.Add(guess);
            } else {
                missing.Add($"{id} ({name})");
            }
        }
        if (missing.Count > 0) {
            warnings.Add($"{missing.Count} assessments with no scope have no probable scope in {File}: {string.Join(", ", missing.Take(20))}"
                + (missing.Count > 20 ? ", ..." : "."));
        }
        foreach (var (id, scope, name) in listed.OrderBy(a => a.Id)) {
            var entry = rules.For(id)!;
            if (scope.Trim().Length > 0) {
                warnings.Add($"Assessment {id} ({name}) now has the scope {scope}; take it out of {File}.");
            } else if (entry.TaxonName is { } given && !string.Equals(given, name, StringComparison.Ordinal)) {
                warnings.Add($"{File} names assessment {id} {given}, but it is an assessment of {name}.");
            }
        }
        return (rows, warnings);
    }
}
