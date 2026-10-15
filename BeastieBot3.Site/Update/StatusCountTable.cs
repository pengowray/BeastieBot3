using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// One row of the status update page's table of taxa by category: how many species and how many
/// subspecies and varieties the wikitext lists with the code, and how many the group compared with has.
public sealed record StatusCountRow(string Code, int Species, int Infra, int IucnSpecies, int IucnInfra);

/// The status update page's table of the taxa the wikitext lists, by the {{IUCN status}} code of
/// their latest assessment, beside the group's taxa when there is a comparison (ListScope). One row per
/// code that either side has, in the order of the list sections, so CR(PE) and LR/nt have rows of their
/// own. HasIucn: the table has the group's columns.
public sealed record StatusCountTable(IReadOnlyList<StatusCountRow> Rows, bool HasIucn) {
    /// Subspecies and varieties get columns of their own only when either side has some.
    public bool ShowInfra => Rows.Any(r => r.Infra > 0 || r.IucnInfra > 0);

    public StatusCountRow Total => new(string.Empty, Rows.Sum(r => r.Species), Rows.Sum(r => r.Infra),
        Rows.Sum(r => r.IucnSpecies), Rows.Sum(r => r.IucnInfra));

    /// The table for a comparison: its listed taxa beside its group's counts.
    public static StatusCountTable For(ListScopeResult scope) => Build(scope.ListedCompared, scope.ScopeCounts);

    /// The table with no comparison: every taxon the text lists that is in the release, each once,
    /// subpopulations left out, as ListScope counts them.
    public static StatusCountTable For(IEnumerable<ListMember> members) =>
        Build(members.Select(m => m.Taxon).Where(t => t.InRelease && t.Kind != TaxonKinds.Subpopulation).DistinctBy(t => t.TaxonId), null);

    /// taxa: the listed taxa, each once; iucn: the group's counts by code, or null for no comparison.
    public static StatusCountTable Build(IEnumerable<StatusTaxon> taxa, IReadOnlyList<GroupCategoryCount>? iucn) {
        var rows = new Dictionary<string, (int Species, int Infra, int IucnSpecies, int IucnInfra)>(StringComparer.Ordinal);
        foreach (var taxon in taxa) {
            var code = Code(taxon);
            var row = rows.GetValueOrDefault(code);
            rows[code] = taxon.Kind == TaxonKinds.Species ? row with { Species = row.Species + 1 } : row with { Infra = row.Infra + 1 };
        }
        foreach (var count in iucn ?? []) {
            var row = rows.GetValueOrDefault(count.Category);
            rows[count.Category] = row with { IucnSpecies = row.IucnSpecies + count.Species, IucnInfra = row.IucnInfra + count.Infra };
        }
        return new StatusCountTable([.. rows
            .Where(r => r.Value != default)
            .OrderBy(r => Order(r.Key))
            .ThenBy(r => r.Key, StringComparer.Ordinal)
            .Select(r => new StatusCountRow(r.Key, r.Value.Species, r.Value.Infra, r.Value.IucnSpecies, r.Value.IucnInfra))],
            iucn is not null);
    }

    // The code of the taxon's latest assessment ("CR(PE)", "LR/nt"); NE for a taxon IUCN has assessed only regionally.
    private static string Code(StatusTaxon taxon) => taxon.LatestGlobal is { } a
        ? IucnStatusTemplate.ToTemplateCode(a.Category, a.PossiblyExtinct, a.PossiblyExtinctInTheWild)
        : StatusSection.NotEvaluated;

    private static int Order(string code) {
        var section = StatusSection.For(code);
        return section is null ? int.MaxValue : StatusSection.All.ToList().IndexOf(section);
    }
}
