using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// One row of the status update page's table of taxa by category: how many species and how many
/// subspecies and varieties the wikitext lists with the code, how many the group compared with has,
/// and how many of those the wikitext leaves out (MissingSpecies, MissingInfra: null when the
/// comparison leaves such taxa out, so none of them is missing).
public sealed record StatusCountRow(string Code, int Species, int Infra, int IucnSpecies, int IucnInfra) {
    public int? MissingSpecies { get; init; }
    public int? MissingInfra { get; init; }
}

/// The missing taxa with one code, for the list under the table, in tree order: species, then
/// subspecies and varieties. SpeciesTotal and InfraTotal: how many there are, which can be more than
/// the rows here (ListScope lists at most ListScope.MaxMissing missing taxa).
public sealed record MissingCategory(string Code, IReadOnlyList<ListTaxonRow> Species, IReadOnlyList<ListTaxonRow> Infra,
    int SpeciesTotal, int InfraTotal);

/// The status update page's table of the taxa the wikitext lists, by the {{IUCN status}} code of
/// their latest assessment, beside the group's taxa when there is a comparison (ListScope). One row per
/// code that either side has, in the order of the list sections, so CR(PE) and LR/nt have rows of their
/// own. HasIucn: the table has the group's columns. ShowMissing: it also has the missing taxa's columns,
/// which a comparison has when it lists the missing taxa (not for a list of part of the group), and
/// Missing names them. Style: how the group's lists name a taxon, for the names of the missing taxa.
public sealed record StatusCountTable(IReadOnlyList<StatusCountRow> Rows, bool HasIucn) {
    /// The most names of missing taxa given for one code; the rest are counted.
    public const int MaxNames = 50;

    public bool ShowMissing { get; init; }
    /// The missing taxa's columns include subspecies and varieties: only when the comparison compares
    /// them (ListScopeResult.InfraChecked), so the table has no column of counts it leaves out.
    public bool ShowMissingInfra { get; init; }
    public IReadOnlyList<MissingCategory> Missing { get; init; } = [];
    public SpeciesListStyle Style { get; init; } = SpeciesListStyle.CommonNameFirst;

    /// Subspecies and varieties get columns of their own only when either side has some.
    public bool ShowInfra => Rows.Any(r => r.Infra > 0 || r.IucnInfra > 0);

    /// Some missing cells have no count: the comparison leaves out some categories.
    public bool MissingCategoriesLeftOut => ShowMissing && Rows.Any(r => r.MissingSpecies is null);

    public StatusCountRow Total => new(string.Empty, Rows.Sum(r => r.Species), Rows.Sum(r => r.Infra),
        Rows.Sum(r => r.IucnSpecies), Rows.Sum(r => r.IucnInfra)) {
        MissingSpecies = SumOrNull(Rows.Select(r => r.MissingSpecies)),
        MissingInfra = SumOrNull(Rows.Select(r => r.MissingInfra)),
    };

    /// The table for a comparison: its listed taxa beside its group's counts, and the taxa it finds
    /// missing in the categories (and of the kinds) it compares.
    public static StatusCountTable For(ListScopeResult scope) {
        var table = Build(scope.ListedCompared, scope.ScopeCounts) with { Style = scope.Style };
        if (scope.Missing is not { } missing) {
            return table;
        }
        bool Compared(string code) => scope.Categories is null || ListScope.InCategories(code, scope.Categories);
        var rows = table.Rows.Select(r => Compared(r.Code)
            ? r with {
                MissingSpecies = Math.Max(0, r.IucnSpecies - r.Species),
                MissingInfra = scope.InfraChecked ? Math.Max(0, r.IucnInfra - r.Infra) : null,
            }
            : r).ToList();
        var byCode = missing.GroupBy(GroupList.StatusCode).ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
        var named = rows
            .Where(r => byCode.ContainsKey(r.Code))
            .Select(r => new MissingCategory(r.Code,
                [.. byCode[r.Code].Where(t => t.Kind == TaxonKinds.Species)],
                [.. byCode[r.Code].Where(t => t.Kind != TaxonKinds.Species)],
                r.MissingSpecies ?? 0, r.MissingInfra ?? 0))
            .ToList();
        return table with { Rows = rows, ShowMissing = true, ShowMissingInfra = table.ShowInfra && scope.InfraChecked, Missing = named };
    }

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

    /// The id of a code's line in the list of missing taxa: "missing-CR-PE" for CR(PE).
    public static string AnchorOf(string code) =>
        "missing-" + System.Text.RegularExpressions.Regex.Replace(code, "[^A-Za-z0-9]+", "-").Trim('-');

    private static int? SumOrNull(IEnumerable<int?> values) {
        var known = values.OfType<int>().ToList();
        return known.Count == 0 ? null : known.Sum();
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
