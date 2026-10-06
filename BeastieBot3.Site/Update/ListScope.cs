using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

/// The groups and taxa ListScope reads. SiteListScopeLookup reads the site database.
public interface IListScopeLookup {
    /// The group and every group above it, kingdom first.
    IReadOnlyList<GroupRow> PathOf(int nodeId);

    /// How many taxa in the group have each {{IUCN status}} code in their latest global assessment.
    IReadOnlyList<GroupCategoryCount> CountsOf(int nodeId);

    /// The group's taxa of these kinds, in tree order.
    IReadOnlyList<ListTaxonRow> TaxaIn(GroupRow group, IReadOnlyCollection<string> kinds);
}

/// What the reader chose. Scope: "rank/name" of a group above the taxa ("family/Felidae"), or null for
/// the group ListScope finds. ListAnyway: list the missing taxa when the text covers too little of
/// the group to be a list of it.
public sealed record ListScopeOptions(string? Scope = null, bool ListAnyway = false);

/// A taxon the text lists that is outside the group, or whose latest category is outside the
/// categories the list gives. Lines: where the text lists it.
public sealed record ListScopeMember(StatusTaxon Taxon, IReadOnlyList<int> Lines, string? WrittenCode, string? CurrentCode);

/// A taxon listed under two or more names (a synonym and its accepted name, after a lump).
public sealed record ListScopeDuplicate(StatusTaxon Taxon, IReadOnlyList<string> Names, IReadOnlyList<int> Lines);

/// The comparison of the taxa a text lists with the group they are in.
/// Scope: the group compared with; Path: the groups the reader can choose instead, kingdom first,
/// ending with the group found. Species: listed species in the scope; SpeciesInScope: the scope's
/// species (in the categories, when Categories is set). Infra*: the same for subspecies and varieties.
/// Categories: the categories the list is of ("CR"), when the text gives only those, or (with
/// CategoriesFromCodes false) when the text gives no codes and nearly all its species are in them now;
/// null for all.
/// Partial: the text lists too little of the scope to be a list of it, so Missing is null unless the
/// reader asked for it anyway. Missing: the scope's taxa (with a latest global assessment) that the text
/// does not list, at most MaxMissing; MissingTotal counts them all.
public sealed record ListScopeResult(
    GroupRow Scope,
    IReadOnlyList<GroupRow> Path,
    int Species,
    int SpeciesInScope,
    int Infra,
    int InfraInScope,
    bool InfraChecked,
    IReadOnlySet<string>? Categories,
    bool CategoriesFromCodes,
    bool Partial,
    IReadOnlyList<ListTaxonRow>? Missing,
    int MissingTotal,
    SpeciesListStyle Style,
    IReadOnlyList<ListScopeMember> Outside,
    IReadOnlyList<ListScopeMember> OtherCategory,
    IReadOnlyList<ListScopeDuplicate> Duplicates);

/// Compares the taxa a text lists (StatusUpdateResult.Members) with the IUCN group they are in, and
/// finds the group's taxa the text leaves out, the taxa it lists that are outside the group or now
/// in another category, and taxa it lists under two names. Report only: the text is never changed.
public static class ListScope {
    /// The share of the listed taxa a group must hold to be the list's group.
    public const double ScopeShare = 0.95;
    /// The share of a group's taxa (in the list's categories, when it has some) that a text must list
    /// to be a list of the whole group.
    public const double ListShare = 0.5;
    /// A list that writes no codes is taken to be a list of one category (or of threatened taxa) when
    /// this share of its species are in it now.
    public const double CurrentCategoryShare = 0.8;
    /// Fewer listed taxa than this: no comparison.
    public const int MinMembers = 3;
    /// A list that covers too little of its group is reported only from this many taxa.
    public const int MinMembersForPartial = 10;
    /// A category filter is used only when the text gives this many codes.
    public const int MinCodes = 5;
    /// The most missing taxa listed: as many lines as one Wikipedia page can hold.
    public const int MaxMissing = GroupList.MaxLines;

    private static readonly HashSet<string> Threatened = ["CR", "EN", "VU"];
    private static readonly HashSet<string> Extinct = ["EX", "EW"];

    public static ListScopeResult? Check(IReadOnlyList<ListMember> members, IListScopeLookup lookup, ListScopeOptions? options = null) {
        options ??= new ListScopeOptions();
        var listed = members.Where(m => m.Taxon is { InRelease: true, NodeId: not null } && m.Taxon.Kind != TaxonKinds.Subpopulation)
            .GroupBy(m => m.Taxon.TaxonId).ToList();
        if (listed.Count < MinMembers) {
            return null;
        }
        var paths = new Dictionary<int, IReadOnlyList<GroupRow>>();
        IReadOnlyList<GroupRow> PathOf(int node) => paths.TryGetValue(node, out var p) ? p : paths[node] = lookup.PathOf(node);
        var holding = new Dictionary<int, int>();
        var groups = new Dictionary<int, GroupRow>();
        foreach (var taxon in listed) {
            foreach (var group in PathOf(taxon.First().Taxon.NodeId!.Value)) {
                holding[group.NodeId] = holding.GetValueOrDefault(group.NodeId) + 1;
                groups[group.NodeId] = group;
            }
        }
        // The deepest group that holds nearly all the listed taxa, so that a taxon IUCN has moved to
        // another genus is reported, not a reason to compare with the family. A Catalogue of Life
        // group (subclass Theria) must hold all of them: a list of mammals with two monotremes is a
        // list of class Mammalia.
        var found = groups.Values
            .Where(g => holding[g.NodeId] >= ScopeShare * listed.Count && (!g.IsCol || holding[g.NodeId] == listed.Count))
            .MaxBy(g => g.Depth);
        if (found is null) {
            return null;
        }
        // The reader can choose a group above the one found, or one below it that holds at least
        // half the listed taxa (genus Phyllanthus in a list of Phyllanthus species, some of which
        // IUCN puts in other genera of the family).
        var path = PathOf(found.NodeId).ToList();
        for (var at = found; ;) {
            var child = groups.Values.Where(g => g.ParentNodeId == at.NodeId && holding[g.NodeId] >= ListShare * listed.Count)
                .MaxBy(g => holding[g.NodeId]);
            if (child is null) {
                break;
            }
            path.Add(child);
            at = child;
        }
        var scope = options.Scope is { } wanted ? path.FirstOrDefault(g => Key(g) == wanted) ?? found : found;
        bool InScope(StatusTaxon taxon) => PathOf(taxon.NodeId!.Value).Any(g => g.NodeId == scope.NodeId);

        var inScope = listed.Where(t => InScope(t.First().Taxon)).ToList();
        var species = inScope.Where(t => t.First().Taxon.Kind == TaxonKinds.Species).ToList();
        var infra = inScope.Where(t => t.First().Taxon.Kind != TaxonKinds.Species).ToList();
        var counts = lookup.CountsOf(scope.NodeId);

        // The categories the list is of: from the codes the text wrote, which still show the
        // categories of taxa that have moved since.
        var codes = members.Select(m => Normalize(m.WrittenCode)).OfType<string>().ToList();
        IReadOnlySet<string>? categories = null;
        if (codes.Count >= MinCodes) {
            var written = codes.ToHashSet();
            categories = written.Count == 1 ? written
                : written.IsSubsetOf(Threatened) ? Threatened
                : written.IsSubsetOf(Extinct) ? Extinct
                : null;
        } else if (codes.Count == 0 && species.Count >= MinCodes) {
            // A list that writes no codes ("List of endangered amphibians" names the species only):
            // the category nearly all its species are in now.
            var current = species.Select(t => Current(t.First().Taxon)).OfType<string>().ToList();
            var top = current.GroupBy(c => c).MaxBy(g => g.Count());
            if (top is not null && top.Count() >= CurrentCategoryShare * species.Count) {
                categories = new HashSet<string> { top.Key };
            } else if (current.Count(Threatened.Contains) >= CurrentCategoryShare * species.Count) {
                categories = Threatened;
            }
        }
        bool InCategories(StatusTaxon t) => categories is null || (Current(t) is { } c && categories.Contains(c));
        int ScopeCount(Func<GroupCategoryCount, int> count, IReadOnlySet<string>? cats) =>
            counts.Where(c => cats is null || (Normalize(c.Category) is { } n && cats.Contains(n))).Sum(count);

        var partial = false;
        if (categories is not null) {
            var listedInCategories = species.Count(t => InCategories(t.First().Taxon));
            if (listedInCategories < ListShare * ScopeCount(c => c.Species, categories)) {
                categories = null;
            }
        }
        if (categories is null) {
            partial = species.Count < ListShare * scope.SpeciesCount;
        }
        if (partial && listed.Count < MinMembersForPartial) {
            return null;
        }
        var speciesInScope = categories is null ? scope.SpeciesCount : ScopeCount(c => c.Species, categories);
        var infraInScope = categories is null ? scope.InfraCount : ScopeCount(c => c.Infra, categories);
        var infraListed = infra.Count(t => InCategories(t.First().Taxon));
        var infraChecked = infraInScope > 0 && infraListed >= ListShare * infraInScope;

        IReadOnlyList<ListTaxonRow>? missing = null;
        var missingTotal = 0;
        if (!partial || options.ListAnyway) {
            var kinds = infraChecked ? new[] { TaxonKinds.Species, TaxonKinds.Subspecies, TaxonKinds.Variety } : new[] { TaxonKinds.Species };
            var listedIds = listed.Select(t => t.Key).ToHashSet();
            var all = lookup.TaxaIn(scope, kinds)
                .Where(t => t.Category is not null && !listedIds.Contains(t.TaxonId)
                    && (categories is null || (Normalize(GroupList.StatusCode(t)) is { } c && categories.Contains(c))))
                .ToList();
            missingTotal = all.Count;
            missing = all.Take(MaxMissing).ToList();
        }

        ListScopeMember ToMember(IGrouping<long, ListMember> t) {
            var taxon = t.First().Taxon;
            return new ListScopeMember(taxon, [.. t.Select(m => m.Line).Distinct()],
                t.Select(m => m.WrittenCode).FirstOrDefault(c => c is not null), CurrentCode(taxon));
        }
        var outside = listed.Where(t => !InScope(t.First().Taxon)).Select(ToMember).ToList();
        var otherCategory = categories is null ? []
            : inScope.Where(t => !InCategories(t.First().Taxon)).Select(ToMember).ToList();
        var duplicates = listed
            .Select(t => (Taxon: t.First().Taxon, Names: t.Select(m => m.Written).Distinct(StringComparer.OrdinalIgnoreCase).ToList(),
                Lines: t.Select(m => m.Line).Distinct().ToList()))
            .Where(d => d.Names.Count > 1 && d.Lines.Count > 1)
            .Select(d => new ListScopeDuplicate(d.Taxon, d.Names, d.Lines))
            .ToList();

        return new ListScopeResult(scope, path, species.Count, speciesInScope, infra.Count, infraInScope, infraChecked, categories,
            codes.Count > 0, partial,
            missing, missingTotal, GroupListQuery.DefaultStyle(PathOf(scope.NodeId)), outside, otherCategory, duplicates);
    }

    /// The value of the Scope option for a group: "family/Felidae".
    public static string Key(GroupRow group) => $"{group.Rank}/{group.Name}";

    /// The bullet lines for the missing taxa, in the style the Wikipedia lists use for the group.
    public static string MissingLines(ListScopeResult result) =>
        string.Join("\n", (result.Missing ?? []).Select(t =>
            SpeciesListLine.Format(GroupList.Entry(t), new SpeciesListLineOptions { Style = result.Style })));

    private static string? CurrentCode(StatusTaxon taxon) => taxon.LatestGlobal is { } a
        ? IucnStatusTemplate.ToTemplateCode(a.Category, a.PossiblyExtinct, a.PossiblyExtinctInTheWild)
        : null;

    private static string? Current(StatusTaxon taxon) => Normalize(CurrentCode(taxon));

    /// The category a code is in, for comparing lists: CR(PE) and CR(PEW) are CR, LR/cd and LR/nt
    /// are NT, LR/lc is LC. Null for no code.
    internal static string? Normalize(string? code) {
        if (code is null) {
            return null;
        }
        var compact = code.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant();
        return compact switch {
            "" => null,
            "CR(PE)" or "CR(PEW)" or "PE" or "PEW" => "CR",
            "LR/CD" or "LR/NT" => "NT",
            "LR/LC" => "LC",
            _ => compact,
        };
    }
}
