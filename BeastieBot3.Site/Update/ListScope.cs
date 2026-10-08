using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
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

    /// What a {{Species table/row}} needs for the group's species (authority, population, citation), by taxon id.
    IReadOnlyDictionary<long, TableTaxonExtra> ExtrasOf(GroupRow group);

    /// The species of the Catalogue of Life (and Wikidata) that IUCN does not have, placed in the group,
    /// less those that are likely an IUCN taxon under another name.
    IReadOnlyList<ExtraSpeciesRow> ExtraSpeciesIn(GroupRow group);

    /// The group's taxa of these kinds whose latest global assessment codes the area, with the record.
    IReadOnlyList<(ListTaxonRow Row, AreaRecord Record)> TaxaInArea(GroupRow group, IReadOnlyCollection<string> kinds, string area);

    /// The area's records for these taxa; a taxon the area has no record for is left out.
    IReadOnlyDictionary<long, AreaRecord> AreaRecordsOf(string area, IReadOnlyCollection<long> taxonIds);

    /// The group's taxa of these kinds that have an assessment in the IUCN region ("Europe"), each
    /// with its latest assessment there in place of the global one.
    IReadOnlyList<ListTaxonRow> TaxaInRegion(GroupRow group, IReadOnlyCollection<string> kinds, string region) => [];
}

/// How a taxon's latest global assessment codes one country or area.
public sealed record AreaRecord(AreaOrigin Origin, AreaPresence Presence, bool Endemic) {
    /// Whether the record puts the taxon on a list of the area of this kind. Presence never leaves a
    /// taxon out: a list of an area's birds usually keeps the extirpated ones, marked.
    public bool Includes(AreaMode mode) => mode switch {
        AreaMode.Endemic => Endemic,
        AreaMode.Native => Origin is AreaOrigin.Native or AreaOrigin.Reintroduced,
        AreaMode.NativeAndIntroduced => Origin <= AreaOrigin.AssistedColonisation,
        _ => true,
    };

    /// Native and extant: nothing to say beside the taxon on a list of the area.
    public bool Plain => Origin is AreaOrigin.Native && Presence is AreaPresence.Extant;
}

/// What the reader chose. Scope: "rank/name" of a group above the taxa ("family/Felidae"), or null for
/// the group ListScope finds. ListAnyway: list the missing taxa when the text covers too little of
/// the group to be a list of it.
/// Extra: also compare with the species the Catalogue of Life and Wikidata have that IUCN does not
/// (extra_species), leaving out those whose name or English Wikipedia article WrittenNames has
/// (SiteNameKey.Fold of every scientific name the text writes and every page it links).
public sealed record ListScopeOptions(string? Scope = null, bool ListAnyway = false, bool Extra = false,
    IReadOnlySet<string>? WrittenNames = null) {
    /// When the text writes no codes, take it to be a list of the threatened or extinct category
    /// nearly all its species are in now. Off for a text known to list a whole group whatever the
    /// categories, such as a genus article (`wikipedia report-species-lists`).
    public bool GuessCategories { get; init; } = true;

    /// The categories the list is of, chosen by the reader or read from the page's title: these are
    /// compared whatever codes the text writes. Null: from the codes the text writes (or the guess).
    public ListCategoryChoice? Categories { get; init; }

    /// The country or area the list is of (an area code: "BR", "HAW-HI"): only the group's taxa that
    /// its records include (AreaMode) are compared. Null: the whole group.
    public string? Area { get; init; }

    public AreaMode AreaMode { get; init; } = AreaMode.Native;

    /// The IUCN region the list is compared with ("Europe"): only the group's taxa with an assessment
    /// there are compared, in their categories there. The members' taxa must come from a status
    /// lookup with the same region. Set, it replaces Area.
    public string? Region { get; init; }
}

/// A listed taxon that the chosen area's records leave out: Record is null when the latest global
/// assessment does not code the area at all, else the record (vagrant, say, for a list of natives).
public sealed record ListScopeAreaMember(ListScopeMember Member, AreaRecord? Record);

/// A taxon the text lists that is outside the group, or whose latest category is outside the
/// categories the list gives. Lines: where the text lists it.
public sealed record ListScopeMember(StatusTaxon Taxon, IReadOnlyList<int> Lines, string? WrittenCode, string? CurrentCode);

/// A taxon listed under two or more names (a synonym and its accepted name, after a lump).
public sealed record ListScopeDuplicate(StatusTaxon Taxon, IReadOnlyList<string> Names, IReadOnlyList<int> Lines);

/// The comparison of the taxa a text lists with the group they are in.
/// Scope: the group compared with; Path: the groups the reader can choose instead, kingdom first,
/// with the group found and any group below it that holds half the listed taxa. Species: listed
/// species in the scope that are in the categories now (all of them when Categories is null);
/// SpeciesListed: every listed species in the scope; SpeciesInScope: the scope's species in the
/// categories. Infra and InfraInScope: the same for subspecies and varieties.
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
    int SpeciesListed,
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
    IReadOnlyList<ListScopeDuplicate> Duplicates) {
    /// How many listed species each group of Path holds, by node id.
    public IReadOnlyDictionary<int, int> ListedIn { get; init; } = new Dictionary<int, int>();

    /// The species of the Catalogue of Life and Wikidata in the scope that the text does not name, as
    /// list rows (TaxonId: minus the extra species id; no assessment); null when not asked for.
    public IReadOnlyList<ListTaxonRow>? MissingExtra { get; init; }

    /// How many such species the scope has, named in the text or not.
    public int ExtraTotal { get; init; }

    /// The categories (or all categories) were chosen (ListScopeOptions.Categories), not read from the codes.
    public bool CategoriesChosen { get; init; }

    /// The area compared with (ListScopeOptions.Area), and which of its records count.
    public string? Area { get; init; }
    public AreaMode AreaMode { get; init; }

    /// Listed taxa in the group that the area's records leave out.
    public IReadOnlyList<ListScopeAreaMember> NotInArea { get; init; } = [];

    /// The IUCN region compared with (ListScopeOptions.Region).
    public string? Region { get; init; }

    /// Listed taxa in the group with no assessment in the region.
    public IReadOnlyList<ListScopeMember> NotInRegion { get; init; } = [];

    /// The area's records of the missing taxa that need a word beside them (introduced, vagrant,
    /// extirpated ...), by taxon id.
    public IReadOnlyDictionary<long, AreaRecord> MissingAreaRecords { get; init; } = new Dictionary<long, AreaRecord>();
}

/// Compares the taxa a text lists (StatusUpdateResult.Members) with the IUCN group they are in, and
/// finds the group's taxa the text leaves out, the taxa it lists that are outside the group or now
/// in another category, and taxa it lists under two names. Report only: the text is never changed.
public static partial class ListScope {
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

        // A list of one area: the group's taxa that the area's records include stand in for the group.
        // A list of one IUCN region: the group's taxa with an assessment there, in their categories there.
        var region = options.Region;
        var area = region is null ? options.Area : null;
        List<(ListTaxonRow Row, AreaRecord Record)>? inArea = null;
        List<ListTaxonRow>? subset = null;
        IReadOnlyDictionary<long, AreaRecord> listedRecords = new Dictionary<long, AreaRecord>();
        if (area is not null) {
            inArea = lookup.TaxaInArea(scope, [TaxonKinds.Species, TaxonKinds.Subspecies, TaxonKinds.Variety], area)
                .Where(t => t.Record.Includes(options.AreaMode)).ToList();
            subset = [.. inArea.Select(t => t.Row)];
            listedRecords = lookup.AreaRecordsOf(area, [.. listed.Select(t => t.Key)]);
        } else if (region is not null) {
            subset = [.. lookup.TaxaInRegion(scope, [TaxonKinds.Species, TaxonKinds.Subspecies, TaxonKinds.Variety], region)];
        }
        var listedById = listed.ToDictionary(t => t.Key, t => t.First().Taxon);
        bool InArea(long taxonId) => area is not null
            ? listedRecords.TryGetValue(taxonId, out var record) && record.Includes(options.AreaMode)
            : region is null || listedById[taxonId].LatestGlobal is not null;

        var inScope = listed.Where(t => InScope(t.First().Taxon)).ToList();
        var species = inScope.Where(t => t.First().Taxon.Kind == TaxonKinds.Species && InArea(t.Key)).ToList();
        var infra = inScope.Where(t => t.First().Taxon.Kind != TaxonKinds.Species && InArea(t.Key)).ToList();
        IReadOnlyList<GroupCategoryCount> counts = subset is null ? lookup.CountsOf(scope.NodeId)
            : [.. subset.GroupBy(GroupList.StatusCode).Select(g => new GroupCategoryCount(g.Key,
                g.Count(t => t.Kind == TaxonKinds.Species), g.Count(t => t.Kind != TaxonKinds.Species), 0))];
        var scopeSpecies = subset?.Count(t => t.Kind == TaxonKinds.Species) ?? scope.SpeciesCount;
        var scopeInfra = subset?.Count(t => t.Kind != TaxonKinds.Species) ?? scope.InfraCount;

        // The categories the list is of: from the codes the text wrote, which still show the
        // categories of taxa that have moved since.
        var codes = members.Select(m => Normalize(m.WrittenCode)).OfType<string>().ToList();
        IReadOnlySet<string>? categories = null;
        var chosen = options.Categories is not null;
        if (options.Categories is { } choice) {
            categories = choice.All ? null : choice.Codes;
        } else if (codes.Count >= MinCodes) {
            var written = codes.ToHashSet();
            categories = written.Count == 1 ? written
                : written.IsSubsetOf(Threatened) ? Threatened
                : written.IsSubsetOf(Extinct) ? Extinct
                : null;
        } else if (options.GuessCategories && codes.Count == 0 && species.Count >= MinCodes) {
            // A list that writes no codes ("List of endangered amphibians" names the species only):
            // the category nearly all its species are in now, when that is a threatened or extinct
            // category. Most species of most genera are LC, so a genus article whose species are
            // nearly all LC is a list of the genus, not of LC species.
            var current = species.Select(t => Current(t.First().Taxon)).OfType<string>().ToList();
            var top = current.GroupBy(c => c).MaxBy(g => g.Count());
            if (top is not null && top.Count() >= CurrentCategoryShare * species.Count && (Threatened.Contains(top.Key) || Extinct.Contains(top.Key))) {
                categories = new HashSet<string> { top.Key };
            } else if (current.Count(Threatened.Contains) >= CurrentCategoryShare * species.Count) {
                categories = Threatened;
            }
        }
        bool InCategories(StatusTaxon t) => categories is null || IsIn(CurrentCode(t), categories);
        int ScopeCount(Func<GroupCategoryCount, int> count, IReadOnlySet<string>? cats) =>
            counts.Where(c => cats is null || IsIn(c.Category, cats)).Sum(count);

        var partial = false;
        // Categories taken from the codes or guessed are dropped when the text lists too few of their
        // taxa to be a list of them; chosen ones are kept.
        if (categories is not null) {
            var listedInCategories = species.Count(t => InCategories(t.First().Taxon));
            if (listedInCategories < ListShare * ScopeCount(c => c.Species, categories)) {
                if (chosen) {
                    // A list of threatened birds of one country: too little of the group's threatened birds.
                    partial = true;
                } else {
                    categories = null;
                }
            }
        }
        if (categories is null) {
            partial = species.Count < ListShare * scopeSpecies;
        }
        if (partial && listed.Count < MinMembersForPartial) {
            return null;
        }
        var speciesInScope = categories is null ? scopeSpecies : ScopeCount(c => c.Species, categories);
        var infraInScope = categories is null ? scopeInfra : ScopeCount(c => c.Infra, categories);
        var infraListed = infra.Count(t => InCategories(t.First().Taxon));
        var infraChecked = infraInScope > 0 && infraListed >= ListShare * infraInScope;

        IReadOnlyList<ListTaxonRow>? missing = null;
        var missingTotal = 0;
        if (!partial || options.ListAnyway) {
            var kinds = infraChecked ? new[] { TaxonKinds.Species, TaxonKinds.Subspecies, TaxonKinds.Variety } : new[] { TaxonKinds.Species };
            var listedIds = listed.Select(t => t.Key).ToHashSet();
            var all = (subset is null ? lookup.TaxaIn(scope, kinds) : subset.Where(r => kinds.Contains(r.Kind)))
                .Where(t => t.Category is not null && !listedIds.Contains(t.TaxonId)
                    && (categories is null || IsIn(GroupList.StatusCode(t), categories)))
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

        var notInRegion = region is null ? [] : inScope.Where(t => !InArea(t.Key)).Select(ToMember).ToList();
        var notInArea = area is null ? []
            : inScope.Where(t => !InArea(t.Key)).Select(t => new ListScopeAreaMember(ToMember(t), listedRecords.GetValueOrDefault(t.Key))).ToList();
        var missingRecords = new Dictionary<long, AreaRecord>();
        if (inArea is not null && missing is not null) {
            var records = inArea.ToDictionary(t => t.Row.TaxonId, t => t.Record);
            foreach (var t in missing) {
                if (records.TryGetValue(t.TaxonId, out var record) && !record.Plain) {
                    missingRecords[t.TaxonId] = record;
                }
            }
        }

        // Species IUCN does not have: only for a list of every category of the whole group (the
        // Catalogue of Life's distributions are not read).
        var extras = options.Extra && categories is null && area is null && region is null && (!partial || options.ListAnyway) ? lookup.ExtraSpeciesIn(scope) : null;
        return new ListScopeResult(scope, path, species.Count(t => InCategories(t.First().Taxon)), species.Count, speciesInScope,
            infraListed, infraInScope, infraChecked, categories,
            codes.Count > 0, partial,
            missing, missingTotal, GroupListQuery.DefaultStyle(PathOf(scope.NodeId)), outside, otherCategory, duplicates) {
            CategoriesChosen = chosen,
            Area = area,
            AreaMode = options.AreaMode,
            NotInArea = notInArea,
            Region = region,
            NotInRegion = notInRegion,
            MissingAreaRecords = missingRecords,
            ListedIn = path.ToDictionary(g => g.NodeId, g => listed.Count(t => t.First().Taxon.Kind == TaxonKinds.Species
                && PathOf(t.First().Taxon.NodeId!.Value).Any(p => p.NodeId == g.NodeId))),
            MissingExtra = extras?
                .Where(e => options.WrittenNames is null || !(options.WrittenNames.Contains(Shared.SiteData.SiteNameKey.Fold(e.ScientificName))
                    || (e.EnwikiTitle is { } title && options.WrittenNames.Contains(Shared.SiteData.SiteNameKey.Fold(title)))))
                .Select(ExtraRow).ToList(),
            ExtraTotal = extras?.Count ?? 0,
        };
    }

    /// Every taxon the comparison counts in the group, listed in the text or not: the group's taxa
    /// (of the area or region when the comparison is of one) with a latest assessment in the list's
    /// categories, species only unless subspecies and varieties were compared, in tree order. Also:
    /// the group's taxa with these ids whatever their category.
    public static List<ListTaxonRow> ComparedTaxa(ListScopeResult result, IListScopeLookup lookup, IReadOnlySet<long>? also = null) {
        string[] kinds = result.InfraChecked ? [TaxonKinds.Species, TaxonKinds.Subspecies, TaxonKinds.Variety] : [TaxonKinds.Species];
        IEnumerable<ListTaxonRow> rows = result.Area is { } area
            ? lookup.TaxaInArea(result.Scope, kinds, area).Where(t => t.Record.Includes(result.AreaMode)).Select(t => t.Row)
            : result.Region is { } region ? lookup.TaxaInRegion(result.Scope, kinds, region)
            : lookup.TaxaIn(result.Scope, kinds);
        var categories = result.Categories;
        return [.. rows.Where(t => t.Category is not null && (categories is null || IsIn(GroupList.StatusCode(t), categories))
            || also?.Contains(t.TaxonId) == true)];
    }

    // An extra species as a list row: no assessment, its CoL or Wikidata name, its article.
    private static ListTaxonRow ExtraRow(ExtraSpeciesRow e) => new(-e.ExtraId, e.ScientificName, TaxonKinds.Species, e.Kingdom, e.Genus,
        e.Epithet, null, null, null, e.CommonNameEn, e.EnwikiTitle, null, null, e.NodeId, e.SortPos, null, null, false, false, null, e.Authority);

    /// The value of the Scope option for a group: "family/Felidae".
    public static string Key(GroupRow group) => $"{group.Rank}/{group.Name}";

    /// The bullet lines for the missing taxa, in the style the Wikipedia lists use for the group. A
    /// taxon whose article is a list ("Carex collifera" redirects to List of Carex species) is not
    /// linked: in that list the link would lead back to the page itself.
    public static string MissingLines(ListScopeResult result) =>
        MissingLines(result.Missing ?? [], result.Style);

    /// The bullet lines for these taxa; {{IUCN status}} only for taxa IUCN has assessed.
    public static string MissingLines(IEnumerable<ListTaxonRow> taxa, SpeciesListStyle style) =>
        string.Join("\n", taxa.Select(t => UnlinkLists(SpeciesListLine.Format(GroupList.Entry(t),
            new SpeciesListLineOptions { Style = style, IncludeStatusTemplate = t.Category is not null }))));

    /// The line with its links to "List of ..." pages replaced by their text.
    internal static string UnlinkLists(string line) =>
        ListLink().Replace(line, m => m.Groups["label"].Success ? m.Groups["label"].Value : m.Groups["target"].Value);

    [GeneratedRegex(@"\[\[(?<target>List of [^|\]]+)(?:\|(?<label>[^\]]+))?\]\]")]
    private static partial Regex ListLink();

    private static string? CurrentCode(StatusTaxon taxon) => taxon.LatestGlobal is { } a
        ? IucnStatusTemplate.ToTemplateCode(a.Category, a.PossiblyExtinct, a.PossiblyExtinctInTheWild)
        : null;

    private static string? Current(StatusTaxon taxon) => Normalize(CurrentCode(taxon));

    /// The category a code is in, for comparing lists: CR(PE) and CR(PEW) are CR, LR/cd and LR/nt
    /// are NT, LR/lc is LC. Null for no code.
    // Whether a taxon's code is one of the categories: by the code itself ("CR(PE)" for a list of
    // possibly extinct taxa), or by its category ("CR(PE)" is in CR).
    private static bool IsIn(string? code, IReadOnlySet<string> categories) =>
        code is not null && (categories.Contains(code.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant())
            || (Normalize(code) is { } c && categories.Contains(c)));

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
