using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Pages;

/// One group directly under the page's group, with its counts.
public sealed record ChildGroup(GroupRow Group, int Threatened, int Extinct);

/// One rank the reader can pick as a heading in the list.
/// Headings: how many headings the rank adds to the list with the other options as they are; null
/// when the rank adds none of its own (genus in species tables, whose tables are one per genus).
public sealed record HeadingChoice(string Rank, bool FromCol, bool Picked, int? Headings);

/// What the two pages of a group share: the group page (/taxa/{rank}/{name}, GroupModel) and its
/// list page (/taxa/{rank}/{name}/list, GroupListModel). When the rank and name match two or more
/// groups (a genus name used in two kingdoms), both pages list them; ?kingdom= or ?parent= picks one.
public abstract class GroupPageModel : PageModel {
    protected readonly SiteDatabase _db;
    protected readonly SiteQueries _queries;
    protected readonly SiteOptions _options;

    protected GroupPageModel(SiteDatabase db, SiteQueries queries, Microsoft.Extensions.Options.IOptions<SiteOptions> options) {
        _db = db;
        _queries = queries;
        _options = options.Value;
    }

    public string RequestedRank { get; private set; } = string.Empty;
    public string RequestedName { get; private set; } = string.Empty;
    public string? Version { get; private set; }

    public GroupRow? Group { get; private set; }
    /// The groups the rank and name match, when more than one does and nothing picks one.
    public IReadOnlyList<(GroupRow Group, IReadOnlyList<GroupRow> Path)> Candidates { get; private set; } = [];
    /// From the kingdom down to the group itself.
    public IReadOnlyList<GroupRow> Path { get; private set; } = [];
    public IReadOnlyList<GroupCategoryCount> Counts { get; private set; } = [];

    /// The address of this kind of page for a group (the group page or its list page), for the
    /// links to each group when the rank and name match two or more.
    public abstract string UrlOf(GroupRow group);

    /// Finds the group. False when no group or more than one matches: the page then says so (404
    /// when none matches) or lists the candidates.
    protected bool LoadGroup(string rank, string name) {
        RequestedRank = rank;
        RequestedName = name;
        Version = _db.Snapshot?.IucnRelease;
        var matches = _queries.FindGroups(rank, name);
        if (matches.Count == 0) {
            Response.StatusCode = StatusCodes.Status404NotFound;
            return false;
        }
        var group = Pick(matches);
        if (group is null) {
            Candidates = matches.Select(m => (m, _queries.GetGroupPath(m.NodeId))).ToList();
            return false;
        }
        Group = group;
        Path = _queries.GetGroupPath(group.NodeId);
        Counts = _queries.GetGroupCounts(group.NodeId);
        return true;
    }

    // A ?kingdom= or ?parent= (the name of any group above it) that leaves one match picks it.
    private GroupRow? Pick(IReadOnlyList<GroupRow> matches) {
        if (matches.Count == 1) {
            return matches[0];
        }
        IEnumerable<GroupRow> left = matches;
        if (SiteEndpoints.FirstQueryValue(Request, "kingdom") is { Length: > 0 } kingdom) {
            left = left.Where(m => string.Equals(m.Kingdom, kingdom, StringComparison.OrdinalIgnoreCase));
        }
        if (SiteEndpoints.FirstQueryValue(Request, "parent") is { Length: > 0 } parent) {
            left = left.Where(m => _queries.GetGroupPath(m.NodeId)
                .Any(p => p.NodeId != m.NodeId && string.Equals(p.Name, parent, StringComparison.OrdinalIgnoreCase)));
        }
        var list = left.ToList();
        return list.Count == 1 && list.Count < matches.Count ? list[0] : null;
    }

    protected static int Sum(IReadOnlyList<GroupCategoryCount> counts, IReadOnlySet<string> codes) =>
        counts.Where(c => codes.Contains(c.Category)).Sum(c => c.Species);

    public int CountOf(IReadOnlySet<string> codes) => Sum(Counts, codes);
}

/// A group page (/taxa/{rank}/{name}): the group's place in the classification, its names, its
/// counts by category and the groups in it, with a link to its list page. An address with list
/// options (from before the list had its own page) is sent to the list page.
[OutputCache(PolicyName = SiteCachePolicies.Group)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class GroupModel : GroupPageModel {
    public GroupModel(SiteDatabase db, SiteQueries queries, Microsoft.Extensions.Options.IOptions<SiteOptions> options) : base(db, queries, options) {
    }

    public IReadOnlyList<ChildGroup> Children { get; private set; } = [];
    public IReadOnlyList<string> ColNames { get; private set; } = [];
    /// The group's names from English Wikipedia (its article's title and the redirects to it).
    public IReadOnlyList<string> WikipediaNames { get; private set; } = [];
    /// The search text (q) when search sent the reader here and the text is one of the group's names,
    /// for the link back to all the results.
    public string? ArrivedQuery { get; private set; }

    public override string UrlOf(GroupRow group) => SiteUrls.Group(group);

    public IActionResult OnGet(string rank, string name) {
        if (Request.Query.Keys.Any(k => GroupListModel.ListQueryKeys.Contains(k))) {
            var query = QueryString.Create(Request.Query.Where(p => p.Key != "q"));
            return Redirect(GroupListModel.PathFor(rank, name, query.ToUriComponent()));
        }
        if (!LoadGroup(rank, name)) {
            return Page();
        }
        var group = Group!;
        // Search engines get the page without ?q=.
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, SiteUrls.Group(group));
        ColNames = _queries.GetGroupColNames(group.NodeId);
        WikipediaNames = _queries.GetGroupWikipediaNames(group.NodeId);
        ArrivedQuery = ArrivalText(SiteEndpoints.FirstQueryValue(Request, "q"), group, WikipediaNames);
        var children = _queries.GetChildGroups(group.NodeId);
        var childCounts = _queries.GetGroupCounts(children.Select(c => c.NodeId).ToList());
        Children = children.Select(c => {
            var counts = childCounts.GetValueOrDefault(c.NodeId) ?? [];
            return new ChildGroup(c, Sum(counts, CategoryGroups.Threatened), Sum(counts, CategoryGroups.Extinct));
        }).ToList();
        return Page();
    }

    /// The search text to link back to: only one of the group's names (its own name or a name from
    /// English Wikipedia, ignoring case and accents), so the link cannot put arbitrary text on the page.
    public static string? ArrivalText(string? q, GroupRow group, IReadOnlyList<string> wikipediaNames) {
        var text = SiteEndpoints.NormalizeQuery(q);
        if (text.Length < 2) {
            return null;
        }
        var key = SiteNameKey.Fold(text);
        return key == SiteNameKey.Fold(group.Name) || wikipediaNames.Any(n => SiteNameKey.Fold(n) == key) ? text : null;
    }
}

/// The list page of a group (/taxa/{rank}/{name}/list): a Wikipedia list of the group's taxa, or
/// species tables, with options.
[OutputCache(PolicyName = SiteCachePolicies.GroupList)]
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class GroupListModel : GroupPageModel {
    public GroupListModel(SiteDatabase db, SiteQueries queries, Microsoft.Extensions.Options.IOptions<SiteOptions> options) : base(db, queries, options) {
    }

    /// The query parameters of the list options. A group page address with any of them is sent to
    /// the group's list page.
    public static readonly IReadOnlySet<string> ListQueryKeys =
        new HashSet<string>([.. GroupListQuery.Keys, .. SpeciesTableQuery.Keys], StringComparer.OrdinalIgnoreCase);

    /// The list page of the group a group page address names: "/taxa/family/felidae/list" with the query
    /// (which starts with "?" or is empty).
    public static string PathFor(string rank, string name, string query = "") =>
        $"/taxa/{Uri.EscapeDataString(rank)}/{Uri.EscapeDataString(name)}/list{query}";

    public override string UrlOf(GroupRow group) => SiteUrls.GroupList(group);

    public GroupListOptions Options { get; private set; } = new();
    public GroupListOptions DefaultOptions { get; private set; } = new();
    public IReadOnlyList<HeadingChoice> HeadingChoices { get; private set; } = [];
    /// How many lines each status section would add to the list, by StatusSection key.
    public IReadOnlyDictionary<string, int> SectionLines { get; private set; } = new Dictionary<string, int>();
    /// Lines the list has with these options (from the counts, before reading the taxa).
    public int LineCount { get; private set; }
    /// The most lines (rows for species tables) a list may have.
    public int MaxLines => Table.IsTable ? SpeciesTable.MaxRows(Table)
        : Options.Line.HasReferences ? ListReferences.MaxLines : GroupList.MaxLines;
    public bool TooLong => LineCount > MaxLines;
    public GroupListResult? List { get; private set; }
    public string Wikitext { get; private set; } = string.Empty;
    /// The list's rows from the picked sources and the notices about possible duplicates; null for
    /// a list of IUCN's taxa only.
    public ListSourceMergeResult? Sources { get; private set; }
    /// The notices of Sources arranged for the possible-duplicates panel.
    public ListNoticeGroups? NoticeGroups { get; private set; }

    /// The list type and the species table options (SpeciesTableQuery).
    public SpeciesTableOptions Table { get; private set; } = new();
    /// The species tables, when the list type is tables.
    public SpeciesTableResult? Tables { get; private set; }

    /// This page's address with the current options, for the address bar after a live update.
    public string CurrentOptionsUrl =>
        SiteUrls.GroupList(Group!, SpeciesTableQuery.Append(GroupListQuery.Write(Options, DefaultOptions), Table));

    public IActionResult OnGet(string rank, string name) {
        if (!LoadGroup(rank, name)) {
            return Page();
        }
        var group = Group!;
        // Every set of list options has its own address; search engines get the page without them.
        ViewData["Canonical"] = SiteUrls.Absolute(_options.BaseUrl, Request, SiteUrls.GroupList(group));
        var ranks = _queries.GetRanksWithin(group);
        DefaultOptions = GroupListQuery.Defaults(Path);
        DefaultOptions = DefaultOptions with { HeadingRanks = DefaultOptions.HeadingRanks.Where(r => ranks.Any(x => x.Rank == r)).ToList() };
        Table = SpeciesTableQuery.Read(Request.Query);
        if (Table.IsTable) {
            // The featured lists' tables include the species IUCN has not evaluated.
            DefaultOptions = DefaultOptions with { Sections = StatusSection.AllKeys };
        }
        Options = GroupListQuery.Read(Request.Query, DefaultOptions, ranks.Select(r => r.Rank).ToList());
        // Species from CoL and Wikidata go in the bulleted list only; the species tables list IUCN's taxa.
        var extraCounts = !Table.IsTable && Options.Sources.HasOtherSources ? _queries.GetExtraSpeciesCounts(group.NodeId) : null;
        var countOptions = Table.IsTable ? SpeciesTable.ListOptions(Options) : Options;
        LineCount = GroupList.CountLines(group, Counts, countOptions) + GroupListSources.ExtraLines(extraCounts, Options);
        SectionLines = GroupList.CountLinesBySection(group, Counts, countOptions, extraCounts?.For(Options.Sources) ?? 0);
        var groupsWithin = _queries.GetGroupsWithin(group);
        var headings = GroupList.CountHeadings(groupsWithin, _queries.GetGroupCountsWithin(group),
            extraCounts is null ? new Dictionary<int, int>() : _queries.GetExtraSpeciesCountsWithin(group, Options.Sources), countOptions);
        HeadingChoices = ranks
            .OrderBy(r => GroupListQuery.RankIndex(r.Rank)).ThenBy(r => r.MinDepth)
            .Select(r => new HeadingChoice(r.Rank, r.OnlyFromCol, Options.HeadingRanks.Contains(r.Rank),
                Table.IsTable && r.Rank == "genus" ? null : headings.GetValueOrDefault(r.Rank)))
            .ToList();
        if (!TooLong && LineCount > 0 && Table.IsTable) {
            BuildTables(group, groupsWithin);
        } else if (!TooLong && LineCount > 0) {
            var kinds = new List<string> { TaxonKinds.Species };
            if (Options.Infra != InfraMode.None) {
                kinds.Add(TaxonKinds.Subspecies);
                kinds.Add(TaxonKinds.Variety);
            }
            if (Options.Subpopulations) {
                kinds.Add(TaxonKinds.Subpopulation);
            }
            var taxa = _queries.GetListTaxa(group, kinds);
            if (GroupListSources.Active(Options)) {
                Sources = GroupListSources.Merge(_queries, group, extraCounts, taxa, Options);
                NoticeGroups = ListNoticeGroups.Build(Sources);
                taxa = Sources.Rows;
            }
            var groups = groupsWithin.Append(group).ToDictionary(g => g.NodeId);
            List = GroupList.Build(taxa, groups, Options);
            var references = Options.Line.HasReferences
                ? ListReferences.Build(List, new ListReferenceQueries(_db).GetCitations(group), Sources, Options.Line.Template,
                    DateOnly.FromDateTime(DateTime.UtcNow))
                : null;
            Wikitext = GroupList.ToWikitext(List, Options, references);
        }
        return Page();
    }

    private void BuildTables(GroupRow group, IReadOnlyList<GroupRow> groupsWithin) {
        var listOptions = SpeciesTable.ListOptions(Options);
        var taxa = _queries.GetListTaxa(group, [TaxonKinds.Species]);
        var groups = groupsWithin.Append(group).ToDictionary(g => g.NodeId);
        List = GroupList.Build(taxa, groups, listOptions);
        Tables = SpeciesTable.Build(List, groups, new SpeciesTableQueries(_db).GetExtras(group), Table);
        Wikitext = SpeciesTable.ToWikitext(Tables, Table);
    }
}

/// Category codes ({{IUCN status}} codes) counted together on group pages.
public static class CategoryGroups {
    public static readonly IReadOnlySet<string> Threatened = new HashSet<string>(StringComparer.Ordinal) { "CR", "CR(PE)", "CR(PEW)", "EN", "VU" };
    public static readonly IReadOnlySet<string> Extinct = new HashSet<string>(StringComparer.Ordinal) { "EX", "EW" };
}
