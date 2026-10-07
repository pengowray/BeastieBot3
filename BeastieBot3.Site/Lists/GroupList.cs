using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

// A Wikipedia list of the taxa in a group, in the formats of the generated Wikipedia lists: status
// sections ("== Critically endangered =="), headings for the ranks the reader picks, and one line per
// taxon rendered by SpeciesListLine (BeastieBot3.Shared), the renderer `wikipedia generate-lists`
// uses. The list is built once as blocks, which the page shows both as wikitext and as a preview.

namespace BeastieBot3.Site.Lists;

public enum InfraMode { None, Separate, UnderSpecies }

/// FirstName: by the name each line starts with (the English name, or the scientific name in the
/// scientific-name-first style or when a taxon has no English name).
public enum ListSort { FirstName, ScientificName, CommonName }

/// The status sections of a list, in the order the Wikipedia lists use. Each holds the {{IUCN
/// status}} codes that go in it; LR/nt and LR/cd go with NT and LR/lc with LC, as in the lists.
/// NE holds the taxa with no global assessment (IUCN assessed them only regionally); their lines
/// have no {{IUCN status}} template.
public sealed record StatusSection(string Key, string Heading, IReadOnlyList<string> Codes, string? StatusContext) {
    public static readonly IReadOnlyList<StatusSection> All = [
        new("EX", "Extinct", ["EX"], "EX"),
        new("EW", "Extinct in the wild", ["EW"], "EW"),
        new("CR", "Critically endangered", ["CR", "CR(PE)", "CR(PEW)"], "CR,CR(PE),CR(PEW)"),
        new("EN", "Endangered", ["EN"], "EN"),
        new("VU", "Vulnerable", ["VU"], "VU"),
        new("NT", "Near threatened", ["NT", "LR/nt", "LR/cd"], "NT"),
        new("LC", "Least concern", ["LC", "LR/lc"], "LC"),
        new("DD", "Data deficient", ["DD"], "DD"),
        new(NotEvaluated, "Not evaluated", [NotEvaluated], NotEvaluated),
    ];

    public const string NotEvaluated = "NE";

    /// The sections a list has unless the reader picks others: every category except NE.
    public static readonly IReadOnlySet<string> DefaultKeys = All.Where(s => s.Key != NotEvaluated).Select(s => s.Key).ToHashSet();

    public static readonly IReadOnlySet<string> AllKeys = All.Select(s => s.Key).ToHashSet();

    public static StatusSection? For(string code) => All.FirstOrDefault(s => s.Codes.Contains(code, StringComparer.Ordinal));
}

public sealed record GroupListOptions {
    public SpeciesListStyle Style { get; init; } = SpeciesListStyle.CommonNameFirst;
    /// Ranks that become headings, broad to narrow ("order", "family", or a CoL rank such as "suborder").
    public IReadOnlyList<string> HeadingRanks { get; init; } = [];
    /// Keys of StatusSection.All to include. Empty means all of them, NE included.
    public IReadOnlySet<string> Sections { get; init; } = StatusSection.DefaultKeys;
    public bool ByStatus { get; init; }
    public InfraMode Infra { get; init; } = InfraMode.None;
    public bool Subpopulations { get; init; }
    public ListSort Sort { get; init; } = ListSort.FirstName;
    public bool StatusTemplate { get; init; } = true;

    /// The sections that are included: all of them when none is picked.
    public IReadOnlySet<string> IncludedSections => Sections.Count == 0 ? StatusSection.AllKeys : Sections;
    /// The "Members of the [[Felidae]] family are called cats." line under a rank heading.
    public bool HeadingNames { get; init; }
    /// Which sources the species come from, and which is preferred (ListSources.cs).
    public ListSourceOptions Sources { get; init; } = ListSourceOptions.Default;
    /// The authority and the reference after each line (ListLineOptions.cs).
    public ListLineOptions Line { get; init; } = ListLineOptions.Default;
    /// Wikitext level of the top headings: 2 is "== ... ==".
    public int TopLevel { get; init; } = 2;
}

public abstract record ListBlock;

/// A heading. Level is the wikitext level (2 to 6). Group is null for a status section heading.
public sealed record HeadingBlock(int Level, string Text, GroupRow? Group, StatusSection? Section) : ListBlock;

/// The "Members of ... are called ..." line under a rank heading.
public sealed record GroupNameBlock(GroupRow Group) : ListBlock;

/// A bold label before the subspecies, varieties or subpopulations of a heading ("Subspecies").
public sealed record LabelBlock(string Text) : ListBlock;

/// One taxon. Nested: shown under its species ("**" in wikitext).
public sealed record LineBlock(ListTaxonRow Taxon, SpeciesListEntry Entry, bool Nested) : ListBlock;

public sealed record GroupListResult(IReadOnlyList<ListBlock> Blocks, int LineCount, int TemplateCount, IReadOnlyList<string> SkippedRanks);

public static class GroupList {
    /// The most lines a list may have: about the number of {{IUCN status}} templates one Wikipedia
    /// page can hold.
    public const int MaxLines = 3600;

    /// The lowest wikitext heading level.
    private const int MaxLevel = 6;

    /// The list's blocks. Taxa must be in tree order (tree_pos); groups are the groups inside the
    /// listed group, by node id.
    public static GroupListResult Build(IReadOnlyList<ListTaxonRow> taxa, IReadOnlyDictionary<int, GroupRow> groups,
        GroupListOptions options) {
        var kept = taxa.Where(t => Included(t, options)).ToList();
        var blocks = new List<ListBlock>();
        var level = Math.Clamp(options.TopLevel, 2, MaxLevel);
        var rankLevels = MaxLevel - level + 1 - (options.ByStatus ? 1 : 0);
        var ranks = options.HeadingRanks.Take(Math.Max(0, rankLevels)).ToList();
        var skipped = options.HeadingRanks.Skip(ranks.Count).ToList();

        if (options.ByStatus) {
            foreach (var section in StatusSection.All.Where(s => options.IncludedSections.Contains(s.Key))) {
                var inSection = kept.Where(t => section.Codes.Contains(StatusCode(t), StringComparer.Ordinal)).ToList();
                if (inSection.Count == 0) {
                    continue;
                }
                blocks.Add(new HeadingBlock(level, section.Heading, null, section));
                AddGroups(blocks, inSection, groups, ranks, level + 1, options);
            }
        } else {
            AddGroups(blocks, kept, groups, ranks, level, options);
        }

        var lines = blocks.OfType<LineBlock>().ToList();
        var templates = options.StatusTemplate ? lines.Count(l => l.Taxon.Category is not null) : 0;
        return new GroupListResult(blocks, lines.Count, templates, skipped);
    }

    /// How many lines the list would have, from the counts of the group, without reading the taxa.
    public static int CountLines(GroupRow group, IReadOnlyList<GroupCategoryCount> counts, GroupListOptions options) =>
        CountLinesBySection(group, counts, options).Where(s => options.IncludedSections.Contains(s.Key)).Sum(s => s.Value);

    /// How many lines each status section would have, by section key, for every section whether or
    /// not the options include it. The taxa with no global assessment (NE) are the group's totals
    /// less the counted ones; extraNotEvaluated adds the species from CoL and Wikidata to NE.
    public static Dictionary<string, int> CountLinesBySection(GroupRow group, IReadOnlyList<GroupCategoryCount> counts,
        GroupListOptions options, int extraNotEvaluated = 0) {
        var notEvaluated = new GroupCategoryCount(StatusSection.NotEvaluated,
            group.SpeciesCount - counts.Sum(c => c.Species),
            group.InfraCount - counts.Sum(c => c.Infra),
            group.SubpopulationCount - counts.Sum(c => c.Subpopulations));
        var bySection = StatusSection.All.ToDictionary(s => s.Key, _ => 0);
        bySection[StatusSection.NotEvaluated] = extraNotEvaluated;
        foreach (var count in counts.Append(notEvaluated)) {
            if (StatusSection.For(count.Category) is not { } section) {
                continue;
            }
            var lines = count.Species;
            if (options.Infra != InfraMode.None) {
                lines += count.Infra;
            }
            if (options.Subpopulations) {
                lines += count.Subpopulations;
            }
            bySection[section.Key] += lines;
        }
        return bySection;
    }

    /// How many headings each rank would add to the list, by rank, from the counts of the groups
    /// inside the listed group: one for each group of the rank that has a line in an included
    /// section, or with status sections, one in each included section the group has lines in.
    /// extraNotEvaluated gives the species from CoL and Wikidata in each group, by node id. Ranks
    /// left out because headings would go deeper than level 6 are counted as if they fit.
    public static Dictionary<string, int> CountHeadings(IEnumerable<GroupRow> groups,
        IReadOnlyDictionary<int, IReadOnlyList<GroupCategoryCount>> counts, IReadOnlyDictionary<int, int> extraNotEvaluated,
        GroupListOptions options) {
        var byRank = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var group in groups) {
            var sections = CountLinesBySection(group, counts.GetValueOrDefault(group.NodeId) ?? [], options,
                    extraNotEvaluated.GetValueOrDefault(group.NodeId))
                .Count(s => s.Value > 0 && options.IncludedSections.Contains(s.Key));
            byRank[group.Rank] = byRank.GetValueOrDefault(group.Rank) + (options.ByStatus ? sections : Math.Min(sections, 1));
        }
        return byRank;
    }

    /// The {{IUCN status}} code of the taxon's latest global assessment; NE when it has none.
    public static string StatusCode(ListTaxonRow taxon) => taxon.Category is { } category
        ? IucnStatusTemplate.ToTemplateCode(category, taxon.PossiblyExtinct, taxon.PossiblyExtinctInTheWild)
        : StatusSection.NotEvaluated;

    private static bool Included(ListTaxonRow taxon, GroupListOptions options) {
        if (StatusSection.For(StatusCode(taxon)) is not { } section || !options.IncludedSections.Contains(section.Key)) {
            return false;
        }
        return taxon.Kind switch {
            TaxonKinds.Species => true,
            TaxonKinds.Subpopulation => options.Subpopulations,
            _ => options.Infra != InfraMode.None,
        };
    }

    // Groups the taxa under a heading for each value of the first rank in ranks, then the next rank
    // inside each, in tree order. A taxon with no group of a rank stays above that rank's headings.
    private static void AddGroups(List<ListBlock> blocks, List<ListTaxonRow> taxa, IReadOnlyDictionary<int, GroupRow> groups,
        IReadOnlyList<string> ranks, int level, GroupListOptions options) {
        if (ranks.Count == 0) {
            AddLines(blocks, taxa, options);
            return;
        }
        var rank = ranks[0];
        var rest = ranks.Skip(1).ToList();
        var ungrouped = new List<ListTaxonRow>();
        var byGroup = new List<(GroupRow Group, List<ListTaxonRow> Taxa)>();
        foreach (var taxon in taxa) {
            var group = AncestorAt(taxon.NodeId, rank, groups);
            if (group is null) {
                ungrouped.Add(taxon);
            } else if (byGroup.Count > 0 && byGroup[^1].Group.NodeId == group.NodeId) {
                byGroup[^1].Taxa.Add(taxon);
            } else {
                byGroup.Add((group, [taxon]));
            }
        }
        if (ungrouped.Count > 0) {
            AddGroups(blocks, ungrouped, groups, rest, level, options);
        }
        foreach (var (group, members) in byGroup) {
            blocks.Add(new HeadingBlock(level, HeadingText(group), group, null));
            if (options.HeadingNames && HasSentenceName(group)) {
                blocks.Add(new GroupNameBlock(group));
            }
            AddGroups(blocks, members, groups, rest, level + 1, options);
        }
    }

    // The lines of one heading: species, with their subspecies, varieties and subpopulations nested
    // under them (InfraMode.UnderSpecies) or listed after them under a label. A taxon whose species
    // is not in the list is always listed after them.
    private static void AddLines(List<ListBlock> blocks, List<ListTaxonRow> taxa, GroupListOptions options) {
        var species = taxa.Where(t => t.Kind == TaxonKinds.Species).ToList();
        var others = taxa.Where(t => t.Kind != TaxonKinds.Species).ToList();
        var speciesIds = species.Select(s => s.TaxonId).ToHashSet();
        var nested = (options.Infra == InfraMode.UnderSpecies ? others : [])
            .Where(o => o.ParentTaxonId is { } p && speciesIds.Contains(p))
            .ToLookup(o => o.ParentTaxonId!.Value);
        var nestedIds = nested.SelectMany(g => g).Select(o => o.TaxonId).ToHashSet();

        foreach (var taxon in Sorted(species, options)) {
            blocks.Add(new LineBlock(taxon, Entry(taxon), Nested: false));
            foreach (var child in Sorted(nested[taxon.TaxonId].ToList(), options)) {
                blocks.Add(new LineBlock(child, Entry(child), Nested: true));
            }
        }
        var rest = others.Where(o => !nestedIds.Contains(o.TaxonId)).ToList();
        var infra = rest.Where(r => r.Kind != TaxonKinds.Subpopulation).ToList();
        var subpopulations = rest.Where(r => r.Kind == TaxonKinds.Subpopulation).ToList();
        foreach (var (label, members) in new[] { (LabelFor(infra), infra), ("Subpopulations", subpopulations) }) {
            if (members.Count == 0) {
                continue;
            }
            if (species.Count > 0 || members != infra && infra.Count > 0) {
                blocks.Add(new LabelBlock(label));
            }
            foreach (var taxon in Sorted(members, options)) {
                blocks.Add(new LineBlock(taxon, Entry(taxon), Nested: false));
            }
        }
    }

    private static string LabelFor(IReadOnlyList<ListTaxonRow> infra) {
        var subspecies = infra.Any(t => t.Kind == TaxonKinds.Subspecies);
        var varieties = infra.Any(t => t.Kind == TaxonKinds.Variety);
        return subspecies && varieties ? "Subspecies and varieties" : varieties ? "Varieties" : "Subspecies";
    }

    // By the English name (the scientific name when there is none), or by the scientific name, which
    // is tree order.
    private static IEnumerable<ListTaxonRow> Sorted(List<ListTaxonRow> taxa, GroupListOptions options) {
        var byCommon = options.Sort == ListSort.CommonName
            || (options.Sort == ListSort.FirstName && options.Style != SpeciesListStyle.ScientificNameFirst);
        return byCommon
            ? taxa.OrderBy(t => t.CommonNameEn ?? t.ScientificName, StringComparer.OrdinalIgnoreCase).ThenBy(t => t.TreePos)
            : taxa.OrderBy(t => t.TreePos);
    }

    public static SpeciesListLineOptions LineOptions(GroupListOptions options, string? statusContext, bool assessed = true) => new() {
        Style = options.Style,
        IncludeStatusTemplate = options.StatusTemplate && assessed,
        StatusContext = statusContext,
        Authority = options.Line.Authority,
    };

    public static SpeciesListEntry Entry(ListTaxonRow taxon) => new() {
        ScientificName = SpeciesName(taxon),
        Genus = taxon.Genus,
        SpeciesEpithet = taxon.SpeciesEpithet,
        InfraType = taxon.InfraRank,
        InfraName = taxon.InfraName,
        Kingdom = taxon.Kingdom,
        SubpopulationName = taxon.SubpopulationName,
        CommonName = taxon.CommonNameEn,
        ArticleTitle = taxon.ListArticleTitle,
        ParentSpeciesArticleTitle = taxon.ListParentArticleTitle,
        StatusCode = StatusCode(taxon),
        PossiblyExtinct = taxon.PossiblyExtinct,
        PossiblyExtinctInTheWild = taxon.PossiblyExtinctInTheWild,
        TaxonId = taxon.TaxonId,
        AssessmentId = taxon.AssessmentId ?? 0,
        YearPublished = taxon.YearPublished?.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Authority = taxon.Authority,
    };

    // A subpopulation's line shows the species' name and then the subpopulation in brackets; IUCN
    // writes the subpopulation into the scientific name ("Lycaon pictus North Africa subpopulation").
    private static string SpeciesName(ListTaxonRow taxon) =>
        taxon.SubpopulationName is { Length: > 0 } subpopulation
        && taxon.ScientificName.EndsWith(" " + subpopulation, StringComparison.Ordinal)
            ? taxon.ScientificName[..^(subpopulation.Length + 1)]
            : taxon.ScientificName;

    /// "Family Felidae"; the name alone for a Catalogue of Life group shown without its rank.
    public static string HeadingText(GroupRow group) =>
        group.ShowRank && group.Rank != "unranked" ? $"{Capitalize(group.Rank)} {group.Name}" : group.Name;

    public static string Capitalize(string text) =>
        text.Length == 0 ? text : char.ToUpperInvariant(text[0]) + text[1..];

    private static GroupRow? AncestorAt(int nodeId, string rank, IReadOnlyDictionary<int, GroupRow> groups) {
        for (var at = groups.GetValueOrDefault(nodeId); at is not null;
             at = at.ParentNodeId is { } parent ? groups.GetValueOrDefault(parent) : null) {
            if (at.Rank == rank) {
                return at;
            }
        }
        return null;
    }

    // ------------------------------------------------------------ wikitext

    /// The list as wikitext. Lines are rendered here with the options' status context per section.
    /// references (ListReferences.Build) puts a <ref> after the lines it has, in the form
    /// options.Line.References asks for.
    public static string ToWikitext(GroupListResult list, GroupListOptions options,
        IReadOnlyDictionary<long, LineReference>? references = null) {
        var sb = new StringBuilder();
        var refs = new ListReferenceWriter(options.Line.References, references);
        string? context = null;
        var previousWasLine = false;
        foreach (var block in list.Blocks) {
            switch (block) {
                case HeadingBlock heading:
                    if (heading.Section is { } section) {
                        context = section.StatusContext;
                    }
                    // A blank line between a list and the next heading, as in the generated lists.
                    if (previousWasLine) {
                        sb.Append('\n');
                    }
                    var marks = new string('=', heading.Level);
                    sb.Append(marks).Append(' ').Append(heading.Text).Append(' ').Append(marks).Append('\n');
                    previousWasLine = false;
                    break;
                case GroupNameBlock name:
                    sb.Append(GroupNameSentence(name.Group)).Append('\n');
                    previousWasLine = false;
                    break;
                case LabelBlock label:
                    if (previousWasLine) {
                        sb.Append('\n');
                    }
                    sb.Append("'''").Append(label.Text).Append("'''\n");
                    previousWasLine = false;
                    break;
                case LineBlock line:
                    var lineOptions = LineOptions(options, options.ByStatus ? context : null, line.Taxon.Category is not null);
                    sb.Append(line.Nested
                        ? "*" + SpeciesListLine.FormatInfraspecificUnderSpecies(line.Entry, lineOptions)
                        : SpeciesListLine.Format(line.Entry, lineOptions));
                    refs.AppendRef(sb, line.Taxon.TaxonId);
                    sb.Append('\n');
                    previousWasLine = true;
                    break;
            }
        }
        refs.AppendRefList(sb);
        return sb.ToString().TrimEnd('\n');
    }

    /// Whether the group's English name can go in a "Members of ..." line: only a name from the rules
    /// files, which give plurals ("cats"). A name from a Wikipedia redirect is an article title
    /// ("Eared seal"), and "are called Eared seal" reads wrong.
    public static bool HasSentenceName(GroupRow group) =>
        group.CommonNameEn is not null && group.CommonNameSource == "rules";

    /// "Members of the [[Felidae]] family are called cats.", as the Wikipedia list headings write it
    /// (HeadingFormatter in the CLI): the rank is left out for a group shown without its rank.
    public static string GroupNameSentence(GroupRow group) {
        var link = group.EnwikiTitle is { } title && !string.Equals(title, group.Name, StringComparison.OrdinalIgnoreCase)
            ? $"[[{title}|{group.Name}]]"
            : $"[[{group.Name}]]";
        return group.ShowRank && group.Rank != "unranked"
            ? $"Members of the {link} {group.Rank} are called {group.CommonNameEn}."
            : $"Members of {link} are called {group.CommonNameEn}.";
    }
}
