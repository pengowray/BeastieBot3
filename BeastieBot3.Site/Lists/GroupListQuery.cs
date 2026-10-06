using System.Globalization;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using Microsoft.Extensions.Primitives;

// The list options of a group page, read from and written to its query string. The page answers GET
// only, so the options live in the address and a list can be linked.
//
//   style   sci | common | commononly   (SpeciesListStyle A, B, C)
//   h       a rank to use as a heading; repeated, broad to narrow ("h=order&h=family"); "none" for no headings
//   cat     a status section to include (EX EW CR EN VU NT LC DD NE); repeated; all but NE when
//           absent; all, NE included, when present with no section ("cat=")
//   status  1: a status section for each category (0: none, the default)
//   infra   none | separate | under    (subspecies and varieties)
//   subpop  1: include subpopulations
//   sort    first | sci | common
//   tpl     0: no {{IUCN status}} templates
//   names   1: "Members of ... are called ..." lines (0: none, the default)
//   level   2 to 4: wikitext level of the top headings

namespace BeastieBot3.Site.Lists;

public static class GroupListQuery {
    public static readonly string[] Keys = ["style", "h", "cat", "status", "infra", "subpop", "sort", "tpl", "names", "level", "src", "prefer"];

    /// Ranks in the order they nest, broad to narrow, for ordering the heading choices. A rank not
    /// listed goes after the listed ranks above it, by depth.
    public static readonly string[] RankOrder = [
        "kingdom", "subkingdom", "phylum", "subphylum", "infraphylum", "superclass", "class", "subclass", "infraclass",
        "subterclass", "superorder", "order", "suborder", "infraorder", "parvorder", "nanorder", "superfamily", "epifamily",
        "family", "subfamily", "supertribe", "tribe", "subtribe", "infratribe", "genus",
    ];

    private static readonly string[] IucnRanks = ["kingdom", "phylum", "class", "order", "family", "genus"];

    /// The options a group's list starts with: a style and headings that suit the group (see
    /// DefaultStyle and DefaultHeadings), every category but NE, no status sections, species only.
    public static GroupListOptions Defaults(IReadOnlyList<GroupRow> path) {
        var style = DefaultStyle(path);
        return new GroupListOptions {
            Style = style,
            HeadingRanks = DefaultHeadings(path[^1]),
        };
    }

    /// The style the Wikipedia lists use for such a group: scientific name first for plants, fungi,
    /// chromists and invertebrates, common name only for mammals and birds, common name first for
    /// other vertebrates.
    public static SpeciesListStyle DefaultStyle(IReadOnlyList<GroupRow> path) {
        var kingdom = path.FirstOrDefault(g => g.Rank == "kingdom")?.Name;
        var phylum = path.FirstOrDefault(g => g.Rank == "phylum")?.Name;
        var className = path.FirstOrDefault(g => g.Rank == "class")?.Name;
        if (!string.Equals(kingdom, "Animalia", StringComparison.OrdinalIgnoreCase)) {
            return SpeciesListStyle.ScientificNameFirst;
        }
        if (phylum is not null && !string.Equals(phylum, "Chordata", StringComparison.OrdinalIgnoreCase)) {
            return SpeciesListStyle.ScientificNameFirst;
        }
        if (className is "Mammalia" or "Aves") {
            return SpeciesListStyle.CommonNameOnly;
        }
        return SpeciesListStyle.CommonNameFirst;
    }

    /// Up to two IUCN ranks from class, order and family, below the group's rank: order and family
    /// for a class, family for an order, none for a family or genus.
    public static IReadOnlyList<string> DefaultHeadings(GroupRow group) {
        var groupRank = Array.IndexOf(IucnRanks, group.Rank);
        if (groupRank < 0) {
            // A CoL group: the IUCN rank below it is the first one deeper in RankOrder.
            var at = Array.IndexOf(RankOrder, group.Rank);
            groupRank = at < 0 ? IucnRanks.Length : IucnRanks.Count(r => Array.IndexOf(RankOrder, r) < at) - 1;
        }
        return new[] { "class", "order", "family" }
            .Where(r => Array.IndexOf(IucnRanks, r) > groupRank)
            .Take(2)
            .ToList();
    }

    public static GroupListOptions Read(IQueryCollection query, GroupListOptions defaults, IReadOnlyCollection<string> availableRanks) {
        var options = defaults;
        if (First(query, "style") is { } style) {
            options = options with {
                Style = style switch {
                    "sci" => SpeciesListStyle.ScientificNameFirst,
                    "commononly" => SpeciesListStyle.CommonNameOnly,
                    "common" => SpeciesListStyle.CommonNameFirst,
                    _ => options.Style,
                },
            };
        }
        if (query.TryGetValue("h", out var headings)) {
            var picked = Values(headings).Where(r => r != "none" && availableRanks.Contains(r)).Distinct().ToList();
            options = options with { HeadingRanks = picked.OrderBy(RankIndex).ToList() };
        } else {
            options = options with { HeadingRanks = options.HeadingRanks.Where(availableRanks.Contains).ToList() };
        }
        if (query.TryGetValue("cat", out var cats)) {
            // None ticked means all of them.
            var keys = Values(cats).Where(c => StatusSection.AllKeys.Contains(c)).ToHashSet();
            options = options with { Sections = keys.Count == 0 ? StatusSection.AllKeys : keys };
        }
        options = First(query, "status") switch {
            "1" => options with { ByStatus = true },
            "0" => options with { ByStatus = false },
            _ => options,
        };
        options = First(query, "infra") switch {
            "separate" => options with { Infra = InfraMode.Separate },
            "under" => options with { Infra = InfraMode.UnderSpecies },
            "none" => options with { Infra = InfraMode.None },
            _ => options,
        };
        if (First(query, "subpop") == "1") {
            options = options with { Subpopulations = true };
        }
        options = First(query, "sort") switch {
            "sci" => options with { Sort = ListSort.ScientificName },
            "common" => options with { Sort = ListSort.CommonName },
            "first" => options with { Sort = ListSort.FirstName },
            _ => options,
        };
        if (First(query, "tpl") == "0") {
            options = options with { StatusTemplate = false };
        }
        options = First(query, "names") switch {
            "1" => options with { HeadingNames = true },
            "0" => options with { HeadingNames = false },
            _ => options,
        };
        if (First(query, "level") is { } level && int.TryParse(level, NumberStyles.None, CultureInfo.InvariantCulture, out var n)) {
            options = options with { TopLevel = Math.Clamp(n, 2, 4) };
        }
        return ReadSources(query, options);
    }

    /// The query string for these options: only the values that differ from the defaults, so the
    /// address of an untouched list is the group's own address.
    public static string Write(GroupListOptions options, GroupListOptions defaults) {
        var parts = new List<string>();
        if (options.Style != defaults.Style) {
            parts.Add("style=" + StyleKey(options.Style));
        }
        if (!options.HeadingRanks.SequenceEqual(defaults.HeadingRanks)) {
            if (options.HeadingRanks.Count == 0) {
                parts.Add("h=none");
            }
            parts.AddRange(options.HeadingRanks.Select(r => "h=" + Uri.EscapeDataString(r)));
        }
        if (!options.IncludedSections.SetEquals(defaults.IncludedSections)) {
            parts.AddRange(StatusSection.All.Where(s => options.IncludedSections.Contains(s.Key)).Select(s => "cat=" + s.Key));
        }
        if (options.ByStatus != defaults.ByStatus) {
            parts.Add("status=" + (options.ByStatus ? "1" : "0"));
        }
        if (options.Infra != defaults.Infra) {
            parts.Add("infra=" + InfraKey(options.Infra));
        }
        if (options.Subpopulations) {
            parts.Add("subpop=1");
        }
        if (options.Sort != ListSort.FirstName) {
            parts.Add("sort=" + (options.Sort == ListSort.ScientificName ? "sci" : "common"));
        }
        if (!options.StatusTemplate) {
            parts.Add("tpl=0");
        }
        if (options.HeadingNames != defaults.HeadingNames) {
            parts.Add("names=" + (options.HeadingNames ? "1" : "0"));
        }
        if (options.TopLevel != defaults.TopLevel) {
            parts.Add("level=" + options.TopLevel.ToString(CultureInfo.InvariantCulture));
        }
        parts.AddRange(options.Sources.Write());
        return parts.Count == 0 ? string.Empty : "?" + string.Join("&", parts);
    }

    // The sources (src, prefer: ListSourceOptions). Species from CoL and Wikidata that IUCN does not
    // have are listed under NE, so picking either source includes the NE section.
    private static GroupListOptions ReadSources(IQueryCollection query, GroupListOptions options) {
        options = options with { Sources = ListSourceOptions.Read(query) };
        if (options.Sources.HasOtherSources && !options.IncludedSections.Contains(StatusSection.NotEvaluated)) {
            options = options with { Sections = options.Sections.Append(StatusSection.NotEvaluated).ToHashSet() };
        }
        return options;
    }

    public static string StyleKey(SpeciesListStyle style) => style switch {
        SpeciesListStyle.ScientificNameFirst => "sci",
        SpeciesListStyle.CommonNameOnly => "commononly",
        _ => "common",
    };

    public static string InfraKey(InfraMode mode) => mode switch {
        InfraMode.Separate => "separate",
        InfraMode.UnderSpecies => "under",
        _ => "none",
    };

    public static int RankIndex(string rank) {
        var at = Array.IndexOf(RankOrder, rank);
        return at < 0 ? RankOrder.Length : at;
    }

    // The last value: a form sends a hidden "0" before a checkbox's "1", so an unticked box still says so.
    private static string? First(IQueryCollection query, string key) =>
        query.TryGetValue(key, out var values) && values.Count > 0 ? values[^1]?.Trim() : null;

    private static IEnumerable<string> Values(StringValues values) =>
        values.Where(v => v is not null).Select(v => v!.Trim());
}
