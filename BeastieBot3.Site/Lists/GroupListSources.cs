using System.Net;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

// The group page's glue for lists with species from the Catalogue of Life and Wikidata: the line
// count before reading the rows, reading the rows and overlaps (SiteQueries.Extra.cs) and merging
// them (ListSourceMerge), and the links after each preview line.

namespace BeastieBot3.Site.Lists;

public static class GroupListSources {
    /// Whether the list needs anything beyond IUCN's own rows.
    public static bool Active(GroupListOptions options) =>
        !options.Sources.Enabled.SetEquals(ListSourceOptions.Default.Enabled);

    /// The most lines the extra species add: those of the picked sources, when NE is included.
    /// An upper bound, since likely duplicates are left out only when the rows are merged.
    public static int ExtraLines(ExtraSpeciesCounts? counts, GroupListOptions options) =>
        counts is null || !options.Sources.HasOtherSources || !options.IncludedSections.Contains(StatusSection.NotEvaluated)
            ? 0
            : counts.For(options.Sources);

    public static ListSourceMergeResult Merge(SiteQueries queries, GroupRow group, ExtraSpeciesCounts? counts,
        IReadOnlyList<ListTaxonRow> taxa, GroupListOptions options) {
        var info = queries.GetIucnSourceInfo(group);
        var lastNode = counts?.LastNodeId ?? group.NodeId;
        IReadOnlyList<ExtraSpeciesRow> extras = counts is not null && options.Sources.HasOtherSources
            && options.IncludedSections.Contains(StatusSection.NotEvaluated)
            ? queries.GetExtraSpecies(group.NodeId, counts.LastNodeId)
            : [];
        var overlaps = options.Sources.HasOtherSources ? queries.GetExtraOverlaps(group, lastNode) : [];
        var extraIds = extras.Select(e => e.ExtraId).ToHashSet();
        var taxonIds = taxa.Select(t => t.TaxonId).ToHashSet();
        var outsideExtras = queries.GetExtraSpeciesByIds(overlaps
            .SelectMany(o => o.OtherExtraId is { } other ? new[] { o.ExtraId, other } : new[] { o.ExtraId })
            .Where(id => !extraIds.Contains(id)).ToHashSet());
        var outsideTaxa = queries.GetOverlapTaxa(overlaps
            .Where(o => o.TaxonId is { } t && !taxonIds.Contains(t)).Select(o => o.TaxonId!.Value).ToHashSet());
        return ListSourceMerge.Merge(taxa, info, extras, overlaps, outsideTaxa, outsideExtras, options.Sources);
    }

    /// How many lines of the list are species only in CoL or Wikidata.
    public static int ExtraLineCount(GroupListResult list) =>
        list.Blocks.OfType<LineBlock>().Count(l => l.Taxon.TaxonId < 0);

    /// HTML to put after each bullet line of the preview, in order: links to the CoL page and the
    /// Wikidata item of a species only in CoL or Wikidata; null for other lines.
    public static IReadOnlyList<string?> PreviewSuffixes(GroupListResult list, ListSourceMergeResult? merge) {
        var suffixes = new List<string?>();
        foreach (var line in list.Blocks.OfType<LineBlock>()) {
            if (merge is null || line.Taxon.TaxonId >= 0 || !merge.Entries.TryGetValue(line.Taxon.TaxonId, out var entry)) {
                suffixes.Add(null);
                continue;
            }
            var links = new List<string>();
            if (entry.ColId is { } colId) {
                links.Add(Link(SiteFormat.CatalogueOfLifeUrl(colId), GroupSourceText.PreviewColLink));
            }
            if (entry.WikidataQid is { } qid) {
                links.Add(Link(SiteFormat.WikidataUrl(qid), GroupSourceText.PreviewWikidataLink));
            }
            suffixes.Add(links.Count == 0 ? null : " <span class=\"preview-source\">" + string.Join(" ", links) + "</span>");
        }
        return suffixes;
    }

    private static string Link(string href, string text) =>
        $"<a href=\"{WebUtility.HtmlEncode(href)}\">{WebUtility.HtmlEncode(text)}</a>";
}
