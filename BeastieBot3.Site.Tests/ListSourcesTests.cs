using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace BeastieBot3.Site.Tests;

// Lists with species from the Catalogue of Life and Wikidata: the query options, the merge with its
// order of preference (ListSourceMerge), and the group page over the fixture's Ursus, which has the
// polar bear from IUCN, Ursus americanus (CoL and Wikidata), Ursus arctos (CoL only) and Ursus
// maritima (Wikidata only, likely the polar bear).
public sealed class ListSourcesTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static ListTaxonRow Iucn(long id, string name, int pos, string category = "LC") {
        var parts = name.Split(' ');
        return new ListTaxonRow(id, name, TaxonKinds.Species, "ANIMALIA", parts[0], parts[1], null, null, null, null, null, null, null,
            7, pos, id * 10, category, false, false, 2020);
    }

    private static ExtraSpeciesRow Extra(int id, string name, bool col, bool wikidata, int sortPos, string? wikidataName = null) {
        var parts = name.Split(' ');
        return new ExtraSpeciesRow(id, col, wikidata, name, wikidataName, parts[0], parts[1], "ANIMALIA",
            col ? "C" + id : null, wikidata ? "Q" + id : null, null, null, 7, sortPos);
    }

    private static ListSourceOptions Options(string order, params ListSource[] sources) => new() {
        Enabled = sources.ToHashSet(),
        Order = ListSourceOptions.Orders.Single(o => o.Key == order).Order,
    };

    private static ListSourceMergeResult Merge(ListSourceOptions options, IReadOnlyList<ListTaxonRow> iucn,
        IReadOnlyDictionary<long, IucnSourceInfo> info, IReadOnlyList<ExtraSpeciesRow> extras, IReadOnlyList<ExtraOverlapRow> overlaps,
        IReadOnlyDictionary<long, OverlapTaxon>? outsideTaxa = null) =>
        ListSourceMerge.Merge(iucn, info, extras, overlaps, outsideTaxa ?? new Dictionary<long, OverlapTaxon>(),
            new Dictionary<int, ExtraSpeciesRow>(), options);

    private static readonly ListTaxonRow Bear = Iucn(1, "Ursus maritimus", 5);
    private static readonly IReadOnlyDictionary<long, IucnSourceInfo> BearInfo = new Dictionary<long, IucnSourceInfo> {
        [1] = new(1, "C9", "Q9", "Thalarctos maritimus", null),
    };

    [Fact]
    public void ExtrasGoInTreeOrder() {
        var result = Merge(Options("icw", ListSource.Iucn, ListSource.Col), [Iucn(1, "Ursus maritimus", 5), Iucn(2, "Ursus thibetanus", 6)],
            new Dictionary<long, IucnSourceInfo>(), [Extra(1, "Ursus americanus", true, false, 4), Extra(2, "Ursus nordicus", true, false, 5),
                Extra(3, "Ursus zeta", true, false, 6)], []);

        Assert.Equal(["Ursus americanus", "Ursus maritimus", "Ursus nordicus", "Ursus thibetanus", "Ursus zeta"],
            result.Rows.Select(r => r.ScientificName));
        Assert.All(result.Rows.Where(r => r.TaxonId < 0), r => Assert.Null(r.Category));
    }

    [Fact]
    public void ALikelyDuplicateFromTheLessPreferredSourceIsLeftOut() {
        var overlaps = new[] { new ExtraOverlapRow(3, 1, null, "gender-ending", true) };
        var extras = new[] { Extra(3, "Ursus maritima", false, true, 5) };

        var iucnFirst = Merge(Options("icw", ListSource.Iucn, ListSource.Wikidata), [Bear], new Dictionary<long, IucnSourceInfo>(), extras, overlaps);
        Assert.Equal(["Ursus maritimus"], iucnFirst.Rows.Select(r => r.ScientificName));
        var notice = Assert.Single(iucnFirst.Notices);
        Assert.True(notice.LeftOut);
        Assert.Equal("Ursus maritima", notice.Entry.ScientificName);
        Assert.Equal(ListSource.Iucn, notice.Kept.Source);

        var wikidataFirst = Merge(Options("wic", ListSource.Iucn, ListSource.Wikidata), [Bear], new Dictionary<long, IucnSourceInfo>(), extras, overlaps);
        Assert.Equal(["Ursus maritima"], wikidataFirst.Rows.Select(r => r.ScientificName));
        Assert.Equal("Ursus maritimus", Assert.Single(wikidataFirst.Notices).Entry.ScientificName);
    }

    [Fact]
    public void TwoEntriesWithTheSameBestSourceAreBothKept() {
        // The polar bear has a Wikidata item, so with Wikidata first both entries come from Wikidata.
        var result = Merge(Options("wic", ListSource.Iucn, ListSource.Wikidata), [Bear], BearInfo,
            [Extra(3, "Ursus maritima", false, true, 5)], [new ExtraOverlapRow(3, 1, null, "gender-ending", true)]);

        Assert.Equal(2, result.Rows.Count);
        Assert.False(Assert.Single(result.Notices).LeftOut);
    }

    [Fact]
    public void PossibleDuplicatesAreBothKept() {
        var result = Merge(Options("icw", ListSource.Iucn, ListSource.Col), [Bear], new Dictionary<long, IucnSourceInfo>(),
            [Extra(3, "Ursus maritimuss", true, false, 5)], [new ExtraOverlapRow(3, 1, null, "spelling", false)]);

        Assert.Equal(2, result.Rows.Count);
        Assert.False(Assert.Single(result.Notices).LeftOut);
    }

    [Fact]
    public void TheNameComesFromThePreferredSource() {
        var colFirst = Merge(Options("ciw", ListSource.Iucn, ListSource.Col), [Bear], BearInfo, [], []);
        var row = Assert.Single(colFirst.Rows);
        Assert.Equal("Thalarctos maritimus", row.ScientificName);
        Assert.Equal("Thalarctos", row.Genus);
        // The assessment stays.
        Assert.Equal("LC", row.Category);

        var iucnFirst = Merge(Options("icw", ListSource.Iucn, ListSource.Col), [Bear], BearInfo, [], []);
        Assert.Equal("Ursus maritimus", Assert.Single(iucnFirst.Rows).ScientificName);
    }

    [Fact]
    public void WithoutIucnOnlyTaxaInAPickedSourceAreListed() {
        var other = Iucn(2, "Ursus thibetanus", 6);
        var result = Merge(Options("icw", ListSource.Col), [Bear, other], BearInfo,
            [Extra(1, "Ursus arctos", true, false, 4), Extra(2, "Ursus nordicus", false, true, 4)], []);

        Assert.Equal(["Ursus arctos", "Thalarctos maritimus"], result.Rows.Select(r => r.ScientificName));
    }

    [Fact]
    public void ALikelyDuplicateOutsideTheGroupLeavesOutTheEntryHere() {
        var outside = new Dictionary<long, OverlapTaxon> { [50] = new(50, "Helarctos malayanus", null, null) };
        var result = Merge(Options("icw", ListSource.Iucn, ListSource.Col), [], new Dictionary<long, IucnSourceInfo>(),
            [Extra(3, "Ursus malayanus", true, false, 5)], [new ExtraOverlapRow(3, 50, null, "iucn-synonym", true)], outside);

        Assert.Empty(result.Rows);
        var notice = Assert.Single(result.Notices);
        Assert.True(notice.LeftOut);
        Assert.False(notice.Kept.InGroup);
    }

    [Fact]
    public void QueryOptions() {
        var defaults = ListSourceOptions.Read(new QueryCollection());
        Assert.Equal([ListSource.Iucn], defaults.Enabled);
        Assert.Empty(defaults.Write());

        var picked = ListSourceOptions.Read(new QueryCollection(new Dictionary<string, StringValues> {
            ["src"] = new(["", "col", "wd", "nonsense"]),
            ["prefer"] = "cwi",
        }));
        Assert.Equal(new HashSet<ListSource> { ListSource.Col, ListSource.Wikidata }, picked.Enabled);
        Assert.Equal(["src=col", "src=wd", "prefer=cwi"], picked.Write());

        var genusOnly = ListSourceOptions.Read(new QueryCollection(new Dictionary<string, StringValues> { ["genera"] = new(["0"]) }));
        Assert.False(genusOnly.OtherGenera);
        Assert.Equal(["genera=0"], genusOnly.Write());
        Assert.True(ListSourceOptions.Read(new QueryCollection(new Dictionary<string, StringValues> { ["genera"] = new(["0", "1"]) })).OtherGenera);

        // Only the hidden empty value: IUCN only.
        Assert.Equal([ListSource.Iucn], ListSourceOptions.Read(new QueryCollection(new Dictionary<string, StringValues> { ["src"] = "" })).Enabled);
    }

    // ------------------------------------------------------------ the group page

    [Fact]
    public async Task GroupListWithAllSources() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus?src=iucn&src=col&src=wd&style=sci&sort=sci");

        Assert.Equal("""
            * [[American black bear|''Ursus americanus'']], American black bear
            * ''[[Ursus arctos]]''
            * [[Polar bear|''Ursus maritimus'']], Polar bear {{IUCN status|VU|22823/14871490|1|year=2015}}
            """.ReplaceLineEndings("\n"), Html.Textarea(html, "list-wikitext"));
        Assert.Contains("3 taxa, including 2 species from CoL or Wikidata that are not on the IUCN Red List", html);
        // The duplicates: counted under the list's size, then a table per reason in the panel.
        var text = Html.Text(html);
        Assert.Contains("Possible duplicates: 1 entry left out", text);
        Assert.Contains("<a href=\"#list-notices\">Possible duplicates</a>", html);
        Assert.Contains("Left out of the list (1 entry)", text);
        Assert.Contains("1 pair: Same genus, and the epithets differ only in the Latin gender ending.", text);
        var panel = Html.Between(html, "<section class=\"list-notices\"", "</section>");
        Assert.Contains("<th scope=\"col\">Left out</th>", panel);
        Assert.Contains("<th scope=\"col\">Likely the same species as</th>", panel);
        Assert.Contains("href=\"/wikidata/Q1003\" class=\"sci-name\">Ursus maritima</a> <span class=\"notice-tags\"><span class=\"notice-source\">Wikidata</span></span>", panel);
        Assert.Contains("Ursus maritimus</a> <span class=\"notice-tags\"><span class=\"notice-source\">IUCN</span> · <span class=\"notice-state notice-state-inlist\">in this list</span></span>", panel);
        Assert.Contains("<details class=\"notice-group\" id=\"dup-out-gender-ending\" open=\"open\">", panel);
        // The preview links the CoL page and the Wikidata item of a species from those sources.
        Assert.Contains("href=\"https://www.catalogueoflife.org/data/taxon/COLAR\">CoL</a>", html);
        Assert.Contains("href=\"https://www.wikidata.org/wiki/Q1001\">Wikidata</a>", html);
        // Picking CoL or Wikidata includes Not Evaluated.
        Assert.Contains("name=\"cat\" value=\"NE\" checked=\"checked\"", html);
    }

    [Fact]
    public async Task GroupListPreferringWikidata() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus?src=wd&style=sci&sort=sci");

        // Only Wikidata: the polar bear (it has an item) and Ursus maritima are both from Wikidata, so both stay.
        Assert.Equal("""
            * [[American black bear|''Ursus americanus'']], American black bear
            * [[Polar bear|''Ursus maritimus'']], Polar bear {{IUCN status|VU|22823/14871490|1|year=2015}}
            * ''[[Ursus maritima]]''
            """.ReplaceLineEndings("\n"), Html.Textarea(html, "list-wikitext"));
        var text = Html.Text(html);
        Assert.Contains("Possible duplicates: 1 pair with both entries kept", text);
        Assert.Contains("Both entries kept (1 pair)", text);
        Assert.Contains("In the list May be the same species as", text);
        Assert.DoesNotContain("Left out of the list", text);
    }

    [Fact]
    public async Task NotEvaluatedBoxIsTickedWithAnotherSourceAndMarkedForLiveUpdates() {
        // site.js copies the box's state from the new page after a live update (data-live-sync).
        var withCol = await _client.GetStringAsync("/taxa/genus/ursus?src=iucn&src=col");
        Assert.Contains("value=\"NE\" checked=\"checked\" data-live-sync=\"\"", withCol);

        var iucnOnly = await _client.GetStringAsync("/taxa/genus/ursus");
        Assert.Contains("value=\"NE\" data-live-sync=\"\"", iucnOnly);
    }

    [Fact]
    public async Task DefaultListHasOnlyIucnTaxa() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus?style=sci&cat=");

        Assert.Equal("* [[Polar bear|''Ursus maritimus'']], Polar bear {{IUCN status|VU|22823/14871490|1|year=2015}}",
            Html.Textarea(html, "list-wikitext"));
        Assert.DoesNotContain("Possible duplicates", html);
    }
}
