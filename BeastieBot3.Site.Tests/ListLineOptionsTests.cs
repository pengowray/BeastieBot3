using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Pages;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace BeastieBot3.Site.Tests;

// The bullet list options for the taxon authority and the references after each line
// (ListLineOptions, ListReferences), the line count of extra species (ExtraSpeciesCounts), and the
// group page over the fixture's Ursus: the polar bear from IUCN, Ursus americanus (CoL and Wikidata),
// Ursus arctos (CoL only) and Ursus maritima (Wikidata only, likely the polar bear).
public sealed class ListLineOptionsTests(SiteFactory factory) : IClassFixture<SiteFactory> {
    private readonly HttpClient _client = factory.Client();

    private static QueryCollection Query(params (string Key, string Value)[] values) =>
        new(values.GroupBy(v => v.Key).ToDictionary(g => g.Key, g => new StringValues(g.Select(v => v.Value).ToArray())));

    [Fact]
    public void Options_RoundTripThroughTheQuery() {
        var options = ListLineOptions.Read(Query(("auth", "small"), ("lrefs", "list"), ("lcite", "q")));

        Assert.Equal(SpeciesListAuthority.Small, options.Authority);
        Assert.Equal(ListReferenceMode.ListDefined, options.References);
        Assert.Equal(ReferenceTemplate.CiteQ, options.Template);
        Assert.Equal(["auth=small", "lrefs=list", "lcite=q"], options.Write());
        Assert.Empty(ListLineOptions.Read(Query()).Write());
        Assert.Equal(ListReferenceMode.Inline, ListLineOptions.Read(Query(("lrefs", "inline"))).References);
        Assert.Equal(SpeciesListAuthority.Plain, ListLineOptions.Read(Query(("auth", "plain"))).Authority);
    }

    // ------------------------------------------------------------ authority and the merge

    private static ListTaxonRow Iucn(long id, string name, string? authority) {
        var parts = name.Split(' ');
        return new ListTaxonRow(id, name, TaxonKinds.Species, "ANIMALIA", parts[0], parts[1], null, null, null, null, null, null, null,
            7, 5, id * 10, "VU", false, false, 2020, authority);
    }

    private static ExtraSpeciesRow Extra(int id, string name, string? wikidataName, string? authority) {
        var parts = name.Split(' ');
        return new ExtraSpeciesRow(id, true, true, name, wikidataName, parts[0], parts[1], "ANIMALIA", "C" + id, "Q" + id, null, null, 7, 1,
            authority);
    }

    private static ListSourceOptions Sources(string order, params ListSource[] sources) => new() {
        Enabled = sources.ToHashSet(),
        Order = ListSourceOptions.Orders.Single(o => o.Key == order).Order,
    };

    private static ListSourceMergeResult Merge(ListSourceOptions options, IReadOnlyList<ListTaxonRow> iucn,
        IReadOnlyDictionary<long, IucnSourceInfo> info, IReadOnlyList<ExtraSpeciesRow> extras) =>
        ListSourceMerge.Merge(iucn, info, extras, [], new Dictionary<long, OverlapTaxon>(), new Dictionary<int, ExtraSpeciesRow>(), options);

    [Fact]
    public void Authority_IsKeptOnlyWithTheNameItBelongsTo() {
        var bear = Iucn(1, "Ursus maritimus", "Phipps, 1774");
        var info = new Dictionary<long, IucnSourceInfo> { [1] = new(1, "C9", null, "Thalarctos maritimus", null) };
        var extra = Extra(2, "Ursus arctos", "Ursus arctus", "Linnaeus, 1758");

        var iucnFirst = Merge(Sources("icw", ListSource.Iucn, ListSource.Col, ListSource.Wikidata), [bear], info, [extra]);
        Assert.Equal("Phipps, 1774", iucnFirst.Rows.Single(r => r.TaxonId == 1).Authority);
        Assert.Equal("Linnaeus, 1758", iucnFirst.Rows.Single(r => r.TaxonId == -2).Authority);

        // Under CoL's name the IUCN authority would be wrong (another genus); under Wikidata's spelling CoL's would be.
        var colFirst = Merge(Sources("ciw", ListSource.Iucn, ListSource.Col, ListSource.Wikidata), [bear], info, [extra]);
        Assert.Equal("Thalarctos maritimus", colFirst.Rows.Single(r => r.TaxonId == 1).ScientificName);
        Assert.Null(colFirst.Rows.Single(r => r.TaxonId == 1).Authority);
        var wikidataFirst = Merge(Sources("wci", ListSource.Col, ListSource.Wikidata), [], info, [extra]);
        Assert.Equal("Ursus arctus", wikidataFirst.Rows.Single().ScientificName);
        Assert.Null(wikidataFirst.Rows.Single().Authority);
    }

    // ------------------------------------------------------------ references

    [Fact]
    public void References_CiteTheSourceOfEachLine() {
        var bear = Iucn(1, "Ursus maritimus", "Phipps, 1774");
        var info = new Dictionary<long, IucnSourceInfo> { [1] = new(1, "C9", "Q9", null, null) };
        var extras = new[] { Extra(2, "Ursus arctos", null, "Linnaeus, 1758") with { InWikidata = false, WikidataQid = null },
            Extra(3, "Ursus zeta", null, null) with { InCol = false, ColId = null, SortPos = 6 } };
        var options = new GroupListOptions {
            Style = SpeciesListStyle.ScientificNameFirst, Sort = ListSort.ScientificName,
            Sources = Sources("icw", ListSource.Iucn, ListSource.Col, ListSource.Wikidata),
            Line = new ListLineOptions { Authority = SpeciesListAuthority.Small, References = ListReferenceMode.ListDefined },
            Sections = StatusSection.AllKeys,
        };
        var merge = Merge(options.Sources, [bear], info, extras);
        var groups = new Dictionary<int, GroupRow> {
            [7] = new(7, null, 0, "genus", "Ursus", "iucn", true, "ANIMALIA", null, null, null, null, 1, 9, 0, 0, 0),
        };
        var list = GroupList.Build(merge.Rows, groups, options);
        var citations = new Dictionary<long, AssessmentCitation> { [1] = new(null, "Q500", null) };
        var references = ListReferences.Build(list, citations, merge, ReferenceTemplate.CiteQ, new DateOnly(2026, 10, 6));

        Assert.Equal("""
            * ''[[Ursus arctos]]'' <small>Linnaeus, 1758</small><ref name="col-C2"/>
            * ''[[Ursus maritimus]]'' <small>Phipps, 1774</small> {{IUCN status|VU|1/10|1|year=2020}}<ref name="IUCNUrsusmaritimus"/>
            * ''[[Ursus zeta]]''<ref name="wd-Q3"/>

            {{reflist|refs=
            <ref name="col-C2">{{Catalogue of Life |id=C2 |title=''Ursus arctos'' Linnaeus, 1758}}</ref>
            <ref name="IUCNUrsusmaritimus">{{cite Q|Q500}}</ref>
            <ref name="wd-Q3">{{cite Q|Q3}}</ref>
            }}
            """.ReplaceLineEndings("\n"), GroupList.ToWikitext(list, options, references));

        var inline = options with { Line = options.Line with { References = ListReferenceMode.Inline } };
        Assert.StartsWith("* ''[[Ursus arctos]]'' <small>Linnaeus, 1758</small><ref name=\"col-C2\">{{Catalogue of Life |id=C2 |title=''Ursus arctos'' Linnaeus, 1758}}</ref>\n",
            GroupList.ToWikitext(list, inline, references));
        Assert.DoesNotContain("reflist", GroupList.ToWikitext(list, inline, references));
    }

    [Fact]
    public void CatalogueOfLife_KeepsPipesOutOfTheTemplate() {
        Assert.Equal("{{Catalogue of Life |id=X1 |title=''Ficus x'' A {{!}} B}}", ListReferences.CatalogueOfLife("X1", "Ficus x", "A | B"));
    }

    // ------------------------------------------------------------ line count

    [Fact]
    public void ExtraCount_FollowsGeneraAndLeavesOutLikelyIucnDuplicatesWhenIucnComesFirst() {
        var counts = new ExtraSpeciesCounts(9, Col: 4, Wikidata: 3, Both: 2) {
            Split = [
                new(1, UnderFamily: false, IucnLikely: false, 3),
                new(1, UnderFamily: true, IucnLikely: false, 1),
                new(2, UnderFamily: false, IucnLikely: true, 2),
                new(2, UnderFamily: true, IucnLikely: false, 1),
                new(3, UnderFamily: false, IucnLikely: false, 2),
            ],
        };
        var all = Sources("icw", ListSource.Iucn, ListSource.Col, ListSource.Wikidata);

        Assert.Equal(7, counts.For(all));
        Assert.Equal(5, counts.For(all with { OtherGenera = false }));
        // IUCN not first, or not ticked: likely duplicates of IUCN taxa can stay in the list.
        Assert.Equal(9, counts.For(Sources("ciw", ListSource.Iucn, ListSource.Col, ListSource.Wikidata)));
        Assert.Equal(9, counts.For(Sources("icw", ListSource.Col, ListSource.Wikidata)));
        Assert.Equal(6, counts.For(Sources("icw", ListSource.Iucn, ListSource.Col)));
        // Counts read without the split: the totals.
        Assert.Equal(9, new ExtraSpeciesCounts(9, 4, 3, 2).For(all));
    }

    // ------------------------------------------------------------ the group page

    [Fact]
    public async Task GroupPage_AuthoritiesAndListDefinedReferences() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus?src=iucn&src=col&src=wd&style=sci&sort=sci&auth=small&lrefs=list");
        var wikitext = Html.Textarea(html, "list-wikitext");

        Assert.Contains("* ''[[Ursus arctos]]'' <small>Linnaeus, 1758</small><ref name=\"col-COLAR\"/>", wikitext);
        Assert.Contains("<ref name=\"col-COLAR\">{{Catalogue of Life |id=COLAR |title=''Ursus arctos'' Linnaeus, 1758}}</ref>", wikitext);
        Assert.Contains("{{reflist|refs=", wikitext);
        Assert.Contains("{{cite iucn", wikitext);
        // The preview numbers the references and lists the citations.
        Assert.Contains("<sup class=\"preview-ref\">[1]</sup>", html);
        Assert.Contains("class=\"preview-refs\"", html);
        Assert.Contains("<small>Linnaeus, 1758</small>", html);
        // The options keep their values.
        Assert.Contains("name=\"auth\" value=\"small\" checked=\"checked\"", html);
        Assert.Contains("name=\"lrefs\" value=\"list\" checked=\"checked\"", html);
    }

    [Fact]
    public async Task GroupPage_WithoutTheOptions_HasNoAuthorityOrReference() {
        var html = await _client.GetStringAsync("/taxa/genus/ursus?src=iucn&src=col&src=wd&style=sci&sort=sci");
        var wikitext = Html.Textarea(html, "list-wikitext");

        Assert.DoesNotContain("<ref", wikitext);
        Assert.DoesNotContain("Linnaeus", wikitext);
    }
}
