using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Pages;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace BeastieBot3.Site.Tests;

// The group page's {{Species table}} list type (SpeciesTable), its query options and the shared
// {{cite iucn}} / {{cite Q}} choice (IucnReference).
public sealed class SpeciesTableTests {
    // Carnivora (2) > Felidae (3) > Panthera (4); Carnivora > Canidae (5) > Canis (6).
    private static readonly GroupRow Carnivora = Group(2, null, "order", "Carnivora", 1, 10, 4);
    private static readonly GroupRow Felidae = Group(3, 2, "family", "Felidae", 1, 6, 2);
    private static readonly GroupRow Panthera = Group(4, 3, "genus", "Panthera", 1, 6, 2);
    private static readonly GroupRow Canidae = Group(5, 2, "family", "Canidae", 7, 10, 3);
    private static readonly GroupRow Canis = Group(6, 5, "genus", "Canis", 7, 10, 3, enwiki: "Canis (genus)");
    private static readonly Dictionary<int, GroupRow> Groups =
        new[] { Carnivora, Felidae, Panthera, Canidae, Canis }.ToDictionary(g => g.NodeId);

    private static GroupRow Group(int id, int? parent, string rank, string name, int first, int last, int species, string? enwiki = null) =>
        new(id, parent, parent is null ? 0 : 1, rank, name, "iucn", true, "ANIMALIA", null, null, null, enwiki, first, last, species, 0, 0);

    private static ListTaxonRow Taxon(long id, string name, int node, int pos, string? category, string? common = null, bool pe = false) {
        var parts = name.Split(' ');
        return new ListTaxonRow(id, name, "species", "ANIMALIA", parts[0], parts[1], null, null, null, common, common, null, null, node, pos,
            category is null ? null : id * 10, category, pe, false, category is null ? null : 2020);
    }

    private static readonly ListTaxonRow[] Taxa = [
        Taxon(1, "Panthera leo", 4, 1, "VU", "Lion"),
        Taxon(3, "Panthera tigris", 4, 3, "EN", "Tiger"),
        Taxon(4, "Canis lupus", 6, 8, "LC", "Grey wolf"),
        Taxon(5, "Canis rufus", 6, 9, null, "Red wolf"),
        Taxon(6, "Canis dirus", 6, 7, "EX"),
    ];

    private static string Citation(long taxon, long assessment, string name) => new IucnCitationParts {
        TaxonId = taxon, AssessmentId = assessment, Year = 2020, ScientificName = name,
        Authors = [new CitationAuthor(CitationAuthorKind.Person, "Smith, A.", "Smith", "A.")],
        DownloadedAtUtc = new DateTime(2026, 8, 20, 0, 0, 0, DateTimeKind.Utc),
    }.ToJson();

    private static readonly Dictionary<long, TableTaxonExtra> Extras = new() {
        [1] = new(1, "(Linnaeus, 1758)", "Decreasing", "20000-25000", Citation(1, 10, "Panthera leo"), "Q100", "P31 P953"),
        [3] = new(3, "(Linnaeus, 1758)", "Increasing", "U", Citation(3, 30, "Panthera tigris"), null, null),
        [4] = new(4, "Linnaeus, 1758", "Stable", null, Citation(4, 40, "Canis lupus"), "Q400", "P31"),
        [5] = new(5, "Audubon & Bachman, 1851", null, null, null, null, null),
        [6] = new(6, "Leidy, 1858", "Unknown", null, null, null, null),
    };

    private static string Wikitext(GroupListOptions listOptions, SpeciesTableOptions options) {
        var listed = SpeciesTable.ListOptions(listOptions);
        var list = GroupList.Build(Taxa, Groups, listed);
        return SpeciesTable.ToWikitext(SpeciesTable.Build(list, Groups, Extras, options), options);
    }

    [Fact]
    public void One_table_per_genus_with_list_defined_references_in_the_featured_lists_format() {
        var wikitext = Wikitext(new GroupListOptions { HeadingRanks = ["family"], Sections = StatusSection.AllKeys },
            new SpeciesTableOptions { Type = ListType.Tables });

        Assert.Equal("""
            {{IUCN statuses|ex=1|ew=0|cr=0|en=1|vu=1|nt=0|lc=1|dd=0|ne=1}}

            == Family Felidae ==
            {{Species table |no-note=y |genus=[[Panthera]] |authority-name= |authority-year= |species-count=two}}
            {{Species table/row
            |name=[[Lion]] |binomial=P. leo
            |image= |image-alt=
            |authority-name=Linnaeus |authority-year=1758 |authority-not-original=yes
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=VU |population=20,000–25,000
            |direction={{decrease|Population declining}}<ref name="IUCNLion"/>
            }}
            {{Species table/row
            |name=[[Tiger]] |binomial=P. tigris
            |image= |image-alt=
            |authority-name=Linnaeus |authority-year=1758 |authority-not-original=yes
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=EN |population=Unknown
            |direction={{increase|Population increasing}}<ref name="IUCNTiger"/>
            }}
            {{Species table/end}}

            == Family Canidae ==
            {{Species table |no-note=y |genus=[[Canis (genus)|Canis]] |authority-name= |authority-year= |species-count=three}}
            {{Species table/row
            |name= |binomial=[[Canis dirus|C. dirus]]{{dagger|alt=Extinct}}
            |image= |image-alt=
            |authority-name=Leidy |authority-year=1858
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=EX |population=Unknown
            |direction={{population change unknown}}
            }}
            {{Species table/row
            |name=[[Grey wolf]] |binomial=C. lupus
            |image= |image-alt=
            |authority-name=Linnaeus |authority-year=1758
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=LC |population=Unknown
            |direction={{steady|Population steady}}<ref name="IUCNGreywolf"/>
            }}
            {{Species table/row
            |name=[[Red wolf]] |binomial=C. rufus
            |image= |image-alt=
            |authority-name=Audubon & Bachman |authority-year=1851
            |range= |range-image=
            |size= |habitat= |diet=
            |iucn-status=NE |population=Unknown
            |direction={{population change unknown}}
            }}
            {{Species table/end}}

            {{reflist|refs=
            <ref name="IUCNLion">{{cite iucn |last1=Smith |first1=A. |year=2020 |title=''Panthera leo'' |volume=2020 |article-number=e.T1A10 |access-date=20 August 2026}}</ref>
            <ref name="IUCNTiger">{{cite iucn |last1=Smith |first1=A. |year=2020 |title=''Panthera tigris'' |volume=2020 |article-number=e.T3A30 |access-date=20 August 2026}}</ref>
            <ref name="IUCNGreywolf">{{cite iucn |last1=Smith |first1=A. |year=2020 |title=''Canis lupus'' |volume=2020 |article-number=e.T4A40 |access-date=20 August 2026}}</ref>
            }}
            """.ReplaceLineEndings("\n"), wikitext);
    }

    [Fact]
    public void Inline_cite_Q_references_with_no_ecology_column_and_no_summary() {
        var wikitext = Wikitext(new GroupListOptions { Sections = new HashSet<string> { "VU", "LC" } },
            new SpeciesTableOptions {
                Type = ListType.Tables, References = TableReferences.Inline, RefNames = TableRefNames.TaxonId,
                Template = ReferenceTemplate.CiteQ, Columns = TableColumns.NoEcology, Summary = false,
            });

        Assert.Equal("""
            {{Species table |no-note=y |no-ecology=yes |genus=[[Panthera]] |authority-name= |authority-year= |species-count=two}}
            {{Species table/row |no-ecology=yes
            |name=[[Lion]] |binomial=P. leo
            |image= |image-alt=
            |authority-name=Linnaeus |authority-year=1758 |authority-not-original=yes
            |range= |range-image=
            |iucn-status=VU |population=20,000–25,000
            |direction={{decrease|Population declining}}<ref name="iucn-1">{{cite Q|Q100 |access-date=20 August 2026}}</ref>
            }}
            {{Species table/end}}
            {{Species table |no-note=y |no-ecology=yes |genus=[[Canis (genus)|Canis]] |authority-name= |authority-year= |species-count=three}}
            {{Species table/row |no-ecology=yes
            |name=[[Grey wolf]] |binomial=C. lupus
            |image= |image-alt=
            |authority-name=Linnaeus |authority-year=1758
            |range= |range-image=
            |iucn-status=LC |population=Unknown
            |direction={{steady|Population steady}}<ref name="iucn-4">{{cite Q|Q400}}</ref>
            }}
            {{Species table/end}}
            """.ReplaceLineEndings("\n"), wikitext);
    }

    [Fact]
    public void No_diet_and_no_references() {
        var wikitext = Wikitext(new GroupListOptions { Sections = new HashSet<string> { "VU" } },
            new SpeciesTableOptions { Type = ListType.Tables, References = TableReferences.None, Columns = TableColumns.NoDiet, Summary = false });

        Assert.Contains("|size= |habitat=\n|no-diet=yes\n|iucn-status=VU", wikitext);
        Assert.DoesNotContain("<ref", wikitext);
        Assert.DoesNotContain("reflist", wikitext);
    }

    [Theory]
    [InlineData("(W.C.H. Peters, 1851)", "W.C.H. Peters", "1851", true)]
    [InlineData("Setzer, 1971", "Setzer", "1971", false)]
    [InlineData("L.", "L.", "", false)]
    [InlineData("(Hack.) Druce", "(Hack.) Druce", "", false)]
    [InlineData(null, "", "", false)]
    public void Splits_the_authority_into_name_and_year(string? authority, string name, string year, bool notOriginal) =>
        Assert.Equal((name, year, notOriginal), SpeciesTable.SplitAuthority(authority));

    [Fact]
    public void Ref_names_in_each_style() {
        var heller = Taxon(44921, "Afronycteris helios", 1, 1, "DD", "Heller's serotine");
        var lordHowe = Taxon(7, "Nyctophilus howensis", 1, 2, "EX", "Lord Howe long-eared bat");
        var noName = Taxon(8, "Laephotis robertsi", 1, 3, "DD");

        Assert.Equal("IUCNHellersserotine", SpeciesTable.RefName(heller, TableRefNames.CommonName));
        Assert.Equal("IUCNLordHowelong-earedbat", SpeciesTable.RefName(lordHowe, TableRefNames.CommonName));
        Assert.Equal("IUCNLaephotisrobertsi", SpeciesTable.RefName(noName, TableRefNames.CommonName));
        Assert.Equal("iucn-44921", SpeciesTable.RefName(heller, TableRefNames.TaxonId));
        Assert.Equal("Afronycteris helios", SpeciesTable.RefName(heller, TableRefNames.ScientificName));
    }

    [Fact]
    public void Two_taxa_with_one_ref_name_are_told_apart_by_taxon_id() {
        var twins = new[] { Taxon(1, "Panthera leo", 4, 1, "VU", "Lion"), Taxon(3, "Panthera tigris", 4, 3, "EN", "Lion") };
        var options = new SpeciesTableOptions { Type = ListType.Tables, Summary = false };
        var list = GroupList.Build(twins, Groups, SpeciesTable.ListOptions(new GroupListOptions()));
        var rows = SpeciesTable.Build(list, Groups, Extras, options).Rows.Select(r => r.RefName).ToList();

        Assert.Equal(["IUCNLion", "IUCNLion-3"], rows);
    }

    [Fact]
    public void Species_count_is_written_in_words_below_100() {
        Assert.Equal("one", SpeciesTable.CountWords(1));
        Assert.Equal("nineteen", SpeciesTable.CountWords(19));
        Assert.Equal("twenty", SpeciesTable.CountWords(20));
        Assert.Equal("forty-two", SpeciesTable.CountWords(42));
        Assert.Equal("1,047", SpeciesTable.CountWords(1047));
    }

    [Fact]
    public void The_cap_is_lower_with_references() {
        Assert.Equal(400, SpeciesTable.MaxRows(new SpeciesTableOptions { Type = ListType.Tables }));
        Assert.Equal(1000, SpeciesTable.MaxRows(new SpeciesTableOptions { Type = ListType.Tables, References = TableReferences.None }));
        Assert.True(SpeciesTable.MaxRowsWithReferences < GroupList.MaxLines);
    }

    [Fact]
    public void Preview_shows_a_table_per_genus_with_status_links() {
        var options = new SpeciesTableOptions { Type = ListType.Tables };
        var list = GroupList.Build(Taxa, Groups, SpeciesTable.ListOptions(new GroupListOptions { Sections = StatusSection.AllKeys }));
        var html = SpeciesTablePreview.ToHtml(SpeciesTable.Build(list, Groups, Extras, options));

        Assert.Contains("<caption>Genus <i>Panthera</i> – two species</caption>", html);
        Assert.Contains("<a href=\"https://en.wikipedia.org/wiki/Lion\">Lion</a>", html);
        Assert.Contains("https://www.iucnredlist.org/species/1/10", html);
        Assert.Contains("(Linnaeus, 1758)", html);
        Assert.Contains("Population declining", html);
        Assert.Contains("<span title=\"Extinct\">†</span>", html);
        Assert.DoesNotContain("{{", html);
    }

    [Fact]
    public void Query_round_trip_writes_table_options_only_for_tables() {
        var query = new QueryCollection(new Dictionary<string, StringValues> {
            ["type"] = "table", ["refs"] = "inline", ["refnames"] = "sci", ["cite"] = "q", ["cols"] = "noecology",
            ["summary"] = new StringValues(["0"]),
        });
        var options = SpeciesTableQuery.Read(query);

        Assert.Equal(new SpeciesTableOptions {
            Type = ListType.Tables, References = TableReferences.Inline, RefNames = TableRefNames.ScientificName,
            Template = ReferenceTemplate.CiteQ, Columns = TableColumns.NoEcology, Summary = false,
        }, options);
        Assert.Equal("?h=none&type=table&refs=inline&refnames=sci&cite=q&cols=noecology&summary=0", SpeciesTableQuery.Append("?h=none", options));
        Assert.Equal("?type=table", SpeciesTableQuery.Append("", new SpeciesTableOptions { Type = ListType.Tables }));
        Assert.Equal("", SpeciesTableQuery.Append("", options with { Type = ListType.Bullets }));
        // A ticked checkbox sends its hidden "0" first.
        var ticked = new QueryCollection(new Dictionary<string, StringValues> { ["summary"] = new StringValues(["0", "1"]) });
        Assert.True(SpeciesTableQuery.Read(ticked).Summary);
    }

    [Fact]
    public void Cite_Q_only_when_chosen_and_the_assessment_has_an_item() {
        var parts = IucnCitationParts.FromJson(Citation(1, 10, "Panthera leo"));
        var iucn = new CiteIucnOptions();
        var q = new CiteQOptions { AccessDate = new DateOnly(2026, 8, 20), WrapInRef = true, RefName = "iucn" };

        Assert.StartsWith("{{cite iucn", IucnReference.Render(ReferenceTemplate.CiteIucn, parts, "Q100", "P953", iucn, q));
        Assert.Equal("<ref name=\"iucn\">{{cite Q|Q100 |access-date=20 August 2026}}</ref>",
            IucnReference.Render(ReferenceTemplate.CiteQ, parts, "Q100", "P31 P953", iucn, q));
        // No full work URL (P953): no access date, which CS1 would report as an error.
        Assert.Equal("<ref name=\"iucn\">{{cite Q|Q100}}</ref>", IucnReference.Render(ReferenceTemplate.CiteQ, parts, "Q100", "P31", iucn, q));
        Assert.StartsWith("{{cite iucn", IucnReference.Render(ReferenceTemplate.CiteQ, parts, null, null, iucn, q));
        Assert.Null(IucnReference.Render(ReferenceTemplate.CiteQ, null, null, null, iucn, q));
        Assert.Equal(ReferenceTemplate.CiteQ, IucnReference.FromQuery("q"));
        Assert.Equal(ReferenceTemplate.CiteIucn, IucnReference.FromQuery("x"));
    }
}
