using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace BeastieBot3.Site.Tests;

public sealed class GroupListTests {
    // Mammalia (1) > Carnivora (2) > Felidae (3) > Panthera (4); Carnivora > Canidae (5) > Canis (6).
    private static readonly GroupRow Mammalia = Group(1, null, "class", "Mammalia", 1, 10);
    private static readonly GroupRow Carnivora = Group(2, 1, "order", "Carnivora", 1, 10);
    private static readonly GroupRow Felidae = Group(3, 2, "family", "Felidae", 1, 6, common: "cats");
    private static readonly GroupRow Panthera = Group(4, 3, "genus", "Panthera", 1, 6);
    private static readonly GroupRow Canidae = Group(5, 2, "family", "Canidae", 7, 10);
    private static readonly GroupRow Canis = Group(6, 5, "genus", "Canis", 7, 10);
    private static readonly Dictionary<int, GroupRow> Groups =
        new[] { Mammalia, Carnivora, Felidae, Panthera, Canidae, Canis }.ToDictionary(g => g.NodeId);

    private static GroupRow Group(int id, int? parent, string rank, string name, int first, int last, string? common = null) =>
        new(id, parent, parent is null ? 0 : 1, rank, name, "iucn", true, "ANIMALIA", null, common, common is null ? null : "rules", null, first, last, 0, 0, 0);

    private static ListTaxonRow Taxon(long id, string name, int node, int pos, string category, string? common = null,
        string kind = "species", long? parent = null, bool pe = false) {
        var parts = name.Split(' ');
        return new ListTaxonRow(id, name, kind, "ANIMALIA", parts[0], parts.Length > 1 ? parts[1] : null,
            kind == "subspecies" ? "ssp." : null, kind == "subspecies" ? parts[^1] : null, null, common, common, null, parent, node, pos,
            id * 10, category, pe, false, 2020);
    }

    private static readonly ListTaxonRow[] Taxa = [
        Taxon(1, "Panthera leo", 4, 1, "VU", "Lion"),
        Taxon(2, "Panthera leo ssp. persica", 4, 2, "EN", "Asiatic lion", "subspecies", parent: 1),
        Taxon(3, "Panthera tigris", 4, 3, "EN", "Tiger"),
        Taxon(4, "Canis lupus", 6, 8, "LC", "Grey wolf"),
        Taxon(5, "Canis rufus", 6, 9, "CR", "Red wolf"),
        Taxon(6, "Canis dirus", 6, 7, "CR", pe: true),
    ];

    [Fact]
    public void Status_sections_then_family_headings_in_the_lists_format() {
        var options = new GroupListOptions { HeadingRanks = ["family"], Style = SpeciesListStyle.CommonNameFirst };
        var list = GroupList.Build(Taxa, Groups, options);
        var wikitext = GroupList.ToWikitext(list, options);

        Assert.Equal("""
            == Critically endangered ==
            === Family Canidae ===
            * ''[[Canis dirus]]'' {{IUCN status|CR(PE)|6/60|1|year=2020}}
            * [[Red wolf]] (''Canis rufus'') {{IUCN status|CR|5/50|1|year=2020}}

            == Endangered ==
            === Family Felidae ===
            Members of the [[Felidae]] family are called cats.
            * [[Tiger]] (''Panthera tigris'') {{IUCN status|EN|3/30|1|year=2020}}

            == Vulnerable ==
            === Family Felidae ===
            Members of the [[Felidae]] family are called cats.
            * [[Lion]] (''Panthera leo'') {{IUCN status|VU|1/10|1|year=2020}}

            == Least concern ==
            === Family Canidae ===
            * [[Grey wolf]] (''Canis lupus'') {{IUCN status|LC|4/40|1|year=2020}}
            """.ReplaceLineEndings("\n"), wikitext);
        Assert.Equal(5, list.LineCount);
    }

    [Fact]
    public void Without_status_sections_shows_the_possibly_extinct_label_and_nests_subspecies() {
        var options = new GroupListOptions {
            ByStatus = false, HeadingRanks = [], Infra = InfraMode.UnderSpecies, Sort = ListSort.ScientificName,
            Style = SpeciesListStyle.ScientificNameFirst, StatusTemplate = false,
        };
        var wikitext = GroupList.ToWikitext(GroupList.Build(Taxa, Groups, options), options);

        Assert.Equal("""
            * [[Lion|''Panthera leo'']], Lion
            ** [[Asiatic lion|''P. leo persica'']], Asiatic lion
            * [[Tiger|''Panthera tigris'']], Tiger
            * ''[[Canis dirus]]'' (possibly extinct)
            * [[Grey wolf|''Canis lupus'']], Grey wolf
            * [[Red wolf|''Canis rufus'']], Red wolf
            """.ReplaceLineEndings("\n").Replace("possibly extinct", "possibly extinct"), wikitext);
    }

    [Fact]
    public void Subspecies_after_the_species_get_a_label() {
        var options = new GroupListOptions { ByStatus = false, HeadingRanks = ["genus"], Infra = InfraMode.Separate, HeadingNames = false };
        var list = GroupList.Build(Taxa, Groups, options);

        var panthera = list.Blocks.SkipWhile(b => b is not HeadingBlock { Text: "Genus Panthera" }).Skip(1).TakeWhile(b => b is not HeadingBlock).ToList();
        Assert.IsType<LabelBlock>(panthera[2]);
        Assert.Equal("Subspecies", ((LabelBlock)panthera[2]).Text);
        Assert.Equal(2L, ((LineBlock)panthera[3]).Taxon.TaxonId);
    }

    [Fact]
    public void A_subpopulation_line_has_the_species_name_and_the_subpopulation_in_brackets() {
        var row = new ListTaxonRow(7, "Lycaon pictus North Africa subpopulation", "subpopulation", "ANIMALIA", "Lycaon", "pictus", null, null,
            "North Africa subpopulation", "African wild dog", "African wild dog", null, 8, 6, 10, 70, "CR", false, false, 2020);
        var line = SpeciesListLine.Format(GroupList.Entry(row), new SpeciesListLineOptions());

        Assert.Equal("* [[African wild dog]] (''Lycaon pictus'') (North Africa subpopulation) {{IUCN status|CR|7/70|1|year=2020}}", line);
    }

    [Fact]
    public void Writes_the_members_line_only_for_a_name_from_the_rules() {
        var fromRedirect = Felidae with { CommonNameEn = "Cat", CommonNameSource = "wikipedia" };
        Assert.False(GroupList.HasSentenceName(fromRedirect));
        Assert.True(GroupList.HasSentenceName(Felidae));
    }

    [Fact]
    public void Leaves_out_heading_ranks_below_level_6() {
        var options = new GroupListOptions { TopLevel = 4, HeadingRanks = ["order", "family", "genus"] };
        var list = GroupList.Build(Taxa, Groups, options);

        Assert.Equal(["genus"], list.SkippedRanks);
        Assert.Equal(6, list.Blocks.OfType<HeadingBlock>().Max(h => h.Level));
    }

    [Fact]
    public void Counts_lines_from_the_category_counts() {
        GroupCategoryCount[] counts = [new("CR(PE)", 2, 1, 0), new("EN", 10, 3, 1), new("LR/nt", 4, 0, 0)];
        Assert.Equal(16, GroupList.CountLines(counts, new GroupListOptions()));
        Assert.Equal(21, GroupList.CountLines(counts, new GroupListOptions { Infra = InfraMode.Separate, Subpopulations = true }));
        Assert.Equal(4, GroupList.CountLines(counts, new GroupListOptions { Sections = new HashSet<string> { "NT" } }));
    }

    [Fact]
    public void Defaults_follow_the_lists_style_for_the_group() {
        Assert.Equal(SpeciesListStyle.CommonNameOnly, GroupListQuery.DefaultStyle([Group(9, null, "kingdom", "Animalia", 1, 1), Group(8, 9, "phylum", "Chordata", 1, 1), Mammalia]));
        Assert.Equal(SpeciesListStyle.ScientificNameFirst, GroupListQuery.DefaultStyle([Group(9, null, "kingdom", "Plantae", 1, 1)]));
        Assert.Equal(["order", "family"], GroupListQuery.DefaultHeadings(Mammalia));
        Assert.Equal(["family"], GroupListQuery.DefaultHeadings(Carnivora));
        Assert.Empty(GroupListQuery.DefaultHeadings(Felidae));
    }

    [Fact]
    public void Query_round_trip_keeps_only_changed_options() {
        var defaults = new GroupListOptions { HeadingRanks = ["order", "family"] };
        var query = new QueryCollection(new Dictionary<string, StringValues> {
            ["style"] = "sci", ["h"] = new(["none", "family", "suborder"]), ["cat"] = new(["", "CR", "EN"]),
            ["tpl"] = new(["0", "1"]), ["names"] = "0", ["level"] = "9",
        });
        var options = GroupListQuery.Read(query, defaults, ["order", "suborder", "family"]);

        Assert.Equal(SpeciesListStyle.ScientificNameFirst, options.Style);
        Assert.Equal(["suborder", "family"], options.HeadingRanks);
        Assert.Equal(["CR", "EN"], options.Sections.Order());
        Assert.True(options.StatusTemplate);
        Assert.False(options.HeadingNames);
        Assert.Equal(4, options.TopLevel);
        Assert.Equal("?style=sci&h=suborder&h=family&cat=CR&cat=EN&names=0&level=4", GroupListQuery.Write(options, defaults));
        Assert.Equal(string.Empty, GroupListQuery.Write(defaults, defaults));
    }
}
