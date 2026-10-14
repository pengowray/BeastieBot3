using System.IO;
using System.Linq;
using System.Text;
using BeastieBot3.Classifications;
using BeastieBot3.StatusLists;

namespace BeastieBot3.Tests;

// Pins the readers of `classifications import` (ColDP, Darwin Core Archive, AviList's workbook rows)
// and the classifications store.
public class ClassificationTests {
    private static string Lines(params string[] lines) => string.Join("\n", lines) + "\n";

    private static (List<ClassificationNode> Nodes, List<ClassificationName> Names) Coldp(params (string File, string Text)[] files) =>
        ColdpClassification.Read(name => files.FirstOrDefault(f => f.File == name) is { Text: { } text } ? new StringReader(text) : null).Build();

    private static string Chain(List<ClassificationNode> nodes, string id) {
        var byId = nodes.ToDictionary(n => n.Id);
        var steps = new List<string>();
        for (string? at = id; at is not null; at = byId[at].ParentId) {
            steps.Add($"{byId[at].Rank} {byId[at].Name}");
        }
        steps.Reverse();
        return string.Join(" > ", steps);
    }

    [Fact]
    public void ColdpNameUsageGivesSpeciesTheirGroupsAndLeavesOutSubspecies() {
        var (nodes, names) = Coldp(("NameUsage.tsv", Lines(
            "col:ID\tcol:parentID\tcol:status\tcol:rank\tcol:scientificName",
            "infraclass:NEOGNATHAE\t\taccepted\tinfraclass\tNeognathae",
            "order:PASSERIFORMES\tinfraclass:NEOGNATHAE\taccepted\torder\tPasseriformes",
            "fam:Passeridae\torder:PASSERIFORMES\taccepted\tfamily\tPasseridae",
            "gen:Passer\tfam:Passeridae\taccepted\tgenus\tPasser",
            "Passer_domesticus\tgen:Passer\taccepted\tspecies\tPasser domesticus",
            "Passer_domesticus_indicus\tPasser_domesticus\taccepted\tsubspecies\tPasser domesticus indicus",
            "syn1\tPasser_domesticus\tsynonym\tspecies\tFringilla domestica")));
        Assert.Equal("infraclass Neognathae > order Passeriformes > family Passeridae > genus Passer > species Passer domesticus",
            Chain(nodes, "Passer_domesticus"));
        Assert.DoesNotContain(nodes, n => n.Rank == "subspecies");
        Assert.Equal(["Passer domesticus False", "Fringilla domestica True"], names.Select(n => $"{n.Name} {n.Synonym}"));
    }

    [Fact]
    public void ColdpTaxonAndNameFilesReadNullAsEmptyAndParentZeroAsNone() {
        var (nodes, names) = Coldp(
            ("Name.tsv", Lines("ID\tscientificName\tauthorship\trank", "202423\tAnimalia\tNULL\tkingdom", "180692\tUrsus\tNULL\tgenus",
                "180542\tUrsus maritimus\tPhipps, 1774\tspecies", "900\tThalarctos maritimus\tNULL\tspecies")),
            ("Taxon.tsv", Lines("ID\tparentID\tnameID", "202423\t0\t202423", "180692\t202423\t180692", "180542\t180692\t180542")),
            ("Synonym.tsv", Lines("ID\ttaxonID\tnameID\tstatus", "180542-900\t180542\t900\tsynonym")));
        Assert.Equal("kingdom Animalia > genus Ursus > species Ursus maritimus", Chain(nodes, "180542"));
        Assert.Contains(names, n => n is { Name: "Thalarctos maritimus", NodeId: "180542", Synonym: true });
    }

    [Fact]
    public void UnwrapRemovesWhatComesBeforeTheZip() {
        var path = Path.GetTempFileName();
        try {
            File.WriteAllBytes(path, [.. Encoding.ASCII.GetBytes("--boundary\r\nContent-Disposition: form-data\r\n\r\n"), .. "PK\u0003\u0004rest"u8.ToArray()]);
            ColdpClassification.Unwrap(path);
            Assert.Equal("PK\u0003\u0004rest", Encoding.ASCII.GetString(File.ReadAllBytes(path)));
        } finally {
            File.Delete(path);
        }
    }

    private static (List<ClassificationNode> Nodes, List<ClassificationName> Names) Dwc(string header, string prefix, params string[] rows) {
        var columns = header.Split('\t');
        var meta = DwcArchive.ReadMeta($"""
            <archive xmlns="http://rs.tdwg.org/dwc/text/">
              <core encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <files><location>taxon.txt</location></files>
                <id index="0"/>
                {string.Join("\n", columns.Select((c, i) => $"<field index=\"{i}\" term=\"http://rs.tdwg.org/dwc/terms/{c}\"/>"))}
              </core>
            </archive>
            """);
        return DwcClassification.Read(DwcArchive.ReadRows(new StringReader(Lines([header, .. rows])), meta.Core), prefix).Build();
    }

    [Fact]
    public void DwcPutsTheColumnGroupsAboveTheHighestParent() {
        const string p = "urn:lsid:ipni.org:names:";
        var (nodes, names) = Dwc("taxonID\tparentNameUsageID\tacceptedNameUsageID\ttaxonomicStatus\ttaxonRank\tscientificName\tscientificNameAuthorship\tkingdom\tclass\torder\tfamily\tgenus",
            p,
            $"{p}1\t\t\tAccepted\tGenus\tMachaonia\t\tPlantae\tEquisetopsida\tGentianales\tRubiaceae\t",
            $"{p}2\t{p}1\t\tAccepted\tSpecies\tMachaonia hahniana Baill.\tBaill.\tPlantae\tEquisetopsida\tGentianales\tRubiaceae\tMachaonia",
            $"{p}3\t\t{p}2\tSynonym\tSpecies\tMachaonia oldname\t\tPlantae\t\t\t\t");
        Assert.Equal("kingdom Plantae > class Equisetopsida > order Gentianales > family Rubiaceae > genus Machaonia > species Machaonia hahniana",
            Chain(nodes, "2"));
        Assert.Contains(names, n => n is { Name: "Machaonia oldname", NodeId: "2", Synonym: true });
    }

    [Fact]
    public void DwcPutsNoColumnGroupsAboveTheArchivesOwnRoot() {
        var (nodes, _) = Dwc("taxonID\tparentNameUsageID\ttaxonomicStatus\ttaxonRank\tscientificName\tkingdom\tfamily\tgenus", "",
            "1\t\taccepted\tSuperdomain\tBiota\t\t\t",
            "2\t1\taccepted\tKingdom\tFungi\t\t\t",
            "3\t2\taccepted\tSpecies\tBotrytis cinerea\tFungi\tSclerotiniaceae\tBotrytis");
        Assert.Equal("superdomain Biota > kingdom Fungi > species Botrytis cinerea", Chain(nodes, "3"));
        Assert.Equal(3, nodes.Count);
    }

    [Fact]
    public void DwcReadsWormsUnacceptedNamesAsSynonymsAndTheBracketedRankAsItsFirstName() {
        const string p = "urn:lsid:marinespecies.org:taxname:";
        var (nodes, names) = Dwc("taxonID\tparentNameUsageID\tacceptedNameUsageID\ttaxonomicStatus\ttaxonRank\tscientificName",
            p,
            $"{p}2\t\t{p}2\taccepted\tKingdom\tAnimalia",
            $"{p}10\t{p}2\t{p}10\taccepted\tPhylum (Division)\tChordata",
            $"{p}137094\t{p}10\t{p}137094\taccepted\tSpecies\tDelphinus delphis",
            $"{p}5\t{p}10\t{p}137094\tunaccepted\tSpecies\tDelphinus vulgaris",
            $"{p}6\t{p}10\t{p}6\tdoubtful\tSpecies\tDelphinus dubius");
        Assert.Equal("kingdom Animalia > phylum Chordata > species Delphinus delphis", Chain(nodes, "137094"));
        Assert.DoesNotContain(nodes, n => n.Id == "6");
        Assert.Contains(names, n => n is { Name: "Delphinus vulgaris", NodeId: "137094", Synonym: true });
    }

    [Fact]
    public void AviListPutsEachSpeciesUnderTheRowsAboveItAndLinksItsBirdLifeId() {
        Dictionary<string, string> Row(string rank, string name, string? url = null) =>
            url is null ? new() { ["Taxon_rank"] = rank, ["Scientific_name"] = name }
                : new() { ["Taxon_rank"] = rank, ["Scientific_name"] = name, ["BirdLife_DataZone_URL"] = url };
        var (tree, links) = AviListClassification.Read([
            Row("order", "Struthioniformes"), Row("family", "Struthionidae"), Row("genus", "Struthio"),
            Row("species", "Struthio camelus", "https://datazone.birdlife.org/species/factsheet/45020636"),
            Row("subspecies", "Struthio camelus syriacus"),
        ]);
        var (nodes, _) = tree.Build();
        Assert.Equal("order Struthioniformes > family Struthionidae > genus Struthio > species Struthio camelus", Chain(nodes, "species:Struthio camelus"));
        Assert.Equal([new ClassificationLink("iucn", "45020636", "species:Struthio camelus")], links);
    }

    [Fact]
    public void TheStoreFindsNodesByNameAndReplacesASourcesRows() {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = ClassificationStore.OpenFromConnection(connection);
        store.Replace("itis", "old", "CC0", null, [new ClassificationNode("1", null, "species", "Aus bus")], [new ClassificationName("Aus bus", "1", false)]);
        store.Replace("itis", "2026-09-22", "CC0", null,
            [new ClassificationNode("10", null, "genus", "Ursus"), new ClassificationNode("11", "10", "species", "Ursus maritimus")],
            [new ClassificationName("Ursus maritimus", "11", false), new ClassificationName("Thalarctos maritimus", "11", true)],
            [new ClassificationLink("iucn", "22823", "11")]);
        Assert.Null(store.Node("itis", "1"));
        Assert.Equal(new ClassificationNode("11", "10", "species", "Ursus maritimus"), store.Node("itis", "11"));
        Assert.Equal(["11"], store.NodesNamed("itis", BeastieBot3.Shared.SiteData.SiteNameKey.Fold("URSUS MARITIMUS"), synonym: false));
        Assert.Equal(["11"], store.NodesNamed("itis", BeastieBot3.Shared.SiteData.SiteNameKey.Fold("Thalarctos maritimus"), synonym: true));
        Assert.Empty(store.NodesNamed("itis", BeastieBot3.Shared.SiteData.SiteNameKey.Fold("Thalarctos maritimus"), synonym: false));
        Assert.Equal(["11"], store.Links("itis", "iucn")["22823"]);
        Assert.Equal("2026-09-22", store.Sources().Single().Version);
    }
}
