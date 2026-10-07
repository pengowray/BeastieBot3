using System.IO;
using System.Linq;
using BeastieBot3.Checklists;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Tests;

// Pins the country checklist parsers and how a written place is read as countries.
public class ChecklistTests {
    private static readonly AreaNames Areas = new([
        new("ZA", "South Africa", null), new("CN", "China", null), new("CD", "Congo, The Democratic Republic of the", null),
        new("CG", "Congo", null), new("CF", "Central African Republic", null), new("KG", "Kyrgyzstan", null), new("MG", "Madagascar", null),
        new("GAL-OO", "Galápagos", "EC"), new("EC", "Ecuador", null),
    ]);

    [Theory]
    [InlineData("South Africa", "ZA")]
    [InlineData("N China", "CN")]
    [InlineData("NE Republic of South Africa", "ZA")]
    [InlineData("Central African Republic", "CF")]
    [InlineData("Congo", "CG")]
    [InlineData("Democratic Republic of Congo", "CD")]
    [InlineData("Nossi Be = Nosy Bé", "MG")]
    [InlineData("Galápagos", "EC")]
    [InlineData("Hispaniola", "DO HT")]
    [InlineData("Indian Ocean", "")]
    public void APlaceIsReadAsCountries(string place, string codes) =>
        Assert.Equal(codes, string.Join(" ", ChecklistCrosscheck.PlaceCountries(place, Areas).Order()));

    [Fact]
    public void FreeTextIsSplitIntoPlacesAndIntroductionsAreMarked() {
        var places = ChecklistSources.PlacesInText("N China (W Xinjiang), Kyrgyzstan; introduced to Madagascar [doubtful]").ToList();
        Assert.Equal([("N China", false), ("Kyrgyzstan", false), ("Madagascar", true)], places);
    }

    [Fact]
    public void MddCountriesAreSplitAndUncertainOnesMarked() {
        var csv = "sciName,countryDistribution\nOrnithorhynchus_anatinus,Australia\nHeteromys_goldmani,Guatemala|Mexico?\nHomo_sapiens,NA\nCanis_familiaris,Domesticated\n";
        var parse = ChecklistSources.ParseMdd(new StringReader(csv), "v");
        Assert.Equal(["Ornithorhynchus anatinus Australia native", "Heteromys goldmani Guatemala native", "Heteromys goldmani Mexico uncertain"],
            parse.Rows.Select(r => $"{r.ScientificName} {r.Area} {r.Origin}"));
    }

    [Fact]
    public void WcvpReadsAcceptedSpeciesWithTheirFlagsAndSynonyms() {
        var names = "plant_name_id|taxon_rank|taxon_status|taxon_name|accepted_plant_name_id\n1|Species|Accepted|Aus bus|1\n2|Species|Synonym|Aus cus|1\n3|Genus|Accepted|Aus|3\n";
        var distribution = "plant_name_id|area_code_l3|introduced|extinct|location_doubtful\n1|BZN|0|0|0\n1|KEN|1|0|0\n1|NAT|0|1|0\n1|TAN|0|0|1\n3|BZN|0|0|0\n";
        var parse = ChecklistSources.ParseWcvp(new StringReader(names), new StringReader(distribution), "v");
        Assert.Equal(["BZN native", "KEN introduced", "NAT extinct", "TAN uncertain"], parse.Rows.Select(r => $"{r.Area} {r.Origin}"));
        Assert.Equal([("Aus cus", "Aus bus")], parse.Synonyms);
    }

    [Fact]
    public void AmphibiaWebReadsIsoCodesAndTheIucnName() {
        var text = "genus\tspecies\tgaa_name\tisocc\tintro_isocc\nRhinella\tbeebei\t\tCO,TT,VE\t\nAdhaerobufo\tsp\tRhinella sp\tBR\tUS\n";
        var parse = ChecklistSources.ParseAmphibiaWeb(new StringReader(text), "v");
        Assert.Equal(["Rhinella beebei CO native", "Rhinella beebei TT native", "Rhinella beebei VE native", "Adhaerobufo sp BR native", "Adhaerobufo sp US introduced"],
            parse.Rows.Select(r => $"{r.ScientificName} {r.Area} {r.Origin}"));
        Assert.Equal([("Rhinella sp", "Adhaerobufo sp")], parse.Synonyms);
    }

    private static string BackboneRow(long id, string status, string rank, long kingdom, long species, string name) =>
        string.Join('\t', [id.ToString(), "0", "\\N", status == "SYNONYM" ? "t" : "f", status, rank, "{}", "x", "SOURCE", "0",
            kingdom.ToString(), "0", "0", "0", "0", "0", species.ToString(), "0", name + " Author", name]);

    [Fact]
    public void GbifBackboneMatchesByNameAndKingdomPreferringAcceptedNames() {
        var backbone = string.Join('\n', [
            BackboneRow(10, "ACCEPTED", "SPECIES", 1, 10, "Panthera leo"),
            BackboneRow(11, "SYNONYM", "SPECIES", 1, 99, "Panthera leo"),
            BackboneRow(20, "ACCEPTED", "SPECIES", 6, 20, "Morus alba"),
            BackboneRow(21, "ACCEPTED", "SPECIES", 1, 21, "Morus alba"),
            BackboneRow(30, "SYNONYM", "SPECIES", 1, 31, "Aus bus"),
            BackboneRow(32, "SYNONYM", "SPECIES", 1, 33, "Aus bus"),
            BackboneRow(40, "ACCEPTED", "GENUS", 1, 40, "Panthera"),
        ]);
        var wanted = new HashSet<(string, long)> { ("Panthera leo", 1), ("Morus alba", 6), ("Aus bus", 1) };
        var keys = GbifChecklist.MatchBackbone(new StringReader(backbone), wanted);
        Assert.Equal(10, keys[("Panthera leo", 1)]);
        Assert.Equal(20, keys[("Morus alba", 6)]);
        // Two synonyms leading to two species: not matched.
        Assert.False(keys.ContainsKey(("Aus bus", 1)));
    }

    [Fact]
    public void GbifFacetCountsAreRead() {
        using var json = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(
            """{"count":3,"facets":[{"field":"SPECIES_KEY","counts":[{"name":"5220126","count":3795},{"name":"2724925","count":12}]}]}"""));
        Assert.Equal([(5220126L, 3795L), (2724925L, 12L)], GbifChecklist.ReadFacet(json));
    }

    [Fact]
    public void MddGivesEnglishNamesAndSynonymsWithAuthorities() {
        var csv = "sciName,mainCommonName,otherCommonNames,countryDistribution\nAbditomys_latidens,Broad-toothed Rat,Luzon Broad-toothed Rat|NA,Philippines\n";
        var parse = ChecklistSources.ParseMdd(new StringReader(csv), "v2.5");
        Assert.Equal(["Broad-toothed Rat", "Luzon Broad-toothed Rat"], parse.Names.Select(n => n.Name));
        var synonyms = "MDD_species,MDD_author,MDD_year,MDD_authority_parentheses,MDD_validity,MDD_original_combination\n"
            + "Abditomys latidens,Sanborn,1952,0,species,Rattus latidens\n"
            + "Abditomys latidens,Musser,1982,0,synonym,Abditomys latidens\n"
            + "Abeomelomys sevia,Tate,1951,1,synonym,Pogonomelomys sevia\n"
            + "Abeomelomys sevia,Someone,1900,0,nomen_dubium,Mus dubius\n";
        var names = ChecklistSources.ParseMddSynonyms(new StringReader(synonyms)).ToList();
        Assert.Equal([new ChecklistName("Abditomys latidens", "Rattus latidens", ChecklistNameTypes.Synonym, "Sanborn, 1952"),
            new ChecklistName("Abeomelomys sevia", "Pogonomelomys sevia", ChecklistNameTypes.Synonym, "Tate, 1951")], names);
    }

    [Fact]
    public void AmphibiaWebGivesEnglishNamesAndSynonyms() {
        var text = "genus\tspecies\tcommon_name\tgaa_name\tsynonymies\tisocc\tintro_isocc\nRhinella\tbeebei\tBeebe's Toad,Beebe Toad\t\tBufo beebei\tCO\t\n";
        var parse = ChecklistSources.ParseAmphibiaWeb(new StringReader(text), "2026-04-01");
        Assert.Equal(["Beebe's Toad common", "Beebe Toad common", "Bufo beebei synonym"], parse.Names.Select(n => $"{n.Name} {n.NameType}"));
    }
}
