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
}
