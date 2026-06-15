using BeastieBot3.Iucn;

namespace BeastieBot3.Tests;

// Pins the API→projection mapping for infraspecific taxa. The IUCN API marks a subspecies/variety
// with taxon.infrarank == true (a JSON bool) and taxon.infra_name set — the rank word only appears
// as a marker in scientific_name. The parser must derive a CSV-style infraType ("subspecies"/
// "variety"), else GlobalSpeciesPredicate would miscount infraspecific assessments as species.
public class IucnAssessmentJsonParserTests {
    [Fact]
    public void Parse_Species_HasNullInfraType() {
        const string json = """
        {"assessment_id": 1, "latest": true, "sis_taxon_id": 100,
         "taxon": {"scientific_name": "Panthera leo", "infrarank": false, "infra_name": null,
                   "kingdom_name": "ANIMALIA", "genus_name": "Panthera", "species_name": "leo"}}
        """;
        var a = IucnAssessmentJsonParser.Parse(json);
        Assert.NotNull(a);
        Assert.Null(a!.InfraType);
        Assert.Equal(100, a.TaxonId);
    }

    [Fact]
    public void Parse_Subspecies_DerivesSubspeciesFromBoolInfrarank() {
        // Mirrors a real payload: infrarank is the bool true, infra_type null, marker is "ssp.".
        const string json = """
        {"assessment_id": 13051607, "latest": true, "sis_taxon_id": 210,
         "taxon": {"scientific_name": "Achatinella bulimoides ssp. rosea", "infrarank": true,
                   "infra_name": "rosea", "infra_type": null,
                   "kingdom_name": "ANIMALIA", "genus_name": "Achatinella", "species_name": "bulimoides"}}
        """;
        var a = IucnAssessmentJsonParser.Parse(json);
        Assert.NotNull(a);
        Assert.Equal("subspecies", a!.InfraType);
        Assert.Equal("rosea", a.InfraName);
        Assert.Equal(210, a.TaxonId);
    }

    [Fact]
    public void Parse_PlantSubspecies_DerivesSubspeciesPlantae() {
        // Matches the CSV's botanical label "subspecies (plantae)" for PLANTAE subspecies.
        const string json = """
        {"assessment_id": 4, "latest": true, "sis_taxon_id": 32874,
         "taxon": {"scientific_name": "Warburgia ugandensis subsp. longifolia", "infrarank": true,
                   "infra_name": "longifolia", "kingdom_name": "PLANTAE",
                   "genus_name": "Warburgia", "species_name": "ugandensis"}}
        """;
        var a = IucnAssessmentJsonParser.Parse(json);
        Assert.NotNull(a);
        Assert.Equal("subspecies (plantae)", a!.InfraType);
    }

    [Fact]
    public void Parse_Variety_DerivesVarietyFromScientificNameMarker() {
        const string json = """
        {"assessment_id": 2, "latest": true, "sis_taxon_id": 300,
         "taxon": {"scientific_name": "Quercus robur var. fastigiata", "infrarank": true,
                   "infra_name": "fastigiata", "kingdom_name": "PLANTAE",
                   "genus_name": "Quercus", "species_name": "robur"}}
        """;
        var a = IucnAssessmentJsonParser.Parse(json);
        Assert.NotNull(a);
        Assert.Equal("variety", a!.InfraType);
    }

    [Fact]
    public void Parse_ExplicitInfraTypeString_WinsOverDerivation() {
        const string json = """
        {"assessment_id": 3, "latest": true, "sis_taxon_id": 400,
         "taxon": {"scientific_name": "Genus species ssp. thing", "infra_type": "subspecies (plantae)",
                   "infra_name": "thing", "genus_name": "Genus", "species_name": "species"}}
        """;
        var a = IucnAssessmentJsonParser.Parse(json);
        Assert.NotNull(a);
        Assert.Equal("subspecies (plantae)", a!.InfraType);
    }
}
