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
    public void MddSubspeciesKeepTheNamesAndAuthoritiesAndLeaveOutSynonyms() {
        // Real entries of MDD v2.5: an authority in brackets with synonyms, a plain authority, a fossil
        // note, a note with synonyms, and one with "&" in the authority.
        var text = "_T. a. acanthion_ (Collett, 1885) (synonyms: _ineptus_ Thomas, 1906); _T. a. lawesii_ Ramsay, 1877; "
            + "_T. a. dixonae_ Werdelin, 1987 (fossil); "
            + "_T. a. altus_ (Owen, 1874) (fossil; synonyms: _birdselli_ (Tedford, 1967), _cooperi_ Owen, 1874); "
            + "_T. a. ecaudatus_ (Ogilby, 1838) (recently extinct; synonyms: _castanotis_ Gray, 1842); "
            + "_T. a. clunius_ Thomas & Rothschild, 1922";
        var (subspecies, notRead) = MddSubspecies.Parse("Tachyglossus aculeatus", text);
        Assert.Equal(0, notRead);
        Assert.Equal([
            "Tachyglossus aculeatus acanthion | (Collett, 1885) | ",
            "Tachyglossus aculeatus lawesii | Ramsay, 1877 | ",
            "Tachyglossus aculeatus dixonae | Werdelin, 1987 | fossil",
            "Tachyglossus aculeatus altus | (Owen, 1874) | fossil",
            "Tachyglossus aculeatus ecaudatus | (Ogilby, 1838) | recently extinct",
            "Tachyglossus aculeatus clunius | Thomas & Rothschild, 1922 | ",
        ], subspecies.Select(s => $"{s.Name} | {s.Authority} | {s.Note}"));
        Assert.All(subspecies, s => Assert.Equal("Tachyglossus aculeatus", s.SpeciesName));
        Assert.All(subspecies, s => Assert.Equal("subspecies", s.Rank));
        // The synonyms are not subspecies.
        Assert.DoesNotContain(subspecies, s => s.Name.Contains("ineptus") || s.Name.Contains("birdselli"));
    }

    [Theory]
    [InlineData("_Z. b. bartoni_ (Thomas, 1907)")]              // initials of another species
    [InlineData("T. a. acanthion (Collett, 1885)")]             // no underscores
    [InlineData("_T. a. acanthion_ (Collett, 1885) (nomen nudum)")] // a note MDD does not use
    [InlineData("_T. a. Acanthion_ Collett, 1885")]             // a capitalised epithet
    public void MddSubspeciesEntriesOfAnotherFormAreCountedNotRead(string entry) {
        var (subspecies, notRead) = MddSubspecies.Parse("Tachyglossus aculeatus", entry + "; _T. a. lawesii_ Ramsay, 1877");
        Assert.Equal(1, notRead);
        Assert.Equal(["Tachyglossus aculeatus lawesii"], subspecies.Select(s => s.Name));
    }

    [Fact]
    public void MddSubspeciesOfNoneAreEmpty() {
        var (none, notRead) = MddSubspecies.Parse("Ornithorhynchus anatinus", "NA");
        Assert.Empty(none);
        Assert.Equal(0, notRead);
        Assert.Empty(MddSubspecies.Parse("Ornithorhynchus anatinus", "").Subspecies);
    }

    [Fact]
    public void MddGivesEachSpeciesItsIdAndSubspecies() {
        var csv = "sciName,id,subspecies,countryDistribution\n"
            + "Tachyglossus_aculeatus,1000002,\"_T. a. acanthion_ (Collett, 1885) (synonyms: _ineptus_ Thomas, 1906); _T. a. lawesii_ Ramsay, 1877\",Australia\n"
            + "Ornithorhynchus_anatinus,1000001,NA,Australia\n";
        var parse = ChecklistSources.ParseMdd(new StringReader(csv), "v2.5");
        Assert.Equal([new ChecklistSpecies("Tachyglossus aculeatus", "1000002"), new ChecklistSpecies("Ornithorhynchus anatinus", "1000001")], parse.Species);
        Assert.Equal(["Tachyglossus aculeatus acanthion", "Tachyglossus aculeatus lawesii"], parse.Infraspecific.Select(s => s.Name));
        Assert.Equal(0, parse.InfraspecificNotRead);
    }

    [Fact]
    public void ReptileDatabaseReadsAcceptedSubspeciesWithTheirAuthorshipAndTheSpeciesPage() {
        var names = "id\tscientific_name\tauthorship\trank\n"
            + "N1\tZootoca vivipara\t(Lichtenstein, 1823)\tspecies\n"
            + "N2\tZootoca vivipara louislantzi\tArribas, 2009\tsubspecies\n"
            + "N3\tZootoca vivipara pannonica\t(Lac & Kluch, 1968)\tsubspecies\n"
            + "N4\tLacerta vivipara sachalinensis\tPerelesin, 1919\tsubspecies\n"
            + "N5\tPhelsuma nigra\tBoettger, 1913\tspecies\n"
            + "N6\tPhelsuma v-nigra anjouanensis\tMeier, 1986\tsubspecies\n";
        // The species before its subspecies and after them: the order of Taxon.tsv does not matter.
        var taxa = "id\tparent_id\tname_id\tlink\n"
            + "T2\tT1\tN2\t\n"
            + "T1\tG1\tN1\thttps://reptile-database.reptarium.cz/species?genus=Zootoca&species=vivipara\n"
            + "T3\tT1\tN3\t\n"
            + "T5\tG2\tN5\t\n"
            + "T6\tT5\tN6\t\n";
        // N4 is only a synonym, so it is not one of the species' subspecies.
        var synonyms = "id\ttaxon_id\tname_id\nS1\tT1\tN4\n";
        var parse = ChecklistSources.ParseReptileDatabase(new StringReader(names), new StringReader(taxa),
            new StringReader("taxon_id\tarea\n"), new StringReader(synonyms), "v");
        Assert.Equal([
            "Zootoca vivipara | Zootoca vivipara louislantzi | Arribas, 2009",
            "Zootoca vivipara | Zootoca vivipara pannonica | (Lac & Kluch, 1968)",
            // As the source has it: the Reptile Database's export gives "Phelsuma nigra" for P. v-nigra.
            "Phelsuma nigra | Phelsuma v-nigra anjouanensis | Meier, 1986",
        ], parse.Infraspecific.Select(s => $"{s.SpeciesName} | {s.Name} | {s.Authority}"));
        // The species page from the taxon's link, else from the name.
        Assert.Equal([new ChecklistSpecies("Zootoca vivipara", "genus=Zootoca&species=vivipara"), new ChecklistSpecies("Phelsuma nigra", "genus=Phelsuma&species=nigra")],
            parse.Species);
    }

    [Fact]
    public void AnImportReplacesTheSourcesSpeciesAndSubspecies() {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var store = ChecklistStore.OpenFromConnection(connection);
        store.Replace("mdd", "v2.4", "CC BY 4.0", null, [], species: [new ChecklistSpecies("Panthera leo", "1006020")],
            infraspecific: [new ChecklistInfraspecific("Panthera leo", "Panthera leo leo", "subspecies", "(Linnaeus, 1758)"),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo nubica", "subspecies")]);
        store.Replace("reptiledb", "v", "CC BY", null, [], species: [new ChecklistSpecies("Python regius", "genus=Python&species=regius")],
            infraspecific: [new ChecklistInfraspecific("Python regius", "Python regius regius", "subspecies")]);
        store.Replace("mdd", "v2.5", "CC BY 4.0", null, [], species: [new ChecklistSpecies("Panthera leo", "1006020")],
            infraspecific: [new ChecklistInfraspecific("Panthera leo", "Panthera leo melanochaita", "subspecies", "(Smith, 1842)"),
                new ChecklistInfraspecific("Panthera leo", "Panthera leo sinhaleya", "subspecies", "Deraniyagala, 1938", MddSubspecies.Fossil)]);

        Assert.Equal([
            new ChecklistInfraspecific("Panthera leo", "Panthera leo melanochaita", "subspecies", "(Smith, 1842)"),
            new ChecklistInfraspecific("Panthera leo", "Panthera leo sinhaleya", "subspecies", "Deraniyagala, 1938", "fossil"),
        ], store.Infraspecific("mdd"));
        Assert.Equal([new ChecklistSpecies("Panthera leo", "1006020")], store.Species("mdd"));
        // The other source's rows stay.
        Assert.Equal(["Python regius regius"], store.Infraspecific("reptiledb").Select(i => i.Name));
        Assert.Equal([("mdd", 2L), ("reptiledb", 1L)], store.Sources().Select(s => (s.Source, s.Subspecies)));
    }

    [Fact]
    public void AmphibiaWebGivesEnglishNamesAndSynonyms() {
        var text = "genus\tspecies\tcommon_name\tgaa_name\tsynonymies\tisocc\tintro_isocc\nRhinella\tbeebei\tBeebe's Toad,Beebe Toad\t\tBufo beebei\tCO\t\n";
        var parse = ChecklistSources.ParseAmphibiaWeb(new StringReader(text), "2026-04-01");
        Assert.Equal(["Beebe's Toad common", "Beebe Toad common", "Bufo beebei synonym"], parse.Names.Select(n => $"{n.Name} {n.NameType}"));
    }
}
