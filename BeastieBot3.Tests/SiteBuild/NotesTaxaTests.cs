using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// The names read from taxonomic notes (TaxonomicNotesNames) and the taxa they are found to be
// (SiteNotesTaxa), for notes_taxon. The notes text below is made up, in the form IUCN writes it.
public class NotesTaxaTests {
    [Fact]
    public void Read_WholeNamesAndAbbreviations_InOrder() {
        const string notes = "In 1940, Lönnberg described the subspecies <i>Cebuella pygmaea niveiventris</i> from Lago Ipixuna. "
            + "The ventral surface of <i>C. p. pygmaea</i> is ochraceous. Evidence for <i>C. p. niveiventris</i>.<br>"
            + "Other names: <i>Callithrix pygmaea</i>, <i>Callithrix (Cebuella) pygmaea</i>; see <i>et al.</i> and <em>C. niveiventris</em>.";
        var names = TaxonomicNotesNames.Read(notes, "Cebuella", "pygmaea");
        Assert.Equal([
            ("Cebuella pygmaea niveiventris", "Cebuella pygmaea niveiventris"),
            ("C. p. pygmaea", "Cebuella pygmaea pygmaea"),
            ("Callithrix pygmaea", "Callithrix pygmaea"),
            // The latest full genus with the initial C is Callithrix by now.
            ("C. niveiventris", "Callithrix niveiventris"),
        ], names.Select(n => (n.Written, n.Full)));
    }

    [Fact]
    public void Read_AbbreviationBeforeAnyWholeName_UsesTheTaxonsOwnGenus() {
        var names = TaxonomicNotesNames.Read("Formerly treated as a subspecies of <i>P. gangetica</i>.", "Platanista", "minor");
        Assert.Equal("Platanista gangetica", Assert.Single(names).Full);
    }

    [Fact]
    public void Read_KeepsTheWrittenOutForm_OfANameAlsoAbbreviated() {
        var names = TaxonomicNotesNames.Read("<i>C. p. niveiventris</i> and later <i>Cebuella pygmaea niveiventris</i>", "Cebuella", "pygmaea");
        Assert.Equal("Cebuella pygmaea niveiventris", Assert.Single(names).Written);
    }

    [Theory]
    [InlineData("<i>et al.</i>")]
    [InlineData("<i>niveiventris</i>")]
    [InlineData("<i>Cebuella</i>")]
    [InlineData("Cebuella pygmaea, not in italics")]
    [InlineData("<i>X. y</i>")]
    [InlineData("")]
    public void Read_NoName(string notes) {
        Assert.Empty(TaxonomicNotesNames.Read(notes, "Cebuella", "pygmaea"));
    }

    private static SiteTaxon Taxon(long id, string name, string kind = "species", string kingdom = "ANIMALIA", bool inRelease = true, params string[] synonyms) {
        var words = name.Split(' ');
        return new SiteTaxon {
            TaxonId = id, ScientificName = name, Kind = kind, Kingdom = kingdom, Genus = words[0], SpeciesEpithet = words[1], InRelease = inRelease,
            IucnSynonyms = synonyms.Select(s => new SiteSynonym(s)).ToList(),
        };
    }

    private static IReadOnlyDictionary<long, List<(long, IReadOnlyList<NotesName>)>> Notes(long taxonId, params (string Written, string Full)[] names) =>
        new Dictionary<long, List<(long, IReadOnlyList<NotesName>)>> { [taxonId] = [(taxonId * 10, names.Select(n => new NotesName(n.Written, n.Full)).ToList())] };

    [Fact]
    public void Find_ByNameThenByOneTaxonsSynonym() {
        var pygmaea = Taxon(136926, "Cebuella pygmaea", synonyms: ["Callithrix pygmaea", "Cebuella pygmaea ssp. pygmaea"]);
        var niveiventris = Taxon(136865, "Cebuella niveiventris", synonyms: ["Cebuella pygmaea ssp. niveiventris"]);
        var marmoset = Taxon(41518, "Callithrix jacchus");
        var stats = new SiteBuildStats();
        var rows = SiteNotesTaxa.Find([pygmaea, niveiventris, marmoset], Notes(136926,
            ("Cebuella pygmaea niveiventris", "Cebuella pygmaea niveiventris"),
            ("C. p. pygmaea", "Cebuella pygmaea pygmaea"),       // its own synonym: left out
            ("Callithrix pygmaea", "Callithrix pygmaea"),        // its own synonym: left out
            ("Callithrix jacchus", "Callithrix jacchus"),
            ("C. niveiventris", "Cebuella niveiventris")), stats); // the same taxon again: left out
        Assert.Equal([
            new SiteNotesTaxon(136926, 136865, 1369260, "Cebuella pygmaea niveiventris", 0),
            new SiteNotesTaxon(136926, 41518, 1369260, null, 1),
        ], rows);
        Assert.Equal((2, 2, 1), (stats.NotesTaxa, stats.NotesTaxonRows, stats.TaxaWithNotesTaxa));
    }

    [Fact]
    public void Find_EveryAssessment_AndTaxaNotInTheRelease() {
        // An old id whose 2008 and 2016 notes name the taxon it was split into.
        var old = Taxon(41758, "Platanista gangetica", inRelease: false);
        var minor = Taxon(41757, "Platanista minor");
        var notes = new Dictionary<long, List<(long, IReadOnlyList<NotesName>)>> {
            [41758] = [
                (2016, [new NotesName("P. minor", "Platanista minor")]),
                (2008, [new NotesName("Platanista gangetica minor", "Platanista gangetica minor"), new NotesName("Platanista minor", "Platanista minor")]),
            ],
        };
        minor.IucnSynonyms = [new SiteSynonym("Platanista gangetica ssp. minor")];
        var stats = new SiteBuildStats();
        var rows = SiteNotesTaxa.Find([old, minor], notes, stats);
        // In 2008 the first name for it is its old one; one row per assessment.
        Assert.Equal([
            new SiteNotesTaxon(41758, 41757, 2008, "Platanista gangetica minor", 0),
            new SiteNotesTaxon(41758, 41757, 2016, null, 0),
        ], rows);
        Assert.Equal((1, 2, 1), (stats.NotesTaxa, stats.NotesTaxonRows, stats.TaxaWithNotesTaxa));
    }

    [Fact]
    public void Find_RankMarkersIgnored_OwnSpeciesAndSubspeciesLeftOut() {
        var species = Taxon(182228, "Sarotherodon tournieri");
        var subspecies = Taxon(183044, "Sarotherodon tournieri ssp. liberiensis", kind: "subspecies");
        var other = Taxon(166520, "Sarotherodon caudomarginatus ssp. caudomarginatus", kind: "subspecies");
        var rows = SiteNotesTaxa.Find([species, subspecies, other], Notes(182228,
            ("Sarotherodon tournieri liberiensis", "Sarotherodon tournieri liberiensis"),
            ("Sarotherodon caudomarginatus caudomarginatus", "Sarotherodon caudomarginatus caudomarginatus")), new SiteBuildStats());
        Assert.Equal(166520, Assert.Single(rows).NamedTaxonId);
        Assert.Null(rows[0].NameInNotes);
    }

    [Fact]
    public void Find_OtherKingdomsTaxaNotInTheReleaseAndNamesOfSeveralTaxa_LeftOut() {
        var bird = Taxon(1, "Ficus variegata");
        var plant = Taxon(2, "Ficus variegata", kingdom: "PLANTAE");
        var old = Taxon(3, "Ficus aurea", inRelease: false);
        var a = Taxon(4, "Ficus alpha", synonyms: ["Ficus beta"]);
        var b = Taxon(5, "Ficus gamma", synonyms: ["Ficus beta"]);
        var stats = new SiteBuildStats();
        var rows = SiteNotesTaxa.Find([bird, plant, old, a, b], Notes(4,
            ("Ficus variegata", "Ficus variegata"), ("Ficus aurea", "Ficus aurea"), ("Ficus beta", "Ficus beta")), stats);
        // Ficus variegata: only the animal of that name (the same kingdom as Ficus alpha). Ficus beta
        // is its own synonym. Ficus aurea is not in the release.
        Assert.Equal(1, Assert.Single(rows).NamedTaxonId);

        rows = SiteNotesTaxa.Find([bird, plant, old, a, b, Taxon(6, "Ficus delta")], Notes(6, ("Ficus beta", "Ficus beta")), stats);
        Assert.Empty(rows);
        Assert.Equal(1, stats.NotesNamesOfSeveralTaxa);
    }
}

// The Red List Authority of a taxon record (taxon.ssc_groups): name and http(s) website only.
public class RedListAuthorityTests {
    [Fact]
    public void ReadSscGroups_NameAndWebsite_DescriptionLeftOut() {
        using var document = System.Text.Json.JsonDocument.Parse("""
            {"ssc_groups": [
              {"name": "IUCN SSC Primate Specialist Group", "url": "http://www.primate-sg.org/", "description": "Chair: A. B. (email: a@example.org)"},
              {"name": "IUCN SSC Primate Specialist Group", "url": "http://www.primate-sg.org/"},
              {"name": "Another group", "url": "mailto:a@example.org"},
              {"name": " ", "url": "https://example.org/"},
              {"url": "https://example.org/"}
            ]}
            """);
        Assert.Equal([("IUCN SSC Primate Specialist Group", "http://www.primate-sg.org/"), ("Another group", (string?)null)],
            SiteApiTaxaReader.ReadSscGroups(document.RootElement));
    }

    [Fact]
    public void ReadSscGroups_NoneWhenMissing() {
        using var document = System.Text.Json.JsonDocument.Parse("""{"scientific_name": "Ursus maritimus"}""");
        Assert.Empty(SiteApiTaxaReader.ReadSscGroups(document.RootElement));
    }
}
