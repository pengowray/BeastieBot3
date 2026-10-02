using System.IO.Compression;
using System.Text;
using BeastieBot3.Iucn.Gbif;

namespace BeastieBot3.Tests.Gbif;

// Pins GbifIucnChecklistReader against a small Darwin Core Archive built in memory. The archive
// mixes what real archives do: a tab-separated core with no quoting and a header line, a quoted
// comma-separated extension, a term given only as a default, a vernacular language term from the
// dwc namespace instead of dcterms, and a synonym whose id is not an IUCN id.
public class GbifIucnChecklistReaderTests {
    private const string Meta = """
        <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
          <core encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
            <files><location>taxon.txt</location></files>
            <id index="0" />
            <field index="1" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
            <field index="2" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
            <field index="3" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
            <field index="4" term="http://rs.tdwg.org/dwc/terms/taxonomicStatus"/>
            <field index="5" term="http://rs.tdwg.org/dwc/terms/acceptedNameUsageID"/>
            <field index="6" term="http://purl.org/dc/terms/bibliographicCitation"/>
            <field index="7" term="http://purl.org/dc/terms/references"/>
            <field term="http://rs.tdwg.org/dwc/terms/kingdom" default="ANIMALIA"/>
          </core>
          <extension encoding="UTF-8" fieldsTerminatedBy="," linesTerminatedBy="\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="0" rowType="http://rs.gbif.org/terms/1.0/Distribution">
            <files><location>distribution.txt</location></files>
            <coreid index="0" />
            <field index="1" term="http://rs.tdwg.org/dwc/terms/locality"/>
            <field index="2" term="http://purl.org/dc/terms/source"/>
            <field index="3" term="http://iucn.org/terms/threatStatus"/>
            <field index="4" term="http://rs.tdwg.org/dwc/terms/occurrenceStatus"/>
          </extension>
          <extension encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="0" rowType="http://rs.gbif.org/terms/1.0/VernacularName">
            <files><location>vernacularname.txt</location></files>
            <coreid index="0" />
            <field index="1" term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
            <field index="2" term="http://rs.tdwg.org/dwc/terms/language"/>
            <field index="3" term="http://rs.gbif.org/terms/1.0/isPreferredName"/>
          </extension>
        </archive>
        """;

    private const string Eml = """
        <?xml version="1.0" encoding="utf-8"?>
        <eml:eml xmlns:eml="https://eml.ecoinformatics.org/eml-2.2.0" packageId="" system="http://gbif.org" scope="system" xml:lang="en">
          <dataset>
            <title>The IUCN Red List of Threatened Species</title>
            <pubDate>
                2026-07-28
            </pubDate>
            <intellectualRights>
              <para>This work is licensed under a <ulink url="http://creativecommons.org/licenses/by/4.0/legalcode"><citetitle>Creative Commons Attribution (CC-BY) 4.0 License</citetitle></ulink>.</para>
            </intellectualRights>
          </dataset>
          <additionalMetadata><metadata><gbif>
            <citation>IUCN (2026). The IUCN Red List of Threatened Species. Version 2026-1. https://www.iucnredlist.org. Downloaded on 2026-07-28. https://doi.org/10.15468/0qnb58</citation>
          </gbif></metadata></additionalMetadata>
        </eml:eml>
        """;

    private const string PolarBearCitation =
        "Wiig, Ø., Amstrup, S., Atwood, T. et al. 2015. Ursus maritimus Phipps, 1774. The IUCN Red List of Threatened Species 2015: https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en";

    // Columns: id, scientificName, authorship, rank, status, acceptedNameUsageID, citation, references.
    private static readonly string[] TaxonRows = {
        "taxonID\tscientificName\tscientificNameAuthorship\ttaxonRank\ttaxonomicStatus\tacceptedNameUsageID\tbibliographicCitation\treferences",
        $"22823\tUrsus maritimus Phipps, 1774\tPhipps, 1774\tspecies\taccepted\t22823\t{PolarBearCitation}\thttps://www.iucnredlist.org/species/22823/14871490",
        $"22823_1\tThalarctos maritimus (Phipps, 1774)\t(Phipps, 1774)\t\tsynonym\t22823\t{PolarBearCitation}\thttps://www.iucnredlist.org/species/22823/14871490",
        // An errata version published in 2016: the DOI keeps the predecessor's assessment id.
        "155037\tPhysiculus parini Paulin, 1991\tPaulin, 1991\tspecies\taccepted\t155037\tIwamoto, T. 2016. Physiculus parini. The IUCN Red List of Threatened Species 2010: doi:10.2305/IUCN.UK.2010-4.RLTS.T155037A4709219.en.\thttps://www.iucnredlist.org/species/155037/115262498",
        // No citation in the taxon row: the distribution row's source is used.
        "201631\tCalyptrogyne occidentalis (Sw.) M.Gómez\t(Sw.) M.Gómez\tspecies\taccepted\t201631\t\thttps://www.iucnredlist.org/species/201631/2709621",
        // A double quote is an ordinary character in a file with no quote character.
        "61674\tDelphinium fissum subsp. caseyi \"B.L.Burtt\" Greuter & Burdet\t\"B.L.Burtt\" Greuter & Burdet\tsubspecies (plantae)\taccepted\t61674\tSmith, A. 2013. Delphinium fissum subsp. caseyi. The IUCN Red List of Threatened Species 2013:\thttps://www.iucnredlist.org/species/61674/3107003",
        // A repeated id is not read twice.
        "61674\tDelphinium fissum subsp. caseyi\t\tsubspecies (plantae)\taccepted\t61674\t\t",
    };

    // Columns: coreid, locality, source, threatStatus, occurrenceStatus. Quoted with ".
    private static readonly string[] DistributionRows = {
        "22823,Europe,\"Regional citation, not the global one\",Least Concern,Present",
        $"22823,Global,\"{PolarBearCitation}\",Vulnerable,Present",
        "155037,Global,\"Iwamoto, T. 2016. Physiculus parini.\",Least Concern,Present",
        "201631,Global,\"Svahnström, V. 2025. Calyptrogyne occidentalis. The IUCN Red List of Threatened Species 2025: https://dx.doi.org/10.2305/IUCN.UK.2025-1.RLTS.T201631A2709621.es\",Endangered,Present",
        "61674,Global,,near threatened,Present",
        "999,Global,,Extinct,Absent",
    };

    private static readonly string[] VernacularRows = {
        "22823\tPolar Bear\teng\ttrue",
        "22823\tOurs blanc\tfra\tfalse",
        "22823_1\tIce Bear\teng\tfalse",
    };

    internal static MemoryStream BuildArchive(string meta = Meta, string? eml = Eml, bool includeExtensions = true) {
        var stream = new MemoryStream();
        using (var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true)) {
            Add(zip, "meta.xml", meta);
            if (eml is not null) {
                Add(zip, "eml.xml", eml);
            }
            Add(zip, "taxon.txt", string.Join("\n", TaxonRows) + "\n");
            if (includeExtensions) {
                Add(zip, "distribution.txt", string.Join("\n", DistributionRows) + "\n");
                Add(zip, "vernacularname.txt", string.Join("\n", VernacularRows) + "\n");
            }
        }
        stream.Position = 0;
        return stream;
    }

    private static void Add(ZipArchive zip, string name, string text) {
        using var entry = zip.CreateEntry(name).Open();
        var bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetBytes(text);
        entry.Write(bytes);
    }

    private static GbifIucnChecklist ReadSample() {
        using var stream = BuildArchive();
        return GbifIucnChecklistReader.Read(stream);
    }

    [Fact]
    public void Read_SplitsAcceptedTaxaAndSynonyms() {
        var checklist = ReadSample();

        Assert.Equal(new long[] { 22823, 61674, 155037, 201631 }, checklist.Taxa.Keys.Order().ToArray());
        var synonym = Assert.Single(checklist.Synonyms);
        Assert.Equal("22823_1", synonym.Id);
        Assert.Equal(22823, synonym.AcceptedId);
        Assert.Equal("Thalarctos maritimus (Phipps, 1774)", synonym.ScientificName);
    }

    [Fact]
    public void Read_CountsDataRowsAfterHeaderLines_AndRowsItCouldNotUse() {
        var files = ReadSample().Files.ToDictionary(f => f.RowTypeName);

        // taxon.txt: 6 data rows after the header line; the repeated 61674 is not used.
        Assert.Equal((6, 1), (files["Taxon"].Rows, files["Taxon"].UnusedRows));
        // distribution.txt: the Europe row loses to the Global row, and 999 is not a taxon.
        Assert.Equal((6, 2), (files["Distribution"].Rows, files["Distribution"].UnusedRows));
        // vernacularname.txt: the name on the synonym is not attached to any taxon.
        Assert.Equal((3, 1), (files["VernacularName"].Rows, files["VernacularName"].UnusedRows));
    }

    [Fact]
    public void Read_FillsTheTaxonFromCoreAndExtensions() {
        var bear = ReadSample().Taxa[22823];

        Assert.Equal("Ursus maritimus Phipps, 1774", bear.ScientificName);
        Assert.Equal("Phipps, 1774", bear.Authorship);
        Assert.Equal("species", bear.Rank);
        Assert.Equal("accepted", bear.TaxonomicStatus);
        Assert.Equal(22823, bear.AcceptedId);
        Assert.Equal("ANIMALIA", bear.Kingdom); // from the field's default in meta.xml
        Assert.Equal(PolarBearCitation, bear.Citation);
        Assert.Equal(GbifCitationSource.Taxon, bear.CitationSource);
        Assert.Equal("https://www.iucnredlist.org/species/22823/14871490", bear.AssessmentUrl);
        Assert.Equal(14871490, bear.AssessmentId);
        Assert.Equal("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", bear.Doi);
        Assert.Equal(new IucnDoiParts(22823, 14871490, "2015-4", "en"), bear.DoiParts);
        // The Global distribution row wins over the Europe one.
        Assert.Equal("Vulnerable", bear.ThreatStatus);
        Assert.Equal("Global", bear.Locality);
        Assert.Equal("Present", bear.OccurrenceStatus);
    }

    [Fact]
    public void Read_AttachesVernacularNames_WithLanguageFromTheDwcTerm() {
        var names = ReadSample().Taxa[22823].VernacularNames;

        Assert.Equal(
            new[] {
                new GbifIucnVernacularName("Polar Bear", "eng", true),
                new GbifIucnVernacularName("Ours blanc", "fra", false),
            },
            names);
        Assert.Empty(ReadSample().Taxa[155037].VernacularNames);
    }

    [Fact]
    public void Read_ErrataDoi_KeepsThePredecessorsAssessmentId() {
        var fish = ReadSample().Taxa[155037];

        Assert.Equal(115262498, fish.AssessmentId);
        Assert.Equal("10.2305/IUCN.UK.2010-4.RLTS.T155037A4709219.en", fish.Doi);
        Assert.Equal(4709219, fish.DoiParts!.AssessmentId);
    }

    [Fact]
    public void Read_TaxonWithNoCitation_UsesTheDistributionSource() {
        var palm = ReadSample().Taxa[201631];

        Assert.Equal(GbifCitationSource.Distribution, palm.CitationSource);
        Assert.StartsWith("Svahnström, V. 2025.", palm.Citation);
        Assert.Equal("10.2305/IUCN.UK.2025-1.RLTS.T201631A2709621.es", palm.Doi);
        Assert.Equal("Endangered", palm.ThreatStatus);
    }

    [Fact]
    public void Read_FileWithNoQuoteCharacter_KeepsDoubleQuotesAsText() {
        var plant = ReadSample().Taxa[61674];

        Assert.Equal("Delphinium fissum subsp. caseyi \"B.L.Burtt\" Greuter & Burdet", plant.ScientificName);
        // A value that starts with a double quote is the case RFC 4180 parsing would change.
        Assert.Equal("\"B.L.Burtt\" Greuter & Burdet", plant.Authorship);
        Assert.Equal("subspecies (plantae)", plant.Rank);
        Assert.Null(plant.Doi);
        Assert.Equal("near threatened", plant.ThreatStatus);
    }

    [Fact]
    public void Read_ParsesTheDatasetDescription() {
        var dataset = ReadSample().Summary.Dataset;

        Assert.Equal("The IUCN Red List of Threatened Species", dataset.Title);
        Assert.Equal("2026-1", dataset.RedListVersion);
        Assert.Equal("Version 2026-1", dataset.VersionText);
        Assert.Equal("2026-07-28", dataset.PubDate);
        Assert.Equal("http://creativecommons.org/licenses/by/4.0/legalcode", dataset.LicenceUrl);
        Assert.Equal("This work is licensed under a Creative Commons Attribution (CC-BY) 4.0 License.", dataset.LicenceText);
        Assert.StartsWith("IUCN (2026). The IUCN Red List of Threatened Species. Version 2026-1.", dataset.Citation);
    }

    [Fact]
    public void Read_ArchiveWithOnlyTheCore_HasNoExtensionData() {
        var coreOnly = Meta[..Meta.IndexOf("<extension", StringComparison.Ordinal)] + "</archive>";
        using var stream = BuildArchive(coreOnly, includeExtensions: false);

        var checklist = GbifIucnChecklistReader.Read(stream);

        var bear = checklist.Taxa[22823];
        Assert.Null(bear.ThreatStatus);
        Assert.Empty(bear.VernacularNames);
        Assert.Equal("10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", bear.Doi);
        Assert.Single(checklist.Files);
    }

    [Fact]
    public void ReadSummary_ArchiveWithoutEml_IsRejected() {
        using var stream = BuildArchive(eml: null);
        using var zip = new ZipArchive(stream, ZipArchiveMode.Read);

        var ex = Assert.Throws<InvalidDataException>(() => GbifIucnChecklistReader.ReadSummary(zip));
        Assert.Contains("eml.xml", ex.Message);
    }

    [Fact]
    public void ReadSummary_CoreThatIsNotTaxa_IsRejected() {
        var occurrences = Meta.Replace("http://rs.tdwg.org/dwc/terms/Taxon", "http://rs.tdwg.org/dwc/terms/Occurrence", StringComparison.Ordinal);
        using var stream = BuildArchive(occurrences);
        using var zip = new ZipArchive(stream, ZipArchiveMode.Read);

        var ex = Assert.Throws<InvalidDataException>(() => GbifIucnChecklistReader.ReadSummary(zip));
        Assert.Contains("not taxa", ex.Message);
    }

    [Fact]
    public void ReadSummary_ZipWithoutMetaXml_IsRejected() {
        using var stream = new MemoryStream();
        using (var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true)) {
            Add(zip, "taxon.txt", "1\tFoo bar\n");
        }
        stream.Position = 0;
        using var read = new ZipArchive(stream, ZipArchiveMode.Read);

        var ex = Assert.Throws<InvalidDataException>(() => GbifIucnChecklistReader.ReadSummary(read));
        Assert.Contains("meta.xml", ex.Message);
    }
}
