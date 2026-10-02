using System.Text;
using BeastieBot3.Iucn.Gbif;

namespace BeastieBot3.Tests.Gbif;

// Pins the meta.xml parser: escapes in delimiter attributes, the spec's defaults, prefixed
// namespaces, and looking a term up by its full URI or by its last part.
public class DwcaArchiveMetaTests {
    private static DwcaArchiveMeta Parse(string xml) =>
        DwcaArchiveMeta.Parse(new MemoryStream(Encoding.UTF8.GetBytes(xml)));

    [Theory]
    [InlineData(@"\t", "\t")]
    [InlineData(@"\n", "\n")]
    [InlineData(@"\r\n", "\r\n")]
    [InlineData(",", ",")]
    [InlineData(@"\\", "\\")]
    [InlineData(@"\x", @"\x")]
    public void Unescape_TurnsBackslashEscapesIntoCharacters(string attribute, string expected) =>
        Assert.Equal(expected, DwcaArchiveMeta.Unescape(attribute));

    [Theory]
    [InlineData("http://rs.tdwg.org/dwc/terms/taxonID", "taxonID")]
    [InlineData("http://rs.gbif.org/terms/1.0/VernacularName", "VernacularName")]
    [InlineData("http://example.org/terms#threatStatus", "threatStatus")]
    [InlineData("dwc:language", "language")]
    [InlineData("plain", "plain")]
    public void LocalName_IsTheLastPartOfTheUri(string uri, string expected) =>
        Assert.Equal(expected, DwcaArchiveMeta.LocalName(uri));

    [Fact]
    public void Parse_PrefixedNamespace_AndSpecDefaults() {
        var meta = Parse("""
            <dwca:archive xmlns:dwca="http://rs.tdwg.org/dwc/text/">
              <dwca:core rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <dwca:files><dwca:location> taxa.csv </dwca:location></dwca:files>
                <dwca:id index="0"/>
                <dwca:field index="1" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
              </dwca:core>
            </dwca:archive>
            """);

        var core = meta.Core;
        Assert.True(core.IsCore);
        Assert.Equal("taxa.csv", core.Location);
        Assert.Equal("Taxon", core.RowTypeName);
        Assert.Equal(",", core.FieldDelimiter);
        Assert.Equal("\"", core.Quote);
        Assert.Equal(0, core.IgnoreHeaderLines);
        Assert.Equal(0, core.IdIndex);
        Assert.Null(meta.MetadataLocation);
        Assert.Empty(meta.Extensions);
    }

    [Fact]
    public void Parse_EmptyEnclosure_MeansNoQuoting() {
        var meta = Parse("""
            <archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
              <core fieldsTerminatedBy="\t" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <files><location>taxon.txt</location></files>
                <id index="0"/>
              </core>
            </archive>
            """);

        Assert.Equal("\t", meta.Core.FieldDelimiter);
        Assert.Null(meta.Core.Quote);
        Assert.Equal(1, meta.Core.IgnoreHeaderLines);
        Assert.Equal("eml.xml", meta.MetadataLocation);
    }

    [Fact]
    public void Terms_AreFoundByFullUri_OrByAnUnambiguousLastPart() {
        var meta = Parse("""
            <archive xmlns="http://rs.tdwg.org/dwc/text/">
              <core rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <files><location>taxon.txt</location></files>
                <id index="0"/>
              </core>
              <extension rowType="http://rs.gbif.org/terms/1.0/VernacularName">
                <files><location>vernacular.txt</location></files>
                <coreid index="0"/>
                <field index="1" term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
                <field index="2" term="http://rs.tdwg.org/dwc/terms/language"/>
                <field index="3" term="http://purl.org/dc/terms/source"/>
                <field index="4" term="http://rs.tdwg.org/dwc/terms/source"/>
                <field term="http://rs.tdwg.org/dwc/terms/countryCode" default="AU"/>
              </extension>
            </archive>
            """);

        var vernacular = meta.Extension("VernacularName")!;
        Assert.False(vernacular.IsCore);
        Assert.Equal(0, vernacular.IdIndex);
        Assert.Equal(1, vernacular.IndexOf("http://rs.tdwg.org/dwc/terms/vernacularName"));
        // Asked for as dcterms:language, found as dwc:language.
        Assert.Equal(2, vernacular.IndexOf("http://purl.org/dc/terms/language"));
        // Two terms end in "source", so only the full URI finds either.
        Assert.Equal(3, vernacular.IndexOf("http://purl.org/dc/terms/source"));
        Assert.Equal(4, vernacular.IndexOf("http://rs.tdwg.org/dwc/terms/source"));
        Assert.Null(vernacular.IndexOf("http://example.org/source"));
        // A default-only term has no column but has a value.
        Assert.Null(vernacular.IndexOf("http://rs.tdwg.org/dwc/terms/countryCode"));
        Assert.Equal("AU", vernacular.Value(new[] { "1", "Koala", "eng", "", "" }, "http://rs.tdwg.org/dwc/terms/countryCode"));
        Assert.Null(meta.Extension("Distribution"));
    }

    [Fact]
    public void Value_EmptyOrMissingColumn_GivesNull_AndTextIsTrimmed() {
        var meta = Parse("""
            <archive xmlns="http://rs.tdwg.org/dwc/text/">
              <core rowType="http://rs.tdwg.org/dwc/terms/Taxon">
                <files><location>taxon.txt</location></files>
                <id index="0"/>
                <field index="1" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
                <field index="5" term="http://rs.tdwg.org/dwc/terms/genus"/>
              </core>
            </archive>
            """);

        var row = new[] { " 42 ", "  Panthera tigris ", "" };
        Assert.Equal("42", meta.Core.Id(row));
        Assert.Equal("Panthera tigris", meta.Core.Value(row, "http://rs.tdwg.org/dwc/terms/scientificName"));
        Assert.Null(meta.Core.Value(row, "http://rs.tdwg.org/dwc/terms/genus"));
        Assert.Null(meta.Core.Value(row, "http://rs.tdwg.org/dwc/terms/kingdom"));
    }

    [Fact]
    public void Parse_ArchiveWithoutCore_IsRejected() {
        var ex = Assert.Throws<InvalidDataException>(() => Parse("""<archive xmlns="http://rs.tdwg.org/dwc/text/"></archive>"""));
        Assert.Contains("<core>", ex.Message);
    }
}
