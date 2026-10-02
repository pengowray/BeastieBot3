using BeastieBot3.Iucn.Gbif;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// The citations the site's About page gives for its CC BY 4.0 sources: the Catalogue of Life release
// (built from metadata.yaml the way ChecklistBank shows it) and GBIF's copy of the IUCN checklist.
public class SourceCitationTests {
    private static ColMetadata Col(int creators, string? citation = null) => new() {
        Doi = "10.48580/dgykv",
        Title = "Catalogue of Life",
        Alias = "COL26.7 XR",
        Issued = "2026-07-17",
        Version = "2026-07-17 XR",
        Citation = citation,
        Creator = Enumerable.Range(1, creators).Select(i => new ColAgent { Given = "Ann", Family = $"Editor{i}" }).ToList(),
        Publisher = new ColAgent { Organisation = "Catalogue of Life Foundation", Address = "Amsterdam, Netherlands", Country = "NL" },
    };

    private const string Tail = " (2026). Catalogue of Life (2026-07-17 XR). Catalogue of Life Foundation, Amsterdam, Netherlands. https://doi.org/10.48580/dgykv";

    [Fact]
    public void Col_OneCreator() =>
        Assert.Equal("Editor1, A." + Tail, ColReleaseCitation.BuildCitation(Col(1), "10.48580/dgykv"));

    [Fact]
    public void Col_UpTo20Creators_AllListed_TheLastAfterAnAmpersand() {
        var expected = string.Join(", ", Enumerable.Range(1, 19).Select(i => $"Editor{i}, A.")) + ", & Editor20, A." + Tail;
        Assert.Equal(expected, ColReleaseCitation.BuildCitation(Col(20), "10.48580/dgykv"));
    }

    // COL26.7 XR has 871 creators; ChecklistBank lists the first 19.
    [Fact]
    public void Col_MoreThan20Creators_First19ThenEtAl() {
        var expected = string.Join(", ", Enumerable.Range(1, 19).Select(i => $"Editor{i}, A.")) + ", et al." + Tail;
        Assert.Equal(expected, ColReleaseCitation.BuildCitation(Col(871), "10.48580/dgykv"));
    }

    [Fact]
    public void Col_AnOrganisationWithNoPersonIsNamed() {
        var metadata = Col(1);
        metadata.Creator!.Add(new ColAgent { Organisation = "WoRMS Editorial Board" });
        Assert.Equal("Editor1, A., & WoRMS Editorial Board" + Tail, ColReleaseCitation.BuildCitation(metadata, "10.48580/dgykv"));
    }

    [Fact]
    public void Col_NoCreators_UsesTheCitationField() =>
        Assert.Equal("Their own citation.", ColReleaseCitation.BuildCitation(Col(0, "Their own citation."), "10.48580/dgykv"));

    [Fact]
    public void Col_NoDoi_EndsWithTheUrl() {
        var metadata = Col(1);
        metadata.Doi = null;
        metadata.Url = "https://www.checklistbank.org/dataset/315834";
        Assert.EndsWith("Amsterdam, Netherlands. https://www.checklistbank.org/dataset/315834", ColReleaseCitation.BuildCitation(metadata, null));
    }

    [Theory]
    [InlineData("Olaf", "O.")]
    [InlineData("Diana Raquel", "D. R.")]
    [InlineData("R. Edward", "R. E.")]
    [InlineData("Jean-Pierre", "J.-P.")]
    [InlineData("Ángel", "Á.")]
    public void Col_InitialsAsChecklistBankWritesThem(string given, string initials) =>
        Assert.Equal(initials, ColReleaseCitation.Initials(given));

    [Fact]
    public void Col_ReadsOnlyTheTextBeforeTheSourceList() {
        var yaml = "doi: 10.48580/dgykv\nalias: COL26.7 XR\nsource:\n - [not read\n";
        var before = ColReleaseCitation.ReadBeforeSources(new StringReader(yaml));
        Assert.Equal("doi: 10.48580/dgykv\nalias: COL26.7 XR\n", before);
        Assert.Equal(new ColReleaseCitationInfo("COL26.7 XR", "10.48580/dgykv", null), ColReleaseCitation.Parse(before));
    }

    // ------------------------------------------------------------ GBIF

    private static GbifIucnDatasetInfo Gbif(string? citation, string? identifier) =>
        new("The IUCN Red List of Threatened Species", "2026-1", "Version 2026-1", "2026-07-28", null, null, citation, identifier);

    [Fact]
    public void Gbif_TheDoiInTheCitationText() {
        var info = GbifChecklistInfo.From(Gbif(
            "IUCN (2026). The IUCN Red List of Threatened Species. Version 2026-1. https://www.iucnredlist.org. Downloaded on 2026-07-28. https://doi.org/10.15468/0qnb58",
            identifier: null));
        Assert.Equal(("2026-1", "2026-07-28", "10.15468/0qnb58"), (info.Version, info.Published, info.Doi));
    }

    [Fact]
    public void Gbif_TheCitationIdentifierFirst() =>
        Assert.Equal("10.15468/abc123", GbifChecklistInfo.From(Gbif("IUCN (2027). https://doi.org/10.15468/0qnb58", "https://doi.org/10.15468/abc123")).Doi);

    [Fact]
    public void Gbif_NoDoiGiven_TheDatasetDoi() {
        var info = GbifChecklistInfo.From(Gbif("IUCN (2026). The IUCN Red List of Threatened Species.", null));
        Assert.Equal(GbifIucnChecklistFiles.DatasetDoi, info.Doi);
        Assert.Equal("IUCN (2026). The IUCN Red List of Threatened Species.", info.Citation);
    }
}
