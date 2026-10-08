using System.Text.Json;
using BeastieBot3.StatusLists;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// `statuses nztcs-import`: the name each NZTCS assessment is stored under, its status text, and the
// workflow light. The titles and species record names are from the database (October 2026).
public sealed class NztcsApiTests {
    [Theory]
    // The species record's name when the title gives the same name, or only its start.
    [InlineData("<i>Apteryx haastii</i> Potts, 1872", "Apteryx haastii", "Apteryx haastii")]
    [InlineData("<i>Acrolejeunea securifolia (Nees) Steph.</i> subsp.<i> securifolia</i>", "Acrolejeunea securifolia subsp. securifolia",
        "Acrolejeunea securifolia subsp. securifolia")]
    [InlineData("<i>Alectryon excelsus</i> Gaertn. subsp.<i> excelsus</i>", "Alectryon excelsus subsp. excelsus", "Alectryon excelsus subsp. excelsus")]
    // The title's name when the record's is out of date.
    [InlineData("<i>Apteryx australis</i> <i>australis</i> Rothschild, 1893", "Apteryx australis lawryi", "Apteryx australis australis")]
    // No species record: the title's name, with an infraspecific rank kept and the authority left out.
    [InlineData("Pyrrosia serpens (G.Forst.) Ching", null, "Pyrrosia serpens")]
    [InlineData("<i>Alectryon excelsus</i> subsp.<i> grandis</i> (Cheeseman) de Lange & E.K.Cameron", null, "Alectryon excelsus subsp. grandis")]
    [InlineData("Kunzea robusta de Lange & Toelken", null, "Kunzea robusta")]
    // A rank after the authority: the name read would be the species', so no name.
    [InlineData("<i>Acrolejeunea securifolia (Nees) Steph.</i> subsp.<i> securifolia</i>", null, null)]
    // Informal names get none, whatever the species record says.
    [InlineData("<i>Apteryx australis</i> <i>\"southern Fiordland\"</i>", "Apteryx australis australis", null)]
    [InlineData("<i>Aciphylla </i> aff.<i> glaucescens </i>(d)<i> </i>(CHR 275220; Chalk Range)<i></i>", null, null)]
    [InlineData("Agaricus sp. \"Kaitorete (PDD 105574)\"", null, null)]
    public void ChooseName(string title, string? recordName, string? expected) =>
        Assert.Equal(expected, NztcsApi.ChooseName(title, recordName));

    [Theory]
    [InlineData("Threatened", "Nationally Vulnerable", "Threatened - Nationally Vulnerable")]
    [InlineData("At Risk", "Declining", "At Risk - Declining")]
    [InlineData("Not Threatened", "Not Threatened", "Not Threatened")]
    [InlineData("Introduced and Naturalised", "Introduced and Naturalised", "Introduced and Naturalised")]
    [InlineData("Not assessed", "Not assessed", null)]
    [InlineData(null, null, null)]
    public void StatusText(string? category, string? status, string? expected) =>
        Assert.Equal(expected, NztcsApi.StatusText(category, status));

    [Fact]
    public void Kept_file_is_read_with_the_species_names() {
        var json = """
            {"assessments":[
              {"assessmentId":185032,"assessmentName":"<i>Apteryx haastii</i> Potts, 1872","categoryTitle":"Threatened",
               "conservationStatusTitle":"Nationally Vulnerable","criteriaTitle":"NVu3p","commonName":"great spotted kiwi",
               "qualifiers":"CD, RF","reportId":1001,"reportName":"Birds 2021 (Robertson et al. 2021)","reportYear":2021,"speciesId":7},
              {"assessmentId":185033,"assessmentName":"Agaricus sp. \"Kaitorete (PDD 105574)\"","categoryTitle":"Data Deficient",
               "conservationStatusTitle":"Data Deficient","speciesId":1000170},
              {"assessmentName":"no ids"}],
             "species":[{"speciesId":7,"scientificName":"Apteryx haastii","namingAuthority":"Potts, 1872"}]}
            """;
        var rows = NztcsImportCommand.Read(json);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new NztcsAssessment(185032, 7, "Apteryx haastii", "Apteryx haastii Potts, 1872", "great spotted kiwi", "Threatened",
            "Nationally Vulnerable", "NVu3p", "CD, RF", 1001, "Birds 2021 (Robertson et al. 2021)", 2021), rows[0]);
        Assert.Null(rows[1].ScientificName);
        Assert.Equal("Agaricus sp. \"Kaitorete (PDD 105574)\"", rows[1].AssessmentName);
    }

    [Fact]
    public void Page_gives_its_total_and_rows() {
        var (total, rows) = NztcsApi.ReadPage("""{"page":1,"pageSize":2,"searchResults":[{"speciesId":1},{"speciesId":2}],"total":16331}""");
        Assert.Equal(16331, total);
        Assert.Equal(2, rows.Count);
        Assert.Equal(JsonValueKind.Object, rows[1].ValueKind);
    }

    private static readonly DateTime Now = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Light() {
        var site = new PublicSiteState { StatusListsPath = "/data/status_lists.sqlite", ReadAtUtc = Now };
        Assert.Equal(("todo", "Not downloaded yet."), Result(PublicSiteProbes.NztcsStep(site)));
        Assert.Equal(("ok", "16,331 assessments, downloaded 2026-10-07."),
            Result(PublicSiteProbes.NztcsStep(site with { Nztcs = new StatusListSourceState(Now.AddDays(-1), 16331) })));
        Assert.Equal(("todo", "16,331 assessments, downloaded 2026-09-07, 31 days ago."),
            Result(PublicSiteProbes.NztcsStep(site with { Nztcs = new StatusListSourceState(Now.AddDays(-31), 16331) })));
        Assert.Equal("todo", PublicSiteProbes.NztcsStep(new PublicSiteState { ReadAtUtc = Now }).Status);
    }

    private static (string, string?) Result(FlowProbeResult r) => (r.Status, r.Detail);
}
