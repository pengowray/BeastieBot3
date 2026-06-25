using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BeastieBot3.Sprat;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins the Phase-1 Australia-list logic: the AustralianStatus vocabulary mapping, and the
// SpratListQueryService record building (membership = EPBC or IUCN CR/EN/VU, the section-driving
// primary status with IUCN fallback, the multi-system annotation, scientific-name parsing, and the
// common-name override) over a tiny in-memory SPRAT import.
public class SpratListTests {
    [Theory]
    [InlineData("Critically Endangered", "CR", true)]
    [InlineData("Endangered", "EN", true)]
    [InlineData("Vulnerable", "VU", true)]
    [InlineData("Critically Endangered (Possibly Extinct)", "CR", true)]
    [InlineData("Vulnerable (Extinct in NT)", "VU", true)]
    [InlineData("Rare, Vulnerable", "Rare", false)]   // first comma value drives the code
    [InlineData("Near Threatened", "NT", false)]
    [InlineData("Rare", "Rare", false)]
    [InlineData("Extinct", "EX", false)]
    [InlineData("Extinct in the wild", "EW", false)]
    [InlineData("Conservation Dependent", "CD", false)]
    [InlineData("", null, false)]
    public void AustralianStatus_MapsCodeAndThreatened(string raw, string? code, bool threatened) {
        Assert.Equal(code, AustralianStatus.ShortCode(raw));
        Assert.Equal(threatened, AustralianStatus.IsThreatened(raw));
    }

    private static IAnsiConsole Silent() =>
        AnsiConsole.Create(new AnsiConsoleSettings {
            Ansi = AnsiSupport.No, ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No, Out = new AnsiConsoleOutput(new StringWriter()),
        });

    // Builds a tiny SPRAT import: header (category row + column-name row) then four data rows.
    private static SqliteConnection SeedSprat() {
        var rows = new[] {
            "\"id\",\"sci\",\"common\",\"epbc\",\"iucn\",\"kingdom\",\"class\",\"order\",\"family\",\"genus\",\"nsw\",\"wa\"",
            "\"Taxon ID\",\"Scientific Name\",\"Common Name\",\"EPBC Threat Status\",\"IUCN Red List\",\"Kingdom\",\"Class\",\"Order\",\"Family\",\"Genus\",\"NSW TSC Act and FM Act\",\"WA WC Act\"",
            // member via EPBC CR; subspecies (animal trinomial); multi-system annotation
            "\"1\",\"Potorous gilbertii\",\"Gilbert's Potoroo\",\"Critically Endangered\",\"Critically Endangered\",\"Animalia\",\"Mammalia\",\"Diprotodontia\",\"Potoroidae\",\"Potorous\",\"\",\"Critically Endangered\"",
            // member via IUCN only (EPBC blank) -> section falls back to IUCN status (EN)
            "\"2\",\"Pseudomys fieldi\",\"Djoongari, Shark Bay Mouse\",\"\",\"Endangered\",\"Animalia\",\"Mammalia\",\"Rodentia\",\"Muridae\",\"Pseudomys\",\"Endangered\",\"\"",
            // plant variety, member via EPBC EN
            "\"3\",\"Acacia gunnii var. minor\",\"Ploughshare Wattle, Dog's Tooth Wattle\",\"Vulnerable\",\"\",\"Plantae\",\"Magnoliopsida\",\"Fabales\",\"Fabaceae\",\"Acacia\",\"\",\"\"",
            // NOT a member: only state-listed (Rare), no EPBC/IUCN threatened status
            "\"4\",\"Banksia nivea\",\"Honeypot Dryandra\",\"\",\"\",\"Plantae\",\"Magnoliopsida\",\"Proteales\",\"Proteaceae\",\"Banksia\",\"\",\"\"",
        };
        var path = Path.Combine(Path.GetTempPath(), $"sprat_q_{System.Guid.NewGuid():N}.csv");
        File.WriteAllText(path, string.Join("\n", rows), new UTF8Encoding(false));

        var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        var store = SpratStore.OpenFromConnection(conn);
        try {
            new SpratImporter(Silent(), store.Connection, path, "test").Run(CancellationToken.None);
        } finally {
            File.Delete(path);
        }
        return conn;
    }

    [Fact]
    public void Query_Membership_PrimaryStatus_And_Annotation() {
        using var conn = SeedSprat();
        using var query = SpratListQueryService.OpenFromConnection(conn);

        var mammals = query.Query(new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Mammalia" }));

        // Both mammals qualify (one by EPBC CR, one by IUCN EN); the non-threatened plant is excluded.
        Assert.Equal(2, mammals.Count);

        var gilbert = mammals.Single(r => r.GenusName == "Potorous");
        Assert.Equal("CR", gilbert.StatusCode);                       // EPBC drives the section
        Assert.Equal("EPBC: CR; IUCN: CR; WA: CR", gilbert.StatusAnnotation);
        Assert.Equal("Gilbert's Potoroo", gilbert.CommonNameOverride);
        Assert.Equal("Potorous", gilbert.GenusName);
        Assert.Equal("gilbertii", gilbert.SpeciesName);

        var mouse = mammals.Single(r => r.GenusName == "Pseudomys");
        Assert.Equal("EN", mouse.StatusCode);                          // EPBC blank -> IUCN fallback
        Assert.Equal("IUCN: EN; NSW: EN", mouse.StatusAnnotation);
        Assert.Equal("Djoongari", mouse.CommonNameOverride);           // first of the comma-joined names
    }

    [Fact]
    public void Query_ParsesPlantVariety_AndFiltersByKingdom() {
        using var conn = SeedSprat();
        using var query = SpratListQueryService.OpenFromConnection(conn);

        var plants = query.Query(new SpratTaxonFilter(Kingdom: "Plantae"));

        // Only the EPBC-VU variety qualifies; the state-only "Rare" Banksia is excluded.
        var acacia = Assert.Single(plants);
        Assert.Equal("VU", acacia.StatusCode);
        Assert.Equal("Acacia", acacia.GenusName);
        Assert.Equal("gunnii", acacia.SpeciesName);
        Assert.Equal("minor", acacia.InfraName);
        Assert.Equal("var.", acacia.InfraType);
        Assert.Equal("EPBC: VU", acacia.StatusAnnotation);
    }
}
