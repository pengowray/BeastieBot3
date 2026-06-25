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

    [Theory]
    // Title-case SPRAT vernaculars are sentence-cased; possessive proper nouns are preserved.
    [InlineData("Gilbert's Potoroo", "Gilbert's potoroo")]
    [InlineData("Southern Right Whale", "Southern right whale")]
    [InlineData("Ploughshare Wattle", "Ploughshare wattle")]
    // A trailing region qualifier that distinguishes SPRAT subspecies is kept verbatim (not stripped).
    [InlineData("Nabarlek (Kimberley)", "Nabarlek (Kimberley)")]
    [InlineData("Mala (Central Australia)", "Mala (Central Australia)")]
    public void CaseVernacular_SentenceCasesAndPreservesQualifier(string raw, string expected) {
        // Empty caps rules → baseline behaviour (no place-name overrides), which is enough to pin the
        // first-word-cap / possessive-preservation / qualifier-preservation contract.
        var result = BeastieBot3.Sprat.SpratListGenerator.CaseVernacular(
            raw, new System.Collections.Generic.Dictionary<string, string>());
        Assert.Equal(expected, result);
    }

    [Theory]
    // Generic SPRAT group labels (all-lowercase, or an indefinite-article phrase) are rejected…
    [InlineData("a shrub", true)]
    [InlineData("an orchid", true)]
    [InlineData("a camaenid land snail", true)]
    [InlineData("fern", true)]
    [InlineData("land snail", true)]
    [InlineData("peacock spider", true)]
    // …while true (Title-cased) vernaculars are kept, including mixed-case edge cases.
    [InlineData("Spotted-tail Quoll", false)]
    [InlineData("Gould's Petrel", false)]
    [InlineData("Shade Tree", false)]
    [InlineData("thorntail Pipefish", false)]
    public void IsGenericDescriptor_RejectsGroupLabelsKeepsVernaculars(string name, bool generic) {
        Assert.Equal(generic, BeastieBot3.Sprat.SpratListQueryService.IsGenericDescriptor(name));
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

    [Fact]
    public void AustralianStatus_QualifyingSetAndSeverity() {
        Assert.True(AustralianStatus.IsQualifyingCode("CR"));
        Assert.True(AustralianStatus.IsQualifyingCode("NT"));
        Assert.True(AustralianStatus.IsQualifyingCode("Rare"));
        Assert.False(AustralianStatus.IsQualifyingCode("LC"));
        Assert.False(AustralianStatus.IsQualifyingCode("EX"));
        Assert.False(AustralianStatus.IsQualifyingCode(null));

        Assert.True(AustralianStatus.Severity("CR") < AustralianStatus.Severity("VU"));
        Assert.True(AustralianStatus.Severity("VU") < AustralianStatus.Severity("NT"));
        Assert.True(AustralianStatus.Severity("NT") < AustralianStatus.Severity("Rare"));

        Assert.Equal("CR", AustralianStatus.MostSevereQualifyingCode(new[] { "Rare", "CR", "VU", null, "LC" }));
        Assert.Equal("Rare", AustralianStatus.MostSevereQualifyingCode(new[] { "Rare", "LC", null }));
        Assert.Null(AustralianStatus.MostSevereQualifyingCode(new[] { "LC", "EX", null }));
    }

    // Seed with Qld (Near Threatened) + SA (Rare) columns to exercise state-act membership.
    private static SqliteConnection SeedReptilesMultiSystem() {
        var rows = new[] {
            "\"id\",\"sci\",\"common\",\"epbc\",\"iucn\",\"kingdom\",\"class\",\"order\",\"family\",\"genus\",\"qld\",\"sa\"",
            "\"Taxon ID\",\"Scientific Name\",\"Common Name\",\"EPBC Threat Status\",\"IUCN Red List\",\"Kingdom\",\"Class\",\"Order\",\"Family\",\"Genus\",\"Qld NC Act\",\"SA NPW Act\"",
            // state-only Vulnerable (Qld) -> member, section VU
            "\"1\",\"Egernia rugosa\",\"Yakka Skink\",\"\",\"\",\"Animalia\",\"Reptilia\",\"Squamata\",\"Scincidae\",\"Egernia\",\"Vulnerable\",\"\"",
            // state-only Near Threatened (Qld) -> member, section NT
            "\"2\",\"Anomalopus mackayi\",\"Long-legged Worm-skink\",\"\",\"\",\"Animalia\",\"Reptilia\",\"Squamata\",\"Scincidae\",\"Anomalopus\",\"Near Threatened\",\"\"",
            // state-only Rare (SA) -> member, section Rare
            "\"3\",\"Tympanocryptis lineata\",\"Lined Earless Dragon\",\"\",\"\",\"Animalia\",\"Reptilia\",\"Squamata\",\"Agamidae\",\"Tympanocryptis\",\"\",\"Rare\"",
            // IUCN Endangered + Qld Critically Endangered, no EPBC -> most-severe drives -> CR
            "\"4\",\"Myuchelys georgesi\",\"Bellinger River Turtle\",\"\",\"Endangered\",\"Animalia\",\"Reptilia\",\"Testudines\",\"Chelidae\",\"Myuchelys\",\"Critically Endangered\",\"\"",
            // IUCN Least Concern only, nothing else -> NOT a member
            "\"5\",\"Christinus marmoratus\",\"Marbled Gecko\",\"\",\"Least Concern\",\"Animalia\",\"Reptilia\",\"Squamata\",\"Gekkonidae\",\"Christinus\",\"\",\"\"",
        };
        var path = Path.Combine(Path.GetTempPath(), $"sprat_ms_{System.Guid.NewGuid():N}.csv");
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
    public void Query_StateActs_WidenMembership_AndDriveSection() {
        using var conn = SeedReptilesMultiSystem();
        using var query = SpratListQueryService.OpenFromConnection(conn);

        var reptiles = query.Query(new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Reptilia" }));

        // Four qualify via state acts / IUCN; the IUCN-Least-Concern-only gecko is excluded.
        Assert.Equal(4, reptiles.Count);
        Assert.DoesNotContain(reptiles, r => r.GenusName == "Christinus");

        Assert.Equal("VU", reptiles.Single(r => r.GenusName == "Egernia").StatusCode);     // state-only VU
        Assert.Equal("NT", reptiles.Single(r => r.GenusName == "Anomalopus").StatusCode);  // state-only NT
        Assert.Equal("Rare", reptiles.Single(r => r.GenusName == "Tympanocryptis").StatusCode); // state-only Rare
        // No EPBC; most-severe of IUCN EN vs Qld CR drives the section.
        var turtle = reptiles.Single(r => r.GenusName == "Myuchelys");
        Assert.Equal("CR", turtle.StatusCode);
        Assert.Equal("IUCN: EN; Qld: CR", turtle.StatusAnnotation);
    }
}
