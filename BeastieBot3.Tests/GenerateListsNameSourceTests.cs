using BeastieBot3.WikipediaLists;
using static BeastieBot3.WikipediaLists.WikipediaListCommand;

namespace BeastieBot3.Tests;

// Pins how `wikipedia generate-lists` picks its common-name source and whether it uses Catalogue
// of Life enrichment. Only the store-backed generator takes the CoL enricher, so when the Common
// names store is missing CoL is off too. The command used to print "Using COL taxonomy enrichment"
// in that case anyway, and switch to the Wikidata/IUCN API caches with a one-line note.
public class GenerateListsNameSourceTests {
    private const string Store = "/data/common_names.sqlite";
    private const string Col = "/data/col.sqlite";

    [Fact]
    public void StoreAndColPresent_UsesBoth() {
        var plan = PlanNameSources(useLegacyNames: false, Store, storeExists: true, noColEnrichment: false, Col, colExists: true);

        Assert.Equal(new NameSourcePlan(true, StoreFallback.None, ColEnrichment.On), plan);
    }

    [Fact]
    public void StoreFileMissing_FallsBackAndTurnsColOff() {
        var plan = PlanNameSources(useLegacyNames: false, Store, storeExists: false, noColEnrichment: false, Col, colExists: true);

        Assert.Equal(new NameSourcePlan(false, StoreFallback.NotFound, ColEnrichment.NeedsStore), plan);
    }

    [Fact]
    public void StorePathNotConfigured_FallsBackAndTurnsColOff() {
        var plan = PlanNameSources(useLegacyNames: false, storePath: null, storeExists: false, noColEnrichment: false, Col, colExists: true);

        Assert.Equal(new NameSourcePlan(false, StoreFallback.NotConfigured, ColEnrichment.NeedsStore), plan);
    }

    // The command does not resolve the store path at all with --use-legacy-names, and the option's
    // help says the CoL database is ignored as with --no-col-enrichment. So CoL is Disabled whatever
    // state its file is in; a configured CoL file that does not exist used to print a yellow
    // "Catalogue of Life database not found" warning.
    [Theory]
    [InlineData(Col, true)]
    [InlineData(Col, false)]
    [InlineData(null, false)]
    public void UseLegacyNames_SkipsStoreAndCol(string? colPath, bool colExists) {
        var plan = PlanNameSources(useLegacyNames: true, storePath: null, storeExists: false, noColEnrichment: false, colPath, colExists);

        Assert.Equal(new NameSourcePlan(false, StoreFallback.LegacyRequested, ColEnrichment.Disabled), plan);
    }

    // ColEnrichment is internal, so the expected value is passed by name (a public test method
    // cannot take an internal type).
    [Theory]
    [InlineData(true, Col, true, nameof(ColEnrichment.Disabled))]
    [InlineData(false, null, false, nameof(ColEnrichment.NotConfigured))]
    [InlineData(false, Col, false, nameof(ColEnrichment.NotFound))]
    public void ColUnavailable_WithStore(bool noColEnrichment, string? colPath, bool colExists, string expected) {
        var plan = PlanNameSources(useLegacyNames: false, Store, storeExists: true, noColEnrichment, colPath, colExists);

        Assert.True(plan.UseStore);
        Assert.Equal(expected, plan.Col.ToString());
    }

    [Fact]
    public void NoColEnrichment_IsReportedAsDisabledEvenWithoutTheStore() {
        // The user turned CoL off, so the missing store is not given as the reason.
        var plan = PlanNameSources(useLegacyNames: false, Store, storeExists: false, noColEnrichment: true, Col, colExists: true);

        Assert.Equal(ColEnrichment.Disabled, plan.Col);
    }
}
