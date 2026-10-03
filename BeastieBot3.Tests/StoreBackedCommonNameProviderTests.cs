using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3.CommonNames;
using BeastieBot3.Wikipedia;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins which Wikipedia article a list line links: the page the taxon's Wikipedia name came from;
// else the taxon's own scientific name when English Wikipedia has a page or a redirect with it;
// else the page the taxon is matched to. Linking the matched page bypassed redirects: the lists
// linked [[Leucoraja]] for Leucoraja wallacei, whose own name redirects to the genus article's
// species list (October 2026).
public sealed class StoreBackedCommonNameProviderTests : IDisposable {
    private readonly string _rulesPath = Path.GetTempFileName();

    public void Dispose() => File.Delete(_rulesPath);

    private static CommonNameStore OpenInMemory() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        return CommonNameStore.OpenFromConnection(connection);
    }

    private static long AddTaxon(CommonNameStore store, string canonical, long iucnId) =>
        store.InsertOrUpdateTaxon(canonical, canonical, "species", "ANIMALIA",
            isExtinct: false, isFossil: false, validityStatus: "valid", primarySource: "iucn", primarySourceId: iucnId.ToString());

    private static IucnSpeciesRecord Record(long iucnId, string genus, string species, string? infraType = null,
        string? infraName = null) {
        var scientific = infraName is null ? $"{genus} {species}" : $"{genus} {species} {infraType} {infraName}";
        return new IucnSpeciesRecord(
            TaxonId: iucnId, AssessmentId: 1, RedlistCategory: "Vulnerable", StatusCode: "VU",
            ScientificNameAssessments: scientific, ScientificNameTaxonomy: scientific,
            KingdomName: "ANIMALIA", PhylumName: "CHORDATA", ClassName: "CHONDRICHTHYES",
            OrderName: "RAJIFORMES", FamilyName: "RAJIDAE", GenusName: genus, SpeciesName: species,
            InfraType: infraType, InfraName: infraName, SubpopulationName: null, Scopes: "Global",
            Authority: null, InfraAuthority: null, PossiblyExtinct: null, PossiblyExtinctInTheWild: null,
            YearPublished: "2024", CommonNameOverride: null, ArticleTitleOverride: null);
    }

    private static Func<string, bool> Titles(params string[] titles) {
        var set = new HashSet<string>(titles, StringComparer.Ordinal);
        return set.Contains;
    }

    [Fact]
    public void AMatchedPage_IsNotLinked_WhenTheOwnNameHasAPageOrRedirect() {
        using var store = OpenInMemory();
        var skate = AddTaxon(store, "leucoraja wallacei", 161492);
        store.InsertCrossReference(skate, "wikipedia", "Leucoraja");
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Leucoraja wallacei", "Leucoraja"));

        Assert.Equal("Leucoraja wallacei", provider.GetWikipediaArticleTitle(Record(161492, "Leucoraja", "wallacei")));
        Assert.Equal("Leucoraja wallacei", provider.GetWikipediaArticleTitleByScientificName("Leucoraja wallacei"));
    }

    [Fact]
    public void AMatchedPage_IsLinked_WhenTheOwnNameWouldBeARedlink() {
        // Wikipedia has no page "Moolgarda buchanani"; its article is "Crenimugil buchanani".
        using var store = OpenInMemory();
        var mullet = AddTaxon(store, "moolgarda buchanani", 1);
        store.InsertCrossReference(mullet, "wikipedia", "Crenimugil buchanani");
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Crenimugil buchanani"));

        Assert.Equal("Crenimugil buchanani", provider.GetWikipediaArticleTitle(Record(1, "Moolgarda", "buchanani")));
    }

    [Fact]
    public void WithoutAWikipediaCache_TheMatchedPageIsLinked() {
        using var store = OpenInMemory();
        var skate = AddTaxon(store, "leucoraja wallacei", 161492);
        store.InsertCrossReference(skate, "wikipedia", "Leucoraja");
        using var provider = new StoreBackedCommonNameProvider(store);

        Assert.Equal("Leucoraja", provider.GetWikipediaArticleTitle(Record(161492, "Leucoraja", "wallacei")));
    }

    [Fact]
    public void ThePageAWikipediaNameCameFrom_IsLinkedFirst() {
        using var store = OpenInMemory();
        var cichlid = AddTaxon(store, "rocio octofasciata", 1);
        store.InsertCrossReference(cichlid, "wikipedia", "Jack Dempsey (fish)");
        store.InsertCommonName(cichlid, "Jack Dempsey", "jackdempsey", "en", "wikipedia_title", "Jack Dempsey (fish)", true);
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Rocio octofasciata"));

        Assert.Equal("Jack Dempsey (fish)", provider.GetWikipediaArticleTitle(Record(1, "Rocio", "octofasciata")));
    }

    [Fact]
    public void ATaxonMatchedToNoPage_HasNoArticle_EvenWhenItsNameHasAPage() {
        // The line then links the scientific name itself (SpeciesLineFormatter).
        using var store = OpenInMemory();
        AddTaxon(store, "leucoraja wallacei", 161492);
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Leucoraja wallacei"));

        Assert.Null(provider.GetWikipediaArticleTitle(Record(161492, "Leucoraja", "wallacei")));
    }

    [Fact]
    public void ASubspecies_IsCheckedByItsNameWithoutTheRankMarker() {
        using var store = OpenInMemory();
        var subspecies = AddTaxon(store, "leucoraja wallacei ssp. australis", 2);
        store.InsertCrossReference(subspecies, "wikipedia", "Leucoraja");
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Leucoraja wallacei australis"));

        Assert.Equal("Leucoraja wallacei australis",
            provider.GetWikipediaArticleTitle(Record(2, "Leucoraja", "wallacei", "ssp.", "australis")));
    }

    [Fact]
    public void TheLine_LinksTheRedirect() {
        using var store = OpenInMemory();
        var skate = AddTaxon(store, "leucoraja wallacei", 161492);
        store.InsertCrossReference(skate, "wikipedia", "Leucoraja");
        using var provider = new StoreBackedCommonNameProvider(store, titleExists: Titles("Leucoraja wallacei"));
        var formatter = new SpeciesLineFormatter(new LegacyTaxaRuleList(_rulesPath), provider, commonNameProvider: null);
        var style = new DisplayPreferences { ListingStyle = ListingStyle.CommonNameFocus, IncludeStatusTemplate = false };

        Assert.Equal("* ''[[Leucoraja wallacei]]''", formatter.FormatSpeciesLine(Record(161492, "Leucoraja", "wallacei"), style, null));
    }

    // EnwikiTitleCheck reads the Wikipedia cache's list of every article title and its downloaded pages.

    [Fact]
    public void TitleCheck_ATitleExists_WhenItIsInTheTitleListOrDownloaded() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var cache = WikipediaCacheStore.OpenFromConnection(connection);
        var now = DateTime.UtcNow;
        long Page(string title) => cache.UpsertPageCandidate(new WikiPageCandidate(title, title, null, now, now)).PageRowId;

        cache.AddDumpTitles(["Leucoraja wallacei", "Hylambates maculatus"]);
        var redirect = Page("Iphisa elegans");
        cache.MarkRedirectStub(redirect, "Iphisa elegans", "Iphisa elegans", "Iphisa", now);
        var missing = Page("Moolgarda buchanani");
        cache.MarkPageMissing(missing, "missing", now);
        // Recorded missing in June, but in the August list of titles: the page was made since.
        var madeSince = Page("Hylambates maculatus");
        cache.MarkPageMissing(madeSince, "missing", now);
        Page("Queued only");
        using var check = new EnwikiTitleCheck(connection, ownsConnection: false);

        Assert.True(check.Exists("Leucoraja wallacei"));
        Assert.True(check.Exists("leucoraja wallacei"));
        Assert.True(check.Exists("Iphisa elegans"));
        Assert.True(check.Exists("Hylambates maculatus"));
        Assert.False(check.Exists("Moolgarda buchanani"));
        Assert.False(check.Exists("Queued only"));
        Assert.False(check.Exists("Never heard of"));
        Assert.True(EnwikiTitleCheck.IsDownloaded(cache, "Iphisa elegans"));
        Assert.False(EnwikiTitleCheck.IsDownloaded(cache, "Leucoraja wallacei"));
    }
}
