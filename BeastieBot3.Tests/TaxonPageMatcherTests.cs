using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikidata;
using BeastieBot3.Wikipedia;

namespace BeastieBot3.Tests;

// Pins the kingdom check in `wikipedia match-taxa` (TaxonPageMatcher) with the two plants it had
// matched to animal articles in October 2026: Ficus variegata to "Ficus variegata (gastropod)"
// and Gaussia princeps to "Gaussia princeps (crustacean)", both through their Wikidata links.
public sealed class TaxonPageMatcherTests : IDisposable {
    private readonly WikiKingdomFixture _fixture = new();

    public void Dispose() => _fixture.Dispose();

    private WikipediaCacheStore Cache => _fixture.Cache;

    private static WikipediaMatchCandidate Candidate(string title, string method = "iucn-taxonomy") =>
        new(title, WikipediaTitleHelper.Normalize(title), method, method, false, null);

    // The candidates match-taxa builds for a plant: the Wikidata sitelink, then the IUCN name.
    private IReadOnlyList<WikipediaMatchCandidate> Candidates(string wikidataTitle, string name, string kingdom) =>
        TaxonPageMatcher.BuildCandidates(
            new WikidataIucnMatchCandidate(wikidataTitle, "CachedName", name, false),
            [new TaxonNameCandidate(name, TaxonNameSource.IucnTaxonomy)],
            n => TaxonPageMatcher.KingdomQualifiedTitles(Cache, n, kingdom));

    private TaxonMatchOutcome Process(string taxonId, string kingdom, Func<IReadOnlyList<WikipediaMatchCandidate>> candidates,
        bool pendingOnly = true) =>
        TaxonPageMatcher.ProcessTaxon(taxonId, kingdom, Cache.GetTaxonMatch(TaxonSources.Iucn, taxonId), Cache, candidates,
            reprocessMatched: false, pendingOnly: pendingOnly, CancellationToken.None);

    [Fact]
    public void APageAboutAnotherKingdom_IsRejected() {
        var forPlant = TaxonPageMatcher.EvaluateCandidate(Candidate("Ficus variegata (gastropod)", "wikidata"), "PLANTAE", Cache);
        var forAnimal = TaxonPageMatcher.EvaluateCandidate(Candidate("Ficus variegata (gastropod)", "wikidata"), "ANIMALIA", Cache);

        Assert.Equal(CandidateEvaluationStatus.Rejected, forPlant.Status);
        Assert.Contains("ANIMALIA, not PLANTAE", forPlant.Notes);
        Assert.Equal(CandidateEvaluationStatus.Matched, forAnimal.Status);
    }

    [Fact]
    public void ARedirectToAPageAboutAnotherKingdom_IsRejected() {
        _fixture.AddRedirect("Gaussia scotti", "Gaussia princeps (crustacean)");

        var evaluation = TaxonPageMatcher.EvaluateCandidate(Candidate("Gaussia scotti", "iucn-synonym"), "PLANTAE", Cache);

        Assert.Equal(CandidateEvaluationStatus.Rejected, evaluation.Status);
    }

    [Fact]
    public void ATitleForAnotherKingdom_IsRejectedWithoutBeingQueued() {
        var evaluation = TaxonPageMatcher.EvaluateCandidate(Candidate("Gaussia princeps (copepod)", "wikidata"), "PLANTAE", Cache);

        Assert.Equal(CandidateEvaluationStatus.Rejected, evaluation.Status);
        Assert.Null(evaluation.PageRowId);
        Assert.Null(Cache.GetPageByNormalizedTitle("Gaussia princeps (copepod)"));
    }

    [Fact]
    public void TheNameWithAWordForTheKingdom_IsTriedAfterTheIucnNames_AndBeforeSynonyms() {
        var candidates = TaxonPageMatcher.BuildCandidates(
            new WikidataIucnMatchCandidate("Ficus variegata (gastropod)", "CachedName", "Ficus variegata", false),
            [
                new TaxonNameCandidate("Ficus variegata", TaxonNameSource.IucnTaxonomy),
                new TaxonNameCandidate("Ficus synonymus", TaxonNameSource.IucnSynonym), // a made-up synonym
            ],
            n => TaxonPageMatcher.KingdomQualifiedTitles(Cache, n, "PLANTAE"));

        Assert.Equal(
            ["Ficus variegata (gastropod)", "Ficus variegata", "Ficus variegata (plant)", "Ficus variegata (tree)", "Ficus synonymus"],
            candidates.Select(c => c.DisplayTitle));
        Assert.Equal(TaxonPageMatcher.KingdomQualifiedMethod, candidates[2].MatchMethod);
    }

    [Fact]
    public void FicusVariegata_MatchedToTheGastropod_IsCheckedAgain_AndWaitsOnThePlantArticle() {
        _fixture.Match(WikiKingdomFixture.FicusVariegataId, _fixture.FicusGastropodPage, "Ficus variegata (gastropod)", "CachedName");

        var outcome = Process(WikiKingdomFixture.FicusVariegataId, "PLANTAE",
            () => Candidates("Ficus variegata (gastropod)", "Ficus variegata", "PLANTAE"));

        Assert.Equal(TaxonProcessResult.Pending, outcome.Result);
        Assert.NotNull(outcome.WrongKingdom);
        var match = Cache.GetTaxonMatch(TaxonSources.Iucn, WikiKingdomFixture.FicusVariegataId)!;
        Assert.Equal(TaxonWikiMatchStatus.Pending, match.MatchStatus);
        Assert.Equal("Ficus variegata (plant)", match.CandidateTitle);
        Assert.Equal(WikiPageDownloadStatus.Pending, Cache.GetPageByNormalizedTitle("Ficus variegata (plant)")?.DownloadStatus);
    }

    [Fact]
    public void FicusVariegata_IsMatchedToThePlantArticle_OnceItIsDownloaded() {
        _fixture.Match(WikiKingdomFixture.FicusVariegataId, _fixture.FicusGastropodPage, "Ficus variegata (gastropod)", "CachedName");
        Process(WikiKingdomFixture.FicusVariegataId, "PLANTAE", () => Candidates("Ficus variegata (gastropod)", "Ficus variegata", "PLANTAE"));
        _fixture.AddPage("Ficus variegata (plant)", disambiguation: false,
            "{{Speciesbox\n| genus = Ficus\n| species = variegata\n| authority = Blume\n}}");

        var outcome = Process(WikiKingdomFixture.FicusVariegataId, "PLANTAE",
            () => Candidates("Ficus variegata (gastropod)", "Ficus variegata", "PLANTAE"));

        Assert.Equal(TaxonProcessResult.Matched, outcome.Result);
        Assert.Null(outcome.WrongKingdom);
        var match = Cache.GetTaxonMatch(TaxonSources.Iucn, WikiKingdomFixture.FicusVariegataId)!;
        Assert.Equal("Ficus variegata (plant)", match.RedirectFinalTitle);
        Assert.Equal(TaxonPageMatcher.KingdomQualifiedMethod, match.MatchMethod);
    }

    [Fact]
    public void GaussiaPrinceps_MatchedToTheCrustacean_WaitsOnItsOwnNameAndThePlantTitles() {
        _fixture.Match(WikiKingdomFixture.GaussiaPrincepsId, _fixture.GaussiaCrustaceanPage, "Gaussia princeps (crustacean)", "TaxonName");

        var outcome = Process(WikiKingdomFixture.GaussiaPrincepsId, "PLANTAE",
            () => Candidates("Gaussia princeps (crustacean)", "Gaussia princeps", "PLANTAE"));

        Assert.Equal(TaxonProcessResult.Pending, outcome.Result);
        Assert.Equal("Gaussia princeps", Cache.GetTaxonMatch(TaxonSources.Iucn, WikiKingdomFixture.GaussiaPrincepsId)!.CandidateTitle);
        foreach (var title in new[] { "Gaussia princeps", "Gaussia princeps (plant)", "Gaussia princeps (tree)", "Gaussia princeps (palm)" }) {
            Assert.Equal(WikiPageDownloadStatus.Pending, Cache.GetPageByNormalizedTitle(title)?.DownloadStatus);
        }
        Assert.Null(Cache.GetPageByNormalizedTitle("Gaussia princeps (copepod)"));
    }

    [Fact]
    public void BeilschmiediaMadagascariensis_MatchedToTheBird_IsCheckedAgain() {
        _fixture.Match(WikiKingdomFixture.BeilschmiediaId, _fixture.BernieriaPage, "Bernieria madagascariensis", "iucn-synonym");
        var existing = Cache.GetTaxonMatch(TaxonSources.Iucn, WikiKingdomFixture.BeilschmiediaId)!;

        Assert.NotNull(TaxonPageMatcher.WrongKingdomMatch(existing, "PLANTAE", Cache));
    }

    [Fact]
    public void AMatchInTheRightKingdom_IsLeftAsItIs() {
        _fixture.Match("6000", _fixture.FicusGastropodPage, "Ficus variegata (gastropod)", "iucn-taxonomy");
        var built = false;

        var outcome = Process("6000", "ANIMALIA", () => {
            built = true;
            return [];
        });

        Assert.Equal(TaxonProcessResult.AlreadyMatched, outcome.Result);
        Assert.False(built);
    }

    [Fact]
    public void WithPendingOnly_OtherCheckedTaxa_AreStillSkipped() {
        Cache.UpsertTaxonMatch(new TaxonWikiMatch(TaxonSources.Iucn, "6001", TaxonWikiMatchStatus.Missing, null, null, null, null,
            null, null, null, DateTime.UtcNow));

        var outcome = Process("6001", "PLANTAE", () => []);

        Assert.Equal(TaxonProcessResult.NotRechecked, outcome.Result);
    }
}
