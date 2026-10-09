using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Pages;

[OutputCache(PolicyName = SiteCachePolicies.Search)]
public sealed class SearchModel : PageModel {
    public const int MaxResults = 50;

    private readonly SiteQueries _queries;

    public SearchModel(SiteQueries queries) {
        _queries = queries;
    }

    public string Query { get; private set; } = string.Empty;
    public bool TooShort { get; private set; }
    public IReadOnlyList<TaxonListItem> Items { get; private set; } = [];
    public long TotalTaxa { get; private set; }

    /// Groups (genera, families and other ranks) whose name is the search text, or the title of
    /// their English Wikipedia article or of a redirect to it.
    public IReadOnlyList<GroupHit> Groups { get; private set; } = [];
    public const int MaxGroups = 10;

    /// Set when the text was a taxon id and an assessment id together (a DOI, "T22823A14871490")
    /// and the site has the taxon but not the assessment.
    public long? MissingAssessmentId { get; private set; }

    /// Set when the text was a Wikidata item ("Q33609") that no taxon or assessment on the site has,
    /// or a Wikidata property ("P31").
    public string? MissingWikidataItem { get; private set; }

    /// The English label of that item or property, when the site uses it ("instance of" for P31).
    public string? WikidataLabel { get; private set; }
    public bool IsWikidataProperty { get; private set; }

    /// The language of the Wikipedia the search text links to, when it is not English.
    public string? NotEnglishWikipedia { get; private set; }

    /// The status update page with a Wikipedia page loaded.
    public static string UpdateUrl(WikipediaPageInput page) =>
        $"{Web.ToolPaths.UpdateStatuses}?{UpdateModel.PageField}={Uri.EscapeDataString(page.Title)}"
        + (page.RevisionId is { } r ? $"&{UpdateModel.RevisionField}={r}" : "") + "#result";

    /// When the search found nothing: search texts with misspelled words corrected that find taxa
    /// or groups, most likely first.
    public IReadOnlyList<string> Suggestions { get; private set; } = [];
    public const int MaxSuggestions = 3;
    // Candidate texts looked up before giving up: each is one exact name lookup, and at most one search.
    private const int MaxCandidates = 12;

    /// Species from the Catalogue of Life and Wikidata that are not on the IUCN Red List, listed after
    /// the IUCN taxa, and how many match in all.
    public IReadOnlyList<ExtraSearchHit> ExtraItems { get; private set; } = [];
    public long ExtraTotal { get; private set; }

    /// q: the search text. all=1 lists the results even when the text names exactly one taxon (the
    /// "See all search results" link on a taxon page uses it, so it does not redirect straight back).
    public IActionResult OnGet() {
        // Read the way the output cache key reads them (the first value of each).
        Query = SiteEndpoints.QueryText(Request);
        var all = SiteEndpoints.FirstQueryValue(Request, "all");
        if (Query.Length == 0) {
            return Page();
        }
        var ids = IdQuery.Parse(Query);
        // A Wikipedia URL or wikilink opens the status update page with that page loaded. Before the
        // id check: a URL with oldid= has digits that would read as an IUCN id.
        if (ids?.WikidataItem is null && WikipediaPageInput.Parse(Query) is { } wikiPage) {
            if (!wikiPage.English) {
                NotEnglishWikipedia = wikiPage.Language;
                return Page();
            }
            return Redirect(UpdateUrl(wikiPage));
        }
        // Before the length check: a taxon id can be a single digit.
        if (ids is not null && FindById(ids, all == "1") is { } idResult) {
            return idResult;
        }
        if (ids?.WikidataItem is { } item) {
            if (_queries.GetExtraSpeciesByWikidataItem(item) is { } extraSpecies) {
                return Redirect(Web.SiteUrls.Extra(extraSpecies));
            }
            MissingWikidataItem = "Q" + item.ToString(System.Globalization.CultureInfo.InvariantCulture);
            WikidataLabel = Display.WikidataTerms.Label(MissingWikidataItem);
            return Page();
        }
        if (ids?.WikidataProperty is { } property) {
            MissingWikidataItem = "P" + property.ToString(System.Globalization.CultureInfo.InvariantCulture);
            WikidataLabel = Display.WikidataTerms.Label(MissingWikidataItem);
            IsWikidataProperty = true;
            return Page();
        }
        if (FtsQuery.IsTooShort(Query)) {
            TooShort = true;
            return Page();
        }

        var result = _queries.Search(Query, MaxResults, cancellationToken: HttpContext.RequestAborted);
        Groups = _queries.FindGroupsByName(Query, MaxGroups);
        if (all != "1" && GroupToGoTo(Groups, result.Hits) is { } group) {
            // When taxa matched too, the group page links back to all the results.
            return Redirect(Web.SiteUrls.Group(group.Group, result.Hits.Count > 0 ? "q=" + Uri.EscapeDataString(Query) : ""));
        }
        if (all != "1" && Groups.Count == 0 && SingleExactMatch(result.Hits) is { } hit) {
            var url = $"/species/{hit.Taxon.TaxonId}";
            if (hit.MatchedNameType != NameTypes.Scientific) {
                // The taxon page says which name was matched, and links back to all the results.
                url += "?q=" + Uri.EscapeDataString(Query);
            }
            return Redirect(url);
        }

        var extra = _queries.SearchExtra(Query, MaxResults, HttpContext.RequestAborted);
        // A species name that only the Catalogue of Life or Wikidata has.
        if (all != "1" && Groups.Count == 0 && !result.Hits.Any(h => h.IsExactMatch)
            && extra.Hits.Where(h => h.IsExactMatch).ToList() is [var onlyExact]) {
            return Redirect(Web.SiteUrls.Extra(onlyExact.Species));
        }

        Items = result.Hits.Select(TaxonListItem.FromHit).ToList();
        TotalTaxa = result.TotalTaxa;
        ExtraItems = extra.Hits;
        ExtraTotal = extra.Total;
        if (Items.Count == 0 && Groups.Count == 0 && ExtraItems.Count == 0) {
            Suggestions = FindSuggestions(Query);
        }
        return Page();
    }

    private IReadOnlyList<string> FindSuggestions(string text) {
        if (_queries.GetNameWordIndex() is not { } index) {
            return [];
        }
        // A text that is a name (of a taxon or a group) first; a text that only finds names containing
        // it ("Panthera le") only when no candidate is a name.
        var candidates = SpellingSuggestions.Candidates(text, index, MaxCandidates);
        var ct = HttpContext.RequestAborted;
        var found = candidates.Where(c => _queries.Search(c, 1, exactOnly: true, countAll: false, cancellationToken: ct).Hits.Count > 0
                || _queries.FindGroupsByName(c, 1).Count > 0)
            .Take(MaxSuggestions).ToList();
        if (found.Count == 0) {
            found = [.. candidates.Take(3).Where(c => _queries.Search(c, 1, countAll: false, cancellationToken: ct).Hits.Count > 0).Take(1)];
        }
        return [.. found.Select(c => char.ToUpperInvariant(c[0]) + c[1..])];
    }

    // The taxa the ids name: a redirect when there is exactly one, else the list. Null when the ids
    // name nothing, so the text is searched as a name.
    private IActionResult? FindById(IdQuery ids, bool all) {
        var hits = _queries.FindByIds(ids);
        if (hits.Count == 0) {
            return null;
        }
        if (ids is { TaxonId: not null, AssessmentId: { } wanted } && !hits.Any(h => h.AssessmentId == wanted)) {
            MissingAssessmentId = wanted;
        }
        if (!all && hits.Count == 1 && MissingAssessmentId is null) {
            return Redirect(SpeciesUrl(hits[0]));
        }
        // A Wikidata item of a taxon in the release is often also the item of an old IUCN id of it.
        if (!all && ids.WikidataItem is not null && hits.Count(h => h.Taxon.InRelease) == 1) {
            return Redirect(SpeciesUrl(hits.Single(h => h.Taxon.InRelease)));
        }
        Items = hits.Select(TaxonListItem.FromIdHit).ToList();
        TotalTaxa = Items.Count;
        return Page();
    }

    /// The page of an id hit: the taxon page, or the taxon's wikitext page with the assessment shown
    /// when the id was an assessment's other than the one the taxon page shows first.
    public static string SpeciesUrl(IdHit hit) => hit is { AssessmentId: { } aid, IsDefault: false }
        ? SpeciesWikitextModel.PathFor(hit.Taxon.TaxonId, $"?assessment={aid}") + "#wikitext"
        : $"/species/{hit.Taxon.TaxonId}";

    /// <summary>
    /// The group a search goes straight to: the only group found, when no taxon matches the text
    /// strongly (<see cref="SearchHit.IsStrongExactMatch"/>). A group found by its name or by the
    /// title of its English Wikipedia article or of a redirect to it ("fruit bat": family
    /// Pteropodidae) comes before a taxon that has the text only as a common name it is not shown
    /// with (a Catalogue of Life vernacular). When a group is found and a taxon matches strongly,
    /// or several groups are found, the results are listed, groups first.
    /// </summary>
    public static GroupHit? GroupToGoTo(IReadOnlyList<GroupHit> groups, IReadOnlyList<SearchHit> hits) =>
        groups.Count == 1 && !hits.Any(h => h.IsStrongExactMatch) ? groups[0] : null;

    /// The one taxon the text names exactly: the only exact match in the release, or, when no taxon
    /// in the release matches exactly, the only exact match. Null when there is none or several. A
    /// taxon not in the release with the same name as one in the release is linked from that
    /// taxon's page. Taxa that have the text only as a common name in a language other than English
    /// are counted only when no other taxon matches exactly, so an English name, a scientific name or
    /// a synonym of one taxon still goes to it when the same words are another taxon's name in
    /// another language.
    public static SearchHit? SingleExactMatch(IReadOnlyList<SearchHit> hits) {
        var exact = hits.Where(h => h.IsExactMatch).ToList();
        var notOtherLanguage = exact.Where(h => !h.IsExactInOtherLanguageOnly).ToList();
        return Single(notOtherLanguage.Count > 0 ? notOtherLanguage : exact);

        static SearchHit? Single(List<SearchHit> exact) {
            var inRelease = exact.Where(h => h.Taxon.InRelease).ToList();
            return inRelease.Count switch {
                1 => inRelease[0],
                0 when exact.Count == 1 => exact[0],
                _ => null,
            };
        }
    }
}
