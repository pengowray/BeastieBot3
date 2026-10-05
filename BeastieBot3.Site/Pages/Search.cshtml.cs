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

    /// Groups (genera, families and other ranks) whose name is the search text.
    public IReadOnlyList<GroupRow> Groups { get; private set; } = [];
    public const int MaxGroups = 10;

    /// Set when the text was a taxon id and an assessment id together (a DOI, "T22823A14871490")
    /// and the site has the taxon but not the assessment.
    public long? MissingAssessmentId { get; private set; }

    /// True when the results are taxa found by an IUCN id in the text, not by a name.
    public bool FoundById { get; private set; }

    /// q: the search text. all=1 lists the results even when the text names exactly one taxon (the
    /// "See all search results" link on a taxon page uses it, so it does not redirect straight back).
    public IActionResult OnGet() {
        // Read the way the output cache key reads them (the first value of each).
        Query = SiteEndpoints.QueryText(Request);
        var all = SiteEndpoints.FirstQueryValue(Request, "all");
        if (Query.Length == 0) {
            return Page();
        }
        // Before the length check: a taxon id can be a single digit.
        if (IdQuery.Parse(Query) is { } ids && FindById(ids, all == "1") is { } idResult) {
            return idResult;
        }
        if (FtsQuery.IsTooShort(Query)) {
            TooShort = true;
            return Page();
        }

        var result = _queries.Search(Query, MaxResults, cancellationToken: HttpContext.RequestAborted);
        Groups = _queries.FindGroupsByName(Query, MaxGroups);
        if (all != "1" && Groups.Count == 1 && !result.Hits.Any(h => h.IsExactMatch)) {
            return Redirect(Web.SiteUrls.Group(Groups[0]));
        }
        if (all != "1" && SingleExactMatch(result.Hits) is { } hit) {
            var url = $"/species/{hit.Taxon.TaxonId}";
            if (hit.MatchedNameType != NameTypes.Scientific) {
                // The taxon page says which name was matched, and links back to all the results.
                url += "?q=" + Uri.EscapeDataString(Query);
            }
            return Redirect(url);
        }

        Items = result.Hits.Select(TaxonListItem.FromHit).ToList();
        TotalTaxa = result.TotalTaxa;
        return Page();
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
        FoundById = true;
        Items = hits.Select(TaxonListItem.FromIdHit).ToList();
        TotalTaxa = Items.Count;
        return Page();
    }

    /// The taxon page of an id hit: with the assessment shown, when the id was an assessment's that
    /// the page does not show first.
    public static string SpeciesUrl(IdHit hit) => hit is { AssessmentId: { } aid, IsDefault: false }
        ? $"/species/{hit.Taxon.TaxonId}?assessment={aid}#wikitext"
        : $"/species/{hit.Taxon.TaxonId}";

    /// The one taxon the text names exactly: the only exact match in the release, or, when no taxon
    /// in the release matches exactly, the only exact match. Null when there is none or several. A
    /// taxon not in the release with the same name as one in the release is linked from that
    /// taxon's page.
    public static SearchHit? SingleExactMatch(IReadOnlyList<SearchHit> hits) {
        var exact = hits.Where(h => h.IsExactMatch).ToList();
        var inRelease = exact.Where(h => h.Taxon.InRelease).ToList();
        return inRelease.Count switch {
            1 => inRelease[0],
            0 when exact.Count == 1 => exact[0],
            _ => null,
        };
    }
}
