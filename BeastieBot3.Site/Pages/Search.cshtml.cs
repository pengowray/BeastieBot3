using BeastieBot3.Site.Data;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

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

    /// q: the search text. all=1 lists the results even when the text names exactly one taxon (the
    /// "See all search results" link on a taxon page uses it, so it does not redirect straight back).
    public IActionResult OnGet(string? q, string? all) {
        Query = SiteEndpoints.NormalizeQuery(q);
        if (Query.Length == 0) {
            return Page();
        }
        if (Query.Length < 2) {
            TooShort = true;
            return Page();
        }

        var result = _queries.Search(Query, MaxResults);
        var exact = result.Hits.Where(h => h.IsExactMatch).ToList();
        if (exact.Count == 1 && all != "1") {
            var hit = exact[0];
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
}
