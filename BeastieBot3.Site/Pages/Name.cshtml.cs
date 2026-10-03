using BeastieBot3.Site.Data;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

/// /name/{name}: a stable link by name, for example from a Wikipedia template. One taxon with that
/// name (scientific, common or synonym, compared after SiteNameKey.Fold) goes straight to its page,
/// and so does one taxon in the release when taxa that are not in it share the name
/// (SearchModel.SingleExactMatch); several are listed; none goes to the search page for the same text.
public sealed class NameModel : PageModel {
    public const int MaxListed = 50;

    private readonly SiteQueries _queries;

    public NameModel(SiteQueries queries) {
        _queries = queries;
    }

    public string Name { get; private set; } = string.Empty;
    public IReadOnlyList<TaxonListItem> Items { get; private set; } = [];

    public IActionResult OnGet(string? name) {
        Name = SiteEndpoints.NormalizeQuery(name?.Replace('_', ' '));
        if (Name.Length == 0) {
            return Redirect("/search");
        }
        var result = _queries.Search(Name, MaxListed, exactOnly: true, countAll: false, cancellationToken: HttpContext.RequestAborted);
        if (result.Hits.Count == 0) {
            return Redirect("/search?q=" + Uri.EscapeDataString(Name));
        }
        if (SearchModel.SingleExactMatch(result.Hits) is { } hit) {
            return Redirect($"/species/{hit.Taxon.TaxonId}");
        }
        Items = result.Hits.Select(TaxonListItem.FromHit).ToList();
        return Page();
    }
}
