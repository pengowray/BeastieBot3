using BeastieBot3.Site.Data;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Pages;

/// The species list maker (/species-list-maker): the name of a genus, family, order or other group
/// (scientific or English). One group found goes straight to its list page; several are listed. When
/// the text names a species instead, its genus, family and order are offered.
[OutputCache(PolicyName = SiteCachePolicies.Cite)]
public sealed class ListMakerModel : PageModel {
    public const int MaxGroups = 20;

    private readonly SiteQueries _queries;

    public ListMakerModel(SiteQueries queries) {
        _queries = queries;
    }

    public string Query { get; private set; } = string.Empty;
    public bool Searched { get; private set; }
    public bool TooShort { get; private set; }
    public IReadOnlyList<GroupHit> Groups { get; private set; } = [];

    /// When the text names one species: the species, and the groups above it (genus first).
    public TaxonRow? Species { get; private set; }
    public IReadOnlyList<GroupRow> SpeciesGroups { get; private set; } = [];

    public IActionResult OnGet() {
        Query = SiteEndpoints.QueryText(Request);
        if (Query.Length == 0) {
            return Page();
        }
        Searched = true;
        if (FtsQuery.IsTooShort(Query)) {
            TooShort = true;
            return Page();
        }
        Groups = _queries.FindGroupsByName(Query, MaxGroups);
        if (Groups.Count == 1) {
            return Redirect(SiteUrls.GroupList(Groups[0].Group));
        }
        if (Groups.Count == 0) {
            var result = _queries.Search(Query, 10, cancellationToken: HttpContext.RequestAborted);
            if (SearchModel.SingleExactMatch(result.Hits) is { } hit && _queries.GetTaxon(hit.Taxon.TaxonId) is { NodeId: { } nodeId } taxon) {
                Species = taxon;
                SpeciesGroups = _queries.GetGroupPath(nodeId)
                    .Where(g => !g.IsCol && g.Rank is "genus" or "family" or "order")
                    .Reverse()
                    .ToList();
            }
        }
        return Page();
    }
}
