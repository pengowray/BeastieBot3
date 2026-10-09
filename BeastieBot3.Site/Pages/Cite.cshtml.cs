using BeastieBot3.Site.Data;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Pages;

/// The cite tool (/cite): a taxon's name or an IUCN id, and whether the citation is for a Wikipedia
/// (which one) or for Wikidata. One taxon or assessment found goes straight to its wikitext page or
/// its Wikidata page; several are listed, each linking that page. #wikidata in the address (site.js)
/// or for=wikidata picks Wikidata.
[OutputCache(PolicyName = SiteCachePolicies.Cite)]
public sealed class CiteModel : PageModel {
    public const int MaxResults = 50;
    public const string ForWikidata = "wikidata";

    private readonly SiteQueries _queries;

    public CiteModel(SiteQueries queries) {
        _queries = queries;
    }

    public string Query { get; private set; } = string.Empty;
    public bool ForWikidataChosen { get; private set; }
    public string Wiki { get; private set; } = OtherWikipedias.English.Code;
    public bool TooShort { get; private set; }
    public bool Searched { get; private set; }
    public IReadOnlyList<TaxonListItem> Items { get; private set; } = [];

    public IActionResult OnGet() {
        Query = SiteEndpoints.QueryText(Request);
        ForWikidataChosen = SiteEndpoints.FirstQueryValue(Request, "for") == ForWikidata;
        Wiki = WikitextOptions.ReadWiki(SiteEndpoints.FirstQueryValue(Request, WikitextOptions.WikiKey));
        if (Query.Length == 0) {
            return Page();
        }
        Searched = true;
        if (IdQuery.Parse(Query) is { } ids && _queries.FindByIds(ids) is { Count: > 0 } idHits) {
            if (idHits.Count == 1) {
                return Redirect(ToolUrl(idHits[0].Taxon.TaxonId, idHits[0] is { AssessmentId: { } aid, IsDefault: false } ? aid : null));
            }
            Items = idHits.Select(h => TaxonListItem.FromIdHit(h) with {
                Url = ToolUrl(h.Taxon.TaxonId, h is { AssessmentId: { } aid, IsDefault: false } ? aid : null),
            }).ToList();
            return Page();
        }
        if (FtsQuery.IsTooShort(Query)) {
            TooShort = true;
            return Page();
        }
        var result = _queries.Search(Query, MaxResults, cancellationToken: HttpContext.RequestAborted);
        if (SearchModel.SingleExactMatch(result.Hits) is { } hit) {
            return Redirect(ToolUrl(hit.Taxon.TaxonId, null));
        }
        Items = result.Hits.Select(h => TaxonListItem.FromHit(h) with { Url = ToolUrl(h.Taxon.TaxonId, null) }).ToList();
        return Page();
    }

    /// The tool page of a taxon for the choice made: its Wikidata page, or its wikitext page for the
    /// chosen Wikipedia; with the assessment when it is not the one the page shows first.
    public string ToolUrl(long taxonId, long? assessmentId) {
        var parts = new List<string>();
        if (assessmentId is { } id) {
            parts.Add("assessment=" + id.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        if (!ForWikidataChosen && Wiki != OtherWikipedias.English.Code) {
            parts.Add(WikitextOptions.WikiKey + "=" + Wiki);
        }
        var query = parts.Count == 0 ? "" : "?" + string.Join('&', parts);
        return ForWikidataChosen ? AssessmentToolModel.WikidataPath(taxonId, query) : AssessmentToolModel.WikipediaPath(taxonId, query);
    }
}
