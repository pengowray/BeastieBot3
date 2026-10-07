using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.OutputCaching;

namespace BeastieBot3.Site.Pages;

/// The page of a species from the Catalogue of Life or Wikidata that is not on the IUCN Red List
/// (an extra_species row), at /col/{CoL ID} or /wikidata/{QID}. An id that belongs to an IUCN taxon
/// goes to that taxon's page instead, except a CoL ID that an extra species also has: the Catalogue
/// of Life then treats an IUCN subspecies as this species, and this page links the IUCN taxon.
[OutputCache(PolicyName = SiteCachePolicies.Species)]
public sealed class ExtraSpeciesModel : PageModel {
    public const string ColSource = "col";
    public const string WikidataSource = "wikidata";

    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;

    public ExtraSpeciesModel(SiteDatabase db, SiteQueries queries) {
        _db = db;
        _queries = queries;
    }

    public string Source { get; private set; } = ColSource;
    public string RequestedId { get; private set; } = string.Empty;
    public ExtraSpeciesRow? Species { get; private set; }
    /// The groups above the species, kingdom first, ending with its genus or (UnderFamily) its family.
    public IReadOnlyList<GroupRow> Path { get; private set; } = [];
    public bool UnderFamily => Path.Count > 0 && Path[^1].Rank == "family";
    public IReadOnlyList<ExtraPairRow> Pairs { get; private set; } = [];
    /// IUCN taxa with the species' Catalogue of Life ID.
    public IReadOnlyList<TaxonSummary> SameColIdTaxa { get; private set; } = [];

    public IActionResult OnGet(string source, string id) {
        Source = source;
        RequestedId = id.Trim();
        if (source == WikidataSource) {
            if (IdQuery.Parse(RequestedId) is not { WikidataItem: { } item }) {
                return NotFoundPage();
            }
            RequestedId = "Q" + item.ToString(System.Globalization.CultureInfo.InvariantCulture);
            // Search goes to the taxon page, or lists the taxa and assessments that have the item.
            if (_queries.FindByIds(new IdQuery(null, null, null, item)).Count > 0) {
                return Redirect("/search?q=" + RequestedId);
            }
            Species = _queries.GetExtraSpeciesByWikidataItem(item);
        } else {
            Species = _queries.GetExtraSpeciesByColId(RequestedId);
            SameColIdTaxa = _queries.GetTaxaWithColId(RequestedId);
            if (Species is null && SameColIdTaxa.Count > 0) {
                return Redirect($"/species/{SameColIdTaxa[0].TaxonId}");
            }
        }
        if (Species is null) {
            return NotFoundPage();
        }
        Path = _queries.GetGroupPath(Species.NodeId);
        Pairs = _queries.GetExtraOverlaps(Species.ExtraId);
        ViewData["NoIndex"] = true;
        return Page();
    }

    private PageResult NotFoundPage() {
        Response.StatusCode = StatusCodes.Status404NotFound;
        ViewData["NoIndex"] = true;
        return Page();
    }

    public string NotFoundHeading => Source == WikidataSource
        ? Display.SiteText.WikidataNotFoundHeading(RequestedId)
        : Display.SiteText.ColIdNotFoundHeading(RequestedId);
    public string NotFoundLine => Source == WikidataSource ? Display.SiteText.WikidataNotFoundLine : Display.SiteText.ColIdNotFoundLine;
}
