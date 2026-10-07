using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

public sealed class IndexModel : PageModel {
    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;

    public IndexModel(SiteDatabase db, SiteQueries queries) {
        _db = db;
        _queries = queries;
    }

    public SiteSnapshot? Snapshot { get; private set; }

    /// The search suggestions, picked again for every visit.
    public IReadOnlyList<HomeExample> Examples { get; private set; } = [];

    public void OnGet() {
        Snapshot = _db.Snapshot;
        Examples = HomeExamples.Pick(Random.Shared, _queries.GetExampleTaxa());
    }
}
