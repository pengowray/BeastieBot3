using BeastieBot3.Site.Data;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

public sealed class IndexModel : PageModel {
    private readonly SiteDatabase _db;

    public IndexModel(SiteDatabase db) {
        _db = db;
    }

    public SiteSnapshot? Snapshot { get; private set; }

    public void OnGet() {
        Snapshot = _db.Snapshot;
    }
}
