using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

/// The tools page (/tools): the row of tool tabs and one line for each tool.
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class ToolsModel : PageModel {
    public void OnGet() {
    }
}
