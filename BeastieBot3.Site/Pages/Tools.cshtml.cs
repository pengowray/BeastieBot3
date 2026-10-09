using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace BeastieBot3.Site.Pages;

/// The tools page (/tools): the site's tools for Wikipedia and Wikidata editors, with an example
/// taxon and genus for the tools that belong to one taxon or group.
[ResponseCache(Duration = 600, Location = ResponseCacheLocation.Any)]
public sealed class ToolsModel : PageModel {
    private readonly SiteQueries _queries;

    public ToolsModel(SiteQueries queries) {
        _queries = queries;
    }

    /// The first species of HomeExamples.Megafauna that the site has; null when the database has none of them.
    public ExampleTaxon? ExampleTaxon { get; private set; }

    /// The genus of ExampleTaxon; null when the site has no single group for it.
    public GroupRow? ExampleGenus { get; private set; }

    public void OnGet() {
        var taxa = _queries.GetExampleTaxa();
        ExampleTaxon = HomeExamples.Megafauna.Select(n => taxa.GetValueOrDefault(n)).FirstOrDefault(t => t is not null);
        if (ExampleTaxon is { } example && _queries.FindGroups("genus", example.ScientificName.Split(' ')[0]) is [var genus]) {
            ExampleGenus = genus;
        }
    }
}
