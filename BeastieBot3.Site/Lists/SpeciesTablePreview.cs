using System.Net;
using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Update;

// A rough HTML preview of the species tables, made from the built tables rather than by reading
// the wikitext back: headings, one table per genus with Template:Species table's caption and column
// names, and for each row the name, the scientific name with its authority, the category code
// linked as {{IUCN status}} links it with a link to the assessment, the population and the trend.
// The blank range, size and ecology columns are left out.

namespace BeastieBot3.Site.Lists;

public static class SpeciesTablePreview {

    public static string ToHtml(SpeciesTableResult result) {
        var html = new StringBuilder();
        foreach (var item in result.Items) {
            switch (item) {
                case TableHeadingItem { Heading: var heading }:
                    var tag = "h" + Math.Min(6, heading.Level + 1);
                    html.Append('<').Append(tag).Append(" class=\"preview-heading\">").Append(Encode(heading.Text))
                        .Append("</").Append(tag).Append('>');
                    break;
                case TableGroupNameItem { Group: var group }:
                    html.Append("<p>").Append(WikitextPreview.Inline(GroupList.GroupNameSentence(group))).Append("</p>");
                    break;
                case GenusTable table:
                    AppendTable(html, table);
                    break;
            }
        }
        return html.ToString();
    }

    private static void AppendTable(StringBuilder html, GenusTable table) {
        // Template:Species table's caption and column names, as Wikipedia shows them.
        html.Append("<div class=\"table-scroll\"><table class=\"preview-table\"><caption>Genus <i>").Append(Encode(table.GenusName))
            .Append("</i> – ").Append(Encode(SpeciesTable.CountWords(table.SpeciesCount))).Append(" species</caption>")
            .Append("<thead><tr><th scope=\"col\">Common name</th><th scope=\"col\">Scientific name</th>")
            .Append("<th scope=\"col\">IUCN status and estimated population</th></tr></thead><tbody>");
        foreach (var row in table.Rows) {
            html.Append("<tr><th scope=\"row\">").Append(Cell(row.Name)).Append("</th><td><i>").Append(Cell(row.Binomial)).Append("</i><br><small>");
            var authority = row.AuthorityYear.Length > 0 ? $"{row.AuthorityName}, {row.AuthorityYear}" : row.AuthorityName;
            html.Append(Encode(row.AuthorityNotOriginal ? $"({authority})" : authority)).Append("</small></td><td>");
            var taxon = row.Taxon;
            var status = taxon.Category is null || taxon.AssessmentId is null
                ? "{{IUCN status|NE}}"
                : IucnStatusTemplate.Render(taxon.Category, taxon.PossiblyExtinct, taxon.PossiblyExtinctInTheWild,
                    taxon.TaxonId, taxon.AssessmentId.Value, taxon.YearPublished?.ToString(System.Globalization.CultureInfo.InvariantCulture));
            html.Append(WikitextPreview.Inline(status)).Append("<br>").Append(Encode(row.Population)).Append("<br>")
                .Append(Encode(TrendText(row.Direction))).Append("</td></tr>");
        }
        html.Append("</tbody></table></div>");
    }

    // A name cell, with the extinct dagger as a dagger.
    private static string Cell(string wikitext) => wikitext.EndsWith(SpeciesTable.Dagger, StringComparison.Ordinal)
        ? WikitextPreview.Inline(wikitext[..^SpeciesTable.Dagger.Length]) + "<span title=\"Extinct\">†</span>"
        : WikitextPreview.Inline(wikitext);

    // The trend templates' own words (their label or alt text).
    private static string TrendText(string direction) => direction switch {
        PopulationTrendTemplate.Decreasing => "Population declining",
        PopulationTrendTemplate.Stable => "Population steady",
        PopulationTrendTemplate.Increasing => "Population increasing",
        _ => "Population change unknown",
    };

    private static string Encode(string text) => WebUtility.HtmlEncode(text);
}
