using System.Globalization;
using System.Text;
using CsvHelper;
using Spectre.Console;

// The files of `wikipedia report-species-lists`:
//   <name>.md            totals, the pages with statuses to update, then the pages with taxa that have
//                        no status or are missing from the group (the largest MaxTaxaPages);
//   <name>.wikitext      the same, for a user page on English Wikipedia (SpeciesListReportDocument);
//   <name>-pages.csv     every page checked, with every count;
//   <name>-statuses.csv  every status that can be updated: the page, line, taxon, code on the page and IUCN's code;
//   <name>-missing.csv   every taxon of a page's group that the page does not list.

namespace BeastieBot3.Wikipedia;

internal sealed class SpeciesListReport {
    /// The most pages in the Markdown table of taxa with no status or missing; every page is in the pages CSV.
    private const int MaxTaxaPages = 1_000;

    private readonly IReadOnlyList<SpeciesListReportRow> _rows;
    private readonly SpeciesListPlan _plan;
    private readonly string? _release;
    private readonly string? _siteBuilt;
    private readonly IReadOnlyList<(string Title, string Error)> _failures;

    public SpeciesListReport(IReadOnlyList<SpeciesListReportRow> rows, SpeciesListPlan plan, string? release, string? siteBuilt,
        IReadOnlyList<(string Title, string Error)> failures) {
        _rows = rows;
        _plan = plan;
        _release = release;
        _siteBuilt = siteBuilt;
        _failures = failures;
    }

    private IEnumerable<SpeciesListPageResult> Results => _rows.Select(r => r.Result);

    public IReadOnlyList<string> Write(string directory, string name) {
        Directory.CreateDirectory(directory);
        var md = Path.Combine(directory, name + ".md");
        var pages = Path.Combine(directory, name + "-pages.csv");
        var statuses = Path.Combine(directory, name + "-statuses.csv");
        var missing = Path.Combine(directory, name + "-missing.csv");
        var wiki = Path.Combine(directory, name + ".wikitext");
        var (pagesName, statusesName, missingName) = (Path.GetFileName(pages), Path.GetFileName(statuses), Path.GetFileName(missing));
        var csvNote = $"Full data is in three CSV files: {pagesName} lists every page with every count; {statusesName} lists every status that can be updated or has the id of another taxon (page, line number, taxon, status on the page, IUCN category); {missingName} lists every taxon missing from a page's group.";
        File.WriteAllText(md, BuildDocument(new MarkdownReportDocument(), pagesName, csvNote), Encoding.UTF8);
        File.WriteAllText(wiki, BuildDocument(new WikitextReportDocument(), "the CSV file of every page, which is not on Wikipedia", csvNote: null), new UTF8Encoding(false));
        WritePagesCsv(pages);
        WriteStatusesCsv(statuses);
        WriteMissingCsv(missing);
        return [md, wiki, pages, statuses, missing];
    }

    public void PrintSummary() {
        var table = new Table().AddColumns("", "Number", "Pages with at least one");
        foreach (var total in Totals()) {
            table.AddRow(Markup.Escape(total.Label), total.Count.ToString("N0"), total.Pages.ToString("N0"));
        }
        AnsiConsole.Write(table);
    }

    private sealed record Total(string Label, string? Example, long Count, int Pages);

    private IEnumerable<Total> Totals() {
        var results = Results.ToList();
        Total Sum(string label, string? example, Func<SpeciesListPageResult, int> count) =>
            new(label, example, results.Sum(r => (long)count(r)), results.Count(r => count(r) > 0));
        yield return Sum("Statuses with a different category from the latest IUCN assessment", "for example, the page says EN and IUCN says CR", r => r.CategoryChanged);
        yield return Sum("Statuses written as NA or RE, categories used only in regional assessments", "the page may give regional statuses, such as the European Red List's", r => r.RegionalCodes);
        yield return Sum("CR statuses that need (PE) or (PEW) added or removed", "for example, CR to CR(PE) for a species now marked possibly extinct", r => r.PossiblyExtinctChanged);
        yield return Sum("{{Species table/row}} rows with the latest category whose population trend is empty or different", "only the direction changes", r => r.TrendChanged);
        yield return Sum("{{IUCN status}} templates with the latest category that cite an older assessment", "only the assessment ids or year change", r => r.NewerAssessment);
        yield return Sum("Statuses that match the latest assessment", null, r => r.UpToDate);
        yield return Sum("{{IUCN status}} templates whose taxon id is not the id of the taxon the row or line names", "or whose id is also on rows that name other taxa; not changed by the update page: check the id", r => r.IdOfAnotherTaxon);
        yield return Sum("Statuses not matched to an IUCN taxon", "the name is not an IUCN name, or matches more than one taxon", r => r.NotMatched);
        yield return Sum("Taxa listed with no IUCN status", null, r => r.TaxaWithoutStatus);
        yield return Sum("Taxa in the page's group that the page does not list", "counted only for pages that list at least half of their group", r => r.MissingFromGroup ?? 0);
        yield return Sum("Listed taxa in a category the list is not about", "for example, a bird now assessed as EN on a list of critically endangered birds", r => r.InOtherCategory);
        yield return Sum("Listed taxa outside the group that most of the page's taxa are in", "for example, a lizard on a list of snakes", r => r.OutsideGroup);
        yield return Sum("Taxa listed twice under two names", "for example, an old synonym and the current name", r => r.ListedTwice);
        yield return Sum("{{cite iucn}} citations of an older assessment than the latest", null, r => r.OlderCitations);
        yield return Sum("{{Species table/row}} populations that differ from IUCN's number of mature individuals", null, r => r.PopulationDiffers);
    }

    // The report as Markdown, or as wikitext for a user page on English Wikipedia. pagesCsv: how the
    // pages CSV is named in the text; csvNote: the line naming the CSV files (Markdown only).
    private string BuildDocument(ReportDocument doc, string pagesCsv, string? csvNote) {
        var results = Results.ToList();
        doc.Heading(1, $"Wikipedia species lists checked against IUCN Red List release {_release ?? "(unknown release)"}");
        var facts = new List<string> { $"IUCN Red List release: {_release ?? "unknown"}. From the species site's database, built {SiteBuilt()}." };
        var downloaded = results.Where(r => r.DownloadedAt is not null).Select(r => r.DownloadedAt!.Value).ToList();
        if (downloaded.Count > 0) {
            facts.Add($"Wikipedia pages downloaded between {downloaded.Min():yyyy-MM-dd} and {downloaded.Max():yyyy-MM-dd}.");
        }
        facts.Add($"Pages checked: {results.Count:N0}.");
        facts.Add($"\"List of\" pages and pages that use {{{{IUCN status}}}} or {{{{Species table/row}}}}: {_plan.ListsFound:N0} found, {_plan.ListsNotDownloaded:N0} not downloaded yet.");
        facts.Add($"Downloaded group articles with a list or table of species: {_plan.GroupArticles:N0} of {_plan.GroupArticlesRead:N0}. A group article is an article about a genus, family or other group above species.");
        facts.Add($"Pages with no IUCN taxon recognised: {results.Count(r => r.TaxaListed == 0 && r.Statuses == 0):N0} (for example, lists of dog breeds, or lists whose names match no IUCN taxon).");
        if (_failures.Count > 0) {
            facts.Add($"Pages that could not be checked because of an error: {_failures.Count:N0}. They are listed at the end of this report with the error.");
        }
        doc.Bullets(facts);
        doc.Table(["", "Number", "Pages with at least one"], [false, true, true],
            Totals().Select(t => (IReadOnlyList<string>)[doc.Text(t.Example is null ? t.Label : $"{t.Label} ({t.Example})"), $"{t.Count:N0}", $"{t.Pages:N0}"]));
        doc.Paragraph("A taxon on several pages is counted once for each page.");
        if (csvNote is not null) {
            doc.Paragraph(csvNote);
        }

        var statusPages = _rows.Where(r => r.Result.Outdated > 0 || r.Result.IdOfAnotherTaxon > 0 || r.Result.RegionalCodes > 0)
            .OrderByDescending(r => r.Result.CategoryChanged)
            .ThenByDescending(r => r.Result.Outdated)
            .ThenBy(r => r.Result.Title, StringComparer.Ordinal)
            .ToList();
        doc.Heading(2, $"Pages with statuses to update or check ({statusPages.Count:N0})");
        doc.Paragraph("Sorted by the number of statuses with a different category, then by the number of other statuses to update, most first. To get a page's updated wikitext, paste the page's wikitext into the update page on Beastie Bot Species Status.");
        doc.Table(["Page", "Different category", "CR(PE) or CR(PEW)", "Same category, new trend", "Same category, older assessment", "Up to date", "Id of another taxon", "Not matched", "Compared with IUCN group"],
            [false, true, true, true, true, true, true, true, false],
            statusPages.Select(row => {
                var r = row.Result;
                return (IReadOnlyList<string>)[PageCell(doc, row), Num(r.CategoryChanged), Num(r.PossiblyExtinctChanged), Num(r.TrendChanged), Num(r.NewerAssessment),
                    Num(r.UpToDate), Num(r.IdOfAnotherTaxon), Num(r.NotMatched), Group(r)];
            }));
        doc.Bullets([
            "Different category: statuses whose code differs from the category of the latest IUCN assessment.",
            "CR(PE) or CR(PEW): CR statuses that need (PE) or (PEW) added or removed to match the latest assessment.",
            "Same category, new trend: {{Species table/row}} rows with the latest category whose population trend (direction) is empty or differs from the latest assessment's.",
            "Same category, older assessment: {{IUCN status}} templates with the latest category that cite an older assessment. Only the assessment ids or year change.",
            "Id of another taxon: {{IUCN status}} templates whose taxon id is not the id of the taxon the row or line names, or whose id is also on rows that name other taxa. The update page does not change them: check the id.",
            "Not matched: statuses whose name is not an IUCN name, or matches more than one IUCN taxon.",
            "\"has NA or RE\": the page writes some statuses as NA or RE, categories used only in regional assessments. Its other statuses may be regional too, so check them before updating the page.",
            "Empty cells are 0.",
        ]);

        var taxaPages = _rows.Where(r => r.Result.TaxaWithoutStatus > 0 || (r.Result.MissingFromGroup ?? 0) > 0 || r.Result.ListedTwice > 0 || r.Result.OutsideGroup > 0)
            .OrderByDescending(r => r.Result.TaxaWithoutStatus + (r.Result.MissingFromGroup ?? 0))
            .ThenBy(r => r.Result.Title, StringComparer.Ordinal)
            .ToList();
        var shown = taxaPages.Take(MaxTaxaPages).ToList();
        doc.Heading(2, $"Pages with taxa that have no status or are missing ({taxaPages.Count:N0})");
        doc.Paragraph(shown.Count < taxaPages.Count
            ? $"The {shown.Count:N0} pages with the most taxa with no status or missing from the group, most first. The other {taxaPages.Count - shown.Count:N0} pages are in {pagesCsv}."
            : "Sorted by the number of taxa with no status or missing from the group, most first.");
        doc.Table(["Page", "Listed, no status", "Missing from group", "Listed twice", "Outside the group", "Compared with IUCN group"],
            [false, true, true, true, true, false],
            shown.Select(row => {
                var r = row.Result;
                var missing = r.MissingFromGroup is { } m ? Num(m) : r.GroupName is null ? "" : "lists part of group";
                return (IReadOnlyList<string>)[PageCell(doc, row), Num(r.TaxaWithoutStatus), missing, Num(r.ListedTwice), Num(r.OutsideGroup), Group(r)];
            }));
        doc.Bullets([
            "Listed, no status: taxa on the page, found by their scientific name, with no IUCN status next to them.",
            "Missing from group: taxa in the IUCN group that the page does not list. \"lists part of group\" means the page lists less than half of the group, so missing taxa are not counted.",
            "Listed twice: taxa listed under two names, such as an old synonym and the current name.",
            "Outside the group: listed taxa outside the group that most of the page's taxa are in.",
            "Empty cells are 0.",
        ]);
        if (_failures.Count > 0) {
            doc.Heading(2, "Pages that could not be checked");
            doc.Bullets(_failures.Select(f => $"{doc.Link(f.Title)}: {f.Error}").ToList());
        }
        return doc.ToString();
    }

    private string SiteBuilt() =>
        DateTime.TryParse(_siteBuilt, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var built)
            ? built.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : "at an unknown time";

    private static string ChangeName(SpeciesListChangeKind change) => change switch {
        SpeciesListChangeKind.Category => "different category",
        SpeciesListChangeKind.RegionalCode => "NA or RE on the page",
        SpeciesListChangeKind.PossiblyExtinct => "CR(PE) or CR(PEW)",
        SpeciesListChangeKind.Trend => "same category, new trend",
        SpeciesListChangeKind.Assessment => "same category, older assessment",
        _ => "id of another taxon",
    };

    private static string PageCell(ReportDocument doc, SpeciesListReportRow row) =>
        doc.Link(row.Result.Title) + (row.Sources.Contains(SpeciesListPlanPage.GroupArticle) ? " (group article)" : "")
        + (row.Result.RegionalCodes > 0 ? " (has NA or RE: may give regional statuses)" : "");

    private static string Num(int n) => n == 0 ? "" : n.ToString("N0", CultureInfo.InvariantCulture);

    private static string Group(SpeciesListPageResult r) {
        if (r.GroupName is null) {
            return "";
        }
        var group = $"{r.GroupRank} {r.GroupName}";
        return r.GroupCategories is { } c ? $"{group} ({c})" : group;
    }

    internal static string Url(string title) =>
        "https://en.wikipedia.org/wiki/" + Uri.EscapeDataString(title.Replace(' ', '_')).Replace("%2F", "/").Replace("%3A", ":");

    private void WritePagesCsv(string path) {
        using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (var header in new[] {
                     "page", "url", "found_by", "revision_id", "downloaded_at", "wikitext_chars", "taxa_listed", "statuses",
                     "different_category", "na_or_re_code", "possibly_extinct_tag", "same_category_new_trend", "same_category_older_assessment", "up_to_date", "id_of_another_taxon", "not_matched", "name_not_found", "name_matches_several",
                     "taxa_without_status", "list_lines_without_status", "tables_without_status_column", "older_citations", "population_differs",
                     "items_not_checked", "group_rank", "group_name", "group_categories", "lists_part_of_group",
                     "missing_from_group", "in_another_category", "outside_group", "listed_twice",
                 }) {
            csv.WriteField(header);
        }
        csv.NextRecord();
        foreach (var row in _rows.OrderBy(r => r.Result.Title, StringComparer.Ordinal)) {
            var r = row.Result;
            object?[] fields = [
                r.Title, Url(r.Title), string.Join("; ", row.Sources), r.RevisionId, r.DownloadedAt?.ToString("yyyy-MM-dd"), r.Bytes,
                r.TaxaListed, r.Statuses, r.CategoryChanged, r.RegionalCodes, r.PossiblyExtinctChanged, r.TrendChanged, r.NewerAssessment, r.UpToDate, r.IdOfAnotherTaxon, r.NotMatched, r.NameNotFound, r.NameAmbiguous,
                r.TaxaWithoutStatus, r.ListLinesWithoutStatus, r.TablesWithoutStatus, r.OlderCitations, r.PopulationDiffers, r.NotChecked,
                r.GroupRank, r.GroupName, r.GroupCategories, r.GroupName is null ? null : r.GroupPartial ? "yes" : "no",
                r.MissingFromGroup, r.InOtherCategory, r.OutsideGroup, r.ListedTwice,
            ];
            foreach (var field in fields) {
                csv.WriteField(field);
            }
            csv.NextRecord();
        }
    }

    private void WriteStatusesCsv(string path) {
        using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (var header in new[] { "page", "line", "kind", "scientific_name", "iucn_taxon_id", "code_on_page", "iucn_code", "iucn_year", "change", "text_on_page", "text_after_update" }) {
            csv.WriteField(header);
        }
        csv.NextRecord();
        foreach (var r in Results.OrderBy(r => r.Title, StringComparer.Ordinal)) {
            foreach (var c in r.Changes) {
                object?[] fields = [r.Title, c.Line, c.Kind, c.ScientificName, c.TaxonId, c.WrittenCode, c.IucnCode, c.IucnYear, ChangeName(c.Change), c.Before, c.After];
                foreach (var field in fields) {
                    csv.WriteField(field);
                }
                csv.NextRecord();
            }
        }
    }

    private void WriteMissingCsv(string path) {
        using var writer = new StreamWriter(path, false, new UTF8Encoding(false));
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (var header in new[] { "page", "group_rank", "group_name", "scientific_name", "iucn_taxon_id", "kind", "iucn_code" }) {
            csv.WriteField(header);
        }
        csv.NextRecord();
        foreach (var r in Results.OrderBy(r => r.Title, StringComparer.Ordinal)) {
            foreach (var m in r.Missing) {
                object?[] fields = [r.Title, r.GroupRank, r.GroupName, m.ScientificName, m.TaxonId, m.Kind, m.IucnCode];
                foreach (var field in fields) {
                    csv.WriteField(field);
                }
                csv.NextRecord();
            }
        }
    }
}
