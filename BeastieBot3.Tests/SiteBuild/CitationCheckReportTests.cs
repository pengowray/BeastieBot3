using System.Text.Json;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// The `site check-citations` report: counts land in the right rows and examples are listed.
public class CitationCheckReportTests {
    private static IucnCitationParse Parse(string json) {
        using var document = JsonDocument.Parse(json);
        return IucnCitationPartsParser.Parse(document.RootElement, null);
    }

    [Fact]
    public void Build_SummarisesParsesFailuresAndWikiComparisons() {
        var tally = new CitationCheckTally(examplesPerClass: 5);
        var dugong = Parse("""
            {"assessment_id":160756767,"sis_taxon_id":6909,"year_published":"2019",
             "citation":"Marsh, H. & Sobtzick, S. 2019. Dugong dugon (amended version of 2015 assessment). The IUCN Red List of Threatened Species 2019: e.T6909A160756767. https://dx.doi.org/10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en. Accessed on 20 August 2026.",
             "taxon":{"sis_id":6909,"scientific_name":"Dugong dugon"},
             "credits":[{"credit_type_name":"assessor","full":"Marsh, H. & Sobtzick, S.","value":["v1","v2"]}],
             "scopes":[{"description":{"en":"Global"},"code":"1"}]}
            """);
        var gorilla = Parse("""
            {"assessment_id":291011372,"sis_taxon_id":39994,"year_published":null,
             "citation":" . Gorilla beringei. The IUCN Red List of Threatened Species : e.T39994A291011372. Accessed on 24 August 2026.",
             "taxon":{"sis_id":39994,"scientific_name":"Gorilla beringei"},"credits":[],"scopes":[{"description":{"en":"Global"},"code":"1"}]}
            """);
        tally.AddLatest(AssessmentScopeKind.Global);
        tally.AddLatest(AssessmentScopeKind.Global);
        tally.AddParse(dugong, AssessmentScopeKind.Global);
        tally.AddParse(gorilla, AssessmentScopeKind.Global);
        tally.WikiCompared = true;
        var cite = CiteIucnTemplateScanner.Find("{{cite iucn |author=Marsh, H. |author2=Sobtzick, S. |year=2019 |title=''Dugong dugon'' |article-number=e.T6909A160756767 |doi=10.2305/IUCN.UK.2015-4.RLTS.T6909A160756767.en}}").Single();
        tally.AddComparison(dugong.Parts!, "Dugong", WikiCitationComparer.Compare(dugong.Parts!, cite, null));

        var report = CitationCheckReport.Build(tally, new CitationCheckInputs("/cache.sqlite", "/wiki.sqlite", null, DateTimeOffset.UnixEpoch, TimeSpan.FromSeconds(3)));

        Assert.Contains("| Read into citation parts | 1 | 50.0% |", report);
        Assert.Contains("| No year published (not published) | 1 | 291011372 Gorilla beringei |", report);
        Assert.Contains("| (amended version of YYYY assessment) | 1 |", report);
        Assert.Contains("| Accepted: this assessment | 1 |", report);
        Assert.Contains("| Identical | 1 | 100.0% |", report);
        Assert.Contains("| Same DOI | Accepted: this assessment | 1 |", report);
        Assert.Contains("| … DOIs identical (where both have one) | 1 | 100.0% |", report);
        Assert.Contains("| \\|amends= | 0 | 0 | 1 | 0 | 0 |", report);
        Assert.Contains("[[Dugong]] 160756767 Dugong dugon (amends 2015): IUCN 2015, wiki none", report);
    }
}
