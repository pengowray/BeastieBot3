using BeastieBot3.WikipediaLists;

// The articles a Wikipedia list line links for each taxon in the release (taxon.list_article_title
// and list_parent_article_title), chosen by SpeciesLineFormatter as `wikipedia generate-lists`
// chooses them, so the lists the site makes link the same pages as the generated lists.

namespace BeastieBot3.SiteBuild;

internal static class SiteListLinks {
    public static void Resolve(IEnumerable<SiteTaxon> taxa, SpeciesLineFormatter lines, SiteBuildStats stats,
        CancellationToken cancellationToken) {
        foreach (var taxon in taxa) {
            cancellationToken.ThrowIfCancellationRequested();
            if (!taxon.InRelease || taxon.Genus is null || taxon.SpeciesEpithet is null || taxon.Kingdom is null) {
                continue;
            }
            var record = ToRecord(taxon);
            taxon.ListArticleTitle = SiteBuildRules.NullIfBlank(lines.ResolveArticleTitle(record));
            if (taxon.ListArticleTitle is not null) {
                stats.ListArticleTitles++;
            } else if (taxon.InfraName is not null) {
                taxon.ListParentArticleTitle = SiteBuildRules.NullIfBlank(lines.ResolveParentSpeciesArticle(record));
            }
        }
    }

    // The fields of a list record that the article resolution reads. A subpopulation is looked up by
    // its species' name: IUCN writes "Lycaon pictus North Africa subpopulation".
    private static IucnSpeciesRecord ToRecord(SiteTaxon taxon) {
        var name = taxon.SubpopulationName is { } subpopulation
            ? SiteBuildRules.SubpopulationParentName(taxon.ScientificName, subpopulation) ?? taxon.ScientificName
            : taxon.ScientificName;
        return ToRecord(taxon, name);
    }

    private static IucnSpeciesRecord ToRecord(SiteTaxon taxon, string name) => new(
        TaxonId: taxon.TaxonId,
        AssessmentId: taxon.LatestGlobalAssessmentId ?? 0,
        RedlistCategory: string.Empty,
        StatusCode: taxon.LatestGlobalStatusCode ?? string.Empty,
        ScientificNameAssessments: name,
        ScientificNameTaxonomy: name,
        KingdomName: taxon.Kingdom!,
        PhylumName: taxon.Phylum,
        ClassName: taxon.ClassName,
        OrderName: taxon.OrderName,
        FamilyName: taxon.Family,
        GenusName: taxon.Genus!,
        SpeciesName: taxon.SpeciesEpithet!,
        InfraType: taxon.InfraRank,
        InfraName: taxon.InfraName,
        SubpopulationName: null,
        Scopes: null,
        Authority: taxon.Authority,
        InfraAuthority: null,
        PossiblyExtinct: null,
        PossiblyExtinctInTheWild: null,
        YearPublished: null);
}
