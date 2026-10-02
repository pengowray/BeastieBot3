using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// The search form. Variant "header" is the compact form in the page header (label visually
/// hidden); "main" is the full form on the home, search and error pages.
public sealed record SearchFormModel(string? Query, string Variant) {
    public bool IsHeader => Variant == "header";
    public string InputId => $"q-{Variant}";
    public string ListId => $"suggest-{Variant}";
}

/// A category badge: the code on the IUCN colour, with the full label beside it unless the caller
/// shows the label elsewhere.
public sealed record BadgeModel(CategoryDisplay Category, bool Large = false, bool ShowLabel = true) {
    public static BadgeModel? For(TaxonSummary taxon, bool large = false) =>
        taxon.Category is null
            ? null
            : new BadgeModel(IucnCategories.Describe(taxon.Category, taxon.PossiblyExtinct, taxon.PossiblyExtinctInTheWild), large);

    public static BadgeModel For(AssessmentRow assessment, bool large = false, bool showLabel = true) =>
        new(IucnCategories.Describe(assessment), large, showLabel);
}

/// One taxon in a list of results (search, name lookup) or of child taxa, with a note saying which
/// of its names matched when that is not its scientific or displayed common name.
public sealed record TaxonListItem(TaxonSummary Taxon, string? MatchNoteLabel = null, string? MatchedNameHtml = null) {
    public static TaxonListItem FromHit(SearchHit hit) {
        var taxon = hit.Taxon;
        switch (hit.MatchedNameType) {
            case NameTypes.Synonym:
                return new TaxonListItem(taxon, SiteText.MatchSynonymLabel, ScientificNameMarkup.ToHtml(hit.MatchedName));
            case NameTypes.Common:
                if (taxon.CommonNameEn is not null && SiteNameKey.Fold(taxon.CommonNameEn) == SiteNameKey.Fold(hit.MatchedName)) {
                    return new TaxonListItem(taxon);
                }
                var name = SiteHtml.Encode(hit.MatchedName);
                var html = string.IsNullOrWhiteSpace(hit.MatchedLanguage)
                    ? name
                    : $"{name} ({SiteHtml.Encode(LanguageNames.Name(hit.MatchedLanguage))})";
                return new TaxonListItem(taxon, SiteText.MatchCommonNameLabel, html);
            default:
                return new TaxonListItem(taxon);
        }
    }
}
