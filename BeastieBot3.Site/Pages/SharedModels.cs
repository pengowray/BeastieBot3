using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

/// The search form. Variant "header" is the compact form in the page header (label visually
/// hidden); "main" is the full form on the home, search and error pages. HideLabel keeps the
/// label for screen readers only, on a page whose heading already says the same.
public sealed record SearchFormModel(string? Query, string Variant, bool HideLabel = false) {
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

    /// The badge of an {{IUCN status}} code as the group counts store it ("CR(PE)", "LR/nt").
    public static BadgeModel ForStatusCode(string code, bool showLabel = false) => code switch {
        "CR(PE)" => new(IucnCategories.Describe("CR", true, false), false, showLabel),
        "CR(PEW)" => new(IucnCategories.Describe("CR", false, true), false, showLabel),
        _ => new(IucnCategories.Describe(code, false, false), false, showLabel),
    };
}

/// A help text behind a small "i" button (_InfoTip). Id: the id of the text, unique on the page.
/// Label: the button's accessible name, naming what the text explains.
public sealed record InfoTipModel(string Id, string Label, string Text);

/// The number after a list option (_OptionCount): how many headings a rank adds, or how many lines a
/// Red List category adds. Key names the option for live updates ("h:genus", "cat:CR"). A null
/// Count shows nothing.
public sealed record OptionCountModel(string Key, int? Count);

/// The possible-duplicates panel under a group's list (_ListNotices). GroupRank: the page's rank, or
/// null for a Catalogue of Life group, for the state of an entry outside the group.
public sealed record ListNoticesModel(BeastieBot3.Site.Lists.ListNoticeGroups Groups, string? GroupRank);

/// One side of a pair in that panel (_ListNoticeEntry). ShowState: whether to write the entry's state
/// after its source (left out when the column heading already says it).
public sealed record ListNoticeSideModel(BeastieBot3.Site.Lists.NoticeSide Side, string? GroupRank, bool ShowState);

/// One taxon in a list of results (search, name lookup) or of child taxa, with a note saying which
/// of its names matched when that is not its scientific or displayed common name.
/// Url: where the name links, when not the taxon page (an assessment found by its id links the page
/// with that assessment shown).
public sealed record TaxonListItem(TaxonSummary Taxon, string? MatchNoteLabel = null, string? MatchedNameHtml = null, string? Url = null) {
    public static TaxonListItem FromIdHit(IdHit hit) => hit.AssessmentId is { } aid
        ? new TaxonListItem(hit.Taxon, SiteText.MatchAssessmentIdLabel,
            SiteHtml.Encode(SiteText.MatchAssessmentId(aid, hit.Scope, hit.YearPublished)), SearchModel.SpeciesUrl(hit))
        : new TaxonListItem(hit.Taxon, SiteText.MatchTaxonIdLabel, SiteHtml.Encode(hit.Taxon.TaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture)));

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
                if (LanguageNames.LangAttribute(hit.MatchedLanguage) is { } lang) {
                    name = $"<span lang=\"{SiteHtml.Encode(lang)}\">{name}</span>";
                }
                var html = LanguageNames.Key(hit.MatchedLanguage).Length == 0
                    ? name
                    : $"{name} ({SiteHtml.Encode(LanguageNames.Name(hit.MatchedLanguage))})";
                return new TaxonListItem(taxon, SiteText.MatchCommonNameLabel, html);
            default:
                return new TaxonListItem(taxon);
        }
    }
}
