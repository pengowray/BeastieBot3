namespace BeastieBot3.Site.Display;

// The wikitext page's choice of Wikipedia (OtherWikipedias) and its notes about each wiki's templates.

public static partial class SiteText {
    /// The accessible name of the row of links to each Wikipedia.
    public const string WikiChooserLabel = "Wikipedia language";

    /// The heading of the wikitext section: "Wikitext for French Wikipedia".
    public static string HeadingWikitextFor(BeastieBot3.Shared.Wikitext.WikipediaEdition edition) => $"Wikitext for {edition.Name} Wikipedia";

    /// The label of the citation box for a wiki other than English: "{{UICN}} citation".
    public static string LabelCiteTemplate(string template) => $"{template} citation";

    public static string WikiNoTaxoboxStatus(string wikiName) =>
        $"{wikiName} Wikipedia's taxoboxes have no conservation status, so this page shows only the citation.";

    // The notes below follow the capabilities on WikipediaEdition. wikiName: "Polish"; template: the
    // wiki's citation template, "{{UICN}}".

    /// TaxoboxMainCategoriesOnly, for a category the box does not have: "CR(PE)" shown as "CR".
    public static string WikiCategoryShownAs(string wikiName, string code, string shownAs) =>
        $"{wikiName} infoboxes have no {code} category, so the status is given as {shownAs}.";

    /// TaxoboxMainCategoriesOnly, for a category the box cannot show at all (NE).
    public static string WikiCategoryNotShown(string wikiName, string code) =>
        $"{wikiName} infoboxes have no {code} category, so only |IUCN id = is given.";

    /// TaxoboxFootnoteRefName.
    public static string WikiFootnoteRefName(string refName) =>
        $"Keep the reference name “{refName}” so that the infobox's footnote shows this citation.";

    /// CitationLinksCurrentAssessment.
    public static string WikiLinksCurrentAssessment(string template) =>
        $"{template} always links to the taxon's current assessment on the IUCN Red List website, even when it cites an earlier assessment.";

    /// CitationLinksFromArticleItem.
    public static string WikiLinksFromArticleItem(string wikiName, string template) =>
        $"Use this citation only in the article about this taxon. {wikiName} Wikipedia's {template} builds its link from the IUCN taxon ID on the article's Wikidata item.";
}
