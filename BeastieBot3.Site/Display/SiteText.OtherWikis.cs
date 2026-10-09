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

    public static string WikiPolishCode(string code, string polishCode) =>
        $"Polish infoboxes have no {code} category, so the status is given as {polishCode}.";

    public static string WikiPolishNoCode(string code) =>
        $"Polish infoboxes have no {code} category, so only |IUCN id = is given.";

    public const string WikiPolishRefName =
        "Keep the reference name “iucn” so that the infobox's footnote shows this citation.";

    public const string WikiFrenchCurrentAssessment =
        "{{UICN}} always links to the taxon's current assessment on the IUCN Red List website, even when it cites an earlier assessment.";

    public const string WikiSpanishWikidataLink =
        "Use this citation only in the article about this taxon. Spanish Wikipedia's {{IUCN}} builds its link from the IUCN taxon ID on the article's Wikidata item.";
}
