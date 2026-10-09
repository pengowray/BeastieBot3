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
        $"{wikiName} Wikipedia's taxoboxes have no conservation status, so this page gives the citation only.";

    public static string WikiPolishCode(string code, string polishCode) =>
        $"Polish infoboxes have no {code} category, so the status is written {polishCode}.";

    public static string WikiPolishNoCode(string code) =>
        $"Polish infoboxes have no {code} category, so the lines give only the IUCN id.";

    public const string WikiPolishRefName =
        "Keep the reference name “iucn”: the infobox then points its footnote at this citation.";

    public const string WikiFrenchCurrentAssessment =
        "{{UICN}} always links IUCN's current assessment of the taxon, whichever assessment it cites.";

    public const string WikiSpanishWikidataLink =
        "Spanish Wikipedia's {{IUCN}} links the IUCN taxon ID on the article's own Wikidata item, so it fits only the article about this taxon.";
}
