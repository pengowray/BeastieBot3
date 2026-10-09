namespace BeastieBot3.Site.Display;

// The choice of citation template ({{cite iucn}} or {{cite Q}}) on the taxon page and the status
// update page. The group page's species tables use the same option labels (GroupText.Tables).

public static partial class SiteText {
    public const string CitationTemplateLabel = "Citation template";
    public const string StatusRefTemplateIucn = "{{cite iucn}}";
    public const string StatusRefTemplateQ = "{{cite Q}} if the assessment has a Wikidata item";
    public const string CitationTemplateHelp = "{{cite Q}} cites the assessment's Wikidata item. With this option, the taxobox status_ref uses {{cite Q}}, and a {{cite Q}} box is added below the {{cite iucn}} box.";
    /// When the assessment has no Wikidata item: CiteQNoItemBefore + link to the Wikidata page + CiteQNoItemAfter.
    public const string CiteQNoItemBefore = "This assessment has no Wikidata item, so the citations use {{cite iucn}}. ";
    public const string CiteQNoItemLink = "QuickStatements commands to create the item";
    public const string CiteQNoItemAfter = " are on the Wikidata references page.";
}
