namespace BeastieBot3.Site.Display;

// The choice of citation template ({{cite iucn}}, {{cite Q}}, or {{cite Q}} with the item's
// QuickStatements commands) on the citations page for English Wikipedia. The group page's species tables use the same option labels (GroupText.Tables).

public static partial class SiteText {
    public const string CitationTemplateLabel = "Citation template";
    public const string StatusRefTemplateIucn = "{{cite iucn}}";
    public const string StatusRefTemplateQ = "{{cite Q}} if the assessment has a Wikidata item";
    public const string StatusRefTemplateQCreate = "{{cite Q}}, and QuickStatements commands for the assessment's Wikidata item";
    public const string CitationTemplateHelp =
        "With {{cite Q}}, the taxobox status_ref uses {{cite Q}}, and a {{cite Q}} box is added below the {{cite iucn}} box. "
        + "The QuickStatements commands create the assessment's Wikidata item if there is none; for an existing item, they add its missing statements and correct its title and label.";
    /// When the assessment has no Wikidata item: CiteQNoItem, then a link CiteQNoItemLink to the third choice.
    public const string CiteQNoItem = "This assessment has no Wikidata item, so the citations use {{cite iucn}}.";
    public const string CiteQNoItemLink = "Show QuickStatements commands to create the item";
}
