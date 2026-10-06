namespace BeastieBot3.Site.Display;

// The choice of citation template ({{cite iucn}} or {{cite Q}}) on the taxon page and the status
// update page. The group page's species tables use the same option labels (GroupText.Tables).

public static partial class SiteText {
    public const string StatusRefTemplateLabel = "Citation in taxobox status_ref";
    public const string StatusRefTemplateIucn = "{{cite iucn}}";
    public const string StatusRefTemplateQ = "{{cite Q}} when the assessment has a Wikidata item";
    public const string StatusRefTemplateHelp = "Applies only to the taxobox status lines. The {{cite iucn}} and {{cite Q}} boxes stay the same.";
    public const string StatusRefNoItem = "This assessment has no Wikidata item, so status_ref uses {{cite iucn}}.";
}
