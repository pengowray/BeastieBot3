namespace BeastieBot3.Site.Pages;

/// One choice in the row under "Cite for" (_CiteForChooser): Wikidata or a Wikipedia. Lang is the
/// wiki's language code for the link's lang attribute (null for Wikidata). OptionsKey is the
/// data-options-link key that site.js uses to keep the link's address in step with the citation
/// options; null for a link that takes no options.
public sealed record CiteForLink(string Text, string? Lang, string Href, bool Current, string? OptionsKey);

/// The heading ("Cite for Wikipedia", "Cite for Wikidata") and the row of choices of the citations pages.
public sealed record CiteForChooser(string Heading, IReadOnlyList<CiteForLink> Links);
