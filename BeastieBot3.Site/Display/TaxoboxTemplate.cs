namespace BeastieBot3.Site.Display;

/// The taxobox an English Wikipedia article about a taxon of this kind uses, for the label of the
/// status parameters box and the notes that name it. The status, status_system and status_ref
/// parameters are the same in every taxobox; only the name differs.
/// Name: "{{Speciesbox}}", or "taxobox" for a subpopulation. Noun: Name as the subject of a
/// sentence ("{{Speciesbox}}", "the taxobox").
public sealed record TaxoboxTemplate(string Name, string Noun) {
    public static readonly TaxoboxTemplate Speciesbox = new("{{Speciesbox}}", "{{Speciesbox}}");

    /// Zoological subspecies.
    public static readonly TaxoboxTemplate Subspeciesbox = new("{{Subspeciesbox}}", "{{Subspeciesbox}}");

    /// Subspecies and varieties under the botanical code (plants, fungi, chromists).
    public static readonly TaxoboxTemplate Infraspeciesbox = new("{{Infraspeciesbox}}", "{{Infraspeciesbox}}");

    /// Subpopulations, which have no taxobox template of their own.
    public static readonly TaxoboxTemplate Generic = new("taxobox", "the taxobox");

    public static TaxoboxTemplate For(string kind, string? kingdom) => kind switch {
        "subspecies" when string.Equals(kingdom?.Trim(), "ANIMALIA", StringComparison.OrdinalIgnoreCase) => Subspeciesbox,
        "subspecies" or "variety" => Infraspeciesbox,
        "subpopulation" => Generic,
        _ => Speciesbox,
    };

    /// "{{Speciesbox}} status parameters", "Taxobox status parameters".
    public string Label => SiteText.TaxoboxLabel(Name);
}
