namespace BeastieBot3.Shared.Wikitext;

// Options of the wikitext renderers shared by the CLI and the public site. The renderers are in this
// folder: CiteIucnRenderer, IucnStatusTemplate, SpeciesboxStatus and ScientificNameMarkup. Keep
// their public signatures; the site codes against them.

/// How {{cite iucn}} names the authors.
public enum CiteAuthorStyle {
    /// |author=Wiig, Ø. |author2=Amstrup, S. ... (the form most en-wiki articles and {{make cite IUCN}} use)
    AuthorN,
    /// |last1=Wiig |first1=Ø. |last2=Amstrup |first2=S. ... (the form Template:Cite IUCN/doc shows).
    /// Organisations and names the parser could not split stay whole as |authorN=, numbered in the
    /// same sequence.
    LastFirst,
}

public sealed record CiteIucnOptions {
    public CiteAuthorStyle AuthorStyle { get; init; } = CiteAuthorStyle.AuthorN;

    /// |access-date= in "D Month YYYY" form; null leaves the parameter out.
    public DateOnly? AccessDate { get; init; }

    /// Wrap the template in <ref>...</ref>.
    public bool WrapInRef { get; init; }

    /// Name for <ref name="...">; ignored unless WrapInRef. Null or blank gives a plain <ref>.
    public string? RefName { get; init; }

    /// |name-list-style=amp, which puts "&" before the last author as IUCN does. Only written when
    /// there are two or more authors.
    public bool NameListStyleAmp { get; init; }
}
