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
    /// same sequence; a sole author that is not a person is |author= ("|author=BirdLife International").
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

    /// Use a person's full given names (CitationAuthor.GivenNames) where IUCN lists them, instead of
    /// the initials IUCN's citation prints. Authors without GivenNames keep their initials.
    public bool FullGivenNames { get; init; }

    /// The name to write in |title= instead of IUCN's citation name, which is always the taxon's
    /// current name: the name the assessment was published under, or the name in the title
    /// registered for its DOI (IucnCitationParts.WithTitleName). Null by default: the site's citation
    /// page sets it (and offers the current name as an option); the status update page, the lists
    /// and the species tables write the current name.
    public string? TitleName { get; init; }

    /// The wiki's copy of {{cite iucn}} to write for (CiteIucnDialect); English Wikipedia's by default.
    public CiteIucnDialect Dialect { get; init; } = CiteIucnDialect.English;
}

/// A Wikipedia's copy of English Wikipedia's {{cite iucn}} (Module:Cite IUCN): the template's name, the
/// parameter that carries the article number ("e.T22823A14871490"), the DOI language suffixes its
/// module accepts, and how it wants the access date. See OtherWikipedias for the evidence.
public sealed record CiteIucnDialect(
    string TemplateName,
    string ArticleNumberParameter,
    IReadOnlySet<string> DoiLanguages,
    bool IsoAccessDate) {
    private static readonly IReadOnlySet<string> FourLanguages = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "en", "es", "fr", "pt" };

    public static readonly CiteIucnDialect English = new("cite iucn", "article-number", FourLanguages, IsoAccessDate: false);
}
