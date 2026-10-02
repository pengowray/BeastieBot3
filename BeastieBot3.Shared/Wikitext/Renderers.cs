namespace BeastieBot3.Shared.Wikitext;

// Signatures of the wikitext renderers shared by the CLI and the public site. Implementations
// replace the NotImplementedException bodies; keep the signatures, the site codes against them.

/// How {{cite iucn}} names the authors.
public enum CiteAuthorStyle {
    /// |author=Wiig, Ø. |author2=Amstrup, S. ... (the form most en-wiki articles and {{make cite IUCN}} use)
    AuthorN,
    /// |last1=Wiig |first1=Ø. |last2=Amstrup |first2=S. ... (the form Template:Cite IUCN/doc shows)
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

    /// |name-list-style=amp, which puts "&" before the last author as IUCN does.
    public bool NameListStyleAmp { get; init; }
}

public static class CiteIucnRenderer {
    /// {{cite iucn}} for one assessment, on one line.
    public static string Render(IucnCitationParts parts, CiteIucnOptions? options = null) =>
        throw new NotImplementedException();
}

public static class SpeciesboxStatus {
    /// The status lines of a {{Speciesbox}} or {{Taxobox}}:
    /// "| status = EN\n| status_system = IUCN3.1\n| status_ref = <ref>...</ref>".
    /// statusRef is the complete reference markup, or null to leave status_ref out.
    public static string Render(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild,
        string? criteriaVersion, string? statusRef) =>
        throw new NotImplementedException();
}

public static class ScientificNameMarkup {
    /// ''Panthera pardus'' ssp. ''orientalis''; ''Panthera leo'' West Africa subpopulation.
    /// Rank markers (ssp., subsp., var., f.) and the subpopulation words stay upright.
    public static string ToWikitext(string scientificName, string? subpopulationName = null) =>
        throw new NotImplementedException();

    /// The same split as ToWikitext, as HTML-encoded text with <i> elements.
    public static string ToHtml(string scientificName, string? subpopulationName = null) =>
        throw new NotImplementedException();
}
