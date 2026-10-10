using System.Text.Json;
using System.Text.Json.Serialization;

namespace BeastieBot3.Shared.Wikitext;

// The parts of one IUCN Red List assessment citation, parsed once at build time (`site build-db`)
// from the cached API payload and stored as JSON in the site database. The public site renders
// {{cite iucn}} from these parts at request time, so author style, access date and ref name are
// options of the renderer, not stored variants.

/// How an author name could be read.
[JsonConverter(typeof(JsonStringEnumConverter<CitationAuthorKind>))]
public enum CitationAuthorKind {
    /// "Surname, Initials" as IUCN publishes it: Last = "Wiig", Initials = "Ø.".
    Person,
    /// An organisation, kept whole: "BirdLife International", "Royal Botanic Gardens, Kew".
    Organisation,
    /// A name the parser could not split with confidence (given name first, typos): Display is
    /// used as published.
    Verbatim,
}

/// One author, in the order the IUCN citation lists them.
/// Display is the name exactly as IUCN's citation writes it ("Wiig, Ø.", "BirdLife International").
/// GivenNames is the person's full given names from the assessment's credits value[] list
/// ("Catherine" for "Sayer, C."), set only when the match is certain; null otherwise.
public sealed record CitationAuthor(
    CitationAuthorKind Kind,
    string Display,
    string? Last = null,
    string? Initials = null,
    string? GivenNames = null);

/// Where a DOI came from. A DOI is only stored when its taxon id matches, and its assessment id
/// matches or belongs to the assessment this one is an errata version of.
[JsonConverter(typeof(JsonStringEnumConverter<DoiSource>))]
public enum DoiSource {
    None,
    /// In IUCN's own citation text in the cached API payload.
    Citation,
    /// From GBIF's copy of the IUCN checklist (CC BY 4.0), which carries the DOI of the latest
    /// global assessment.
    Gbif,
    /// From a Wikidata item for the assessment (P356).
    Wikidata,
    /// Found by `iucn resolve-dois`: in Crossref's list of IUCN DOIs, or by checking possible DOIs at doi.org.
    Resolved,
}

public sealed record IucnCitationParts {
    public required long TaxonId { get; init; }
    public required long AssessmentId { get; init; }

    /// Year published: the citation year, and the |volume= of {{cite iucn}}.
    public required int Year { get; init; }

    /// The taxon name as IUCN's citation title gives it, with rank markers ("Panthera pardus ssp.
    /// orientalis", "Cupressus arizonica var. glabra") and, for a subpopulation, its name and the word
    /// "subpopulation" ("Panthera leo West Africa subpopulation"). No "(... assessment)" annotations.
    public required string ScientificName { get; init; }

    /// The subpopulation name inside ScientificName ("West Africa"), so a renderer knows which
    /// trailing words stay upright. Null for species and infraspecific taxa.
    public string? SubpopulationName { get; init; }

    /// The region of a regional assessment ("Europe" from "(Europe assessment)"); null for global.
    public string? RegionalScope { get; init; }

    /// From "(errata version published in YYYY)": the year the errata version was published.
    public int? ErrataYear { get; init; }

    /// From "(amended version of YYYY assessment)": the year of the assessment it amends.
    public int? AmendsYear { get; init; }

    public IReadOnlyList<CitationAuthor> Authors { get; init; } = [];

    /// IUCN's author string ends with "et al.": render |display-authors=etal.
    public bool AuthorsEtAl { get; init; }

    /// "10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en", without a resolver prefix.
    public string? Doi { get; init; }
    public DoiSource DoiSource { get; init; }

    /// The name part of the title registered with Crossref for Doi ("Canis mesomelas" from "Canis
    /// mesomelas: Hoffmann, M."); ScientificName is the taxon's current name. Crossref's title has
    /// the name current when the record was last deposited, which for some 2008 and 2010 DOIs was
    /// 2015. Set by `site build-db` only when Doi names this assessment's own
    /// ids and `iucn resolve-dois` stored Crossref's title for it; null otherwise.
    public string? RegisteredName { get; init; }

    /// The day Doi was registered with Crossref ("2015-09-10", Crossref's created date), set with
    /// RegisteredName; null when not known. Depositing a record again keeps its title, so
    /// RegisteredName is the name current on this day.
    public string? DoiCreated { get; init; }

    /// True when RegisteredName is the name the assessment was published under: Doi names a Red List
    /// release of 2015 or later and was created in that year or the next. IUCN registered the DOIs of
    /// the releases before 2015 in 2015 and 2016, with the names current then; from 2015 it
    /// registered them when it published each release (in October 2026, all but about 1,000 of the
    /// 175,000 DOIs of releases from 2015 on were created in the release's year or the next).
    [JsonIgnore]
    public bool RegisteredNameIsFromPublication =>
        RegisteredName is not null && DoiReleaseYear(Doi) is { } release && release >= 2015
        && DoiCreated is { Length: >= 4 } created && int.TryParse(created[..4], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var year)
        && year - release is >= 0 and <= 1;

    /// The year of the Red List release a DOI names: 2019 for "10.2305/IUCN.UK.2019-3.RLTS.T...",
    /// 2008 for "10.2305/IUCN.UK.2008.RLTS.T..."; null for any other text.
    public static int? DoiReleaseYear(string? doi) {
        var match = doi is null ? null : DoiRelease.Match(doi);
        return match is { Success: true } && int.TryParse(match.Groups[1].Value, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var year) ? year : null;
    }

    private static readonly System.Text.RegularExpressions.Regex DoiRelease =
        new(@"IUCN\.UK\.(\d{4})(?:-\d+)?\.RLTS\.", System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// RegisteredName when it names the taxon differently from currentName (more than "ssp." for
    /// "subsp.", brackets or spacing: WikidataCitation.SameName) and is not one of IUCN's internal
    /// names ("Physella acuta_new"); null otherwise. The history tables compare it with the row's
    /// taxon's current name, and a citation with ScientificName.
    public string? RegisteredNameDifferentFrom(string? currentName) {
        var registered = WikidataCitation.NameText(RegisteredName);
        return registered.Length == 0 || WikidataCitation.IsIucnInternalName(registered)
            || WikidataCitation.SameName(registered, currentName) ? null : registered;
    }

    /// These parts with name as ScientificName, for a citation whose |title= gives another name than
    /// IUCN's citation (CiteIucnOptions.TitleName); the parts unchanged when name is null, blank or
    /// names the taxon as ScientificName does (WikidataCitation.SameName). SubpopulationName is
    /// cleared, since the name is written whole ("Sousa chinensis (Eastern Taiwan Strait subpopulation)").
    public IucnCitationParts WithTitleName(string? name) {
        var text = WikidataCitation.NameText(name);
        return text.Length == 0 || WikidataCitation.SameName(text, ScientificName)
            ? this
            : this with { ScientificName = text, SubpopulationName = null };
    }

    /// IUCN's citation text from the payload with its "Accessed on ..." sentence removed.
    public string? IucnCitationText { get; init; }

    /// When the API payload was downloaded (UTC).
    public DateTime? DownloadedAtUtc { get; init; }

    /// "e.T22823A14871490": |article-number= of {{cite iucn}}.
    [JsonIgnore]
    public string ArticleNumber => $"e.T{TaxonId}A{AssessmentId}";

    /// https://www.iucnredlist.org/species/{taxonId}/{assessmentId}
    [JsonIgnore]
    public string Url => $"https://www.iucnredlist.org/species/{TaxonId}/{AssessmentId}";

    private static readonly JsonSerializerOptions JsonOptions = new() {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);

    public static IucnCitationParts? FromJson(string? json) =>
        string.IsNullOrWhiteSpace(json) ? null : JsonSerializer.Deserialize<IucnCitationParts>(json, JsonOptions);
}
