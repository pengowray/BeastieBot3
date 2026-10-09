using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace BeastieBot3.Shared.Wikitext;

// The citation and the taxobox status of an IUCN assessment for Wikipedias other than English.
//
// Evidence: a survey of each wiki's templates, fetched on 2026-10-09 with read-only API requests
// (template sources, documentation pages and live article revisions). Where a doc page and the
// template's source disagreed, the source was followed. The full survey, with a link for each claim,
// is docs/other-wikipedias.md.
// - de: Vorlage:Taxobox has no conservation status. Vorlage:IUCN (doc Vorlage:IUCN/Doku): Year (the
//   Red List edition), ID, ScientificName (plain; the template italicises it), AssessmentID (without
//   it the link is a search), YearAssessed, Assessor ("M. C. M. Kierulff, A. B. Rylands, ..."), Abruf
//   (YYYY-MM-DD). Live: Löwe, oldid 269766074.
// - fr: the taxobox status is its own line, {{Taxobox UICN | VU | A3c }} (Modèle:Taxobox UICN):
//   positional code (upper case; PE, PEW, CD for LR/cd, NT for LR/nt, LC for LR/lc), positional
//   criteria. Live: Ours blanc, oldid 240165899. Modèle:UICN: positional taxon id, positional
//   description ("''Ursus maritimus'' Phipps, 1774"), consulté le (French date), rang (default
//   "espèce"). It links https://www.iucnredlist.org/details/<id>/0, the current assessment.
// - es: Plantilla:Ficha de taxón takes status, status_system (IUCN3.1, IUCN2.3) and status_ref with
//   the English codes, PE and PEW included (Plantilla:Ficha de taxón/estado de conservación).
//   Plantilla:IUCN: título, asesores (one field), año, edición, consultado ("9 de octubre de 2026");
//   its link is P627 of the article's own Wikidata item. Live: Panthera leo, oldid 175619158.
// - pl: Szablon:Zwierzę infobox and Szablon:Takson infobox: |status IUCN = (upper case EX, EW, CR,
//   EN, VU, NT, LC, DD only) and |IUCN id =; the box points its footnote at <ref name="iucn"> when the
//   article has one. Szablon:IUCN: id, nazwa, autor ("Ø. Wiig, S. Amstrup, ..."), iucn rok, wersja,
//   doi (the link then goes to the DOI), data dostępu (ISO). Live: Niedźwiedź polarny, oldid 80433403.
// - pt: Predefinição:Info/Taxonomia: estado (English codes, LR/cd form), sistema_estado (iucn3.1 or
//   iucn2.3, lower case), estado_ref. Predefinição:Citar iucn (Módulo:Iucn): an older copy of the
//   English module that reads |page= or |página= and accepts DOIs ending .en only.
// - uk: Шаблон:Картка:Таксономія and Speciesbox take the English status lines. Шаблон:Cite IUCN
//   (Модуль:Cite IUCN, 2025): |page=, DOIs ending .en, .es, .fr or .pt.
// - ja: Template:生物分類表: |status = (3.1 codes as in English; 1994 codes as VU2.3, EN2.3, ...,
//   LR/cd, LR/nt, LR/lc), no status system for IUCN codes (leave it out: with PE it names an image
//   that does not exist), |status_ref =. Template:Cite iucn is a copy of the English module with
//   |article-number= and DOIs in en, es, fr and pt.
// - zh: the Speciesbox takes the English status lines. Template:IUCN (doc Template:IUCN/doc): author1
//   ..., year, id, title, errata and amends as IUCN's words ("errata version published in 2017"),
//   page (the article number), doi, access-date (ISO). Live: 狮.

/// A Wikipedia this site writes IUCN citations and taxobox status lines for, and what its templates
/// take. Code: the wiki's language code ("fr"); Name: its English name; NativeName: its name in its
/// language. OtherWikipedias.All has one for each wiki, with the facts from the evidence above.
public sealed record WikipediaEdition(string Code, string Name, string NativeName) {
    public bool IsEnglish => Code == "en";

    /// The template its citation uses ("{{UICN}}"), for labels.
    public required string CitationTemplate { get; init; }

    /// The taxobox its status lines are for ("{{Ficha de taxón}}"); null when its taxoboxes have no
    /// conservation status (de).
    public string? TaxoboxTemplate { get; init; }

    /// How its citation template differs from English {{cite iucn}}, when it is a copy of it (en, ja,
    /// pt, uk); null otherwise. A copy takes the author style (|author= or |last=/|first=) and
    /// |name-list-style=amp.
    public CiteIucnDialect? CiteIucnCopy { get; init; }

    /// Its citation can give the authors' full given names (CiteIucnOptions.FullGivenNames): the
    /// {{cite iucn}} copies and zh's {{IUCN}}.
    public bool TakesFullGivenNames { get; init; }

    /// Its citation links the taxon's current assessment, whatever assessment it cites (fr's {{UICN}}).
    public bool CitationLinksCurrentAssessment { get; init; }

    /// Its citation builds its link from the IUCN taxon ID (P627) on the article's own Wikidata item
    /// (es's {{IUCN}}), so it suits only the article about the taxon.
    public bool CitationLinksFromArticleItem { get; init; }

    /// Its taxobox has only the categories EX, EW, CR, EN, VU, NT, LC and DD (pl), so the status lines
    /// give MainCategoryCode's code, or no status.
    public bool TaxoboxMainCategoriesOnly { get; init; }

    /// The ref name that its taxobox's status footnote points at when the article has a reference with
    /// that name (pl: "iucn"); null for a taxobox that has a reference parameter or no footnote.
    public string? TaxoboxFootnoteRefName { get; init; }
}

/// What a citation and the taxobox lines need beyond the citation parts: the category and flags,
/// the criteria version and criteria, the year assessed, the authority and the taxon's kind.
public sealed record AssessmentFacts(
    string Category,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild,
    string? CriteriaVersion,
    string? Criteria,
    int? AssessedYear,
    string? Authority,
    string Kind);

public static partial class OtherWikipedias {
    public static readonly CiteIucnDialect Portuguese = new("citar iucn", "page", new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "en" }, IsoAccessDate: true);
    public static readonly CiteIucnDialect Ukrainian = new("Cite IUCN", "page", CiteIucnDialect.English.DoiLanguages, IsoAccessDate: true);

    public static readonly WikipediaEdition English = new("en", "English", "English") {
        CitationTemplate = "{{cite iucn}}", TaxoboxTemplate = "{{Speciesbox}}", CiteIucnCopy = CiteIucnDialect.English, TakesFullGivenNames = true,
    };

    /// The Wikipedias the site writes for, English first, then by size.
    public static readonly IReadOnlyList<WikipediaEdition> All = [
        English,
        new("de", "German", "Deutsch") { CitationTemplate = "{{IUCN}}" },
        new("fr", "French", "Français") {
            CitationTemplate = "{{UICN}}", TaxoboxTemplate = "{{Taxobox UICN}}", CitationLinksCurrentAssessment = true,
        },
        new("es", "Spanish", "Español") {
            CitationTemplate = "{{IUCN}}", TaxoboxTemplate = "{{Ficha de taxón}}", CitationLinksFromArticleItem = true,
        },
        new("pl", "Polish", "Polski") {
            CitationTemplate = "{{IUCN}}", TaxoboxTemplate = "{{Zwierzę infobox}}", TaxoboxMainCategoriesOnly = true, TaxoboxFootnoteRefName = "iucn",
        },
        new("zh", "Chinese", "中文") { CitationTemplate = "{{IUCN}}", TaxoboxTemplate = "{{Speciesbox}}", TakesFullGivenNames = true },
        new("ja", "Japanese", "日本語") {
            CitationTemplate = "{{cite iucn}}", TaxoboxTemplate = "{{生物分類表}}", CiteIucnCopy = CiteIucnDialect.English, TakesFullGivenNames = true,
        },
        new("uk", "Ukrainian", "Українська") {
            CitationTemplate = "{{Cite IUCN}}", TaxoboxTemplate = "{{Speciesbox}}", CiteIucnCopy = Ukrainian, TakesFullGivenNames = true,
        },
        new("pt", "Portuguese", "Português") {
            CitationTemplate = "{{citar iucn}}", TaxoboxTemplate = "{{Info/Taxonomia}}", CiteIucnCopy = Portuguese, TakesFullGivenNames = true,
        },
    ];

    /// The Wikipedia with the code ("fr"), or null.
    public static WikipediaEdition? Find(string? code) =>
        string.IsNullOrWhiteSpace(code) ? null : All.FirstOrDefault(e => string.Equals(e.Code, code.Trim(), StringComparison.OrdinalIgnoreCase));

    /// The citation of the assessment for the wiki, wrapped in <ref> when options.WrapInRef. English
    /// and the wikis with a copy of its {{cite iucn}} (edition.CiteIucnCopy) go through CiteIucnRenderer.
    public static string Citation(WikipediaEdition edition, IucnCitationParts parts, AssessmentFacts facts, CiteIucnOptions options) {
        var template = edition.CiteIucnCopy is { } dialect
            ? CiteIucnRenderer.Render(parts, options with { WrapInRef = false, Dialect = dialect })
            : edition.Code switch {
                "de" => German(parts, facts, options.AccessDate),
                "fr" => French(parts, facts, options.AccessDate),
                "es" => Spanish(parts, options.AccessDate),
                "pl" => Polish(parts, options.AccessDate),
                "zh" => Chinese(parts, options),
                _ => CiteIucnRenderer.Render(parts, options with { WrapInRef = false, Dialect = CiteIucnDialect.English }),
            };
        return Wrap(template, options);
    }

    /// The taxobox status lines for the wiki; null when its taxoboxes have no conservation status (de).
    /// statusRef: the complete reference markup, or null to leave it out (fr and pl have no reference
    /// parameter).
    public static string? TaxoboxLines(WikipediaEdition edition, AssessmentFacts facts, long taxonId, string? statusRef) {
        if (edition.TaxoboxTemplate is null) {
            return null;
        }
        var codeEn = SpeciesboxStatus.ToStatusCode(facts.Category, facts.PossiblyExtinct, facts.PossiblyExtinctInTheWild);
        var system = SpeciesboxStatus.ToStatusSystem(codeEn, facts.CriteriaVersion);
        switch (edition.Code) {
            case "fr": {
                var frCode = codeEn switch { "LR/cd" => "CD", "LR/nt" => "NT", "LR/lc" => "LC", _ => codeEn };
                var criteria = WikitextValue.Clean(facts.Criteria);
                return criteria.Length == 0 ? $"{{{{Taxobox UICN | {frCode} }}}}" : $"{{{{Taxobox UICN | {frCode} | {criteria} }}}}";
            }
            case "pl":
                return MainCategoryCode(codeEn) is { } plCode
                    ? $"| status IUCN = {plCode}\n| IUCN id = {taxonId.ToString(CultureInfo.InvariantCulture)}"
                    : $"| IUCN id = {taxonId.ToString(CultureInfo.InvariantCulture)}";
            case "pt": {
                var sb = new StringBuilder();
                sb.Append("| estado = ").Append(codeEn).Append('\n');
                sb.Append("| sistema_estado = ").Append(system.ToLowerInvariant());
                if (!string.IsNullOrWhiteSpace(statusRef)) {
                    sb.Append("\n| estado_ref = ").Append(statusRef.Trim());
                }
                return sb.ToString();
            }
            case "ja": {
                var jaCode = system == "IUCN2.3" && codeEn is "EX" or "EW" or "CR" or "EN" or "VU" or "DD" ? codeEn + "2.3" : codeEn;
                var sb = new StringBuilder("| status = ").Append(jaCode);
                if (!string.IsNullOrWhiteSpace(statusRef)) {
                    sb.Append("\n| status_ref = ").Append(statusRef.Trim());
                }
                return sb.ToString();
            }
            default:
                // English, es, uk and zh take the English lines.
                return SpeciesboxStatus.Render(facts.Category, facts.PossiblyExtinct, facts.PossiblyExtinctInTheWild, facts.CriteriaVersion, statusRef);
        }
    }

    /// The code in a taxobox that has only the categories EX, EW, CR, EN, VU, NT, LC and DD
    /// (WikipediaEdition.TaxoboxMainCategoriesOnly; the Polish infoboxes), from an English taxobox
    /// code. Such a box has no possibly extinct or Lower Risk categories: PE and PEW are CR, LR/cd and
    /// LR/nt NT, LR/lc LC. Null for a code it cannot show (NE, NA, RE).
    public static string? MainCategoryCode(string taxoboxCode) => taxoboxCode switch {
        "PE" or "PEW" => "CR",
        "LR/cd" or "LR/nt" => "NT",
        "LR/lc" => "LC",
        "EX" or "EW" or "CR" or "EN" or "VU" or "NT" or "LC" or "DD" => taxoboxCode,
        _ => null,
    };

    // {{IUCN |Year= |ID= |ScientificName= |AssessmentID= |YearAssessed= |Assessor= |Abruf=}}, in the
    // order of the doc's example.
    private static string German(IucnCitationParts parts, AssessmentFacts facts, DateOnly? accessed) {
        var p = new List<(string, string)> {
            ("Year", Year(parts.Year)),
            ("ID", parts.TaxonId.ToString(CultureInfo.InvariantCulture)),
            ("ScientificName", PlainName(parts)),
            ("AssessmentID", parts.AssessmentId.ToString(CultureInfo.InvariantCulture)),
        };
        var assessors = InitialsFirst(parts);
        if (assessors.Length > 0) {
            if (facts.AssessedYear is { } assessed) {
                p.Add(("YearAssessed", Year(assessed)));
            }
            p.Add(("Assessor", assessors));
        }
        if (accessed is { } date) {
            p.Add(("Abruf", Iso(date)));
        }
        return Template("IUCN", p);
    }

    // {{UICN|<taxon id>|''Name'' Authority|rang=sous-espèce|consulté le=9 octobre 2026}}
    private static string French(IucnCitationParts parts, AssessmentFacts facts, DateOnly? accessed) {
        var description = ScientificNameMarkup.ToWikitext(NameWithoutAnnotations(parts), WikitextValue.Clean(parts.SubpopulationName));
        var authority = WikitextValue.Clean(facts.Authority);
        if (authority.Length > 0) {
            description += " " + authority;
        }
        var sb = new StringBuilder("{{UICN|").Append(parts.TaxonId.ToString(CultureInfo.InvariantCulture)).Append('|').Append(description);
        var rank = facts.Kind switch { "subspecies" => "sous-espèce", "variety" => "variété", "subpopulation" => "sous-population", _ => null };
        if (rank is not null) {
            sb.Append("|rang=").Append(rank);
        }
        if (accessed is { } date) {
            sb.Append("|consulté le=").Append(date.Day.ToString(CultureInfo.InvariantCulture)).Append(' ')
                .Append(FrenchMonths[date.Month - 1]).Append(' ').Append(date.Year.ToString(CultureInfo.InvariantCulture));
        }
        return sb.Append("}}").ToString();
    }

    // {{IUCN|título=|asesores=|año=|edición=|consultado=9 de octubre de 2026}}
    private static string Spanish(IucnCitationParts parts, DateOnly? accessed) {
        var p = new List<(string, string)> { ("título", PlainName(parts)) };
        var assessors = IucnForm(parts);
        if (assessors.Length > 0) {
            p.Add(("asesores", assessors));
        }
        p.Add(("año", Year(parts.Year)));
        if (RedListVersion(parts.Doi) is { } version) {
            p.Add(("edición", version));
        }
        if (accessed is { } date) {
            p.Add(("consultado", $"{date.Day.ToString(CultureInfo.InvariantCulture)} de {SpanishMonths[date.Month - 1]} de {date.Year.ToString(CultureInfo.InvariantCulture)}"));
        }
        return Template("IUCN", p);
    }

    // {{IUCN |id= |nazwa= |autor= |iucn rok= |wersja= |doi= |data dostępu=}}
    private static string Polish(IucnCitationParts parts, DateOnly? accessed) {
        var p = new List<(string, string)> {
            ("id", parts.TaxonId.ToString(CultureInfo.InvariantCulture)),
            ("nazwa", PlainName(parts)),
        };
        var assessors = InitialsFirst(parts);
        if (assessors.Length > 0) {
            p.Add(("autor", assessors));
        }
        p.Add(("iucn rok", Year(parts.Year)));
        if (RedListVersion(parts.Doi) is { } version) {
            p.Add(("wersja", version));
        }
        if (CiteIucnRenderer.AcceptableDoi(parts.Doi, parts.TaxonId, parts.AssessmentId, parts.ErrataYear is not null) is { } doi) {
            p.Add(("doi", doi));
        }
        if (accessed is { } date) {
            p.Add(("data dostępu", Iso(date)));
        }
        return Template("IUCN", p);
    }

    // {{IUCN |author1= ... |year= |id= |title= |errata=errata version published in 2017 |page= |doi= |access-date=}}
    private static string Chinese(IucnCitationParts parts, CiteIucnOptions options) {
        var rawName = CiteIucnRenderer.StripAnnotations(WikitextValue.Clean(parts.ScientificName), out var errataFromTitle, out var amendsFromTitle, out _);
        var errata = parts.ErrataYear ?? errataFromTitle;
        var amends = errata is null ? parts.AmendsYear ?? amendsFromTitle : null;
        var p = new List<(string Name, string Value)>();
        CiteIucnRenderer.AddAuthors(p, parts, CiteAuthorStyle.AuthorN, options.FullGivenNames, out _);
        // The doc numbers every author, the first one included.
        if (p.Count > 0 && p[0].Name == "author") {
            p[0] = ("author1", p[0].Value);
        }
        p.Add(("year", Year(parts.Year)));
        p.Add(("id", parts.TaxonId.ToString(CultureInfo.InvariantCulture)));
        p.Add(("title", ScientificNameMarkup.ToWikitext(rawName, WikitextValue.Clean(parts.SubpopulationName))));
        if (errata is { } e) {
            p.Add(("errata", $"errata version published in {Year(e)}"));
        }
        if (amends is { } a) {
            p.Add(("amends", $"amended version of {Year(a)} assessment"));
        }
        p.Add(("page", parts.ArticleNumber));
        if (CiteIucnRenderer.AcceptableDoi(parts.Doi, parts.TaxonId, parts.AssessmentId, errata is not null) is { } doi) {
            p.Add(("doi", doi));
        }
        if (options.AccessDate is { } date) {
            p.Add(("access-date", Iso(date)));
        }
        return Template("IUCN", p);
    }

    private static readonly string[] FrenchMonths =
        ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"];
    private static readonly string[] SpanishMonths =
        ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"];

    private static string Year(int year) => year.ToString(CultureInfo.InvariantCulture);
    private static string Iso(DateOnly date) => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    // The scientific name as IUCN writes it, without italics markup or the errata/amended notes:
    // "Panthera leo ssp. persica", "Orcaella brevirostris Songkhla Lake subpopulation".
    private static string PlainName(IucnCitationParts parts) {
        var name = NameWithoutAnnotations(parts);
        var subpopulation = WikitextValue.Clean(parts.SubpopulationName);
        return subpopulation.Length == 0 || name.Contains(subpopulation, StringComparison.Ordinal) ? name : $"{name} {subpopulation}";
    }

    private static string NameWithoutAnnotations(IucnCitationParts parts) =>
        CiteIucnRenderer.StripAnnotations(WikitextValue.Clean(parts.ScientificName), out _, out _, out _);

    // "Ø. Wiig, S. Amstrup, G. Thiemann": a person as initials and surname, others as IUCN writes them.
    private static string InitialsFirst(IucnCitationParts parts) => string.Join(", ", parts.Authors
        .Select(a => a.Kind == CitationAuthorKind.Person && !string.IsNullOrWhiteSpace(a.Last)
            ? $"{WikitextValue.Clean(a.Initials)} {WikitextValue.Clean(a.Last)}".Trim()
            : WikitextValue.Clean(a.Display))
        .Where(s => s.Length > 0));

    // "Wiig, Ø., Amstrup, S. & Thiemann, G.": IUCN's own form.
    private static string IucnForm(IucnCitationParts parts) {
        var names = parts.Authors.Select(a => WikitextValue.Clean(a.Display)).Where(s => s.Length > 0).ToList();
        return names.Count switch {
            0 => string.Empty,
            1 => names[0],
            _ => string.Join(", ", names.Take(names.Count - 1)) + " & " + names[^1],
        };
    }

    /// The Red List version a DOI names ("2015-4" in 10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en), or null.
    public static string? RedListVersion(string? doi) =>
        doi is not null && DoiVersion().Match(doi) is { Success: true } m ? m.Groups["version"].Value : null;

    private static string Template(string name, List<(string Name, string Value)> p) {
        var sb = new StringBuilder("{{").Append(name);
        foreach (var (key, value) in p) {
            sb.Append(" |").Append(key).Append('=').Append(value);
        }
        return sb.Append("}}").ToString();
    }

    private static string Wrap(string template, CiteIucnOptions options) {
        if (!options.WrapInRef) {
            return template;
        }
        var refName = CiteIucnRenderer.SanitizeRefName(options.RefName);
        return refName.Length == 0 ? $"<ref>{template}</ref>" : $"<ref name=\"{refName}\">{template}</ref>";
    }

    [GeneratedRegex(@"IUCN\.UK\.(?<version>\d{4}-\d)\.RLTS", RegexOptions.IgnoreCase)]
    private static partial Regex DoiVersion();
}
