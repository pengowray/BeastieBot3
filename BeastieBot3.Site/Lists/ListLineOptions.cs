using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Pages;

// What a bullet list line adds after the names: the taxon authority and a reference to the source the
// line comes from. Read from and written to the query string with the other list options
// (GroupListQuery). The keys differ from the species tables' refs and cite, because both sets of
// options are in one form:
//
//   auth    small | plain       the authority after the scientific name (none when absent)
//   lrefs   list | inline       a reference after each line: list-defined in a {{reflist|refs=}} at the
//                               end, or the full citation in the line (none when absent)
//   lcite   q                   {{cite Q}} for an IUCN assessment that has a Wikidata item (IucnReference);
//                               {{cite iucn}} when absent

namespace BeastieBot3.Site.Lists;

public enum ListReferenceMode {
    None,
    /// <ref name="..."/> after the line, the citations in a {{reflist|refs=}} at the end.
    ListDefined,
    /// <ref name="...">citation</ref> after the line.
    Inline,
}

public sealed record ListLineOptions {
    public static readonly ListLineOptions Default = new();

    public SpeciesListAuthority Authority { get; init; } = SpeciesListAuthority.None;
    public ListReferenceMode References { get; init; } = ListReferenceMode.None;
    public ReferenceTemplate Template { get; init; } = ReferenceTemplate.CiteIucn;

    public bool HasReferences => References != ListReferenceMode.None;

    public static readonly string[] Keys = ["auth", "lrefs", "lcite"];

    public static ListLineOptions Read(IQueryCollection query) => new() {
        Authority = Last(query, "auth") switch {
            "small" => SpeciesListAuthority.Small,
            "plain" => SpeciesListAuthority.Plain,
            _ => SpeciesListAuthority.None,
        },
        References = Last(query, "lrefs") switch {
            "list" => ListReferenceMode.ListDefined,
            "inline" => ListReferenceMode.Inline,
            _ => ListReferenceMode.None,
        },
        Template = IucnReference.FromQuery(Last(query, "lcite")),
    };

    /// The query string parts that differ from the defaults.
    public IEnumerable<string> Write() {
        if (Authority != SpeciesListAuthority.None) {
            yield return "auth=" + AuthorityKey(Authority);
        }
        if (References != ListReferenceMode.None) {
            yield return "lrefs=" + ReferencesKey(References);
        }
        if (Template != ReferenceTemplate.CiteIucn) {
            yield return "lcite=" + IucnReference.QueryValue(Template);
        }
    }

    public static string AuthorityKey(SpeciesListAuthority value) => value switch {
        SpeciesListAuthority.Small => "small",
        SpeciesListAuthority.Plain => "plain",
        _ => "none",
    };

    public static string ReferencesKey(ListReferenceMode value) => value switch {
        ListReferenceMode.ListDefined => "list",
        ListReferenceMode.Inline => "inline",
        _ => "none",
    };

    // The last value: a form sends a hidden value before a checkbox's.
    private static string? Last(IQueryCollection query, string key) =>
        query.TryGetValue(key, out var values) && values.Count > 0 ? values[^1]?.Trim() : null;
}
