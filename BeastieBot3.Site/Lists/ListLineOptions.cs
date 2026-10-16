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
//   other   a list key          another list (OtherStatusChoices key: "au-epbc", "jncc-redlist-gb");
//                               none when absent
//   oline   0                   no status from the list in `other` after each line (the status is added
//                               when absent; species tables never show it)
//   oonly   1                   only the taxa with a status in the list in `other` (all taxa when absent)
//   oskip   a status            a status in the list whose taxa oonly leaves out
//                               (OtherStatusFilter.Key: the status as the list writes it, NatureServe's
//                               rounded global rank); repeated. Only the left-out statuses are kept, so a
//                               status left out stays left out on another group's page, and every
//                               status is in when there is none. The form sends instead each status it
//                               shows (oshown, hidden) and the ticked ones (ostatus); the left-out
//                               statuses are then the shown ones not ticked, and any oskip not shown.

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
    /// The OtherStatusChoices key of the list whose status goes after each line; null for none.
    public string? OtherList { get; init; }
    /// Whether each bulleted line ends with the taxon's status in OtherList.
    public bool OtherOnLines { get; init; } = true;
    /// Whether the list has only the taxa with a status in OtherList.
    public bool OtherOnly { get; init; }
    /// The statuses (OtherStatusFilter.Key) whose taxa OtherOnly leaves out.
    public IReadOnlySet<string> OtherSkip { get; init; } = new HashSet<string>(StringComparer.Ordinal);

    public bool HasReferences => References != ListReferenceMode.None;
    /// Whether a list is chosen and the list maker lists only the taxa with a status in it.
    public bool OnlyOtherListed => OtherList is not null && OtherOnly;
    /// Whether a list is chosen and each bulleted line ends with the taxon's status in it.
    public bool OtherStatusOnLines => OtherList is not null && OtherOnLines;

    public static readonly string[] Keys = ["auth", "lrefs", "lcite", "other", "oline", "oonly", "oskip", "ostatus", "oshown"];

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
        OtherList = Last(query, "other") is { Length: > 0 } other ? other : null,
        OtherOnLines = Last(query, "oline") != "0",
        OtherOnly = Last(query, "oonly") == "1",
        OtherSkip = ReadSkip(query),
    };

    private static HashSet<string> ReadSkip(IQueryCollection query) {
        var skip = Values(query, "oskip").ToHashSet(StringComparer.Ordinal);
        if (query.ContainsKey("oshown")) {
            var shown = Values(query, "oshown").ToHashSet(StringComparer.Ordinal);
            var ticked = Values(query, "ostatus").ToHashSet(StringComparer.Ordinal);
            skip.ExceptWith(shown);
            skip.UnionWith(shown.Where(s => !ticked.Contains(s)));
        }
        return skip;
    }

    private static IEnumerable<string> Values(IQueryCollection query, string key) =>
        query.TryGetValue(key, out var values) ? values.Where(v => v is { Length: > 0 }).Select(v => v!.Trim()) : [];

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
        if (OtherList is not null) {
            yield return "other=" + Uri.EscapeDataString(OtherList);
            if (!OtherOnLines) {
                yield return "oline=0";
            }
            if (OtherOnly) {
                yield return "oonly=1";
            }
            foreach (var status in OtherSkip.Order(StringComparer.Ordinal)) {
                yield return "oskip=" + Uri.EscapeDataString(status);
            }
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
