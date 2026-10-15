using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

// The text after a bullet list line that gives the taxon's status in another list
// (ListLineOptions.OtherList), such as " (EPBC Act: {{EPBC status|VU}})" or " (Great Britain Red List:
// Near Threatened)".
//
// English Wikipedia has inline templates for two of the lists (checked 2026-10-16):
// - {{EPBC status}} (Module:EPBC status): EX, EW, CR, EN, VU and CD, the EPBC Act's six categories.
// - {{ESA status}}: shows its first parameter in bold, coloured and with a hover title for e, t, xn, xe,
//   c, e(s/a), t(s/a) and the proposed forms (pe, pt, ...).
// Every other list's statuses are written as text. Wikipedia's taxobox status systems (Module:
// Conservation status: NZTCS, COSEWIC, CITES, NatureServe and others) are for taxoboxes, not list lines.

namespace BeastieBot3.Site.Lists;

public static class OtherStatusLineEnd {
    /// The text after each line, by taxon id, for the taxa with a status in the list.
    public static IReadOnlyDictionary<long, string> Build(OtherStatusChoice choice, IReadOnlyDictionary<long, IReadOnlyList<ChosenStatus>> statuses) =>
        statuses
            .Select(p => (p.Key, Text: For(choice, p.Value)))
            .Where(p => p.Text is not null)
            .ToDictionary(p => p.Key, p => p.Text!);

    /// " (Heading: status; status (population))"; null when there are no statuses.
    public static string? For(OtherStatusChoice choice, IReadOnlyList<ChosenStatus> statuses) {
        if (statuses.Count == 0) {
            return null;
        }
        var parts = statuses
            .Select(s => {
                var status = Template(choice.System, s.Status) ?? s.Status;
                return s.Population is { Length: > 0 } p && p != choice.List.Area ? $"{status} ({p})" : status;
            })
            .Distinct(StringComparer.Ordinal);
        return GroupLineText.OtherStatusLineEnd(choice.Heading, string.Join("; ", parts));
    }

    /// The inline template for a status of a list that has one; null for any other list or status.
    public static string? Template(string system, string status) => system switch {
        OtherStatusSystems.Epbc when EpbcCode(status) is { } code => $"{{{{EPBC status|{code}}}}}",
        OtherStatusSystems.Esa when EsaCode(status) is { } code => $"{{{{ESA status|{code}}}}}",
        _ => null,
    };

    /// The EPBC Act category code of a status as other_status writes it (OtherStatusSystems.EpbcLabel).
    public static string? EpbcCode(string status) => status.Trim().ToUpperInvariant() switch {
        "EXTINCT" => "EX",
        "EXTINCT IN THE WILD" => "EW",
        "CRITICALLY ENDANGERED" => "CR",
        "ENDANGERED" => "EN",
        "VULNERABLE" => "VU",
        "CONSERVATION DEPENDENT" => "CD",
        _ => null,
    };

    /// The {{ESA status}} code of an ECOS listing status.
    public static string? EsaCode(string status) => status.Trim().ToUpperInvariant() switch {
        "ENDANGERED" => "E",
        "THREATENED" => "T",
        "EXPERIMENTAL POPULATION, NON-ESSENTIAL" => "XN",
        "EXPERIMENTAL POPULATION, ESSENTIAL" => "XE",
        "SIMILARITY OF APPEARANCE (ENDANGERED)" => "E(S/A)",
        "SIMILARITY OF APPEARANCE (THREATENED)" => "T(S/A)",
        "PROPOSED ENDANGERED" => "PE",
        "PROPOSED THREATENED" => "PT",
        "CANDIDATE" => "C",
        _ => null,
    };
}
