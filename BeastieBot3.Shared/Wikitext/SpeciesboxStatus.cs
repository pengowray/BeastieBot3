using System.Text;

namespace BeastieBot3.Shared.Wikitext;

// The conservation status lines of {{Speciesbox}}, {{Taxobox}} and the other automatic taxoboxes.
//
// Evidence (fetched from en.wikipedia.org on 2026-10-03):
// - Template:Taxobox/species passes |status_system= and |status= to Module:Conservation status
//   (revision of 2026-09-04 by Ahecht), which upper-cases both before comparing.
// - Under status_system=IUCN3.1 the module accepts EX, EW, CR, EN, VU, NT, LC, DD, NE, NR, NA, PE and
//   PEW. PE renders the CR image with "Critically endangered, possibly extinct", PEW with "..., possibly
//   extinct in the wild"; both add Category:IUCN Red List critically endangered species. Any other
//   code, LR/cd, LR/nt and LR/lc included, renders "Invalid status" and adds Category:Invalid
//   conservation status.
// - Under status_system=IUCN2.3 it accepts EX, EW, CR, EN, VU, LR/cd (or CD), LR/nt (or NT),
//   LR/lc (or LC), DD, NE, NR, PE and PEW, with the IUCN 2.3 images. NA is not accepted there.
// - Wikipedia:Conservation status lists the same codes per system and gives LR/cd, LR/nt and LR/lc in
//   that case; Template:Taxobox/doc's table of IUCN statuses gives the same spellings.
// - Articles use PE and PEW: 386 mainspace pages match `status = PE` and 27 `status = PEW` (search,
//   2026-10-03), e.g. Baiji and Eskimo curlew (PE, IUCN3.1), Panamanian golden frog (PEW, IUCN3.1), and
//   Spix's macaw before its 2019 reassessment as EW (revision 843194988, May 2018: PEW, IUCN3.1).
//   The IUCN flags match those articles (Baiji and Eskimo curlew possiblyExtinct, the golden frog
//   possiblyExtinctInTheWild); Ivory-billed woodpecker shows CR, and its 2020 assessment has neither
//   flag. A caller that wants plain CR for a flagged assessment passes false for both flags.
// - Assessments made under criteria version 2.3 use status_system=IUCN2.3 in articles (Arrau turtle,
//   Wodyetia: LR/cd), and the LR categories only exist there.

public static class SpeciesboxStatus {
    /// The status lines of a {{Speciesbox}} or {{Taxobox}}:
    /// "| status = EN\n| status_system = IUCN3.1\n| status_ref = <ref>...</ref>".
    /// statusRef is the complete reference markup, or null to leave status_ref out.
    /// CR with the possibly extinct flags becomes PE or PEW. The system is IUCN2.3 for the LR
    /// categories and for criteria version 2.x, IUCN3.1 otherwise; NA and RE have no IUCN2.3 code and
    /// stay on IUCN3.1. Regional statuses (RE) are not accepted by either system: the taxobox shows
    /// the global status, so callers should pass a global assessment.
    public static string Render(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild,
        string? criteriaVersion, string? statusRef) {
        var code = ToStatusCode(category, possiblyExtinct, possiblyExtinctInTheWild);
        var system = ToStatusSystem(code, criteriaVersion);

        var sb = new StringBuilder();
        sb.Append("| status = ").Append(code).Append('\n');
        sb.Append("| status_system = ").Append(system);
        if (!string.IsNullOrWhiteSpace(statusRef)) {
            sb.Append('\n').Append("| status_ref = ").Append(statusRef.Trim());
        }
        return sb.ToString();
    }

    // The taxobox code: the {{IUCN status}} code, except that the module spells the possibly extinct
    // forms PE and PEW rather than CR(PE) and CR(PEW).
    private static string ToStatusCode(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild) {
        var code = IucnStatusTemplate.ToTemplateCode(category.Trim(), possiblyExtinct, possiblyExtinctInTheWild);
        return code switch {
            "CR(PE)" => "PE",
            "CR(PEW)" => "PEW",
            _ => code,
        };
    }

    private static string ToStatusSystem(string code, string? criteriaVersion) {
        if (code.StartsWith("LR/", StringComparison.OrdinalIgnoreCase)) {
            return "IUCN2.3";
        }
        if (code is "NA" or "RE") {
            return "IUCN3.1";
        }
        var version = criteriaVersion?.Trim();
        return version is not null && version.StartsWith("2.", StringComparison.Ordinal) ? "IUCN2.3" : "IUCN3.1";
    }
}
