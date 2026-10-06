using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Display;

// The credits of an assessment on the taxon page, under the {{cite iucn}} box. The type labels are
// the headings IUCN's assessment pages use (checked on iucnredlist.org, October 2026).

public static partial class SiteText {
    /// The summary of the credits section. distinctNames: the number of different names, null when
    /// a group is given only in citation form and cannot be counted.
    public static string CreditsSummary(int? distinctNames) => distinctNames switch {
        null => "Credits as given by IUCN",
        1 => "Credits as given by IUCN (1 name)",
        { } n => $"Credits as given by IUCN ({n:N0} names)",
    };

    /// IUCN's heading for a credit type; a type IUCN may add later is shown as IUCN names it.
    public static string CreditTypeLabel(string type) => type switch {
        CreditTypes.Assessor => "Assessor(s)",
        CreditTypes.Evaluator => "Reviewer(s)",
        CreditTypes.Contributor => "Contributor(s)",
        CreditTypes.Facilitators => "Facilitator(s) / Compiler(s)",
        CreditTypes.Institutions => "Partner(s) / Institution(s)",
        _ => type,
    };
}
