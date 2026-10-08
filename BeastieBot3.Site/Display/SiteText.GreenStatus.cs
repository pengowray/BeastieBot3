using System.Globalization;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

// The species page's IUCN Green Status of Species section and its {{cite iucn}} box.
public static partial class SiteText {
    public const string HeadingGreenStatus = "IUCN Green Status of Species";
    public const string GreenStatusIntro =
        "The Green Status measures how far a species has recovered compared with its population and range before major human impact; it is assessed separately from the Red List assessment.";
    public const string GreenStatusRecoveryCategory = "Species recovery category";
    public const string GreenStatusRecoveryScore = "Species Recovery Score";
    public const string GreenStatusMetricsHeading = "Conservation Impact Metrics";
    public const string GreenStatusColMetric = "Metric";
    public const string GreenStatusColCategory = "Category";
    public const string GreenStatusColScore = "Score";
    public const string GreenStatusLegacy = "Conservation Legacy";
    public const string GreenStatusLegacyHelp = "How much better off the species is now than it would be without past conservation.";
    public const string GreenStatusDependence = "Conservation Dependence";
    public const string GreenStatusDependenceHelp = "How much worse off the species would be in 10 years without current conservation.";
    public const string GreenStatusGain = "Conservation Gain";
    public const string GreenStatusGainHelp = "How much better off the species could be in 10 years with planned conservation.";
    public const string GreenStatusPotential = "Recovery Potential";
    public const string GreenStatusPotentialHelp = "How much the species could recover in the long term (100 years).";
    public const string GreenStatusAssessed = "Date assessed";
    public const string GreenStatusLink = "Green Status on the IUCN Red List website";

    /// The label of a list of people: singular for one name ("Reviewer"), plural for two or more.
    /// IUCN writes one person "Carroll, J." and several "Gupta, G. & McGowan, P.".
    public static string GreenStatusPeople(string role, string names) {
        var several = names.Contains(" & ", StringComparison.Ordinal) || names.Count(c => c == ',') > 1;
        return role switch {
            "assessors" => several ? "Assessors" : "Assessor",
            "reviewers" => several ? "Reviewers" : "Reviewer",
            "facilitators" => several ? "Facilitators" : "Facilitator",
            "contributors" => several ? "Contributors" : "Contributor",
            _ => several ? "Compilers" : "Compiler",
        };
    }

    /// "50% (range 17% to 67%)", "17% (range −42% to 58%)" with a minus sign; the range is written
    /// even when it is the best estimate alone. Just the best estimate when IUCN gives no range;
    /// null without a best estimate.
    public static string? GreenStatusScore(GreenStatusMetric metric) {
        if (metric.Best is not { } best) {
            return null;
        }
        return metric.Min is { } min && metric.Max is { } max
            ? $"{Percent(best)} (range {Percent(min)} to {Percent(max)})"
            : Percent(best);
    }

    private static string Percent(int value) =>
        (value < 0 ? "−" + (-value).ToString(CultureInfo.InvariantCulture) : value.ToString(CultureInfo.InvariantCulture)) + "%";

    // The {{cite iucn}} box in the wikitext section, and its option.
    public const string GreenStatusCiteLabel = "{{cite iucn}} citation of the Green Status assessment";
    public const string GreenStatusCiteNoteYearAssessed = "|year= is the year assessed. IUCN's own citation gives the year published, which can be later.";
    public const string GreenStatusCiteNoteYearPublished = "|year= is a best guess of the year published (see the option below).";
    public const string GreenStatusCiteNoteNumber =
        "No article number or DOI: the number IUCN gives a Green Status assessment changes with every Red List release, and the assessment has no DOI.";
    public const string GreenStatusYearLegend = "Year of the Green Status citation";
    public const string GreenStatusYearAssessed = "Year assessed";
    public const string GreenStatusYearPublished = "Year published (best guess)";
    public const string GreenStatusYearHelp =
        "IUCN's API gives no year published. The best guess is the year of the Red List release the assessment first appeared in, when this site saw that; otherwise the later of the year assessed and the year the Red List assessment on the same page was published.";
}
