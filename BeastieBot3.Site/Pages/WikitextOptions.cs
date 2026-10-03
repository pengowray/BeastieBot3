using System.Globalization;
using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Pages;

/// The citation options form on a taxon page, read from and written to the query string:
///   authors=author|lastfirst   fullnames=1   access=download|today|none   ref=1   refname=...   amp=1   opts=1
/// Unticked checkboxes are not sent by the browser, so the form also sends opts=1: with it, a
/// missing ref or amp means "off" and a missing or empty refname means a plain <ref>; without it (a
/// plain link) the defaults apply. fullnames is off by default, so it needs no opts=1: fullnames=1
/// turns it on and anything else leaves it off. An empty value and a missing one always mean the
/// same thing, because the output cache cannot tell them apart.
///
/// DefaultRefName is the default ref name of the assessment the page shows (DefaultRefNames). A
/// RefName equal to it is the default, not the visitor's choice, so links to other assessments
/// use their own default instead of carrying it along.
public sealed record WikitextOptions(CiteAuthorStyle AuthorStyle, string Access, bool WrapInRef, string RefName, bool Amp, string DefaultRefName) {
    public const string AccessDownload = "download";
    public const string AccessToday = "today";
    public const string AccessNone = "none";
    public const int MaxRefNameLength = 40;

    public static readonly WikitextOptions Default =
        new(CiteAuthorStyle.AuthorN, AccessDownload, WrapInRef: true, DefaultRefNames.LatestGlobal, Amp: false, DefaultRefNames.LatestGlobal);

    /// Full given names where IUCN lists them, instead of initials (CiteIucnOptions.FullGivenNames).
    public bool FullGivenNames { get; init; }

    public static WikitextOptions FromQuery(string? authors, string? access, string? opts, string? wrapRef, string? refName, string? amp,
        string defaultRefName = DefaultRefNames.LatestGlobal, string? fullNames = null) {
        var formSent = opts == "1";
        var style = string.Equals(authors, "lastfirst", StringComparison.OrdinalIgnoreCase) ? CiteAuthorStyle.LastFirst : CiteAuthorStyle.AuthorN;
        var accessValue = access?.Trim().ToLowerInvariant() switch {
            AccessToday => AccessToday,
            AccessNone => AccessNone,
            _ => AccessDownload,
        };
        var name = string.IsNullOrWhiteSpace(refName) ? (formSent ? string.Empty : defaultRefName) : refName.Trim();
        if (name.Length > MaxRefNameLength) {
            name = name[..MaxRefNameLength];
        }
        return new WikitextOptions(
            style,
            accessValue,
            formSent ? wrapRef == "1" : Default.WrapInRef,
            name,
            formSent ? amp == "1" : Default.Amp,
            defaultRefName) {
            FullGivenNames = fullNames == "1",
        };
    }

    /// The |access-date= these options give: today (the server's UTC date), the date the assessment
    /// was downloaded from IUCN (null when that date is not known), or none.
    public DateOnly? AccessDate(DateOnly today, DateOnly? downloaded) => Access switch {
        AccessToday => today,
        AccessNone => null,
        _ => downloaded,
    };

    public CiteIucnOptions ToCiteIucnOptions(DateOnly today, DateOnly? downloaded) => new() {
        AuthorStyle = AuthorStyle,
        AccessDate = AccessDate(today, downloaded),
        WrapInRef = WrapInRef,
        RefName = RefName,
        NameListStyleAmp = Amp,
        FullGivenNames = FullGivenNames,
    };

    /// {{cite Q}} takes the same ref options as {{cite iucn}}, and the access date when the item has a
    /// URL (WikidataCite sets ItemHasUrl). Its authors come from the Wikidata item, so the author
    /// options do not apply.
    public CiteQOptions ToCiteQOptions(DateOnly today, DateOnly? downloaded) => new() {
        AccessDate = AccessDate(today, downloaded),
        WrapInRef = WrapInRef,
        RefName = RefName,
    };

    /// The ref name the visitor chose, or null when it is this page's default.
    public string? CustomRefName => RefName == DefaultRefName ? null : RefName;

    /// The query string that reproduces these options (only what differs from the defaults) on the
    /// page for an assessment whose default ref name is targetDefaultRefName, starting with "?", or
    /// empty. assessmentId is the assessment to show, or null for the page's default one.
    public string ToQuery(long? assessmentId, string targetDefaultRefName) {
        var parts = new List<string>();
        if (assessmentId is { } id) {
            parts.Add("assessment=" + id.ToString(CultureInfo.InvariantCulture));
        }
        if (AuthorStyle == CiteAuthorStyle.LastFirst) {
            parts.Add("authors=lastfirst");
        }
        if (FullGivenNames) {
            parts.Add("fullnames=1");
        }
        if (Access != AccessDownload) {
            parts.Add("access=" + Access);
        }
        var name = CustomRefName ?? targetDefaultRefName;
        if (WrapInRef != Default.WrapInRef || Amp != Default.Amp || name != targetDefaultRefName) {
            parts.Add("opts=1");
            if (WrapInRef) {
                parts.Add("ref=1");
            }
            if (Amp) {
                parts.Add("amp=1");
            }
            // With opts=1 a missing refname means a plain <ref>, so the name is always written,
            // the target's default included.
            if (name.Length > 0) {
                parts.Add("refname=" + Uri.EscapeDataString(name));
            }
        }
        return parts.Count == 0 ? string.Empty : "?" + string.Join('&', parts);
    }
}

/// The default ref name of an assessment's citation. The latest global assessment gets "iucn", the
/// name most species articles already give their IUCN citation, so its citation can replace that
/// one. Any other assessment is cited beside it, so its name must differ: "iucn2008" for an earlier
/// global assessment ("iucn2010-5539282", with the assessment id, when the taxon has two global
/// assessments published that year), and "iucn-gulf-of-mexico" for a regional one.
public static class DefaultRefNames {
    public const string LatestGlobal = "iucn";

    public static string For(AssessmentRow assessment, long? latestGlobalId, IReadOnlyList<AssessmentRow> globalHistory) {
        var id = assessment.AssessmentId.ToString(CultureInfo.InvariantCulture);
        if (!assessment.IsGlobal) {
            var slug = Slug(assessment.Scope);
            var name = slug.Length == 0 ? "iucn-" + id : "iucn-" + slug;
            return name.Length > WikitextOptions.MaxRefNameLength ? name[..WikitextOptions.MaxRefNameLength].TrimEnd('-') : name;
        }
        if (assessment.AssessmentId == latestGlobalId) {
            return LatestGlobal;
        }
        if (assessment.YearPublished is not { } year) {
            return "iucn-" + id;
        }
        var yearText = year.ToString(CultureInfo.InvariantCulture);
        var sameYear = globalHistory.Count(a => a.YearPublished == year);
        return sameYear > 1 ? $"iucn{yearText}-{id}" : "iucn" + yearText;
    }

    /// "Gulf of Mexico" -> "gulf-of-mexico", "S. Africa FW" -> "s-africa-fw": lower case, accents
    /// removed, and every run of other characters a single hyphen.
    public static string Slug(string text) {
        var sb = new StringBuilder(text.Length);
        var hyphen = false;
        foreach (var c in text.Normalize(NormalizationForm.FormD)) {
            if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark) {
                continue;
            }
            if (char.IsLetterOrDigit(c)) {
                if (hyphen && sb.Length > 0) {
                    sb.Append('-');
                }
                hyphen = false;
                sb.Append(char.ToLowerInvariant(c));
            } else {
                hyphen = true;
            }
        }
        return sb.ToString().Normalize(NormalizationForm.FormC);
    }
}
