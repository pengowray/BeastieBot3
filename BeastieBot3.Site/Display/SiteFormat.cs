using System.Globalization;
using System.Text;

namespace BeastieBot3.Site.Display;

/// Number, date and link formatting shared by the pages.
public static class SiteFormat {
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    /// 179000 -> "179,000".
    public static string Number(long n) => n.ToString("N0", Invariant);

    /// "18 August 2026".
    public static string Date(DateOnly date) => date.ToString("d MMMM yyyy", Invariant);

    /// A 'yyyy-MM-dd' value as "18 August 2026"; anything else is returned as it is.
    public static string Date(string? isoDate) =>
        TryParseDate(isoDate, out var date) ? Date(date) : isoDate ?? string.Empty;

    public static bool TryParseDate(string? text, out DateOnly date) {
        date = default;
        if (string.IsNullOrWhiteSpace(text)) {
            return false;
        }
        var trimmed = text.Trim();
        if (trimmed.Length >= 10 && DateOnly.TryParseExact(trimmed[..10], "yyyy-MM-dd", Invariant, DateTimeStyles.None, out date)) {
            return true;
        }
        return false;
    }

    /// "between 18 August and 1 September 2026", "between 30 December 2025 and 2 January 2026", or
    /// "on 18 August 2026" when both dates are the same.
    public static string DateRange(DateOnly from, DateOnly to) {
        if (to < from) {
            (from, to) = (to, from);
        }
        if (from == to) {
            return "on " + Date(from);
        }
        var start = from.Year == to.Year
            ? from.ToString("d MMMM", Invariant)
            : Date(from);
        return $"between {start} and {Date(to)}";
    }

    /// IUCN writes ranks above genus in capitals ("ANIMALIA"); pages show "Animalia".
    public static string TitleCase(string name) {
        var trimmed = name.Trim();
        if (trimmed.Length == 0) {
            return trimmed;
        }
        return char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    public static string IucnAssessmentUrl(long taxonId, long assessmentId) =>
        $"https://www.iucnredlist.org/species/{taxonId}/{assessmentId}";

    public static string WikipediaUrl(string title) =>
        "https://en.wikipedia.org/wiki/" + EscapePathSegment(title.Trim().Replace(' ', '_'));

    public static string WikispeciesUrl(string title) =>
        "https://species.wikimedia.org/wiki/" + EscapePathSegment(title.Trim().Replace(' ', '_'));

    public static string WikidataUrl(string qid) =>
        "https://www.wikidata.org/wiki/" + EscapePathSegment(qid.Trim());

    public static string CatalogueOfLifeUrl(string colId) =>
        "https://www.catalogueoflife.org/data/taxon/" + EscapePathSegment(colId.Trim());

    public static string SpratUrl(long spratTaxonId) =>
        "https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id=" + spratTaxonId.ToString(Invariant);

    // Percent-encodes a path segment, leaving the characters Wikipedia titles commonly contain and
    // that are safe in a path ( ) , ' ! : _ - . ~ as they are, so links stay readable.
    private static string EscapePathSegment(string text) {
        var sb = new StringBuilder(text.Length);
        foreach (var b in Encoding.UTF8.GetBytes(text)) {
            var c = (char)b;
            if (b < 0x80 && (char.IsAsciiLetterOrDigit(c) || c is '(' or ')' or ',' or '\'' or '!' or ':' or '_' or '-' or '.' or '~')) {
                sb.Append(c);
            } else {
                sb.Append('%').Append(b.ToString("X2", Invariant));
            }
        }
        return sb.ToString();
    }

    private static readonly System.Text.RegularExpressions.Regex ProvisionalPattern = new(
        @"\b(sp|ssp|subsp|var)\.\s*nov\.", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// The marker of a working name of a taxon not yet formally described, as written ("sp. nov."),
    /// and the rank it names; null for any other name. "Notogomphus sp. nov. 'gorilla'", "Hauffenia
    /// sp. nov." (168 taxa in 2026-1). The last marker counts: "Genus sp. nov. ssp. nov." is a subspecies.
    public static (string Marker, string Rank)? ProvisionalMarker(string scientificName) {
        var matches = ProvisionalPattern.Matches(scientificName);
        if (matches.Count == 0) {
            return null;
        }
        var match = matches[^1];
        var rank = match.Groups[1].Value switch {
            "sp" => "species",
            "var" => "variety",
            _ => "subspecies",
        };
        return (match.Value, rank);
    }

    public static bool IsProvisionalName(string scientificName) => ProvisionalMarker(scientificName) is not null;
}
