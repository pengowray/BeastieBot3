using System.Globalization;
using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Data;

/// <summary>
/// The ids in an IUCN Red List web address. The host is iucnredlist.org or redlist.org with any
/// subdomain (www., oldredlist., apistaging., maps.), but not nationalredlist.org, whose ids are not
/// IUCN's. Only the current site's /species/ pages have an assessment id. The second number of an
/// earlier site's /details/ address is the region of the assessment shown (Region): 0 for the
/// global assessment, 1 for Europe, 3 for the Mediterranean.
///   https://www.iucnredlist.org/species/22823/14871490, /fr/species/22823/14871490
///   https://www.iucnredlist.org/species/pdf/14871490 (an assessment's PDF: assessment id only)
///   http://www.iucnredlist.org/details/22823/0, /details/22823/all, /details/full/22823/0
///   http://www.iucnredlist.org/apps/redlist/details/22823/0, /apps/redlist/details/22823/0/full
///   http://www.iucnredlist.org/search/details.php/22823/all, /search/details.php?species=22823
///   http://www.iucnredlist.org/search/rangemap.php?species=22823
///   http://maps.iucnredlist.org/map.html?id=22823
///   https://apiv3.iucnredlist.org/api/v3/taxonredirect/22823
/// </summary>
public sealed partial record RedListAddress(long? TaxonId, long? AssessmentId, long? Region) {
    // Longer numbers than this are not ids and would overflow long.
    private const int MaxDigits = 15;

    /// Whether the address is of a regional assessment: an earlier site's /details/ address whose
    /// region is not 0.
    public bool IsRegional => Region is > 0;

    /// The first Red List address in the text, or null.
    public static RedListAddress? Find(string text) {
        var m = Address().Match(text);
        return m.Success ? FromMatch(m) : null;
    }

    /// Every Red List address in the text that names a taxon or an assessment.
    public static IEnumerable<RedListAddress> FindAll(string text) =>
        Address().Matches(text).Select(FromMatch).Where(a => a.TaxonId is not null || a.AssessmentId is not null);

    private static RedListAddress FromMatch(Match m) =>
        new(Id(m, "t"), Id(m, "a"), m.Groups["r"].Success && long.TryParse(m.Groups["r"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var r) ? r : null);

    private static long? Id(Match m, string group) =>
        m.Groups[group] is { Success: true, Value: var digits } && digits.Length <= MaxDigits
            && long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0
            ? n
            : null;

    [GeneratedRegex(
        @"(?<![a-z0-9-])(?:[a-z0-9-]+\.)*(?:iucn)?redlist\.org/+(?:"
        + @"(?:[a-z]{2}(?:-[a-z]+)?/)?species/(?:pdf/(?<a>\d{1,15})|(?<t>\d{1,15})(?:/(?<a>\d{1,15}))?)"
        + @"|(?:apps/redlist/)?details/(?:[a-z]+/)?(?<t>\d{1,15})(?:/(?<r>\d{1,6})(?!\d))?"
        + @"|search/details\.php(?:/|\?(?:[^#\s]*&)?(?:species|id)=)(?<t>\d{1,15})"
        + @"|search/rangemap\.php\?(?:[^#\s]*&)?species=(?<t>\d{1,15})"
        + @"|map\.html\?(?:[^#\s]*&)?id=(?<t>\d{1,15})"
        + @"|api/v\d+/taxonredirect/(?<t>\d{1,15})"
        + @")(?!\d)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Address();
}
