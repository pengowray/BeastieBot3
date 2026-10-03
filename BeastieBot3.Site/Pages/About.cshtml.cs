using System.Globalization;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

public sealed class AboutModel : PageModel {
    private readonly SiteDatabase _db;
    private readonly SiteOptions _options;

    public AboutModel(SiteDatabase db, IOptions<SiteOptions> options) {
        _db = db;
        _options = options.Value;
    }

    public string Version { get; private set; } = "?";
    public string? ReleaseYear { get; private set; }
    public string? ApiDateRange { get; private set; }
    public string? GbifVersion { get; private set; }
    public string? ColRelease { get; private set; }
    public string? SpratDate { get; private set; }
    public string? BuiltDate { get; private set; }

    /// The newest date on which `iucn resolve-dois` checked an assessment's DOI, in Crossref's list
    /// or at doi.org, when the database has it.
    public string? DoiCheckedDate { get; private set; }

    /// GBIF's recommended citation of its copy of the IUCN checklist, when the database has it.
    public string? GbifCitation { get; private set; }

    /// https://doi.org/ and the checklist's DOI (the known DOI when the database has none).
    public string GbifUrl { get; private set; } = DoiUrl(SiteText.GbifChecklistDoi);

    /// The Catalogue of Life release's recommended citation, when the database has it.
    public string? ColCitation { get; private set; }

    /// https://doi.org/ and the release's DOI, or the Catalogue of Life website when there is none.
    public string ColUrl { get; private set; } = SiteText.CatalogueOfLifeUrl;
    public string Contact { get; private set; } = SiteText.ContactFallback;
    public string? ContactUrl { get; private set; }

    public void OnGet() {
        var snapshot = _db.Snapshot;
        if (!string.IsNullOrWhiteSpace(_options.ContactText)) {
            Contact = _options.ContactText.Trim();
        }
        ContactUrl = string.IsNullOrWhiteSpace(_options.ContactUrl) ? null : _options.ContactUrl.Trim();
        if (snapshot is null) {
            return;
        }
        Version = snapshot.IucnRelease ?? "?";
        ReleaseYear = Version.Length >= 4 && Version[..4].All(char.IsAsciiDigit) ? Version[..4] : null;
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom), out var from)
            && SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedTo), out var to)) {
            ApiDateRange = SiteFormat.DateRange(from, to);
        }
        GbifVersion = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistPublished), out var gbif)
            ? SiteFormat.Date(gbif)
            : snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistVersion);
        ColRelease = snapshot.Get(SiteDbSchema.MetaKeys.ColRelease);
        GbifCitation = snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistCitation);
        if (snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistDoi) is { } gbifDoi) {
            GbifUrl = DoiUrl(gbifDoi);
        }
        ColCitation = snapshot.Get(SiteDbSchema.MetaKeys.ColCitation);
        if (snapshot.Get(SiteDbSchema.MetaKeys.ColDoi) is { } colDoi) {
            ColUrl = DoiUrl(colDoi);
        }
        SpratDate = SpratReportDate(snapshot.Get(SiteDbSchema.MetaKeys.SpratReport));
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnDoiCheckedTo), out var doiChecked)) {
            DoiCheckedDate = SiteFormat.Date(doiChecked);
        }
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.BuiltAtUtc), out var built)) {
            BuiltDate = SiteFormat.Date(built);
        }
    }

    /// "10.15468/0qnb58" -> "https://doi.org/10.15468/0qnb58". A value that is already a URL is kept.
    internal static string DoiUrl(string doi) {
        var trimmed = doi.Trim();
        return trimmed.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || trimmed.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
            ? trimmed
            : "https://doi.org/" + trimmed;
    }

    /// A citation with links, followed by the source's URL when the citation does not include it.
    public static string CitationHtml(string? citation, string url) {
        if (string.IsNullOrWhiteSpace(citation)) {
            return SiteHtml.Linkify(url);
        }
        var text = citation.Trim();
        return text.Contains(url, StringComparison.OrdinalIgnoreCase)
            ? SiteHtml.Linkify(text)
            : SiteHtml.Linkify(text) + " " + SiteHtml.Linkify(url);
    }

    // SPRAT report files are named "ddMMyyyy-HHmmss-report.csv"; the date is shown when it parses,
    // otherwise the file name.
    internal static string? SpratReportDate(string? fileName) {
        if (string.IsNullOrWhiteSpace(fileName)) {
            return null;
        }
        var name = Path.GetFileName(fileName.Trim());
        if (name.Length >= 8 && DateOnly.TryParseExact(name[..8], "ddMMyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)) {
            return SiteFormat.Date(date);
        }
        return name;
    }
}
