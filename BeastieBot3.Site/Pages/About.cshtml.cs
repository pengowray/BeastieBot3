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
        SpratDate = SpratReportDate(snapshot.Get(SiteDbSchema.MetaKeys.SpratReport));
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.BuiltAtUtc), out var built)) {
            BuiltDate = SiteFormat.Date(built);
        }
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
