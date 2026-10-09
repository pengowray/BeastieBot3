using System.Globalization;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Pages;

public sealed class AboutModel : PageModel {
    private readonly SiteDatabase _db;
    private readonly SiteQueries _queries;
    private readonly SiteOptions _options;

    public AboutModel(SiteDatabase db, SiteQueries queries, IOptions<SiteOptions> options) {
        _db = db;
        _queries = queries;
        _options = options.Value;
    }

    /// The national and subnational red lists from GBIF that the site database has, by country.
    public IReadOnlyList<OtherStatusListRow> RedLists { get; private set; } = [];

    public string Version { get; private set; } = "?";
    public string? ReleaseYear { get; private set; }
    public string? ApiDateRange { get; private set; }
    public string? GbifVersion { get; private set; }
    public string? ColRelease { get; private set; }
    /// The Mammal Diversity Database's version ("v2.5") and the date of AmphibiaWeb's names file, when the site has their names.
    public string? MddVersion { get; private set; }
    public string? AmphibiaWebDate { get; private set; }
    /// The Reptile Database's version ("ChecklistBank 1008 (2026-06)"), when the site lists its subspecies.
    public string? ReptileDbVersion { get; private set; }
    public string? SpratDate { get; private set; }
    /// When the site database's copies of NatureServe Explorer and of ECOS were downloaded; null when it has none.
    public string? NatureServeDate { get; private set; }
    public string? EcosDate { get; private set; }
    public string? NztcsDate { get; private set; }
    public string? SalveDate { get; private set; }
    /// JNCC's spreadsheet: when it was downloaded, its date and the attribution JNCC asks for.
    public string? JnccDate { get; private set; }
    /// When the BDC Statuts was downloaded.
    public string? FranceDate { get; private set; }
    /// When Japan's Red List was downloaded.
    public string? JapanDate { get; private set; }
    public string? JnccFileDate { get; private set; }
    public string? JnccAttribution { get; private set; }
    /// The Checklist of CITES Species: when it was downloaded, as a date and as its citation's access date (dd/MM/yyyy).
    public string? CitesDate { get; private set; }
    public int? CitesYear { get; private set; }
    public string? CitesAccessed { get; private set; }
    /// The date SALVE's citation form wants: "08 de out. de 2026".
    public string? SalveAccessed { get; private set; }
    public int? SalveYear { get; private set; }
    /// The year of NatureServeDate, for NatureServe's citation form.
    public int? NatureServeYear { get; private set; }
    public string? BuiltDate { get; private set; }

    /// The Red List versions of IUCN's Table 7 and Table 9 files the site's reasons for change and
    /// Possibly Extinct markers come from ("2007 to 2026-1"); null when the database has none.
    public string? Table7Versions { get; private set; }
    public string? Table9Versions { get; private set; }

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
        RedLists = _queries.GetOtherStatusLists(BeastieBot3.Shared.SiteData.OtherStatusSystems.NationalRedList);
        ReleaseYear = Version.Length >= 4 && Version[..4].All(char.IsAsciiDigit) ? Version[..4] : null;
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom), out var from)
            && SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.IucnApiDownloadedTo), out var to)) {
            ApiDateRange = SiteFormat.DateRange(from, to);
        }
        GbifVersion = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistPublished), out var gbif)
            ? SiteFormat.Date(gbif)
            : snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistVersion);
        ColRelease = snapshot.Get(SiteDbSchema.MetaKeys.ColRelease);
        MddVersion = snapshot.Get(SiteDbSchema.MetaKeys.MddVersion);
        ReptileDbVersion = snapshot.Get(SiteDbSchema.MetaKeys.ReptileDbVersion);
        AmphibiaWebDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.AmphibiaWebVersion), out var aw) ? SiteFormat.Date(aw) : null;
        GbifCitation = snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistCitation);
        if (snapshot.Get(SiteDbSchema.MetaKeys.GbifChecklistDoi) is { } gbifDoi) {
            GbifUrl = DoiUrl(gbifDoi);
        }
        ColCitation = snapshot.Get(SiteDbSchema.MetaKeys.ColCitation);
        if (snapshot.Get(SiteDbSchema.MetaKeys.ColDoi) is { } colDoi) {
            ColUrl = DoiUrl(colDoi);
        }
        SpratDate = SpratReportDate(snapshot.Get(SiteDbSchema.MetaKeys.SpratReport));
        Table7Versions = SiteText.VersionRange(snapshot.Get(SiteDbSchema.MetaKeys.Table7FirstVersion), snapshot.Get(SiteDbSchema.MetaKeys.Table7LastVersion));
        Table9Versions = SiteText.VersionRange(snapshot.Get(SiteDbSchema.MetaKeys.Table9FirstVersion), snapshot.Get(SiteDbSchema.MetaKeys.Table9LastVersion));
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.NatureServeFetched), out var natureServe)) {
            NatureServeDate = SiteFormat.Date(natureServe);
            NatureServeYear = natureServe.Year;
        }
        EcosDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.EcosFetched), out var ecos) ? SiteFormat.Date(ecos) : null;
        NztcsDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.NztcsFetched), out var nztcs) ? SiteFormat.Date(nztcs) : null;
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.SalveFetched), out var salve)) {
            SalveDate = SiteFormat.Date(salve);
            SalveYear = salve.Year;
            string[] months = ["jan.", "fev.", "mar.", "abr.", "maio", "jun.", "jul.", "ago.", "set.", "out.", "nov.", "dez."];
            SalveAccessed = $"{salve.Day:00} de {months[salve.Month - 1]} de {salve.Year}";
        }
        FranceDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.FranceFetched), out var france) ? SiteFormat.Date(france) : null;
        JapanDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.JapanFetched), out var japan) ? SiteFormat.Date(japan) : null;
        JnccDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.JnccFetched), out var jncc) ? SiteFormat.Date(jncc) : null;
        JnccFileDate = SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.JnccFileDate), out var jnccFile) ? SiteFormat.Date(jnccFile) : null;
        JnccAttribution = snapshot.Get(SiteDbSchema.MetaKeys.JnccAttribution);
        if (SiteFormat.TryParseDate(snapshot.Get(SiteDbSchema.MetaKeys.CitesFetched), out var cites)) {
            CitesDate = SiteFormat.Date(cites);
            CitesYear = cites.Year;
            CitesAccessed = cites.ToString("dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
        }
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
