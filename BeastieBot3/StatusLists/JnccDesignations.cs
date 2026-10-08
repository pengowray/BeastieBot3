using System.Globalization;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using ExcelDataReader;

// JNCC's Conservation Designations for UK Taxa: an Excel workbook (8.1 MB in June 2026) whose
// "Master List" sheet has one row per taxon and designation (27,152 rows of 15,211 taxa in the
// 2026-06-09 file): GB and England red lists, Birds of Conservation Concern, Nationally Rare and
// Scarce, the UK and country priority species lists, the Wildlife and Countryside Act and the
// Northern Ireland and habitats legislation, and the international conventions and EU directives
// as they apply to UK taxa. Each taxon is named by its recommended name in the Natural History
// Museum's UK Species Inventory (UKSI), with its taxon version key.
//
// The workbook's name has the date of its release (taxon-designations-20260609.xlsx) and changes
// with each release, so the download reads the current link from the resource page. JNCC has no
// API for its resources and no CSV of the workbook: the page's zip holds the same workbook and two
// PDFs of guidance. Licence: Open Government Licence v3.0, with JNCC's attribution statement,
// which names the year of the release.
//
// Columns left out: Comments (notes of up to 5,126 characters), Source description and
// designation description (descriptions of each list), and Criteria description (IUCN criteria
// codes on some red list rows, sentences on others).

namespace BeastieBot3.StatusLists;

/// One row of the Master List: a taxon and one of its designations. ScientificName is the UKSI
/// recommended name, DesignatedName the name the source published. Kingdom (IUCN's spelling) and
/// Rank are read from JNCC's groups and the form of the name; Scope, Area, StatusCode and
/// Population from the designation (JnccClassification). DesignatedOn: yyyy-MM-dd.
internal sealed record JnccDesignation(
    int RowNumber,
    string TaxonVersionKey,
    string ScientificName,
    string? Authority,
    string? Qualifier,
    string? Rank,
    string? DesignatedName,
    string? CommonName,
    string? Category,
    string? TaxonGroup,
    string? Kingdom,
    string ReportingCategory,
    string? SortCode,
    string Designation,
    string DesignationCode,
    string? StatusCode,
    string? Population,
    string? IucnVersion,
    string? Scope,
    string? Area,
    string? Source,
    string? SourceUrl,
    string? DesignatedOn);

/// The workbook's link on the resource page. FileName is the last part of the URL.
internal sealed record JnccSpreadsheetLink(string Url, string FileName);

internal static partial class JnccDesignations {
    public const string ResourceId = "478f7160-967b-4366-acdf-8941fd33850b";
    public const string ResourcePageUrl = "https://jncc.gov.uk/resources/" + ResourceId;
    public const string DataFolderUrl = "https://data.jncc.gov.uk/data/" + ResourceId + "/";

    public const string Title = "Conservation Designations for UK Taxa, Joint Nature Conservation Committee (JNCC)";
    public const string Licence = "Open Government Licence v3.0";

    public const string SheetName = "Master List";

    /// JNCC's attribution statement, as the resource page gives it, for a release of this year.
    public static string Attribution(int year) =>
        $"Contains JNCC/NE/NRW/NatureScot/NIEA data © copyright and database right {year.ToString(CultureInfo.InvariantCulture)}";

    /// The workbook's link: an href to taxon-designations-<yyyyMMdd>.xlsx (the newest date when the
    /// page has several), else the first .xlsx in the resource's data folder. Null when there is none.
    public static JnccSpreadsheetLink? FindSpreadsheetLink(string html) {
        var links = Href().Matches(html)
            .Select(m => WebUtility.HtmlDecode(m.Groups[1].Value).Trim())
            .Select(href => Uri.TryCreate(new Uri(ResourcePageUrl), href, out var uri) ? uri : null)
            .OfType<Uri>()
            .Where(uri => uri.Scheme is "https" or "http" && uri.AbsolutePath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            .Select(uri => new JnccSpreadsheetLink(uri.AbsoluteUri, Uri.UnescapeDataString(Path.GetFileName(uri.AbsolutePath))))
            .ToList();
        var dated = links
            .Where(l => DatedName().IsMatch(l.FileName) && DateInFileName(l.FileName) is not null)
            .OrderByDescending(l => DateInFileName(l.FileName))
            .FirstOrDefault();
        return dated ?? links.FirstOrDefault(l => l.Url.StartsWith(DataFolderUrl, StringComparison.OrdinalIgnoreCase));
    }

    /// The release date in a file name: the first 8 digits that are a date (yyyyMMdd).
    public static DateOnly? DateInFileName(string path) {
        foreach (Match match in EightDigits().Matches(Path.GetFileName(path))) {
            if (DateOnly.TryParseExact(match.Value, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)) {
                return date;
            }
        }
        return null;
    }

    /// The status_source row for an imported file: the URL it was downloaded from (the link found
    /// on the resource page, else the data folder's URL for a file with JNCC's name, else the
    /// resource page), and the attribution with the year of the release in the file's name.
    public static StatusSourceInfo SourceFor(string file, JnccSpreadsheetLink? downloaded, StatusSourceInfo source) {
        var name = Path.GetFileName(file);
        var url = downloaded is not null && string.Equals(downloaded.FileName, name, StringComparison.Ordinal) ? downloaded.Url
            : DatedName().IsMatch(name) ? DataFolderUrl + Uri.EscapeDataString(name)
            : ResourcePageUrl;
        var year = DateInFileName(name)?.Year ?? source.FetchedAtUtc.Year;
        return source with { Url = url, Citation = Attribution(year) };
    }

    // ---- reading the workbook ----

    // Column headings of the Master List.
    internal const string Category = "Category";
    internal const string TaxonGroup = "Taxon group";
    internal const string RecommendedName = "Recommended taxon name";
    internal const string RecommendedAuthority = "Recommended authority";
    internal const string RecommendedQualifier = "Recommended qualifier";
    internal const string TaxonVersionKey = "Recommended taxon version";
    internal const string DesignatedName = "Designated name";
    internal const string CommonName = "Common name";
    internal const string Source = "Source";
    internal const string SourceUrl = "URL source";
    internal const string DateDesignated = "Date designated";
    internal const string ReportingCategory = "Reporting category";
    internal const string Designation = "Designation";
    internal const string DesignationAbbreviation = "Designation abbreviation";
    internal const string IucnVersion = "IUCN version";
    internal const string Comments = "Comments";
    internal const string SortOrder = "Reporting category sort order";

    private static readonly string[] Required = [TaxonVersionKey, RecommendedName, ReportingCategory, Designation, DesignationAbbreviation];

    // The heading row is searched for in the first rows of the sheet; the 2026 file has a line of
    // text above it.
    private const int HeaderSearchRows = 20;

    /// Reads the Master List sheet. Rows with no taxon version key, name, reporting category,
    /// designation or designation code are skipped and counted. Throws InvalidDataException when the
    /// workbook has no Master List sheet or the sheet has no heading row with the columns needed.
    public static IReadOnlyList<JnccDesignation> Read(Stream stream, out int skipped) {
        skipped = 0;
        // ExcelDataReader's settings ask for Windows-1252 (its fallback for old .xls files), which
        // .NET has only once the code page provider is registered. Registering it again is harmless.
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        using var reader = ExcelReaderFactory.CreateOpenXmlReader(stream);
        do {
            if (string.Equals(reader.Name?.Trim(), SheetName, StringComparison.OrdinalIgnoreCase)) {
                return ReadSheet(reader, out skipped);
            }
        } while (reader.NextResult());
        throw new InvalidDataException($"The workbook has no sheet named \"{SheetName}\".");
    }

    private static List<JnccDesignation> ReadSheet(IExcelDataReader reader, out int skipped) {
        skipped = 0;
        Dictionary<string, int>? columns = null;
        while (columns is null && reader.Depth < HeaderSearchRows && reader.Read()) {
            var headings = Enumerable.Range(0, reader.FieldCount).Select(i => Text(reader.GetValue(i))).ToList();
            if (headings.Any(h => string.Equals(h, TaxonVersionKey, StringComparison.OrdinalIgnoreCase))) {
                columns = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < headings.Count; i++) {
                    if (headings[i] is { } heading) {
                        columns.TryAdd(heading, i);
                    }
                }
            }
        }
        if (columns is null) {
            throw new InvalidDataException($"No heading row with \"{TaxonVersionKey}\" in the first {HeaderSearchRows} rows of \"{SheetName}\".");
        }
        var missing = Required.Where(c => !columns.ContainsKey(c)).ToList();
        if (missing.Count > 0) {
            throw new InvalidDataException($"\"{SheetName}\" has no column {string.Join(", ", missing.Select(c => $"\"{c}\""))}.");
        }

        var rows = new List<JnccDesignation>();
        while (reader.Read()) {
            string? Cell(string heading) => columns.TryGetValue(heading, out var i) && i < reader.FieldCount ? Text(reader.GetValue(i)) : null;
            if (Cell(TaxonVersionKey) is not { } key || Cell(RecommendedName) is not { } name || Cell(ReportingCategory) is not { } reporting
                || Cell(Designation) is not { } designation || Cell(DesignationAbbreviation) is not { } code) {
                if (Enumerable.Range(0, reader.FieldCount).Any(i => Text(reader.GetValue(i)) is not null)) {
                    skipped++;
                }
                continue;
            }
            var source = Cell(Source);
            var category = Cell(Category);
            var group = Cell(TaxonGroup);
            var kingdom = JnccClassification.KingdomOf(category, group);
            var scope = JnccClassification.ScopeOf(code, source, Cell(Comments));
            var (statusCode, population) = JnccClassification.StatusOf(code);
            rows.Add(new JnccDesignation(
                RowNumber: reader.Depth + 1,
                TaxonVersionKey: key,
                ScientificName: name,
                Authority: Cell(RecommendedAuthority),
                Qualifier: Cell(RecommendedQualifier),
                Rank: JnccClassification.RankOf(name, kingdom),
                DesignatedName: Cell(DesignatedName),
                CommonName: Cell(CommonName),
                Category: category,
                TaxonGroup: group,
                Kingdom: kingdom,
                ReportingCategory: reporting,
                SortCode: Cell(SortOrder),
                Designation: designation,
                DesignationCode: code,
                StatusCode: statusCode,
                Population: population,
                IucnVersion: Cell(IucnVersion),
                Scope: scope?.Scope,
                Area: scope?.Area,
                Source: source,
                SourceUrl: Cell(SourceUrl),
                DesignatedOn: columns.TryGetValue(DateDesignated, out var dateColumn) && dateColumn < reader.FieldCount
                    ? DateText(reader.GetValue(dateColumn))
                    : null));
        }
        return rows;
    }

    // A cell as trimmed text; null when empty. Whole numbers have no decimal point (IUCN version 2001).
    private static string? Text(object? value) {
        var text = value switch {
            null or DBNull => null,
            string s => s,
            double d when d == Math.Floor(d) && Math.Abs(d) < 1e15 => ((long)d).ToString(CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            DateTime dt => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString(),
        };
        text = text?.Trim();
        return string.IsNullOrEmpty(text) ? null : text;
    }

    // A date cell as yyyy-MM-dd: a date, a date serial number, or text in ISO or day/month/year form.
    private static string? DateText(object? value) {
        DateTime? date = value switch {
            DateTime dt => dt,
            double d when d is > 0 and < 2958466 => DateTime.FromOADate(d),
            string s when DateTime.TryParseExact(s.Trim(), ["yyyy-MM-dd", "yyyy-MM-ddTHH:mm:ss", "dd/MM/yyyy", "d/M/yyyy"],
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed) => parsed,
            _ => null,
        };
        return date?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    [GeneratedRegex("""href\s*=\s*["']([^"']+)["']""", RegexOptions.IgnoreCase)]
    private static partial Regex Href();

    [GeneratedRegex(@"^taxon-designations-\d{8}\.xlsx$", RegexOptions.IgnoreCase)]
    private static partial Regex DatedName();

    [GeneratedRegex(@"(?<!\d)\d{8}(?!\d)")]
    private static partial Regex EightDigits();
}
