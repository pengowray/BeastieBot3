using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Xml.Linq;
using CsvHelper;
using CsvHelper.Configuration;

// Reads a Darwin Core Archive (DwC-A): meta.xml says which file holds the core rows and each
// extension's rows, how its fields are separated and quoted, how many header lines to skip, its
// encoding, and which column holds which term. Nothing is assumed about column order.
//
// Terms are matched by their local name ("threatStatus" for http://iucn.org/terms/threatStatus,
// "taxonID" for http://rs.tdwg.org/dwc/terms/taxonID), because archives use different namespaces
// for the same term. A field with a default and no index gives every row that value.
//
// Every archive seen so far is tab-separated with fieldsEnclosedBy='' (no quoting at all), so a
// stray quote in a name must not start a quoted field: an empty enclosure reads in CsvHelper's
// NoEscape mode. Line ends (\n, \r\n or \r) are recognised by CsvHelper whatever linesTerminatedBy says.

namespace BeastieBot3.StatusLists;

/// One data file of an archive: the core or an extension.
/// RowType: the local name of the row type ("Taxon", "Distribution"). Fields: term local name to
/// column index. Defaults: term local name to the value every row has.
internal sealed record DwcFile(
    string RowType,
    string Location,
    string Delimiter,
    char? Quote,
    int HeaderLines,
    Encoding Encoding,
    int IdIndex,
    IReadOnlyDictionary<string, int> Fields,
    IReadOnlyDictionary<string, string> Defaults);

internal sealed record DwcMeta(DwcFile Core, IReadOnlyList<DwcFile> Extensions) {
    public DwcFile? Extension(string rowType) =>
        Extensions.FirstOrDefault(e => string.Equals(e.RowType, rowType, StringComparison.OrdinalIgnoreCase));
}

/// One row: its id (the core's id or an extension's coreid) and its terms. A value is trimmed, and
/// an empty value is null.
internal sealed class DwcRow {
    private readonly string[] _values;
    private readonly DwcFile _file;

    internal DwcRow(DwcFile file, string[] values) {
        _file = file;
        _values = values;
    }

    public string? Id => Value(_file.IdIndex);

    public string? this[string term] =>
        _file.Fields.TryGetValue(term, out var index) ? Value(index)
        : _file.Defaults.TryGetValue(term, out var value) && value.Trim().Length > 0 ? value.Trim()
        : null;

    private string? Value(int index) =>
        index >= 0 && index < _values.Length && _values[index].Trim() is { Length: > 0 } value ? value : null;
}

internal static class DwcArchive {
    private static readonly XNamespace Text = "http://rs.tdwg.org/dwc/text/";

    static DwcArchive() {
        // Legacy encodings named in meta.xml ("windows-1252") need the code pages provider.
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    public static DwcMeta ReadMeta(ZipArchive zip) {
        var entry = FindEntry(zip, "meta.xml") ?? throw new InvalidDataException("The archive has no meta.xml.");
        using var stream = entry.Open();
        return ReadMeta(XDocument.Load(stream));
    }

    public static DwcMeta ReadMeta(string xml) => ReadMeta(XDocument.Parse(xml));

    private static DwcMeta ReadMeta(XDocument document) {
        var root = document.Root ?? throw new InvalidDataException("meta.xml is empty.");
        var core = root.Element(Text + "core") ?? throw new InvalidDataException("meta.xml has no core.");
        return new DwcMeta(ReadFile(core, "id"), root.Elements(Text + "extension").Select(e => ReadFile(e, "coreid")).ToList());
    }

    private static DwcFile ReadFile(XElement element, string idElement) {
        var location = element.Element(Text + "files")?.Element(Text + "location")?.Value.Trim();
        if (string.IsNullOrEmpty(location)) {
            throw new InvalidDataException($"meta.xml gives no file for {element.Attribute("rowType")?.Value}.");
        }
        var fields = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var defaults = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in element.Elements(Text + "field")) {
            var term = LocalName(field.Attribute("term")?.Value);
            if (term is null) {
                continue;
            }
            if (int.TryParse(field.Attribute("index")?.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)) {
                fields.TryAdd(term, index);
            } else if (field.Attribute("default")?.Value is { } value) {
                defaults.TryAdd(term, value);
            }
        }
        var idIndex = int.TryParse(element.Element(Text + idElement)?.Attribute("index")?.Value, NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var id) ? id : -1;
        var quote = Unescape(element.Attribute("fieldsEnclosedBy")?.Value ?? "\"");
        return new DwcFile(
            LocalName(element.Attribute("rowType")?.Value) ?? "",
            location,
            Unescape(element.Attribute("fieldsTerminatedBy")?.Value ?? ","),
            quote.Length == 0 ? null : quote[0],
            int.TryParse(element.Attribute("ignoreHeaderLines")?.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var header) ? header : 0,
            EncodingOf(element.Attribute("encoding")?.Value),
            idIndex,
            fields,
            defaults);
    }

    /// The part of a term URI after its last '/' or '#': "threatStatus" for http://iucn.org/terms/threatStatus.
    internal static string? LocalName(string? uri) {
        if (string.IsNullOrWhiteSpace(uri)) {
            return null;
        }
        var trimmed = uri.Trim().TrimEnd('/');
        var cut = trimmed.LastIndexOfAny(['/', '#']);
        return cut >= 0 ? trimmed[(cut + 1)..] : trimmed;
    }

    /// meta.xml writes control characters as escapes: "\t", "\n", "\r".
    internal static string Unescape(string value) =>
        value.Replace("\\t", "\t", StringComparison.Ordinal).Replace("\\n", "\n", StringComparison.Ordinal)
            .Replace("\\r", "\r", StringComparison.Ordinal);

    private static Encoding EncodingOf(string? name) {
        if (string.IsNullOrWhiteSpace(name) || name.Replace("-", "", StringComparison.Ordinal).Equals("UTF8", StringComparison.OrdinalIgnoreCase)) {
            return new UTF8Encoding(false);
        }
        try {
            return Encoding.GetEncoding(name.Trim());
        } catch (ArgumentException) {
            return new UTF8Encoding(false);
        }
    }

    /// The rows of one data file of the archive.
    public static IEnumerable<DwcRow> ReadRows(ZipArchive zip, DwcFile file) {
        var entry = FindEntry(zip, file.Location) ?? throw new InvalidDataException($"{file.Location} is not in the archive.");
        using var reader = new StreamReader(entry.Open(), file.Encoding, detectEncodingFromByteOrderMarks: true);
        foreach (var row in ReadRows(reader, file)) {
            yield return row;
        }
    }

    internal static IEnumerable<DwcRow> ReadRows(TextReader reader, DwcFile file) {
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture) {
            Delimiter = file.Delimiter,
            HasHeaderRecord = false,
            BadDataFound = null,
            MissingFieldFound = null,
            DetectColumnCountChanges = false,
            IgnoreBlankLines = true,
            Mode = file.Quote is null ? CsvMode.NoEscape : CsvMode.RFC4180,
        };
        if (file.Quote is { } quote) {
            configuration.Quote = quote;
        }
        using var parser = new CsvParser(reader, configuration);
        var skipped = 0;
        while (parser.Read()) {
            if (skipped < file.HeaderLines) {
                skipped++;
                continue;
            }
            if (parser.Record is { } record) {
                yield return new DwcRow(file, record);
            }
        }
    }

    // A data file's location is relative to meta.xml, which can sit in a folder inside the zip.
    private static ZipArchiveEntry? FindEntry(ZipArchive zip, string location) =>
        zip.GetEntry(location)
        ?? zip.Entries.FirstOrDefault(e => e.FullName.EndsWith("/" + location, StringComparison.OrdinalIgnoreCase))
        ?? zip.Entries.FirstOrDefault(e => e.FullName.Equals(location, StringComparison.OrdinalIgnoreCase));
}
