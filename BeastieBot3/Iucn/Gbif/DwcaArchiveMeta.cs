using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

// Reads a Darwin Core Archive's meta.xml (https://dwc.tdwg.org/text/): which file is the core and
// which are extensions, how each file is delimited, how many header lines to skip, and which
// column holds each term. Readers look columns up by term, never by position, because the column
// order is the publisher's choice and can change between releases.

namespace BeastieBot3.Iucn.Gbif;

/// One data file (the core or an extension) as meta.xml describes it.
internal sealed class DwcaFileDescriptor {
    private readonly Dictionary<string, int> _indexByTerm;
    private readonly Dictionary<string, int> _indexByLocalName;
    private readonly Dictionary<string, string> _defaultByTerm;
    private readonly Dictionary<string, string> _defaultByLocalName;

    public DwcaFileDescriptor(
        bool isCore,
        string rowType,
        string location,
        string fieldDelimiter,
        string? quote,
        int ignoreHeaderLines,
        Encoding encoding,
        int? idIndex,
        IReadOnlyList<(string Term, int? Index, string? Default)> fields) {
        IsCore = isCore;
        RowType = rowType;
        Location = location;
        FieldDelimiter = fieldDelimiter;
        Quote = quote;
        IgnoreHeaderLines = ignoreHeaderLines;
        Encoding = encoding;
        IdIndex = idIndex;
        Fields = fields;

        _indexByTerm = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        _defaultByTerm = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (term, index, value) in fields) {
            if (index is int i) {
                _indexByTerm.TryAdd(term, i);
            } else if (value is not null) {
                _defaultByTerm.TryAdd(term, value);
            }
        }
        _indexByLocalName = UniqueByLocalName(_indexByTerm);
        _defaultByLocalName = UniqueByLocalName(_defaultByTerm);
    }

    public bool IsCore { get; }

    /// The row type URI, e.g. "http://rs.tdwg.org/dwc/terms/Taxon".
    public string RowType { get; }

    /// The file's path inside the archive, e.g. "taxon.txt".
    public string Location { get; }

    public string FieldDelimiter { get; }

    /// The character that encloses quoted fields, or null when fields are never quoted.
    public string? Quote { get; }

    public int IgnoreHeaderLines { get; }

    public Encoding Encoding { get; }

    /// The column of the row id: &lt;id&gt; in the core, &lt;coreid&gt; (the core row it belongs to) in an extension.
    public int? IdIndex { get; }

    public IReadOnlyList<(string Term, int? Index, string? Default)> Fields { get; }

    /// The last part of the row type URI: "Taxon", "Distribution", "VernacularName".
    public string RowTypeName => DwcaArchiveMeta.LocalName(RowType);

    /// <summary>
    /// The column holding <paramref name="term"/>. Accepts the full term URI, or its last part
    /// ("language") when only one of the file's terms ends that way.
    /// </summary>
    public int? IndexOf(string term) =>
        _indexByTerm.TryGetValue(term, out var index) ? index
        : _indexByLocalName.TryGetValue(DwcaArchiveMeta.LocalName(term), out index) ? index
        : null;

    public bool HasTerm(string term) => IndexOf(term) is not null || DefaultOf(term) is not null;

    /// The value meta.xml gives a term that has a default and no column.
    public string? DefaultOf(string term) =>
        _defaultByTerm.TryGetValue(term, out var value) ? value
        : _defaultByLocalName.TryGetValue(DwcaArchiveMeta.LocalName(term), out value) ? value
        : null;

    /// <summary>
    /// The term's value in one row: the column's text, trimmed, or the term's default when the
    /// column is empty or the term has no column. Null when neither has a value.
    /// </summary>
    public string? Value(IReadOnlyList<string?> row, string term) {
        if (IndexOf(term) is int index && index < row.Count) {
            var text = row[index]?.Trim();
            if (!string.IsNullOrEmpty(text)) {
                return text;
            }
        }
        return DefaultOf(term);
    }

    /// The row's id (core) or core id (extension), trimmed. Null when the row has no such column.
    public string? Id(IReadOnlyList<string?> row) =>
        IdIndex is int index && index < row.Count && row[index]?.Trim() is { Length: > 0 } id ? id : null;

    private static Dictionary<string, int> UniqueByLocalName(Dictionary<string, int> byTerm) =>
        byTerm
            .GroupBy(kv => DwcaArchiveMeta.LocalName(kv.Key), StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() == 1)
            .ToDictionary(g => g.Key, g => g.Single().Value, StringComparer.OrdinalIgnoreCase);

    private static Dictionary<string, string> UniqueByLocalName(Dictionary<string, string> byTerm) =>
        byTerm
            .GroupBy(kv => DwcaArchiveMeta.LocalName(kv.Key), StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() == 1)
            .ToDictionary(g => g.Key, g => g.Single().Value, StringComparer.OrdinalIgnoreCase);
}

/// The core and extension files of a Darwin Core Archive, and the metadata file it names.
internal sealed record DwcaArchiveMeta(
    DwcaFileDescriptor Core,
    IReadOnlyList<DwcaFileDescriptor> Extensions,
    string? MetadataLocation) {

    /// <summary>
    /// The extension whose row type ends with <paramref name="rowTypeName"/> ("Distribution",
    /// "VernacularName"), or null when the archive has none.
    /// </summary>
    public DwcaFileDescriptor? Extension(string rowTypeName) =>
        Extensions.FirstOrDefault(e => string.Equals(e.RowTypeName, rowTypeName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Parses meta.xml. Throws <see cref="InvalidDataException"/> when the file has no core or a
    /// file has no location.
    /// </summary>
    public static DwcaArchiveMeta Parse(Stream metaXml) {
        XDocument document;
        try {
            document = XDocument.Load(metaXml);
        } catch (System.Xml.XmlException ex) {
            throw new InvalidDataException($"meta.xml is not valid XML: {ex.Message}", ex);
        }
        var archive = document.Root ?? throw new InvalidDataException("meta.xml is empty.");
        // Match on local names: the archive namespace is sometimes the default namespace and
        // sometimes declared with a prefix.
        var core = archive.Elements().FirstOrDefault(e => e.Name.LocalName == "core")
            ?? throw new InvalidDataException("meta.xml has no <core> element.");
        var extensions = archive.Elements()
            .Where(e => e.Name.LocalName == "extension")
            .Select(e => ParseFile(e, isCore: false))
            .ToList();
        var metadata = (string?)archive.Attribute("metadata");
        return new DwcaArchiveMeta(ParseFile(core, isCore: true), extensions, string.IsNullOrWhiteSpace(metadata) ? null : metadata.Trim());
    }

    private static DwcaFileDescriptor ParseFile(XElement element, bool isCore) {
        var rowType = (string?)element.Attribute("rowType") ?? string.Empty;
        var location = element.Elements().FirstOrDefault(e => e.Name.LocalName == "files")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "location")?.Value.Trim();
        if (string.IsNullOrEmpty(location)) {
            throw new InvalidDataException($"meta.xml names no file for the {(isCore ? "core" : "extension")} with row type {rowType}.");
        }

        // The spec's defaults: comma-separated, double-quoted, no header line, UTF-8.
        var delimiter = Unescape((string?)element.Attribute("fieldsTerminatedBy") ?? ",");
        var quoteAttribute = element.Attribute("fieldsEnclosedBy");
        var quote = quoteAttribute is null ? "\"" : Unescape(quoteAttribute.Value);
        var ignore = int.TryParse((string?)element.Attribute("ignoreHeaderLines"), NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? n : 0;
        var encoding = ReadEncoding((string?)element.Attribute("encoding"));

        var idName = isCore ? "id" : "coreid";
        var idIndex = ReadIndex(element.Elements().FirstOrDefault(e => e.Name.LocalName == idName));

        var fields = element.Elements()
            .Where(e => e.Name.LocalName == "field")
            .Select(e => (Term: ((string?)e.Attribute("term") ?? string.Empty).Trim(), Index: ReadIndex(e), Default: (string?)e.Attribute("default")))
            .Where(f => f.Term.Length > 0)
            .ToList();

        return new DwcaFileDescriptor(isCore, rowType.Trim(), location, delimiter, quote.Length == 0 ? null : quote, ignore, encoding, idIndex, fields);
    }

    private static int? ReadIndex(XElement? element) =>
        int.TryParse((string?)element?.Attribute("index"), NumberStyles.None, CultureInfo.InvariantCulture, out var index) ? index : null;

    private static Encoding ReadEncoding(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        }
        try {
            return Encoding.GetEncoding(name.Trim());
        } catch (ArgumentException) {
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
        }
    }

    /// <summary>
    /// meta.xml writes control characters as backslash escapes in attributes ("\t" is the two
    /// characters backslash and t). Turns "\t", "\n", "\r" and "\\" into the characters they stand for.
    /// </summary>
    internal static string Unescape(string value) {
        if (value.IndexOf('\\') < 0) {
            return value;
        }
        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++) {
            var c = value[i];
            if (c == '\\' && i + 1 < value.Length) {
                var next = value[i + 1];
                var replacement = next switch {
                    't' => '\t',
                    'n' => '\n',
                    'r' => '\r',
                    '\\' => '\\',
                    _ => (char?)null,
                };
                if (replacement is char r) {
                    builder.Append(r);
                    i++;
                    continue;
                }
            }
            builder.Append(c);
        }
        return builder.ToString();
    }

    /// The last part of a term or row type URI: "http://rs.tdwg.org/dwc/terms/taxonID" gives "taxonID".
    internal static string LocalName(string uri) {
        var trimmed = uri.TrimEnd('/', '#');
        var cut = trimmed.LastIndexOfAny(new[] { '/', '#', ':' });
        return cut >= 0 ? trimmed[(cut + 1)..] : trimmed;
    }
}
