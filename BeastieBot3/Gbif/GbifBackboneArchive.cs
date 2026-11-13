using System.Globalization;
using System.IO.Compression;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Iucn.Gbif;

// The GBIF Backbone Taxonomy as GBIF publishes it: a Darwin Core Archive (backbone.zip, about 930 MB,
// CC BY 4.0, https://doi.org/10.15468/39omei) with every name usage in Taxon.tsv (about 7.7 million
// rows, 2.25 GB unzipped) and the vernacular names GBIF took from its source datasets in
// VernacularName.tsv (1.5 million rows). `gbif backbone-download` saves it as backbone-<date>.zip
// (date from the server's Last-Modified header) in Datasets:GBIF_backbone_dir; `site build-db`
// reads the newest one straight from the zip, without unpacking it.
//
// Columns are found through meta.xml (DwcaArchiveMeta), never by position. Every file of the archive
// is tab-separated with fieldsEnclosedBy="" (no quoting), so a row is split on tabs alone, which
// keeps a pass over Taxon.tsv at about 15 seconds.
//
// VernacularName.tsv gives each name's source dataset only by its title. The archive's dataset/
// folder has the EML of the datasets the backbone was built from, and their titles name most of the
// sources, among them the Handbook of the Mammals of the World chapters (titled with a family name,
// "Muridae"; their EML abstracts name the book).

namespace BeastieBot3.Gbif;

/// The archive's own description (eml.xml): title, publication date (pubDate, else the date of its
/// dateStamp), licence, the citation GBIF asks for and the DOI in it.
internal sealed record GbifBackboneInfo(string? Title, string? PubDate, string? LicenceUrl, string? Citation, string? Doi);

/// One row of Taxon.tsv, with the columns the site build reads. Id and AcceptedId are GBIF taxon
/// keys (the ids of www.gbif.org/species/{key}, Wikidata's P846); AcceptedId is null for an accepted
/// or doubtful name.
internal readonly record struct GbifTaxonRow(
    long Id, long? AcceptedId, string Status, string Rank, string? CanonicalName, string? ScientificName,
    string? Authorship, string? Kingdom);

/// One row of VernacularName.tsv: the taxon key, the name, its language as GBIF gives it (ISO 639-1,
/// or empty) and the title of the dataset GBIF took it from (or empty).
internal readonly record struct GbifVernacularRow(long TaxonId, string Name, string? Language, string? Source);

internal static class GbifTaxonomicStatus {
    public const string Accepted = "accepted";
    public const string Doubtful = "doubtful";
    public const string ProParteSynonym = "proparte synonym";

    /// "synonym", "homotypic synonym" and "heterotypic synonym"; not "proparte synonym", a name GBIF
    /// gives to two or more accepted taxa (the Catalogue of Life's ambiguous synonym).
    public static bool IsSynonym(string status) => status is "synonym" or "homotypic synonym" or "heterotypic synonym";

    public static bool IsAccepted(string status) => status is Accepted or Doubtful;
}

internal static partial class GbifBackboneArchive {
    /// GBIF's current backbone (unchanged since 2023-08-28 as of November 2026).
    public const string DownloadUrl = "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip";
    public const string DatasetPageUrl = "https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c";

    /// Text in the EML abstract of each Handbook of the Mammals of the World chapter dataset.
    public const string HandbookText = "Handbook of the Mammals of the World";

    [GeneratedRegex(@"^backbone-(?<date>\d{4}-\d{2}-\d{2})\.zip$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex FileNamePattern();

    public static string FileNameFor(DateOnly date) => $"backbone-{date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}.zip";

    /// The date in a name like "backbone-2023-08-28.zip"; null for any other name.
    public static DateOnly? DateOf(string path) {
        var match = FileNamePattern().Match(Path.GetFileName(path));
        return match.Success && DateOnly.TryParseExact(match.Groups["date"].Value, "yyyy-MM-dd", CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var date) ? date : null;
    }

    /// The newest backbone-*.zip in the folder (by the date in its name); null when it has none.
    public static string? FindNewest(string? directory) {
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory)) {
            return null;
        }
        return Directory.EnumerateFiles(directory, "backbone-*.zip")
            .Select(path => (Path: path, Date: DateOf(path)))
            .Where(f => f.Date is not null)
            .OrderByDescending(f => f.Date)
            .Select(f => f.Path)
            .FirstOrDefault();
    }

    public static ZipArchive Open(string path) => new(File.OpenRead(path), ZipArchiveMode.Read, leaveOpen: false);

    /// The archive's meta.xml. Throws InvalidDataException when it has none, or no core.
    public static DwcaArchiveMeta ReadMeta(ZipArchive zip) {
        var entry = zip.GetEntry("meta.xml") ?? throw new InvalidDataException("The archive has no meta.xml.");
        using var stream = entry.Open();
        return DwcaArchiveMeta.Parse(stream);
    }

    public static GbifBackboneInfo ReadInfo(ZipArchive zip) {
        var entry = zip.GetEntry("eml.xml");
        if (entry is null) {
            return new GbifBackboneInfo(null, null, null, null, null);
        }
        using var memory = new MemoryStream();
        using (var stream = entry.Open()) {
            stream.CopyTo(memory);
        }
        memory.Position = 0;
        var eml = GbifIucnEml.Parse(memory);
        // The backbone's eml.xml has no pubDate; its dateStamp is when the backbone was built.
        var date = eml.PubDate;
        if (date is null) {
            memory.Position = 0;
            var stamp = XDocument.Load(memory).Descendants().FirstOrDefault(e => e.Name.LocalName == "dateStamp")?.Value.Trim();
            date = stamp is { Length: >= 10 } ? stamp[..10] : null;
        }
        return new GbifBackboneInfo(eml.Title, date, eml.LicenceUrl, eml.Citation, IucnCitationText.ExtractDoi(eml.Citation));
    }

    /// The titles of the datasets in the archive's dataset/ folder whose EML names the Handbook of the
    /// Mammals of the World (63 chapters in the 2023-08-28 backbone).
    public static HashSet<string> HandbookTitles(ZipArchive zip) {
        var titles = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in zip.Entries) {
            if (!entry.FullName.StartsWith("dataset/", StringComparison.Ordinal) || !entry.FullName.EndsWith(".xml", StringComparison.Ordinal)) {
                continue;
            }
            using var reader = new StreamReader(entry.Open());
            var text = reader.ReadToEnd();
            if (!text.Contains(HandbookText, StringComparison.Ordinal)) {
                continue;
            }
            try {
                var title = XDocument.Parse(text).Descendants().FirstOrDefault(e => e.Name.LocalName == "title")?.Value.Trim();
                if (!string.IsNullOrEmpty(title)) {
                    titles.Add(title);
                }
            } catch (System.Xml.XmlException) {
                // A dataset description that is not XML names no title.
            }
        }
        return titles;
    }

    /// Every row of Taxon.tsv, in file order.
    public static IEnumerable<GbifTaxonRow> ReadTaxa(ZipArchive zip, DwcaArchiveMeta meta, CancellationToken ct) {
        var file = meta.Core;
        var id = file.IdIndex ?? file.IndexOf("taxonID") ?? throw new InvalidDataException($"{file.Location} has no id column.");
        var accepted = Required(file, "acceptedNameUsageID");
        var status = Required(file, "taxonomicStatus");
        var rank = Required(file, "taxonRank");
        var canonical = Required(file, "canonicalName");
        var scientific = Required(file, "scientificName");
        var authorship = Required(file, "scientificNameAuthorship");
        var kingdom = Required(file, "kingdom");
        foreach (var fields in Rows(zip, file, ct)) {
            if (!TryKey(Field(fields, id), out var key)) {
                continue;
            }
            yield return new GbifTaxonRow(key, TryKey(Field(fields, accepted), out var acceptedKey) ? acceptedKey : null,
                Field(fields, status) ?? string.Empty, Field(fields, rank) ?? string.Empty, Field(fields, canonical),
                Field(fields, scientific), Field(fields, authorship), Field(fields, kingdom));
        }
    }

    /// Every row of VernacularName.tsv, in file order; empty when the archive has no such extension.
    public static IEnumerable<GbifVernacularRow> ReadVernacularNames(ZipArchive zip, DwcaArchiveMeta meta, CancellationToken ct) {
        if (meta.Extension("VernacularName") is not { } file) {
            yield break;
        }
        var id = file.IdIndex ?? throw new InvalidDataException($"{file.Location} has no coreid column.");
        var name = Required(file, "vernacularName");
        var language = file.IndexOf("language");
        var source = file.IndexOf("source");
        foreach (var fields in Rows(zip, file, ct)) {
            if (!TryKey(Field(fields, id), out var key) || Field(fields, name) is not { } text) {
                continue;
            }
            yield return new GbifVernacularRow(key, text, language is { } l ? Field(fields, l) : null, source is { } s ? Field(fields, s) : null);
        }
    }

    private static int Required(DwcaFileDescriptor file, string term) =>
        file.IndexOf(term) ?? throw new InvalidDataException($"{file.Location} has no {term} column.");

    // The fields of each data row (header lines skipped), split on the file's delimiter. Only
    // unquoted files are read this way; a quoted one is refused rather than read wrongly.
    private static IEnumerable<string[]> Rows(ZipArchive zip, DwcaFileDescriptor file, CancellationToken ct) {
        if (!string.IsNullOrEmpty(file.Quote)) {
            throw new InvalidDataException($"{file.Location} has quoted fields, which this reader does not read.");
        }
        var entry = zip.GetEntry(file.Location) ?? throw new InvalidDataException($"The archive has no {file.Location}.");
        var delimiter = file.FieldDelimiter.Length == 1 ? file.FieldDelimiter[0]
            : throw new InvalidDataException($"{file.Location}: a delimiter of more than one character is not supported.");
        using var reader = new StreamReader(entry.Open(), file.Encoding, detectEncodingFromByteOrderMarks: true, bufferSize: 1 << 16);
        for (var skip = 0; skip < file.IgnoreHeaderLines; skip++) {
            if (reader.ReadLine() is null) {
                yield break;
            }
        }
        var count = 0;
        for (var line = reader.ReadLine(); line is not null; line = reader.ReadLine()) {
            if ((++count & 0xFFFF) == 0) {
                ct.ThrowIfCancellationRequested();
            }
            if (line.Length > 0) {
                yield return line.Split(delimiter);
            }
        }
    }

    private static string? Field(string[] fields, int index) =>
        index < fields.Length && fields[index].Trim() is { Length: > 0 } value ? value : null;

    private static bool TryKey(string? text, out long key) {
        key = 0;
        return text is not null && long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out key);
    }
}
