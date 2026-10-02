using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;

// Reads GBIF's copy of the IUCN Red List checklist (iucn gbif-download): a Darwin Core Archive whose
// core is one row per taxon and whose extensions add vernacular names and one "distribution" row
// per taxon with the Red List category. In the 2026-1 archive:
//   taxon.txt          accepted taxa (taxonID = the IUCN SIS taxon id) and synonyms (taxonID like
//                      "158236_1", acceptedNameUsageID = the accepted taxon's SIS id). Every row has
//                      the latest global assessment's citation (dcterms:bibliographicCitation, with
//                      the DOI for nearly every taxon) and page URL (dcterms:references).
//   distribution.txt   one row per accepted taxon: locality "Global", the category in
//                      iucn:threatStatus, and the same citation again in dcterms:source.
//   vernacularname.txt name, language (ISO 639 three-letter codes, sometimes a language name), isPreferredName.
// Columns are found through meta.xml's term mapping, never by position.

namespace BeastieBot3.Iucn.Gbif;

internal sealed record GbifIucnVernacularName(string Name, string? Language, bool IsPreferred);

/// Which column the taxon's citation text was read from.
internal enum GbifCitationSource {
    None,
    /// dcterms:bibliographicCitation in the taxon row.
    Taxon,
    /// dcterms:source in the taxon's distribution row (used when the taxon row has no citation).
    Distribution,
}

/// <summary>An accepted taxon of the checklist, keyed by its IUCN SIS taxon id.</summary>
/// <param name="ScientificName">The name with its authority, as the archive gives it ("Calocedrus rupestris Aver., Hiep &amp; L.K.Phan").</param>
/// <param name="Rank">"species", "subspecies", "subspecies (plantae)" or "variety".</param>
/// <param name="Citation">The latest global assessment's citation. It has no "Accessed on" sentence and no e.T article number.</param>
/// <param name="AssessmentUrl">dcterms:references: "https://www.iucnredlist.org/species/{taxonId}/{assessmentId}".</param>
/// <param name="AssessmentId">The assessment id in <paramref name="AssessmentUrl"/>.</param>
/// <param name="Doi">The DOI in the citation, without "https://doi.org/". Null when the citation has none.</param>
/// <param name="ThreatStatus">iucn:threatStatus as written: "Endangered", "Least Concern", and lower case ("near threatened") for the old Lower Risk categories.</param>
/// <param name="Locality">The distribution row's locality ("Global").</param>
/// <param name="OccurrenceStatus">"Present", or "Absent" for Extinct and Extinct in the Wild taxa.</param>
internal sealed record GbifIucnTaxon(
    long TaxonId,
    string ScientificName,
    string? Authorship,
    string? Rank,
    string? Kingdom,
    string? TaxonomicStatus,
    long? AcceptedId,
    string? Citation,
    GbifCitationSource CitationSource,
    string? AssessmentUrl,
    long? AssessmentId,
    string? Doi,
    string? ThreatStatus,
    string? Locality,
    string? OccurrenceStatus,
    IReadOnlyList<GbifIucnVernacularName> VernacularNames) {

    /// The taxon and assessment ids in the DOI. For an errata version published 2015 to 2018 the
    /// DOI's assessment id is the predecessor's, not <see cref="AssessmentId"/>.
    public IucnDoiParts? DoiParts => IucnDoi.Parse(Doi);
}

/// <summary>A synonym row. Its id is the archive's own ("158236_1"), not an IUCN id.</summary>
internal sealed record GbifIucnSynonym(string Id, string ScientificName, string? Authorship, long AcceptedId);

/// <summary>How many data rows one file had, and how many of them were not used.</summary>
/// <param name="UnusedRows">Core: rows that are neither an accepted taxon with a numeric id nor a
/// synonym of one, plus repeated ids. Extensions: rows whose core id is not an accepted taxon,
/// plus extra distribution rows for a taxon that already has one.</param>
internal sealed record GbifIucnFileCount(string Location, string RowTypeName, int Rows, int UnusedRows);

internal sealed record GbifIucnChecklist(
    GbifIucnArchiveSummary Summary,
    IReadOnlyDictionary<long, GbifIucnTaxon> Taxa,
    IReadOnlyList<GbifIucnSynonym> Synonyms,
    IReadOnlyList<GbifIucnFileCount> Files);

/// The archive's description: its files and the dataset metadata, read without reading any rows.
internal sealed record GbifIucnArchiveSummary(DwcaArchiveMeta Meta, GbifIucnDatasetInfo Dataset) {
    public IEnumerable<DwcaFileDescriptor> Files => Meta.Extensions.Prepend(Meta.Core);
}

internal static class GbifIucnChecklistReader {
    public const string TaxonRowType = "Taxon";
    public const string DistributionRowType = "Distribution";
    public const string VernacularNameRowType = "VernacularName";

    private const string Dwc = "http://rs.tdwg.org/dwc/terms/";
    private const string Dc = "http://purl.org/dc/terms/";
    public const string TaxonIdTerm = Dwc + "taxonID";
    public const string ScientificNameTerm = Dwc + "scientificName";
    public const string AuthorshipTerm = Dwc + "scientificNameAuthorship";
    public const string RankTerm = Dwc + "taxonRank";
    public const string KingdomTerm = Dwc + "kingdom";
    public const string TaxonomicStatusTerm = Dwc + "taxonomicStatus";
    public const string AcceptedIdTerm = Dwc + "acceptedNameUsageID";
    public const string BibliographicCitationTerm = Dc + "bibliographicCitation";
    public const string ReferencesTerm = Dc + "references";
    public const string SourceTerm = Dc + "source";
    public const string LocalityTerm = Dwc + "locality";
    public const string OccurrenceStatusTerm = Dwc + "occurrenceStatus";
    public const string ThreatStatusTerm = "http://iucn.org/terms/threatStatus";
    public const string VernacularNameTerm = Dwc + "vernacularName";
    public const string LanguageTerm = Dc + "language";
    public const string IsPreferredNameTerm = "http://rs.gbif.org/terms/1.0/isPreferredName";

    /// The newest iucn-checklist-*.zip in <paramref name="directory"/>, or null.
    public static string? FindNewest(string? directory) => GbifIucnChecklistFiles.FindNewest(directory);

    public static GbifIucnChecklist Read(string zipPath, CancellationToken cancellationToken = default) {
        using var zip = OpenZip(zipPath);
        return Read(zip, cancellationToken);
    }

    public static GbifIucnChecklist Read(Stream zipStream, CancellationToken cancellationToken = default) {
        using var zip = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: true);
        return Read(zip, cancellationToken);
    }

    public static GbifIucnChecklist Read(ZipArchive zip, CancellationToken cancellationToken = default) {
        var summary = ReadSummary(zip);
        var meta = summary.Meta;
        var files = new List<GbifIucnFileCount>();

        var core = ReadCore(zip, meta.Core, cancellationToken, out var synonyms, out var coreCount);
        files.Add(coreCount);

        var distributions = new Dictionary<long, DistributionRow>();
        if (meta.Extension(DistributionRowType) is { } distributionFile) {
            files.Add(ReadDistributions(zip, distributionFile, core, distributions, cancellationToken));
        }

        var vernaculars = new Dictionary<long, List<GbifIucnVernacularName>>();
        if (meta.Extension(VernacularNameRowType) is { } vernacularFile) {
            files.Add(ReadVernacularNames(zip, vernacularFile, core, vernaculars, cancellationToken));
        }

        // Extensions this reader does not use are still counted, so the row counts cover every file.
        foreach (var other in meta.Extensions.Where(e =>
                     !string.Equals(e.RowTypeName, DistributionRowType, StringComparison.OrdinalIgnoreCase)
                     && !string.Equals(e.RowTypeName, VernacularNameRowType, StringComparison.OrdinalIgnoreCase))) {
            var rows = ReadRows(zip, other, cancellationToken).Count();
            files.Add(new GbifIucnFileCount(other.Location, other.RowTypeName, rows, rows));
        }

        var taxa = new Dictionary<long, GbifIucnTaxon>(core.Count);
        foreach (var (id, row) in core) {
            distributions.TryGetValue(id, out var distribution);
            var (citation, source) = row.Citation is not null ? (row.Citation, GbifCitationSource.Taxon)
                : distribution?.Source is not null ? (distribution.Source, GbifCitationSource.Distribution)
                : (null, GbifCitationSource.None);
            var doi = IucnDoi.Extract(citation) ?? IucnDoi.Extract(distribution?.Source);
            taxa[id] = new GbifIucnTaxon(
                id,
                row.ScientificName,
                row.Authorship,
                row.Rank,
                row.Kingdom,
                row.TaxonomicStatus,
                row.AcceptedId,
                citation,
                source,
                row.AssessmentUrl,
                IucnDoi.ParseAssessmentUrl(row.AssessmentUrl)?.AssessmentId,
                doi,
                distribution?.ThreatStatus,
                distribution?.Locality,
                distribution?.OccurrenceStatus,
                vernaculars.TryGetValue(id, out var names) ? names : Array.Empty<GbifIucnVernacularName>());
        }

        return new GbifIucnChecklist(summary, taxa, synonyms, files);
    }

    public static GbifIucnArchiveSummary ReadSummary(string zipPath) {
        using var zip = OpenZip(zipPath);
        return ReadSummary(zip);
    }

    /// <summary>
    /// Reads meta.xml and eml.xml and checks that every file meta.xml names is in the zip.
    /// Throws <see cref="InvalidDataException"/> when the archive is not a taxon checklist.
    /// </summary>
    public static GbifIucnArchiveSummary ReadSummary(ZipArchive zip) {
        var metaEntry = zip.GetEntry("meta.xml") ?? throw new InvalidDataException("The zip has no meta.xml.");
        DwcaArchiveMeta meta;
        using (var stream = metaEntry.Open()) {
            meta = DwcaArchiveMeta.Parse(stream);
        }
        if (!string.Equals(meta.Core.RowTypeName, TaxonRowType, StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidDataException($"The archive's core rows are {meta.Core.RowType}, not taxa.");
        }
        if (meta.Core.IdIndex is null && meta.Core.IndexOf(TaxonIdTerm) is null) {
            throw new InvalidDataException("meta.xml gives no column for the taxon id.");
        }
        if (meta.Core.IndexOf(ScientificNameTerm) is null) {
            throw new InvalidDataException("meta.xml gives no column for the scientific name.");
        }
        foreach (var file in meta.Extensions.Prepend(meta.Core)) {
            if (zip.GetEntry(file.Location) is null) {
                throw new InvalidDataException($"meta.xml names {file.Location}, which is not in the zip.");
            }
        }

        var emlEntry = zip.GetEntry(meta.MetadataLocation ?? "eml.xml") ?? throw new InvalidDataException("The zip has no eml.xml.");
        using var emlStream = emlEntry.Open();
        return new GbifIucnArchiveSummary(meta, GbifIucnEml.Parse(emlStream));
    }

    /// <summary>
    /// The data rows of one file, after its header lines, as arrays of raw field text. Uses the
    /// file's delimiter and quote character from meta.xml; a file with no quote character is split
    /// on the delimiter only, so a double quote is an ordinary character in it.
    /// </summary>
    public static IEnumerable<string[]> ReadRows(ZipArchive zip, DwcaFileDescriptor file, CancellationToken cancellationToken = default) {
        var entry = zip.GetEntry(file.Location) ?? throw new InvalidDataException($"meta.xml names {file.Location}, which is not in the zip.");
        var config = new CsvConfiguration(CultureInfo.InvariantCulture) {
            Delimiter = file.FieldDelimiter,
            HasHeaderRecord = false,
            BadDataFound = null,
            MissingFieldFound = null,
            DetectColumnCountChanges = false,
            TrimOptions = TrimOptions.None,
        };
        if (file.Quote is { } quote) {
            config.Quote = quote[0];
            config.Mode = CsvMode.RFC4180;
        } else {
            config.Mode = CsvMode.NoEscape;
        }

        using var stream = entry.Open();
        using var text = new StreamReader(stream, file.Encoding, detectEncodingFromByteOrderMarks: true);
        using var csv = new CsvReader(text, config);
        var skip = file.IgnoreHeaderLines;
        while (csv.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            if (skip > 0) {
                skip--;
                continue;
            }
            // Parser.Record, not TryGetField: with MissingFieldFound off, TryGetField past the
            // last column returns true with null.
            if (csv.Parser.Record is { } record) {
                yield return record;
            }
        }
    }

    private sealed record CoreRow(
        string ScientificName,
        string? Authorship,
        string? Rank,
        string? Kingdom,
        string? TaxonomicStatus,
        long? AcceptedId,
        string? Citation,
        string? AssessmentUrl);

    private sealed record DistributionRow(string? Locality, string? Source, string? ThreatStatus, string? OccurrenceStatus);

    private static Dictionary<long, CoreRow> ReadCore(
        ZipArchive zip,
        DwcaFileDescriptor file,
        CancellationToken cancellationToken,
        out List<GbifIucnSynonym> synonyms,
        out GbifIucnFileCount count) {
        var taxa = new Dictionary<long, CoreRow>();
        synonyms = new List<GbifIucnSynonym>();
        var rows = 0;
        var unused = 0;
        foreach (var record in ReadRows(zip, file, cancellationToken)) {
            rows++;
            var id = file.Id(record) ?? file.Value(record, TaxonIdTerm);
            var name = file.Value(record, ScientificNameTerm);
            if (id is null || name is null) {
                unused++;
                continue;
            }
            var status = file.Value(record, TaxonomicStatusTerm);
            var acceptedId = ParseId(file.Value(record, AcceptedIdTerm));
            var authorship = file.Value(record, AuthorshipTerm);

            if (status is not null && status.Contains("synonym", StringComparison.OrdinalIgnoreCase)) {
                if (acceptedId is long accepted) {
                    synonyms.Add(new GbifIucnSynonym(id, name, authorship, accepted));
                } else {
                    unused++;
                }
                continue;
            }
            if (ParseId(id) is not long taxonId || taxa.ContainsKey(taxonId)) {
                unused++;
                continue;
            }
            taxa[taxonId] = new CoreRow(
                name,
                authorship,
                file.Value(record, RankTerm),
                file.Value(record, KingdomTerm),
                status,
                acceptedId,
                file.Value(record, BibliographicCitationTerm),
                file.Value(record, ReferencesTerm));
        }
        count = new GbifIucnFileCount(file.Location, file.RowTypeName, rows, unused);
        return taxa;
    }

    private static GbifIucnFileCount ReadDistributions(
        ZipArchive zip,
        DwcaFileDescriptor file,
        Dictionary<long, CoreRow> core,
        Dictionary<long, DistributionRow> distributions,
        CancellationToken cancellationToken) {
        var rows = 0;
        var unused = 0;
        foreach (var record in ReadRows(zip, file, cancellationToken)) {
            rows++;
            if (ParseId(file.Id(record)) is not long taxonId || !core.ContainsKey(taxonId)) {
                unused++;
                continue;
            }
            var row = new DistributionRow(
                file.Value(record, LocalityTerm),
                file.Value(record, SourceTerm),
                file.Value(record, ThreatStatusTerm),
                file.Value(record, OccurrenceStatusTerm));
            // One row per taxon is expected. If there are more, the global one wins.
            if (!distributions.TryGetValue(taxonId, out var existing)) {
                distributions[taxonId] = row;
            } else {
                unused++;
                if (!IsGlobal(existing) && IsGlobal(row)) {
                    distributions[taxonId] = row;
                }
            }
        }
        return new GbifIucnFileCount(file.Location, file.RowTypeName, rows, unused);
    }

    private static GbifIucnFileCount ReadVernacularNames(
        ZipArchive zip,
        DwcaFileDescriptor file,
        Dictionary<long, CoreRow> core,
        Dictionary<long, List<GbifIucnVernacularName>> vernaculars,
        CancellationToken cancellationToken) {
        var rows = 0;
        var unused = 0;
        foreach (var record in ReadRows(zip, file, cancellationToken)) {
            rows++;
            var name = file.Value(record, VernacularNameTerm);
            if (name is null || ParseId(file.Id(record)) is not long taxonId || !core.ContainsKey(taxonId)) {
                unused++;
                continue;
            }
            if (!vernaculars.TryGetValue(taxonId, out var names)) {
                names = new List<GbifIucnVernacularName>();
                vernaculars[taxonId] = names;
            }
            names.Add(new GbifIucnVernacularName(name, file.Value(record, LanguageTerm), IsTrue(file.Value(record, IsPreferredNameTerm))));
        }
        return new GbifIucnFileCount(file.Location, file.RowTypeName, rows, unused);
    }

    private static bool IsGlobal(DistributionRow row) =>
        string.Equals(row.Locality, "Global", StringComparison.OrdinalIgnoreCase);

    private static bool IsTrue(string? value) =>
        value is not null && (value.Equals("true", StringComparison.OrdinalIgnoreCase)
                              || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
                              || value == "1");

    private static long? ParseId(string? value) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id) ? id : null;

    private static ZipArchive OpenZip(string zipPath) {
        try {
            return ZipFile.OpenRead(zipPath);
        } catch (InvalidDataException ex) {
            throw new InvalidDataException($"The file is not a zip: {ex.Message}", ex);
        }
    }
}
