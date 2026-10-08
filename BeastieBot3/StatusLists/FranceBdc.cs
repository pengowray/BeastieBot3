using System.Globalization;
using System.IO.Compression;
using System.Net;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;

// PatriNat's "Base de connaissance Statuts des espèces" (BDC Statuts): every status of every taxon
// of TAXREF in France, its overseas territories, regions and départements, one row per taxon,
// status and place, with the document that gives it. BDC.zip (32.6 MB) holds one CSV of 447,664
// rows in version 18 (BDC_18/bdc_18_01.csv, 329 MB) and the list of status types. The import reads
// the CSV inside the zip without extracting it and keeps the rows of StoredTypes: the national and
// regional red lists, and the national, overseas, regional and départemental protection lists.
//
// Licence: Licence Ouverte / Open Licence 2.0 (Etalab), as data.gouv.fr lists the dataset. MNHN's
// own sites (inpn.mnhn.fr, taxref.mnhn.fr) have been down since a cyberattack in July 2025, so the
// files come from PatriNat's temporary download page, which says they will be removed when INPN
// is back.

namespace BeastieBot3.StatusLists;

/// One stored row of the BDC: a taxon's status in one place. RowNumber is the row's number in the
/// CSV, counting every row from 1 (the file has no row id). CdNom is the TAXREF id of the name the
/// source document used, CdRef the id of its accepted name. Remark holds the red list remark's text
/// and parts (FranceRedListRemark), FranceRemark.None for other types. IsCurrent is set by
/// FranceBdc.MarkCurrent for red list rows and is null for other types.
internal sealed record FranceStatusRow(
    long RowNumber,
    long CdNom,
    long CdRef,
    string TypeCode,
    string Code,
    string? Label,
    FranceRemark Remark,
    string TerritoryCode,
    string Name,
    string? Author,
    string? Kingdom,
    string? Phylum,
    string? TaxClass,
    string? TaxOrder,
    string? Family,
    long? CdDoc) {
    public bool? IsCurrent { get; init; }
}

/// A status type of the BDC (CD_TYPE_STATUT), with its French label and group. Stored is true for
/// the types whose rows the import keeps.
internal sealed record FranceStatusType(string Code, string Label, string? Group, bool Stored);

/// A place where statuses apply (CD_SIG), as the BDC names it, with an English name from
/// FranceTerritories.
internal sealed record FranceTerritory(string Code, string Name, string? NameEn, string? AdminLevel, string? Iso31661, string? Iso31662);

/// A source document (CD_DOC): Year and Title are read from the citation, Citation is the citation
/// as plain text, CitationHtml as the BDC gives it (null when the BDC gives none). BirdPopulation is
/// the population of birds the title names, when it names one (FranceBdc.TitlePopulation).
internal sealed record FranceDocument(long CdDoc, int? Year, string? Title, string? Citation, string? CitationHtml, string? Url,
    string? BirdPopulation);

/// What the import reads from BDC.zip. Version is the BDC's version (18 for BDC_18), CsvDate the
/// date of the CSV inside the zip, FileCount the number of files in the zip. Skipped counts rows of
/// stored types with no TAXREF id, code, place or name; SentenceRemarks the red list remarks not
/// stored; PopulationsFromTitles the bird rows whose population comes from their document's title.
internal sealed record FranceBdcData(
    IReadOnlyList<FranceStatusRow> Rows,
    IReadOnlyList<FranceStatusType> Types,
    IReadOnlyList<FranceTerritory> Territories,
    IReadOnlyList<FranceDocument> Documents,
    string? Version,
    DateTime? CsvDate,
    int FileCount,
    long RowsRead,
    int Skipped,
    int SentenceRemarks,
    int PopulationsFromTitles);

internal static partial class FranceBdc {
    public const string DownloadUrl = "https://assets.patrinat.fr/files/referentiel/BDC.zip";
    public const string DownloadPageUrl = "https://www.patrinat.fr/fr/page-temporaire-de-telechargement-des-referentiels-de-donnees-lies-linpn-7353";
    public const string DataGouvUrl = "https://www.data.gouv.fr/datasets/statuts-reglementaires-et-de-conservation-des-especes";

    public const string Title = "Base de connaissance « Statuts » des espèces (BDC Statuts), PatriNat";
    public const string Licence = "Licence Ouverte / Open Licence 2.0 (Etalab), https://www.etalab.gouv.fr/licence-ouverte-open-licence/";

    /// The national red list (Liste rouge nationale) and the regional red lists.
    public static readonly IReadOnlySet<string> RedListTypes = new HashSet<string>(StringComparer.Ordinal) { "LRN", "LRR" };

    /// The types stored: the red lists, and the national (PN), overseas collectivities' (POM),
    /// regional (PR) and départemental (PD) protection lists. Left out: IUCN's global and European
    /// red lists (LRM, LRE), which the site has from IUCN; the international conventions and EU
    /// directives (BERN, BONN, BARC, OSPAR, DH, DO), whose remarks say which populations a listing
    /// covers and are too long to store; ZNIEFF determinant species (ZDET); national action plans
    /// (PNA, exPNA); the regulations on introductions, trade and control (REGL, REGLII, REGLLUTTE,
    /// REGLSO); and the lists of taxa whose records are sensitive (SENSNAT, SENSREG, SENSDEP).
    public static readonly IReadOnlySet<string> StoredTypes = new HashSet<string>(StringComparer.Ordinal) {
        "LRN", "LRR", "PN", "POM", "PR", "PD",
    };

    /// The BDC's citation form, as its download page on INPN gave it for versions 16 and 17 ("Gargominy,
    /// O. & Régnier, C. 2024. Base de connaissance "Statuts" des espèces en France. Version pour TAXREF
    /// v17.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant deux fichiers. [version du 29 mai 2024]"),
    /// with the version, the date of the CSV inside the zip and the number of files in the zip.
    /// PatriNat's page for version 18 could not be read (INPN is down).
    public static string Citation(string version, DateTime csvDate, int fileCount) =>
        $"Gargominy, O. & Régnier, C. {csvDate.Year.ToString(CultureInfo.InvariantCulture)}. Base de connaissance \"Statuts\" des espèces en France. "
        + $"Version pour TAXREF v{version}.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant {FrenchCount(fileCount)}. "
        + $"[version du {FrenchDate(csvDate)}]";

    private static readonly string[] FrenchMonths = {
        "janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre",
    };

    private static readonly string[] FrenchNumbers = { "un", "deux", "trois", "quatre", "cinq", "six", "sept", "huit", "neuf", "dix" };

    /// "24 juillet 2025", "1er mai 2024".
    public static string FrenchDate(DateTime date) =>
        $"{(date.Day == 1 ? "1er" : date.Day.ToString(CultureInfo.InvariantCulture))} {FrenchMonths[date.Month - 1]} {date.Year.ToString(CultureInfo.InvariantCulture)}";

    /// "un fichier", "deux fichiers" ... "dix fichiers", then digits.
    public static string FrenchCount(int files) =>
        (files is >= 1 and <= 10 ? FrenchNumbers[files - 1] : files.ToString(CultureInfo.InvariantCulture)) + (files == 1 ? " fichier" : " fichiers");

    /// The message for a file that PatriNat no longer gives at <paramref name="url"/>: where to look
    /// for it, and the option that takes its new address.
    public static string MissingFileMessage(string url, string problem, string option) =>
        $"{url}: {problem}. PatriNat lists the BDC and TAXREF files on its temporary download page while INPN is down: {DownloadPageUrl} "
        + $"(the data.gouv.fr pages are {DataGouvUrl} and {FranceTaxref.DataGouvUrl}). Give the file's new address with {option}.";

    // ---- reading BDC.zip ----

    // CSV columns read. The others: CD_SUP, LB_TYPE_STATUT and REGROUPEMENT_TYPE (read into the
    // types), NOM_COMPLET_HTML and NOM_VALIDE_HTML (the names as HTML), GROUP1_INPN and GROUP2_INPN
    // (INPN's informal groups), THEMATIQUE and TYPE_VALUE (the same in every row).
    private static readonly string[] Required = {
        "CD_NOM", "CD_REF", "CD_TYPE_STATUT", "LB_TYPE_STATUT", "REGROUPEMENT_TYPE", "CODE_STATUT", "LABEL_STATUT", "RQ_STATUT",
        "CD_SIG", "CD_DOC", "LB_NOM", "LB_AUTEUR", "REGNE", "PHYLUM", "CLASSE", "ORDRE", "FAMILLE", "LB_ADM_TR", "NIVEAU_ADMIN",
        "CD_ISO3166_1", "CD_ISO3166_2", "FULL_CITATION", "DOC_URL",
    };

    /// Reads BDC.zip: the rows of StoredTypes from every bdc_<version>_<part>.csv in it, in name
    /// order, with the documents and places of those rows and every status type. Throws
    /// InvalidDataException when the file is not a zip, has no such CSV, or a CSV lacks a column.
    public static FranceBdcData ReadZip(string path) {
        using var zip = ZipFile.OpenRead(path);
        var parts = zip.Entries.Where(e => BdcCsvName().IsMatch(e.Name)).OrderBy(e => e.Name, StringComparer.OrdinalIgnoreCase).ToList();
        if (parts.Count == 0) {
            throw new InvalidDataException($"{Path.GetFileName(path)} has no bdc_<version>_<part>.csv file. The BDC zip's layout may have changed.");
        }
        var reader = new BdcReader();
        foreach (var part in parts) {
            using var stream = part.Open();
            using var text = new StreamReader(stream);
            reader.Read(text, part.Name);
        }
        var version = BdcCsvName().Match(parts[0].Name).Groups["version"].Value;
        var fileCount = zip.Entries.Count(e => e.Name.Length > 0);
        return reader.Result(version, parts[0].LastWriteTime.DateTime, fileCount);
    }

    /// Reads one BDC CSV (for tests and for ReadZip).
    public static FranceBdcData Read(TextReader text) {
        var reader = new BdcReader();
        reader.Read(text, "the BDC file");
        return reader.Result(null, null, 1);
    }

    private sealed class BdcReader {
        private readonly List<FranceStatusRow> _rows = new();
        private readonly Dictionary<string, FranceStatusType> _types = new(StringComparer.Ordinal);
        private readonly Dictionary<string, FranceTerritory> _territories = new(StringComparer.Ordinal);
        private readonly Dictionary<long, FranceDocument> _documents = new();
        private long _rowNumber;
        private int _skipped;
        private int _sentences;
        private int _populationsFromTitles;

        public void Read(TextReader text, string fileName) {
            using var csv = new CsvReader(text, new CsvConfiguration(CultureInfo.InvariantCulture) {
                HasHeaderRecord = true,
                BadDataFound = null,
                MissingFieldFound = null,
            });
            if (!csv.Read() || !csv.ReadHeader() || csv.HeaderRecord is not { } header) {
                throw new InvalidDataException($"{fileName} is empty.");
            }
            var index = new Dictionary<string, int>(StringComparer.Ordinal);
            for (var i = 0; i < header.Length; i++) {
                index.TryAdd(header[i].Trim(), i);
            }
            var missing = Required.Where(c => !index.ContainsKey(c)).ToList();
            if (missing.Count > 0) {
                throw new InvalidDataException($"{fileName} has no {string.Join(", ", missing)} column. The BDC's format may have changed.");
            }
            string? Field(string column) => csv.GetField(index[column]) is { } value && value.Trim().Length > 0 ? value.Trim() : null;

            while (csv.Read()) {
                _rowNumber++;
                var type = Field("CD_TYPE_STATUT");
                if (type is null) {
                    _skipped++;
                    continue;
                }
                if (!_types.ContainsKey(type)) {
                    _types[type] = new FranceStatusType(type, Field("LB_TYPE_STATUT") ?? type, Field("REGROUPEMENT_TYPE"), StoredTypes.Contains(type));
                }
                if (!StoredTypes.Contains(type)) {
                    continue;
                }
                var code = Field("CODE_STATUT");
                var territory = Field("CD_SIG");
                var name = Field("LB_NOM");
                if (Long(Field("CD_NOM")) is not { } cdNom || Long(Field("CD_REF")) is not { } cdRef || code is null || territory is null
                    || name is null) {
                    _skipped++;
                    continue;
                }
                var remark = FranceRemark.None;
                if (RedListTypes.Contains(type)) {
                    // The remark as given, untrimmed.
                    remark = FranceRedListRemark.Parse(code, csv.GetField(index["RQ_STATUT"]));
                    if (remark.IsSentence) {
                        _sentences++;
                    }
                }
                if (!_territories.ContainsKey(territory)) {
                    _territories[territory] = new FranceTerritory(territory, Field("LB_ADM_TR") ?? territory, FranceTerritories.EnglishName(territory),
                        Field("NIVEAU_ADMIN"), Field("CD_ISO3166_1"), Field("CD_ISO3166_2"));
                }
                var cdDoc = Long(Field("CD_DOC"));
                FranceDocument? document = null;
                if (cdDoc is { } doc && !_documents.TryGetValue(doc, out document)) {
                    document = _documents[doc] = Document(doc, Field("FULL_CITATION"), Field("DOC_URL"));
                }
                var taxClass = Field("CLASSE");
                // A regional list of breeding birds ("Liste rouge des oiseaux nicheurs de Franche-Comté")
                // gives no population in its remarks.
                if (RedListTypes.Contains(type) && remark.Population is null && taxClass == "Aves" && document?.BirdPopulation is { } population) {
                    remark = remark with { Population = population };
                    _populationsFromTitles++;
                }
                _rows.Add(new FranceStatusRow(_rowNumber, cdNom, cdRef, type, code, Field("LABEL_STATUT"), remark, territory, name,
                    Field("LB_AUTEUR"), Field("REGNE"), Field("PHYLUM"), taxClass, Field("ORDRE"), Field("FAMILLE"), cdDoc));
            }
        }

        public FranceBdcData Result(string? version, DateTime? csvDate, int fileCount) {
            var years = _documents.Values.ToDictionary(d => d.CdDoc, d => d.Year);
            return new FranceBdcData(MarkCurrent(_rows, years), _types.Values.OrderBy(t => t.Code, StringComparer.Ordinal).ToList(),
                _territories.Values.OrderBy(t => t.Code, StringComparer.Ordinal).ToList(), _documents.Values.OrderBy(d => d.CdDoc).ToList(),
                version, csvDate, fileCount, _rowNumber, _skipped, _sentences, _populationsFromTitles);
        }
    }

    // ---- documents ----

    /// A document from its citation as the BDC gives it (HTML with <em> and entities).
    public static FranceDocument Document(long cdDoc, string? citationHtml, string? url) {
        if (citationHtml is null) {
            return new FranceDocument(cdDoc, null, null, null, null, url, null);
        }
        var plain = PlainText(citationHtml);
        var title = DocumentTitle(citationHtml);
        return new FranceDocument(cdDoc, DocumentYear(plain), title, plain, citationHtml, url, TitlePopulation(title));
    }

    /// The population of birds a list's title names, when it names one: breeding for "oiseaux
    /// nicheurs", wintering for "oiseaux hivernants", visiting for "de passage" or "migrateurs".
    /// Null when the title names none, or several ("oiseaux nicheurs, de passage et hivernants").
    public static string? TitlePopulation(string? title) {
        if (title is null) {
            return null;
        }
        var named = new List<string>();
        if (title.Contains("nicheur", StringComparison.OrdinalIgnoreCase)) {
            named.Add(FranceRedListRemark.Breeding);
        }
        if (title.Contains("hivernant", StringComparison.OrdinalIgnoreCase)) {
            named.Add(FranceRedListRemark.Wintering);
        }
        if (title.Contains("de passage", StringComparison.OrdinalIgnoreCase) || title.Contains("migrateur", StringComparison.OrdinalIgnoreCase)) {
            named.Add(FranceRedListRemark.Visiting);
        }
        return named.Count == 1 ? named[0] : null;
    }

    /// The first year in a citation: "UICN Comité français, MNHN &amp; CBIG. 2019. ..." gives 2019,
    /// "Arrêté du 25 mars 2015 ..." 2015. Null when it has none.
    public static int? DocumentYear(string citation) =>
        Year().Match(citation) is { Success: true } match ? int.Parse(match.Value, CultureInfo.InvariantCulture) : null;

    /// The longest italic part of a citation (the title of a red list), as plain text without the
    /// full stops and spaces around it. Null when the citation has no italic part.
    public static string? DocumentTitle(string citationHtml) {
        var title = Italic().Matches(citationHtml)
            .Select(m => PlainText(m.Groups[1].Value).Trim(' ', '.', ',', ';', ':'))
            .OrderByDescending(t => t.Length)
            .FirstOrDefault();
        return string.IsNullOrEmpty(title) ? null : title;
    }

    /// HTML as plain text: tags removed, entities decoded, runs of white space as one space.
    public static string PlainText(string html) =>
        Spaces().Replace(WebUtility.HtmlDecode(Tags().Replace(html, "")), " ").Trim();

    // ---- the current row ----

    /// Marks which red list rows are current. Rows are grouped by type, accepted name (CdRef), place
    /// and population (breeding, wintering, visiting, or none). In each group the rows of the newest
    /// document (by its year; a document with no year is older than any) are current, and of those,
    /// when one is the row of the accepted name itself (CdNom = CdRef), only the rows of the accepted
    /// name. Two rows can stay current: a list that gives one name two categories. Rows of other types
    /// get IsCurrent null.
    public static IReadOnlyList<FranceStatusRow> MarkCurrent(IReadOnlyList<FranceStatusRow> rows, IReadOnlyDictionary<long, int?> documentYears) {
        int Year(FranceStatusRow row) => row.CdDoc is { } doc && documentYears.TryGetValue(doc, out var year) && year is { } y ? y : int.MinValue;
        var current = new HashSet<long>();
        foreach (var group in rows.Where(r => RedListTypes.Contains(r.TypeCode))
                     .GroupBy(r => (r.TypeCode, r.CdRef, r.TerritoryCode, r.Remark.Population ?? ""))) {
            var newest = group.Max(Year);
            var candidates = group.Where(r => Year(r) == newest).ToList();
            if (candidates.Any(r => r.CdNom == r.CdRef)) {
                candidates = candidates.Where(r => r.CdNom == r.CdRef).ToList();
            }
            foreach (var row in candidates) {
                current.Add(row.RowNumber);
            }
        }
        return rows.Select(r => r with { IsCurrent = RedListTypes.Contains(r.TypeCode) ? current.Contains(r.RowNumber) : null }).ToList();
    }

    private static long? Long(string? text) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;

    [GeneratedRegex(@"^bdc_(?<version>\d+)_\d+\.csv$", RegexOptions.IgnoreCase)]
    private static partial Regex BdcCsvName();

    [GeneratedRegex(@"\b(?:1[6-9]|20)\d\d\b")]
    private static partial Regex Year();

    [GeneratedRegex(@"<em>(.*?)</em>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Italic();

    [GeneratedRegex(@"<[^>]+>")]
    private static partial Regex Tags();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();
}
