using System.Globalization;
using System.IO.Compression;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;

// TAXREF, the French national taxonomic reference (PatriNat), which the BDC's CD_NOM and CD_REF
// ids come from. TAXREF_v18_2025.zip (60.6 MB) holds TAXREFv18.txt (708,685 names, tab-separated)
// and TAXREF_LIENS.txt (2,016,747 links from a name to its id in another database, among them the
// IUCN Red List's taxon id for 23,279 names). The import keeps, for the accepted names (CD_REF) of
// the stored statuses only: every name with that accepted name (the accepted name, its synonyms and
// the names TAXREF puts under it), for matching by name; and their links to the IUCN Red List,
// BirdLife, the Catalogue of Life and GBIF, for matching by id.
//
// Licence: TAXREF's terms of use (taxref.mnhn.fr, "Conditions d'utilisation") put it under the
// Licence Ouverte, with TAXREF cited; data.gouv.fr lists it under the Licence Ouverte 2.0.

namespace BeastieBot3.StatusLists;

/// A TAXREF name: its id (CdNom), the id of its accepted name (CdRef), its rank code (RANG: ES
/// species, SSES subspecies, VAR variety ...), the name without author and the author.
internal sealed record TaxrefName(long CdNom, long CdRef, string? Rank, string Name, string? Author);

/// A link from a TAXREF name to its id in another database (Source is TAXREF's name for it).
internal sealed record TaxrefLink(long CdNom, long CdRef, string Source, string ExternalId);

/// What the import reads from the TAXREF zip. Version is TAXREF's version (18 for TAXREFv18.txt),
/// Date the date of TAXREFv<version>.txt in the zip, DataFileCount the number of .txt and .csv files
/// in the zip.
internal sealed record FranceTaxrefData(
    IReadOnlyList<TaxrefName> Names,
    IReadOnlyList<TaxrefLink> Links,
    string Version,
    DateTime Date,
    int DataFileCount);

internal static partial class FranceTaxref {
    public const string DownloadUrl = "https://assets.patrinat.fr/files/referentiel/TAXREF_v18_2025.zip";
    public const string DataGouvUrl = "https://www.data.gouv.fr/datasets/referentiel-taxonomique-taxref-1";

    public const string Title = "TAXREF, référentiel taxonomique pour la France, PatriNat";

    /// TAXREF_LIENS sources kept. "IUCN Red List > BirdLife" gives BirdLife's ids, which are the
    /// IUCN Red List's taxon ids of birds.
    public const string IucnRedList = "IUCN Red List";
    public const string BirdLife = "IUCN Red List > BirdLife";
    public const string CatalogueOfLife = "Catalogue of Life";
    public const string Gbif = "GBIF";

    public static readonly IReadOnlySet<string> LinkSources = new HashSet<string>(StringComparer.Ordinal) {
        IucnRedList, BirdLife, CatalogueOfLife, Gbif,
    };

    /// TAXREF's citation form, as taxref.mnhn.fr gave it for version 18.0 ("TAXREF [Eds] 2025. TAXREF
    /// v18.0, référentiel taxonomique pour la France. PatriNat (OFB-CNRS-MNHN-IRD), Muséum national
    /// d'Histoire naturelle, Paris. Archive de téléchargement contenant 8 fichiers générés le 9
    /// janvier 2025. https://inpn.mnhn.fr/telechargement/referentielEspece/taxref/18.0/menu"), with
    /// the version, the number of data files and the date of TAXREFv<version>.txt of the zip read.
    public static string Citation(string version, DateTime date, int dataFiles) =>
        $"TAXREF [Eds] {date.Year.ToString(CultureInfo.InvariantCulture)}. TAXREF v{version}.0, référentiel taxonomique pour la France. "
        + "PatriNat (OFB-CNRS-MNHN-IRD), Muséum national d'Histoire naturelle, Paris. "
        + $"Archive de téléchargement contenant {dataFiles.ToString(CultureInfo.InvariantCulture)} fichiers "
        + $"générés le {FranceBdc.FrenchDate(date)}. https://inpn.mnhn.fr/telechargement/referentielEspece/taxref/{version}.0/menu";

    /// Reads the TAXREF zip: the names whose accepted name is in <paramref name="acceptedIds"/>, and
    /// the links of those names to LinkSources. Throws InvalidDataException when the file is not a
    /// zip or has no TAXREFv<version>.txt or TAXREF_LIENS.txt, or when a file lacks a column.
    public static FranceTaxrefData ReadZip(string path, IReadOnlySet<long> acceptedIds) {
        using var zip = ZipFile.OpenRead(path);
        var namesEntry = zip.Entries.FirstOrDefault(e => NamesFile().IsMatch(e.Name))
            ?? throw new InvalidDataException($"{Path.GetFileName(path)} has no TAXREFv<version>.txt file. Is it a TAXREF zip?");
        var linksEntry = zip.Entries.FirstOrDefault(e => e.Name.Equals("TAXREF_LIENS.txt", StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidDataException($"{Path.GetFileName(path)} has no TAXREF_LIENS.txt file.");

        IReadOnlyList<TaxrefName> names;
        using (var text = new StreamReader(namesEntry.Open())) {
            names = ReadNames(text, acceptedIds);
        }
        IReadOnlyList<TaxrefLink> links;
        using (var text = new StreamReader(linksEntry.Open())) {
            links = ReadLinks(text, names.ToDictionary(n => n.CdNom, n => n.CdRef));
        }
        var dataFiles = zip.Entries.Count(e => e.Name.EndsWith(".txt", StringComparison.OrdinalIgnoreCase)
                                               || e.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase));
        return new FranceTaxrefData(names, links, NamesFile().Match(namesEntry.Name).Groups["version"].Value,
            namesEntry.LastWriteTime.DateTime, dataFiles);
    }

    /// The names of TAXREFv<version>.txt whose CD_REF is in <paramref name="acceptedIds"/>.
    public static IReadOnlyList<TaxrefName> ReadNames(TextReader text, IReadOnlySet<long> acceptedIds) {
        using var csv = TabReader(text);
        var index = Header(csv, "TAXREFv<version>.txt", "CD_NOM", "CD_REF", "RANG", "LB_NOM", "LB_AUTEUR");
        var names = new List<TaxrefName>();
        var seen = new HashSet<long>();
        while (csv.Read()) {
            if (Long(csv.GetField(index["CD_REF"])) is not { } cdRef || !acceptedIds.Contains(cdRef)
                || Long(csv.GetField(index["CD_NOM"])) is not { } cdNom || !seen.Add(cdNom)
                || Text(csv.GetField(index["LB_NOM"])) is not { } name) {
                continue;
            }
            names.Add(new TaxrefName(cdNom, cdRef, Text(csv.GetField(index["RANG"])), name, Text(csv.GetField(index["LB_AUTEUR"]))));
        }
        return names;
    }

    /// The links of TAXREF_LIENS.txt from a name in <paramref name="acceptedIdOf"/> (CD_NOM to CD_REF)
    /// to one of LinkSources, each once.
    public static IReadOnlyList<TaxrefLink> ReadLinks(TextReader text, IReadOnlyDictionary<long, long> acceptedIdOf) {
        using var csv = TabReader(text);
        var index = Header(csv, "TAXREF_LIENS.txt", "CT_NAME", "CD_NOM", "CT_SP_ID");
        var links = new List<TaxrefLink>();
        var seen = new HashSet<(long, string, string)>();
        while (csv.Read()) {
            if (Text(csv.GetField(index["CT_NAME"])) is not { } source || !LinkSources.Contains(source)
                || Long(csv.GetField(index["CD_NOM"])) is not { } cdNom || !acceptedIdOf.TryGetValue(cdNom, out var cdRef)
                || Text(csv.GetField(index["CT_SP_ID"])) is not { } id || !seen.Add((cdNom, source, id))) {
                continue;
            }
            links.Add(new TaxrefLink(cdNom, cdRef, source, id));
        }
        return links;
    }

    // TAXREF's files are tab-separated, with every text value in double quotes and "" for a quote
    // inside one; a value of TAXREF_LIENS.txt can run over two lines.
    private static CsvReader TabReader(TextReader text) => new(text, new CsvConfiguration(CultureInfo.InvariantCulture) {
        Delimiter = "\t",
        HasHeaderRecord = true,
        BadDataFound = null,
        MissingFieldFound = null,
    });

    private static Dictionary<string, int> Header(CsvReader csv, string fileName, params string[] required) {
        if (!csv.Read() || !csv.ReadHeader() || csv.HeaderRecord is not { } header) {
            throw new InvalidDataException($"{fileName} is empty.");
        }
        var index = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < header.Length; i++) {
            index.TryAdd(header[i].Trim(), i);
        }
        var missing = required.Where(c => !index.ContainsKey(c)).ToList();
        if (missing.Count > 0) {
            throw new InvalidDataException($"{fileName} has no {string.Join(", ", missing)} column. TAXREF's format may have changed.");
        }
        return index;
    }

    private static string? Text(string? value) => value is not null && value.Trim().Length > 0 ? value.Trim() : null;

    private static long? Long(string? text) =>
        long.TryParse(text?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;

    [GeneratedRegex(@"^TAXREFv(?<version>\d+)\.txt$", RegexOptions.IgnoreCase)]
    private static partial Regex NamesFile();
}
