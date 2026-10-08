using System.Text;
using System.Text.RegularExpressions;

// Japan's national Red List, published by the Ministry of the Environment (環境省), as the status
// lists store keeps it: one row per taxon or threatened local population, for the thirteen groups
// the Ministry assesses. The latest list of each group comes from one of two editions:
//   - the 5th Red List (環境省第５次レッドリスト): vascular plants, bryophytes, algae, lichens and
//     fungi (published 2025) and birds, reptiles and amphibians (2026). The Ministry's Biodiversity
//     Center of Japan publishes one CSV file per group on ikilog.biodic.go.jp, listed on the e-Gov
//     data portal as the dataset "レッドリスト/レッドデータブック_第５次レッドリスト";
//   - the Red List 2020 (環境省レッドリスト2020): mammals, brackish and freshwater fishes, insects,
//     molluscs and other invertebrates, until the 5th Red List covers them. It is only a PDF (131
//     pages, every group of 2020), read by JapanRedList2020Pdf; the import keeps the PDF's rows of
//     these five groups only.
// The CSV files are read by JapanRedListCsv. Licence: the Ministry's terms apply the Public Data
// License (Version 1.0), which asks for the source to be named and, when the content is edited, for
// a statement that it was edited and by whom.

namespace BeastieBot3.StatusLists;

/// One of the Ministry's thirteen groups: Key is the store's key ("mammals"), English and Japanese
/// its names (Japanese as the list writes it), Kingdom IUCN's spelling of the kingdom of every taxon
/// in it (null for algae, which IUCN puts in several kingdoms), and ListVersion and ListYear the
/// edition its latest list is in and the year that list was published. FileName is the file it is
/// read from, under the name in its URL.
internal sealed record JapanRedListGroup(
    string Key,
    string English,
    string Japanese,
    string? Kingdom,
    string ListVersion,
    int ListYear,
    string FileName,
    string Url) {
    public bool FromPdf => FileName == JapanRedList.PdfFileName;
}

/// One row of a list: a taxon, or a threatened local population (Category LP) of the taxon named
/// by ScientificName. Category is the code (JapanRedListCategory.Codes); CategoryJapanese the
/// category as the list writes it. Population is the place an LP row names. HigherTaxa, Criteria
/// and ListNumber are as written, where the list gives them. SourcePage is set for the PDF's rows;
/// SourceLine is the CSV row's number in its file, or the line's number on its PDF page.
internal sealed record JapanRedListEntry(
    JapanRedListGroup Group,
    string Category,
    string CategoryJapanese,
    string? JapaneseName,
    string ScientificName,
    string? Population,
    string? HigherTaxa,
    string? Criteria,
    string? ListNumber,
    int? SourcePage,
    int SourceLine);

/// What the import read: the rows, the Red List 2020 PDF's category headings of the groups taken
/// from it (each with the number of rows the heading states and the number read), and the lines
/// and rows that could not be read.
internal sealed record JapanRedListRead(
    IReadOnlyList<JapanRedListEntry> Entries,
    IReadOnlyList<JapanPdfHeading> PdfHeadings,
    IReadOnlyList<string> Problems);

internal static partial class JapanRedList {
    public const string Title = "Red List of the Ministry of the Environment, Japan (環境省レッドリスト)";
    public const string SiteUrl = "https://www.env.go.jp/nature/kisho/hozen/redlist/index.html";
    public const string Licence = "Public Data License (Version 1.0) (PDL1.0)";

    public const string RedList2020 = "Red List 2020";
    public const string FifthRedList = "5th Red List";

    public const string PdfFileName = "900515981.pdf";
    public const string PdfUrl = "https://www.env.go.jp/content/" + PdfFileName;
    private const string Ikilog = "https://ikilog.biodic.go.jp/rdbdata/files/";

    private const string Animalia = "ANIMALIA";
    private const string Plantae = "PLANTAE";
    private const string Fungi = "FUNGI";

    /// The groups in the Ministry's order.
    public static readonly IReadOnlyList<JapanRedListGroup> Groups = [
        Pdf("mammals", "Mammals", "哺乳類"),
        Csv("birds", "Birds", "鳥類", Animalia, 2026, "redlist2026_birds.csv"),
        Csv("reptiles", "Reptiles", "爬虫類", Animalia, 2026, "redlist2026_reptiles.csv"),
        Csv("amphibians", "Amphibians", "両生類", Animalia, 2026, "redlist2026_amphibian.csv"),
        Pdf("fishes", "Brackish and freshwater fishes", "汽水・淡水魚類"),
        Pdf("insects", "Insects", "昆虫類"),
        Pdf("molluscs", "Molluscs", "貝類"),
        Pdf("other-invertebrates", "Other invertebrates", "その他無脊椎動物"),
        Csv("vascular-plants", "Vascular plants", "維管束植物", Plantae, 2025, "redlist2025_ikansoku.csv"),
        Csv("bryophytes", "Bryophytes", "蘚苔類", Plantae, 2025, "redlist2025_sentairui.csv"),
        Csv("algae", "Algae", "藻類", null, 2025, "redlist2025_sorui.csv"),
        Csv("lichens", "Lichens", "地衣類", Fungi, 2025, "redlist2025_chiirui.csv"),
        Csv("fungi", "Fungi", "菌類", Fungi, 2025, "redlist2025_kinrui.csv"),
    ];

    private static JapanRedListGroup Pdf(string key, string english, string japanese) =>
        new(key, english, japanese, Animalia, RedList2020, 2020, PdfFileName, PdfUrl);

    private static JapanRedListGroup Csv(string key, string english, string japanese, string? kingdom, int year, string fileName) =>
        new(key, english, japanese, kingdom, FifthRedList, year, fileName, $"{Ikilog}redlist{year}/{fileName}");

    /// The nine files of an import, each once, in the order they are downloaded: the eight CSV files,
    /// then the PDF.
    public static IReadOnlyList<(string FileName, string Url)> Files { get; } = Groups
        .OrderBy(g => g.FromPdf)
        .Select(g => (g.FileName, g.Url))
        .Distinct()
        .ToList();

    /// The source statement stored in status_source.citation, for the species site: in English, then
    /// in the form of the Ministry's example for edited content under PDL1.0
    /// ("出典：「...」（環境省）（URL）を加工して作成"). PDL1.0 asks for the source and, for edited
    /// content, who edited it. The site adds the download date.
    public const string Citation =
        "Source: Red List 2020 (環境省レッドリスト2020) and 5th Red List (環境省第５次レッドリスト), Ministry of the Environment, Japan. "
        + "The lists are used under the Public Data License (Version 1.0). "
        + "Beastie Bot Species Status edited them: it converted the categories to letter codes and matched the names to species on this site. "
        + "出典：「環境省レッドリスト2020」（環境省）（" + PdfUrl + "）及び「環境省第５次レッドリスト」（環境省）（https://ikilog.biodic.go.jp/）"
        + "を加工してBeastie Bot Species Statusが作成";

    /// Reads the nine files in <paramref name="folder"/>. Throws InvalidDataException when a file is
    /// missing, because the import replaces every Japanese row, or when a file has no rows.
    public static JapanRedListRead Read(string folder) {
        var missing = Files.Select(f => f.FileName).Where(name => !File.Exists(Path.Combine(folder, name))).ToList();
        if (missing.Count > 0) {
            throw new InvalidDataException(
                $"{folder} does not have {string.Join(", ", missing)}. The import needs all {Files.Count} files, because it replaces every stored row of Japan's Red List.");
        }
        var problems = new List<string>();
        var entries = new List<JapanRedListEntry>();
        var pdf = JapanRedList2020Pdf.Read(Path.Combine(folder, PdfFileName));
        problems.AddRange(pdf.Unread);
        foreach (var group in Groups) {
            var rows = group.FromPdf
                ? pdf.Rows.Where(r => r.Group == group.Japanese).Select(r => FromPdf(r, group)).ToList()
                : JapanRedListCsv.Read(Path.Combine(folder, group.FileName), group, problems);
            if (rows.Count == 0) {
                throw new InvalidDataException($"No rows of {group.English} ({group.Japanese}) in {group.FileName}.");
            }
            entries.AddRange(rows);
        }
        var headings = pdf.Headings.Where(h => Groups.Any(g => g.FromPdf && g.Japanese == h.Group)).ToList();
        return new JapanRedListRead(entries, headings, problems);
    }

    private static JapanRedListEntry FromPdf(JapanPdfRow row, JapanRedListGroup group) =>
        new(group, row.Category, row.CategoryWritten, row.JapaneseName, row.ScientificName,
            row.Category == JapanRedListCategory.LocalPopulation ? PopulationOf(row.JapaneseName) : null,
            row.HigherTaxa, null, null, row.Page, row.Line);

    /// The place an LP row's Japanese name gives: the text before its last の ("九州地方のカワネズミ":
    /// 九州地方; "本州の太平洋側湖沼系群のニシン": 本州の太平洋側湖沼系群). Null when the name has no の.
    public static string? PopulationOf(string? japaneseName) {
        var at = japaneseName?.LastIndexOf('の') ?? -1;
        return at > 0 ? japaneseName![..at].Trim() : null;
    }

    /// A scientific name as written, with full-width letters and punctuation made ASCII (NFKC:
    /// "Utricularia ｘ japonica", "Borniopsis sp．") and runs of spaces made one. Letters with
    /// diacritics are kept ("Cladonia koyaënsis").
    public static string CleanScientificName(string name) =>
        Spaces().Replace(name.Normalize(NormalizationForm.FormKC), " ").Trim();

    /// True for "－" (full-width hyphen-minus), "―" (horizontal bar), "-" and empty text: the lists'
    /// marks for "none".
    public static bool IsNone(string? value) =>
        string.IsNullOrWhiteSpace(value) || value.Trim() is "－" or "―" or "-" or "ー";

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();
}

/// The categories of the Ministry's Red List and their codes. The 5th Red List splits 絶滅危惧I類
/// (Endangered) into IA (CR) and IB (EN) for every group; the Red List 2020 does not split it for
/// some taxa of molluscs, other invertebrates, bryophytes, algae, lichens and fungi, which are I類
/// (CR+EN).
internal static partial class JapanRedListCategory {
    public const string CrEn = "CR+EN";
    public const string LocalPopulation = "LP";

    /// Every code, most severe first.
    public static readonly IReadOnlyList<string> Codes = ["EX", "EW", "CR", "EN", CrEn, "VU", "NT", "DD", LocalPopulation];

    // The Japanese name of each category, after Normalize.
    private static readonly Dictionary<string, string> CodeOfName = new(StringComparer.Ordinal) {
        ["絶滅"] = "EX",
        ["野生絶滅"] = "EW",
        ["絶滅危惧IA類"] = "CR",
        ["絶滅危惧IB類"] = "EN",
        ["絶滅危惧I類"] = CrEn,
        ["絶滅危惧II類"] = "VU",
        ["準絶滅危惧"] = "NT",
        ["情報不足"] = "DD",
        ["絶滅のおそれのある地域個体群"] = LocalPopulation,
    };

    /// A category as written, in one form: NFKC (Roman numerals Ⅰ and Ⅱ become I and II,
    /// full-width Ａ and brackets become A and ASCII brackets), with every space removed.
    public static string Normalize(string written) =>
        Space().Replace(written.Normalize(NormalizationForm.FormKC), "");

    /// The code of a category in any form the lists write: "絶滅危惧ⅠＡ類（CR）" (plant CSVs),
    /// "絶滅危惧IB類" and "EN" (the animal CSVs' two columns), "絶滅危惧I類（CR+EN）" and
    /// "情報不足 （DD）" (the PDF's headings). When both a Japanese name and a code in brackets are
    /// given, they must agree. Null for any other text.
    public static string? Code(string written) {
        var text = Normalize(written);
        string? bracketed = null;
        var match = BracketedCode().Match(text);
        if (match.Success) {
            bracketed = match.Groups[1].Value;
            text = text[..match.Index];
            if (!Codes.Contains(bracketed)) {
                return null;
            }
        }
        if (text.Length == 0) {
            return bracketed;
        }
        var named = CodeOfName.TryGetValue(text, out var code) ? code : Codes.Contains(text) ? text : null;
        return bracketed is null || named == bracketed ? named : null;
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex Space();

    [GeneratedRegex(@"\(([A-Z+]+)\)$")]
    private static partial Regex BracketedCode();
}
