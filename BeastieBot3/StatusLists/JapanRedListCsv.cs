using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;

// The CSV files of the Ministry of the Environment's 5th Red List, one per group. Two layouts:
//   - birds, reptiles and amphibians (redlist2026_*.csv, 178 columns): three heading rows, the
//     column numbers, then the block each column is in ("5thRL", "RL2020" ... "1stRL", then habitat,
//     region and threat blocks), then the column names. The 5thRL block has 掲載No. (list number,
//     BI0001), 分科会名 (the committee: 爬虫類・両生類 for both reptiles and amphibians), カテゴリーJPN,
//     カテゴリーENG (EX, EW, CR, EN, VU, NT, DD, LP), 目名 (order), 科名 (family), 和名, 学名 and
//     判定基準 (criteria). Only that block is read: the earlier lists' names and categories, the
//     habitat columns, the region columns and the threat columns are left out;
//   - vascular plants, bryophytes, algae, lichens and fungi (redlist2025_*.csv): one heading row,
//     カテゴリー ("絶滅危惧ⅠＡ類（CR）"), 分類群 (the group), 和名 and 学名.
// The files do not share an encoding: in April 2026 the vascular plant, bryophyte, algae and fungi
// files were Shift_JIS, and the bird, reptile, amphibian and lichen files UTF-8 with a byte order
// mark, so each is decoded by what it holds.

namespace BeastieBot3.StatusLists;

internal static class JapanRedListCsv {
    // Column names.
    internal const string ListNumber = "掲載No.";
    internal const string CategoryJapanese = "カテゴリーJPN";
    internal const string CategoryEnglish = "カテゴリーENG";
    internal const string Category = "カテゴリー";
    internal const string Order = "目名";
    internal const string Family = "科名";
    internal const string JapaneseName = "和名";
    internal const string ScientificName = "学名";
    internal const string Criteria = "判定基準";

    // The block of the animal files' columns that is the 5th Red List.
    internal const string FifthBlock = "5thRL";

    // The heading row is searched for in the first rows of a file.
    private const int HeaderSearchRows = 5;

    /// Reads one group's file. Rows that cannot be read are skipped and described in
    /// <paramref name="problems"/>.
    public static IReadOnlyList<JapanRedListEntry> Read(string path, JapanRedListGroup group, ICollection<string> problems) {
        using var reader = new StringReader(Decode(File.ReadAllBytes(path)));
        return Read(reader, group, problems);
    }

    /// The text of a file: UTF-8 when it starts with a byte order mark or is valid UTF-8, else
    /// Shift_JIS (Windows code page 932, which .NET has only with CodePagesEncodingProvider).
    public static string Decode(byte[] bytes) {
        if (bytes.AsSpan().StartsWith(Encoding.UTF8.Preamble)) {
            return Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
        }
        try {
            return new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetString(bytes);
        } catch (DecoderFallbackException) {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            return Encoding.GetEncoding(932).GetString(bytes);
        }
    }

    /// Reads the rows of either layout. Throws InvalidDataException when no heading row has 和名 and
    /// 学名, or when the category column is missing.
    internal static IReadOnlyList<JapanRedListEntry> Read(TextReader reader, JapanRedListGroup group, ICollection<string> problems) {
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) {
            HasHeaderRecord = false,
            BadDataFound = null,
            MissingFieldFound = null,
        });
        var rows = new List<(int Line, string[] Cells)>();
        while (csv.Read()) {
            rows.Add((csv.Parser.Row, csv.Parser.Record ?? []));
        }

        var headingIndex = rows.Take(HeaderSearchRows).ToList()
            .FindIndex(r => r.Cells.Contains(JapaneseName) && r.Cells.Contains(ScientificName));
        if (headingIndex < 0) {
            throw new InvalidDataException($"{group.FileName}: no heading row with the columns {JapaneseName} and {ScientificName} in the first {HeaderSearchRows} rows.");
        }
        var headings = rows[headingIndex].Cells;
        // In the animal files, the row above the column names names each column's block.
        var blocks = headingIndex > 0 && rows[headingIndex - 1].Cells.Contains(FifthBlock) ? rows[headingIndex - 1].Cells : null;
        int Column(string name) {
            for (var i = 0; i < headings.Length; i++) {
                if (headings[i].Trim() == name && (blocks is null || (i < blocks.Length && blocks[i].Trim() == FifthBlock))) {
                    return i;
                }
            }
            return -1;
        }

        var english = Column(CategoryEnglish);
        var japanese = english >= 0 ? Column(CategoryJapanese) : Column(Category);
        if (japanese < 0 && english < 0) {
            throw new InvalidDataException($"{group.FileName}: no column {CategoryEnglish} or {Category}.");
        }
        var name = Column(JapaneseName);
        var scientific = Column(ScientificName);
        var number = Column(ListNumber);
        var order = Column(Order);
        var family = Column(Family);
        var criteria = Column(Criteria);

        var entries = new List<JapanRedListEntry>();
        foreach (var (line, cells) in rows.Skip(headingIndex + 1)) {
            string? Cell(int index) => index >= 0 && index < cells.Length && !JapanRedList.IsNone(cells[index]) ? cells[index].Trim() : null;
            if (cells.All(string.IsNullOrWhiteSpace)) {
                continue;
            }
            var writtenJapanese = Cell(japanese);
            var writtenEnglish = Cell(english);
            var code = writtenEnglish is not null ? JapanRedListCategory.Code(writtenEnglish) : null;
            var codeOfJapanese = writtenJapanese is not null ? JapanRedListCategory.Code(writtenJapanese) : null;
            var category = english >= 0 ? (codeOfJapanese is null || codeOfJapanese == code ? code : null) : codeOfJapanese;
            var scientificName = Cell(scientific);
            if (category is null || scientificName is null) {
                problems.Add($"{group.FileName}, row {line}: " + (category is null
                    ? $"unknown category \"{writtenJapanese} {writtenEnglish}\""
                    : "no scientific name"));
                continue;
            }
            var japaneseName = Cell(name);
            var higherTaxa = string.Join(' ', new[] { Cell(order), Cell(family) }.OfType<string>());
            entries.Add(new JapanRedListEntry(
                group, category, writtenJapanese ?? writtenEnglish!, japaneseName, JapanRedList.CleanScientificName(scientificName),
                category == JapanRedListCategory.LocalPopulation ? JapanRedList.PopulationOf(japaneseName) : null,
                higherTaxa.Length > 0 ? higherTaxa : null, Cell(criteria), Cell(number), null, line));
        }
        return entries;
    }
}
