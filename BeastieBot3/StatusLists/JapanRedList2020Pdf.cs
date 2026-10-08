using System.Text.RegularExpressions;
using BeastieBot3.Iucn.SummaryTables;

// The Ministry of the Environment's Red List 2020 (環境省レッドリスト2020) as published: a PDF of
// 131 pages, "別添資料３" of the Ministry's announcement, with every group of 2020 one after another.
// PdfLines reads its words with their x positions. The lines are:
//   - a running header on every page: "別添資料３" and the group in brackets ("【哺乳類】");
//   - a page footer: "5 / 131 ページ";
//   - a group's title: "【哺乳類】環境省レッドリスト2020";
//   - a category heading with the number of rows under it: "●絶滅危惧IA類（CR） 12種", or
//     "26集団" (populations) under 絶滅のおそれのある地域個体群（LP）;
//   - "―" or "－" under a heading with no rows;
//   - one row per taxon or population, in columns: for insects the order ("コウチュウ目"), for other
//     invertebrates the phylum, class and order ("節足動物門 甲殻綱 エビ目"); then the Japanese name;
//     then the scientific name without authors (subspecies as trinomials). The plant groups add
//     authors; those groups come from the 5th Red List's CSV files instead.
// The scientific name's column starts where the heading's count starts (within a point), so every
// word that starts at or after that x is part of the scientific name. pdftotext -layout puts two
// names of this PDF on the line above or below their row (オキナワキムラグモ（広義）,
// アユミコケムシ); PdfLines puts them on their row.

namespace BeastieBot3.StatusLists;

/// One row of the PDF. Group is the group's Japanese name as its title gives it; CategoryWritten the
/// heading's text without the "●" and the count; Line the line's number on its page, top to bottom.
internal sealed record JapanPdfRow(
    string Group,
    string CategoryWritten,
    string Category,
    string? HigherTaxa,
    string JapaneseName,
    string ScientificName,
    int Page,
    int Line);

/// A category heading: the number of rows it states ("12種") and the number of rows read under it.
/// Category is null when the heading's text is not a known category.
internal sealed record JapanPdfHeading(string Group, string CategoryWritten, string? Category, int Stated, int Page) {
    public int Read { get; set; }
}

internal sealed record JapanPdfParse(IReadOnlyList<JapanPdfRow> Rows, IReadOnlyList<JapanPdfHeading> Headings, IReadOnlyList<string> Unread);

internal static partial class JapanRedList2020Pdf {
    // A word that starts this many points left of the heading's count is still in the scientific
    // name's column. The column starts 0.2 to 0.5 points left of the count in the 2020 PDF.
    private const double ColumnTolerance = 3.0;

    public static JapanPdfParse Read(string path) => Parse(PdfLines.Read(path, out _));

    internal static JapanPdfParse Parse(IReadOnlyList<PdfLine> lines) {
        var rows = new List<JapanPdfRow>();
        var headings = new List<JapanPdfHeading>();
        var unread = new List<string>();
        string? group = null;
        JapanPdfHeading? heading = null;
        double column = 0;
        var page = 0;
        var lineOnPage = 0;
        foreach (var line in lines) {
            lineOnPage = line.Page == page ? lineOnPage + 1 : 1;
            page = line.Page;
            var text = line.Text.Trim();
            if (text.Length == 0 || Footer().IsMatch(text) || text.StartsWith("別添資料", StringComparison.Ordinal) || RunningHeader().IsMatch(text)) {
                continue;
            }
            var title = GroupTitle().Match(text);
            if (title.Success) {
                group = title.Groups[1].Value;
                heading = null;
                continue;
            }
            if (text.StartsWith('●')) {
                var count = line.Words.Count > 1 ? Count().Match(line.Words[^1].Text) : Match.Empty;
                if (group is null || !count.Success) {
                    unread.Add($"page {page}, line {lineOnPage}: {text}");
                    heading = null;
                    continue;
                }
                var written = string.Join(' ', line.Words.Take(line.Words.Count - 1).Select(w => w.Text)).TrimStart('●').Trim();
                heading = new JapanPdfHeading(group, written, JapanRedListCategory.Code(written), int.Parse(count.Groups[1].Value), page);
                headings.Add(heading);
                column = line.Words[^1].Left;
                continue;
            }
            if (JapanRedList.IsNone(text)) {
                continue;
            }
            var row = heading is { Category: not null } ? ReadRow(line, column) : null;
            if (row is null) {
                unread.Add($"page {page}, line {lineOnPage}: {text}");
                continue;
            }
            rows.Add(new JapanPdfRow(group!, heading!.CategoryWritten, heading.Category!, row.Value.HigherTaxa, row.Value.JapaneseName,
                row.Value.ScientificName, page, lineOnPage));
            heading.Read++;
        }
        return new JapanPdfParse(rows, headings, unread);
    }

    // The columns of one row, or null when the words do not make a row: no scientific name, a
    // scientific name with Japanese letters in it or not starting with a letter, or no Japanese name.
    private static (string? HigherTaxa, string JapaneseName, string ScientificName)? ReadRow(PdfLine line, double column) {
        var scientific = line.Words.Where(w => w.Left >= column - ColumnTolerance).Select(w => w.Text).ToList();
        var before = line.Words.Where(w => w.Left < column - ColumnTolerance).Select(w => w.Text).ToList();
        if (scientific.Count == 0 || before.Count == 0 || !char.IsAsciiLetter(scientific[0][0]) || scientific.Any(w => Japanese().IsMatch(w))) {
            return null;
        }
        // The classification columns: the words before the name that end in 門 (phylum), 綱
        // (class) or 目 (order). At least one word is left for the name.
        var taxa = 0;
        while (taxa < before.Count - 1 && Classification().IsMatch(before[taxa])) {
            taxa++;
        }
        return (taxa > 0 ? string.Join(' ', before.Take(taxa)) : null,
            string.Join(' ', before.Skip(taxa)),
            JapanRedList.CleanScientificName(string.Join(' ', scientific)));
    }

    [GeneratedRegex(@"^\d+ / \d+ ページ$")]
    private static partial Regex Footer();

    [GeneratedRegex(@"^【[^】]+】$")]
    private static partial Regex RunningHeader();

    [GeneratedRegex(@"^【([^】]+)】環境省レッドリスト2020$")]
    private static partial Regex GroupTitle();

    [GeneratedRegex(@"^(\d+)(種|集団)$")]
    private static partial Regex Count();

    [GeneratedRegex(@"(門|綱|目)$")]
    private static partial Regex Classification();

    // Hiragana, katakana, kanji and Japanese punctuation (、・「」).
    [GeneratedRegex(@"[\p{IsHiragana}\p{IsKatakana}\p{IsCJKUnifiedIdeographs}\p{IsCJKSymbolsandPunctuation}]")]
    private static partial Regex Japanese();
}
