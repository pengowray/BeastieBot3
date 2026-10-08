using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UglyToad.PdfPig;

// The words of a PDF page as lines of text with x positions, for the table parsers. IUCN's summary
// tables are spreadsheets saved as PDF: every row is one line of words, and a column is found by
// where its words are, not by the spaces between them.
//
// Words are built from the page's letters in the order the page draws them, not with PdfPig's word
// extractor: on some lines of the 2014-2 Table 7 that extractor joins letters of one column to a
// word of another ("ilac-crowned", with the "L" and the "V" of "VU" elsewhere). A word ends at a
// space, at a change of font, or where the next letter does not start just after the last one
// ends. Words that touch are then joined ("L" + "ilac-crowned"). Keeping each word's font lets the
// parser keep a cell that overflows into the next column, such as a long group name over the
// scientific name in the 2025-1 table, apart from the text it overlaps.

namespace BeastieBot3.Iucn.SummaryTables;

/// One word on a page. Left and Right are in PDF points from the left edge of the page.
internal sealed record PdfWord(string Text, double Left, double Right, string Font = "") {
    public double Center => (Left + Right) / 2;
}

/// The words on one line of a page, left to right. Page counts from 1. Baseline is in PDF points
/// from the bottom of the page.
internal sealed record PdfLine(int Page, double Baseline, IReadOnlyList<PdfWord> Words) {
    public string Text => string.Join(' ', Words.Select(w => w.Text));
}

/// One letter in drawing order: its text, where its baseline starts and ends, the baseline's height
/// on the page, and its font.
internal readonly record struct PdfLetter(string Value, double Left, double Right, double Baseline, string Font);

internal static class PdfLines {
    // Letters whose baselines are this close (in points) are on the same line.
    private const double SameLineTolerance = 2.0;
    // A gap wider than this (in points) between two letters ends a word.
    private const double WordGap = 1.0;

    /// Every line of every page, top to bottom.
    public static IReadOnlyList<PdfLine> Read(string path, out int pageCount) {
        using var document = PdfDocument.Open(path);
        pageCount = document.NumberOfPages;
        var lines = new List<PdfLine>();
        foreach (var page in document.GetPages()) {
            var letters = page.Letters.Select(l => new PdfLetter(l.Value, l.StartBaseLine.X, l.EndBaseLine.X, l.StartBaseLine.Y, l.FontName ?? ""));
            lines.AddRange(Group(page.Number, letters));
        }
        return lines;
    }

    /// Builds words from letters in drawing order, then groups the words into lines, top to bottom,
    /// each line's words left to right.
    internal static IReadOnlyList<PdfLine> Group(int page, IEnumerable<PdfLetter> letters) {
        var words = new List<(PdfWord Word, double Baseline)>();
        var text = new StringBuilder();
        PdfLetter last = default;
        double left = 0, right = 0;
        void Flush() {
            if (text.Length > 0) words.Add((new PdfWord(text.ToString(), left, right, last.Font), last.Baseline));
            text.Clear();
        }
        foreach (var letter in letters) {
            if (string.IsNullOrWhiteSpace(letter.Value)) {
                Flush();
                continue;
            }
            if (text.Length > 0 && (Math.Abs(letter.Baseline - last.Baseline) > SameLineTolerance || letter.Font != last.Font
                    || letter.Left < right - WordGap || letter.Left - right > WordGap)) {
                Flush();
            }
            if (text.Length == 0) {
                left = letter.Left;
                right = letter.Right;
            } else {
                right = Math.Max(right, letter.Right);
            }
            text.Append(letter.Value);
            last = letter;
        }
        Flush();

        var lines = new List<PdfLine>();
        var current = new List<PdfWord>();
        double baseline = 0;
        foreach (var (word, wordBaseline) in words.OrderByDescending(w => w.Baseline).ThenBy(w => w.Word.Left)) {
            if (current.Count > 0 && Math.Abs(wordBaseline - baseline) > SameLineTolerance) {
                lines.Add(new PdfLine(page, baseline, JoinTouching(current)));
                current = new List<PdfWord>();
            }
            if (current.Count == 0) baseline = wordBaseline;
            current.Add(word);
        }
        if (current.Count > 0) lines.Add(new PdfLine(page, baseline, JoinTouching(current)));
        return lines;
    }

    /// Joins a word to the one before it when it starts where that one ends. The fonts may differ:
    /// the 2019-3 table draws its hyphens ("2019\u20103", "Sun\u2010tailed") in a font of their own.
    private static List<PdfWord> JoinTouching(List<PdfWord> words) {
        var joined = new List<PdfWord>();
        foreach (var word in words.OrderBy(w => w.Left)) {
            var previous = joined.Count > 0 ? joined[^1] : null;
            if (previous is not null && word.Left >= previous.Right - WordGap && word.Left - previous.Right <= WordGap) {
                joined[^1] = previous with { Text = previous.Text + word.Text, Right = Math.Max(previous.Right, word.Right) };
            } else {
                joined.Add(word);
            }
        }
        return joined;
    }
}
