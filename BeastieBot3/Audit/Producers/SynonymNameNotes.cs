using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

// Finds nomenclatural notes written inside a synonym's name field, such as
// "Eumeces schneideri (Daudin, 1802) [orth. error]", and separates them from the name itself.
//
// Two things make this worth reporting apart from the whitespace and markup scans. The name field
// stops being a name, so anything that parses it gets the note as part of the text. And the same
// note is written several ways ("orth. error", "orth. err.", "orth error", "orth.error"), which
// only shows up when the notes are listed together.
//
// A square-bracketed year is left alone: "[1803]" is the standard way of recording an inferred
// publication date and belongs to the authority, not to the name.

namespace BeastieBot3.Audit.Producers;

internal readonly record struct SynonymNameNote(string Text, bool IsDate) {
    /// Notes that differ only in punctuation, spacing or case share a key, which is how the
    /// spellings of one note are grouped together.
    public string Key => SynonymNameNotes.VariantKey(Text);
}

internal static class SynonymNameNotes {
    private static readonly Regex Bracketed = new(@"\[([^\[\]]*)\]", RegexOptions.Compiled);
    private static readonly Regex ShortTag = new("<[^<>]{1,64}>", RegexOptions.Compiled);
    private static readonly Regex YearOnly = new(@"^\(?\d{4}\)?(\s*[-/]\s*\d{2,4})?\??$", RegexOptions.Compiled);

    /// Every bracketed run in the name, in the order they appear. Empty when there are none.
    public static IReadOnlyList<SynonymNameNote> Find(string? name) {
        if (string.IsNullOrEmpty(name) || name.IndexOf('[') < 0) {
            return Array.Empty<SynonymNameNote>();
        }

        var notes = new List<SynonymNameNote>();
        foreach (Match m in Bracketed.Matches(name)) {
            var inner = ShortTag.Replace(m.Groups[1].Value, "").Trim();
            if (inner.Length == 0) {
                continue;
            }
            notes.Add(new SynonymNameNote(inner, YearOnly.IsMatch(inner)));
        }
        return notes;
    }

    /// The notes that are not a publication year: the ones that leave the field unparseable.
    public static IReadOnlyList<SynonymNameNote> FindWordNotes(string? name) =>
        Find(name).Where(n => !n.IsDate).ToList();

    /// The name with its bracketed runs taken out. Whitespace is left for TextIrregularities.Clean
    /// so both reports suggest the same normalised text.
    public static string StripNotes(string name) => Bracketed.Replace(name, " ");

    /// Punctuation, spacing and case removed, so "orth. error", "orth error" and "orth.error"
    /// group together. Not shown to anyone; it only decides what counts as the same note.
    public static string VariantKey(string text) {
        var sb = new StringBuilder(text.Length);
        foreach (var c in text) {
            if (char.IsLetterOrDigit(c)) {
                sb.Append(char.ToLowerInvariant(c));
            }
        }
        return sb.ToString();
    }
}
