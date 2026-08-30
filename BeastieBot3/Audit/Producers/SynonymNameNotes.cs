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
// Not every square bracket is a note. Three bracket uses are standard nomenclature and are counted
// but not reported as issues:
//   - an inferred publication year: "[1803]"
//   - an inferred or attributed author: "Anonymous [Bennett], 1830", "[Denis & Schiffermüller], 1775"
//   - an expansion of an abbreviated name, where the bracket touches a letter: "S[imia] erythropyga"

namespace BeastieBot3.Audit.Producers;

internal enum SynonymNoteKind {
    /// A worded nomenclatural note ("orth. error", "nom. nud.", "fide Smith, 1935", ...).
    Note,
    /// A bracketed publication year: "[1803]".
    Year,
    /// A bracketed author name or initials: "[Bennett]", "[A.]", "[Denis & Schiffermüller]".
    Author,
    /// Brackets expanding an abbreviated name in place: "S[imia]".
    Expansion,
}

internal readonly record struct SynonymNameNote(string Text, SynonymNoteKind Kind) {
    public bool IsWordNote => Kind == SynonymNoteKind.Note;

    /// Notes that differ only in punctuation, spacing or case share a key, which is how the
    /// spellings of one note are grouped together.
    public string Key => SynonymNameNotes.VariantKey(Text);
}

internal static class SynonymNameNotes {
    private static readonly Regex Bracketed = new(@"\[([^\[\]]*)\]", RegexOptions.Compiled);
    private static readonly Regex ShortTag = new("<[^<>]{1,64}>", RegexOptions.Compiled);
    private static readonly Regex YearOnly = new(@"^\(?\d{4}\)?(\s*[-/]\s*\d{2,4})?\??$", RegexOptions.Compiled);

    // Words that mark bracketed text as a nomenclatural note even when it is capitalised like a
    // name ("Illegitimate", "Orth. error", "Natio baracoensis"). Matched case-insensitively as
    // substrings of the bracketed text.
    private static readonly string[] NoteWords = {
        "orth", "oth.", "ort.", "nom", "nomen", "fide", "error", "eror", "inval", "invalid",
        "illegit", "preoccup", "pre-occup", "pro parte", "partim", "sic", "homonym", "synonym",
        "emend", "ememd", "spelling", "misspel", "unavailable", "unpublished", "replacement",
        "hybrid", "superfl", "oblit", "dubium", "novum", "suppress", "subst", "isonym", "lapsus",
        "apsus", "sched", "unaccepted", "incorrect", "transferred", "typo", "gender", "cited",
        "original", "earliest", "ambiguous", "alternate", "junior", "natio", "comb.", "des.",
        "rej.", "in part", "in error", "used previously", "skull",
    };

    /// Every bracketed run in the name, in the order they appear, each classified as a worded
    /// note, a publication year, an author attribution, or an in-name expansion. Empty when there
    /// are none.
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
            notes.Add(new SynonymNameNote(inner, ClassifyMatch(name, m, inner)));
        }
        return notes;
    }

    /// The worded notes only: the ones that leave the field holding more than a name.
    public static IReadOnlyList<SynonymNameNote> FindWordNotes(string? name) =>
        Find(name).Where(n => n.IsWordNote).ToList();

    /// The name with its worded-note brackets taken out. Standard bracket uses (years, author
    /// attributions, expansions) are kept. Whitespace is left for TextIrregularities.Clean so both
    /// reports suggest the same normalised text.
    public static string StripNotes(string name) =>
        Bracketed.Replace(name, m => {
            var inner = ShortTag.Replace(m.Groups[1].Value, "").Trim();
            return inner.Length == 0 || ClassifyMatch(name, m, inner) == SynonymNoteKind.Note ? " " : m.Value;
        });

    private static SynonymNoteKind ClassifyMatch(string name, Match m, string inner) {
        // A bracket that touches a letter is expanding an abbreviation in place: "S[imia]".
        var before = m.Index > 0 ? name[m.Index - 1] : ' ';
        var afterIdx = m.Index + m.Length;
        var after = afterIdx < name.Length ? name[afterIdx] : ' ';
        if (char.IsLetter(before) || char.IsLetter(after)) {
            return SynonymNoteKind.Expansion;
        }
        if (YearOnly.IsMatch(inner)) {
            return SynonymNoteKind.Year;
        }
        var lower = inner.ToLowerInvariant();
        if (NoteWords.Any(w => lower.Contains(w, StringComparison.Ordinal))) {
            return SynonymNoteKind.Note;
        }
        return LooksLikeAuthor(inner) ? SynonymNoteKind.Author : SynonymNoteKind.Note;
    }

    // True when the bracketed text reads as an author name or initials: capitalised words (with
    // diacritics, dots, hyphens and apostrophes), "&" between co-authors, and the lowercase
    // particles that occur in surnames. No digits, and at least one capitalised token.
    private static bool LooksLikeAuthor(string text) {
        if (text.Any(char.IsDigit)) {
            return false;
        }
        var tokens = text.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length == 0) {
            return false;
        }
        var sawCapital = false;
        foreach (var token in tokens) {
            if (token is "&" or "&amp;") {
                continue;
            }
            if (token is "van" or "von" or "de" or "der" or "den" or "da" or "del" or "di" or "le" or "la" or "f." or "f") {
                continue;
            }
            if (char.IsUpper(token[0]) && token.All(c => char.IsLetter(c) || c is '.' or '-' or '\'' or '’')) {
                sawCapital = true;
                continue;
            }
            return false;
        }
        return sawCapital;
    }

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
