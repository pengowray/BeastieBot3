using System.Globalization;
using System.Text;

namespace BeastieBot3.Shared.Wikitext;

// Italics for an IUCN taxon name. The genus and the epithets are italic; rank markers, qualifiers,
// the hybrid sign and everything after the name proper (a subpopulation, an informal 'sp. nov.'
// label, a number) stay upright:
//   ''Panthera pardus'' ssp. ''orientalis''
//   ''Zea mays'' subsp. ''mexicana'' Durango subpopulation
//   ''Crataegus'' x ''canescens''
//   ''Acmella'' sp. nov. 'Ba Tai'
//
// The name is read word by word. After the genus there is room for one epithet (the species), and
// each rank marker makes room for one more. A lowercase word that fits is italic; the first word that
// is not a marker and does not fit starts the upright tail. Over every scientific name in the 2026-1
// CSV this gives the expected result, including "Eschrichtius robustus western subpopulation" (the
// species epithet uses the only room, so "western" starts the tail) and "Ferrissia sp. indet.". When
// the subpopulation name is known it is marked upright directly, whatever words it contains.

public static class ScientificNameMarkup {
    // Rank markers: upright, and each makes room for one more italic epithet.
    private static readonly HashSet<string> RankMarkers = new(StringComparer.Ordinal) {
        "ssp.", "subsp.", "var.", "subvar.", "f.", "fo.", "forma", "subf.",
    };

    // Qualifiers: upright, and they do not change how many epithets may follow.
    // "x" and "×" are the hybrid sign; "cf" without a dot occurs once in the data.
    private static readonly HashSet<string> Qualifiers = new(StringComparer.Ordinal) {
        "×", "x", "cf.", "cf", "aff.", "sp.", "spp.", "nov.", "indet.", "agg.",
    };

    private readonly record struct Segment(string Text, bool Italic);

    // One word of the name; GluedToNext is set on a hybrid sign written without a space ("×Agropogon").
    private readonly record struct Word(string Text, bool Italic, bool GluedToNext = false);

    /// ''Panthera pardus'' ssp. ''orientalis''; ''Panthera leo'' West Africa subpopulation.
    /// Rank markers (ssp., subsp., var., f.) and the subpopulation words stay upright.
    public static string ToWikitext(string scientificName, string? subpopulationName = null) {
        var sb = new StringBuilder();
        foreach (var segment in Split(scientificName, subpopulationName)) {
            if (segment.Italic) {
                sb.Append("''").Append(EscapeItalicApostrophes(segment.Text)).Append("''");
            } else {
                sb.Append(EscapeApostropheRuns(segment.Text));
            }
        }
        return sb.ToString();
    }

    /// The same split as ToWikitext, as HTML-encoded text with <i> elements.
    public static string ToHtml(string scientificName, string? subpopulationName = null) {
        var sb = new StringBuilder();
        foreach (var segment in Split(scientificName, subpopulationName)) {
            if (segment.Italic) {
                sb.Append("<i>").Append(HtmlEncode(segment.Text)).Append("</i>");
            } else {
                sb.Append(HtmlEncode(segment.Text));
            }
        }
        return sb.ToString();
    }

    // Splits the name into alternating italic and upright runs. Spaces between two words of the same
    // kind stay inside the run (''Panthera pardus''); a space between runs is upright text.
    private static List<Segment> Split(string scientificName, string? subpopulationName) {
        var name = CollapseSpaces(scientificName);
        var tail = FindSubpopulationTail(name, subpopulationName);
        var core = tail is null ? name : name[..^tail.Length].TrimEnd();

        var words = new List<Word>();
        var slots = 1;
        var inTail = false;
        var genusSeen = false;
        foreach (var word in core.Split(' ', StringSplitOptions.RemoveEmptyEntries)) {
            if (inTail) {
                words.Add(new Word(word, false));
                continue;
            }
            if (!genusSeen) {
                if (Qualifiers.Contains(word)) {
                    words.Add(new Word(word, false));
                    continue;
                }
                genusSeen = true;
                AddWithLeadingHybridSign(words, word);
                continue;
            }
            if (RankMarkers.Contains(word)) {
                slots++;
                words.Add(new Word(word, false));
                continue;
            }
            if (Qualifiers.Contains(word)) {
                words.Add(new Word(word, false));
                continue;
            }
            if (slots > 0 && IsEpithet(word)) {
                slots--;
                AddWithLeadingHybridSign(words, word);
                continue;
            }
            inTail = true;
            words.Add(new Word(word, false));
        }

        if (tail is not null) {
            words.Add(new Word(tail, false));
        }

        // A space between two italic words stays inside the italic run (''Panthera pardus''); any
        // other space is upright, so a run of italics never starts or ends with a space.
        var segments = new List<Segment>();
        for (var i = 0; i < words.Count; i++) {
            if (i > 0 && !words[i - 1].GluedToNext) {
                Append(segments, " ", words[i - 1].Italic && words[i].Italic);
            }
            Append(segments, words[i].Text, words[i].Italic);
        }
        return segments;
    }

    private static void Append(List<Segment> segments, string text, bool italic) {
        if (segments.Count > 0 && segments[^1].Italic == italic) {
            segments[^1] = segments[^1] with { Text = segments[^1].Text + text };
        } else {
            segments.Add(new Segment(text, italic));
        }
    }

    // "×Agropogon" is the hybrid sign glued to a name: the sign stays upright, the name is italic.
    private static void AddWithLeadingHybridSign(List<Word> words, string word) {
        if (word.Length > 1 && word[0] == '×') {
            words.Add(new Word("×", false, GluedToNext: true));
            words.Add(new Word(word[1..], true));
        } else {
            words.Add(new Word(word, true));
        }
    }

    // A lowercase word of letters, hyphens and underscores ("pardus", "st-hilairei", "ottonis_new").
    private static bool IsEpithet(string word) {
        if (!char.IsLower(word[0])) {
            return false;
        }
        foreach (var c in word) {
            if (!char.IsLetter(c) && c != '-' && c != '_' && char.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark) {
                return false;
            }
        }
        return true;
    }

    // The CSV's subpopulationName includes the word "subpopulation" ("West Africa subpopulation");
    // IucnCitationParts.SubpopulationName may not ("West Africa"). Either way the tail is the name plus
    // a following "subpopulation" when the scientific name has one. Null when the name does not end
    // with it.
    private static string? FindSubpopulationTail(string name, string? subpopulationName) {
        if (string.IsNullOrWhiteSpace(subpopulationName)) {
            return null;
        }
        var subpop = CollapseSpaces(subpopulationName);
        const string word = " subpopulation";
        foreach (var candidate in new[] { subpop + word, subpop }) {
            if (name.Length > candidate.Length
                && name.EndsWith(candidate, StringComparison.OrdinalIgnoreCase)
                && name[name.Length - candidate.Length - 1] == ' ') {
                return name[^candidate.Length..];
            }
        }
        return null;
    }

    private static string CollapseSpaces(string text) {
        var sb = new StringBuilder(text.Length);
        var pendingSpace = false;
        foreach (var c in text) {
            if (char.IsWhiteSpace(c)) {
                pendingSpace = sb.Length > 0;
                continue;
            }
            if (pendingSpace) {
                sb.Append(' ');
                pendingSpace = false;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }

    // Inside ''...'' an apostrophe at either end would merge with the italic markup into ''' (bold).
    private static string EscapeItalicApostrophes(string text) {
        var escaped = EscapeApostropheRuns(text);
        if (escaped.StartsWith('\'')) {
            escaped = "&#39;" + escaped[1..];
        }
        if (escaped.EndsWith('\'')) {
            escaped = escaped[..^1] + "&#39;";
        }
        return escaped;
    }

    // Two or more apostrophes in a row are italic or bold markup in wikitext; a single one is text.
    private static string EscapeApostropheRuns(string text) {
        if (!text.Contains("''", StringComparison.Ordinal)) {
            return text;
        }
        var sb = new StringBuilder(text.Length);
        for (var i = 0; i < text.Length; i++) {
            var c = text[i];
            var inRun = c == '\'' && ((i > 0 && text[i - 1] == '\'') || (i + 1 < text.Length && text[i + 1] == '\''));
            sb.Append(inRun ? "&#39;" : c.ToString());
        }
        return sb.ToString();
    }

    // Only the five characters HTML needs; letters such as ë stay as they are.
    internal static string HtmlEncode(string text) {
        var sb = new StringBuilder(text.Length);
        foreach (var c in text) {
            sb.Append(c switch {
                '&' => "&amp;",
                '<' => "&lt;",
                '>' => "&gt;",
                '"' => "&quot;",
                '\'' => "&#39;",
                _ => c.ToString(),
            });
        }
        return sb.ToString();
    }
}
