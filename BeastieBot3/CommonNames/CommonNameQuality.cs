using System;
using System.Net;
using System.Text.RegularExpressions;

// Finds common names from the sources that are not usable as names, and repairs the ones that are
// a good name plus extra text. A profile of the common names store (October 2026) found these:
//   - Wikipedia taxobox names with wiki markup: a citation template after the name ("Sunda slow
//     loris{sfn|Groves|2005|p=122}"), the next infobox parameter after it ("Chapala chub | image =
//     ..."), {okina} and {lang|..} templates, footnote markers ("mallee[2]");
//   - author and year citations: "Calvert, 1902", "Mountain Ground Skink (Walters, 2008)",
//     "Grewia crenata (J.R.Forst. & G.Forst.) Schinz & Guillaumin (1921), non (Unger) Heer (1857)";
//   - OCR errors in Catalogue of Life names from scanned books: a digit for a letter ("Weil 3
//     bauch-Graslandmaus" for Weißbauch-Graslandmaus), stray backslashes, letters run together
//     ("AtrcanTidenr Bat AfrcanTrdent nosed Bat");
//   - names cut off at a bracket ("Pholidoscelis polops (Cope", "and grant)");
//   - glosses: "Da Xiong Mao (meaning large bear cat)", "meaning large bear cat";
//   - IUCN placeholder names in Wikidata: "Species code: Rc".
// Assess is pure. The two OCR rules for the scanned Catalogue of Life names only apply to names
// labelled English: a lone digit between words ("Libélula de 4 manchas" is a real Spanish name)
// and capitals inside words (romanised Russian in IUCN's data has "MalyiI").
// Names that look odd but are real are kept: "Cassin's 17-year Cicada", "Tortuga B2", "Type 3
// Evening Grosbeak", "European pilchard (=sardine)", "Taw Nwar (aka) Sai", "Манул [manul]".

namespace BeastieBot3.CommonNames;

/// <summary>Whether a common name can be used as it is, after a repair, or not at all.</summary>
internal enum CommonNameVerdict {
    Good,
    Repaired,
    Junk,
}

/// <summary>What <see cref="CommonNameQuality"/> repaired in a name, or why it is junk.</summary>
internal enum CommonNameFlaw {
    None,

    // Repairs: what is left after removing the extra text is the name.

    /// <summary>Backslashes before the name, from OCR ("\\ Woolly Akodont").</summary>
    LeadingBackslash,
    /// <summary>An HTML tag, comment or entity ("Cascading Bean&lt;!--").</summary>
    HtmlMarkup,
    /// <summary>{okina} or {{okina}} for the ʻokina ("Kāwa{okina}u").</summary>
    Okina,
    /// <summary>The whole name inside a lang or nowrap template ("{Lang|haw|Reef triggerfish|italic=no}").</summary>
    LangTemplate,
    /// <summary>Templates after the name, usually a citation ("Sunda slow loris{sfn|Groves|2005|p=122}").</summary>
    TrailingTemplate,
    /// <summary>A wikilink: the link text is kept, a [[File:...]] is removed.</summary>
    WikiLink,
    /// <summary>A footnote marker ("Alexander River mallee[2] or milkshake mallee").</summary>
    FootnoteMarker,
    /// <summary>The next infobox parameter after the name ("Chapala chub | image = ...").</summary>
    ParameterTail,
    /// <summary>URL encoding ("Flinders Ranges%2C Barcoo").</summary>
    PercentEncoding,
    /// <summary>Underscores for spaces ("Mountain_gorilla").</summary>
    Underscore,
    /// <summary>A possessive split by OCR ("Merriam' ’ s Wapiti").</summary>
    SplitApostrophe,
    /// <summary>A translation after the name: "(meaning ...)", or "= ..." ("Da Xiong Mao (meaning large bear cat)").</summary>
    MeaningGloss,
    /// <summary>An author and year in brackets after the name ("Mountain Ground Skink (Walters, 2008)").</summary>
    AuthorYearNote,
    /// <summary>A remark with an exclamation mark in brackets after the name ("(Smallest Mammal!)").</summary>
    ExclamationNote,

    // Junk: the name is not used.

    /// <summary>Nothing is left: no letters, or only an infobox parameter ("| image = ...").</summary>
    Empty,
    /// <summary>Wiki markup that cannot be removed: a template, link, pipe, '' italics or a stray '='.</summary>
    WikiMarkup,
    /// <summary>A year, so an author citation rather than a name ("Calvert, 1902").</summary>
    AuthorCitation,
    /// <summary>A digit standing for a letter, in a name labelled English ("Grol 3 e Hamsterratte").</summary>
    OcrDigit,
    /// <summary>Other OCR errors: a backslash inside the name, a slash for a letter ("(/ asiae)").</summary>
    OcrArtefact,
    /// <summary>Two or more words with capitals inside them, in a name labelled English ("AtrcanTidenr Bat AfrcanTrdent nosed Bat").</summary>
    MixedCaseWords,
    /// <summary>A bracket without its pair ("Pholidoscelis polops (Cope").</summary>
    UnbalancedBrackets,
    /// <summary>A gloss with no name ("meaning large bear cat").</summary>
    GlossOnly,
    /// <summary>IUCN's placeholder for a species without a name ("Species code: Rc").</summary>
    SpeciesCode,
}

/// <summary>The verdict on one name, the name to use (repaired, or as given when Good), and the flaw.</summary>
internal readonly record struct CommonNameAssessment(CommonNameVerdict Verdict, string Name, CommonNameFlaw Flaw) {
    public bool IsJunk => Verdict == CommonNameVerdict.Junk;
    public bool IsRepaired => Verdict == CommonNameVerdict.Repaired;
}

internal static class CommonNameQuality {
    private const RegexOptions Options = RegexOptions.Compiled | RegexOptions.CultureInvariant;

    private static readonly Regex HtmlComment = new(@"<!--.*?(?:-->|$)", Options | RegexOptions.Singleline);
    private static readonly Regex HtmlTag = new(@"<[^<>]+>", Options);
    private static readonly Regex HtmlEntity = new(@"&(?:#\d+|#x[0-9a-f]+|[a-z]+);", Options | RegexOptions.IgnoreCase);
    private static readonly Regex OkinaTemplate = new(@"\{\{?\s*okina\s*\}\}?", Options | RegexOptions.IgnoreCase);
    private static readonly Regex WholeLangTemplate = new(@"^\{\{?\s*(lang|nowrap)\s*\|([^{}]*)\}\}?$", Options | RegexOptions.IgnoreCase);
    private static readonly Regex TrailingTemplates = new(@"(?:\s*\{\{?[^{}]*\}\}?)+\s*$", Options);
    private static readonly Regex FileLink = new(@"\[\[(?:File|Image):[^\]]*\]\]", Options | RegexOptions.IgnoreCase);
    private static readonly Regex WikiLinkPattern = new(@"\[\[(?:[^\]|]*\|)?([^\]|]*)\]\]", Options);
    private static readonly Regex Footnote = new(@"\[\d{1,3}\]", Options);
    private static readonly Regex PercentEscape = new(@"%[0-9A-Fa-f]{2}", Options);
    private static readonly Regex UnderscoreInWord = new(@"(?<=\p{L})_(?=\p{L})", Options);
    private static readonly Regex SplitPossessive = new(@"(\p{L})(['’]) ['’] s\b", Options);
    private static readonly Regex MeaningNote = new(@"\s*\(\s*meaning\b[^()]*\)\s*$", Options | RegexOptions.IgnoreCase);
    private static readonly Regex EqualsGloss = new(@"(?<!\()\s*=\s.*$", Options);
    private static readonly Regex AuthorYearBrackets = new(
        @"\s*\((?:after\s+)?\p{L}[\p{L}\p{M}.&,'’ -]*?,?\s+(?:1[6-9]\d{2}|20\d{2})\)\.?\s*$", Options | RegexOptions.IgnoreCase);
    private static readonly Regex ExclamationBrackets = new(@"\s*\([^()]*!\)\s*$", Options);
    private static readonly Regex Whitespace = new(@"\s+", Options);

    private static readonly Regex SpeciesCodePattern = new(@"^species\s+code\s*:", Options | RegexOptions.IgnoreCase);
    private static readonly Regex GlossStart = new(@"^meaning\b", Options | RegexOptions.IgnoreCase);
    private static readonly Regex Italics = new(@"''[^']+''", Options);
    private static readonly Regex StrayEquals = new(@"(?<!\()=", Options);
    private static readonly Regex SlashForLetter = new(@"\(\s*\p{Ll}?\s*/\s*\p{Ll}", Options);
    private static readonly Regex Year = new(@"\b(?:1[6-9]\d{2}|20\d{2})\b", Options);
    // A digit between words standing for a letter: "Weil 3 bauch", "Gib 6 n", "Goeld 1 ’ s",
    // "Mount Pirr 1 Deermouse". A count is followed by a hyphen ("2-lined") or a capital ("Type 3
    // Evening"), and 0 or 1 is never a count between two words.
    private static readonly Regex DigitForLetter = new(@"\p{L}\s\d\s(?:\p{Ll}|[-’'])|\p{L}\s[01]\s\p{L}", Options);

    /// <summary>
    /// The verdict on <paramref name="name"/> in <paramref name="language"/> (an ISO 639 code,
    /// "en" for English; the two OCR rules only apply to English).
    /// </summary>
    public static CommonNameAssessment Assess(string? name, string? language = "en") {
        if (string.IsNullOrWhiteSpace(name)) {
            return new CommonNameAssessment(CommonNameVerdict.Junk, name ?? string.Empty, CommonNameFlaw.Empty);
        }
        var english = language is null || language.Equals("en", StringComparison.OrdinalIgnoreCase);
        if (!NeedsCheck(name, english)) {
            return new CommonNameAssessment(CommonNameVerdict.Good, name, CommonNameFlaw.None);
        }

        var text = name.Trim();
        var repair = CommonNameFlaw.None;
        void Repaired(CommonNameFlaw flaw) {
            if (repair == CommonNameFlaw.None) {
                repair = flaw;
            }
        }

        if (text[0] == '\\') {
            text = text.TrimStart('\\', ' ', '\t');
            Repaired(CommonNameFlaw.LeadingBackslash);
        }

        if (text.Contains('<') || text.Contains('&')) {
            var before = text;
            text = HtmlComment.Replace(text, string.Empty);
            text = HtmlTag.Replace(text, " ");
            if (HtmlEntity.IsMatch(text)) {
                text = WebUtility.HtmlDecode(text);
            }
            if (text != before) {
                Repaired(CommonNameFlaw.HtmlMarkup);
            }
        }

        if (text.Contains('{')) {
            if (OkinaTemplate.IsMatch(text)) {
                text = OkinaTemplate.Replace(text, "ʻ");
                Repaired(CommonNameFlaw.Okina);
            }
            if (WholeLangTemplate.Match(text.Trim()) is { Success: true } lang) {
                var parts = lang.Groups[2].Value.Split('|');
                var isLang = lang.Groups[1].Value.Equals("lang", StringComparison.OrdinalIgnoreCase);
                if (isLang && parts[0].Trim().Equals("la", StringComparison.OrdinalIgnoreCase)) {
                    // Latin: the scientific name in a lang template.
                    return Junk(text, CommonNameFlaw.WikiMarkup);
                }
                var inner = isLang ? (parts.Length > 1 ? parts[1] : string.Empty) : parts[0];
                text = inner;
                Repaired(CommonNameFlaw.LangTemplate);
            }
            var withoutTemplates = TrailingTemplates.Replace(text, string.Empty);
            if (withoutTemplates != text && HasLetter(withoutTemplates)) {
                text = withoutTemplates;
                Repaired(CommonNameFlaw.TrailingTemplate);
            }
        }

        if (text.Contains("[[", StringComparison.Ordinal)) {
            text = FileLink.Replace(text, string.Empty);
            text = WikiLinkPattern.Replace(text, "$1");
            Repaired(CommonNameFlaw.WikiLink);
        }

        if (text.Contains('[') && Footnote.IsMatch(text)) {
            text = Footnote.Replace(text, string.Empty);
            Repaired(CommonNameFlaw.FootnoteMarker);
        }

        var pipe = text.IndexOf('|');
        if (pipe >= 0) {
            text = text[..pipe];
            if (!HasLetter(text)) {
                return Junk(name, CommonNameFlaw.Empty);
            }
            Repaired(CommonNameFlaw.ParameterTail);
        }

        if (text.Contains('%') && PercentEscape.IsMatch(text)) {
            try {
                text = Uri.UnescapeDataString(text);
                Repaired(CommonNameFlaw.PercentEncoding);
            } catch (UriFormatException) {
                // Left as it is; the junk checks below decide.
            }
        }

        if (text.Contains('_') && UnderscoreInWord.IsMatch(text)) {
            text = UnderscoreInWord.Replace(text, " ");
            Repaired(CommonNameFlaw.Underscore);
        }

        if (SplitPossessive.IsMatch(text)) {
            text = SplitPossessive.Replace(text, "$1$2s");
            Repaired(CommonNameFlaw.SplitApostrophe);
        }

        if (text.Contains('(') || text.Contains('=')) {
            text = RemoveNote(text, MeaningNote, CommonNameFlaw.MeaningGloss, Repaired);
            text = RemoveNote(text, EqualsGloss, CommonNameFlaw.MeaningGloss, Repaired);
            text = RemoveNote(text, AuthorYearBrackets, CommonNameFlaw.AuthorYearNote, Repaired);
            text = RemoveNote(text, ExclamationBrackets, CommonNameFlaw.ExclamationNote, Repaired);
        }

        text = Whitespace.Replace(text, " ").Trim();
        if (!HasLetter(text)) {
            return Junk(name, CommonNameFlaw.Empty);
        }

        if (JunkFlaw(text, english) is { } junk) {
            return Junk(name, junk);
        }
        return repair == CommonNameFlaw.None
            ? new CommonNameAssessment(CommonNameVerdict.Good, name, CommonNameFlaw.None)
            : new CommonNameAssessment(CommonNameVerdict.Repaired, text, repair);
    }

    private static CommonNameFlaw? JunkFlaw(string text, bool english) {
        if (SpeciesCodePattern.IsMatch(text)) {
            return CommonNameFlaw.SpeciesCode;
        }
        if (GlossStart.IsMatch(text)) {
            return CommonNameFlaw.GlossOnly;
        }
        if (text.IndexOfAny(['{', '}', '|', '<', '>']) >= 0
            || text.Contains("[[", StringComparison.Ordinal) || text.Contains("]]", StringComparison.Ordinal)
            || Italics.IsMatch(text) || StrayEquals.IsMatch(text)) {
            return CommonNameFlaw.WikiMarkup;
        }
        if (text.Contains('\\') || text[0] == '/' || SlashForLetter.IsMatch(text)) {
            return CommonNameFlaw.OcrArtefact;
        }
        if (!BracketsBalance(text)) {
            return CommonNameFlaw.UnbalancedBrackets;
        }
        if (Year.IsMatch(text)) {
            return CommonNameFlaw.AuthorCitation;
        }
        if (english && DigitForLetter.IsMatch(text)) {
            return CommonNameFlaw.OcrDigit;
        }
        if (english && MixedCaseWordCount(text) >= 2) {
            return CommonNameFlaw.MixedCaseWords;
        }
        return null;
    }

    private static string RemoveNote(string text, Regex note, CommonNameFlaw flaw, Action<CommonNameFlaw> repaired) {
        var match = note.Match(text);
        if (!match.Success || match.Index == 0) {
            return text;
        }
        var left = text[..match.Index];
        if (!HasLetter(left)) {
            return text;
        }
        repaired(flaw);
        return left;
    }

    private static CommonNameAssessment Junk(string name, CommonNameFlaw flaw) =>
        new(CommonNameVerdict.Junk, name, flaw);

    // Only names with one of these can need a repair or be junk, so most names skip the regexes.
    private static bool NeedsCheck(string name, bool english) {
        var previousLower = false;
        for (var i = 0; i < name.Length; i++) {
            var c = name[i];
            switch (c) {
                case '\\' or '|' or '{' or '}' or '[' or ']' or '<' or '>' or '=' or '_' or '%' or '(' or ')'
                    or '&' or ':' or '!' or '/':
                    return true;
                case '\'' or '’' when i + 1 < name.Length && (name[i + 1] == ' ' || name[i + 1] == '\''):
                    return true;
            }
            if (char.IsDigit(c) || (previousLower && char.IsUpper(c))) {
                return true;
            }
            previousLower = char.IsLower(c);
        }
        return name.Contains("meaning", StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasLetter(string text) {
        foreach (var c in text) {
            if (char.IsLetter(c)) {
                return true;
            }
        }
        return false;
    }

    private static bool BracketsBalance(string text) {
        var round = 0;
        var square = 0;
        foreach (var c in text) {
            switch (c) {
                case '(':
                    round++;
                    break;
                case ')':
                    if (--round < 0) return false;
                    break;
                case '[':
                    square++;
                    break;
                case ']':
                    if (--square < 0) return false;
                    break;
            }
        }
        return round == 0 && square == 0;
    }

    // Words with a capital after a run of three or more lower-case letters: OCR runs words
    // together ("AtrcanTidenr"). Name prefixes have two lower-case letters at most (McDowell,
    // MacArthur, KwaZulu, DeKay, uMlalazi); Fitz is the exception (FitzSimons).
    private static int MixedCaseWordCount(string text) {
        var count = 0;
        foreach (var word in text.Split(' ', StringSplitOptions.RemoveEmptyEntries)) {
            var lowerRun = 0;
            var runStart = 0;
            for (var i = 0; i < word.Length; i++) {
                var c = word[i];
                if (char.IsLower(c)) {
                    if (lowerRun == 0) {
                        runStart = i;
                    }
                    lowerRun++;
                    continue;
                }
                if (char.IsUpper(c) && lowerRun >= 3 && !IsFitz(word, runStart)) {
                    count++;
                    break;
                }
                lowerRun = 0;
            }
        }
        return count;
    }

    private static bool IsFitz(string word, int lowerRunStart) =>
        lowerRunStart == 1 && word.StartsWith("Fitz", StringComparison.Ordinal);
}
