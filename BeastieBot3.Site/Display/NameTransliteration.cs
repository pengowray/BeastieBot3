using System.Text;
using ICU4N.Globalization;
using ICU4N.Text;

namespace BeastieBot3.Site.Display;

/// The Latin-letter transliteration of a common name written in another script, shown under the
/// name on taxon pages: ICU's Any-Latin transform (pinyin with tone marks for Chinese, Hepburn-style
/// romaji for kana, the Revised Romanization's transliteration form for Hangul, ISO-style
/// transliterations for Cyrillic, Greek, Indic and other alphabets). Letters that ICU4N's data has no
/// transform for are transliterated by AnyAscii, in the scripts where its output keeps the vowels
/// (FallbackScripts).
///
/// Left out (null): a name already in Latin letters; a name in a script written without most of its
/// vowels (Arabic, Hebrew, Syriac), whose transliteration would mislead; Thai and Lao, which ICU gives
/// only with diacritics few readers know; Myanmar, Khmer, Sinhala and Tibetan, which neither library
/// transliterates with their inherent vowels; and Chinese characters in any language but Chinese,
/// because ICU reads them as Mandarin (a Japanese name with kanji would get the Chinese reading).
public static class NameTransliteration {
    private static readonly object Lock = new();
    private static Transliterator? _anyLatin;

    private static readonly HashSet<int> LeftOutScripts = [
        UScript.Arabic, UScript.Hebrew, UScript.Syriac,
        UScript.Thai, UScript.Lao,
        UScript.Myanmar, UScript.Khmer, UScript.Sinhala, UScript.Tibetan,
    ];

    /// The scripts whose letters ICU leaves as they are and AnyAscii transliterates well: Cyrillic
    /// (the letters of Kazakh, Kyrgyz, Bashkir, Chechen and other languages that ICU's Cyrillic-Latin
    /// does not have, such as ү, ө and Ӏ), Ethiopic, Canadian syllabics, Mongolian, N'Ko, Ol Chiki,
    /// Tifinagh, Syloti Nagri, Meetei Mayek and Cherokee.
    private static readonly HashSet<int> FallbackScripts = [
        UScript.Cyrillic, UScript.Ethiopic, UScript.CanadianAboriginal, UScript.Mongolian, UScript.GetCodeFromName("Nkoo"),
        UScript.OlChiki, UScript.Tifinagh, UScript.SylotiNagri, UScript.GetCodeFromName("Mtei"), UScript.Cherokee,
    ];

    /// lang: the name's language code as stored ("zh", "ja", "yue"), or null.
    public static string? For(string name, string? lang) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }
        var other = false;
        var han = false;
        foreach (var rune in name.EnumerateRunes()) {
            if (!Rune.IsLetter(rune)) {
                continue;
            }
            var script = UScript.GetScript(rune.Value);
            if (IsLatinOrShared(script)) {
                continue;
            }
            if (LeftOutScripts.Contains(script)) {
                return null;
            }
            other = true;
            han |= script == UScript.Han;
        }
        if (!other || (han && !IsChinese(lang))) {
            return null;
        }
        string latin;
        try {
            lock (Lock) {
                _anyLatin ??= Transliterator.GetInstance("Any-Latin");
                latin = _anyLatin.Transliterate(MalayalamChillus(name));
            }
        } catch (Exception) {
            // An alpha port of ICU: a name it cannot handle gets no transliteration rather than an error page.
            return null;
        }
        if (Fallback(latin) is not { } done) {
            return null;
        }
        // ICU writes the descender of Kazakh and Bashkir letters (Қ, Ҡ) as a spacing low line after the
        // letter ("Kˌ"); a combining comma below puts it under the letter ("K̦").
        done = done.Replace('\u02CC', '\u0326').Normalize(NormalizationForm.FormC).Trim();
        return done.Length == 0 || done == name ? null : done;
    }

    // The letters ICU left in another script, run by run, through AnyAscii; null when a run is in a
    // script AnyAscii does not transliterate well. Cherokee syllables come out capitalised
    // ("AGoDeHi"), so they are lowered ("agodehi").
    private static string? Fallback(string text) {
        StringBuilder? result = null;
        var runStart = -1;
        var runScript = 0;
        for (var i = 0; i <= text.Length; i += i < text.Length && char.IsSurrogatePair(text, i) ? 2 : 1) {
            int? script = null;
            if (i < text.Length) {
                var codePoint = char.ConvertToUtf32(text, i);
                var s = UScript.GetScript(codePoint);
                // A combining mark goes with the letter before it.
                if (runStart >= 0 && s == UScript.Inherited) {
                    continue;
                }
                if (Rune.IsLetter(new Rune(codePoint)) && !IsLatinOrShared(s)) {
                    script = s;
                }
            }
            if (runStart >= 0 && script != runScript) {
                if (!FallbackScripts.Contains(runScript)) {
                    return null;
                }
                var ascii = AnyAscii.Transliteration.Transliterate(text[runStart..i]);
                result!.Append(runScript == UScript.Cherokee ? ascii.ToLowerInvariant() : ascii);
                runStart = -1;
            }
            if (i == text.Length) {
                break;
            }
            if (script is { } letterScript) {
                if (runStart < 0) {
                    result ??= new StringBuilder(text[..i]);
                    runStart = i;
                    runScript = letterScript;
                }
            } else {
                result?.Append(text, i, char.IsSurrogatePair(text, i) ? 2 : 1);
            }
        }
        return result?.ToString() ?? text;
    }

    // ICU4N's Malayalam-Latin predates the atomic chillu letters (ൻ ർ ൽ ൾ ൺ ൿ and the newer ൔ ൕ ൖ), so
    // each is written as its consonant with a virama, the older spelling of the same letter.
    private static string MalayalamChillus(string text) {
        if (!text.Any(c => c is >= 'ൔ' and <= 'ൖ' or >= 'ൺ' and <= 'ൿ')) {
            return text;
        }
        var sb = new StringBuilder(text.Length + 4);
        foreach (var c in text) {
            var consonant = c switch {
                'ൺ' => 'ണ', 'ൻ' => 'ന', 'ർ' => 'ര', 'ൽ' => 'ല',
                'ൾ' => 'ള', 'ൿ' => 'ക', 'ൔ' => 'മ', 'ൕ' => 'യ', 'ൖ' => 'ഴ',
                _ => '\0',
            };
            if (consonant == '\0') {
                sb.Append(c);
            } else {
                sb.Append(consonant).Append('്');
            }
        }
        return sb.ToString();
    }

    private static bool IsLatinOrShared(int script) => script is UScript.Latin or UScript.Common or UScript.Inherited;

    // "zh", "zh-Hans", "zh-TW"; Cantonese (yue), Wu (wuu), Min Nan (nan) and Hakka (hak) are other codes.
    private static bool IsChinese(string? lang) =>
        lang is { } code && (code.Equals("zh", StringComparison.OrdinalIgnoreCase) || code.StartsWith("zh-", StringComparison.OrdinalIgnoreCase)
            || code.Equals("cmn", StringComparison.OrdinalIgnoreCase));
}
