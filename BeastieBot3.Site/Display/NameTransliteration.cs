using System.Text;
using ICU4N.Globalization;
using ICU4N.Text;

namespace BeastieBot3.Site.Display;

/// The Latin-letter transliteration of a common name written in another script, shown under the
/// name on taxon pages: ICU's Any-Latin transform (pinyin with tone marks for Chinese, Hepburn-style
/// romaji for kana, ISO-style transliterations for Cyrillic, Greek, Indic and other alphabets).
///
/// Left out (null): a name already in Latin letters; a name in a script written without most of its
/// vowels (Arabic, Hebrew, Syriac, Thaana), whose transliteration would mislead; Thai and Lao, which
/// ICU gives only with diacritics few readers know; and Chinese characters in any language but
/// Chinese, because ICU reads them as Mandarin (a Japanese name with kanji would get the Chinese
/// reading).
public static class NameTransliteration {
    private static readonly object Lock = new();
    private static Transliterator? _anyLatin;

    private static readonly HashSet<int> LeftOutScripts = [
        UScript.Arabic, UScript.Hebrew, UScript.Syriac, UScript.Thaana,
        UScript.Thai, UScript.Lao,
    ];

    /// lang: the name's language code as stored ("zh", "ja", "yue"), or null.
    public static string? For(string name, string? lang) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }
        var other = false;
        var han = false;
        for (var i = 0; i < name.Length; i += char.IsSurrogatePair(name, i) ? 2 : 1) {
            var codePoint = char.ConvertToUtf32(name, i);
            if (!Rune.IsLetter(new Rune(codePoint))) {
                continue;
            }
            var script = UScript.GetScript(codePoint);
            if (script is UScript.Latin or UScript.Common or UScript.Inherited) {
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
                latin = _anyLatin.Transliterate(name);
            }
        } catch (Exception) {
            // An alpha port of ICU: a name it cannot handle gets no transliteration rather than an error page.
            return null;
        }
        latin = latin.Normalize(NormalizationForm.FormC).Trim();
        return latin.Length == 0 || latin == name || !IsLatin(latin) ? null : latin;
    }

    // "zh", "zh-Hans", "zh-TW"; Cantonese (yue), Wu (wuu), Min Nan (nan) and Hakka (hak) are other codes.
    private static bool IsChinese(string? lang) =>
        lang is { } code && (code.Equals("zh", StringComparison.OrdinalIgnoreCase) || code.StartsWith("zh-", StringComparison.OrdinalIgnoreCase)
            || code.Equals("cmn", StringComparison.OrdinalIgnoreCase));

    // Every letter of the result is a Latin letter: the transform left nothing in the old script.
    private static bool IsLatin(string text) {
        for (var i = 0; i < text.Length; i += char.IsSurrogatePair(text, i) ? 2 : 1) {
            var codePoint = char.ConvertToUtf32(text, i);
            if (Rune.IsLetter(new Rune(codePoint)) && UScript.GetScript(codePoint) is not (UScript.Latin or UScript.Common or UScript.Inherited)) {
                return false;
            }
        }
        return true;
    }
}
