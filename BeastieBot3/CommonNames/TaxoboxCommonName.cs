using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

// The common name in a Wikipedia taxobox's "name" field, for `common-names aggregate --source
// wikipedia` (source wikipedia_taxobox). The field holds wikitext and often more than one name:
//   "Silver birch<br />''Betula pendula''", "''Abies grandis''<br/>Grand fir",
//   "Purple onion<br>Granat-Kugellauch", "Titberry,<br>Indian allophylus",
//   "Aceramarca gracile opossum<ref name=MSW3>{MSW3 Gardner | pages = 6}</ref>".
// The first line (split at <br>) that is a usable English name is taken: lines in italics (a
// scientific name), in a non-Latin script, naming a family ("Salamandridae") or repeating the page
// title are skipped; a line that starts with a lower-case letter continues the line before
// ("Hoogstraal's striped<br/>grass mouse"). CommonNameQuality repairs or rejects what is left:
// templates, links, the next infobox parameter. The aggregator then drops a result that is the
// taxon's scientific name (ScientificNameCheck). Until October 2026 the taxobox parser
// (Taxonomy/TaxoboxParser) kept one brace of a nested template ("{sfn|Groves|2005}" for
// "{{sfn|Groves|2005}}") and ran on into the next parameter when it was on the same line. Pages
// in the Wikipedia cache keep the fields that parser stored until they are downloaded again, so
// single-brace templates and "| image = ..." tails are still repaired here.

namespace BeastieBot3.CommonNames;

internal static class TaxoboxCommonName {
    private const RegexOptions Options = RegexOptions.Compiled | RegexOptions.CultureInvariant;
    private static readonly Regex Ref = new(@"<ref[^>]*/>|<ref[^>]*>.*?</ref\s*>", Options | RegexOptions.Singleline | RegexOptions.IgnoreCase);
    private static readonly Regex LineBreak = new(@"<br\s*/?\s*>", Options | RegexOptions.IgnoreCase);
    private static readonly Regex TrailingSeparator = new(@"(?:\s*[,;]|\s+or)\s*$", Options | RegexOptions.IgnoreCase);

    /// <summary>
    /// The English common name in a taxobox name field, or null when it has none. A line that is
    /// the same name as <paramref name="pageTitle"/> (the article's title) is skipped.
    /// </summary>
    public static string? FromNameField(string? value, string? pageTitle = null) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }
        var titleKey = string.IsNullOrWhiteSpace(pageTitle)
            ? null
            : CommonNameNormalizer.NormalizeForMatching(CommonNameNormalizer.RemoveDisambiguationSuffix(pageTitle));

        var text = Ref.Replace(value, string.Empty);
        // A reference that is never closed runs to the end of the field.
        var openRef = text.IndexOf("<ref", StringComparison.OrdinalIgnoreCase);
        if (openRef >= 0) {
            text = text[..openRef];
        }

        foreach (var line in Lines(text)) {
            if (line.Contains("''", StringComparison.Ordinal)) {
                continue; // italics: a scientific name
            }
            var quality = CommonNameQuality.Assess(line, "en");
            if (quality.IsJunk) {
                continue;
            }
            var name = Unwrap(TrailingSeparator.Replace(quality.Name, string.Empty).Trim());
            if (name.Length == 0 || !StartsWithLatinLetter(name) || IsHigherTaxonName(name)
                || (titleKey is not null && CommonNameNormalizer.NormalizeForMatching(name) == titleKey)) {
                continue;
            }
            return name;
        }
        return null;
    }

    // Splits at <br>; a line starting with a lower-case letter continues the one before unless that
    // one ends with a comma, a semicolon or "or".
    private static IEnumerable<string> Lines(string text) {
        var lines = new List<string>();
        foreach (var part in LineBreak.Split(text)) {
            var line = part.Trim();
            if (line.Length == 0) {
                continue;
            }
            if (lines.Count > 0 && char.IsLower(line[0]) && !TrailingSeparator.IsMatch(lines[^1])) {
                lines[^1] = lines[^1] + " " + line;
            } else {
                lines.Add(line);
            }
        }
        return lines;
    }

    // "(Octopus agave)" on its own line under the scientific name.
    private static string Unwrap(string name) =>
        name.Length > 2 && name[0] == '(' && name[^1] == ')' && name.IndexOf(')') == name.Length - 1
            ? name[1..^1].Trim()
            : name;

    // The first letter, past any ʻokina, is in a Latin script (Basic Latin to Latin Extended-B, or
    // Latin Extended Additional): not "粗根韭 cu gen jiu" or "Лук Вешнякова".
    private static bool StartsWithLatinLetter(string name) {
        foreach (var c in name) {
            if (!char.IsLetter(c) || CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.ModifierLetter) {
                continue;
            }
            return c <= 'ɏ' || (c >= 'Ḁ' && c <= 'ỿ');
        }
        return false;
    }

    // A family, subfamily or superfamily name on a taxobox for a group ("Salamandridae").
    private static bool IsHigherTaxonName(string name) =>
        !name.Contains(' ')
        && (name.EndsWith("idae", StringComparison.Ordinal) || name.EndsWith("inae", StringComparison.Ordinal)
            || name.EndsWith("aceae", StringComparison.Ordinal) || name.EndsWith("oidea", StringComparison.Ordinal));
}
