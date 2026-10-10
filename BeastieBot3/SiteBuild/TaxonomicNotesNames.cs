using System.Net;
using System.Text.RegularExpressions;

// The scientific names in an assessment's taxonomic notes (documentation.taxonomic_notes, HTML),
// for "Named in IUCN's taxonomic notes" on the site (notes_taxon). Only the names are read; the notes
// are narrative text, which the site database must not hold.
//
// IUCN writes names in italics (<i> or <em>), so only italic text is read. A run of italics that is a
// whole name is kept: "Cebuella pygmaea", "Callithrix (Cebuella) pygmaea", "Cebuella pygmaea
// niveiventris", "Panthera pardus ssp. orientalis". A name with its genus abbreviated takes the genus
// of the latest full name with that initial ("C. pygmaea", "C. niveiventris"), and a trinomial with
// the species abbreviated too takes the latest full species of that genus with that initial ("C. p.
// niveiventris"). Before the first full name, the taxon's own genus and species are the latest. A
// lone genus or epithet, and anything else in italics ("et al.", "sensu stricto"), is not a name.
//
// In a sample of 3,000 latest global assessments with notes (October 2026), 1,993 italic runs were
// whole names and 2,247 abbreviated ones; about 19% of the assessments named another taxon in the
// release by a whole name alone.

namespace BeastieBot3.SiteBuild;

/// One name in the notes: Written as the notes write it ("C. p. niveiventris"), Full with the
/// abbreviations expanded ("Cebuella pygmaea niveiventris").
internal sealed record NotesName(string Written, string Full) {
    public bool IsAbbreviated => !string.Equals(Written, Full, StringComparison.Ordinal);
}

internal static partial class TaxonomicNotesNames {
    /// The names in the notes in the order of their first mention, each once (by Full; a written-out
    /// mention is kept in place of an abbreviated one). ownGenus and ownSpecies: the assessed taxon's.
    public static IReadOnlyList<NotesName> Read(string? notesHtml, string? ownGenus, string? ownSpecies) {
        var found = new List<NotesName>();
        if (string.IsNullOrWhiteSpace(notesHtml)) {
            return found;
        }
        // The latest full genus for each initial, and the latest full species for each genus and initial.
        var genera = new Dictionary<char, string>();
        var species = new Dictionary<(string Genus, char Initial), string>();
        void Remember(string genus, string? epithet) {
            genera[genus[0]] = genus;
            if (epithet is not null) {
                species[(genus, epithet[0])] = epithet;
            }
        }
        if (IsGenus(ownGenus)) {
            Remember(ownGenus!, IsEpithet(ownSpecies) ? ownSpecies : null);
        }

        var index = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (Match italic in Italic().Matches(notesHtml)) {
            var text = Clean(italic.Groups[2].Value);
            var name = ReadName(text, genera, species);
            if (name is null) {
                continue;
            }
            if (!name.IsAbbreviated) {
                var words = name.Full.Split(' ');
                Remember(words[0], words.Length > 1 ? words[1] : null);
            }
            if (index.TryGetValue(name.Full, out var at)) {
                if (found[at].IsAbbreviated && !name.IsAbbreviated) {
                    found[at] = name;
                }
                continue;
            }
            index[name.Full] = found.Count;
            found.Add(name);
        }
        return found;
    }

    private static NotesName? ReadName(string text, Dictionary<char, string> genera, Dictionary<(string, char), string> species) {
        var full = FullName().Match(text);
        if (full.Success) {
            // The subgenus in brackets is left out of the name to look up.
            var name = $"{full.Groups["genus"].Value} {full.Groups["species"].Value}";
            if (full.Groups["infra"].Success) {
                name += full.Groups["marker"].Success ? $" {full.Groups["marker"].Value} {full.Groups["infra"].Value}" : $" {full.Groups["infra"].Value}";
            }
            return new NotesName(text, name);
        }
        var abbreviated = AbbreviatedName().Match(text);
        if (!abbreviated.Success || !genera.TryGetValue(abbreviated.Groups["g"].Value[0], out var genus)) {
            return null;
        }
        if (abbreviated.Groups["s"].Success) {
            // "C. p. niveiventris": the species from the latest one of the genus with that initial.
            if (!species.TryGetValue((genus, abbreviated.Groups["s"].Value[0]), out var epithet)) {
                return null;
            }
            return new NotesName(text, $"{genus} {epithet} {abbreviated.Groups["rest"].Value}");
        }
        return new NotesName(text, $"{genus} {abbreviated.Groups["rest"].Value}");
    }

    // Tags removed, entities decoded, no-break spaces as spaces, runs of spaces as one, and the
    // punctuation that italics often take in at either end removed.
    private static string Clean(string html) {
        var text = WebUtility.HtmlDecode(Tag().Replace(html, string.Empty)).Replace(' ', ' ');
        return Spaces().Replace(text, " ").Trim(' ', '.', ',', ';', ':', '(', ')', '[', ']', '"', '\'', '“', '”', '‘', '’');
    }

    private static bool IsGenus(string? word) => word is { Length: > 1 } && GenusWord().IsMatch(word);
    private static bool IsEpithet(string? word) => word is { Length: > 1 } && EpithetWord().IsMatch(word);

    [GeneratedRegex(@"<(i|em)\b[^>]*>(.*?)</\1>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Italic();

    [GeneratedRegex(@"<[^>]+>")]
    private static partial Regex Tag();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Spaces();

    [GeneratedRegex(@"^[A-Z][a-z]+$")]
    private static partial Regex GenusWord();

    [GeneratedRegex(@"^[a-z][a-z-]+$")]
    private static partial Regex EpithetWord();

    // "Genus species", "Genus (Subgenus) species", "Genus species infra", "Genus species ssp. infra".
    [GeneratedRegex(@"^(?<genus>[A-Z][a-z]+)(?: \([A-Z][a-z]+\))? (?<species>[a-z][a-z-]+)(?: (?:(?<marker>ssp\.|subsp\.|var\.) )?(?<infra>[a-z][a-z-]+))?$")]
    private static partial Regex FullName();

    // "G. species", "G. species infra", "G. s. infra" (the genus, and the species, abbreviated).
    [GeneratedRegex(@"^(?<g>[A-Z])\. (?:(?<s>[a-z])\. ?)?(?<rest>[a-z][a-z-]+(?: [a-z][a-z-]+)?)$")]
    private static partial Regex AbbreviatedName();
}
