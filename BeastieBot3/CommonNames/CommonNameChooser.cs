using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BeastieBot3.WikipediaLists;
using BeastieBot3.WikipediaLists.Legacy;

// Chooses the English common name shown for a taxon. The Wikipedia lists (`wikipedia
// generate-lists` through StoreBackedCommonNameProvider and SpeciesLineFormatter, `sprat
// generate-lists`, list headings) and the species site (`site build-db`, SiteCommonNamesReader)
// all choose it here, in this order:
//   1. a common name set for the taxon in rules-list.txt ("Panthera leo = lion"), used even when
//      another taxon has the same name;
//   2. the best of the store's names for the taxon (ChooseBest): by source priority, skipping
//      junk (CommonNameQuality: wiki markup, author citations, OCR errors...) and a name another
//      taxon keeps (AmbiguousNames), repaired where CommonNameQuality can repair it ("Sunda slow
//      loris{sfn|...}" is "Sunda slow loris"), capitalised with the caps rules, except that a
//      Wikipedia article title keeps the title's capitals (DisplayCasing);
//   3. nothing, when the name from step 1 or 2 is not usable as a common name (IsUnusable): the
//      scientific name again, a working name ("sp. nov."), or a name with an authority and year.
// The lists have fallbacks of their own (a SPRAT name, the legacy Wikidata/IUCN provider) that
// they use only when steps 1 and 2 find nothing.

namespace BeastieBot3.CommonNames;

/// <summary>What <see cref="CommonNameChooser.Choose"/> found for a taxon.</summary>
internal enum CommonNameChoiceKind {
    /// <summary>Neither rules-list.txt nor the store has a name for the taxon.</summary>
    None,
    /// <summary>The name set in rules-list.txt.</summary>
    Rules,
    /// <summary>The store's best name.</summary>
    Store,
    /// <summary>A name was found but is not usable (<see cref="CommonNameChooser.IsUnusable"/>); show none.</summary>
    Unusable,
}

/// <summary>The chooser's result: the name to show (null for None and Unusable) and where it came from.</summary>
internal readonly record struct CommonNameChoice(CommonNameChoiceKind Kind, string? Name) {
    /// <summary>True when rules-list.txt or the store had a name, usable or not.</summary>
    public bool Found => Kind != CommonNameChoiceKind.None;
}

/// <summary>
/// The names <see cref="CommonNameChooser.Choose"/> needs for one taxon: the rules-list.txt key, and
/// the scientific name, genus and species epithet the unusable check compares the name with.
/// </summary>
internal readonly record struct CommonNameSubject(string? RulesKey, string? ScientificName, string? Genus, string? SpeciesEpithet);

internal sealed class CommonNameChooser {
    private readonly Lazy<AmbiguousNames> _ambiguous;
    private readonly IReadOnlyDictionary<string, string> _capsRules;
    private readonly LegacyTaxaRuleList? _rules;

    private CommonNameChooser(Lazy<AmbiguousNames> ambiguous, IReadOnlyDictionary<string, string> capsRules,
        LegacyTaxaRuleList? rules) {
        _ambiguous = ambiguous;
        _capsRules = capsRules;
        _rules = rules;
    }

    /// <summary>
    /// A chooser over <paramref name="store"/>'s English names and caps rules. The ambiguity
    /// verdicts are read from the store on first use (the store caches them); with
    /// <paramref name="allowAmbiguous"/> no name is skipped as ambiguous.
    /// </summary>
    public static CommonNameChooser ForStore(CommonNameStore store, LegacyTaxaRuleList? rules = null,
        bool allowAmbiguous = false) =>
        new(allowAmbiguous ? new Lazy<AmbiguousNames>(AmbiguousNames.None) : new Lazy<AmbiguousNames>(() => store.GetAmbiguousNames("en")),
            store.GetAllCapsRules(), rules);

    /// <summary>A chooser with only rules-list.txt, for list generation without the store.</summary>
    public static CommonNameChooser RulesOnly(LegacyTaxaRuleList? rules) =>
        new(new Lazy<AmbiguousNames>(AmbiguousNames.None), new Dictionary<string, string>(), rules);

    /// <summary>The same chooser with <paramref name="rules"/> as its rules-list.txt.</summary>
    public CommonNameChooser WithRules(LegacyTaxaRuleList? rules) => new(_ambiguous, _capsRules, rules);

    /// <summary>The ambiguity rule's verdicts this chooser skips names by.</summary>
    public AmbiguousNames Ambiguous => _ambiguous.Value;

    /// <summary>Steps 1 to 3 for one taxon. <paramref name="storeName"/> gives the store's best
    /// name (<see cref="FromStore"/>) and is only called when rules-list.txt has none.</summary>
    public CommonNameChoice Choose(CommonNameSubject taxon, Func<string?>? storeName) {
        string? name;
        CommonNameChoiceKind kind;
        if (FromRules(taxon.RulesKey) is { } manual) {
            name = manual;
            kind = CommonNameChoiceKind.Rules;
        } else if (storeName?.Invoke() is { } best && !string.IsNullOrWhiteSpace(best)) {
            name = best;
            kind = CommonNameChoiceKind.Store;
        } else {
            return new CommonNameChoice(CommonNameChoiceKind.None, null);
        }
        return IsUnusable(name, taxon.ScientificName, taxon.Genus, taxon.SpeciesEpithet)
            ? new CommonNameChoice(CommonNameChoiceKind.Unusable, null)
            : new CommonNameChoice(kind, name);
    }

    /// <summary>Step 1: the common name rules-list.txt sets for <paramref name="scientificName"/>,
    /// first letter upper-cased; null when it sets none.</summary>
    public string? FromRules(string? scientificName) {
        var manual = _rules?.Get(scientificName ?? string.Empty)?.CommonName;
        return string.IsNullOrWhiteSpace(manual) ? null : ProseFormat.Uppercase(manual);
    }

    /// <summary>
    /// Step 2: the best of a taxon's names (<see cref="ChooseBest"/>) with its
    /// <see cref="CommonNameResult.DisplayName"/> capitalised (<see cref="DisplayCasing"/>); null
    /// when every name is junk or ambiguous for the taxon, or it has none.
    /// <paramref name="storeTaxonId"/> is the store's taxa.id; <paramref name="scientificName"/>,
    /// when given, is the taxon's name, so a name that is it with a subgenus is skipped.
    /// </summary>
    public CommonNameResult? FromStore(long storeTaxonId, IEnumerable<CommonNameCandidate> candidates,
        string? scientificName = null) {
        var all = candidates as IReadOnlyCollection<CommonNameCandidate> ?? candidates.ToList();
        var best = ChooseBest(storeTaxonId, all, Ambiguous, scientificName);
        return best is null ? null : best with { DisplayName = DisplayCasing(best, all, Capitalize) };
    }

    /// <summary>
    /// The capitalisation of the chosen name. A Wikipedia article title keeps the title's
    /// capitals, with the first letter upper case: English Wikipedia titles are in sentence case
    /// with proper nouns capitalised ("Large Palau flying fox", "Banded martin"), and the caps
    /// rules would lower-case a place name they have no rule for ("Large palau flying fox") or
    /// capitalise a word that a rule capitalises elsewhere ("Banded Martin"). A name from another
    /// source that is one of the taxon's Wikipedia titles apart from its capitals is shown with the
    /// title's capitals. Any other name gets the caps rules (<paramref name="capitalize"/>), so a
    /// taxobox name in title case ("White Ash") is still lower-cased.
    /// </summary>
    internal static string DisplayCasing(CommonNameResult best, IEnumerable<CommonNameCandidate> candidates,
        Func<string, string> capitalize) {
        if (IsWikipediaTitle(best.Source)) {
            return TitleCasing(best.DisplayName);
        }
        var shown = capitalize(best.DisplayName);
        foreach (var candidate in candidates) {
            if (IsWikipediaTitle(candidate.Source) && UsableName(candidate) is { } title
                && TitleCasing(title.Name) is var titleForm
                && string.Equals(titleForm, shown, StringComparison.OrdinalIgnoreCase)) {
                return titleForm;
            }
        }
        return shown;
    }

    private static bool IsWikipediaTitle(string source) =>
        string.Equals(source, "wikipedia_title", StringComparison.OrdinalIgnoreCase);

    // A Wikipedia title as a common name: without its disambiguation ("Jack Dempsey (fish)"),
    // straight quotes and single spaces, first letter upper case, other capitals as in the title.
    private static string TitleCasing(string title) =>
        ProseFormat.Uppercase(CommonNameNormalizer.NormalizeDisplayTypography(
            CommonNameNormalizer.RemoveDisambiguationSuffix(title))) ?? title;

    /// <summary>
    /// The caps rules applied to a name: the first word is title-cased; later words are lower-cased
    /// unless caps.txt, an internal capital or a possessive keeps them capitalised
    /// (<see cref="CommonNameNormalizer.ApplyCapitalization"/>).
    /// </summary>
    public string Capitalize(string name) =>
        string.IsNullOrWhiteSpace(name) ? name : CommonNameNormalizer.ApplyCapitalization(name, _capsRules);

    /// <summary>
    /// The one ranking of a taxon's common names: source priority
    /// (<see cref="CommonNameStore.GetSourcePriority"/>), then preferred names first, then raw name
    /// for determinism. The first name wins that is not junk (<see cref="CommonNameQuality"/>), not
    /// the taxon's <paramref name="scientificName"/> with a subgenus, and not ambiguous for
    /// <paramref name="taxonId"/> (<see cref="AmbiguousNames.IsAmbiguousFor"/>, by the repaired
    /// name's key). <paramref name="taxonId"/> is the store's taxa.id. Pass
    /// <see cref="AmbiguousNames.None"/> to skip no name as ambiguous. The result's RawName is the
    /// name as stored; its DisplayName is the repaired name, not yet capitalised.
    /// </summary>
    internal static CommonNameResult? ChooseBest(long taxonId, IEnumerable<CommonNameCandidate> candidates,
        AmbiguousNames ambiguousNames, string? scientificName = null) {
        var sorted = candidates
            .OrderBy(c => CommonNameStore.GetSourcePriority(c.Source, c.IsPreferred))
            .ThenByDescending(c => c.IsPreferred)
            .ThenBy(c => c.RawName, StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in sorted) {
            if (UsableName(candidate) is not { } usable
                || IsScientificNameWithSubgenus(usable.Name, scientificName)
                || ambiguousNames.IsAmbiguousFor(taxonId, usable.NormalizedName)) {
                continue;
            }
            return new CommonNameResult(
                RawName: candidate.RawName,
                DisplayName: usable.Name,
                NormalizedName: usable.NormalizedName,
                Source: candidate.Source,
                IsPreferred: candidate.IsPreferred,
                IsAmbiguous: false);
        }

        return null;
    }

    /// <summary>
    /// The name to use for an English name from the store and the key the ambiguity rule compares
    /// it by: as stored when <see cref="CommonNameQuality"/> finds it good, repaired (with the
    /// repaired name's key) when it can be repaired, and null when it is junk.
    /// <see cref="CommonNameStore"/> builds <see cref="AmbiguousNames"/> from the same keys.
    /// </summary>
    internal static (string Name, string NormalizedName)? UsableName(CommonNameCandidate candidate, string language = "en") {
        var quality = CommonNameQuality.Assess(candidate.RawName, language);
        if (quality.IsJunk) {
            return null;
        }
        if (!quality.IsRepaired) {
            return (candidate.RawName, candidate.NormalizedName);
        }
        return CommonNameNormalizer.NormalizeForMatching(quality.Name) is { } key ? (quality.Name, key) : null;
    }

    // "Holothuria (Metriatyla) lessoni" for Holothuria lessoni: a Wikidata label that is the
    // scientific name with its subgenus.
    private static readonly Regex NameWithSubgenus = new(@"^(\S+) \([^()\s]+\) (\S+)$", RegexOptions.Compiled);

    private static bool IsScientificNameWithSubgenus(string name, string? scientificName) {
        if (string.IsNullOrWhiteSpace(scientificName) || NameWithSubgenus.Match(name.Trim()) is not { Success: true } match) {
            return false;
        }
        return string.Equals($"{match.Groups[1].Value} {match.Groups[2].Value}", FirstTwoTokens(scientificName.Trim()),
            StringComparison.OrdinalIgnoreCase);
    }

    // A 4-digit year (1600–2099) betrays a botanical/zoological authority citation rather than a
    // vernacular name; common names effectively never contain one.
    private static readonly Regex AuthorityYearPattern = new(@"\b(1[6-9]\d{2}|20\d{2})\b", RegexOptions.Compiled);

    /// <summary>
    /// Step 3: true when <paramref name="candidate"/> is not usable as a common name: blank, a
    /// working placeholder ("sp. nov.", " spp."), a homonym or authority string (" non ", a year),
    /// or the scientific name repeated (the whole name, or its first two words equal to the
    /// binomial).
    /// </summary>
    public static bool IsUnusable(string? candidate, string? scientificName, string? genusName, string? speciesName) {
        if (string.IsNullOrWhiteSpace(candidate)) {
            return true;
        }

        var name = candidate.Trim();

        if (name.Contains("sp. nov", StringComparison.OrdinalIgnoreCase)) return true;
        if (name.Contains(" spp.", StringComparison.OrdinalIgnoreCase)) return true;
        if (name.Contains(" non ", StringComparison.Ordinal)) return true;
        if (AuthorityYearPattern.IsMatch(name)) return true;

        // A "common name" that is really just the scientific name repeated.
        if (!string.IsNullOrWhiteSpace(scientificName) &&
            string.Equals(name, scientificName, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        var binomial = BuildBinomial(genusName, speciesName);
        if (!string.IsNullOrWhiteSpace(binomial) &&
            string.Equals(FirstTwoTokens(name), binomial, StringComparison.OrdinalIgnoreCase)) {
            return true;
        }

        return false;
    }

    private static string? BuildBinomial(string? genusName, string? speciesName) {
        var genus = genusName?.Trim();
        var species = speciesName?.Trim();
        if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
            return null;
        }
        return $"{genus} {species}";
    }

    private static string FirstTwoTokens(string name) {
        var parts = name.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        return parts.Length <= 2 ? string.Join(' ', parts) : parts[0] + " " + parts[1];
    }
}
