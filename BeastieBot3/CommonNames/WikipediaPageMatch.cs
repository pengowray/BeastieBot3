using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BeastieBot3.Taxonomy;

// Which taxa that `wikipedia match-taxa` matched to a Wikipedia page get common names from it (its
// title and taxobox name) in `common-names aggregate --source wikipedia`.
//
// The matcher matches 2,361 pages to more than one IUCN taxon (September 2026). Typical cases:
// - a split species: "Channel-billed toucan" (taxobox Ramphastos vitellinus) is matched to
//   R. vitellinus and, through redirects from their names, to R. culminatus, R. ariel and
//   R. citreolaemus;
// - a subspecies matched through a synonym: "Hector's dolphin" to Cephalorhynchus hectori and to
//   C. hectori maui, which IUCN lists with the synonym "Cephalorhynchus hectori";
// - Catalogue of Life names: "European green toad" (Bufotes viridis) to B. balearicus, B. perrini
//   and B. sitibundus.
// Giving the page's names to every one of them made the subspecies show "Hector's dolphin", and a
// name several taxa share is skipped for all of them, so R. vitellinus showed no common name at
// all. The page's names go to the taxon the page's taxobox names, when one of the matched taxa has
// that name; failing that, to the taxa matched by their own name rather than through a synonym or a
// CoL name; failing that, to all of them.
//
// Two more rules apply to every matched page. A page about a genus or a higher taxon gives no names
// to a species or subspecies matched to it, unless the genus has one species
// (IsGenusPageOfSpecies): "Maple" is no name of Acer kwangnanense. And a title or taxobox name that
// is the scientific name the page's own taxobox gives is not a common name (IsTaxoboxSubject):
// "Tliltocatl epicureanus" for Brachypelma epicureanum.

namespace BeastieBot3.CommonNames;

/// <summary>A taxon matched to a page: its IUCN id, the matcher's method, and its names in the store.</summary>
internal sealed record MatchedTaxon(string TaxonIdentifier, string? MatchMethod, TaxonScientificNames Names);

internal static class WikipediaPageMatch {
    /// <summary>
    /// True when the matcher found the page through a name that is not the taxon's own: an IUCN
    /// synonym ("iucn-synonym") or a Catalogue of Life name ("col-accepted", "col-synonym" and the
    /// other "col-" methods). The taxon's own names, its Wikidata item and unknown methods are not.
    /// </summary>
    public static bool IsThroughAnotherName(string? matchMethod) =>
        matchMethod is not null
        && (matchMethod.Equals("iucn-synonym", StringComparison.Ordinal) || matchMethod.StartsWith("col-", StringComparison.Ordinal));

    /// <summary>
    /// The IUCN ids of the taxa, among <paramref name="taxa"/> (all matched to one page), that get
    /// the page's names. <paramref name="subjectName"/> is the scientific name the page's taxobox
    /// gives (<see cref="SubjectName"/>), or null. In order: the taxa whose accepted name is the
    /// subject; those with it as a synonym; those matched by their own name; all of them.
    /// </summary>
    public static IReadOnlySet<string> TaxaGivenNames(IReadOnlyList<MatchedTaxon> taxa, string? subjectName) {
        var all = taxa.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal);
        if (taxa.Count <= 1) {
            return all;
        }

        // Compared without rank markers: the store keeps "nanger granti ssp. granti", a
        // Subspeciesbox gives "Nanger granti granti".
        var subject = WithoutRankMarkers(ScientificNameNormalizer.Normalize(subjectName));
        if (subject is not null) {
            var byAcceptedName = taxa.Where(t => WithoutRankMarkers(t.Names.Canonical) == subject).ToList();
            if (byAcceptedName.Count > 0) {
                return byAcceptedName.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal);
            }
            var bySynonym = taxa.Where(t => t.Names.Synonyms.Any(s => WithoutRankMarkers(s) == subject)).ToList();
            if (bySynonym.Count > 0) {
                return bySynonym.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal);
            }
        }

        var byOwnName = taxa.Where(t => !IsThroughAnotherName(t.MatchMethod)).ToList();
        return byOwnName.Count > 0
            ? byOwnName.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal)
            : all;
    }

    private static string? WithoutRankMarkers(string? name) =>
        name is null ? null : string.Join(' ', ScientificNameCheck.WithoutRankMarkers(name.Split(' ', StringSplitOptions.RemoveEmptyEntries)));

    private const RegexOptions Options = RegexOptions.Compiled | RegexOptions.CultureInvariant;
    private static readonly Regex Ref = new(@"<ref[^>]*/>|<ref[^>]*>.*?</ref\s*>", Options | RegexOptions.Singleline | RegexOptions.IgnoreCase);
    private static readonly Regex Template = new(@"\{\{?[^{}]*\}\}?", Options);
    private static readonly Regex Link = new(@"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", Options);
    private static readonly Regex Tag = new(@"<[^>]+>", Options);
    private static readonly Regex PlainName = new(@"^[A-Z][a-z]+( [a-z][a-z\-]*){1,3}$", Options);
    private static readonly Regex PlainEpithet = new(@"^[a-z][a-z\-]*$", Options);

    /// <summary>
    /// The scientific name a taxobox gives for its page, from its parameters as the Wikipedia cache
    /// stores them: "taxon" (Speciesbox), "trinomial" or "binomial" (Taxobox), or "genus" with
    /// "species" and "subspecies" (Speciesbox, Subspeciesbox). Null when none of them holds a plain
    /// scientific name once references, templates, links, italics and the hybrid sign are removed.
    /// </summary>
    public static string? SubjectName(IReadOnlyDictionary<string, string> fields) {
        foreach (var key in new[] { "taxon", "trinomial", "binomial" }) {
            if (fields.TryGetValue(key, out var value) && Clean(value) is { } name && PlainName.IsMatch(name)) {
                return name;
            }
        }
        if (fields.TryGetValue("genus", out var genusValue) && Clean(genusValue) is { } genus
            && fields.TryGetValue("species", out var speciesValue) && Clean(speciesValue) is { } species
            && PlainEpithet.IsMatch(species)) {
            var name = $"{genus} {species}";
            if (fields.TryGetValue("subspecies", out var subspeciesValue) && Clean(subspeciesValue) is { } subspecies
                && PlainEpithet.IsMatch(subspecies)) {
                name += " " + subspecies;
            }
            return PlainName.IsMatch(name) ? name : null;
        }
        return null;
    }

    /// <summary>
    /// The scientific names a page's taxobox gives for the page's own taxon, as keys for
    /// <see cref="IsTaxoboxSubject"/>: the "taxon", "binomial" and "trinomial" parameters, and
    /// "genus" with "species" (and "subspecies"), cleaned as in <see cref="SubjectName"/>. Unlike
    /// <see cref="SubjectName"/>, a one-word taxon (a genus page) and a name with other punctuation
    /// ("Cyanea st.-johnii") are included, because they are only compared with a name for
    /// equality.
    /// </summary>
    public static IReadOnlySet<string> SubjectKeys(IReadOnlyDictionary<string, string> fields) {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in new[] { "taxon", "trinomial", "binomial" }) {
            if (fields.TryGetValue(key, out var value) && SubjectKey(Clean(value)) is { } name) {
                keys.Add(name);
            }
        }
        if (fields.TryGetValue("genus", out var genusValue) && Clean(genusValue) is { } genus
            && fields.TryGetValue("species", out var speciesValue) && Clean(speciesValue) is { } species) {
            var name = $"{genus} {species}";
            if (SubjectKey(name) is { } binomial) {
                keys.Add(binomial);
            }
            if (fields.TryGetValue("subspecies", out var subspeciesValue) && Clean(subspeciesValue) is { } subspecies
                && SubjectKey($"{name} {subspecies}") is { } trinomial) {
                keys.Add(trinomial);
            }
        }
        return keys;
    }

    /// <summary>
    /// True when <paramref name="name"/> (a page title without its disambiguation, or a taxobox
    /// name) is one of the scientific names the page's taxobox gives for its own taxon
    /// (<see cref="SubjectKeys"/>), ignoring case, rank markers, a subgenus and the hybrid sign:
    /// the title "Tliltocatl epicureanus" of the page whose taxobox has taxon = Tliltocatl
    /// epicureanus, matched to Brachypelma epicureanum. Such a title is a scientific name even when
    /// the store knows neither its genus nor its epithet.
    /// </summary>
    public static bool IsTaxoboxSubject(string? name, IReadOnlySet<string> subjectKeys) =>
        subjectKeys.Count > 0 && SubjectKey(name) is { } key && subjectKeys.Contains(key);

    // A name as SubjectKeys and IsTaxoboxSubject compare it: lower case, without double quotes,
    // the hybrid sign, rank markers or a subgenus.
    private static string? SubjectKey(string? name) {
        if (string.IsNullOrWhiteSpace(name)) {
            return null;
        }
        var normalized = ScientificNameNormalizer.Normalize(
            name.Replace("\"", "").Replace('\u00a0', ' ').Replace("&times;", " ").Replace('×', ' '));
        return WithoutRankMarkers(normalized) is { Length: > 0 } key ? key : null;
    }

    /// <summary>
    /// True when the page is about a genus or a higher taxon, judged from its taxobox parameters
    /// (as the Wikipedia cache stores them): it names no species ("binomial", "trinomial",
    /// "species", "subspecies", "binomial_text", "species_text" and "trinomial_text" are empty),
    /// and its "taxon" (or, without one, its "genus") is one plain word: "Trachycephalus",
    /// "Acer" (the page "Maple"), "Capraiuscola". The rank the cache stores is not used: it is
    /// "genus" for any one-word taxon or name, including the Raiatea starling's "Aplonis/?/?", a
    /// species whose genus is uncertain.
    /// </summary>
    public static bool IsGenusOrHigherPage(IReadOnlyDictionary<string, string> fields) =>
        PageGenus(fields) is not null;

    /// <summary>
    /// The one-word taxon of a page about a genus or a higher taxon (<see cref="IsGenusOrHigherPage"/>),
    /// such as "Trachycephalus"; null for any other page.
    /// </summary>
    public static string? PageGenus(IReadOnlyDictionary<string, string> fields) {
        foreach (var key in SpeciesParameters) {
            if (fields.TryGetValue(key, out var value) && Clean(value) is not null) {
                return null;
            }
        }
        var taxon = fields.TryGetValue("taxon", out var taxonValue) ? Clean(taxonValue) : null;
        var name = taxon ?? (fields.TryGetValue("genus", out var genusValue) ? Clean(genusValue) : null);
        return name is not null && OneWordTaxon.IsMatch(name) ? name : null;
    }

    /// <summary>
    /// True when the page's title and taxobox name are not common names of a taxon matched to it,
    /// because the page is about a genus or a higher taxon (<see cref="PageGenus"/>) and the taxon
    /// is a species or below (<paramref name="taxonCanonical"/>, the store's name for it, has two or
    /// more words): "Casque-headed tree frogs" on the page "Trachycephalus" is no name of
    /// Trachycephalus vermiculatus. The names are kept when the page's genus has one species:
    /// the taxobox says it is monotypic (<paramref name="markedMonotypic"/>), or
    /// <paramref name="speciesInGenus"/> (the number of species the store has in the page's genus,
    /// not the taxon's genus) is 1.
    /// </summary>
    public static bool IsGenusPageOfSpecies(IReadOnlyDictionary<string, string> fields, string? taxonCanonical,
        bool markedMonotypic, Func<string, int> speciesInGenus) =>
        taxonCanonical?.Trim().Contains(' ') == true
        && !markedMonotypic
        && PageGenus(fields) is { } genus
        && speciesInGenus(genus) != 1;

    private static readonly string[] SpeciesParameters =
        ["binomial", "trinomial", "species", "subspecies", "binomial_text", "species_text", "trinomial_text"];

    private static readonly Regex OneWordTaxon = new(@"^[A-Z][a-z]+$", Options);

    private static string? Clean(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }
        var text = Ref.Replace(value, " ");
        // Templates are removed from the inside out; the cache holds some with one brace.
        string previous;
        do {
            previous = text;
            text = Template.Replace(text, " ");
        } while (text != previous);
        text = Link.Replace(text, "$1");
        text = Tag.Replace(text, " ");
        text = text.Replace("'''", "").Replace("''", "").Replace("&nbsp;", " ");
        // The hybrid sign: "Yucca × schottii" is the hybrid Yucca schottii.
        text = text.Replace("&times;", " ").Replace('×', ' ');
        text = Regex.Replace(text, @"\s+", " ").Trim();
        return text.Length == 0 ? null : text;
    }
}
