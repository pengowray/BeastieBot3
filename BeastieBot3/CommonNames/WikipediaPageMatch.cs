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

        var subject = ScientificNameNormalizer.Normalize(subjectName);
        if (subject is not null) {
            var byAcceptedName = taxa.Where(t => t.Names.Canonical == subject).ToList();
            if (byAcceptedName.Count > 0) {
                return byAcceptedName.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal);
            }
            var bySynonym = taxa.Where(t => t.Names.Synonyms.Contains(subject)).ToList();
            if (bySynonym.Count > 0) {
                return bySynonym.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal);
            }
        }

        var byOwnName = taxa.Where(t => !IsThroughAnotherName(t.MatchMethod)).ToList();
        return byOwnName.Count > 0
            ? byOwnName.Select(t => t.TaxonIdentifier).ToHashSet(StringComparer.Ordinal)
            : all;
    }

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
    /// scientific name once references, templates, links and italics are removed.
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
        text = Regex.Replace(text, @"\s+", " ").Trim();
        return text.Length == 0 ? null : text;
    }
}
