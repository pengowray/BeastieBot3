using System.Text.RegularExpressions;

// Reading an IUCN provisional name. An assessment may be published for a species that has not been
// formally described yet, under a working name like "Notogomphus sp. nov. 'gorilla'". Where the
// quoted tag is a would-be epithet, a binomial can be built from it and looked for in the
// catalogues; where it is a locality, a collector code or a description ("Bavispe Trout",
// "B = Bester 11112", "HC - blind"), there is nothing to look up.
//
// Split out from the producer because these are the decisions worth pinning in tests: what counts
// as provisional at all, which tags yield a candidate, and which are deliberately left alone.

namespace BeastieBot3.Audit.Producers;

internal enum ProvisionalOutcome {
    /// Not a provisional name.
    NotProvisional,
    /// A binomial (or trinomial) was built from the quoted epithet.
    Candidate,
    /// The tag is qualified with "cf." or "aff.", which says the taxon resembles that species
    /// rather than being it. Deliberately not turned into a candidate.
    Qualified,
    /// The tag is a locality, collector code or description, not a would-be epithet.
    TagNotAnEpithet,
    /// No quoted tag at all: a bare "sp. nov.", or a letter or number code.
    NoTag,
}

internal readonly record struct ProvisionalName(ProvisionalOutcome Outcome, string? Tag, string? CandidateName) {
    public bool IsProvisional => Outcome != ProvisionalOutcome.NotProvisional;

    public static readonly ProvisionalName No = new(ProvisionalOutcome.NotProvisional, null, null);
}

internal static class ProvisionalNames {
    // "sp. nov.", "ssp. nov.", "subsp. nov.". The trailing period is required, so the real
    // subspecies epithet in "Conophytum flavum subsp. novicium" is not read as a marker.
    private static readonly Regex Marker = new(@"\b(?:sp|ssp|subsp)\.\s*nov\.", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // The tag IUCN writes after the marker, in single or double quotes.
    private static readonly Regex Tag = new(@"(?:sp|ssp|subsp)\.\s*nov\.\s*(?:'([^']*)'|""([^""]*)"")",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // "cf. gurneyi", "c.f. walteri", "aff. wagneri": a comparison, not an identification.
    private static readonly Regex QualifiedTag = new(@"^(?:c\.?\s*f\.?|aff\.?)\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // A would-be species epithet: one plain lower-case word, hyphens allowed.
    private static readonly Regex EpithetTag = new(@"^[a-z][a-z-]+$", RegexOptions.Compiled);

    /// Reads a provisional name and, where the tag allows, builds the described name it would have.
    /// genus and species come from the taxonomy row's own fields; species is used only for an
    /// infraspecific provisional name, where the marker sits after a real binomial.
    public static ProvisionalName Read(string? scientificName, string? genus, string? species, bool infraspecific) {
        if (string.IsNullOrWhiteSpace(scientificName) || !Marker.IsMatch(scientificName)) {
            return ProvisionalName.No;
        }

        var match = Tag.Match(scientificName);
        if (!match.Success) {
            return new ProvisionalName(ProvisionalOutcome.NoTag, null, null);
        }
        var tag = (match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value).Trim();
        if (tag.Length == 0) {
            return new ProvisionalName(ProvisionalOutcome.NoTag, null, null);
        }
        if (QualifiedTag.IsMatch(tag)) {
            return new ProvisionalName(ProvisionalOutcome.Qualified, tag, null);
        }
        if (!EpithetTag.IsMatch(tag)) {
            return new ProvisionalName(ProvisionalOutcome.TagNotAnEpithet, tag, null);
        }
        if (string.IsNullOrWhiteSpace(genus)) {
            return new ProvisionalName(ProvisionalOutcome.TagNotAnEpithet, tag, null);
        }
        // An infraspecific provisional name needs its parent binomial; without it there is no
        // trinomial to build and the tag alone says nothing.
        if (infraspecific && string.IsNullOrWhiteSpace(species)) {
            return new ProvisionalName(ProvisionalOutcome.TagNotAnEpithet, tag, null);
        }
        var candidate = infraspecific
            ? $"{genus!.Trim()} {species!.Trim()} {tag}"
            : $"{genus!.Trim()} {tag}";
        return new ProvisionalName(ProvisionalOutcome.Candidate, tag, candidate);
    }

    /// True when a name published by another source is itself provisional, so it does not answer
    /// the question this report asks.
    public static bool IsProvisional(string? name) => !string.IsNullOrWhiteSpace(name) && Marker.IsMatch(name);
}
