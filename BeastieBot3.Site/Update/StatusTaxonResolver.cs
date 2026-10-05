using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// The taxon an item of the status updater is about, found from the names in its row or line, in this
/// order:
///   1. a scientific name of exactly one taxon in the release (a trinomial also with ssp., subsp. and var.);
///   2. a synonym of exactly one taxon (from any source in the name table), noted;
///   3. the taxon id in the IUCN citations on the item's status (a {{cite iucn}} written there, or a
///      named reference used there whose definition has a T…A… id), when they name one taxon, noted.
///      This also settles a name that matches several taxa, when the cited taxon is one of them;
///   4. an English common name of exactly one taxon, only when asked for (MatchCommonNames) and never
///      for an item the article gives NE; when not asked for, the note says which name would match.
/// One resolver serves one Update: ReadReferences reads the text's named references first.
public sealed partial class StatusTaxonResolver {
    private readonly IStatusLookup _lookup;
    private readonly bool _matchCommonNames;

    // The taxon ids in the IUCN citations each named reference cites ("T44853A22072238" in
    // <ref name="IUCNBroad-headedserotine">{{cite iucn |article-number=e.T44853A22072238 ...}}</ref>).
    private readonly Dictionary<string, HashSet<long>> _refTaxa = new(StringComparer.Ordinal);

    public StatusTaxonResolver(IStatusLookup lookup, bool matchCommonNames) {
        _lookup = lookup;
        _matchCommonNames = matchCommonNames;
    }

    /// Taxon: the taxon found, or null with Failure saying why. HowFound: a note on how the taxon was
    /// found when not by its scientific name, or (CommonNameNotUsed) how it could have been found.
    public sealed record NameMatch(StatusTaxon? Taxon, StatusNote? Failure, StatusNote? HowFound);

    public void ReadReferences(WikitextScanner s) {
        _refTaxa.Clear();
        foreach (Match m in RefDefinition().Matches(s.Masked)) {
            var name = m.Groups["name"].Value.Trim();
            foreach (Match id in AssessmentInText().Matches(m.Groups["body"].Value)) {
                if (long.TryParse(id.Groups["t"].Value, out var taxonId)) {
                    if (!_refTaxa.TryGetValue(name, out var ids)) {
                        _refTaxa[name] = ids = [];
                    }
                    ids.Add(taxonId);
                }
            }
        }
    }

    /// names: the names in the item's row or line. context: the spans holding the item's status and
    /// its references, where an IUCN citation is looked for; null for none. notEvaluated: the article
    /// gives the item NE. Such a taxon is often one IUCN has not split out yet ("Kruger serotine",
    /// described in 2026, is IUCN's English name for Neoromicia melckorum), so it is never found by a
    /// common name, which would give it another taxon's status.
    public NameMatch Resolve(IReadOnlyList<string> names, WikitextScanner? s, IReadOnlyList<TextSpan>? context, bool notEvaluated) {
        if (names.Count == 0) {
            return ByCitation(s, context) ?? new NameMatch(null, new StatusNote(StatusNoteKind.NoName), null);
        }
        foreach (var kind in new[] { StatusNameKind.Scientific, StatusNameKind.Synonym }) {
            var ids = new HashSet<long>();
            string? matched = null;
            foreach (var name in names) {
                foreach (var id in NameVariants(name).SelectMany(v => _lookup.InReleaseTaxaWithName(v, kind))) {
                    matched ??= name;
                    ids.Add(id);
                }
            }
            if (ids.Count == 1) {
                var note = kind == StatusNameKind.Synonym ? new StatusNote(StatusNoteKind.MatchedBySynonym, matched) : null;
                return new NameMatch(_lookup.GetTaxon(ids.First()), null, note);
            }
            if (ids.Count > 1) {
                // The item's own IUCN citation can say which of them it is.
                if (ByCitation(s, context) is { Taxon: { } settled } cited && ids.Contains(settled.TaxonId)) {
                    return cited;
                }
                return new NameMatch(null, new StatusNote(StatusNoteKind.NameAmbiguous, string.Join(", ", names), ids.Count), null);
            }
        }
        if (ByCitation(s, context) is { } byCitation) {
            return byCitation;
        }
        var notFound = new StatusNote(StatusNoteKind.NameNotFound, string.Join(", ", names));
        foreach (var name in notEvaluated ? [] : names) {
            var ids = _lookup.InReleaseTaxaWithName(name, StatusNameKind.EnglishCommonName);
            if (ids.Count != 1) {
                continue;
            }
            return _matchCommonNames
                ? new NameMatch(_lookup.GetTaxon(ids.First()), null, new StatusNote(StatusNoteKind.MatchedByCommonName, name))
                : new NameMatch(null, notFound, new StatusNote(StatusNoteKind.CommonNameNotUsed, name));
        }
        return new NameMatch(null, notFound, null);
    }

    // The one taxon the IUCN citations in the context name. Null when there is no context, or the
    // citations name no taxon or more than one.
    private NameMatch? ByCitation(WikitextScanner? s, IReadOnlyList<TextSpan>? context) {
        if (s is null || context is null) {
            return null;
        }
        var (ids, refName) = CitedTaxa(s, context);
        return ids.Count == 1
            ? new NameMatch(_lookup.GetTaxon(ids.First()), null, new StatusNote(StatusNoteKind.MatchedByCitation, refName))
            : null;
    }

    // The taxa in the release that the IUCN citations in the spans name, directly ({{cite iucn}} in
    // the span) or through a named reference used in the span (<ref name="X"/>). A taxon id not in
    // the release counts as its current taxon. RefName: the first reference used, or null when the
    // citation is written in the span.
    private (HashSet<long> TaxonIds, string? RefName) CitedTaxa(WikitextScanner s, IEnumerable<TextSpan> spans) {
        var ids = new HashSet<long>();
        string? refName = null;
        foreach (var span in spans) {
            var text = s.Masked[span.Start..span.End];
            foreach (Match id in AssessmentInText().Matches(text)) {
                if (long.TryParse(id.Groups["t"].Value, out var taxonId)) {
                    ids.Add(taxonId);
                }
            }
            foreach (Match use in RefUse().Matches(text)) {
                var name = use.Groups["name"].Value.Trim();
                if (_refTaxa.TryGetValue(name, out var cited)) {
                    ids.UnionWith(cited);
                    refName ??= name;
                }
            }
        }
        var inRelease = new HashSet<long>();
        foreach (var id in ids) {
            var taxon = _lookup.GetTaxon(id);
            if (taxon is { InRelease: true }) {
                inRelease.Add(id);
            } else if (taxon?.CurrentTaxonId is { } current) {
                inRelease.Add(current);
            }
        }
        return (inRelease, refName);
    }

    // IUCN writes a subspecies "Panthera tigris ssp. sumatrae" (animals) or "subsp." (plants) and a
    // variety "var.": a trinomial is also tried with each marker, and a marker with the other ones.
    internal static IEnumerable<string> NameVariants(string name) {
        yield return name;
        var words = name.Split(' ');
        string[] markers = ["ssp.", "subsp.", "var."];
        if (words.Length == 3) {
            foreach (var marker in markers) {
                yield return $"{words[0]} {words[1]} {marker} {words[2]}";
            }
        } else if (words.Length == 4 && markers.Contains(words[2])) {
            yield return $"{words[0]} {words[1]} {words[3]}";
            foreach (var marker in markers.Where(m => m != words[2])) {
                yield return $"{words[0]} {words[1]} {marker} {words[3]}";
            }
        }
    }

    /// An IUCN assessment's ids in text: "T22823A14871490" (in a DOI or article number) or a Red List
    /// address "/species/22823/14871490".
    [GeneratedRegex(@"T(?<t>\d+)A(?<a>\d+)|/species/(?<t>\d+)/(?<a>\d+)", RegexOptions.IgnoreCase)]
    internal static partial Regex AssessmentInText();

    [GeneratedRegex(@"<ref\s+name\s*=\s*[""']?(?<name>[^""'/>]+?)[""']?\s*>(?<body>.*?)</ref\s*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex RefDefinition();

    [GeneratedRegex(@"<ref\s+name\s*=\s*[""']?(?<name>[^""'/>]+?)[""']?\s*/?>", RegexOptions.IgnoreCase)]
    private static partial Regex RefUse();
}
