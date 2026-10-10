using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;

// The other taxa in the release named in the taxonomic notes of a taxon's latest global assessment
// (notes_taxon), for "Named in IUCN's taxonomic notes" on its page. TaxonomicNotesNames reads the
// names; this finds the taxon each one is, in the same kingdom:
//
//   1. the taxa in the release with that scientific name, rank markers ignored ("Sarotherodon
//      tournieri liberiensis" is "Sarotherodon tournieri ssp. liberiensis");
//   2. else the taxa in the release whose IUCN synonyms have that name ("Cebuella pygmaea
//      niveiventris" is Cebuella niveiventris, which lists "Cebuella pygmaea ssp. niveiventris").
//
// A name is left out when it is the taxon's own name or one of its own IUCN synonyms, when it fits
// two or more taxa, and when the taxon it fits is the assessed taxon, its species, or one of its
// subspecies or varieties (same genus and species epithet), which the page lists already. The notes
// say nothing the site can read about how the taxa are related, so no relation is stored.

namespace BeastieBot3.SiteBuild;

/// NameInNotes: the name as the notes write it, when it is not the named taxon's own scientific name
/// (rank markers, abbreviations and spacing aside); null when it is.
internal sealed record SiteNotesTaxon(long TaxonId, long NamedTaxonId, long AssessmentId, string? NameInNotes, int Position);

internal static partial class SiteNotesTaxa {
    /// notes: the names read from each taxon's latest global assessment, by taxon id. Call it while
    /// the taxa still have their IUCN synonyms (before the names are written).
    public static List<SiteNotesTaxon> Find(IReadOnlyCollection<SiteTaxon> taxa,
        IReadOnlyDictionary<long, (long AssessmentId, IReadOnlyList<NotesName> Names)> notes, SiteBuildStats stats) {
        var inRelease = taxa.Where(t => t.InRelease && !string.IsNullOrWhiteSpace(t.Kingdom)).ToList();
        var byName = inRelease
            .GroupBy(t => (Kingdom(t), Key(t.ScientificName)))
            .ToDictionary(g => g.Key, g => g.ToList());
        var bySynonym = new Dictionary<(string, string), List<SiteTaxon>>();
        foreach (var taxon in inRelease) {
            foreach (var key in taxon.IucnSynonyms.Select(s => Key(s.Name)).Distinct(StringComparer.Ordinal)) {
                if (!bySynonym.TryGetValue((Kingdom(taxon), key), out var list)) {
                    bySynonym[(Kingdom(taxon), key)] = list = [];
                }
                list.Add(taxon);
            }
        }
        var byId = inRelease.ToDictionary(t => t.TaxonId);

        var rows = new List<SiteNotesTaxon>();
        foreach (var (taxonId, (assessmentId, names)) in notes.OrderBy(n => n.Key)) {
            if (!byId.TryGetValue(taxonId, out var taxon)) {
                continue;
            }
            var kingdom = Kingdom(taxon);
            var ownKeys = taxon.IucnSynonyms.Select(s => Key(s.Name)).Append(Key(taxon.ScientificName)).ToHashSet(StringComparer.Ordinal);
            var named = new HashSet<long>();
            foreach (var name in names) {
                var key = Key(name.Full);
                if (ownKeys.Contains(key)) {
                    continue;
                }
                var candidates = byName.TryGetValue((kingdom, key), out var withName) ? withName
                    : bySynonym.TryGetValue((kingdom, key), out var listing) ? listing
                    : null;
                if (candidates is null) {
                    continue;
                }
                if (candidates.Count != 1) {
                    stats.NotesNamesOfSeveralTaxa++;
                    continue;
                }
                var other = candidates[0];
                if (other.TaxonId == taxon.TaxonId || SameSpecies(other, taxon) || !named.Add(other.TaxonId)) {
                    continue;
                }
                var nameInNotes = Key(other.ScientificName) == key ? null : name.Written;
                rows.Add(new SiteNotesTaxon(taxonId, other.TaxonId, assessmentId, nameInNotes, named.Count - 1));
            }
            if (named.Count > 0) {
                stats.TaxaWithNotesTaxa++;
            }
        }
        stats.NotesTaxa = rows.Count;
        return rows;
    }

    // A name folded (SiteNameKey.Fold) with its rank markers removed.
    internal static string Key(string name) => SiteNameKey.Fold(RankMarker().Replace(name, " "));

    private static string Kingdom(SiteTaxon taxon) => taxon.Kingdom!.Trim().ToUpperInvariant();

    // The same genus and species epithet: a species and its own subspecies and varieties.
    private static bool SameSpecies(SiteTaxon a, SiteTaxon b) =>
        a.Genus is not null && a.SpeciesEpithet is not null
        && string.Equals(a.Genus, b.Genus, StringComparison.OrdinalIgnoreCase)
        && string.Equals(a.SpeciesEpithet, b.SpeciesEpithet, StringComparison.OrdinalIgnoreCase);

    [GeneratedRegex(@"\s(?:ssp|subsp|var)\.\s", RegexOptions.IgnoreCase)]
    private static partial Regex RankMarker();
}
