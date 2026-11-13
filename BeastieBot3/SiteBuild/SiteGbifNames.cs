using System.IO.Compression;
using BeastieBot3.Gbif;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.Shared.SiteData;

// Common names and synonyms from the GBIF Backbone Taxonomy (`gbif backbone-download`) for
// `site build-db`, source 'gbif'. Read from the archive in three passes: Taxon.tsv twice, then
// VernacularName.tsv.
//
// Which GBIF taxon a site taxon takes (taxa in the release; not subpopulations, which have their
// species' name; never old IUCN ids):
//   1. By id: the GBIF taxon keys on the taxon's Wikidata item (P846, which are the keys of this
//      backbone; P14607 holds GBIF's 2026 keys and is not read). A key that is a synonym leads to its
//      accepted taxon. A key in another kingdom, or at another rank (a species' key that is a genus),
//      is not used, and neither are keys that lead to two or more GBIF taxa. The name is tried then.
//   2. By name: the one accepted (else doubtful) GBIF name usage with the taxon's scientific name
//      (rank markers ignored: GBIF writes "Cerinthe glabra pirinica" for a variety) in its kingdom.
//      A name two accepted usages have in one kingdom is not matched.
//   3. A GBIF taxon that two or more site taxa lead to (GBIF lumps them) goes only to the taxon with
//      its name; when none or several have its name, none takes it.
//
// Synonyms: the usages whose accepted taxon is the matched one, with status synonym, homotypic or
// heterotypic synonym (not pro parte synonyms, as with the Catalogue of Life's ambiguous synonyms),
// written as GBIF writes the name without its authorship ("Cerinthe glabra var. pirinica").
//
// Common names: the VernacularName.tsv rows of the matched taxon, left out when
//   - GBIF took them from the Catalogue of Life or the IUCN Red List: the site reads both directly,
//     at newer releases, and a name either has dropped since would come back credited to GBIF;
//   - they come from a Handbook of the Mammals of the World chapter: GBIF labels every one of their
//     names English, the French, German and Spanish ones included ("Roussette a barbe noire"), and
//     the Catalogue of Life has the same chapters with each name's language. A chapter is a dataset
//     whose description (in the archive's dataset/ folder) names the book, or whose title is one
//     family name ("Calomyscidae"; 147 titles in the 2023-08-28 backbone, every name labelled
//     English, most with no description in the archive);
//   - they are labelled English and have no dataset title: 465 rows in the 2023-08-28 backbone,
//     the same chapters' names in four languages ("Minioptére australasien");
//   - they have no language, or a code the site cannot name (SiteLanguageCodes; English is kept);
//   - OtherLanguageNameRules finds them to be a scientific name or a name with a column heading.
// SiteDbBuild.WriteNames also leaves out an English name that no other source gives in English and
// that the taxon has in another language from any source, and SiteNameSet leaves out junk and keeps
// one row per name, language and source,
// with the titles of GBIF's source datasets in name.source_detail.
// GBIF's names do not change how the site chooses a taxon's English name (common_name_en).

namespace BeastieBot3.SiteBuild;

/// A common name from the GBIF Backbone Taxonomy: its language as stored ("en", "fr", "yue") and the
/// title of the GBIF dataset it came from (null when GBIF gives none).
internal readonly record struct SiteGbifName(string Name, string Language, string? Dataset);

/// How a site taxon was matched to a GBIF taxon, or why it was not.
internal enum GbifMatchKind {
    ById,
    ByName,
    /// Two or more accepted GBIF usages have the taxon's name in its kingdom.
    NameAmbiguous,
    /// The GBIF taxon is one that two or more site taxa lead to, and not under this taxon's name.
    Lumped,
    NotFound,
}

/// Why the taxon's GBIF ids (P846) were not used; the name was tried instead.
internal enum GbifIdProblem {
    None,
    /// No P846 key is in the backbone.
    NotInBackbone,
    /// A key is in another kingdom, or at a rank that does not fit the taxon.
    OtherKingdomOrRank,
    /// The keys lead to two or more GBIF taxa.
    Several,
}

/// What the GBIF reader did, for the build summary.
internal sealed class GbifNameCounts {
    public string? Version;
    public string? Citation;
    public string? Doi;
    /// Site taxa considered (in the release, not subpopulations).
    public int Taxa;
    public int TaxaWithId;
    public readonly Dictionary<GbifMatchKind, int> Matches = new();
    public readonly Dictionary<GbifIdProblem, int> IdProblems = new();
    public int Synonyms;
    /// The vernacular names read for matched taxa, and what the rules did with them.
    public readonly OtherNameCounts Names = new();
    /// Names GBIF took from the Catalogue of Life or the IUCN Red List, by dataset title.
    public readonly Dictionary<string, int> CopiedSources = new(StringComparer.Ordinal);
    /// Names from the Handbook of the Mammals of the World chapters (and other datasets titled with a
    /// family name), and English names with no dataset title.
    public int HandbookNames;
    public int EnglishWithoutDataset;
    /// English names that no other source gives in English and that the taxon has in another
    /// language (left out in WriteNames).
    public int EnglishInAnotherLanguage;
    /// name rows written with source 'gbif', and of those the names no other source gives the taxon
    /// in the same language, by language.
    public readonly Dictionary<string, (int Rows, int New)> ByLanguage = new(StringComparer.Ordinal);

    public int Count(GbifMatchKind kind) => Matches.GetValueOrDefault(kind);
}

internal static partial class SiteGbifNames {
    /// GBIF's titles of the datasets the site reads directly at newer releases.
    public static readonly IReadOnlySet<string> CopiedSourceTitles = new HashSet<string>(StringComparer.Ordinal) {
        "Catalogue of Life Checklist",
        "The IUCN Red List of Threatened Species",
    };

    /// Reads the archive and adds the GBIF synonyms (GbifSynonyms) and common names (GbifNames) of
    /// each matched taxon. gbifIds: the taxon's P846 values. Run after every other synonym list is
    /// read, because the name rules compare the names with the synonyms.
    public static void Read(string path, IReadOnlyCollection<SiteTaxon> taxa, IReadOnlyDictionary<long, List<string>> gbifIds,
        GbifNameCounts counts, Func<string, bool>? isEpithet, CancellationToken ct) {
        using var zip = GbifBackboneArchive.Open(path);
        var meta = GbifBackboneArchive.ReadMeta(zip);
        var info = GbifBackboneArchive.ReadInfo(zip);
        counts.Version = info.PubDate;
        counts.Citation = info.Citation;
        counts.Doi = info.Doi;

        var subjects = taxa.Where(t => t.InRelease && t.Kind != SiteTaxonKind.Subpopulation).ToList();
        counts.Taxa = subjects.Count;
        var ids = new Dictionary<long, long[]>();
        foreach (var taxon in subjects) {
            if (gbifIds.TryGetValue(taxon.TaxonId, out var values)) {
                var keys = values.Select(v => long.TryParse(v, out var k) ? k : 0).Where(k => k > 0).Distinct().ToArray();
                if (keys.Length > 0) {
                    ids[taxon.TaxonId] = keys;
                }
            }
        }
        counts.TaxaWithId = ids.Count;
        var wantedIds = ids.Values.SelectMany(k => k).ToHashSet();
        var wantedNames = subjects.Select(NameKey).ToHashSet(StringComparer.Ordinal);

        // Pass 1: the rows of the taxa's ids, and the accepted usages with one of their names.
        var idRows = new Dictionary<long, GbifUsage>();
        var nameRows = new Dictionary<string, List<GbifUsage>>(StringComparer.Ordinal);
        foreach (var row in GbifBackboneArchive.ReadTaxa(zip, meta, ct)) {
            if (wantedIds.Contains(row.Id)) {
                idRows[row.Id] = GbifUsage.Of(row);
            }
            if (GbifTaxonomicStatus.IsAccepted(row.Status) && row.CanonicalName is { } canonical
                && wantedNames.Contains(SiteNameKey.Fold(canonical))) {
                var key = SiteNameKey.Fold(canonical);
                if (!nameRows.TryGetValue(key, out var list)) {
                    nameRows[key] = list = new List<GbifUsage>();
                }
                list.Add(GbifUsage.Of(row));
            }
        }

        var candidates = new Dictionary<long, (long Key, GbifMatchKind Kind)>();
        foreach (var taxon in subjects) {
            var (key, kind, problem) = Match(taxon, ids.GetValueOrDefault(taxon.TaxonId), idRows, nameRows);
            if (problem != GbifIdProblem.None) {
                counts.IdProblems[problem] = counts.IdProblems.GetValueOrDefault(problem) + 1;
            }
            if (key is { } k) {
                candidates[taxon.TaxonId] = (k, kind);
            } else {
                counts.Matches[kind] = counts.Matches.GetValueOrDefault(kind) + 1;
            }
        }

        // Pass 2: the matched taxa's own names (for lumps), and their synonyms.
        var candidateKeys = candidates.Values.Select(c => c.Key).ToHashSet();
        var canonicalOf = new Dictionary<long, string>();
        var synonyms = new Dictionary<long, List<SiteSynonym>>();
        foreach (var row in GbifBackboneArchive.ReadTaxa(zip, meta, ct)) {
            if (candidateKeys.Contains(row.Id) && row.CanonicalName is { } canonical) {
                canonicalOf[row.Id] = canonical;
            }
            if (row.AcceptedId is { } accepted && candidateKeys.Contains(accepted) && GbifTaxonomicStatus.IsSynonym(row.Status)
                && SynonymName(row) is { } name) {
                if (!synonyms.TryGetValue(accepted, out var list)) {
                    synonyms[accepted] = list = new List<SiteSynonym>();
                }
                list.Add(new SiteSynonym(name, row.Authorship));
            }
        }

        var byTaxonId = subjects.ToDictionary(t => t.TaxonId);
        var matched = new Dictionary<long, List<SiteTaxon>>();
        foreach (var (taxonId, kind) in ResolveLumps(candidates, byTaxonId, canonicalOf)) {
            counts.Matches[kind] = counts.Matches.GetValueOrDefault(kind) + 1;
            if (kind is not (GbifMatchKind.ById or GbifMatchKind.ByName)) {
                continue;
            }
            var key = candidates[taxonId].Key;
            if (!matched.TryGetValue(key, out var list)) {
                matched[key] = list = new List<SiteTaxon>();
            }
            var taxon = byTaxonId[taxonId];
            list.Add(taxon);
            foreach (var synonym in synonyms.GetValueOrDefault(key) ?? []) {
                taxon.GbifSynonyms.Add(synonym);
                counts.Synonyms++;
            }
        }

        // Pass 3: the matched taxa's common names.
        var handbook = GbifBackboneArchive.HandbookTitles(zip);
        var keysOf = new Dictionary<long, TaxonScientificKeys>();
        foreach (var row in GbifBackboneArchive.ReadVernacularNames(zip, meta, ct)) {
            if (!matched.TryGetValue(row.TaxonId, out var matchedTaxa)) {
                continue;
            }
            foreach (var taxon in matchedTaxa) {
                if (!keysOf.TryGetValue(taxon.TaxonId, out var keys)) {
                    keysOf[taxon.TaxonId] = keys = TaxonScientificKeys.For(taxon);
                }
                Consider(taxon, row, keys, handbook, counts, isEpithet);
            }
        }
    }

    /// The site taxon's GBIF taxon before lumps are resolved: by id, else by name. Pure, for tests.
    internal static (long? Key, GbifMatchKind Kind, GbifIdProblem Problem) Match(SiteTaxon taxon, long[]? ids,
        IReadOnlyDictionary<long, GbifUsage> idRows, IReadOnlyDictionary<string, List<GbifUsage>> nameRows) {
        var problem = GbifIdProblem.None;
        if (ids is { Length: > 0 }) {
            var found = ids.Where(idRows.ContainsKey).Select(id => idRows[id]).ToList();
            if (found.Count == 0) {
                problem = GbifIdProblem.NotInBackbone;
            } else if (found.Any(u => !SameKingdom(taxon, u.Kingdom) || !FitsRank(taxon, u.Rank))) {
                problem = GbifIdProblem.OtherKingdomOrRank;
            } else {
                var keys = found.Select(u => u.AcceptedId ?? u.Id).Distinct().ToList();
                if (keys.Count == 1) {
                    return (keys[0], GbifMatchKind.ById, problem);
                }
                problem = GbifIdProblem.Several;
            }
        }
        if (!nameRows.TryGetValue(NameKey(taxon), out var usages)) {
            return (null, GbifMatchKind.NotFound, problem);
        }
        var inKingdom = usages.Where(u => SameKingdom(taxon, u.Kingdom) && FitsRank(taxon, u.Rank)).ToList();
        var best = inKingdom.Any(u => u.Status == GbifTaxonomicStatus.Accepted)
            ? inKingdom.Where(u => u.Status == GbifTaxonomicStatus.Accepted).ToList()
            : inKingdom;
        var distinct = best.Select(u => u.Id).Distinct().ToList();
        return distinct.Count switch {
            0 => (null, GbifMatchKind.NotFound, problem),
            1 => (distinct[0], GbifMatchKind.ByName, problem),
            _ => (null, GbifMatchKind.NameAmbiguous, problem),
        };
    }

    /// The outcome for each candidate: its own kind, or Lumped when its GBIF taxon is one that two or
    /// more candidates lead to and it is not the one candidate with that taxon's name. Pure, for tests.
    internal static IEnumerable<(long TaxonId, GbifMatchKind Kind)> ResolveLumps(
        IReadOnlyDictionary<long, (long Key, GbifMatchKind Kind)> candidates, IReadOnlyDictionary<long, SiteTaxon> taxa,
        IReadOnlyDictionary<long, string> canonicalOf) {
        foreach (var group in candidates.GroupBy(c => c.Value.Key)) {
            var members = group.ToList();
            if (members.Count == 1) {
                yield return (members[0].Key, members[0].Value.Kind);
                continue;
            }
            var gbifName = canonicalOf.TryGetValue(group.Key, out var canonical) ? SiteNameKey.Fold(canonical) : null;
            var named = members.Where(m => gbifName is not null && NameKey(taxa[m.Key]) == gbifName).ToList();
            foreach (var member in members) {
                yield return named.Count == 1 && named[0].Key == member.Key ? (member.Key, member.Value.Kind) : (member.Key, GbifMatchKind.Lumped);
            }
        }
    }

    private static void Consider(SiteTaxon taxon, GbifVernacularRow row, TaxonScientificKeys keys, IReadOnlySet<string> handbook,
        GbifNameCounts counts, Func<string, bool>? isEpithet) {
        var name = OtherLanguageNameRules.TrimDirectionMarks(row.Name);
        if (name.Length == 0) {
            return;
        }
        var names = counts.Names;
        names.Read++;
        if (row.Source is { } source && CopiedSourceTitles.Contains(source)) {
            counts.CopiedSources[source] = counts.CopiedSources.GetValueOrDefault(source) + 1;
            return;
        }
        if (row.Source is { } chapter && IsBookChapter(chapter, handbook)) {
            counts.HandbookNames++;
            return;
        }
        var language = Language(row.Language);
        if (language is null) {
            names.LanguageLeftOut++;
            var shown = string.IsNullOrWhiteSpace(row.Language) ? "(none)" : row.Language.Trim().ToLowerInvariant();
            names.LanguagesLeftOut[shown] = names.LanguagesLeftOut.GetValueOrDefault(shown) + 1;
            return;
        }
        if (language == "en") {
            if (string.IsNullOrWhiteSpace(row.Source)) {
                counts.EnglishWithoutDataset++;
                return;
            }
            names.English++;
        }
        var drop = OtherLanguageNameRules.Check(name, keys, isEpithet);
        if (drop != OtherNameDrop.None) {
            names.Dropped[drop] = names.Dropped.GetValueOrDefault(drop) + 1;
            return;
        }
        names.Kept++;
        taxon.GbifNames.Add(new SiteGbifName(name, language, row.Source is { Length: > 0 } dataset ? dataset : null));
    }

    /// Whether a dataset title is a book chapter's: one that the archive's dataset descriptions say
    /// is from the Handbook of the Mammals of the World, or one family name ("Calomyscidae").
    internal static bool IsBookChapter(string title, IReadOnlySet<string> handbook) =>
        handbook.Contains(title) || FamilyTitle().IsMatch(title);

    [System.Text.RegularExpressions.GeneratedRegex("^[A-Z][a-z]+idae$")]
    private static partial System.Text.RegularExpressions.Regex FamilyTitle();

    /// The stored language of a GBIF language code: "en" for English, else SiteLanguageCodes's code;
    /// null when the code is missing or the site cannot name the language.
    internal static string? Language(string? code) {
        if (string.IsNullOrWhiteSpace(code)) {
            return null;
        }
        var lower = code.Trim().ToLowerInvariant();
        return lower is "en" or "eng" ? "en" : SiteLanguageCodes.Normalise(lower);
    }

    /// The synonym's name as GBIF writes it without its authorship ("Cerinthe glabra var. pirinica"),
    /// else its canonical name; null when it has neither.
    internal static string? SynonymName(GbifTaxonRow row) {
        if (row.ScientificName is { } scientific) {
            if (row.Authorship is not { Length: > 0 } authorship) {
                return scientific;
            }
            if (scientific.EndsWith(authorship, StringComparison.Ordinal) && scientific.Length > authorship.Length) {
                return scientific[..^authorship.Length].TrimEnd();
            }
        }
        return row.CanonicalName;
    }

    /// The taxon's scientific name for matching: without IUCN's rank marker, folded (SiteNameKey.Fold).
    internal static string NameKey(SiteTaxon taxon) => SiteNameKey.Fold(StatusListNameIndex.Key(taxon.ScientificName));

    private static bool SameKingdom(SiteTaxon taxon, string? gbifKingdom) =>
        taxon.Kingdom is not { Length: > 0 } kingdom || string.Equals(kingdom.Trim(), gbifKingdom?.Trim(), StringComparison.OrdinalIgnoreCase);

    // A species takes a GBIF species; a subspecies or variety takes a GBIF subspecies, variety or form.
    private static bool FitsRank(SiteTaxon taxon, string rank) => taxon.Kind == SiteTaxonKind.Species
        ? rank == "species"
        : rank is "subspecies" or "variety" or "form";
}

/// The columns of a Taxon.tsv row that matching uses.
internal sealed record GbifUsage(long Id, long? AcceptedId, string Status, string Rank, string? Kingdom) {
    public static GbifUsage Of(GbifTaxonRow row) => new(row.Id,
        GbifTaxonomicStatus.IsAccepted(row.Status) ? null : row.AcceptedId, row.Status, row.Rank, row.Kingdom);
}
