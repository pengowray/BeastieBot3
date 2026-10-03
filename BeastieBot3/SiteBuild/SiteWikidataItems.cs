using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Wikidata;

// The Wikidata items that stand for IUCN assessments as publications, for `site build-db`:
// assessment.wikidata_item_qid and assessment.wikidata_item_properties.
//
// Source: the Wikidata cache's wikidata_iucn_assessment_items table (`wikidata iucn-assessment-items`),
// items that carry an IUCN Red List DOI or assessment URL, with the taxon and assessment ids read
// from it. In October 2026 it held 6,578 items, all found by DOI:
//
//   instance of (P31)                     items   kept
//   Q13442814 scholarly article           6,571   yes
//   Q1172284  data set                        5   yes
//   Q1379672  evaluation                      1   yes (Eleutherodactylus nortoni, with the DOI, an author and the taxon)
//   Q16521 taxon + Q55808 seabird             1   no: Zino's petrel's taxon item, which carries an assessment DOI
//
// Rule: an item is kept when one of its classes is in PublicationClasses and none is taxon (Q16521).
// Any other class is left out and named in the build summary, so a new class shows up there and can
// be added here.
//
// Matching (SiteAssessmentPass): an assessment gets the item whose ids are its own taxon and
// assessment ids. An errata version with no item of its own gets the item of the assessment its
// citation's DOI names (IucnDoiSelector accepted that DOI for it: IUCN gave errata versions
// published 2015 to 2018 the DOI of the assessment they correct). The two rows then share one item,
// as they share one DOI; offering a batch to create a second item would give Wikidata two items with
// one DOI. Items whose assessment is not in the site database are counted and not used.
//
// wikidata_item_properties lists, in WikidataCitation.JudgedProperties order, the properties the
// cached row shows the item has: P31 (instance_of), P1476 (title), P1433 (published_in), P921
// (main_subjects), P953 (urls, which also holds P854 and P856 values), P577 (publication_date), P356
// (doi or all_dois), P2093 (author_string_count > 0), P50 (author_item_count > 0), and "Len" when it
// has an English label.
//
// The item's title statements (title_statements, every rank, with their language) and English label
// go into assessment.wikidata_item_titles and wikidata_item_label_en, for the site's commands that
// replace a title or label that differs from the item model (WikidataCitation.FixCommands).

namespace BeastieBot3.SiteBuild;

/// One item kept for an assessment. TitlesJson: its title statements (WikidataTitle JSON), null when
/// the cache has not recorded them; LabelEn: its English label.
internal sealed record SiteWikidataItem(string Qid, long TaxonId, long AssessmentId, string Properties,
    string? TitlesJson = null, string? LabelEn = null);

internal sealed class SiteWikidataItems {
    /// P31 classes that make an item a publication of the assessment.
    public static readonly IReadOnlyDictionary<string, string> PublicationClasses = new Dictionary<string, string>(StringComparer.Ordinal) {
        ["Q13442814"] = "scholarly article",
        ["Q1172284"] = "data set",
        ["Q1379672"] = "evaluation",
    };

    public const string TaxonClass = WikidataAssessmentItemTable.TaxonQid;

    /// Kept items by assessment id (the lowest Q-id when several name one assessment).
    public Dictionary<long, SiteWikidataItem> ByAssessment { get; } = new();

    /// Items read, kept by class ("Q13442814 scholarly article"), left out by their classes.
    public int Read { get; private set; }
    public Dictionary<string, int> KeptByClass { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, int> DroppedByClasses { get; } = new(StringComparer.Ordinal);
    /// Items with no taxon or assessment id, and further items for an assessment that already has one.
    public int WithoutIds { get; private set; }
    public int SecondItemForAnAssessment { get; private set; }
    /// Items kept whose title statements the cache has not recorded (a table filled before
    /// title_statements existed): the site offers no title change for them.
    public int TitlesNotRecorded { get; private set; }

    /// Item Q-ids that some assessment row used.
    public HashSet<string> Used { get; } = new(StringComparer.Ordinal);

    public void Add(WikidataAssessmentItemRow row) {
        Read++;
        if (!IsPublication(row.InstanceOf)) {
            var classes = row.InstanceOf.Count == 0 ? "no class" : string.Join(" ", row.InstanceOf);
            DroppedByClasses[classes] = DroppedByClasses.GetValueOrDefault(classes) + 1;
            return;
        }
        var kept = row.InstanceOf.First(PublicationClasses.ContainsKey);
        var label = $"{kept} {PublicationClasses[kept]}";
        KeptByClass[label] = KeptByClass.GetValueOrDefault(label) + 1;
        if (row.TaxonId is not { } taxonId || row.AssessmentId is not { } assessmentId) {
            WithoutIds++;
            return;
        }
        var properties = Properties(row);
        var titles = row.TitleStatements is null ? null : WikidataTitle.ListToJson(row.TitleStatements);
        if (row.TitleStatements is null) {
            TitlesNotRecorded++;
        }
        if (!ByAssessment.TryAdd(assessmentId, new SiteWikidataItem(row.Qid, taxonId, assessmentId, string.Join(' ', properties), titles, row.LabelEn))) {
            SecondItemForAnAssessment++;
        }
    }

    /// The item for an assessment: its own, else (for an errata version) the one the assessment its
    /// DOI names has. Null when neither, or when the item's taxon id is another taxon's.
    public (SiteWikidataItem Item, bool ThroughDoi)? Find(long taxonId, long assessmentId, string? doi) {
        if (ByAssessment.TryGetValue(assessmentId, out var own) && own.TaxonId == taxonId) {
            return (own, false);
        }
        if (IucnDoiSelector.TryParse(doi) is { } named && named.TaxonId == taxonId && named.AssessmentId != assessmentId
            && ByAssessment.TryGetValue(named.AssessmentId, out var shared) && shared.TaxonId == taxonId) {
            return (shared, true);
        }
        return null;
    }

    public static bool IsPublication(IReadOnlyList<string> classes) =>
        classes.Any(PublicationClasses.ContainsKey) && !classes.Contains(TaxonClass, StringComparer.Ordinal);

    /// The properties the cached row shows, in WikidataCitation.JudgedProperties order.
    public static IReadOnlyList<string> Properties(WikidataAssessmentItemRow row) {
        var present = new HashSet<string>(StringComparer.Ordinal);
        if (row.InstanceOf.Count > 0) present.Add("P31");
        if (!string.IsNullOrWhiteSpace(row.Title)) present.Add("P1476");
        if (row.PublishedIn.Count > 0) present.Add("P1433");
        if (row.MainSubjects.Count > 0) present.Add("P921");
        if (row.Urls.Count > 0) present.Add("P953");
        if (!string.IsNullOrWhiteSpace(row.PublicationDate)) present.Add("P577");
        if (!string.IsNullOrWhiteSpace(row.Doi) || row.AllDois.Count > 0) present.Add("P356");
        if (row.AuthorStringCount > 0) present.Add("P2093");
        if (row.AuthorItemCount > 0) present.Add("P50");
        if (!string.IsNullOrWhiteSpace(row.LabelEn)) present.Add(WikidataCitation.EnglishLabelToken);
        return WikidataCitation.JudgedProperties.Where(present.Contains).ToList();
    }
}
