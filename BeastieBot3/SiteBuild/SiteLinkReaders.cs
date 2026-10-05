using BeastieBot3.Iucn.Gbif;
using Microsoft.Data.Sqlite;

// The links and outside identifiers of each taxon, and the outside DOIs, for `site build-db`. Every
// source is opened read-only.
//
//   enwiki_title   enwiki_cache.sqlite taxon_wiki_matches: taxon_source 'iucn', match_status
//                  'matched', the final title after redirects.
//   wikidata_qid   wikidata_cache.sqlite: an item stating the IUCN taxon id (P627 claim) -> 'p627';
//                  several items -> SiteBuildRules.ChooseP627Item. Otherwise an item matched by name
//                  (wikidata_pending_iucn_matches, methods TaxonName and CachedName, not through a
//                  synonym) -> 'name-match'.
//                  An item that states the id only at deprecated rank (wikidata_deprecated_iucn_taxon_ids,
//                  which `wikidata iucn-assessment-items` fills) is chosen only when every item does
//                  (wikidata_p627_deprecated). The other items stating the id go in
//                  wikidata_other_items, with their P141 statements.
//   wikidata_p141  for a 'p627' item the cache has downloaded: its P141 statements with their rank and,
//                  from their references, the stated in (P248) items, the IUCN taxon IDs (P627), how
//                  many there are, and whether one cites IUCN (wikidata_p141_statements and
//                  wikidata_p141_references, plus the Red List editions in
//                  wikidata_iucn_red_list_editions and the assessment items, and the item's JSON for
//                  the few statements the index cannot decide), and the day it was downloaded
//                  (wikidata_item_downloaded).
//   col_id         the CoL placement file's species_match (species only; Accepted, Synonym and
//                  ProvisionallyAccepted give the accepted usage id), keyed by IUCN's own kingdom,
//                  genus and species spelling; otherwise the common names store's CoL cross-reference.
//                  Subpopulations get none: CoL has no subpopulations.
//   SPRAT          sprat.sqlite sprat_species. The profile of the whole taxon: by exact scientific
//                  name, then by each name in IUCN_Red_List_Listed_Names. Profiles of populations:
//                  every row named after the taxon with a population in brackets
//                  (SiteBuildRules.ClassifySpratName). Only an EPBC-listed row gives a status.
//                  The two listed-name columns are named from the report's header text, so a report
//                  may lack them; they then read as empty (SpratTableColumns), with a warning.
//   DOI cache     iucn_doi_cache.sqlite doi_check: DOIs `iucn resolve-dois` found in Crossref's list
//                  of IUCN DOIs or at doi.org; crossref_works: the title Crossref registered for each
//                  DOI, for the name in the titles of new Wikidata items for assessments.
//   DOIs           GBIF's copy of the IUCN checklist (the current global assessment of each taxon)
//                  and Wikidata items for assessments (wikidata_iucn_assessment_items).
//   Wikidata items for assessments: the same table, items that are publications (SiteWikidataItems).
//                  A cache written before the table existed gives none.
//   GBIF citation  The checklist's recommended citation from its eml.xml, and the dataset DOI: the
//                  citation's identifier, else the DOI in the citation text, else 10.15468/0qnb58.

namespace BeastieBot3.SiteBuild;

internal static partial class SiteLinkReaders {
    public static SqliteConnection OpenReadOnly(string path) => SiteIucnCsvReader.OpenReadOnly(path);

    // ------------------------------------------------------------ GBIF

    public static GbifChecklistInfo ReadGbif(string zipPath, IReadOnlyDictionary<long, SiteTaxon> taxa,
        SiteDoiSources dois, CancellationToken cancellationToken) {
        var checklist = GbifIucnChecklistReader.Read(zipPath, cancellationToken);
        foreach (var (taxonId, taxon) in checklist.Taxa) {
            if (taxa.ContainsKey(taxonId) && taxon.Doi is not null) {
                dois.Gbif[taxonId] = (taxon.AssessmentId, taxon.Doi);
            }
        }
        return GbifChecklistInfo.From(checklist.Summary.Dataset);
    }
}

/// What the site shows about GBIF's copy of the IUCN checklist: its version, publication date,
/// recommended citation and dataset DOI (without a resolver prefix).
internal sealed record GbifChecklistInfo(string? Version, string? Published, string? Citation, string Doi) {
    public static GbifChecklistInfo From(GbifIucnDatasetInfo dataset) => new(
        dataset.RedListVersion ?? dataset.VersionText,
        dataset.PubDate,
        dataset.Citation,
        Iucn.Gbif.IucnDoi.Extract(dataset.CitationIdentifier) ?? Iucn.Gbif.IucnDoi.Extract(dataset.Citation) ?? GbifIucnChecklistFiles.DatasetDoi);
}
