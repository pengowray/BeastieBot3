using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

// Shared records for the Wikidata IUCN status dry run: what IUCN says about a taxon (its latest
// global assessment), what a Wikidata item currently says, how the two were linked, and the
// assessment items already on Wikidata. Readers fill these from the local caches; the link
// classifier and the edit planner are pure functions over them.
//
// Nothing here talks to Wikidata. The plan is built against cached entity JSON, so every item
// snapshot carries the revision it was read at, which becomes the edit's baserevid.

namespace BeastieBot3.WikidataEdits;

// ---------------------------------------------------------------- IUCN side

/// One person or group credited on an assessment. Type is IUCN's credit_type_name
/// ("assessor", "evaluator", "contributor", "facilitator", "compiler", ...), lower-cased.
/// Order is the position within that type, from 1, as the citation lists them.
internal sealed record IucnCredit(string Type, string Name, int Order);

/// The latest global assessment of one taxon, from the IUCN API cache.
internal sealed record IucnGlobalAssessment {
    public required long TaxonId { get; init; }
    public required long AssessmentId { get; init; }
    public required string ScientificName { get; init; }
    public required bool IsInfrarank { get; init; }
    /// IUCN code as published: EX, EW, CR, EN, VU, NT, LC, DD, or a 2.3 code (LR/cd, LR/nt, LR/lc).
    public required string CategoryCode { get; init; }
    public bool PossiblyExtinct { get; init; }
    public bool PossiblyExtinctInTheWild { get; init; }
    public string? Criteria { get; init; }
    /// Criteria version ("3.1", "2.3"), not the Red List release.
    public string? CriteriaVersion { get; init; }
    public int? YearPublished { get; init; }
    public DateOnly? AssessmentDate { get; init; }
    /// https://www.iucnredlist.org/species/{taxonId}/{assessmentId}
    public required string Url { get; init; }
    /// Citation text with IUCN's trailing "Accessed on ..." removed (that date is only our download date).
    public string? Citation { get; init; }
    /// The DOI when the citation carries one (most don't), without the https://doi.org/ prefix.
    public string? Doi { get; init; }
    /// Assessors first, as the citation names them; then the other credit types.
    public IReadOnlyList<IucnCredit> Credits { get; init; } = Array.Empty<IucnCredit>();
    /// When the assessment JSON was downloaded: the "retrieved" date for a reference.
    public required DateTime DownloadedAtUtc { get; init; }
    /// The assessment carries errata (an amended version); YearPublished is then the amendment year.
    public bool IsAmended { get; init; }
}

// ---------------------------------------------------------------- Wikidata side

/// A reference block on a statement. Values maps a property to its snak values rendered as plain
/// strings: an item id ("Q136547248"), a string or external id ("15951"), a URL, or a time in
/// Wikibase form ("+2025-11-12T00:00:00Z"). Raw is the reference JSON exactly as cached.
internal sealed record WdReference(
    string? Hash,
    IReadOnlyDictionary<string, IReadOnlyList<string>> Values,
    JsonObject Raw);

/// One statement. Rank is "preferred", "normal" or "deprecated". ValueId is set for item values,
/// ValueString for string/external-id values; both are null for somevalue/novalue snaks.
internal sealed record WdStatement(
    string Id,
    string Property,
    string Rank,
    string? ValueId,
    string? ValueString,
    IReadOnlyDictionary<string, IReadOnlyList<string>> Qualifiers,
    IReadOnlyList<WdReference> References,
    JsonObject Raw);

/// What a taxon item says now, as far as the status edit cares.
internal sealed record WdTaxonItem {
    public required string Qid { get; init; }
    /// The revision the cached JSON was read at; an edit built on it sends this as baserevid.
    public required long LastRevId { get; init; }
    public DateTime? DownloadedAtUtc { get; init; }
    public string? LabelEn { get; init; }
    /// P31 instance of.
    public IReadOnlyList<string> InstanceOf { get; init; } = Array.Empty<string>();
    /// P225 taxon name values (non-deprecated).
    public IReadOnlyList<string> TaxonNames { get; init; } = Array.Empty<string>();
    /// P105 taxon rank item ids (non-deprecated).
    public IReadOnlyList<string> TaxonRanks { get; init; } = Array.Empty<string>();
    /// P171 parent taxon item ids (non-deprecated).
    public IReadOnlyList<string> ParentTaxa { get; init; } = Array.Empty<string>();
    /// Every P627 IUCN taxon ID statement, all ranks.
    public IReadOnlyList<WdStatement> IucnTaxonIds { get; init; } = Array.Empty<WdStatement>();
    /// Every P141 IUCN conservation status statement, all ranks.
    public IReadOnlyList<WdStatement> ConservationStatuses { get; init; } = Array.Empty<WdStatement>();
}

// ---------------------------------------------------------------- linking

/// How an IUCN taxon came to be linked to an item, strongest first.
internal enum LinkSource {
    /// The item carries a P627 claim with the taxon id.
    P627Claim,
    /// backfill-iucn: exact P225 match on the IUCN name.
    SearchTaxonName,
    /// backfill-iucn: exact P225 match on a synonym (IUCN or CoL-derived; which one is not recorded).
    SearchTaxonNameSynonym,
    /// backfill-iucn: the name matched an item already in the local cache, no online search.
    CachedName,
    /// backfill-iucn: English label match on a taxon item.
    Label,
}

internal sealed record TaxonItemLink(long TaxonId, string Qid, LinkSource Source, string? MatchedName);

/// An item already on Wikidata for one IUCN assessment publication (most are scholarly articles
/// created from DOIs in 2018). TaxonId/AssessmentId are parsed from the DOI or URL when present.
internal sealed record ExistingAssessmentItem {
    public required string Qid { get; init; }
    public long? TaxonId { get; init; }
    public long? AssessmentId { get; init; }
    public string? Doi { get; init; }
    public string? Title { get; init; }
    public IReadOnlyList<string> InstanceOf { get; init; } = Array.Empty<string>();
    /// P921 main subject.
    public IReadOnlyList<string> MainSubjects { get; init; } = Array.Empty<string>();
    public int? PublicationYear { get; init; }
}
