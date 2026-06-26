using System.Collections.Generic;

// One row of an audit report, in a shared shape so every listing can be rendered by the
// same HtmlListRenderer and written to CSV the same way. Not every field applies to every
// report; producers populate what they have and leave the rest null. Report-specific values
// that do not fit a canonical field go in Extra, keyed by column key.

namespace BeastieBot3.Audit.Model;

internal sealed class AuditFinding {
    public string ReportId { get; init; } = "";

    // Stable identity for this finding within its report (e.g. "{taxonId}:{issueType}"), used to
    // pin one-time commentary to a specific row so the note survives re-sorting and can be
    // re-evaluated against the next release.
    public string? Key { get; init; }

    // Identity / linking. taxonId is the IUCN SIS id (the importer maps internalTaxonId ->
    // taxonId, and the API's root_sis_id / sis_taxon_id are the same value).
    public long? TaxonId { get; init; }
    public long? AssessmentId { get; init; }
    public string? RedlistUrl { get; init; }

    // Names.
    public string? ScientificName { get; init; }
    public string? CommonName { get; init; }
    public string? InfraType { get; init; }
    public string? InfraName { get; init; }
    public string? SubpopulationName { get; init; }

    // Rank: "species" when infraType and subpopulationName are both empty.
    public string? Rank { get; init; }
    public bool IsFullSpecies { get; init; }

    // Linnaean ladder (as stored in the IUCN export; high ranks are UPPERCASE there).
    public string? Kingdom { get; init; }
    public string? Phylum { get; init; }
    public string? Class { get; init; }
    public string? Order { get; init; }
    public string? Family { get; init; }
    public string? Genus { get; init; }
    public string? Species { get; init; }

    // Status, normalized to a short code (EX, CR, EN, ...) plus the raw category text.
    public string? StatusCode { get; init; }
    public string? StatusCategory { get; init; }
    public string? YearPublished { get; init; }

    // True/false/unknown: the CSV export is latest-only so CSV-sourced rows leave this null.
    public bool? Latest { get; init; }

    public string DataSource { get; init; } = "";

    // Field-level detail for cleanup/formatting findings.
    public string? Field { get; init; }
    public string? CurrentValue { get; init; }
    public string? SuggestedValue { get; init; }
    public string? IssueType { get; init; }

    // Higher number sorts first (more likely to help). Producers assign this; the renderer
    // keeps the producer's order for the default view and the short preview.
    public int SeverityTier { get; init; }

    public string? Detail { get; init; }

    // Release-agnostic notes derived from this row's own fields (carry forward across
    // releases) plus any finding-keyed one-time commentary attached by the producer.
    public List<string> Notes { get; } = new();

    // Report-specific values keyed by AuditColumn.Key, for columns outside the canonical set.
    public Dictionary<string, string?> Extra { get; } = new();

    public string? Get(string key) => Extra.TryGetValue(key, out var v) ? v : null;
}
