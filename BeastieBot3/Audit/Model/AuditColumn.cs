using System;
using System.Collections.Generic;

// A column definition shared by the HTML renderer and the CSV writer, so a report is
// described once and rendered consistently in both places. Value() returns the raw string
// (used verbatim in CSV and as the displayed text); the column Type drives HTML markup
// (badge, link, monospace, number alignment, whitespace markers). SortKey() optionally
// overrides the value used for client-side sorting (e.g. a numeric key, or a status rank).

namespace BeastieBot3.Audit.Model;

internal enum AuditColumnType {
    Text,
    Code,        // monospace
    Number,      // right-aligned, sorted numerically
    Status,      // coloured Red List category badge (value = status code)
    Taxon,       // italic scientific name, linked to RedlistUrl when present
    Url,         // external link
    LongText,    // narrative text, truncated in the cell with the full text on hover
    Whitespace,  // shows otherwise-invisible characters (spaces, tabs, NBSP) as markers
    Boolean,
    Viewer,      // a button that opens the shared modal viewer, carrying Data as data-* attributes
}

internal sealed class AuditColumn {
    public required string Key { get; init; }
    public required string Header { get; init; }
    public AuditColumnType Type { get; init; } = AuditColumnType.Text;

    // Raw display/CSV value for this finding.
    public required Func<AuditFinding, string?> Value { get; init; }

    // Optional sort key override (numeric columns sort numerically by default; use this for
    // status rank ordering or to sort a Taxon column by name). When null, the cell text is used.
    public Func<AuditFinding, string?>? SortKey { get; init; }

    // Link target for Url / Taxon columns. When null, Taxon falls back to the finding's RedlistUrl.
    public Func<AuditFinding, string?>? Href { get; init; }

    // Short help shown under the header on the full-list page.
    public string? Help { get; init; }

    // When true, the column renders only in HTML and is skipped by the CSV writer. Used by
    // interactive columns (a Viewer button) whose payload would not make sense in a flat CSV.
    public bool HtmlOnly { get; init; }

    // For a Viewer column: the data-* attributes attached to the button, each derived from the
    // finding. The key is the attribute name without the "data-" prefix (e.g. "view-html" emits
    // data-view-html); values are HTML-escaped by the renderer. Null values are omitted.
    public IReadOnlyDictionary<string, Func<AuditFinding, string?>>? Data { get; init; }

    public bool IsNumeric => Type == AuditColumnType.Number;
}
