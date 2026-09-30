using System;
using System.Collections.Generic;
using BeastieBot3.Audit.Commentary;

// The whole audit bundle: site-wide settings, the release it describes, provenance, and the
// ordered list of reports. Built by the command, consumed by AuditSiteRenderer.

namespace BeastieBot3.Audit.Model;

internal sealed class AuditSiteConfig {
    public string SiteTitle { get; init; } = "IUCN Red List data observations";
    public string Subtitle { get; init; } = "Unofficial notes for the next release's data review";
    public string ContactName { get; init; } = "Pengo Wray";
    public string Contact { get; init; } = "feedback@pengowray.com";
    public string CsvLicence { get; init; } = "CC0 1.0 (public domain dedication)";
}

internal sealed record AuditDataSource(string Name, string Detail);

internal sealed class AuditDocument {
    public required string Release { get; init; }        // e.g. "2025-2"
    public int? ReleaseYear { get; init; }
    public required string GeneratedAt { get; init; }    // e.g. "2026-06-26"
    public IReadOnlyList<AuditDataSource> DataSources { get; init; } = Array.Empty<AuditDataSource>();
    public required IReadOnlyList<AuditReport> Reports { get; init; }
    public AuditSiteConfig Config { get; init; } = new();
    public AuditCommentary? CommentarySource { get; init; }

    // Row counts from the most recent earlier release in release-counts.yml, for the index's
    // "Since <release>" column. Null when no earlier release is recorded.
    public string? PreviousRelease { get; init; }
    public AuditReleaseCounts? ReleaseCounts { get; init; }

    // The --limit value of a limited run: most producers read at most this many database rows, so
    // every count in the document is partial. Null for a full run. Every page of a limited run
    // carries a notice saying so (AuditPageLayout), and the run writes no release-counts.yml.
    public long? RowLimit { get; init; }

    public bool IsLimited => RowLimit is not null;

    // The release the "Since <release>" columns compare against. Null for a limited run as well as
    // when no earlier release is recorded: a partial count beside a full count from the previous
    // release would read as a real change ("fixed (was 3,898)").
    public string? SinceRelease => IsLimited ? null : PreviousRelease;
}
