using System;
using System.Collections.Generic;
using BeastieBot3.Audit.Commentary;

// The whole audit bundle: site-wide settings, the release it describes, provenance, and the
// ordered list of reports. Built by the command, consumed by AuditSiteRenderer.

namespace BeastieBot3.Audit.Model;

internal sealed class AuditSiteConfig {
    public string SiteTitle { get; init; } = "IUCN Red List data observations";
    public string Subtitle { get; init; } = "An unofficial, independent review to support data improvement";
    public string ContactName { get; init; } = "Pengo Wray";
    public string Contact { get; init; } = "pengowray@gmail.com";
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
}
