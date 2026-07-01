using System;
using System.Collections.Generic;
using BeastieBot3.Audit.Model;

// One report producer. Produce returns a fully-built AuditReport (which may legitimately have
// zero findings, rendered as "no observations this release"), or null when the underlying data
// source is unavailable so the command can skip it and tell the user what to build first.

namespace BeastieBot3.Audit.Producers;

internal interface IAuditReportProducer {
    string Id { get; }
    AuditReport? Produce(AuditContext ctx);
}

// A producer that emits several related reports from a single pass over the data, so the
// expensive shared work runs once. The CoL crosscheck uses this to publish its observation
// kinds (names not found, near matches, synonymy, placement, authority) as separate report
// pages without re-matching the whole release per kind. Returns an empty list when the data
// source is unavailable (the same meaning as a single producer returning null).
internal interface IAuditReportSetProducer {
    string Id { get; }
    IReadOnlyList<AuditReport> Produce(AuditContext ctx);
}

// Adapts an ordinary single-report producer to the set-producer contract so the command can
// iterate one uniform list. A null report becomes an empty set (skipped).
internal sealed class SingleReportProducer : IAuditReportSetProducer {
    private readonly IAuditReportProducer _inner;

    public SingleReportProducer(IAuditReportProducer inner) => _inner = inner;

    public string Id => _inner.Id;

    public IReadOnlyList<AuditReport> Produce(AuditContext ctx) {
        var report = _inner.Produce(ctx);
        return report is null ? Array.Empty<AuditReport>() : new[] { report };
    }
}
