using BeastieBot3.Audit.Model;

// One report producer. Produce returns a fully-built AuditReport (which may legitimately have
// zero findings, rendered as "no observations this release"), or null when the underlying data
// source is unavailable so the command can skip it and tell the user what to build first.

namespace BeastieBot3.Audit.Producers;

internal interface IAuditReportProducer {
    string Id { get; }
    AuditReport? Produce(AuditContext ctx);
}
