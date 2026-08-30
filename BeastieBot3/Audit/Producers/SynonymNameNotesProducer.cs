using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BeastieBot3.Audit.Model;
using BeastieBot3.Infrastructure;

// Synonyms whose name field carries a nomenclatural note, e.g. "Eumeces schneideri (Daudin, 1802)
// [orth. error]". Separate from the whitespace and markup reports because the observation is
// different: the text is well-formed, it just is not only a name. The second summary table is the
// one that carries the work, listing the notes that are written more than one way.
//
// Bracketed publication years ("[1803]") are counted but not listed: that is standard nomenclature.
// Reads SynonymFormattingScan.

namespace BeastieBot3.Audit.Producers;

internal sealed class SynonymNameNotesProducer : IAuditReportProducer {
    public string Id => "synonym-name-notes";

    public AuditReport? Produce(AuditContext ctx) {
        var conn = ctx.IucnApiCacheOrNull();
        if (conn is null || !AuditContext.ObjectExists(conn, "taxa")) {
            return null;
        }

        var scan = SynonymFormattingScan.Scan(conn, ctx);

        var findings = scan.NoteRecords
            .Select(BuildFinding)
            .OrderBy(f => f.IssueType, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.TaxonId)
            .ThenBy(f => f.CurrentValue, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var allNotes = scan.NoteRecords.SelectMany(r => r.Notes).ToList();

        return new AuditReport {
            Id = Id,
            Title = "Notes written inside synonym names",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.FixableData,
            DataSourceLabel = "IUCN API (taxon synonyms)",
            Blurb = "Synonyms whose name field carries a bracketed nomenclatural note, such as [orth. error], alongside the name itself.",
            Summary =
                "Each row is a synonym whose name field also carries a nomenclatural note in square brackets, such as " +
                "`Eumeces schneideri (Daudin, 1802) [orth. error]`, together with the name on its own. " +
                "The scientific name column is the accepted taxon the synonym belongs to.\n\n" +
                "Not every square bracket is treated as a note. Three standard uses of brackets are counted in the summary but not listed: " +
                "an inferred publication year (`[1803]`), an inferred or attributed author (`Anonymous [Bennett], 1830`), " +
                "and an expansion of an abbreviated name (`S[imia] erythropyga`). Those are conventional nomenclature and belong where they are.\n\n" +
                "### Why it matters\n\n" +
                "While a note sits in the name field, the field holds more than a name. " +
                "A search index, an export, or another database that matches on the name treats the note as part of the text, so the lookup fails. " +
                "Listing the notes together also shows a second problem: the same note is spelled several ways (the second table below).\n\n" +
                "### Suggestion\n\n" +
                "Keep the note, but hold it in a field of its own so the name field holds only the name. " +
                "Settle on one spelling per note: the variants table shows which ones currently have several.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("Accepted taxon"),
                AuditColumns.CurrentValue("Synonym (current)", AuditColumnType.Whitespace),
                AuditColumns.SuggestedValue("Name without the note", AuditColumnType.Code),
                AuditColumns.IssueType("Note"),
                AuditColumns.Status(),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId(),
                AuditColumns.AssessmentId(),
                AuditColumns.RedlistLink(),
            },
            Findings = findings,
            SummaryTables = new List<AuditSummaryTable> {
                ByNote(allNotes, scan),
                VariantSpellings(allNotes),
            },
        };
    }

    private static AuditFinding BuildFinding(SynonymNoteRecord record) {
        var r = record.Record;
        var labels = string.Join("; ", record.Notes.Select(n => n.Text));
        var finding = new AuditFinding {
            ReportId = "synonym-name-notes",
            Key = $"{r.RootSisId}:{r.Synonym}",
            TaxonId = r.RootSisId,
            AssessmentId = r.AssessmentId,
            RedlistUrl = !string.IsNullOrEmpty(r.Url) ? r.Url : IucnUrls.Species(r.RootSisId, r.AssessmentId),
            ScientificName = r.AcceptedName,
            CommonName = r.CommonName,
            Kingdom = r.Kingdom,
            Phylum = r.Phylum,
            Class = r.Class,
            Order = r.Order,
            Family = r.Family,
            StatusCode = r.StatusCode,
            StatusCategory = r.StatusCategory,
            YearPublished = r.Year,
            DataSource = "iucn-api",
            Field = "synonym",
            CurrentValue = r.Synonym,
            SuggestedValue = r.Suggested,
            IssueType = labels,
            SeverityTier = record.Notes.Count,
            Detail = labels,
        };
        if (r.Suggested is null) {
            finding.Notes.Add("Nothing is left once the note is removed, so no name can be suggested.");
        }
        return finding;
    }

    // Every distinct note and how many synonyms carry it, most common first.
    private static AuditSummaryTable ByNote(IReadOnlyList<SynonymNameNote> notes, SynonymScanResult scan) {
        var rows = notes
            .GroupBy(n => n.Text, StringComparer.Ordinal)
            .OrderByDescending(g => g.Count())
            .ThenBy(g => g.Key, StringComparer.OrdinalIgnoreCase)
            .Select(g => new[] { g.Key, g.Count().ToString("N0", CultureInfo.InvariantCulture) } as IReadOnlyList<string>)
            .ToList();
        rows.Add(new[] { "Total", notes.Count.ToString("N0", CultureInfo.InvariantCulture) });

        var distinct = Math.Max(0, rows.Count - 1);
        return new AuditSummaryTable {
            Title = "Notes by text",
            Note = $"{distinct:N0} distinct notes across {notes.Count:N0} occurrences, out of {scan.TotalSynonyms:N0} synonym names examined. " +
                   "Occurrences can outnumber the rows above because a synonym can carry more than one note. " +
                   $"Standard bracket uses are not counted here: {scan.DateBracketCount:N0} bracketed publication years and {scan.AuthorBracketCount:N0} bracketed author attributions or in-name expansions.",
            Headers = new[] { "Note", "Occurrences" },
            Rows = rows,
            NumericColumns = new[] { 1 },
        };
    }

    // Notes whose wording matches once punctuation, spacing and case are set aside, but whose
    // spelling differs. This is the table to act on.
    private static AuditSummaryTable VariantSpellings(IReadOnlyList<SynonymNameNote> notes) {
        var groups = notes
            .GroupBy(n => n.Key, StringComparer.Ordinal)
            .Select(g => new {
                Spellings = g.GroupBy(n => n.Text, StringComparer.Ordinal)
                             .OrderByDescending(s => s.Count())
                             .ThenBy(s => s.Key, StringComparer.Ordinal)
                             .ToList(),
                Total = g.Count(),
            })
            .Where(x => x.Spellings.Count > 1)
            .OrderByDescending(x => x.Total)
            .ToList();

        var rows = groups
            .Select(x => new[] {
                string.Join(" · ", x.Spellings.Select(s => $"{s.Key} ({s.Count():N0})")),
                x.Spellings.Count.ToString("N0", CultureInfo.InvariantCulture),
                x.Total.ToString("N0", CultureInfo.InvariantCulture),
            } as IReadOnlyList<string>)
            .ToList();

        var note = rows.Count == 0
            ? "Every note is written one way. There is nothing to reconcile."
            : $"{rows.Count:N0} notes are written more than one way, covering {groups.Sum(g => g.Total):N0} occurrences. " +
              "Spellings are grouped by their wording with punctuation, spacing and case set aside, so orth. error and orth.error appear on one row.";

        return new AuditSummaryTable {
            Title = "Notes written more than one way",
            Note = note,
            Headers = new[] { "Spellings found", "Spellings", "Occurrences" },
            Rows = rows,
            NumericColumns = new[] { 1, 2 },
        };
    }
}
