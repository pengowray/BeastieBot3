using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Col;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

// Where IUCN accepted names and classification diverge from the Catalogue of Life reference: names
// CoL has no exact match for, names CoL treats as a synonym of a different accepted name, authority
// differences, and higher-rank placement differences. Replicates the matching in
// IucnColCrosscheckCommand (which is private) and reuses the CoL repository and ladder helpers.
//
// This is much higher volume than the other reports, so the HTML pages list the high-signal rows
// (name not in CoL, CoL synonym, and placement differences above genus), and the authority and
// genus/species-level differences are summarised by class. The CSV download carries every row.

namespace BeastieBot3.Audit.Producers;

internal sealed class ColCrosscheckProducer : IAuditReportProducer {
    public string Id => "col-crosscheck";

    // Ladder ranks treated as name-level rather than higher-taxonomy placement.
    private static readonly HashSet<string> NameLevelRanks = new(StringComparer.OrdinalIgnoreCase) {
        "genus", "subgenus", "species", "subspecies", "variety", "form", "forma",
    };

    public AuditReport? Produce(AuditContext ctx) {
        var iucn = ctx.IucnCsvOrNull();
        var col = ctx.ColOrNull();
        if (iucn is null || col is null) {
            return null;
        }
        var iucnRepo = new IucnTaxonomyRepository(iucn);
        if (!iucnRepo.ObjectExists("view_assessments_html_taxonomy_html", "view") || !AuditContext.ObjectExists(col, "nameusage")) {
            return null;
        }
        var colRepo = new ColTaxonRepository(col);

        var rows = iucnRepo.ReadRows(0, ctx.Ct)
            .Where(r => string.IsNullOrWhiteSpace(r.SubpopulationName))
            .ToList();
        if (ctx.Limit is > 0 && rows.Count > ctx.Limit.Value) {
            rows = rows.Take((int)ctx.Limit.Value).ToList();
        }

        var all = new List<AuditFinding>();
        var buffer = new List<TaxonLadder>();
        foreach (var row in rows) {
            ctx.Ct.ThrowIfCancellationRequested();
            all.AddRange(Classify(row, colRepo, buffer, ctx.Ct));
        }

        static int Order(AuditFinding f) => f.SeverityTier;
        var allOrdered = all
            .OrderByDescending(Order)
            .ThenBy(f => f.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Order, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.Family, StringComparer.OrdinalIgnoreCase)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var highSignal = allOrdered.Where(IsHighSignal).ToList();

        var byIssue = new[] { "missing-from-col", "synonym-in-col", "classification-difference", "authority-difference" }
            .Select(k => new[] { Label(k), allOrdered.Count(f => f.IssueType == k).ToString("N0") } as IReadOnlyList<string>)
            .ToList();

        var summaryTables = new List<AuditSummaryTable> {
            new() {
                Title = "All differences by kind",
                Note = $"Over {rows.Count:N0} assessments compared. The full set of every row below is in the CSV download.",
                Headers = new[] { "Difference", "Count" }, Rows = byIssue, NumericColumns = new[] { 1 },
            },
            ByClassSummary("Authority differences by class", allOrdered.Where(f => f.IssueType == "authority-difference")),
            ByClassSummary("Genus and species placement differences by class", allOrdered.Where(f => f.IssueType == "classification-difference" && IsNameLevel(f.Field))),
        };

        return new AuditReport {
            Id = Id,
            Title = "Differences from the Catalogue of Life reference",
            Tier = AuditReportTier.IucnCore,
            Breakage = BreakageClass.Advisory,
            DataSourceLabel = $"IUCN Red List {ctx.Release} vs Catalogue of Life",
            Summary =
                "Each row pairs an IUCN assessment with its best Catalogue of Life match and records where the two independent catalogues differ. " +
                "These describe divergence between two catalogues rather than a defect in either. The primary match is exact, but when it fails a fuzzy pass looks for near matches and the row names the closest CoL candidate and how it differs (punctuation, diacritics, Unicode encoding, or a spelling variant). " +
                "The lists below show the higher-signal rows: a name with no exact CoL match, a name CoL treats as a synonym, and placement differences above genus. " +
                "Authority differences and genus or species level placement differences are summarised by class here and are included in full in the CSV download.",
            Columns = new List<AuditColumn> {
                AuditColumns.ScientificName("IUCN name"),
                AuditColumns.Rank(),
                AuditColumns.IssueType("Difference"),
                AuditColumns.Field(),
                AuditColumns.CurrentValue("IUCN value", AuditColumnType.Text),
                AuditColumns.SuggestedValue("CoL value", AuditColumnType.Text),
                AuditColumns.Category("IUCN status"),
                AuditColumns.Class(),
                AuditColumns.Family(),
                AuditColumns.TaxonId("Taxon id"),
                AuditColumns.RedlistLink(),
                AuditColumns.Detail(),
            },
            Findings = highSignal,
            CsvFindings = allOrdered,
            HeadlineCount = highSignal.Count,
            SummaryTables = summaryTables,
            GroupLevels = AuditGroups.ByClassOrderFamily,
        };
    }

    private static bool IsHighSignal(AuditFinding f) => f.IssueType switch {
        "missing-from-col" => true,
        "synonym-in-col" => true,
        "classification-difference" => !IsNameLevel(f.Field),
        _ => false, // authority-difference and others: CSV + summary only
    };

    private static bool IsNameLevel(string? rank) => rank is not null && NameLevelRanks.Contains(rank);

    private static AuditSummaryTable ByClassSummary(string title, IEnumerable<AuditFinding> findings) {
        var rows = findings
            .GroupBy(f => f.Class ?? "(unspecified)")
            .OrderByDescending(g => g.Count())
            .Take(15)
            .Select(g => new[] { g.Key, g.Count().ToString("N0") } as IReadOnlyList<string>)
            .ToList();
        if (rows.Count == 0) {
            rows.Add(new[] { "(none)", "0" });
        }
        return new AuditSummaryTable {
            Title = title,
            Note = "Top classes shown. Every row is in the CSV download.",
            Headers = new[] { "Class", "Count" }, Rows = rows, NumericColumns = new[] { 1 },
        };
    }

    private static IEnumerable<AuditFinding> Classify(IucnTaxonomyRow row, ColTaxonRepository colRepo, List<TaxonLadder> buffer, CancellationToken ct) {
        var (primary, accepted, candidates, method) = FindBestMatch(row, colRepo, ct);
        var (rank, isFull) = AuditMapping.Rank(row.InfraType, row.SubpopulationName);
        var name = AuditMapping.Decode(!string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy) ? row.ScientificNameTaxonomy : row.ScientificNameAssessments);

        AuditFinding Make(string issueType, string? field, string? current, string? suggested, int severity, string detail) => new() {
            ReportId = "col-crosscheck",
            Key = $"{row.TaxonId}:{issueType}:{field}",
            TaxonId = row.TaxonId,
            AssessmentId = row.AssessmentId,
            RedlistUrl = IucnUrls.Species(row.TaxonId, row.AssessmentId),
            ScientificName = name ?? $"SIS {row.TaxonId}",
            Rank = rank,
            IsFullSpecies = isFull,
            InfraType = row.InfraType,
            InfraName = row.InfraName,
            Kingdom = row.KingdomName,
            Phylum = row.PhylumName,
            Class = row.ClassName,
            Order = row.OrderName,
            Family = row.FamilyName,
            Genus = row.GenusName,
            Species = row.SpeciesName,
            StatusCode = AuditMapping.CodeFromCategory(row.RedlistCategory),
            StatusCategory = row.RedlistCategory,
            DataSource = "iucn-csv+col",
            Field = field,
            CurrentValue = current,
            SuggestedValue = suggested,
            IssueType = issueType,
            SeverityTier = severity,
            Detail = detail,
        };

        if (primary is null) {
            var (suggested, detail) = SuggestNearMatches(row, colRepo, name, ct);
            yield return Make("missing-from-col", "scientificName", name, suggested, 4, detail);
            yield break;
        }

        if (accepted is not null && !string.Equals(primary.Id, accepted.Id, StringComparison.Ordinal)) {
            yield return Make("synonym-in-col", "scientificName", name, AuditMapping.Decode(accepted.ScientificName), 3,
                $"Catalogue of Life treats this name as a synonym of {AuditMapping.Decode(accepted.ScientificName)}.");
        }

        var iucnAuthority = AuditMapping.Decode(GetIucnAuthority(row));
        var colAuthority = AuditMapping.Decode(primary.Authorship);
        if (!string.IsNullOrWhiteSpace(iucnAuthority) && !string.IsNullOrWhiteSpace(colAuthority) && !AuthorityNormalizer.Equivalent(iucnAuthority, colAuthority)) {
            yield return Make("authority-difference", "authority", iucnAuthority, colAuthority, 1,
                $"Naming authority differs: IUCN '{iucnAuthority}' versus CoL '{colAuthority}'.");
        }

        foreach (var diff in ClassificationDifferences(row, primary, accepted, colRepo, buffer, ct)) {
            var severity = IsNameLevel(diff.Rank) ? 1 : 2;
            yield return Make("classification-difference", diff.Rank, diff.Iucn, diff.Col, severity,
                $"{Capitalise(diff.Rank)} placement differs between IUCN and Catalogue of Life.");
        }
    }

    // When no exact CoL match exists, look for near matches: names in the same genus or sharing the
    // same epithet, ranked by how they differ. A formatting-equivalent name (punctuation, diacritics,
    // Unicode encoding, ...) is reported as the likely same name; otherwise the closest spelling
    // variants are offered as possible alternatives. Returns the best CoL name for the suggested
    // column plus a human-readable detail line.
    private static (string? Suggested, string Detail) SuggestNearMatches(
        IucnTaxonomyRow row, ColTaxonRepository colRepo, string? iucnName, CancellationToken ct) {
        const string noneDetail = "No Catalogue of Life match for this name, and no close candidate found by fuzzy search.";
        if (string.IsNullOrWhiteSpace(iucnName)) {
            return (null, noneDetail);
        }

        var pool = new List<ColTaxonRecord>();
        if (!string.IsNullOrWhiteSpace(row.GenusName)) {
            pool.AddRange(colRepo.FindByGenericName(row.GenusName!, ct));
        }
        if (!string.IsNullOrWhiteSpace(row.SpeciesName)) {
            pool.AddRange(colRepo.FindBySpecificEpithet(row.SpeciesName!, ct));
        }

        var scored = new List<(string Name, ScientificNameDifference.Result Diff)>();
        var seenNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var c in pool.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First())) {
            ct.ThrowIfCancellationRequested();
            var colName = AuditMapping.Decode(c.ScientificName);
            if (string.IsNullOrWhiteSpace(colName) || !seenNames.Add(colName!)) {
                continue;
            }
            var diff = ScientificNameDifference.Classify(iucnName!, colName!);
            if (diff.Kind is ScientificNameDifference.Kind.Exact or ScientificNameDifference.Kind.Unrelated) {
                continue;
            }
            scored.Add((colName!, diff));
        }

        if (scored.Count == 0) {
            return (null, noneDetail);
        }

        // Formatting-equivalent matches first (these are very likely the same name), then by edit
        // distance, then alphabetically for a stable order.
        scored.Sort((x, y) => {
            var byKind = (x.Diff.IsFormattingEquivalent ? 0 : 1).CompareTo(y.Diff.IsFormattingEquivalent ? 0 : 1);
            if (byKind != 0) return byKind;
            var byDistance = x.Diff.Distance.CompareTo(y.Diff.Distance);
            return byDistance != 0 ? byDistance : string.Compare(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
        });

        var best = scored[0];
        if (best.Diff.IsFormattingEquivalent) {
            return (best.Name, $"No exact Catalogue of Life match. CoL has '{best.Name}', which {best.Diff.Description} — likely the same name.");
        }

        var alternatives = scored.Take(3).Select(s => $"'{s.Name}'").ToList();
        var list = alternatives.Count == 1 ? alternatives[0] : string.Join(", ", alternatives);
        return (best.Name,
            $"No exact Catalogue of Life match. Closest CoL {(alternatives.Count == 1 ? "name is" : "names are")} {list} ({best.Diff.Description}); may be a spelling variant or a different taxon.");
    }

    private static IEnumerable<(string Rank, string Iucn, string Col)> ClassificationDifferences(
        IucnTaxonomyRow row, ColTaxonRecord primary, ColTaxonRecord? accepted, ColTaxonRepository colRepo, List<TaxonLadder> buffer, CancellationToken ct) {
        buffer.Clear();
        buffer.Add(TaxonLadderFactory.FromIucn(row));
        buffer.Add(TaxonLadderFactory.FromColClassification("COL-match", primary));
        var lineage = colRepo.GetParentChain(primary, ct);
        if (lineage.Count > 0) {
            buffer.Add(TaxonLadderFactory.FromColLineage("COL-match(lineage)", lineage));
        }
        if (accepted is not null && !string.Equals(accepted.Id, primary.Id, StringComparison.Ordinal)) {
            buffer.Add(TaxonLadderFactory.FromColClassification("COL-accepted", accepted));
            var acceptedLineage = colRepo.GetParentChain(accepted, ct);
            if (acceptedLineage.Count > 0) {
                buffer.Add(TaxonLadderFactory.FromColLineage("COL-accepted(lineage)", acceptedLineage));
            }
        }

        var ladders = buffer.ToArray();
        var labels = ladders.Select(l => l.SourceLabel).ToList();
        var baseline = labels.FirstOrDefault(l => l.Equals("COL-match(lineage)", StringComparison.OrdinalIgnoreCase))
            ?? labels.FirstOrDefault(l => l.Equals("COL-accepted(lineage)", StringComparison.OrdinalIgnoreCase))
            ?? labels.FirstOrDefault(l => l.StartsWith("COL", StringComparison.OrdinalIgnoreCase))
            ?? "IUCN";

        var result = TaxonLadderAlignment.Align(ladders);
        foreach (var r in result.Rows) {
            var bval = r.Values.TryGetValue(baseline, out var b) ? b : null;
            var ival = r.Values.TryGetValue("IUCN", out var i) ? i : null;
            if (!string.IsNullOrWhiteSpace(bval) && !string.IsNullOrWhiteSpace(ival) &&
                !string.Equals(bval!.Trim(), ival!.Trim(), StringComparison.OrdinalIgnoreCase)) {
                yield return (r.Rank, ival!.Trim(), bval!.Trim());
            }
        }
    }

    // --- replicated matching helpers (private in IucnColCrosscheckCommand) ---

    private static (ColTaxonRecord? Primary, ColTaxonRecord? Accepted, IReadOnlyList<ColTaxonRecord> Candidates, string Method)
        FindBestMatch(IucnTaxonomyRow row, ColTaxonRepository repo, CancellationToken ct) {
        var candidates = new List<ColTaxonRecord>();
        var methods = new List<string>();
        var primaryName = !string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy) ? row.ScientificNameTaxonomy : row.ScientificNameAssessments;

        if (!string.IsNullOrWhiteSpace(primaryName)) {
            var byName = repo.FindByScientificName(primaryName, ct);
            if (byName.Count > 0) {
                candidates.AddRange(byName);
                methods.Add("scientificName");
            }
        }
        if (candidates.Count == 0 && !string.IsNullOrWhiteSpace(row.GenusName) && !string.IsNullOrWhiteSpace(row.SpeciesName)) {
            var byComp = repo.FindByComponents(row.GenusName, row.SpeciesName, row.InfraName, ct);
            if (byComp.Count > 0) {
                candidates.AddRange(byComp);
                methods.Add("components");
            }
        }

        var unique = candidates.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First()).ToList();
        var primary = ChoosePrimary(unique, !string.IsNullOrWhiteSpace(row.InfraName));
        ColTaxonRecord? accepted = null;
        if (primary is not null && !string.IsNullOrWhiteSpace(primary.AcceptedNameUsageId)) {
            accepted = repo.GetById(primary.AcceptedNameUsageId, ct);
        }
        return (primary, accepted, unique, methods.Count == 0 ? "none" : string.Join(",", methods.Distinct()));
    }

    private static ColTaxonRecord? ChoosePrimary(IReadOnlyList<ColTaxonRecord> candidates, bool expectInfra) {
        if (candidates.Count == 0) {
            return null;
        }
        var accepted = candidates.Where(c => Looks(c.Status, "accepted")).ToList();
        if (accepted.Count > 0) {
            var p = PickByInfra(accepted, expectInfra);
            if (p is not null) {
                return p;
            }
        }
        var synonym = candidates.Where(c => Looks(c.Status, "synonym")).ToList();
        if (synonym.Count > 0) {
            var p = PickByInfra(synonym, expectInfra);
            if (p is not null) {
                return p;
            }
        }
        return PickByInfra(candidates, expectInfra) ?? candidates[0];
    }

    private static ColTaxonRecord? PickByInfra(IEnumerable<ColTaxonRecord> candidates, bool expectInfra) {
        var list = candidates.ToList();
        if (list.Count == 0) {
            return null;
        }
        return expectInfra
            ? list.FirstOrDefault(c => LooksInfraRank(c.Rank)) ?? list[0]
            : list.FirstOrDefault(c => !LooksInfraRank(c.Rank)) ?? list[0];
    }

    private static bool Looks(string? status, string token) =>
        !string.IsNullOrWhiteSpace(status) && status.Trim().ToLowerInvariant().Contains(token, StringComparison.Ordinal);

    private static bool LooksInfraRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return false;
        }
        var n = rank.Trim().ToLowerInvariant();
        return n.Contains("subspecies", StringComparison.Ordinal) || n.Contains("variety", StringComparison.Ordinal) || n.Contains("form", StringComparison.Ordinal);
    }

    private static string? GetIucnAuthority(IucnTaxonomyRow row) =>
        !string.IsNullOrWhiteSpace(row.InfraName) && !string.IsNullOrWhiteSpace(row.InfraAuthority) ? row.InfraAuthority!.Trim() : row.Authority?.Trim();

    private static string Label(string issueType) => issueType switch {
        "missing-from-col" => "No exact CoL match",
        "synonym-in-col" => "Synonym in CoL",
        "classification-difference" => "Classification difference",
        "authority-difference" => "Authority difference",
        _ => issueType,
    };

    private static string Capitalise(string? s) =>
        string.IsNullOrEmpty(s) ? "Rank" : char.ToUpperInvariant(s[0]) + s[1..];
}
