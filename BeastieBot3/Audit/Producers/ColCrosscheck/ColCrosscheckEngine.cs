using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using BeastieBot3.Audit;
using BeastieBot3.Audit.Model;
using BeastieBot3.Col;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn;
using BeastieBot3.Taxonomy;

// The one pass over the Red List that feeds every CoL crosscheck report. It matches each IUCN
// assessment to its best Catalogue of Life name usage, and separately compares the higher-rank
// names (genus, family, order, class) IUCN uses against the same names in CoL. Findings are sorted
// into per-report buckets on ColCrosscheckData; ColCrosscheckProducer wraps each bucket in an
// AuditReport. Running the matching once here (rather than once per report) keeps the crosscheck,
// the slowest part of the audit, to a single scan.
//
// CoL's NameUsage table is denormalised: a synonym's parentID points at its accepted taxon, and
// every accepted name carries its own higher-rank ancestors (kingdom..family) inline. There is no
// acceptedNameUsageID column in this schema, so synonymy is resolved through parentID and the
// classification comparison reads the inline ancestor columns instead of walking the tree.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal sealed class ColCrosscheckEngine {
    private readonly ColTaxonRepository _col;
    private readonly IucnSynonymIndex? _iucnSynonyms;

    public ColCrosscheckEngine(ColTaxonRepository col, IucnSynonymIndex? iucnSynonyms = null) {
        _col = col ?? throw new ArgumentNullException(nameof(col));
        _iucnSynonyms = iucnSynonyms;
    }

    public ColCrosscheckData Run(IReadOnlyList<IucnTaxonomyRow> rows, CancellationToken ct) {
        var data = new ColCrosscheckData();
        var higher = new Dictionary<string, HigherTaxon>(StringComparer.Ordinal);

        foreach (var row in rows) {
            ct.ThrowIfCancellationRequested();
            ClassifyAssessedTaxon(row, data);
            AccumulateHigherTaxa(row, higher);
        }
        data.AssessedCompared = rows.Count;

        foreach (var taxon in higher.Values) {
            ct.ThrowIfCancellationRequested();
            ClassifyHigherTaxon(taxon, data);
        }
        data.HigherTaxaCompared = higher.Count;

        return data;
    }

    // --- assessed taxa (species, subspecies, varieties) --------------------------------------

    private void ClassifyAssessedTaxon(IucnTaxonomyRow row, ColCrosscheckData data) {
        var (rank, isFull) = AuditMapping.Rank(row.InfraType, row.SubpopulationName);
        var name = AuditMapping.Decode(!string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy)
            ? row.ScientificNameTaxonomy
            : row.ScientificNameAssessments);
        var severity = isFull ? 3 : (rank == "subspecies" ? 2 : 1);

        var primary = FindBestMatch(row);
        if (primary is null) {
            var near = SuggestNearMatch(row, name);
            if (near.Best is null) {
                data.NotFound.Add(SpeciesFinding(ColCrosscheckProducer.NotFoundId, row, rank, isFull, name,
                    "missing-from-col", "scientificName", name, null, colId: null, severity,
                    "No Catalogue of Life match for this name, and no close candidate from fuzzy search."));
            } else {
                var colName = AuditMapping.Decode(near.Best.ScientificName);
                var detail = near.Diff.IsFormattingEquivalent
                    ? $"No exact Catalogue of Life match. CoL has '{colName}', which {near.Diff.Description}; likely the same name."
                    : $"No exact Catalogue of Life match. Closest CoL name is '{colName}' ({near.Diff.Description}); may be a spelling variant or a different taxon.";
                data.CloseMatch.Add(SpeciesFinding(ColCrosscheckProducer.CloseMatchId, row, rank, isFull, name,
                    "close-col-match", "scientificName", name, colName, near.Best.Id, severity, detail));
            }
            return;
        }

        if (IsSynonymStatus(primary.Status)) {
            var accepted = string.IsNullOrWhiteSpace(primary.ParentId) ? null : _col.GetById(primary.ParentId, CancellationToken.None);
            var acceptedName = AuditMapping.Decode(accepted?.ScientificName);
            var linkId = accepted?.Id ?? primary.Id;
            var match = _iucnSynonyms?.Lookup(acceptedName, row.TaxonId) ?? IucnSynonymMatch.Unknown;
            var detail = acceptedName is null
                ? "Catalogue of Life treats this name as a synonym."
                : $"Catalogue of Life treats this name as a synonym of {acceptedName}.";
            if (match == IucnSynonymMatch.SameTaxon) {
                detail += " IUCN already lists that name as a synonym of this same taxon, so the two catalogues disagree on which name is accepted.";
            }
            var finding = SpeciesFinding(ColCrosscheckProducer.SynonymId, row, rank, isFull, name,
                "synonym-in-col", "scientificName", name, acceptedName, linkId, severity, detail);
            SetExtra(finding, "colAuthority", AuditMapping.Decode(accepted?.Authorship));
            SetExtra(finding, "iucnSynonym", IucnSynonymLabel(match));
            data.Synonym.Add(finding);
            return;
        }

        // A match that is neither accepted nor a synonym (a "misapplied" usage that happens to be the
        // only CoL record for this name) is not a clean accepted match, so it gets no authority
        // comparison and no finding rather than being reported as validly matched.
        if (!IsAcceptedStatus(primary.Status)) {
            return;
        }

        // Accepted (or provisionally accepted) match: the only per-taxon divergence left to check is
        // the naming authority. Placement is compared once per higher taxon in the second pass.
        var iucnAuthority = AuditMapping.Decode(GetIucnAuthority(row));
        var colAuthority = AuditMapping.Decode(primary.Authorship);
        if (!string.IsNullOrWhiteSpace(iucnAuthority) && !string.IsNullOrWhiteSpace(colAuthority)) {
            // Author citations are compared with spacing removed, so "A.J. Wagner" and
            // "A. J. Wagner" are treated as equal (spacing is covered elsewhere); what remains is a
            // genuine typo, an encoding difference, or a year difference.
            var compareIucn = StripSpaces(AuthorityNormalizer.Normalize(iucnAuthority));
            var compareCol = StripSpaces(AuthorityNormalizer.Normalize(colAuthority));
            if (!string.Equals(compareIucn, compareCol, StringComparison.OrdinalIgnoreCase)) {
                var diff = ScientificNameDifference.Classify(compareIucn, compareCol);
                if (ColDifference.Classify(diff) == ColDifference.Bucket.Typo) {
                    data.Authority.Add(SpeciesFinding(ColCrosscheckProducer.AuthorityId, row, rank, isFull, name,
                        "authority-difference", "authority", iucnAuthority, colAuthority, primary.Id, 1,
                        $"Naming authority differs and {diff.Description}: IUCN '{iucnAuthority}' versus CoL '{colAuthority}'."));
                }
            }
        }
    }

    // --- higher taxa (genus, family, order, class) ------------------------------------------

    private void AccumulateHigherTaxa(IucnTaxonomyRow row, Dictionary<string, HigherTaxon> map) {
        Accumulate(map, row, "genus", row.GenusName, "family", row.FamilyName);
        Accumulate(map, row, "family", row.FamilyName, "order", row.OrderName);
        Accumulate(map, row, "order", row.OrderName, "class", row.ClassName);
        Accumulate(map, row, "class", row.ClassName, "phylum", row.PhylumName);
    }

    private static void Accumulate(Dictionary<string, HigherTaxon> map, IucnTaxonomyRow row,
        string rank, string? name, string parentRank, string? parentName) {
        if (string.IsNullOrWhiteSpace(name)) {
            return;
        }
        var key = $"{rank}|{(row.KingdomName ?? string.Empty).Trim().ToLowerInvariant()}|{name.Trim().ToLowerInvariant()}";
        if (!map.TryGetValue(key, out var taxon)) {
            taxon = new HigherTaxon {
                Rank = rank,
                Name = name.Trim(),
                Kingdom = row.KingdomName,
                Phylum = row.PhylumName,
                ParentRank = parentRank,
                ParentName = string.IsNullOrWhiteSpace(parentName) ? null : parentName.Trim(),
                Sample = row,
            };
            map[key] = taxon;
        }
        taxon.SpeciesCount++;
    }

    private void ClassifyHigherTaxon(HigherTaxon taxon, ColCrosscheckData data) {
        // IUCN records higher-rank names in upper case (MURICIDAE) while CoL uses the Linnaean
        // capitalisation (Muricidae). FindByScientificName is an exact, index-backed lookup, so the
        // name is folded to the CoL convention here rather than paying for a NOCASE scan per taxon.
        var usages = _col.FindByScientificName(ToColCase(taxon.Name), CancellationToken.None)
            .Where(u => RankMatches(u.Rank, taxon.Rank))
            .ToList();
        if (usages.Count == 0) {
            return;
        }

        var accepted = usages.Where(u => IsAcceptedStatus(u.Status)).ToList();
        if (accepted.Count > 0) {
            var best = PickBestHigher(accepted, taxon);
            CompareHigherPlacement(taxon, best, data);
            return;
        }

        // No accepted usage of this name anywhere in CoL, but at least one usage exists: CoL treats
        // the name itself as a synonym. Requiring zero accepted usages avoids flagging names that
        // are accepted in one group and a synonym in another (homonyms).
        var synonym = usages.FirstOrDefault(u => IsSynonymStatus(u.Status));
        if (synonym is null) {
            return;
        }
        var acceptedTarget = string.IsNullOrWhiteSpace(synonym.ParentId) ? null : _col.GetById(synonym.ParentId, CancellationToken.None);
        var targetName = AuditMapping.Decode(acceptedTarget?.ScientificName);
        var linkId = acceptedTarget?.Id ?? synonym.Id;
        var detail = targetName is null
            ? $"Catalogue of Life treats this {taxon.Rank} name as a synonym."
            : $"Catalogue of Life treats this {taxon.Rank} name as a synonym of {targetName}.";
        var finding = HigherFinding(ColCrosscheckProducer.SynonymHigherId, taxon,
            "synonym-in-col", "scientificName", taxon.Name, targetName, linkId, detail);
        SetExtra(finding, "colAuthority", AuditMapping.Decode(acceptedTarget?.Authorship));
        data.SynonymHigher.Add(finding);
    }

    private static void CompareHigherPlacement(HigherTaxon taxon, ColTaxonRecord best, ColCrosscheckData data) {
        // Only compare within the same broad group, so a plant order is never lined up against an
        // animal order of a similar name.
        if (!GateMatches(taxon, best)) {
            return;
        }
        var iucnParent = AuditMapping.Decode(taxon.ParentName);
        var colParent = AuditMapping.Decode(ColParentFor(taxon.Rank, best));
        if (string.IsNullOrWhiteSpace(iucnParent) || string.IsNullOrWhiteSpace(colParent)) {
            return;
        }

        var diff = ScientificNameDifference.Classify(iucnParent!, colParent!);
        switch (ColDifference.Classify(diff)) {
            case ColDifference.Bucket.Typo:
                data.Classification.Add(HigherFinding(ColCrosscheckProducer.ClassificationId, taxon,
                    "classification-difference", taxon.ParentRank, iucnParent, colParent, best.Id,
                    $"{Capitalise(taxon.ParentRank)} placement differs and {diff.Description}: IUCN '{iucnParent}' versus CoL '{colParent}'."));
                break;
            case ColDifference.Bucket.Genuine:
                data.Reorg.Add(HigherFinding(ColCrosscheckProducer.ReorgId, taxon,
                    "classification-difference", taxon.ParentRank, iucnParent, colParent, best.Id,
                    $"IUCN places this {taxon.Rank} in {taxon.ParentRank} '{iucnParent}'; Catalogue of Life places it in '{colParent}'."));
                break;
        }
    }

    // Prefer an accepted usage that lines up with the IUCN taxon: same phylum first, then same
    // kingdom, then a firmly accepted status over a provisional one. This disambiguates homonyms
    // and steps around the occasional stray placement in CoL.
    private static ColTaxonRecord PickBestHigher(IReadOnlyList<ColTaxonRecord> accepted, HigherTaxon taxon) =>
        accepted
            .OrderByDescending(u => EqualsFold(u.Phylum, taxon.Phylum))
            .ThenByDescending(u => EqualsFold(u.Kingdom, taxon.Kingdom))
            .ThenByDescending(u => Looks(u.Status, "accepted") && !Looks(u.Status, "provisional"))
            .First();

    // The comparison happens within one broad group. For genus/family/order the compared parent
    // (family/order/class) sits below phylum, so phylum is an independent gate. For class the
    // compared parent is phylum itself, so the gate is taken one rank higher (kingdom); otherwise
    // the gate and the comparison would be the same value and no class-rank difference could surface.
    private static bool GateMatches(HigherTaxon taxon, ColTaxonRecord best) =>
        string.Equals(taxon.Rank, "class", StringComparison.Ordinal)
            ? EqualsFold(taxon.Kingdom, best.Kingdom)
            : EqualsFold(taxon.Phylum, best.Phylum);

    private static string? ColParentFor(string rank, ColTaxonRecord col) => rank switch {
        "genus" => col.Family,
        "family" => col.Order,
        "order" => col.Class,
        "class" => col.Phylum,
        _ => null,
    };

    // --- CoL matching for assessed taxa (exact name, then components) ------------------------

    private ColTaxonRecord? FindBestMatch(IucnTaxonomyRow row) {
        var candidates = new List<ColTaxonRecord>();
        var primaryName = !string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy)
            ? row.ScientificNameTaxonomy
            : row.ScientificNameAssessments;

        if (!string.IsNullOrWhiteSpace(primaryName)) {
            candidates.AddRange(_col.FindByScientificName(primaryName!, CancellationToken.None));
        }
        if (candidates.Count == 0 && !string.IsNullOrWhiteSpace(row.GenusName) && !string.IsNullOrWhiteSpace(row.SpeciesName)) {
            candidates.AddRange(_col.FindByComponents(row.GenusName, row.SpeciesName, row.InfraName, CancellationToken.None));
        }

        var unique = candidates.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First()).ToList();
        return ChoosePrimary(unique, !string.IsNullOrWhiteSpace(row.InfraName));
    }

    private static ColTaxonRecord? ChoosePrimary(IReadOnlyList<ColTaxonRecord> candidates, bool expectInfra) {
        if (candidates.Count == 0) {
            return null;
        }
        var accepted = candidates.Where(c => IsAcceptedStatus(c.Status)).ToList();
        if (accepted.Count > 0) {
            return PickByInfra(accepted, expectInfra);
        }
        var synonym = candidates.Where(c => IsSynonymStatus(c.Status)).ToList();
        if (synonym.Count > 0) {
            return PickByInfra(synonym, expectInfra);
        }
        return PickByInfra(candidates, expectInfra);
    }

    private static ColTaxonRecord PickByInfra(IReadOnlyList<ColTaxonRecord> candidates, bool expectInfra) =>
        expectInfra
            ? candidates.FirstOrDefault(c => LooksInfraRank(c.Rank)) ?? candidates[0]
            : candidates.FirstOrDefault(c => !LooksInfraRank(c.Rank)) ?? candidates[0];

    // When no exact match exists, find the nearest CoL name in the same genus or sharing the
    // epithet, ranked by how it differs (formatting-equivalent first, then smallest edit distance).
    private NearMatch SuggestNearMatch(IucnTaxonomyRow row, string? iucnName) {
        if (string.IsNullOrWhiteSpace(iucnName)) {
            return NearMatch.None;
        }

        var pool = new List<ColTaxonRecord>();
        if (!string.IsNullOrWhiteSpace(row.GenusName)) {
            pool.AddRange(_col.FindByGenericName(row.GenusName!, CancellationToken.None));
        }
        if (!string.IsNullOrWhiteSpace(row.SpeciesName)) {
            pool.AddRange(_col.FindBySpecificEpithet(row.SpeciesName!, CancellationToken.None));
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var scored = new List<(ColTaxonRecord Record, ScientificNameDifference.Result Diff)>();
        foreach (var candidate in pool.GroupBy(c => c.Id, StringComparer.Ordinal).Select(g => g.First())) {
            var colName = AuditMapping.Decode(candidate.ScientificName);
            if (string.IsNullOrWhiteSpace(colName) || !seen.Add(colName!)) {
                continue;
            }
            var diff = ScientificNameDifference.Classify(iucnName!, colName!);
            if (diff.Kind is ScientificNameDifference.Kind.Exact or ScientificNameDifference.Kind.Unrelated) {
                continue;
            }
            scored.Add((candidate, diff));
        }

        if (scored.Count == 0) {
            return NearMatch.None;
        }

        scored.Sort((x, y) => {
            var byKind = (x.Diff.IsFormattingEquivalent ? 0 : 1).CompareTo(y.Diff.IsFormattingEquivalent ? 0 : 1);
            if (byKind != 0) {
                return byKind;
            }
            var byDistance = x.Diff.Distance.CompareTo(y.Diff.Distance);
            return byDistance != 0
                ? byDistance
                : string.Compare(x.Record.ScientificName, y.Record.ScientificName, StringComparison.OrdinalIgnoreCase);
        });

        return new NearMatch(scored[0].Record, scored[0].Diff);
    }

    // --- finding factories ------------------------------------------------------------------

    private static AuditFinding SpeciesFinding(string reportId, IucnTaxonomyRow row, string rank, bool isFull,
        string? name, string issueType, string? field, string? current, string? suggested, string? colId,
        int severity, string detail) {
        var finding = new AuditFinding {
            ReportId = reportId,
            Key = $"{row.TaxonId}:{issueType}",
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
        var colUrl = ColUrls.Taxon(colId);
        if (colUrl is not null) {
            finding.Extra["colUrl"] = colUrl;
        }
        return finding;
    }

    private static AuditFinding HigherFinding(string reportId, HigherTaxon taxon, string issueType, string? field,
        string? current, string? suggested, string? colId, string detail) {
        var finding = new AuditFinding {
            ReportId = reportId,
            Key = $"{taxon.Rank}:{taxon.Name}:{issueType}",
            ScientificName = taxon.Name,
            Rank = taxon.Rank,
            Kingdom = taxon.Kingdom,
            Phylum = taxon.Phylum,
            Class = taxon.Sample.ClassName,
            Order = taxon.Sample.OrderName,
            Family = taxon.Sample.FamilyName,
            DataSource = "iucn-csv+col",
            Field = field,
            CurrentValue = current,
            SuggestedValue = suggested,
            IssueType = issueType,
            SeverityTier = taxon.SpeciesCount,
            Detail = detail,
        };
        var colUrl = ColUrls.Taxon(colId);
        if (colUrl is not null) {
            finding.Extra["colUrl"] = colUrl;
        }
        finding.Extra["iucnSpecies"] = taxon.SpeciesCount.ToString();
        finding.Extra["sampleName"] = AuditMapping.Decode(!string.IsNullOrWhiteSpace(taxon.Sample.ScientificNameTaxonomy)
            ? taxon.Sample.ScientificNameTaxonomy
            : taxon.Sample.ScientificNameAssessments);
        var sampleUrl = IucnUrls.Species(taxon.Sample.TaxonId, taxon.Sample.AssessmentId);
        if (sampleUrl is not null) {
            finding.Extra["sampleUrl"] = sampleUrl;
        }
        return finding;
    }

    // --- status / rank helpers --------------------------------------------------------------

    private static bool IsAcceptedStatus(string? status) => Looks(status, "accepted");
    private static bool IsSynonymStatus(string? status) => Looks(status, "synonym");

    private static bool Looks(string? status, string token) =>
        !string.IsNullOrWhiteSpace(status) && status.Trim().ToLowerInvariant().Contains(token, StringComparison.Ordinal);

    private static bool LooksInfraRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return false;
        }
        var n = rank.Trim().ToLowerInvariant();
        return n.Contains("subspecies", StringComparison.Ordinal)
            || n.Contains("variety", StringComparison.Ordinal)
            || n.Contains("form", StringComparison.Ordinal);
    }

    private static bool RankMatches(string? colRank, string wanted) =>
        !string.IsNullOrWhiteSpace(colRank) && string.Equals(colRank.Trim(), wanted, StringComparison.OrdinalIgnoreCase);

    private static bool EqualsFold(string? a, string? b) =>
        !string.IsNullOrWhiteSpace(a) && !string.IsNullOrWhiteSpace(b) && string.Equals(a.Trim(), b.Trim(), StringComparison.OrdinalIgnoreCase);

    // Fold a higher-rank name to the Linnaean capitalisation CoL stores (first letter upper, rest
    // lower), so an exact index lookup matches IUCN's upper-case form.
    private static string ToColCase(string name) {
        var trimmed = name.Trim();
        return trimmed.Length == 0 ? trimmed : char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    private static string StripSpaces(string value) =>
        new(value.Where(c => !char.IsWhiteSpace(c)).ToArray());

    private static void SetExtra(AuditFinding finding, string key, string? value) {
        if (!string.IsNullOrWhiteSpace(value)) {
            finding.Extra[key] = value!;
        }
    }

    // Column text for whether the CoL accepted name is already an IUCN synonym. Unknown (no API
    // cache) leaves the cell blank rather than asserting "no".
    private static string? IucnSynonymLabel(IucnSynonymMatch match) => match switch {
        IucnSynonymMatch.SameTaxon => "of same taxon",
        IucnSynonymMatch.OtherTaxon => "of other taxon",
        IucnSynonymMatch.None => "no",
        _ => null,
    };

    private static string? GetIucnAuthority(IucnTaxonomyRow row) =>
        !string.IsNullOrWhiteSpace(row.InfraName) && !string.IsNullOrWhiteSpace(row.InfraAuthority)
            ? row.InfraAuthority!.Trim()
            : row.Authority?.Trim();

    private static string Capitalise(string? s) =>
        string.IsNullOrEmpty(s) ? "Rank" : char.ToUpperInvariant(s[0]) + s[1..];

    private readonly record struct NearMatch(ColTaxonRecord? Best, ScientificNameDifference.Result Diff) {
        public static NearMatch None => new(null, default);
    }

    private sealed class HigherTaxon {
        public required string Rank { get; init; }
        public required string Name { get; init; }
        public string? Kingdom { get; init; }
        public string? Phylum { get; init; }
        public required string ParentRank { get; init; }
        public string? ParentName { get; init; }
        public required IucnTaxonomyRow Sample { get; init; }
        public int SpeciesCount { get; set; }
    }
}

// Findings sorted into per-report buckets by ColCrosscheckEngine; ColCrosscheckProducer turns each
// non-empty bucket into an AuditReport page.
internal sealed class ColCrosscheckData {
    public List<AuditFinding> NotFound { get; } = new();
    public List<AuditFinding> CloseMatch { get; } = new();
    public List<AuditFinding> Synonym { get; } = new();
    public List<AuditFinding> SynonymHigher { get; } = new();
    public List<AuditFinding> Classification { get; } = new();
    public List<AuditFinding> Reorg { get; } = new();
    public List<AuditFinding> Authority { get; } = new();
    public int AssessedCompared { get; set; }
    public int HigherTaxaCompared { get; set; }
}
