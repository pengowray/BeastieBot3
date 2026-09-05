using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    private readonly OtherSourceIndex? _otherSources;

    public ColCrosscheckEngine(ColTaxonRepository col, IucnSynonymIndex? iucnSynonyms = null,
        OtherSourceIndex? otherSources = null) {
        _col = col ?? throw new ArgumentNullException(nameof(col));
        _iucnSynonyms = iucnSynonyms;
        _otherSources = otherSources;
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

        AddOtherSources(data, ct);
        AddNameInUse(data.Synonym, f => f.SuggestedValue, ct);
        AddNameInUse(data.SynonymLead, f => f.Get("colAcceptedForSynonym") ?? f.Get("iucnSynonymInCol"), ct);
        return data;
    }

    // Runs over the not-found bucket alone, after the main pass. That bucket is a few hundred rows
    // out of nearly two hundred thousand, so a handful of indexed queries each costs nothing. A row
    // where Wikidata or Wikipedia supplies a name CoL does hold moves to its own bucket (ViaWiki):
    // that row has a lead to check, which is a different job from the plain not-found list.
    private void AddOtherSources(ColCrosscheckData data, CancellationToken ct) {
        if (_otherSources is null || data.NotFound.Count == 0) {
            return;
        }
        data.OtherSourcesChecked = true;

        var remaining = new List<AuditFinding>(data.NotFound.Count);
        foreach (var finding in data.NotFound) {
            ct.ThrowIfCancellationRequested();
            if (finding.TaxonId is not { } taxonId) {
                remaining.Add(finding);
                continue;
            }
            var hit = _otherSources.Lookup(taxonId, finding.ScientificName, ct);
            if (hit.WikidataId is not null) {
                SetExtra(finding, "wikidataId", hit.WikidataId);
                SetExtra(finding, "wikidataUrl", OtherSourceIndex.WikidataUrl(hit.WikidataId));
            }
            if (hit.WikipediaTitle is not null) {
                SetExtra(finding, "wikipediaTitle", hit.WikipediaTitle);
                SetExtra(finding, "wikipediaUrl", OtherSourceIndex.WikipediaUrl(hit.WikipediaTitle));
            }

            // The one that closes the gap: a name from those sources that CoL does record. An
            // accepted name is worth more than a synonym, so the whole list is tried before
            // settling for the first hit.
            ColTaxonRecord? best = null;
            string? bestName = null;
            foreach (var name in hit.OtherNames) {
                foreach (var usage in _col.FindByScientificName(name, ct)) {
                    if (best is null || (!IsAcceptedStatus(best.Status) && IsAcceptedStatus(usage.Status))) {
                        best = usage;
                        bestName = name;
                    }
                }
                if (best is not null && IsAcceptedStatus(best.Status)) {
                    break;
                }
            }
            if (best is null || bestName is null) {
                if (hit.WikidataId is not null) {
                    data.OtherSourcesWithWikidata++;
                }
                if (hit.WikipediaTitle is not null) {
                    data.OtherSourcesWithWikipedia++;
                }
                remaining.Add(finding);
                continue;
            }

            finding.ReportId = ColCrosscheckProducer.ViaWikiId;
            SetExtra(finding, "otherName", bestName);
            SetExtra(finding, "otherNameColStatus", ColStatusLabel(best.Status));
            if (IsSynonymStatus(best.Status) && !string.IsNullOrWhiteSpace(best.ParentId)) {
                SetExtra(finding, "otherNameSynonymOf", AuditMapping.Decode(_col.GetById(best.ParentId!, ct)?.ScientificName));
            }
            SetExtra(finding, "colUrl", ColUrls.Taxon(best.Id));
            finding.Notes.Add(IsAcceptedStatus(best.Status)
                ? $"{bestName}, recorded for this taxon on Wikidata or Wikipedia, is an accepted name in the Catalogue of Life."
                : $"{bestName}, recorded for this taxon on Wikidata or Wikipedia, is a synonym in the Catalogue of Life.");
            data.ViaWiki.Add(finding);
        }
        data.NotFound.Clear();
        data.NotFound.AddRange(remaining);
    }

    // For the pages where IUCN and CoL use different names for one taxon: which name Wikidata and
    // English Wikipedia use. Neither source is an authority, but each is a third party that chose
    // one of the two names, so "CoL name" on a row is a reason to look at it first.
    private void AddNameInUse(List<AuditFinding> findings, Func<AuditFinding, string?> colName, CancellationToken ct) {
        if (_otherSources is null) {
            return;
        }
        foreach (var finding in findings) {
            ct.ThrowIfCancellationRequested();
            if (finding.TaxonId is not { } taxonId) {
                continue;
            }
            var hit = _otherSources.Lookup(taxonId, finding.ScientificName, ct);
            SetExtra(finding, "nameInUse", hit.NameInUse(finding.ScientificName, colName(finding)));
        }
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
                // The IUCN name is in no CoL usage and no near spelling exists, but the taxon may
                // still be in CoL under one of the names IUCN itself files as a synonym. That is a
                // different lookup from the one the close-match path does, which only asks about the
                // near name, and it finds the cases where the two catalogues use different genera.
                var viaSynonym = FindViaIucnSynonym(row);
                if (viaSynonym.Accepted is not null) {
                    data.AcceptedDiffers.Add(AcceptedDiffersFinding(row, rank, isFull, name,
                        AuditMapping.Decode(viaSynonym.Accepted.ScientificName),
                        AuditMapping.Decode(viaSynonym.Accepted.Authorship),
                        viaSynonym.Accepted.Id, severity, Origin.IucnSynonym));
                    return;
                }

                if (viaSynonym.SynonymUsage is not null) {
                    var leadDetail = viaSynonym.SynonymAcceptedName is null
                        ? $"IUCN also lists {viaSynonym.Name} as a synonym of this taxon; the Catalogue of Life has {viaSynonym.Name} as a synonym but does not link it to an accepted name."
                        : $"IUCN also lists {viaSynonym.Name} as a synonym of this taxon; the Catalogue of Life holds {viaSynonym.Name} as a synonym of {viaSynonym.SynonymAcceptedName}.";
                    var lead = SpeciesFinding(ColCrosscheckProducer.SynonymLeadId, row, rank, isFull, name,
                        "col-under-other-name", "scientificName", name, viaSynonym.SynonymAcceptedName,
                        viaSynonym.SynonymUsage.Id, severity, leadDetail);
                    SetExtra(lead, "iucnSynonymInCol", viaSynonym.Name);
                    SetExtra(lead, "colAcceptedForSynonym", viaSynonym.SynonymAcceptedName);
                    data.SynonymLead.Add(lead);
                    return;
                }

                data.NotFound.Add(SpeciesFinding(ColCrosscheckProducer.NotFoundId, row, rank, isFull, name,
                    "missing-from-col", "scientificName", name, null, colId: null, severity,
                    "No Catalogue of Life match for this name, no close candidate from fuzzy search, and no other IUCN-listed name for the taxon is in CoL either."));
            } else {
                var colName = AuditMapping.Decode(near.Best.ScientificName);
                // Two follow-up checks on the suggested name, one lookup each. Both change what the
                // row means: a CoL synonym points at a different accepted name, and a name IUCN
                // already lists as a synonym of this taxon is a recorded pair, not a spelling slip.
                var colIsSynonym = IsSynonymStatus(near.Best.Status);
                var colAccepted = colIsSynonym && !string.IsNullOrWhiteSpace(near.Best.ParentId)
                    ? _col.GetById(near.Best.ParentId!, CancellationToken.None)
                    : null;
                var colAcceptedName = AuditMapping.Decode(colAccepted?.ScientificName);
                var iucnMatch = _iucnSynonyms?.Lookup(colName, row.TaxonId) ?? IucnSynonymMatch.Unknown;

                var detail = near.Diff.IsFormattingEquivalent
                    ? $"Absent from the Catalogue of Life, as an accepted name and as a synonym. CoL has '{colName}', which {near.Diff.Description}; likely the same name."
                    : $"Absent from the Catalogue of Life, as an accepted name and as a synonym. Closest CoL name is '{colName}' ({near.Diff.Description}); may be a spelling variant or a different taxon.";
                if (colIsSynonym) {
                    detail += colAcceptedName is null
                        ? $" CoL records '{colName}' as a synonym."
                        : $" CoL records '{colName}' as a synonym of {colAcceptedName}.";
                }
                if (iucnMatch == IucnSynonymMatch.SameTaxon) {
                    // IUCN's own synonym list already links the two names, so the pair can be joined
                    // and "align the spelling" is the wrong suggestion for it. Reported separately.
                    data.AcceptedDiffers.Add(AcceptedDiffersFinding(row, rank, isFull, name, colName,
                        AuditMapping.Decode(near.Best.Authorship), near.Best.Id, severity, Origin.CloseSpelling));
                    return;
                }
                if (iucnMatch == IucnSynonymMatch.OtherTaxon) {
                    detail += $" IUCN lists '{colName}' as a synonym of a different taxon.";
                }

                var closeFinding = SpeciesFinding(ColCrosscheckProducer.CloseMatchId, row, rank, isFull, name,
                    "close-col-match", "scientificName", name, colName, near.Best.Id, severity, detail);
                SetExtra(closeFinding, "colStatus", ColStatusLabel(near.Best.Status));
                SetExtra(closeFinding, "colSynonymOf", colAcceptedName);
                SetExtra(closeFinding, "colYear", ColYear(near.Best));
                SetExtra(closeFinding, "iucnSynonym", IucnSynonymLabel(iucnMatch));
                data.CloseMatch.Add(closeFinding);
            }
            return;
        }

        if (IsSynonymStatus(primary.Status)) {
            var accepted = string.IsNullOrWhiteSpace(primary.ParentId) ? null : _col.GetById(primary.ParentId, CancellationToken.None);
            var acceptedName = AuditMapping.Decode(accepted?.ScientificName);
            var linkId = accepted?.Id ?? primary.Id;
            var match = _iucnSynonyms?.Lookup(acceptedName, row.TaxonId) ?? IucnSynonymMatch.Unknown;
            if (match == IucnSynonymMatch.SameTaxon) {
                // Each catalogue records the other's name, so the pair is joinable through IUCN's
                // synonym list. That is a different observation from a name CoL has moved into
                // synonymy while IUCN carries no pointer to the replacement, so it gets its own page.
                data.AcceptedDiffers.Add(AcceptedDiffersFinding(row, rank, isFull, name, acceptedName,
                    AuditMapping.Decode(accepted?.Authorship), linkId, severity, Origin.Mutual));
                return;
            }
            var detail = acceptedName is null
                ? "Catalogue of Life treats this name as a synonym."
                : $"Catalogue of Life treats this name as a synonym of {acceptedName}.";
            var finding = SpeciesFinding(ColCrosscheckProducer.SynonymId, row, rank, isFull, name,
                "synonym-in-col", "scientificName", name, acceptedName, linkId, severity, detail);
            SetExtra(finding, "colAuthority", AuditMapping.Decode(accepted?.Authorship));
            SetExtra(finding, "colYear", ColYear(accepted));
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
            // Compare only the author-name letters: strip commas, brackets, digits (years), and
            // spacing. A difference that is purely one of those is dropped, so what remains is a real
            // difference in the author name itself (a spelling, diacritic, or encoding slip).
            var iucnLetters = AuthorLetters(iucnAuthority);
            var colLetters = AuthorLetters(colAuthority);
            if (iucnLetters.Length > 0 && colLetters.Length > 0 &&
                !string.Equals(iucnLetters, colLetters, StringComparison.OrdinalIgnoreCase)) {
                var diff = ScientificNameDifference.Classify(iucnLetters, colLetters);
                if (ColDifference.Classify(diff) == ColDifference.Bucket.Typo) {
                    var authFinding = SpeciesFinding(ColCrosscheckProducer.AuthorityId, row, rank, isFull, name,
                        "authority-difference", "authority", iucnAuthority, colAuthority, primary.Id, 1,
                        $"Naming authority differs in the author name ({diff.Description}): IUCN '{iucnAuthority}' versus CoL '{colAuthority}'.");
                    SetExtra(authFinding, "colYear", ColYear(primary));
                    data.Authority.Add(authFinding);
                }
            }
        }
    }

    // How a row reached the accepted-name-differs report, which is only used to word its detail line.
    private enum Origin {
        Mutual,        // CoL records the IUCN name as a synonym of its accepted name
        CloseSpelling, // CoL has no entry for the IUCN name; the pair was found by spelling
        IucnSynonym,   // CoL has no entry for the IUCN name; the pair came from IUCN's synonym list
    }

    // Looks the taxon's own IUCN synonyms up in CoL. An accepted hit means CoL holds the taxon under
    // a name IUCN files as a synonym, which is a name disagreement rather than a missing taxon. A
    // synonym-only hit is a lead, not a match: the chain sometimes lands on an unrelated name, so it
    // is reported as what it is and left on the not-found page.
    private ViaSynonym FindViaIucnSynonym(IucnTaxonomyRow row) {
        if (_iucnSynonyms is null) {
            return default;
        }
        ColTaxonRecord? firstSynonym = null;
        string? firstSynonymName = null;
        foreach (var candidate in _iucnSynonyms.SynonymsOf(row.TaxonId)) {
            var usages = _col.FindByScientificName(candidate, CancellationToken.None);
            foreach (var usage in usages) {
                if (IsAcceptedStatus(usage.Status)) {
                    return new ViaSynonym(candidate, usage, null, null);
                }
                if (firstSynonym is null && IsSynonymStatus(usage.Status)) {
                    firstSynonym = usage;
                    firstSynonymName = candidate;
                }
            }
        }
        if (firstSynonym is null) {
            return default;
        }
        var parent = string.IsNullOrWhiteSpace(firstSynonym.ParentId)
            ? null
            : _col.GetById(firstSynonym.ParentId!, CancellationToken.None);
        return new ViaSynonym(firstSynonymName, null, firstSynonym, AuditMapping.Decode(parent?.ScientificName));
    }

    private readonly record struct ViaSynonym(string? Name, ColTaxonRecord? Accepted,
        ColTaxonRecord? SynonymUsage, string? SynonymAcceptedName);

    // One row of the accepted-name-differs report. The three origins differ only in how the pair was
    // found, which the detail line states. Rows whose two authorities credit the same author but a
    // different year are lifted above the rest by their severity tier.
    private AuditFinding AcceptedDiffersFinding(IucnTaxonomyRow row, string rank, bool isFull, string? name,
        string? colName, string? colAuthority, string? colId, int severity, Origin origin) {
        var iucnAuthority = AuditMapping.Decode(GetIucnAuthority(row));
        var clash = AuthorityYearClash(iucnAuthority, colAuthority);

        var detail = origin switch {
            Origin.Mutual => $"The Catalogue of Life treats {name} as a synonym of {colName}; IUCN treats {colName} as a synonym of {name}.",
            Origin.CloseSpelling => $"IUCN treats {colName} as a synonym of {name}; the Catalogue of Life has no entry for the spelling {name}.",
            _ => $"IUCN treats {colName} as a synonym of {name}; the Catalogue of Life has no entry for {name} under any spelling.",
        };
        if (clash is not null) {
            detail += $" Both credit {clash.Value.Author}, but the years differ: {clash.Value.IucnYear} in IUCN, {clash.Value.ColYear} in the Catalogue of Life.";
        }

        var finding = SpeciesFinding(ColCrosscheckProducer.AcceptedDiffersId, row, rank, isFull, name,
            "accepted-name-differs", "scientificName", name, colName, colId,
            clash is null ? severity : severity + 10, detail);
        SetExtra(finding, "iucnAuthority", iucnAuthority);
        SetExtra(finding, "colAuthority", colAuthority);
        if (clash is not null) {
            SetExtra(finding, "authorityYears", $"IUCN {clash.Value.IucnYear} vs CoL {clash.Value.ColYear}");
        }
        return finding;
    }

    // Set only where the two catalogues credit the same author and give different years. A different
    // author means a different name, which is not a date to check, so those rows are left blank.
    private static (string Author, string IucnYear, string ColYear)? AuthorityYearClash(string? iucnAuthority, string? colAuthority) {
        if (string.IsNullOrWhiteSpace(iucnAuthority) || string.IsNullOrWhiteSpace(colAuthority)) {
            return null;
        }
        var iucnYear = ExtractYear(iucnAuthority);
        var colYear = ExtractYear(colAuthority);
        if (iucnYear is null || colYear is null || string.Equals(iucnYear, colYear, StringComparison.Ordinal)) {
            return null;
        }
        if (!string.Equals(AuthorLetters(iucnAuthority!), AuthorLetters(colAuthority!), StringComparison.OrdinalIgnoreCase)) {
            return null;
        }
        return (AuthorDisplay(iucnAuthority!), iucnYear, colYear);
    }

    // The author name on its own, for the detail sentence: brackets and the year taken off.
    private static string AuthorDisplay(string authority) {
        var text = authority.Replace("(", "").Replace(")", "").Replace("[", "").Replace("]", "");
        text = YearPattern.Replace(text, "");
        return text.Trim().TrimEnd(',', ';', ' ').Trim();
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
        SetExtra(finding, "colYear", ColYear(acceptedTarget));
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
            YearPublished = row.YearPublished,
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

    // OrdinalIgnoreCase rather than lowercasing first: this runs for every CoL candidate of every
    // assessed taxon, and Trim().ToLowerInvariant() allocated two strings per call to answer a
    // question neither trimming nor case can change.
    private static bool Looks(string? status, string token) =>
        !string.IsNullOrWhiteSpace(status) && status.Contains(token, StringComparison.OrdinalIgnoreCase);

    private static bool LooksInfraRank(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return false;
        }
        return rank.Contains("subspecies", StringComparison.OrdinalIgnoreCase)
            || rank.Contains("variety", StringComparison.OrdinalIgnoreCase)
            || rank.Contains("form", StringComparison.OrdinalIgnoreCase);
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

    // The letters of an authority string: only alphabetic characters (accented letters kept, so a
    // diacritic still counts as a difference). Everything else — spacing, digits (years), and all
    // punctuation (commas, brackets, periods, apostrophes, hyphens, ampersands) — is dropped, so two
    // authorities with the same letters differ only in formatting or year, which the "minor authority
    // differences" report deliberately ignores.
    private static string AuthorLetters(string value) {
        var sb = new StringBuilder(value.Length);
        foreach (var ch in value) {
            if (char.IsLetter(ch)) {
                sb.Append(ch);
            }
        }
        return sb.ToString();
    }

    // The year to show for a CoL name: its recorded name-published year when present, otherwise the
    // year embedded in the authorship string (author citations usually carry it).
    private static string? ColYear(ColTaxonRecord? record) {
        if (record is null) {
            return null;
        }
        if (!string.IsNullOrWhiteSpace(record.NamePublishedInYear)) {
            return record.NamePublishedInYear!.Trim();
        }
        return ExtractYear(record.Authorship);
    }

    private static string? ExtractYear(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var match = YearPattern.Match(text);
        return match.Success ? match.Value : null;
    }

    private static readonly System.Text.RegularExpressions.Regex YearPattern =
        new(@"1[5-9]\d\d|20\d\d", System.Text.RegularExpressions.RegexOptions.Compiled);

    private static void SetExtra(AuditFinding finding, string key, string? value) {
        if (!string.IsNullOrWhiteSpace(value)) {
            finding.Extra[key] = value!;
        }
    }

    // Column text for whether the CoL accepted name is already an IUCN synonym. Unknown (no API
    // cache) leaves the cell blank rather than asserting "no".
    // What the Catalogue of Life itself makes of a matched name. "other" covers the rarer usage
    // states (misapplied, ambiguous), which are neither an accepted name nor a plain synonym.
    private static string ColStatusLabel(string? status) =>
        IsAcceptedStatus(status) ? "accepted" : IsSynonymStatus(status) ? "synonym of" : "other";

    private static string? IucnSynonymLabel(IucnSynonymMatch match) => match switch {
        IucnSynonymMatch.SameTaxon => "synonym of this taxon",
        IucnSynonymMatch.OtherTaxon => "synonym of another taxon",
        IucnSynonymMatch.None => "not listed",
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
    public List<AuditFinding> ViaWiki { get; } = new();
    public List<AuditFinding> SynonymLead { get; } = new();
    public List<AuditFinding> CloseMatch { get; } = new();
    public List<AuditFinding> Synonym { get; } = new();
    public List<AuditFinding> AcceptedDiffers { get; } = new();
    public List<AuditFinding> SynonymHigher { get; } = new();
    public List<AuditFinding> Classification { get; } = new();
    public List<AuditFinding> Reorg { get; } = new();
    public List<AuditFinding> Authority { get; } = new();
    public int AssessedCompared { get; set; }
    public int HigherTaxaCompared { get; set; }

    // Whether the Wikidata/Wikipedia caches were read at all, and what they turned up for the rows
    // that stay on the not-found list (rows with a CoL name via those sources are in ViaWiki).
    // False means the check did not run, which is not the same as finding nothing.
    public bool OtherSourcesChecked { get; set; }
    public int OtherSourcesWithWikidata { get; set; }
    public int OtherSourcesWithWikipedia { get; set; }
}
