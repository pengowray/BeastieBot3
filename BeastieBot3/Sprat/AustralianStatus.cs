using System;

// Normalises the conservation-status vocabularies of the Australian listing systems (EPBC Act,
// IUCN Red List as carried in SPRAT, and the eight state/territory acts) to short display codes and
// a threatened/not-threatened decision. Each system has its own wording — EPBC uses the IUCN-style
// "Critically Endangered"/"Endangered"/"Vulnerable", while states add "Rare", "Near Threatened",
// "Presumed Extinct", parenthetical qualifiers ("Vulnerable (Extinct in NT)"), and occasional
// comma-joined multi-status cells. This is the Australian analogue of IucnRedlistStatus; it is kept
// deliberately small and is the single place to widen the "threatened" definition when state acts
// start gating membership (currently only EPBC + IUCN do — see SpratListQueryService).

namespace BeastieBot3.Sprat;

internal static class AustralianStatus {
    /// <summary>
    /// A short display code for a raw status string (e.g. "Critically Endangered" → "CR",
    /// "Near Threatened" → "NT", "Rare" → "Rare"), or null when the cell is blank. Robust to
    /// parenthetical qualifiers and comma-joined values (the first value is used). Unrecognised
    /// wording is returned trimmed so nothing is silently dropped from the annotation.
    /// </summary>
    public static string? ShortCode(string? raw) {
        var head = Head(raw);
        if (head is null) {
            return null;
        }

        var lower = head.ToLowerInvariant();
        if (lower.StartsWith("critically endangered")) return "CR";
        if (lower == "endangered" || lower.StartsWith("endangered ")) return "EN";
        if (lower == "vulnerable" || lower.StartsWith("vulnerable ")) return "VU";
        if (lower.StartsWith("extinct in the wild")) return "EW";
        if (lower == "extinct" || lower == "presumed extinct" || lower.StartsWith("extinct ")) return "EX";
        if (lower == "near threatened" || lower == "lower risk/near threatened" || lower == "lr/nt") return "NT";
        if (lower.Contains("conservation dependent") || lower == "lr/cd") return "CD";
        if (lower == "rare") return "Rare";
        if (lower == "lower risk/least concern" || lower == "lr/lc" || lower == "least concern") return "LC";
        if (lower == "data deficient") return "DD";
        return head;
    }

    /// <summary>
    /// True when the raw status is a threatened category — Critically Endangered, Endangered, or
    /// Vulnerable. This is the Phase-1 "threatened" definition shared by every system that gates
    /// list membership. (Extinct/EW and the state-specific "Rare"/"Near Threatened" are deliberately
    /// excluded for now; widen here when those systems start qualifying species.)
    /// </summary>
    public static bool IsThreatened(string? raw) {
        var code = ShortCode(raw);
        return code is "CR" or "EN" or "VU";
    }

    // First non-blank, paren-stripped value of a possibly comma-joined status cell.
    private static string? Head(string? raw) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return null;
        }

        var first = raw.Split(',')[0].Trim();
        var paren = first.IndexOf('(');
        if (paren > 0) {
            first = first[..paren].Trim();
        }
        return string.IsNullOrWhiteSpace(first) ? null : first;
    }
}
